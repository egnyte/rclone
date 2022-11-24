// Package egnyte provides an interface to Egnyte
package egnyte

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/egnyte/egnyte-go-sdk/egnyte"
	"github.com/pkg/errors"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/config/configstruct"
	"github.com/rclone/rclone/fs/fserrors"
	"github.com/rclone/rclone/fs/fshttp"
	"github.com/rclone/rclone/fs/hash"
	"github.com/rclone/rclone/lib/oauthutil"
	"github.com/rclone/rclone/lib/pacer"
	"golang.org/x/oauth2"
	"golang.org/x/sync/errgroup"
)

const (
	clientId                       = ""
	encryptedClientSecret          = ""
	minSleep                       = 200 * time.Millisecond
	retryAfterHeader               = "Retry-After"
	DefaultChunkUploadThreadsCount = 4
	DefaultChunkUploadSize         = 104857600 //100MB
	retry                          = 3
	chunkRetry                     = 120
)

var (
	oauthScopes = []string{egnyte.FilesystemScope}
	// Description of how to auth for this app
	endpoint          = egnyte.OAuthEndpoint("")
	egnyteOauthConfig = &oauth2.Config{
		Scopes:       oauthScopes,
		Endpoint:     endpoint,
		ClientID:     clientId,
		ClientSecret: encryptedClientSecret,
		RedirectURL:  oauthutil.RedirectPublicSecureURL,
	}
)

var RetryErrorCodes = []int{ // retryErrorCodes is a slice of error codes that we will retry
	http.StatusBadRequest,
	http.StatusRequestEntityTooLarge,
	http.StatusInternalServerError,
	http.StatusNotImplemented,
	http.StatusBadGateway,
	http.StatusServiceUnavailable,
	http.StatusGatewayTimeout,
	http.StatusInsufficientStorage,
	http.StatusForbidden,
}

// Register with Fs
func init() {
	fs.Register(&fs.RegInfo{
		Name:        "egnyte",
		Description: "Egnyte",
		NewFs:       NewFs,
		Config: func(ctx context.Context, name string, m configmap.Mapper, config fs.ConfigIn) (*fs.ConfigOut, error) {
			domain, ok := m.Get("domain")
			if !ok {
				log.Fatalf("Failed to configure token")
			}
			endpoint := egnyte.OAuthEndpoint(domain)
			if endpoint.AuthURL == "" {
				log.Fatalf("Failed to configure token: invalid domain")
			}
			egnyteOauthConfig.Endpoint = endpoint
			return oauthutil.ConfigOut("", &oauthutil.Options{
				OAuth2Config: egnyteOauthConfig,
				NoOffline:    true,
				OAuth2Opts: []oauth2.AuthCodeOption{
					oauth2.SetAuthURLParam("token_access_type", "offline"),
				},
			})

		},
		Options: []fs.Option{{
			Name: config.ConfigClientID,
			Help: "OAuth Client Id\n",
		},
			{
				Name: config.ConfigClientSecret,
				Help: "OAuth Client Secret\n",
			},
			{
				Name: "domain",
				Help: "Destination Egnyte domain",
			}, {
				Name:     "upload_threads",
				Help:     "Threads count for chunked upload of files",
				Default:  DefaultChunkUploadThreadsCount,
				Advanced: true,
			}},
	})
}

// Options defines the configuration for this backend
type Options struct {
	Token              string `config:"token"`
	Root               string
	Domain             string `config:"domain"`
	ChunkUploadThreads int    `config:"upload_threads"`
	ChunkSize          int64  `config:"chunk_size"`
}

// Fs represents a remote storage server
type Fs struct {
	name       string         // name of this remote
	targetPath string         // path to a file
	root       string         // the path we are working on if any
	opt        Options        // parsed options
	features   *fs.Features   // optional features
	svc        *egnyte.Client // the connection to the storage server
	pacer      *fs.Pacer      // To pace the API calls
	client     *http.Client   // authorized client
	rootSlash  string
}

// Object describes a storage object
type Object struct {
	fs        *Fs       // what this object is part of
	remote    string    // The remote path
	sha512sum string    // The SHA512 sum of the object
	bytes     int64     // Bytes in the object
	modTime   time.Time // Modified time of the object
	entryID   string    // Entry id of the object
}

// shouldRetry determines whether a given err rates being retried
func shouldRetry(err error) (again bool, errOut error) {
	if err != nil {

		if fserrors.ShouldRetry(err) {
			again = true
		} else {
			if egErr, ok := err.(*egnyte.Error); ok {
				for _, e := range RetryErrorCodes {
					if egErr.StatusCode == e {
						again = true
						break
					}
				}
			}
		}
		if egErr, ok := err.(*egnyte.Error); ok {
			if egErr.StatusCode == http.StatusUnauthorized {
				fs.Infof(nil, "Received 401 from Egnyte.")
			} else if egErr.StatusCode == http.StatusTooManyRequests {
				again = true
				var retryAfter = 3
				retryAfterString := egErr.Header.Get(retryAfterHeader)
				if retryAfterString != "" {
					var err error
					retryAfter, err = strconv.Atoi(retryAfterString)
					if err != nil {
						fs.Errorf(nil, "Malformed %s header %q: %v", retryAfterHeader, retryAfterString, err)
					}
				}
				err = pacer.RetryAfterError(err, time.Duration(retryAfter)*time.Second)
			}
		}
	}
	return again, err
}

// Name of the remote (as passed into NewFs)
func (f *Fs) Name() string {
	return f.name
}

// Root of the remote (as passed into NewFs)
func (f *Fs) Root() string {
	return f.root
}

// String converts this Fs to a string
func (f *Fs) String() string {
	return fmt.Sprintf("Egnyte root %s", f.root)
}

// Features returns the optional features of this Fs
func (f *Fs) Features() *fs.Features {
	return f.features
}

// Precision returns the precision
func (f *Fs) Precision() time.Duration {
	return time.Second
}

// Hashes returns the supported hash sets.
func (f *Fs) Hashes() hash.Set {
	return hash.Set(hash.None)
}

// NewFs constructs an Fs from the path
func NewFs(ctx context.Context, name, root string, m configmap.Mapper) (fs.Fs, error) {
	opt := new(Options)
	err := configstruct.Set(m, opt)
	if err != nil {
		return nil, err
	}
	if root == "" {
		root = opt.Root
	}

	f := &Fs{
		name:  name,
		root:  root,
		opt:   *opt,
		pacer: fs.NewPacer(ctx, pacer.NewEgnyteCloudDrive(pacer.MinSleep(minSleep))),
	}
	f.pacer.SetRetries(retry)
	f.setRoot(root)
	f.features = (&fs.Features{
		CaseInsensitive:         true,
		CanHaveEmptyDirectories: true,
	}).Fill(ctx, f)

	f.svc, err = f.NewClient(ctx)
	if err != nil {
		return f, err
	}
	f.opt.ChunkSize = DefaultChunkUploadSize
	err = f.pacer.Call(func() (bool, error) {
		_, err = f.svc.Object(f.root).Get(ctx)
		return shouldRetry(err)
	})
	if err == nil {
		f.root = path.Dir(root)
		return f, fs.ErrorIsFile
	}
	return f, nil
}

// setRoot changes the root of the Fs
func (f *Fs) setRoot(root string) {
	f.root = strings.TrimSuffix(root, "/")
	if f.root != "" {
		f.rootSlash = f.root + "/"
	}
}

func (f *Fs) relativeRemote(remote string) string {
	return strings.TrimPrefix(remote, f.rootSlash)
}

func (f *Fs) NewClient(ctx context.Context) (client *egnyte.Client, err error) {
	// Create a new authorized Egnyte client.
	f.client = fshttp.NewClient(ctx)
	token := oauth2.Token{}
	err = json.Unmarshal([]byte(f.opt.Token), &token)
	if err != nil {
		log.Fatalf("failed to parce access token")
		return nil, err
	}
	err = f.pacer.Call(func() (bool, error) {
		client, err = egnyte.NewClient(ctx, f.opt.Domain, token.AccessToken, f.client)
		return shouldRetry(err)
	})
	if err != nil {
		return client, errors.Wrap(err, "couldn't create Egnyte client")
	}
	return client, nil
}

// List the objects and directories in dir into entries.  The
// entries can be returned in any order but should be for a
// complete directory.
//
// dir should be "" to list the root, and should not have
// trailing slashes.
//
// This should return ErrDirNotFound if the directory isn't
// found.
func (f *Fs) List(ctx context.Context, dir string) (fs.DirEntries, error) {
	var object *egnyte.Object
	remote := f.root
	if dir != "" {
		remote += "/" + dir
	}
	if f.targetPath != "" {
		remote = f.targetPath
	}
	var err error
	err = f.pacer.Call(func() (bool, error) {
		object, err = f.svc.Object(remote).List(ctx)
		return shouldRetry(err)
	})
	if err != nil {
		if egErr, ok := err.(*egnyte.Error); ok {
			if egErr.StatusCode == http.StatusNotFound {
				err = fs.ErrorDirNotFound
			}
		}
		return nil, err
	}
	var entries fs.DirEntries
	for _, dir := range object.Folders {

		entry := fs.NewDir(f.relativeRemote(dir.Path), dir.ModTime).SetSize(int64(dir.Size))
		entries = append(entries, entry)
	}
	for _, file := range object.Files {
		remote := f.relativeRemote(file.Path)
		entry, err := f.newObjectWithInfo(ctx, remote, file)
		if err != nil {
			return nil, err
		}
		entries = append(entries, fs.Object(entry))
	}
	return entries, nil
}

// Return an Object from a path
//
// If it can't be found it returns the error fs.ErrorObjectNotFound.
func (f *Fs) newObjectWithInfo(ctx context.Context, remote string, info *egnyte.Object) (fs.Object, error) {
	o := &Object{
		fs:     f,
		remote: remote,
	}
	o.setMetaData(info)
	return o, nil
}

// NewObject finds the Object at remote.  If it can't be found
// it returns the error fs.ErrorObjectNotFound
func (f *Fs) NewObject(ctx context.Context, remote string) (fs.Object, error) {
	var object *egnyte.Object
	var err error
	err = f.pacer.Call(func() (bool, error) {
		object, err = f.svc.Object(path.Join(f.root, remote)).List(ctx)
		return shouldRetry(err)
	})
	if err != nil {
		if egErr, ok := err.(*egnyte.Error); ok {
			if egErr.StatusCode == http.StatusNotFound {
				return nil, fs.ErrorObjectNotFound
			}
		}
	}
	return f.newObjectWithInfo(ctx, remote, object)
}

// Put the object into the domain
//
// Copy the reader in to the new object which is returned
func (f *Fs) Put(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	o, err := f.NewObject(ctx, src.Remote())
	if err == fs.ErrorObjectNotFound {
		o = &Object{
			fs:     f,
			remote: src.Remote(),
		}
	}
	err = o.Update(ctx, in, src, options...)
	return o, err
}

// Mkdir creates the directory if it doesn't exist
func (f *Fs) Mkdir(ctx context.Context, dir string) error {
	remote := path.Join(f.root, dir)
	obj := f.svc.Object(remote)
	obj.IsFolder = true
	err := f.pacer.Call(func() (bool, error) {
		_, err := obj.Create(ctx)
		return shouldRetry(err)
	})
	return err
}

// Rmdir deletes the directory if the fs is at the root
func (f *Fs) Rmdir(ctx context.Context, dir string) error {
	o := &Object{
		fs:     f,
		remote: dir,
	}
	return o.Remove(ctx)

}

// SetModTime sets the modification time of the local fs object
func (o *Object) SetModTime(ctx context.Context, t time.Time) error {
	o.modTime = t
	return nil
}

// Open an object for read
func (o *Object) Open(ctx context.Context, options ...fs.OpenOption) (io.ReadCloser, error) {
	var resp io.ReadCloser
	var err error
	fullPath := path.Join(o.fs.Root(), o.Remote())
	err = o.fs.pacer.Call(func() (bool, error) {
		resp, err = o.fs.svc.Object(fullPath).Get(ctx)
		return shouldRetry(err)
	})
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// Returns the remote path for the object
func (o *Object) remotePath() string {
	return path.Join(o.fs.rootSlash, o.remote)
}

// Update the object with the contents of the io.Reader, modTime and size
func (o *Object) Update(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) error {
	remotePath := o.remotePath()
	modTime := src.ModTime(ctx)
	o.remote = src.Remote()
	o.bytes = src.Size()
	o.modTime = src.ModTime(ctx)
	var err error
	object := egnyte.Object{
		Client:  o.fs.svc,
		Path:    remotePath,
		Body:    in,
		Size:    int(src.Size()),
		ModTime: modTime,
	}
	if o.bytes > DefaultChunkUploadSize {
		err = o.fs.pacer.Call(func() (bool, error) {
			err := o.ChunkedUpload(ctx, path.Join(o.fs.Root(), o.remote), in, o.Size(), &object)
			return shouldRetry(err)
		})
	} else {
		err = o.fs.pacer.Call(func() (bool, error) {
			_, err := object.Create(ctx)
			return shouldRetry(err)
		})
	}
	if err != nil {
		fs.Debugf(nil, "unable to upload file %v, %v", path.Join(o.fs.Root(), o.remote), err)
		return fmt.Errorf("unable to upload file %v", path.Join(o.fs.Root(), o.remote))
	}

	return err
}

// Remove an object
func (o *Object) Remove(ctx context.Context) error {
	err := o.fs.pacer.Call(func() (bool, error) {
		return shouldRetry(o.fs.svc.Object(path.Join(o.fs.Root(), o.Remote())).Delete(ctx))
	})
	return err
}

// Fs returns the parent Fs
func (o *Object) Fs() fs.Info {
	return o.fs
}

// Hash returns the supported hash sets
func (o *Object) Hash(ctx context.Context, ty hash.Type) (string, error) {
	return o.sha512sum, nil
}

// Storable returns a boolean as to whether this object is storable
func (o *Object) Storable() bool {
	return true
}

// Return a string version
func (o *Object) String() string {
	if o == nil {
		return "<nil>"
	}
	return o.Remote()
}

// Remote returns the remote path
func (o *Object) Remote() string {
	return o.remote
}

// ModTime returns the modification time of the object
func (o *Object) ModTime(context.Context) time.Time {
	return o.modTime
}

// Size returns the size of an object in bytes
func (o *Object) Size() int64 {
	return o.bytes
}

// setMetaData sets the fs data from a egnyte.Object
func (o *Object) setMetaData(info *egnyte.Object) {
	if info != nil {
		o.bytes = int64(info.Size)
		o.sha512sum = info.Checksum
		o.modTime = info.ModTime
		o.entryID = info.EntryID
	}
}

// upload first chunk of large files
func (o *Object) uploadFirstChunk(ctx context.Context, path string, egObj *egnyte.Object, chunkInfo *egnyte.ChunkUploadInfo) (egnyte.UploadInfo, error) {
	chunkData, _, chunkNum, err := chunkInfo.GetChunk()
	if err != nil {
		return egnyte.UploadInfo{}, err
	}
	uploadInfo := egnyte.UploadInfo{
		Path:      path,
		Data:      bytes.NewReader(chunkData),
		Csum:      egnyte.SHA512Digest(chunkData),
		ChunkNum:  chunkNum,
		ChunkSize: DefaultChunkUploadSize,
		UploadID:  "",
	}
	extraHeaders := map[string]string{
		"X-Egnyte-Chunk-Num":             fmt.Sprint(uploadInfo.ChunkNum),
		"X-Egnyte-Chunk-Sha512-Checksum": uploadInfo.Csum,
	}

	err = o.fs.pacer.Call(func() (bool, error) {
		err := egObj.ChunkUpload(ctx, &uploadInfo, extraHeaders)
		if err != nil {
			uploadInfo.Data = bytes.NewReader(chunkData)
			fs.Infof(nil, "Upload interrupted - Retrying.:%v, %v", err.Error(), uploadInfo)
			return true, err
		}
		return false, err
	})
	return uploadInfo, err
}

func (o *Object) uploadChunk(ctx context.Context, path string, egObj *egnyte.Object, chunkInfo *egnyte.ChunkUploadInfo, sema chan struct{}, UploadID string) error {
	pacer := fs.NewPacer(ctx, pacer.NewEgnyteCloudDrive(pacer.MinSleep(minSleep)))
	pacer.SetRetries(chunkRetry)
	defer func() {
		<-sema
	}()
	chunkData, _, chunkNum, err := chunkInfo.GetChunk()
	if err != nil {
		return err
	}

	uploadInfo := egnyte.UploadInfo{
		Path:      path,
		Data:      bytes.NewReader(chunkData),
		Csum:      egnyte.SHA512Digest(chunkData),
		ChunkNum:  chunkNum,
		ChunkSize: DefaultChunkUploadSize,
		UploadID:  UploadID,
	}
	extraHeaders := map[string]string{
		"X-Egnyte-Chunk-Num":             fmt.Sprint(chunkNum),
		"X-Egnyte-Chunk-Sha512-Checksum": egnyte.SHA512Digest(chunkData),
		"X-Egnyte-Upload-Id":             uploadInfo.UploadID,
	}
	err = pacer.Call(func() (bool, error) {
		err := egObj.ChunkUpload(ctx, &uploadInfo, extraHeaders)
		if err != nil {
			uploadInfo.Data = bytes.NewReader(chunkData)
			fs.Infof(nil, "Upload interrupted - Retrying.:%v, %v", err.Error(), uploadInfo)
			return true, err
		}
		return false, err
	})
	chunkInfo.SetChunkCheckSum(uploadInfo.ChunkNum, uploadInfo.Csum)
	return err
}

// uploadLastChunk of large files
func (o *Object) uploadLastChunk(ctx context.Context, path string, egObj *egnyte.Object, chunkInfo *egnyte.ChunkUploadInfo, UploadID string) error {
	pacer := fs.NewPacer(ctx, pacer.NewEgnyteCloudDrive(pacer.MinSleep(minSleep)))
	pacer.SetRetries(chunkRetry)
	chunkData, chunkNum := chunkInfo.GetLastChunk()
	csum := egnyte.SHA512Digest(chunkData)
	uploadInfo := egnyte.UploadInfo{
		Path:      path,
		Data:      bytes.NewReader(chunkData),
		Csum:      egnyte.SHA512Digest(chunkData),
		ChunkNum:  chunkNum,
		ChunkSize: DefaultChunkUploadSize,
		UploadID:  UploadID,
	}

	chunkInfo.SetChunkCheckSum(chunkNum, csum)
	finalCsum := chunkInfo.GetResultCsum()
	uploadInfo.ChunkNum = chunkNum
	uploadInfo.Data = bytes.NewReader(chunkData)
	uploadInfo.Csum = egnyte.SHA512Digest([]byte(finalCsum))

	extraHeaders := map[string]string{
		"X-Egnyte-Chunk-Num": fmt.Sprint(uploadInfo.ChunkNum),
		"X-Sha512-Checksum": fmt.Sprintf("%s-%s-%s-%s", egnyte.CheckSumResponseVersion,
			fmt.Sprint(uploadInfo.ChunkNum), fmt.Sprint(uploadInfo.ChunkSize), uploadInfo.Csum),
		"X-Egnyte-Upload-Id":  uploadInfo.UploadID,
		"X-Egnyte-Last-Chunk": fmt.Sprint(true),
	}
	err := pacer.Call(func() (bool, error) {
		err := egObj.ChunkUpload(ctx, &uploadInfo, extraHeaders)
		if err != nil {
			uploadInfo.Data = bytes.NewReader(chunkData)
			fs.Infof(nil, "Upload interrupted - Retrying.:%v, %v", err.Error(), uploadInfo)
			return true, err
		}
		return false, err
	})

	return err
}

// ChunkedUpload: Upload large files in chunks
func (o *Object) ChunkedUpload(ctx context.Context, path string, in io.Reader, size int64, egObj *egnyte.Object) error {

	chunkInfo := egnyte.ChunkUploadInfo{}
	chunkInfo.Init(in, size, DefaultChunkUploadSize)
	var err error
	uploadInfo, err := o.uploadFirstChunk(ctx, path, egObj, &chunkInfo)
	if err != nil {
		return err
	}

	chunkInfo.SetChunkCheckSum(uploadInfo.ChunkNum, uploadInfo.Csum)
	limiter := make(chan struct{}, o.fs.opt.ChunkUploadThreads)
	g, gCtx := errgroup.WithContext(context.Background())
	for {

		switch {
		case gCtx.Err() == context.Canceled:
			return err
		default:
			limiter <- struct{}{}
		}
		if chunkInfo.GetRemainingBytes() == 0 {
			break
		}

		g.Go(func() (err error) {
			return o.uploadChunk(gCtx, path, egObj, &chunkInfo, limiter, uploadInfo.UploadID)
		})

	}
	err = g.Wait()
	if err != nil {
		return err
	}

	return o.uploadLastChunk(ctx, path, egObj, &chunkInfo, uploadInfo.UploadID)
}

// Check the interfaces are satisfied
var (
	_ fs.Fs     = (*Fs)(nil)
	_ fs.Object = (*Object)(nil)
)
