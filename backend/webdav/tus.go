// /home/kf/projects/rclone_tus/rclone/backend/webdav/tus.go

package webdav

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/eventials/go-tus"
	"github.com/rclone/rclone/fs"
)

func (f *Fs) shouldRetryTusMerge(ctx context.Context, resp *http.Response, err error, sleepTime *time.Duration, wasLocked *bool) (bool, error) {
	// Not found. Can be returned by NextCloud when merging chunks of an upload.
	if resp != nil && resp.StatusCode == 404 {
		if *wasLocked {
			// Assume a 404 error after we've received a 423 error is actually a success
			return false, nil
		}
		return true, err
	}

	// 423 LOCKED
	if resp != nil && resp.StatusCode == 423 {
		*wasLocked = true
		fs.Logf(f, "Sleeping for %v to wait for chunks to be merged after 423 error", *sleepTime)
		time.Sleep(*sleepTime)
		*sleepTime *= 2
		return true, fmt.Errorf("merging the uploaded chunks failed with 423 LOCKED. This usually happens when the chunks merging is still in progress on NextCloud, but it may also indicate a failed transfer: %w", err)
	}

	return f.shouldRetry(ctx, resp, err)
}

// set the chunk size for testing
func (f *Fs) setUploadTusSize(cs fs.SizeSuffix) (old fs.SizeSuffix, err error) {
	old, f.opt.ChunkSize = f.opt.ChunkSize, cs
	return
}

func (o *Object) uploadFileViaTus(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (err error) {

	// create the tus client.
	url := o.fs.opt.URL

	config := tus.DefaultConfig()
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	c := &http.Client{Transport: tr}
	config.HttpClient = c
	client, _ := tus.NewClient(url, config)

	metadata := map[string]string{
		"filename": src.Remote(),
	}
	fingerprint := "my fingerprint"

	// create an upload from a file.
	upload := tus.NewUpload(in, src.Size(), metadata, fingerprint)

	// create the uploader.
	uploader, _ := client.CreateUpload(upload)

	// start the uploading process.
	err = uploader.Upload()

	return err
}
