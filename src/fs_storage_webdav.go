package src

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"path"
	"sort"
	"strings"
	"sync"
	"time"
)

func entryPath(entry Entry) string {
	cleaned := path.Clean(entry.Path)
	if strings.HasSuffix(entry.Path, "/") && cleaned != "/" {
		cleaned += "/"
	}
	return cleaned
}

func normalizeWebDAVPath(p string) string {
	segments := strings.Split(p, "/")
	for i, segment := range segments {
		segment = strings.TrimRight(segment, " ")
		trailingDots := len(segment) - len(strings.TrimRight(segment, "."))
		segments[i] = strings.TrimRight(segment, ".") + strings.Repeat("_", trailingDots)
	}
	return strings.Join(segments, "/")
}

// webdavStorage is a Storage on a WebDAV server; root is the remote folder (with trailing slash) that maps to "/".
type webdavStorage struct {
	ctx       context.Context
	client    *WebDAVClient
	sleep     func(time.Duration)
	root      string
	retries   int
	trash     *webdavTrash
	trashRoot string

	dirsMu      sync.Mutex
	madeDirs    map[string]bool
	pathsMu     sync.RWMutex
	remotePaths map[string]string
}

func newWebDAVStorage(a *App) *webdavStorage {
	w := &webdavStorage{
		ctx:         a.ctx,
		client:      a.client,
		sleep:       a.sleep,
		root:        a.cfg.RemotePath,
		retries:     a.cfg.DirectoryRetries,
		madeDirs:    map[string]bool{},
		remotePaths: map[string]string{},
		trashRoot:   a.cfg.RemotePath + trashDirName + "/",
	}
	w.trash = newWebDAVTrash(w.client, w.trashRoot, directoryRequestTimeout)
	indexPath := w.trashRoot + trashIndexName
	w.trash.index = trashIndex{
		read: func() ([]trashEntry, error) {
			var buffer bytes.Buffer
			if err := w.client.GetFile(indexPath, directoryRequestTimeout, &buffer); err != nil {
				if httpStatus(err) == http.StatusNotFound {
					return nil, nil
				}
				return nil, err
			}
			return parseTrashIndex(buffer.String()), nil
		},
		write: func(entries []trashEntry) error {
			if err := w.makeDir(w.trashRoot); err != nil {
				return err
			}
			data := renderTrashIndex(entries)
			return w.client.PutFile(indexPath, strings.NewReader(data), int64(len(data)), directoryRequestTimeout)
		},
	}
	return w
}

func (w *webdavStorage) Name() string { return "WebDAV server" }

func (w *webdavStorage) remote(p string) (string, string, error) {
	cleaned, err := cleanStoragePath(p)
	if err != nil {
		return "", "", err
	}
	remoteRelative := cleaned
	w.pathsMu.RLock()
	if original, ok := w.remotePaths[cleaned]; ok {
		remoteRelative = original
	}
	w.pathsMu.RUnlock()
	if remoteRelative == cleaned {
		remoteRelative = normalizeWebDAVPath(remoteRelative)
	}
	return w.root + strings.TrimPrefix(remoteRelative, "/"), cleaned, nil
}

// list retries a failed request: the client is checked (and recreated if broken) before every repeat.
func (w *webdavStorage) list(remotePath string) ([]Entry, error) {
	entries, err := w.client.ListDirectory(remotePath, directoryRequestTimeout)
	for retry := 1; err != nil && retry <= w.retries && w.ctx.Err() == nil; retry++ {
		logMessage(fmt.Sprintf("Directory request error %s: %v. Retry %d of %d.", remotePath, err, retry, w.retries), "error")
		if isRateLimitError(err) {
			w.sleep(directoryListDelay)
		} else {
			w.client.CheckAndRecreate()
		}
		entries, err = w.client.ListDirectory(remotePath, directoryRequestTimeout)
	}
	return entries, err
}

func (w *webdavStorage) ReadDir(dir string) ([]StorageEntry, error) {
	remotePath, cleaned, err := w.remote(dir)
	if err != nil {
		return nil, err
	}
	if cleaned[len(cleaned)-1] != '/' {
		remotePath += "/"
	}

	w.sleep(directoryListDelay)
	if err := w.ctx.Err(); err != nil {
		return nil, err
	}
	entries, err := w.list(remotePath)
	if err != nil {
		return nil, err
	}

	var result []StorageEntry
	for _, entry := range entries {
		full := entryPath(entry)
		if !strings.HasPrefix(full, w.root) || full == w.root {
			continue
		}
		relative := "/" + strings.TrimPrefix(full, w.root)
		if entry.IsDir && !strings.HasSuffix(relative, "/") {
			relative += "/"
		}
		if _, err := cleanStoragePath(relative); err != nil || hiddenInStorage(relative, entry.IsDir) {
			continue
		}

		normalized := normalizeWebDAVPath(relative)
		w.pathsMu.Lock()
		w.remotePaths[normalized] = relative
		w.pathsMu.Unlock()
		item := StorageEntry{Path: normalized, IsDir: entry.IsDir}
		if !entry.IsDir {
			if entry.Size != nil {
				item.Size = *entry.Size
			}
			if sum := normalizeETag(entry.ETag); isMD5Hex(sum) {
				item.MD5 = sum
			}
			item.ModTime, _ = http.ParseTime(entry.LastMod)
		}
		result = append(result, item)
	}
	return result, nil
}

func (w *webdavStorage) Open(p string, size int64) (io.ReadCloser, error) {
	remotePath, _, err := w.remote(p)
	if err != nil {
		return nil, err
	}
	timeout := calculateDownloadTimeout(size)
	reader, err := w.client.OpenFile(remotePath, timeout)
	if err == nil || httpStatus(err) == http.StatusServiceUnavailable {
		// for the 503 the file gets ERROR status without a client check
		return reader, err
	}
	w.client.CheckAndRecreate()
	return w.client.OpenFile(remotePath, timeout)
}

func (w *webdavStorage) makeDir(remoteDir string) error {
	w.dirsMu.Lock()
	made := w.madeDirs[remoteDir]
	w.dirsMu.Unlock()
	if made {
		return nil
	}
	if err := w.client.MakeCollection(remoteDir, directoryRequestTimeout); err != nil {
		// 405/301: the collection already exists
		if status := httpStatus(err); status != http.StatusMethodNotAllowed && status != http.StatusMovedPermanently {
			return err
		}
	}
	w.dirsMu.Lock()
	w.madeDirs[remoteDir] = true
	w.dirsMu.Unlock()
	return nil
}

// ensureDirs creates every folder of remoteDir below the root.
func (w *webdavStorage) ensureDirs(remoteDir string) error {
	current := w.root
	for _, segment := range strings.Split(strings.Trim(strings.TrimPrefix(remoteDir, w.root), "/"), "/") {
		if segment == "" {
			continue
		}
		current += segment + "/"
		if err := w.makeDir(current); err != nil {
			return err
		}
	}
	return nil
}

func (w *webdavStorage) Put(p string, r io.Reader, size int64, wantMD5 string) error {
	remotePath, _, err := w.remote(p)
	if err != nil {
		return err
	}
	if err := w.ensureDirs(path.Dir(remotePath) + "/"); err != nil {
		return err
	}

	temporaryPath := remotePath + tempSuffix
	hash := md5.New()
	err = w.client.PutFile(temporaryPath, io.TeeReader(r, hash), size, calculateDownloadTimeout(size))
	if err == nil {
		err = w.verify(temporaryPath, size, wantMD5, hex.EncodeToString(hash.Sum(nil)))
	}
	if err == nil {
		err = w.client.Move(temporaryPath, remotePath, directoryRequestTimeout)
	}
	if err != nil {
		w.client.Delete(temporaryPath, directoryRequestTimeout)
		return err
	}
	return nil
}

// verify compares what was sent with the expectation and with what the server stored.
func (w *webdavStorage) verify(temporaryPath string, size int64, wantMD5, sent string) error {
	if wantMD5 != "" && sent != wantMD5 {
		return &md5MismatchError{expected: wantMD5, actual: sent}
	}
	entry, err := w.client.Stat(temporaryPath, directoryRequestTimeout)
	if err != nil {
		return err
	}
	// a server whose ETag is not an MD5 can only be checked by size
	if etag := normalizeETag(entry.ETag); isMD5Hex(etag) && etag != sent {
		return &md5MismatchError{expected: sent, actual: etag}
	}
	if entry.Size != nil && size >= 0 && *entry.Size != size {
		return fmt.Errorf("size on server %d, expected %d", *entry.Size, size)
	}
	return nil
}

func (w *webdavStorage) Copy(from, to string) error {
	source, _, err := w.remote(from)
	if err != nil {
		return err
	}
	destination, _, err := w.remote(to)
	if err != nil {
		return err
	}
	if err := w.ensureDirs(path.Dir(destination) + "/"); err != nil {
		return err
	}
	return w.client.Copy(source, destination, directoryRequestTimeout)
}

func (w *webdavStorage) Remove(paths []string) ([]string, error) {
	now := time.Now()
	var moved []trashEntry
	var removed []string
	for _, p := range paths {
		if w.ctx.Err() != nil {
			break
		}
		source, cleaned, err := w.remote(p)
		if err != nil {
			logMessage(err.Error(), "error")
			continue
		}
		destination := w.trashRoot + strings.TrimPrefix(cleaned, "/")
		var size int64
		entry, err := w.client.Stat(source, directoryRequestTimeout)
		if err == nil {
			if entry.Size != nil {
				size = *entry.Size
			}
			err = w.ensureDirs(path.Dir(destination) + "/")
		}
		if err == nil {
			err = w.client.Move(source, destination, directoryRequestTimeout)
		}
		if err != nil {
			logMessage(fmt.Sprintf("Failed to move %s to trash on server: %v", cleaned, err), "error")
			continue
		}
		moved = append(moved, trashEntry{Path: cleaned, Size: size, DeletedAt: now})
		removed = append(removed, cleaned)
	}
	if len(moved) == 0 {
		return nil, nil
	}
	sort.Strings(removed)
	return removed, recordTrashed(w.trash, w.trash.index, moved)
}
