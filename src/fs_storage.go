package src

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	trashDirName      = ".trash"
	trashIndexName    = "TRASH.md"
	reportFilePrefix  = "download-report-"
	reportFileExt     = ".html"
	replaceWarnName   = "REPLACE_WARN.md"
	deleteWarnName    = "DELETE_WARN.md"
	downloadErrorName = "DOWNLOAD_ERROR.md"
	tempSuffix        = ".tmp"
)

var serviceFiles = map[string]bool{
	"download-report" + reportFileExt: true,
	replaceWarnName:                   true,
	deleteWarnName:                    true,
	downloadErrorName:                 true,
	trashIndexName:                    true,
	trashStateName:                    true,
}

func isServiceName(name string) bool {
	return serviceFiles[name] || (strings.HasPrefix(name, reportFilePrefix) && strings.HasSuffix(name, reportFileExt))
}

// StorageEntry is a directory or a file of a Storage; Path is relative to the storage root ("/dir/file", "/dir/").
type StorageEntry struct {
	Path    string
	IsDir   bool
	Size    int64
	MD5     string // empty when the storage cannot tell it
	ModTime time.Time
}

// Storage is a file system the sync algorithm works with. Every nuance (temporary files,
// verification, directories, trash) lives in implementations; .trash is never exposed by ReadDir.
type Storage interface {
	Name() string
	// ReadDir returns the directory content: nested directories and files.
	ReadDir(dir string) ([]StorageEntry, error)
	// Open reads a file; size is the expected size (used to pick a timeout), -1 if unknown.
	Open(path string, size int64) (io.ReadCloser, error)
	// Put creates the file (and missing directories) via a temporary file, verifies it
	// against wantMD5 (when not empty) and size (when not negative), and replaces the original.
	Put(path string, r io.Reader, size int64, wantMD5 string) error
	// Copy duplicates a file inside the storage.
	Copy(from, to string) error
	// Remove moves files to .trash, records the deletion date in .state.json and rows in TRASH.md.
	// It returns the paths that were moved.
	Remove(paths []string) ([]string, error)
	// Trash returns this storage's trash.
	Trash() Trash
}

// cleanStoragePath validates a storage-relative path; .trash is off limits.
func cleanStoragePath(p string) (string, error) {
	if p == "" || strings.ContainsAny(p, "\x00\\") {
		return "", fmt.Errorf("invalid path: %q", p)
	}
	for _, segment := range strings.Split(p, "/") {
		if segment == ".." {
			return "", fmt.Errorf("invalid path: %q", p)
		}
	}
	cleaned := path.Clean("/" + p)
	if strings.HasSuffix(p, "/") && cleaned != "/" {
		cleaned += "/"
	}
	if cleaned == "/"+trashDirName || strings.HasPrefix(cleaned, "/"+trashDirName+"/") {
		return "", fmt.Errorf("trash path is not accessible: %q", p)
	}
	return cleaned, nil
}

// hiddenInStorage reports entries that never take part in sync: service files and .trash at the root, temporary files.
func hiddenInStorage(p string, isDir bool) bool {
	trimmed := strings.TrimSuffix(p, "/")
	name := path.Base(trimmed)
	if !isDir && strings.HasSuffix(name, tempSuffix) {
		return true
	}
	if path.Dir(trimmed) == "/" {
		return name == trashDirName || (!isDir && isServiceName(name))
	}
	return false
}

func fileMD5(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()

	hash := md5.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

type md5MismatchError struct{ expected, actual string }

func (e *md5MismatchError) Error() string {
	return fmt.Sprintf("MD5 mismatch: expected %s, got %s", e.expected, e.actual)
}

func errorCode(err error) string {
	if status := httpStatus(err); status != 0 {
		return strconv.Itoa(status)
	}
	if _, ok := err.(*md5MismatchError); ok {
		return "MD5"
	}
	return "ERR"
}

// normalizeETag strips quotes and the weak prefix so an MD5 ETag can be compared with a hash.
func normalizeETag(etag string) string {
	return strings.ToLower(strings.Trim(strings.TrimPrefix(strings.TrimPrefix(etag, "W/"), "w/"), `"`))
}

func isMD5Hex(value string) bool {
	_, err := hex.DecodeString(value)
	return len(value) == 32 && err == nil
}

func calculateDownloadTimeout(size int64) time.Duration {
	if size <= 0 {
		return defaultDownloadTimeout
	}

	const minSpeedMbps = 2
	const extraReserve = 5 * time.Second

	// (Mbps * 1024 * 1024) / 8 bits = bytes per second
	speedBytesPerSecond := float64(minSpeedMbps*1024*1024) / 8
	seconds := float64(size) / speedBytesPerSecond
	return time.Duration(seconds*float64(time.Second)) + extraReserve
}

func safeJoin(root, relativePath string) (string, error) {
	var segments []string
	for _, segment := range strings.Split(relativePath, "/") {
		if segment == "" {
			continue
		}
		if segment == "." || segment == ".." || strings.ContainsAny(segment, `\/`) {
			return "", fmt.Errorf("invalid path: %s", relativePath)
		}
		segments = append(segments, segment)
	}

	joined := root
	for _, segment := range segments {
		joined = joined + string(os.PathSeparator) + segment
	}
	return joined, nil
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func modeLabel(mode string) string {
	if mode == modeFSToWebDAV {
		return "fs => webdav"
	}
	return "webdav => fs"
}
