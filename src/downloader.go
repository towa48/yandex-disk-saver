package src

import (
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type md5MismatchError struct{ expected, actual string }

func (e *md5MismatchError) Error() string {
	return fmt.Sprintf("MD5 не совпал: ожидался %s, получен %s", e.expected, e.actual)
}

func errorCode(err error) string {
	if status := httpStatus(err); status != 0 {
		return strconv.Itoa(status)
	}
	var mismatch *md5MismatchError
	if errors.As(err, &mismatch) {
		return "MD5"
	}
	return "ERR"
}

// normalizeETag strips quotes and the weak prefix so an MD5 ETag can be compared with a hash.
func normalizeETag(etag string) string {
	return strings.ToLower(strings.Trim(strings.TrimPrefix(strings.TrimPrefix(etag, "W/"), "w/"), `"`))
}

func calculateDownloadTimeout(size *int64) time.Duration {
	if size == nil || *size == 0 {
		return 0
	}

	const minSpeedMbps = 2
	const extraReserve = 5 * time.Second

	// (Mbps * 1024 * 1024) / 8 bits = bytes per second
	speedBytesPerSecond := float64(minSpeedMbps*1024*1024) / 8
	seconds := float64(*size) / speedBytesPerSecond
	return time.Duration(seconds*float64(time.Second)) + extraReserve
}

func safeJoin(root, remotePath string) (string, error) {
	var segments []string
	for _, segment := range strings.Split(remotePath, "/") {
		if segment == "" {
			continue
		}
		if segment == "." || segment == ".." || strings.ContainsAny(segment, `\/`) {
			return "", fmt.Errorf("недопустимый путь WebDAV: %s", remotePath)
		}
		segments = append(segments, segment)
	}

	localPath := filepath.Join(append([]string{root}, segments...)...)
	relative, err := filepath.Rel(root, localPath)
	if err != nil || strings.HasPrefix(relative, "..") || filepath.IsAbs(relative) {
		return "", fmt.Errorf("путь выходит за папку назначения: %s", remotePath)
	}
	return localPath, nil
}

func (a *App) localPathFor(remotePath string) (string, error) {
	return safeJoin(a.cfg.DestinationDir, remotePath)
}

// fetchToFile streams the file to temporaryPath and returns its MD5.
func (a *App) fetchToFile(remotePath, temporaryPath string, timeout time.Duration) (string, error) {
	attempt := func() (string, error) {
		file, err := os.Create(temporaryPath)
		if err != nil {
			return "", err
		}
		hash := md5.New()
		err = a.client.GetFile(remotePath, timeout, io.MultiWriter(file, hash))
		if closeErr := file.Close(); err == nil {
			err = closeErr
		}
		return hex.EncodeToString(hash.Sum(nil)), err
	}

	sum, err := attempt()
	if err == nil || httpStatus(err) == http.StatusServiceUnavailable {
		// for the 503 set ERROR status for the file
		return sum, err
	}

	// trying to recreate the client if the error is not a 503
	a.client.CheckAndRecreate()
	return attempt()
}

// fetchToDisk downloads to <file>.tmp, verifies MD5, then swaps it in for the original.
func (a *App) fetchToDisk(remotePath, etag string, timeout time.Duration) error {
	destination, err := a.localPathFor(remotePath)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(destination), 0o755); err != nil {
		return err
	}

	temporaryPath := destination + ".tmp"
	actual, err := a.fetchToFile(remotePath, temporaryPath, timeout)
	if err != nil {
		os.Remove(temporaryPath)
		return err
	}

	if etag != "" {
		if expected := normalizeETag(etag); actual != expected {
			os.Remove(temporaryPath)
			return &md5MismatchError{expected: expected, actual: actual}
		}
	}

	if err := os.Remove(destination); err != nil && !errors.Is(err, fs.ErrNotExist) {
		os.Remove(temporaryPath)
		return err
	}
	if err := os.Rename(temporaryPath, destination); err != nil {
		os.Remove(temporaryPath)
		return err
	}
	return nil
}

// attemptDownload makes one download attempt and updates the file status; it reports success.
func (a *App) attemptDownload(remotePath string) bool {
	if a.ctx.Err() != nil {
		return false
	}

	var etag string
	var size *int64
	var found bool
	a.store.With(func(s *State) {
		if file := s.Files[remotePath]; file != nil {
			found, etag, size = true, file.ETag, file.Size
		}
	})
	if !found {
		return false
	}

	timeout := calculateDownloadTimeout(size)
	if timeout <= 0 {
		timeout = defaultDownloadTimeout
	}
	logMessage(fmt.Sprintf("Начало скачивания: %s. Таймаут: %.2f сек.", remotePath, timeout.Seconds()), "trace")

	if err := a.fetchToDisk(remotePath, etag, timeout); err != nil {
		logMessage(fmt.Sprintf("Ошибка %s: %v.", remotePath, err), "error")

		var attempts int
		a.store.With(func(s *State) {
			file := s.Files[remotePath]
			file.Status = statusError
			file.Attempts++
			file.ErrorCode = errorCode(err)
			attempts = file.Attempts
		})
		if attempts >= a.cfg.MaxFileRetries {
			a.appendDownloadError(remotePath, size, errorCode(err))
		}
		return false
	}

	var localSize int64
	if destination, err := a.localPathFor(remotePath); err == nil {
		if info, err := os.Stat(destination); err == nil {
			localSize = info.Size()
		}
	}
	a.store.With(func(s *State) {
		s.Files[remotePath].Status = statusCompleted
		s.LocalFiles[remotePath] = &LocalFile{Path: remotePath, Size: localSize, MD5: normalizeETag(etag), Status: statusCompleted}
	})
	logMessage(fmt.Sprintf("Скачан: %s", remotePath), "trace")
	return true
}
