package src

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

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

func (a *App) localPathFor(remotePath string) (string, error) {
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

	localPath := filepath.Join(append([]string{a.cfg.DestinationDir}, segments...)...)
	relative, err := filepath.Rel(a.cfg.DestinationDir, localPath)
	if err != nil || strings.HasPrefix(relative, "..") || filepath.IsAbs(relative) {
		return "", fmt.Errorf("путь выходит за папку назначения: %s", remotePath)
	}
	return localPath, nil
}

func (a *App) getFileContents(remotePath string, timeout time.Duration) ([]byte, error) {
	data, err := a.client.GetFile(remotePath, timeout)
	if err == nil || httpStatus(err) == http.StatusServiceUnavailable {
		// for the 503 set ERROR status for the file
		return data, err
	}

	// trying to recreate the client if the error is not a 503
	a.client.CheckAndRecreate()
	return a.client.GetFile(remotePath, timeout)
}

func (a *App) fetchToDisk(remotePath, etag string, timeout time.Duration) error {
	destination, err := a.localPathFor(remotePath)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(destination), 0o755); err != nil {
		return err
	}

	contents, err := a.getFileContents(remotePath, timeout)
	if err != nil {
		return err
	}

	temporaryPath := destination + ".part"
	if err := os.WriteFile(temporaryPath, contents, 0o644); err != nil {
		return err
	}

	if etag != "" {
		expected := strings.ToLower(strings.Trim(strings.TrimPrefix(strings.TrimPrefix(etag, "W/"), "w/"), `"`))
		sum := md5.Sum(contents)
		actual := hex.EncodeToString(sum[:])

		if actual != expected {
			os.Remove(temporaryPath)
			os.Remove(destination)
			a.store.With(func(s *State) {
				if file := s.Files[remotePath]; file != nil {
					file.IntegrityFailures++
				}
			})
			return fmt.Errorf("MD5 не совпал: ожидался %s, получен %s", expected, actual)
		}
	}

	return os.Rename(temporaryPath, destination)
}

func (a *App) downloadFile(remotePath string, timeout time.Duration) {
	if a.ctx.Err() != nil {
		return
	}

	var etag string
	var found bool
	a.store.With(func(s *State) {
		if file := s.Files[remotePath]; file != nil {
			found, etag = true, file.ETag
		}
	})
	if !found {
		return
	}

	if timeout <= 0 {
		timeout = defaultDownloadTimeout
	}
	logMessage(fmt.Sprintf("Начало скачивания: %s. Таймаут: %.2f сек.", remotePath, timeout.Seconds()), "trace")

	a.store.Update(func(s *State) { s.Files[remotePath].Status = statusDownloading })

	if err := a.fetchToDisk(remotePath, etag, timeout); err != nil {
		logMessage(fmt.Sprintf("Ошибка %s: %v.", remotePath, err), "error")

		var attempts int
		a.store.Update(func(s *State) {
			file := s.Files[remotePath]
			file.Status = statusError
			file.Attempts++
			attempts = file.Attempts
		})

		// append the file to the error queue if it hasn't reached the max retries yet
		if attempts < maxFileRetries {
			a.errorQueue.Push(remotePath)
		}
		return
	}

	a.store.Update(func(s *State) {
		file := s.Files[remotePath]
		file.Status = statusCompleted
		file.DownloadedAt = time.Now().UTC().Format("2006-01-02T15:04:05.000Z")
	})
	logMessage(fmt.Sprintf("Скачан: %s", remotePath), "trace")
}
