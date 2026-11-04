package src

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
)

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// runPipeline: scan WebDAV, scan disk, compare, copy duplicates, queue replacements and downloads, then clean up.
func (a *App) runPipeline() {
	defer a.stop()
	ready := a.prepareDownloads()
	a.mainQueue.Close()
	a.workers.Wait()

	if ready && a.ctx.Err() == nil {
		a.handleDeletions()
		a.purgeTrash()
		logMessage("=> Синхронизация завершена.", "info")
	}
}

func (a *App) prepareDownloads() bool {
	logMessage("Сканирую WebDAV...", "info")
	if err := a.scanRemote(a.cfg.RemotePath); err != nil {
		if a.ctx.Err() == nil {
			logMessage(fmt.Sprintf("Ошибка сканирования WebDAV: %v", err), "error")
			if isRateLimitError(err) {
				logMessage("Получен HTTP 429; клиент не пересоздаётся.", "warn")
			}
		}
		return false
	}

	logMessage("Сканирую файловую систему...", "info")
	if err := a.scanLocal(a.cfg.DestinationDir, "/"); err != nil {
		if a.ctx.Err() == nil {
			logMessage(fmt.Sprintf("Ошибка сканирования файловой системы: %v", err), "error")
		}
		return false
	}

	a.markMatchingCompleted()
	a.copyDuplicates()
	if a.ctx.Err() != nil {
		return false
	}
	a.queueReplacementsAndDownloads()
	return a.ctx.Err() == nil
}

func (a *App) markMatchingCompleted() {
	a.store.With(func(s *State) {
		for remotePath, file := range s.Files {
			local := s.LocalFiles[remotePath]
			if local != nil && local.Status == statusCompleted && local.MD5 != "" && local.MD5 == normalizeETag(file.ETag) {
				file.Status = statusCompleted
			}
		}
	})
}

type copyJob struct{ remotePath, sourcePath, md5 string }

// copyDuplicates fills pending files that are absent locally from local files with the same MD5.
func (a *App) copyDuplicates() {
	var jobs []copyJob
	a.store.With(func(s *State) {
		byMD5 := map[string]string{}
		for _, p := range sortedKeys(s.LocalFiles) {
			if local := s.LocalFiles[p]; local.Status == statusCompleted && local.MD5 != "" {
				if _, exists := byMD5[local.MD5]; !exists {
					byMD5[local.MD5] = p
				}
			}
		}
		for _, p := range sortedKeys(s.Files) {
			file := s.Files[p]
			if file.Status != statusPending || s.LocalFiles[p] != nil {
				continue
			}
			if sum := normalizeETag(file.ETag); sum != "" {
				if source, ok := byMD5[sum]; ok {
					jobs = append(jobs, copyJob{remotePath: p, sourcePath: source, md5: sum})
				}
			}
		}
	})

	for _, job := range jobs {
		if a.ctx.Err() != nil {
			return
		}
		size, err := a.copyLocal(job)
		if err != nil {
			logMessage(fmt.Sprintf("Не удалось скопировать %s -> %s: %v", job.sourcePath, job.remotePath, err), "error")
			continue
		}
		a.store.With(func(s *State) {
			s.Files[job.remotePath].Status = statusCompleted
			s.LocalFiles[job.remotePath] = &LocalFile{Path: job.remotePath, Size: size, MD5: job.md5, Status: statusCompleted}
		})
		logMessage(fmt.Sprintf("Скопирован: %s -> %s", job.sourcePath, job.remotePath), "trace")
	}
}

func (a *App) copyLocal(job copyJob) (int64, error) {
	source, err := a.localPathFor(job.sourcePath)
	if err != nil {
		return 0, err
	}
	destination, err := a.localPathFor(job.remotePath)
	if err != nil {
		return 0, err
	}
	if err := os.MkdirAll(filepath.Dir(destination), 0o755); err != nil {
		return 0, err
	}

	in, err := os.Open(source)
	if err != nil {
		return 0, err
	}
	defer in.Close()

	temporaryPath := destination + tempSuffix
	out, err := os.Create(temporaryPath)
	if err != nil {
		return 0, err
	}
	hash := md5.New()
	size, err := io.Copy(io.MultiWriter(out, hash), in)
	if closeErr := out.Close(); err == nil {
		err = closeErr
	}
	if err == nil && hex.EncodeToString(hash.Sum(nil)) != job.md5 {
		err = fmt.Errorf("MD5 источника изменился")
	}
	if err == nil {
		err = os.Rename(temporaryPath, destination)
	}
	if err != nil {
		os.Remove(temporaryPath)
		return 0, err
	}
	return size, nil
}

// queueReplacementsAndDownloads queues changed files (if under the size limit) first, then missing ones.
func (a *App) queueReplacementsAndDownloads() {
	type replaceRow struct {
		path       string
		remoteSize int64
		localSize  int64
	}
	var replace []replaceRow
	var missing []string
	var replaceSize, totalSize int64

	a.store.With(func(s *State) {
		for _, p := range sortedKeys(s.Files) {
			file := s.Files[p]
			var size int64
			if file.Size != nil {
				size = *file.Size
			}
			totalSize += size
			if file.Status != statusPending {
				continue
			}
			if local := s.LocalFiles[p]; local != nil {
				replace = append(replace, replaceRow{path: p, remoteSize: size, localSize: local.Size})
				replaceSize += size
			} else {
				missing = append(missing, p)
			}
		}
	})

	if replaceSize*100 > int64(a.cfg.ReplaceWarnPercent)*totalSize {
		logMessage(fmt.Sprintf("Замена %d файлов пропущена: размер превышает %d%% хранилища.", len(replace), a.cfg.ReplaceWarnPercent), "warn")
		var rows [][]string
		for _, row := range replace {
			rows = append(rows, []string{row.path, strconv.FormatInt(row.remoteSize, 10), strconv.FormatInt(row.localSize, 10)})
		}
		header := "# Предупреждение о замене\n\nФайлы на сервере отличаются от локальных (не совпал ETag), но не заменены: " +
			fmt.Sprintf("их общий размер на сервере превышает %d%% размера всех файлов на сервере.\n\n", a.cfg.ReplaceWarnPercent) +
			"| Файл | Размер на сервере, байт | Размер в файловой системе, байт |\n| --- | --- | --- |\n"
		if err := a.appendRows(replaceWarnName, header, rows, true); err != nil {
			logMessage(fmt.Sprintf("Не удалось записать %s: %v", replaceWarnName, err), "error")
		}
		replace = nil
	}

	for _, row := range replace {
		a.mainQueue.Push(row.path)
	}
	for _, p := range missing {
		a.mainQueue.Push(p)
	}
	logMessage(fmt.Sprintf("В очереди: замена %d, докачка %d.", len(replace), len(missing)), "info")
}
