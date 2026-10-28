package main

import (
	"context"
	"errors"
	"fmt"
	"path"
	"strings"
)

type RemoteFile struct {
	RemotePath string
	Size       *int64
	ETag       string
	LastMod    string
}

func metadataChanged(previous *File, remote RemoteFile) bool {
	if previous.ETag != "" && remote.ETag != "" {
		return previous.ETag != remote.ETag
	}
	if previous.Size != nil && remote.Size != nil && *previous.Size != *remote.Size {
		return true
	}
	return previous.LastMod != "" && remote.LastMod != "" && previous.LastMod != remote.LastMod
}

func sameSize(a, b *int64) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

func entryPath(entry Entry) string {
	cleaned := path.Clean(entry.Path)
	if strings.HasSuffix(entry.Path, "/") && cleaned != "/" {
		cleaned += "/"
	}
	return cleaned
}

func (a *App) getDirectoryContents(remotePath string, retry int) ([]Entry, error) {
	entries, err := a.client.ListDirectory(remotePath, directoryRequestTimeout)
	if err == nil {
		return entries, nil
	}

	if isRateLimitError(err) && retry < 3 {
		a.sleep(directoryListDelay)
		return a.getDirectoryContents(remotePath, retry+1)
	}

	logMessage(fmt.Sprintf("Ошибка запроса каталога %s: %v. Проверяю подключение к этому же каталогу.", remotePath, err), "error")
	a.client.CheckAndRecreate()
	return a.client.ListDirectory(remotePath, directoryRequestTimeout)
}

func (a *App) listRemoteFiles(remotePath string, onFile func(RemoteFile)) error {
	if err := a.ctx.Err(); err != nil {
		return err
	}

	var folder *Folder
	a.store.With(func(s *State) {
		if existing := s.Folders[remotePath]; existing != nil {
			folder = existing
		} else {
			folder = &Folder{Path: remotePath, Status: statusPending, Children: []string{}}
		}
	})

	if folder.Status == statusCompleted {
		return nil
	}

	var entries []Entry
	if folder.Status == statusDownloading {
		// resumed folder: only the known child folders are rescanned
		for _, child := range folder.Children {
			entries = append(entries, Entry{Path: child, IsDir: true})
		}
	} else {
		a.sleep(directoryListDelay)
		if err := a.ctx.Err(); err != nil {
			return err
		}
		var err error
		if entries, err = a.getDirectoryContents(folder.Path, 0); err != nil {
			return err
		}
	}

	a.store.With(func(s *State) {
		folder.Status = statusDownloading
		s.Folders[remotePath] = folder
	})

	logMessage(fmt.Sprintf("directory contents for %s: %d entries", folder.Path, len(entries)), "info")

	var childrenFolders []string
	var childrenFiles []RemoteFile
	for _, entry := range entries {
		p := entryPath(entry)
		if entry.IsDir {
			if !strings.HasSuffix(p, "/") {
				p += "/"
			}
			childrenFolders = append(childrenFolders, p)
		} else {
			childrenFiles = append(childrenFiles, RemoteFile{
				RemotePath: p,
				Size:       entry.Size,
				ETag:       entry.ETag,
				LastMod:    entry.LastMod,
			})
		}
	}

	logMessage(fmt.Sprintf("children folders for %s: %d entries", folder.Path, len(childrenFolders)), "trace")

	a.store.Update(func(*State) {
		for _, child := range childrenFolders {
			if !contains(folder.Children, child) {
				folder.Children = append(folder.Children, child)
			}
		}
		logMessage(fmt.Sprintf("children updated for %s", folder.Path), "trace")
	})

	logMessage(fmt.Sprintf("process files of %s", folder.Path), "trace")
	for _, file := range childrenFiles {
		onFile(file)
	}
	a.store.Save()

	var children []string
	a.store.With(func(*State) { children = append(children, folder.Children...) })
	for _, child := range children {
		logMessage(fmt.Sprintf("process next child for %s: %s", folder.Path, child), "trace")
		if err := a.listRemoteFiles(child, onFile); err != nil {
			return err
		}
	}

	a.store.Update(func(*State) {
		folder.Status = statusCompleted
		logMessage(fmt.Sprintf("status completed for %s", folder.Path), "trace")
	})
	return nil
}

func contains(values []string, value string) bool {
	for _, candidate := range values {
		if candidate == value {
			return true
		}
	}
	return false
}

func (a *App) registerRemoteFile(remote RemoteFile) {
	a.store.With(func(s *State) {
		previous := s.Files[remote.RemotePath]
		switch {
		case previous == nil:
			s.Files[remote.RemotePath] = &File{
				RemotePath: remote.RemotePath,
				Size:       remote.Size,
				ETag:       remote.ETag,
				LastMod:    remote.LastMod,
				Status:     statusPending,
			}
			a.mainQueue.Push(remote.RemotePath)
		case previous.Status == statusCompleted:
		case metadataChanged(previous, remote):
			if previous.Status == statusDownloading {
				return
			}
			previous.Size, previous.ETag, previous.LastMod = remote.Size, remote.ETag, remote.LastMod
			previous.Status = statusPending
			previous.Attempts = 0
			a.mainQueue.Push(remote.RemotePath)
		case !sameSize(previous.Size, remote.Size) || previous.ETag != remote.ETag || previous.LastMod != remote.LastMod:
			previous.Size, previous.ETag, previous.LastMod = remote.Size, remote.ETag, remote.LastMod
		}
	})
}

func (a *App) discoverRemoteFiles() {
	defer func() {
		a.mainQueue.Close()
		logMessage("=> Сканирование завершено.", "info")
	}()

	err := a.listRemoteFiles(a.cfg.RemotePath, a.registerRemoteFile)
	if err == nil || errors.Is(err, context.Canceled) {
		return
	}
	logMessage(fmt.Sprintf("Ошибка сканирования WebDAV: %v", err), "error")
	if isRateLimitError(err) {
		logMessage("Получен HTTP 429; клиент не пересоздаётся.", "warn")
	}
}
