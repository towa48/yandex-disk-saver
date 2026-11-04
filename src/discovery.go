package src

import (
	"fmt"
	"path"
	"strings"
)

func entryPath(entry Entry) string {
	cleaned := path.Clean(entry.Path)
	if strings.HasSuffix(entry.Path, "/") && cleaned != "/" {
		cleaned += "/"
	}
	return cleaned
}

func (a *App) getDirectoryContents(remotePath string) ([]Entry, error) {
	entries, err := a.client.ListDirectory(remotePath, directoryRequestTimeout)
	for retry := 1; err != nil && retry <= a.cfg.DirectoryRetries && a.ctx.Err() == nil; retry++ {
		logMessage(fmt.Sprintf("Ошибка запроса каталога %s: %v. Повтор %d из %d.", remotePath, err, retry, a.cfg.DirectoryRetries), "error")
		if isRateLimitError(err) {
			a.sleep(directoryListDelay)
		} else {
			a.client.CheckAndRecreate()
		}
		entries, err = a.client.ListDirectory(remotePath, directoryRequestTimeout)
	}
	return entries, err
}

// scanRemote builds the WebDAV tree recursively; every file starts as pending.
func (a *App) scanRemote(remotePath string) error {
	if err := a.ctx.Err(); err != nil {
		return err
	}
	a.sleep(directoryListDelay)
	if err := a.ctx.Err(); err != nil {
		return err
	}

	entries, err := a.getDirectoryContents(remotePath)
	if err != nil {
		if ctxErr := a.ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		logMessage(fmt.Sprintf("Каталог %s не получен, помечен как error: %v", remotePath, err), "error")
		a.store.With(func(s *State) {
			s.Folders[remotePath] = &Folder{Path: remotePath, Status: statusError, Children: []string{}}
		})
		return nil
	}
	logMessage(fmt.Sprintf("directory contents for %s: %d entries", remotePath, len(entries)), "info")

	folder := &Folder{Path: remotePath, Status: statusPending, Children: []string{}}
	a.store.With(func(s *State) {
		s.Folders[remotePath] = folder
		for _, entry := range entries {
			p := entryPath(entry)
			if entry.IsDir {
				if !strings.HasSuffix(p, "/") {
					p += "/"
				}
				folder.Children = append(folder.Children, p)
				continue
			}
			s.Files[p] = &File{
				RemotePath: p,
				Size:       entry.Size,
				ETag:       entry.ETag,
				LastMod:    entry.LastMod,
				Status:     statusPending,
			}
		}
	})

	for _, child := range folder.Children {
		if err := a.scanRemote(child); err != nil {
			return err
		}
	}

	a.store.With(func(*State) { folder.Status = statusCompleted })
	return nil
}
