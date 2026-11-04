package src

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
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

// scanLocal builds the filesystem tree; a file stays pending until its MD5 is stored.
func (a *App) scanLocal(dir, treePath string) error {
	if err := a.ctx.Err(); err != nil {
		return err
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}

	folder := &LocalFolder{Path: treePath, Status: statusPending, Children: []string{}}
	a.store.With(func(s *State) { s.LocalFolders[treePath] = folder })

	for _, entry := range entries {
		name := entry.Name()
		if treePath == "/" && (name == trashDirName || serviceFiles[name] ||
			(strings.HasPrefix(name, reportFilePrefix) && strings.HasSuffix(name, reportFileExt))) {
			continue
		}

		childPath := treePath + name
		if entry.IsDir() {
			childPath += "/"
			a.store.With(func(*State) { folder.Children = append(folder.Children, childPath) })
			if err := a.scanLocal(filepath.Join(dir, name), childPath); err != nil {
				return err
			}
			continue
		}
		if !entry.Type().IsRegular() || strings.HasSuffix(name, tempSuffix) {
			continue
		}

		info, err := entry.Info()
		if err != nil {
			logMessage(fmt.Sprintf("Не удалось прочитать %s: %v", childPath, err), "error")
			continue
		}
		local := &LocalFile{Path: childPath, Size: info.Size(), Status: statusPending}
		a.store.With(func(s *State) {
			s.LocalFiles[childPath] = local
			folder.Children = append(folder.Children, childPath)
		})

		sum, err := fileMD5(filepath.Join(dir, name))
		if err != nil {
			logMessage(fmt.Sprintf("Не удалось посчитать MD5 %s: %v", childPath, err), "error")
			a.store.With(func(s *State) { delete(s.LocalFiles, childPath) })
			continue
		}
		a.store.With(func(*State) { local.MD5, local.Status = sum, statusCompleted })
	}

	a.store.With(func(*State) { folder.Status = statusCompleted })
	return nil
}
