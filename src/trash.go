package src

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

const trashTimeFormat = "2006-01-02 15:04:05"

type trashEntry struct {
	Path      string
	Size      int64
	DeletedAt time.Time
}

func (a *App) trashRoot() string { return filepath.Join(a.cfg.DestinationDir, trashDirName) }

func readTrashIndex(indexPath string) ([]trashEntry, error) {
	data, err := os.ReadFile(indexPath)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}

	var entries []trashEntry
	for _, line := range strings.Split(string(data), "\n") {
		if !strings.HasPrefix(strings.TrimSpace(line), "|") {
			continue
		}
		cells := splitMDRow(line)
		if len(cells) < 3 {
			continue
		}
		size, sizeErr := strconv.ParseInt(cells[1], 10, 64)
		deletedAt, timeErr := time.ParseInLocation(trashTimeFormat, cells[2], time.Local)
		if sizeErr != nil || timeErr != nil {
			continue // header or separator row
		}
		entries = append(entries, trashEntry{Path: cells[0], Size: size, DeletedAt: deletedAt})
	}
	return entries, nil
}

func writeTrashIndex(indexPath string, entries []trashEntry) error {
	sort.Slice(entries, func(i, j int) bool { return entries[i].Path < entries[j].Path })

	var content strings.Builder
	content.WriteString("# Корзина\n\nФайлы, удалённые с сервера и перемещённые в .trash.\n\n")
	content.WriteString("| Файл | Размер, байт | Дата удаления |\n| --- | --- | --- |\n")
	for _, entry := range entries {
		content.WriteString(mdRow(entry.Path, strconv.FormatInt(entry.Size, 10), entry.DeletedAt.Format(trashTimeFormat)))
	}
	return os.WriteFile(indexPath, []byte(content.String()), 0o644)
}

// moveFile renames, falling back to copy+remove across volumes.
func moveFile(source, destination string) error {
	if err := os.MkdirAll(filepath.Dir(destination), 0o755); err != nil {
		return err
	}
	if err := os.Remove(destination); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	if err := os.Rename(source, destination); err == nil {
		return nil
	}

	in, err := os.Open(source)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.Create(destination)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		os.Remove(destination)
		return err
	}
	if err := out.Close(); err != nil {
		os.Remove(destination)
		return err
	}
	in.Close()
	return os.Remove(source)
}

// handleDeletions trashes local files missing on the server, unless they exceed the size threshold.
func (a *App) handleDeletions() {
	var stale []LocalFile
	var staleSize, totalSize int64
	a.store.With(func(s *State) {
		var failedFolders []string
		for p, folder := range s.Folders {
			if folder.Status == statusError {
				failedFolders = append(failedFolders, p)
			}
		}
		underFailedFolder := func(p string) bool {
			for _, folder := range failedFolders {
				if strings.HasPrefix(p, folder) {
					return true
				}
			}
			return false
		}
		for p, local := range s.LocalFiles {
			totalSize += local.Size
			// content of a folder that failed to list is unknown, so its files are never stale
			if s.Files[p] == nil && !underFailedFolder(p) {
				stale = append(stale, *local)
				staleSize += local.Size
			}
		}
	})
	if len(stale) == 0 {
		return
	}
	sort.Slice(stale, func(i, j int) bool { return stale[i].Path < stale[j].Path })

	if staleSize*100 > int64(a.cfg.DeleteWarnPercent)*totalSize {
		logMessage(fmt.Sprintf("Файлы к удалению (%d шт.) превышают %d%% хранилища, удаление пропущено.", len(stale), a.cfg.DeleteWarnPercent), "warn")
		var rows [][]string
		for _, file := range stale {
			rows = append(rows, []string{file.Path, strconv.FormatInt(file.Size, 10)})
		}
		header := "# Предупреждение об удалении\n\nФайлы, которых больше нет на сервере, не удалены из файловой системы: " +
			fmt.Sprintf("их общий размер превышает %d%% размера хранилища.\n\n", a.cfg.DeleteWarnPercent) +
			"| Файл | Размер, байт |\n| --- | --- |\n"
		if err := a.appendRows(deleteWarnName, header, rows, true); err != nil {
			logMessage(fmt.Sprintf("Не удалось записать %s: %v", deleteWarnName, err), "error")
		}
		return
	}

	indexPath := filepath.Join(a.trashRoot(), trashIndexName)
	existing, err := readTrashIndex(indexPath)
	if err != nil {
		logMessage(fmt.Sprintf("Не удалось прочитать %s: %v", trashIndexName, err), "error")
		return
	}
	byPath := map[string]trashEntry{}
	for _, entry := range existing {
		byPath[entry.Path] = entry
	}

	moved := 0
	for _, file := range stale {
		source, err := a.localPathFor(file.Path)
		if err != nil {
			logMessage(err.Error(), "error")
			continue
		}
		destination, err := safeJoin(a.trashRoot(), file.Path)
		if err != nil {
			logMessage(err.Error(), "error")
			continue
		}
		if err := moveFile(source, destination); err != nil {
			logMessage(fmt.Sprintf("Не удалось переместить %s в корзину: %v", file.Path, err), "error")
			continue
		}
		byPath[file.Path] = trashEntry{Path: file.Path, Size: file.Size, DeletedAt: time.Now()}
		a.store.With(func(s *State) { delete(s.LocalFiles, file.Path) })
		moved++
	}
	if moved == 0 {
		return
	}

	entries := make([]trashEntry, 0, len(byPath))
	for _, entry := range byPath {
		entries = append(entries, entry)
	}
	if err := os.MkdirAll(a.trashRoot(), 0o755); err == nil {
		err = writeTrashIndex(indexPath, entries)
		if err != nil {
			logMessage(fmt.Sprintf("Не удалось записать %s: %v", trashIndexName, err), "error")
		}
	}
	logMessage(fmt.Sprintf("В корзину перемещено файлов: %d", moved), "info")
}

// purgeTrash permanently removes trash files older than the retention period.
func (a *App) purgeTrash() {
	indexPath := filepath.Join(a.trashRoot(), trashIndexName)
	entries, err := readTrashIndex(indexPath)
	if err != nil {
		logMessage(fmt.Sprintf("Не удалось прочитать %s: %v", trashIndexName, err), "error")
		return
	}
	if len(entries) == 0 {
		return
	}

	cutoff := time.Now().AddDate(0, 0, -a.cfg.TrashRetentionDays)
	kept := make([]trashEntry, 0, len(entries))
	for _, entry := range entries {
		if !entry.DeletedAt.Before(cutoff) {
			kept = append(kept, entry)
			continue
		}
		target, err := safeJoin(a.trashRoot(), entry.Path)
		if err != nil {
			logMessage(err.Error(), "error")
			continue
		}
		if err := os.Remove(target); err != nil && !errors.Is(err, fs.ErrNotExist) {
			logMessage(fmt.Sprintf("Не удалось удалить %s из корзины: %v", entry.Path, err), "error")
			kept = append(kept, entry)
			continue
		}
		for dir := filepath.Dir(target); dir != a.trashRoot() && os.Remove(dir) == nil; dir = filepath.Dir(dir) {
		}
		logMessage(fmt.Sprintf("Удалён из корзины окончательно: %s", entry.Path), "info")
	}

	if len(kept) == len(entries) {
		return
	}
	if err := writeTrashIndex(indexPath, kept); err != nil {
		logMessage(fmt.Sprintf("Не удалось записать %s: %v", trashIndexName, err), "error")
	}
}
