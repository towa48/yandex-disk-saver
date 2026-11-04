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

const trashTimeFormat = time.DateTime

type trashEntry struct {
	Path      string
	Size      int64
	DeletedAt time.Time
}

func parseTrashIndex(data string) []trashEntry {
	var entries []trashEntry
	for _, line := range strings.Split(data, "\n") {
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
	return entries
}

func readTrashIndex(indexPath string) ([]trashEntry, error) {
	data, err := os.ReadFile(indexPath)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	return parseTrashIndex(string(data)), nil
}

func renderTrashIndex(entries []trashEntry) string {
	sort.Slice(entries, func(i, j int) bool { return entries[i].Path < entries[j].Path })

	var content strings.Builder
	content.WriteString("# Trash\n\nFiles deleted during sync and moved to .trash.\n\n")
	content.WriteString("| File | Size, bytes | Deleted at |\n| --- | --- | --- |\n")
	for _, entry := range entries {
		content.WriteString(mdRow(entry.Path, strconv.FormatInt(entry.Size, 10), entry.DeletedAt.Format(trashTimeFormat)))
	}
	return content.String()
}

func writeTrashIndex(indexPath string, entries []trashEntry) error {
	if err := os.MkdirAll(filepath.Dir(indexPath), 0o755); err != nil {
		return err
	}
	return os.WriteFile(indexPath, []byte(renderTrashIndex(entries)), 0o644)
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

// trashIndex reads and writes TRASH.md of one storage.
type trashIndex struct {
	read  func() ([]trashEntry, error)
	write func([]trashEntry) error
}

// recordTrashed saves deletion dates to .state.json and adds rows to TRASH.md after files were moved into the trash.
func recordTrashed(trash Trash, index trashIndex, moved []trashEntry) error {
	dates := make(map[string]time.Time, len(moved))
	for _, entry := range moved {
		dates[entry.Path] = entry.DeletedAt
	}
	if err := trash.Remember(dates); err != nil {
		return err
	}

	existing, err := index.read()
	if err != nil {
		return err
	}
	byPath := make(map[string]trashEntry, len(existing)+len(moved))
	for _, entry := range existing {
		byPath[entry.Path] = entry
	}
	for _, entry := range moved {
		byPath[entry.Path] = entry
	}
	entries := make([]trashEntry, 0, len(byPath))
	for _, entry := range byPath {
		entries = append(entries, entry)
	}
	return index.write(entries)
}

// purgeTrashBefore permanently deletes trash files deleted before cutoff and drops them from TRASH.md.
func purgeTrashBefore(trash Trash, index trashIndex, cutoff time.Time) error {
	items, err := trash.List()
	if err != nil {
		return fmt.Errorf("failed to read trash: %w", err)
	}

	deleted := map[string]bool{}
	for _, item := range items {
		if !item.DeletedAt.Before(cutoff) {
			continue
		}
		if err := trash.Delete(item.Path); err != nil {
			logMessage(fmt.Sprintf("Failed to delete %s from trash: %v", item.Path, err), "error")
			continue
		}
		deleted[item.Path] = true
		logMessage(fmt.Sprintf("Permanently deleted from trash: %s", item.Path), "info")
	}
	if len(deleted) == 0 {
		return nil
	}

	entries, err := index.read()
	if err != nil {
		return err
	}
	kept := make([]trashEntry, 0, len(entries))
	for _, entry := range entries {
		if !deleted[entry.Path] {
			kept = append(kept, entry)
		}
	}
	if len(kept) == len(entries) {
		return nil
	}
	return index.write(kept)
}
