package src

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// diskTrash is the trash on the local disk.
type diskTrash struct {
	root  string
	state trashStateStore
	index trashIndex
}

func newDiskTrash(root string) *diskTrash {
	statePath := filepath.Join(root, trashStateName)
	return &diskTrash{root: root, state: trashStateStore{
		load: func() ([]byte, error) {
			data, err := os.ReadFile(statePath)
			if errors.Is(err, fs.ErrNotExist) {
				return nil, nil
			}
			return data, err
		},
		save: func(data []byte) error {
			if err := os.MkdirAll(root, 0o755); err != nil {
				return err
			}
			temporaryPath := statePath + tempSuffix
			if err := os.WriteFile(temporaryPath, data, 0o644); err != nil {
				return err
			}
			return os.Rename(temporaryPath, statePath)
		},
	}}
}

func (t *diskTrash) List() ([]TrashItem, error) {
	var items []TrashItem
	err := filepath.WalkDir(t.root, func(p string, entry fs.DirEntry, err error) error {
		if err != nil {
			if p == t.root && errors.Is(err, fs.ErrNotExist) {
				return filepath.SkipAll
			}
			return err
		}
		if !entry.Type().IsRegular() {
			return nil
		}
		relative, err := filepath.Rel(t.root, p)
		if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
			return nil
		}
		itemPath, err := cleanTrashPath(filepath.ToSlash(relative))
		if err != nil {
			return nil // state and index files
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		sum, err := fileMD5(p)
		if err != nil {
			return err
		}
		items = append(items, TrashItem{Path: itemPath, MD5: sum, Size: info.Size()})
		return nil
	})
	if err != nil {
		return nil, err
	}
	t.state.fill(items)
	return items, nil
}

func (t *diskTrash) Remember(deletedAt map[string]time.Time) error {
	t.state.remember(deletedAt)
	return nil
}

func (t *diskTrash) Delete(p string) error {
	cleaned, err := cleanTrashPath(p)
	if err != nil {
		return err
	}
	target, err := safeJoin(t.root, cleaned)
	if err != nil {
		return err
	}
	info, err := os.Lstat(target)
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	if err == nil {
		if !info.Mode().IsRegular() {
			return fmt.Errorf("only files can be deleted from trash: %s", cleaned)
		}
		if err := os.Remove(target); err != nil {
			return err
		}
	}
	for dir := filepath.Dir(target); dir != t.root && strings.HasPrefix(dir, t.root) && os.Remove(dir) == nil; dir = filepath.Dir(dir) {
	}
	t.state.forget(cleaned)
	return nil
}
