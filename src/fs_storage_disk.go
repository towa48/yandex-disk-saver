package src

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"time"
)

// diskStorage is a Storage on the local disk rooted at root.
type diskStorage struct {
	ctx   context.Context
	root  string
	trash *diskTrash
}

func newDiskStorage(ctx context.Context, root string) *diskStorage {
	trashRoot := filepath.Join(root, trashDirName)
	indexPath := filepath.Join(trashRoot, trashIndexName)
	trash := newDiskTrash(trashRoot)
	trash.index = trashIndex{
		read:  func() ([]trashEntry, error) { return readTrashIndex(indexPath) },
		write: func(entries []trashEntry) error { return writeTrashIndex(indexPath, entries) },
	}
	return &diskStorage{
		ctx:   ctx,
		root:  root,
		trash: trash,
	}
}

func (d *diskStorage) Name() string { return "file system" }

func (d *diskStorage) resolve(p string) (string, string, error) {
	cleaned, err := cleanStoragePath(p)
	if err != nil {
		return "", "", err
	}
	full, err := safeJoin(d.root, cleaned)
	return full, cleaned, err
}

func (d *diskStorage) ReadDir(dir string) ([]StorageEntry, error) {
	if err := d.ctx.Err(); err != nil {
		return nil, err
	}
	full, cleaned, err := d.resolve(dir)
	if err != nil {
		return nil, err
	}
	if cleaned[len(cleaned)-1] != '/' {
		cleaned += "/"
	}
	entries, err := os.ReadDir(full)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) && cleaned == "/" {
			return nil, nil
		}
		return nil, err
	}

	var result []StorageEntry
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() {
			if child := cleaned + name + "/"; !hiddenInStorage(child, true) {
				result = append(result, StorageEntry{Path: child, IsDir: true})
			}
			continue
		}
		child := cleaned + name
		if !entry.Type().IsRegular() || hiddenInStorage(child, false) {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			logMessage(fmt.Sprintf("Failed to read %s: %v", child, err), "error")
			continue
		}
		if err := d.ctx.Err(); err != nil {
			return nil, err
		}
		sum, err := fileMD5(filepath.Join(full, name))
		if err != nil {
			logMessage(fmt.Sprintf("Failed to calculate MD5 of %s: %v", child, err), "error")
		}
		result = append(result, StorageEntry{Path: child, Size: info.Size(), MD5: sum, ModTime: info.ModTime()})
	}
	return result, nil
}

func (d *diskStorage) Open(p string, size int64) (io.ReadCloser, error) {
	full, _, err := d.resolve(p)
	if err != nil {
		return nil, err
	}
	return os.Open(full)
}

func (d *diskStorage) Put(p string, r io.Reader, size int64, wantMD5 string) error {
	full, _, err := d.resolve(p)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		return err
	}

	temporaryPath := full + tempSuffix
	file, err := os.Create(temporaryPath)
	if err != nil {
		return err
	}
	hash := md5.New()
	written, err := io.Copy(io.MultiWriter(file, hash), r)
	if closeErr := file.Close(); err == nil {
		err = closeErr
	}
	if err == nil && size >= 0 && written != size {
		err = fmt.Errorf("received %d bytes, expected %d", written, size)
	}
	if sum := hex.EncodeToString(hash.Sum(nil)); err == nil && wantMD5 != "" && sum != wantMD5 {
		err = &md5MismatchError{expected: wantMD5, actual: sum}
	}
	if err != nil {
		os.Remove(temporaryPath)
		return err
	}

	if err := os.Remove(full); err != nil && !errors.Is(err, fs.ErrNotExist) {
		os.Remove(temporaryPath)
		return err
	}
	if err := os.Rename(temporaryPath, full); err != nil {
		os.Remove(temporaryPath)
		return err
	}
	return nil
}

func (d *diskStorage) Copy(from, to string) error {
	source, err := d.Open(from, -1)
	if err != nil {
		return err
	}
	defer source.Close()
	return d.Put(to, source, -1, "")
}

func (d *diskStorage) Remove(paths []string) ([]string, error) {
	now := time.Now()
	var moved []trashEntry
	var removed []string
	for _, p := range paths {
		full, cleaned, err := d.resolve(p)
		if err != nil {
			logMessage(err.Error(), "error")
			continue
		}
		destination, err := safeJoin(filepath.Join(d.root, trashDirName), cleaned)
		if err != nil {
			logMessage(err.Error(), "error")
			continue
		}
		info, err := os.Lstat(full)
		if err != nil || !info.Mode().IsRegular() {
			logMessage(fmt.Sprintf("Failed to move %s to trash: file is not accessible", cleaned), "error")
			continue
		}
		if err := moveFile(full, destination); err != nil {
			logMessage(fmt.Sprintf("Failed to move %s to trash: %v", cleaned, err), "error")
			continue
		}
		moved = append(moved, trashEntry{Path: cleaned, Size: info.Size(), DeletedAt: now})
		removed = append(removed, cleaned)
	}
	if len(moved) == 0 {
		return nil, nil
	}
	return removed, recordTrashed(d.trash, d.trash.index, moved)
}
