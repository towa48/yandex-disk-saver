package src

import (
	"bytes"
	"strings"
	"time"
)

// webdavTrash is the trash on the WebDAV server; root is the collection path with a trailing slash.
type webdavTrash struct {
	client  *WebDAVClient
	root    string
	timeout time.Duration
	state   trashStateStore
	index   trashIndex
}

func newWebDAVTrash(client *WebDAVClient, root string, timeout time.Duration) *webdavTrash {
	statePath := root + trashStateName
	return &webdavTrash{client: client, root: root, timeout: timeout, state: trashStateStore{
		load: func() ([]byte, error) {
			var buffer bytes.Buffer
			if err := client.GetFile(statePath, timeout, &buffer); err != nil {
				if httpStatus(err) == 404 {
					return nil, nil
				}
				return nil, err
			}
			return buffer.Bytes(), nil
		},
		save: func(data []byte) error {
			return client.PutFile(statePath, bytes.NewReader(data), int64(len(data)), timeout)
		},
	}}
}

func (t *webdavTrash) List() ([]TrashItem, error) {
	var items []TrashItem
	if err := t.walk(t.root, &items); err != nil {
		return nil, err
	}
	t.state.fill(items)
	return items, nil
}

func (t *webdavTrash) walk(dir string, items *[]TrashItem) error {
	entries, err := t.client.ListDirectory(dir, t.timeout)
	if err != nil {
		if dir == t.root && httpStatus(err) == 404 {
			return nil
		}
		return err
	}
	for _, entry := range entries {
		p := entryPath(entry)
		if !strings.HasPrefix(p, t.root) || p == t.root {
			continue
		}
		if entry.IsDir {
			if err := t.walk(strings.TrimSuffix(p, "/")+"/", items); err != nil {
				return err
			}
			continue
		}
		itemPath, err := cleanTrashPath(strings.TrimPrefix(p, t.root))
		if err != nil {
			continue // state and index files
		}
		item := TrashItem{Path: itemPath}
		if sum := normalizeETag(entry.ETag); isMD5Hex(sum) {
			item.MD5 = sum
		}
		if entry.Size != nil {
			item.Size = *entry.Size
		}
		*items = append(*items, item)
	}
	return nil
}

func (t *webdavTrash) Remember(deletedAt map[string]time.Time) error {
	t.state.remember(deletedAt)
	return nil
}

func (t *webdavTrash) Delete(p string) error {
	cleaned, err := cleanTrashPath(p)
	if err != nil {
		return err
	}
	if err := t.client.Delete(t.root+strings.TrimPrefix(cleaned, "/"), t.timeout); err != nil && httpStatus(err) != 404 {
		return err
	}
	t.state.forget(cleaned)
	return nil
}
