package src

import (
	"encoding/json"
	"fmt"
	"path"
	"strings"
	"time"
)

const trashStateName = ".state.json"

// TrashItem describes a file inside the trash; Path is relative to the trash root ("/dir/file").
type TrashItem struct {
	Path      string
	MD5       string
	Size      int64
	DeletedAt time.Time
}

// Trash is a view of the .trash folder; implementations never touch anything outside it.
type Trash interface {
	List() ([]TrashItem, error)
	// Delete removes the file permanently.
	Delete(path string) error
	// Remember records deletion dates (by trash path) after files were moved into the trash.
	Remember(deletedAt map[string]time.Time) error
	// PurgeTrash permanently deletes trash files deleted before cutoff.
	PurgeTrash(cutoff time.Time) error
}

// cleanTrashPath validates a trash-relative path and rejects traversal and reserved root files.
func cleanTrashPath(p string) (string, error) {
	if p == "" || strings.ContainsAny(p, "\x00\\") {
		return "", fmt.Errorf("invalid trash path: %q", p)
	}
	for _, segment := range strings.Split(p, "/") {
		if segment == ".." {
			return "", fmt.Errorf("invalid trash path: %q", p)
		}
	}
	cleaned := path.Clean("/" + p)
	if cleaned == "/" || cleaned == "/"+trashStateName || cleaned == "/"+trashIndexName {
		return "", fmt.Errorf("invalid trash path: %q", p)
	}
	return cleaned, nil
}

type trashStateFile struct {
	Files map[string]trashStateEntry `json:"files"`
}

type trashStateEntry struct {
	DeletedAt time.Time `json:"deletedAt"`
}

// trashStateStore keeps deletion dates in .state.json; load returns nil data when the file is absent.
type trashStateStore struct {
	load func() ([]byte, error)
	save func([]byte) error
}

func (s trashStateStore) read() trashStateFile {
	state := trashStateFile{Files: map[string]trashStateEntry{}}
	data, err := s.load()
	if err != nil {
		logMessage(fmt.Sprintf("Failed to read %s: %v", trashStateName, err), "error")
		return state
	}
	if len(data) > 0 {
		if err := json.Unmarshal(data, &state); err != nil || state.Files == nil {
			logMessage(fmt.Sprintf("File %s is corrupted and will be recreated", trashStateName), "warn")
			state = trashStateFile{Files: map[string]trashStateEntry{}}
		}
	}
	return state
}

func (s trashStateStore) write(state trashStateFile) {
	data, err := json.MarshalIndent(state, "", "  ")
	if err == nil {
		err = s.save(data)
	}
	if err != nil {
		logMessage(fmt.Sprintf("Failed to save %s: %v", trashStateName, err), "error")
	}
}

// fill sets DeletedAt from the state; unknown files count as deleted just now and are saved, vanished ones are dropped.
func (s trashStateStore) fill(items []TrashItem) {
	state := s.read()
	now := time.Now()
	changed := false

	present := make(map[string]bool, len(items))
	for i := range items {
		present[items[i].Path] = true
		entry, known := state.Files[items[i].Path]
		if !known {
			entry = trashStateEntry{DeletedAt: now}
			state.Files[items[i].Path] = entry
			changed = true
		}
		items[i].DeletedAt = entry.DeletedAt
	}
	for p := range state.Files {
		if !present[p] {
			delete(state.Files, p)
			changed = true
		}
	}
	if changed {
		s.write(state)
	}
}

func (s trashStateStore) remember(deletedAt map[string]time.Time) {
	state := s.read()
	for p, at := range deletedAt {
		state.Files[p] = trashStateEntry{DeletedAt: at}
	}
	s.write(state)
}

func (s trashStateStore) forget(p string) {
	state := s.read()
	if _, known := state.Files[p]; known {
		delete(state.Files, p)
		s.write(state)
	}
}

func (d *diskStorage) Trash() Trash {
	return d.trash
}

func (w *webdavStorage) Trash() Trash {
	return w.trash
}

func (t *diskTrash) PurgeTrash(cutoff time.Time) error {
	return purgeTrashBefore(t, t.index, cutoff)
}

func (t *webdavTrash) PurgeTrash(cutoff time.Time) error {
	return purgeTrashBefore(t, t.index, cutoff)
}
