package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

type File struct {
	RemotePath        string `json:"remotePath"`
	Size              *int64 `json:"size"`
	ETag              string `json:"etag"`
	LastMod           string `json:"lastmod"`
	Status            string `json:"status"`
	Attempts          int    `json:"attempts"`
	NextRetryAt       *int64 `json:"nextRetryAt"`
	DownloadedAt      string `json:"downloadedAt,omitempty"`
	IntegrityFailures int    `json:"integrityFailures,omitempty"`
}

type Folder struct {
	Path     string   `json:"path"`
	Status   string   `json:"status"`
	Children []string `json:"children"`
}

type State struct {
	Version int                `json:"version"`
	Files   map[string]*File   `json:"files"`
	Folders map[string]*Folder `json:"folders"`
}

// StateStore guards State with a mutex; callbacks must not call back into the store.
type StateStore struct {
	mu    sync.Mutex
	path  string
	state State
}

func NewStateStore(path string) *StateStore {
	return &StateStore{
		path:  path,
		state: State{Version: 1, Files: map[string]*File{}, Folders: map[string]*Folder{}},
	}
}

func (s *StateStore) With(fn func(*State)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	fn(&s.state)
}

// Update applies fn and saves the result in one locked step.
func (s *StateStore) Update(fn func(*State)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	fn(&s.state)
	s.saveLocked()
}

func (s *StateStore) Save() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.saveLocked()
}

func (s *StateStore) saveLocked() {
	if err := s.writeFile(); err != nil {
		logMessage(fmt.Sprintf("Не удалось сохранить состояние: %v", err), "error")
	}
}

func (s *StateStore) writeFile() error {
	snapshot, err := json.MarshalIndent(&s.state, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		return err
	}
	temporaryPath := s.path + ".tmp"
	if err := os.WriteFile(temporaryPath, snapshot, 0o644); err != nil {
		return err
	}
	return os.Rename(temporaryPath, s.path)
}

func (s *StateStore) Load() {
	data, err := os.ReadFile(s.path)
	if err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			logMessage(fmt.Sprintf("Не удалось загрузить состояние: %v. Будет создано новое.", err), "error")
		}
		return
	}

	var saved State
	if err := json.Unmarshal(data, &saved); err != nil {
		logMessage(fmt.Sprintf("Не удалось загрузить состояние: %v. Будет создано новое.", err), "error")
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if saved.Files != nil {
		for key, file := range saved.Files {
			if file == nil {
				delete(saved.Files, key)
			}
		}
		s.state.Files = saved.Files
		s.state.Version = saved.Version
		if s.state.Version == 0 {
			s.state.Version = 1
		}
	}
	if saved.Folders != nil {
		for key, folder := range saved.Folders {
			if folder == nil {
				delete(saved.Folders, key)
				continue
			}
			folder.Children = uniqueSorted(folder.Children)
		}
		s.state.Folders = saved.Folders
	}
}

// uniqueSorted fixes duplicate folder entries.
func uniqueSorted(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	result := make([]string, 0, len(values))
	for _, value := range values {
		if _, exists := seen[value]; !exists {
			seen[value] = struct{}{}
			result = append(result, value)
		}
	}
	sort.Strings(result)
	return result
}
