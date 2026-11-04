package src

import (
	"sync"
	"time"
)

// FileNode is a file in a scanned tree. Status, Attempts and ErrorCode are the sync state of a source file.
type FileNode struct {
	Path      string
	Size      int64
	MD5       string
	ModTime   time.Time
	Status    string
	Attempts  int
	ErrorCode string
}

type FolderNode struct {
	Path     string
	Status   string
	Children []string
}

// Tree is a scanned storage; paths are relative to the storage root.
type Tree struct {
	Files   map[string]*FileNode
	Folders map[string]*FolderNode
}

func newTree() Tree {
	return Tree{Files: map[string]*FileNode{}, Folders: map[string]*FolderNode{}}
}

// underFailedFolder reports whether p is inside a folder whose listing failed.
func (t *Tree) underFailedFolder(p string) bool {
	for folderPath, folder := range t.Folders {
		if folder.Status == statusError && len(p) >= len(folderPath) && p[:len(folderPath)] == folderPath {
			return true
		}
	}
	return false
}

// State lives in memory only.
type State struct {
	Src Tree
	Dst Tree
}

func (s *State) tree(source bool) *Tree {
	if source {
		return &s.Src
	}
	return &s.Dst
}

// StateStore guards State with a mutex; callbacks must not call back into the store.
type StateStore struct {
	mu    sync.Mutex
	state State
}

func NewStateStore() *StateStore {
	return &StateStore{state: State{Src: newTree(), Dst: newTree()}}
}

func (s *StateStore) With(fn func(*State)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	fn(&s.state)
}
