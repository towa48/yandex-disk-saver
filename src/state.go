package src

import "sync"

type File struct {
	RemotePath string
	Size       *int64
	ETag       string
	LastMod    string
	Status     string
	Attempts   int
	ErrorCode  string
}

type Folder struct {
	Path     string
	Status   string
	Children []string
}

type LocalFile struct {
	Path   string
	Size   int64
	MD5    string
	Status string
}

type LocalFolder struct {
	Path     string
	Status   string
	Children []string
}

// State lives in memory only. Local trees use the same path form as WebDAV ones ("/dir/file", "/dir/").
type State struct {
	Files        map[string]*File
	Folders      map[string]*Folder
	LocalFiles   map[string]*LocalFile
	LocalFolders map[string]*LocalFolder
}

// StateStore guards State with a mutex; callbacks must not call back into the store.
type StateStore struct {
	mu    sync.Mutex
	state State
}

func NewStateStore() *StateStore {
	return &StateStore{state: State{
		Files:        map[string]*File{},
		Folders:      map[string]*Folder{},
		LocalFiles:   map[string]*LocalFile{},
		LocalFolders: map[string]*LocalFolder{},
	}}
}

func (s *StateStore) With(fn func(*State)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	fn(&s.state)
}
