package src

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestDiskStoragePutRemoveAndGuards(t *testing.T) {
	root := testDir(t)
	st := newDiskStorage(context.Background(), root)

	if err := st.Put("/a/b.txt", strings.NewReader("one"), 3, md5Hex("one")); err != nil {
		t.Fatal(err)
	}
	assertFile(t, filepath.Join(root, "a", "b.txt"), "one")

	// a failed verification keeps the original and leaves no tmp file
	if err := st.Put("/a/b.txt", strings.NewReader("two"), 3, md5Hex("other")); err == nil {
		t.Fatal("ожидалась ошибка MD5")
	}
	assertFile(t, filepath.Join(root, "a", "b.txt"), "one")
	if _, err := os.Stat(filepath.Join(root, "a", "b.txt"+tempSuffix)); err == nil {
		t.Error("остался tmp-файл")
	}

	if err := st.Copy("/a/b.txt", "/c/d.txt"); err != nil {
		t.Fatal(err)
	}
	assertFile(t, filepath.Join(root, "c", "d.txt"), "one")

	for _, bad := range []string{"/../x", "/.trash/x", "/a/../../x", ""} {
		if err := st.Put(bad, strings.NewReader("x"), 1, ""); err == nil {
			t.Errorf("Put(%q) должен вернуть ошибку", bad)
		}
	}

	removed, err := st.Remove([]string{"/a/b.txt", "/missing.txt"})
	if err != nil || len(removed) != 1 || removed[0] != "/a/b.txt" {
		t.Fatalf("Remove: %v, %v", removed, err)
	}
	assertFile(t, filepath.Join(root, trashDirName, "a", "b.txt"), "one")
	if _, err := os.Stat(filepath.Join(root, "a", "b.txt")); err == nil {
		t.Error("файл должен быть перемещён")
	}
	index, _ := os.ReadFile(filepath.Join(root, trashDirName, trashIndexName))
	state, _ := os.ReadFile(filepath.Join(root, trashDirName, trashStateName))
	if !strings.Contains(string(index), "/a/b.txt") || !strings.Contains(string(state), "/a/b.txt") {
		t.Errorf("TRASH.md: %q, .state.json: %q", index, state)
	}

	entries, err := st.ReadDir("/")
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Path, "/"+trashDirName) {
			t.Errorf("корзина видна в ReadDir: %s", entry.Path)
		}
	}

	if err := st.Trash().PurgeTrash(time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, trashDirName, "a", "b.txt")); err == nil {
		t.Error("файл должен быть удалён из корзины")
	}
	index, _ = os.ReadFile(filepath.Join(root, trashDirName, trashIndexName))
	if strings.Contains(string(index), "/a/b.txt") {
		t.Errorf("строка должна быть убрана из TRASH.md: %q", index)
	}
}

func TestPurgeTrashUsesStateDeletionDate(t *testing.T) {
	root := testDir(t)
	trashRoot := filepath.Join(root, trashDirName)
	if err := os.MkdirAll(trashRoot, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(trashRoot, "file.txt"), []byte("old"), 0o644); err != nil {
		t.Fatal(err)
	}

	deletedAt := time.Now().AddDate(-1, 0, 0)
	state, err := json.Marshal(trashStateFile{Files: map[string]trashStateEntry{
		"/file.txt": {DeletedAt: deletedAt},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(trashRoot, trashStateName), state, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := writeTrashIndex(filepath.Join(trashRoot, trashIndexName), []trashEntry{{
		Path: "/file.txt", Size: 3, DeletedAt: time.Now(),
	}}); err != nil {
		t.Fatal(err)
	}

	st := newDiskStorage(context.Background(), root)
	if err := st.Trash().PurgeTrash(time.Now().AddDate(0, 0, -30)); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(trashRoot, "file.txt")); !os.IsNotExist(err) {
		t.Errorf("файл со старой датой в JSON должен быть удалён, stat error: %v", err)
	}
}
