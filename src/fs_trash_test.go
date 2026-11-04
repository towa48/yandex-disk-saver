package src

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestDiskTrashListDeleteAndState(t *testing.T) {
	dir := testDir(t)
	root := filepath.Join(dir, trashDirName)
	outside := filepath.Join(dir, "outside.txt")
	for p, contents := range map[string]string{
		filepath.Join(root, "a.txt"):        "A",
		filepath.Join(root, "sub", "b.txt"): "BB",
		filepath.Join(root, trashIndexName): "| index |",
		outside:                             "keep",
	} {
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(contents), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	trash := newDiskTrash(root)
	items, err := trash.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 2 {
		t.Fatalf("ожидалось 2 файла, получено %+v", items)
	}
	byPath := map[string]TrashItem{}
	for _, item := range items {
		byPath[item.Path] = item
		if time.Since(item.DeletedAt) > time.Minute {
			t.Errorf("%s: неизвестный файл должен считаться удалённым только что", item.Path)
		}
	}
	if got := byPath["/a.txt"]; got.MD5 != md5Hex("A") || got.Size != 1 {
		t.Errorf("a.txt: %+v", got)
	}
	if got := byPath["/sub/b.txt"]; got.MD5 != md5Hex("BB") || got.Size != 2 {
		t.Errorf("sub/b.txt: %+v", got)
	}

	statePath := filepath.Join(root, trashStateName)
	old := time.Now().AddDate(-1, 0, 0).UTC().Truncate(time.Second)
	data, _ := json.Marshal(trashStateFile{Files: map[string]trashStateEntry{"/a.txt": {DeletedAt: old}, "/gone.txt": {DeletedAt: old}}})
	if err := os.WriteFile(statePath, data, 0o644); err != nil {
		t.Fatal(err)
	}
	items, err = trash.List()
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range items {
		if item.Path == "/a.txt" && !item.DeletedAt.Equal(old) {
			t.Errorf("дата из state не использована: %v", item.DeletedAt)
		}
	}
	var saved trashStateFile
	raw, _ := os.ReadFile(statePath)
	if err := json.Unmarshal(raw, &saved); err != nil {
		t.Fatal(err)
	}
	if _, exists := saved.Files["/gone.txt"]; exists {
		t.Error("запись об отсутствующем файле должна быть удалена из state")
	}
	if _, exists := saved.Files["/sub/b.txt"]; !exists {
		t.Error("новый файл должен быть добавлен в state")
	}

	for _, bad := range []string{"/../outside.txt", "../outside.txt", "/sub/../../outside.txt", "/" + trashIndexName, "/" + trashStateName, "", "/"} {
		if err := trash.Delete(bad); err == nil {
			t.Errorf("Delete(%q) должен вернуть ошибку", bad)
		}
	}
	if _, err := os.Stat(outside); err != nil {
		t.Fatal("файл вне корзины затронут")
	}

	if err := trash.Delete("/sub/b.txt"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "sub")); err == nil {
		t.Error("пустая папка должна быть удалена")
	}
	if _, err := os.Stat(filepath.Join(root, "a.txt")); err != nil {
		t.Error("a.txt не должен быть удалён")
	}
	raw, _ = os.ReadFile(statePath)
	saved = trashStateFile{}
	json.Unmarshal(raw, &saved)
	if _, exists := saved.Files["/sub/b.txt"]; exists {
		t.Error("запись об удалённом файле должна быть убрана из state")
	}
}
