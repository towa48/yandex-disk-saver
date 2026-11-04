package src

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func md5Hex(contents string) string {
	sum := md5.Sum([]byte(contents))
	return hex.EncodeToString(sum[:])
}

func davResponse(href, properties string) string {
	href = (&url.URL{Path: href}).EscapedPath()
	return fmt.Sprintf(`<d:response><d:href>%s</d:href><d:propstat><d:prop>%s</d:prop><d:status>HTTP/1.1 200 OK</d:status></d:propstat></d:response>`, href, properties)
}

func multistatusBody(responses ...string) string {
	return `<?xml version="1.0"?><d:multistatus xmlns:d="DAV:">` + strings.Join(responses, "") + `</d:multistatus>`
}

func writeMultistatus(w http.ResponseWriter, body string) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusMultiStatus)
	fmt.Fprint(w, body)
}

func testConfig(serverURL, dir string) Config {
	return Config{
		URL:                serverURL,
		Username:           "user",
		Password:           "password",
		UserAgent:          defaultUserAgent,
		RemotePath:         "/",
		DestinationDir:     filepath.Join(dir, "downloads"),
		MaxFileRetries:     3,
		DirectoryRetries:   5,
		ReplaceWarnPercent: 10,
		DeleteWarnPercent:  10,
		TrashRetentionDays: 180,
	}
}

func startApp(cfg Config) (cancel context.CancelFunc, done chan struct{}) {
	ctx, cancel := context.WithCancel(context.Background())
	done = make(chan struct{})
	go func() {
		defer close(done)
		Run(ctx, cfg)
	}()
	return cancel, done
}

func TestDownloadsRetriesAndStopsPromptly(t *testing.T) {
	dir := testDir(t)

	var mu sync.Mutex
	var failedRequests []time.Time
	var userAgents []string
	corruptRequests := 0

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "PROPFIND" {
			if r.URL.Path == "/" {
				writeMultistatus(w, multistatusBody(
					davResponse("/", "<d:resourcetype><d:collection/></d:resourcetype>"),
					davResponse("/folder/", "<d:resourcetype><d:collection/></d:resourcetype>"),
				))
				return
			}
			writeMultistatus(w, multistatusBody(
				davResponse("/folder/ok.txt", fmt.Sprintf("<d:resourcetype/><d:getcontentlength>2</d:getcontentlength><d:getetag>%s</d:getetag>", md5Hex("ok"))),
				davResponse("/folder/fail.txt", "<d:resourcetype/><d:getcontentlength>4</d:getcontentlength><d:getetag>fail-v1</d:getetag>"),
				davResponse("/folder/corrupt.txt", fmt.Sprintf("<d:resourcetype/><d:getcontentlength>4</d:getcontentlength><d:getetag>%s</d:getetag>", md5Hex("good"))),
			))
			return
		}

		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		mu.Lock()
		userAgents = append(userAgents, r.Header.Get("User-Agent"))
		switch {
		case strings.HasSuffix(r.URL.Path, "/corrupt.txt"):
			corruptRequests++
			contents := "good"
			if corruptRequests == 1 {
				contents = "bad!"
			}
			mu.Unlock()
			fmt.Fprint(w, contents)
		case strings.HasSuffix(r.URL.Path, "/fail.txt"):
			failedRequests = append(failedRequests, time.Now())
			mu.Unlock()
			http.Error(w, "intentional failure", http.StatusInternalServerError)
		default:
			mu.Unlock()
			fmt.Fprint(w, "ok")
		}
	}))
	defer server.Close()

	cfg := testConfig(server.URL, dir)
	cancel, done := startApp(cfg)
	defer cancel()

	snapshot := func() (int, int) {
		mu.Lock()
		defer mu.Unlock()
		return len(failedRequests), corruptRequests
	}

	deadline := time.Now().Add(18 * time.Second)
	for time.Now().Before(deadline) {
		failed, corrupt := snapshot()
		_, errFile := os.Stat(filepath.Join(cfg.DestinationDir, downloadErrorName))
		if failed >= 4 && corrupt >= 2 && errFile == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	failed, corrupt := snapshot()
	if failed < 4 {
		t.Fatalf("повторная попытка не выполнена: %d запросов", failed)
	}
	if corrupt < 2 {
		t.Fatalf("повторная загрузка после ошибки MD5 не выполнена: %d запросов", corrupt)
	}

	mu.Lock()
	for _, agent := range userAgents {
		if agent != defaultUserAgent {
			t.Errorf("неожиданный User-Agent: %q", agent)
		}
	}
	if gap := failedRequests[2].Sub(failedRequests[1]); gap < 200*time.Millisecond {
		t.Errorf("повтор из очереди должен учитывать задержку перед загрузкой: %v", gap)
	}
	mu.Unlock()

	assertFile(t, filepath.Join(cfg.DestinationDir, "folder", "ok.txt"), "ok")
	assertFile(t, filepath.Join(cfg.DestinationDir, "folder", "corrupt.txt"), "good")

	startedAt := time.Now()
	cancel()
	select {
	case <-done:
	case <-time.After(4 * time.Second):
		t.Fatal("остановка приложения заняла больше 4 секунд")
	}
	if time.Since(startedAt) >= 4*time.Second {
		t.Fatal("остановка приложения заняла больше 4 секунд")
	}

	downloadErrors, err := os.ReadFile(filepath.Join(cfg.DestinationDir, downloadErrorName))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(downloadErrors), "/folder/fail.txt") || !strings.Contains(string(downloadErrors), "500") {
		t.Errorf("DOWNLOAD_ERROR.md: %s", downloadErrors)
	}
	if strings.Contains(string(downloadErrors), "ok.txt") || strings.Contains(string(downloadErrors), "corrupt.txt") {
		t.Errorf("DOWNLOAD_ERROR.md содержит лишние файлы: %s", downloadErrors)
	}
	if _, err := os.Stat(filepath.Join(cfg.DestinationDir, "folder", "fail.txt.tmp")); err == nil {
		t.Error("остался tmp-файл")
	}

	report, err := os.ReadFile(filepath.Join(cfg.DestinationDir, "download-report-"+time.Now().Format("20060102")+".html"))
	if err != nil {
		t.Fatal(err)
	}
	for _, expected := range []string{"Папка (до 2 уровней)", "Общий размер"} {
		if !strings.Contains(string(report), expected) {
			t.Errorf("в отчёте нет %q", expected)
		}
	}
}

func TestShutdownWaitsForActiveDownload(t *testing.T) {
	dir := testDir(t)
	const contents = "done"

	started := make(chan struct{})
	release := make(chan struct{})
	var startedOnce sync.Once

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "PROPFIND" {
			writeMultistatus(w, multistatusBody(davResponse("/slow.txt",
				fmt.Sprintf("<d:resourcetype/><d:getcontentlength>%d</d:getcontentlength><d:getetag>%s</d:getetag>", len(contents), md5Hex(contents)))))
			return
		}

		if r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/slow.txt") {
			w.Header().Set("Content-Length", fmt.Sprint(len(contents)))
			w.WriteHeader(http.StatusOK)
			w.(http.Flusher).Flush()
			startedOnce.Do(func() { close(started) })
			<-release
			fmt.Fprint(w, contents)
			return
		}

		w.WriteHeader(http.StatusMethodNotAllowed)
	}))
	defer server.Close()
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()

	cfg := testConfig(server.URL, dir)
	cancel, done := startApp(cfg)
	defer cancel()

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("загрузка не началась")
	}

	cancel()
	select {
	case <-done:
		t.Fatal("приложение должно ждать завершения активной загрузки")
	case <-time.After(100 * time.Millisecond):
	}

	close(release)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("ожидание загрузки при остановке превысило 5 секунд")
	}

	assertFile(t, filepath.Join(cfg.DestinationDir, "slow.txt"), contents)
}

func assertFile(t *testing.T, path, expected string) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Errorf("не удалось прочитать %s: %v", path, err)
		return
	}
	if string(data) != expected {
		t.Errorf("%s: %q, ожидалось %q", path, data, expected)
	}
}

func TestSyncCopiesReplacesDownloadsAndTrashes(t *testing.T) {
	dir := testDir(t)
	files := map[string]string{"/a.txt": "A", "/dup.txt": "A", "/new.txt": "new", "/change.txt": "fresh"}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "PROPFIND" {
			var responses []string
			for name, contents := range files {
				responses = append(responses, davResponse(name, fmt.Sprintf("<d:resourcetype/><d:getcontentlength>%d</d:getcontentlength><d:getetag>%s</d:getetag>", len(contents), md5Hex(contents))))
			}
			writeMultistatus(w, multistatusBody(responses...))
			return
		}
		if contents, ok := files[r.URL.Path]; ok && r.Method == http.MethodGet {
			if r.URL.Path == "/dup.txt" || r.URL.Path == "/a.txt" {
				t.Errorf("файл %s не должен скачиваться", r.URL.Path)
			}
			fmt.Fprint(w, contents)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	cfg := testConfig(server.URL, dir)
	cfg.ReplaceWarnPercent = 100
	cfg.DeleteWarnPercent = 100
	if err := os.MkdirAll(cfg.DestinationDir, 0o755); err != nil {
		t.Fatal(err)
	}
	for name, contents := range map[string]string{"a.txt": "A", "change.txt": "old", "stale.txt": "zzz"} {
		if err := os.WriteFile(filepath.Join(cfg.DestinationDir, name), []byte(contents), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	cancel, done := startApp(cfg)
	defer cancel()

	index := filepath.Join(cfg.DestinationDir, trashDirName, trashIndexName)
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(index); err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	cancel()
	<-done

	assertFile(t, filepath.Join(cfg.DestinationDir, "a.txt"), "A")
	assertFile(t, filepath.Join(cfg.DestinationDir, "dup.txt"), "A")
	assertFile(t, filepath.Join(cfg.DestinationDir, "new.txt"), "new")
	assertFile(t, filepath.Join(cfg.DestinationDir, "change.txt"), "fresh")
	assertFile(t, filepath.Join(cfg.DestinationDir, trashDirName, "stale.txt"), "zzz")
	if _, err := os.Stat(filepath.Join(cfg.DestinationDir, "stale.txt")); err == nil {
		t.Error("stale.txt должен быть перемещён в корзину")
	}
	data, err := os.ReadFile(index)
	if err != nil || !strings.Contains(string(data), "/stale.txt") {
		t.Errorf("TRASH.md: %q, %v", data, err)
	}
}

type fakeDAV struct {
	mu      sync.Mutex
	files   map[string]string
	dirs    map[string]bool
	puts    []string
	deletes []string
	copies  int
}

func (d *fakeDAV) entry(p string) string {
	if d.dirs[p] {
		return davResponse(p, "<d:resourcetype><d:collection/></d:resourcetype>")
	}
	c := d.files[p]
	return davResponse(p, fmt.Sprintf("<d:resourcetype/><d:getcontentlength>%d</d:getcontentlength><d:getetag>%s</d:getetag>", len(c), md5Hex(c)))
}

func (d *fakeDAV) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	d.mu.Lock()
	defer d.mu.Unlock()
	p := r.URL.Path
	destination := func() string {
		u, _ := url.Parse(r.Header.Get("Destination"))
		return u.Path
	}

	switch r.Method {
	case "PROPFIND":
		if c, ok := d.files[p]; ok {
			_ = c
			writeMultistatus(w, multistatusBody(d.entry(p)))
			return
		}
		if !d.dirs[p] {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		responses := []string{d.entry(p)}
		if r.Header.Get("Depth") == "1" {
			for child := range d.files {
				if path.Dir(child)+"/" == p || (p == "/" && path.Dir(child) == "/") {
					responses = append(responses, d.entry(child))
				}
			}
			for child := range d.dirs {
				if child != p && path.Dir(strings.TrimSuffix(child, "/"))+"/" == p || (p == "/" && child != "/" && path.Dir(strings.TrimSuffix(child, "/")) == "/") {
					responses = append(responses, d.entry(child))
				}
			}
		}
		writeMultistatus(w, multistatusBody(responses...))
	case http.MethodGet:
		c, ok := d.files[p]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		fmt.Fprint(w, c)
	case http.MethodPut:
		body, _ := io.ReadAll(r.Body)
		d.files[p] = string(body)
		d.puts = append(d.puts, p)
		w.WriteHeader(http.StatusCreated)
	case "MKCOL":
		if d.dirs[p] {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		d.dirs[p] = true
		w.WriteHeader(http.StatusCreated)
	case "MOVE", "COPY":
		c, ok := d.files[p]
		to := destination()
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if _, exists := d.files[to]; exists && r.Header.Get("Overwrite") == "F" {
			w.WriteHeader(http.StatusPreconditionFailed)
			return
		}
		d.files[to] = c
		if r.Method == "MOVE" {
			delete(d.files, p)
		} else {
			d.copies++
		}
		w.WriteHeader(http.StatusCreated)
	case http.MethodDelete:
		d.deletes = append(d.deletes, p)
		delete(d.files, p)
		w.WriteHeader(http.StatusNoContent)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func TestReverseSyncUploadsCopiesReplacesAndTrashes(t *testing.T) {
	dir := testDir(t)
	dav := &fakeDAV{
		files: map[string]string{"/keep.txt": "same", "/change.txt": "old", "/gone.txt": "bye"},
		dirs:  map[string]bool{"/": true},
	}
	server := httptest.NewServer(dav)
	defer server.Close()

	cfg := testConfig(server.URL, dir)
	cfg.SyncMode = modeFSToWebDAV
	cfg.ReplaceWarnPercent = 100
	cfg.DeleteWarnPercent = 100
	for name, contents := range map[string]string{"keep.txt": "same", "change.txt": "newer!", "add.txt": "added", "dup.txt": "same", "sub/n.txt": "nested"} {
		target := filepath.Join(cfg.DestinationDir, name)
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(target, []byte(contents), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	cancel, done := startApp(cfg)
	defer cancel()
	select {
	case <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("приложение не завершилось само")
	}

	dav.mu.Lock()
	defer dav.mu.Unlock()
	expected := map[string]string{
		"/keep.txt": "same", "/change.txt": "newer!", "/add.txt": "added", "/dup.txt": "same",
		"/sub/n.txt": "nested", "/.trash/gone.txt": "bye",
	}
	for p, contents := range expected {
		if dav.files[p] != contents {
			t.Errorf("%s: %q, ожидалось %q", p, dav.files[p], contents)
		}
	}
	if _, exists := dav.files["/gone.txt"]; exists {
		t.Error("gone.txt должен быть перемещён в корзину")
	}
	if !strings.Contains(dav.files["/.trash/TRASH.md"], "/gone.txt") {
		t.Errorf("TRASH.md: %q", dav.files["/.trash/TRASH.md"])
	}
	if dav.copies != 1 {
		t.Errorf("COPY: %d, ожидалось 1", dav.copies)
	}
	for _, put := range dav.puts {
		if strings.Contains(put, "dup.txt") {
			t.Errorf("dup.txt не должен загружаться: %s", put)
		}
	}
	if len(dav.deletes) != 0 {
		t.Errorf("не ожидались DELETE: %v", dav.deletes)
	}
	for p := range dav.files {
		if strings.HasSuffix(p, ".tmp") {
			t.Errorf("остался tmp-файл: %s", p)
		}
	}
}

func TestWebDAVStorageNormalizesPaths(t *testing.T) {
	dir := testDir(t)
	dav := &fakeDAV{
		files: map[string]string{"/dir.name. /file.name.txt. ": "contents"},
		dirs:  map[string]bool{"/": true, "/dir.name. /": true},
	}
	server := httptest.NewServer(dav)
	defer server.Close()

	cfg := testConfig(server.URL, dir)
	app := &App{cfg: cfg, ctx: context.Background(), client: NewWebDAVClient(cfg)}
	st := newWebDAVStorage(app)
	st.sleep = func(time.Duration) {}

	directories, err := st.ReadDir("/")
	if err != nil {
		t.Fatal(err)
	}
	if len(directories) != 1 || directories[0].Path != "/dir.name_/" {
		t.Fatalf("normalized directories: %+v", directories)
	}

	files, err := st.ReadDir(directories[0].Path)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 1 || files[0].Path != "/dir.name_/file.name.txt_" {
		t.Fatalf("normalized files: %+v", files)
	}

	file, err := st.Open(files[0].Path, files[0].Size)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	contents, err := io.ReadAll(file)
	if err != nil {
		t.Fatal(err)
	}
	if string(contents) != "contents" {
		t.Errorf("contents: %q, want %q", contents, "contents")
	}
}

// testDir is a temp dir whose cleanup tolerates Windows briefly locking freshly written files.
func testDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "sync-test-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		for i := 0; i < 20; i++ {
			if os.RemoveAll(dir) == nil {
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
	})
	return dir
}
