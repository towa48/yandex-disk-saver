package src

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
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
	dir := t.TempDir()

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
		if agent != userAgent {
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
	dir := t.TempDir()
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
	dir := t.TempDir()
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
