package src

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

type HTTPError struct {
	Status int
	Method string
	Path   string
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("%s %s: HTTP %d", e.Method, e.Path, e.Status)
}

func httpStatus(err error) int {
	var httpErr *HTTPError
	if errors.As(err, &httpErr) {
		return httpErr.Status
	}
	return 0
}

func isRateLimitError(err error) bool {
	return httpStatus(err) == http.StatusTooManyRequests
}

type Entry struct {
	Path    string
	IsDir   bool
	Size    *int64
	ETag    string
	LastMod string
}

type WebDAVClient struct {
	cfg  Config
	mu   sync.Mutex
	http *http.Client
}

func newHTTPClient() *http.Client {
	return &http.Client{Transport: http.DefaultTransport.(*http.Transport).Clone()}
}

func NewWebDAVClient(cfg Config) *WebDAVClient {
	return &WebDAVClient{cfg: cfg, http: newHTTPClient()}
}

func (c *WebDAVClient) httpClient() *http.Client {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.http
}

func (c *WebDAVClient) recreate() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.http.CloseIdleConnections()
	c.http = newHTTPClient()
}

func (c *WebDAVClient) newRequest(ctx context.Context, method, remotePath string) (*http.Request, error) {
	target := strings.TrimRight(c.cfg.URL, "/") + (&url.URL{Path: remotePath}).EscapedPath()
	req, err := http.NewRequestWithContext(ctx, method, target, nil)
	if err != nil {
		return nil, err
	}
	req.SetBasicAuth(c.cfg.Username, c.cfg.Password)
	req.Header.Set("User-Agent", userAgent)
	return req, nil
}

func (c *WebDAVClient) ListDirectory(remotePath string, timeout time.Duration) ([]Entry, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	req, err := c.newRequest(ctx, "PROPFIND", remotePath)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Depth", "1")

	resp, err := c.httpClient().Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusMultiStatus && resp.StatusCode != http.StatusOK {
		return nil, &HTTPError{Status: resp.StatusCode, Method: "PROPFIND", Path: remotePath}
	}
	return parseMultistatus(resp.Body, remotePath)
}

func (c *WebDAVClient) GetFile(remotePath string, timeout time.Duration) ([]byte, error) {
	// Deliberately detached from the app context: shutdown waits for active downloads.
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	req, err := c.newRequest(ctx, http.MethodGet, remotePath)
	if err != nil {
		return nil, err
	}

	resp, err := c.httpClient().Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return nil, &HTTPError{Status: resp.StatusCode, Method: http.MethodGet, Path: remotePath}
	}
	return io.ReadAll(resp.Body)
}

// CheckAndRecreate probes the server and replaces the HTTP client if the probe fails.
func (c *WebDAVClient) CheckAndRecreate() {
	logMessage("Проверяю состояние клиента", "warn")
	if _, err := c.ListDirectory(c.cfg.RemotePath, directoryRequestTimeout); err != nil {
		logMessage(fmt.Sprintf("ERROR: %v", err), "error")
		logMessage("Ошибка при проверке клиента, пересоздаю.", "warn")
		c.recreate()
		return
	}
	logMessage("Клиент в рабочем состоянии", "warn")
}

type multistatus struct {
	Responses []struct {
		Href      string `xml:"href"`
		Propstats []struct {
			Status string `xml:"status"`
			Prop   struct {
				ResourceType struct {
					Inner string `xml:",innerxml"`
				} `xml:"resourcetype"`
				ContentLength string `xml:"getcontentlength"`
				ETag          string `xml:"getetag"`
				LastModified  string `xml:"getlastmodified"`
			} `xml:"prop"`
		} `xml:"propstat"`
	} `xml:"response"`
}

func parseMultistatus(body io.Reader, requestPath string) ([]Entry, error) {
	var parsed multistatus
	if err := xml.NewDecoder(body).Decode(&parsed); err != nil {
		return nil, fmt.Errorf("не удалось разобрать ответ PROPFIND: %w", err)
	}

	self := strings.TrimSuffix(requestPath, "/")
	var entries []Entry
	for _, response := range parsed.Responses {
		href, err := url.Parse(strings.TrimSpace(response.Href))
		if err != nil || href.Path == "" || strings.TrimSuffix(href.Path, "/") == self {
			continue
		}

		entry := Entry{Path: href.Path}
		for _, propstat := range response.Propstats {
			if !strings.Contains(propstat.Status, " 200 ") {
				continue
			}
			prop := propstat.Prop
			entry.IsDir = entry.IsDir || strings.Contains(prop.ResourceType.Inner, "collection")
			if size, err := strconv.ParseInt(strings.TrimSpace(prop.ContentLength), 10, 64); err == nil && size >= 0 {
				entry.Size = &size
			}
			if prop.ETag != "" {
				entry.ETag = strings.TrimSpace(prop.ETag)
			}
			if prop.LastModified != "" {
				entry.LastMod = strings.TrimSpace(prop.LastModified)
			}
		}
		entries = append(entries, entry)
	}
	return entries, nil
}
