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
	req.Header.Set("User-Agent", c.cfg.UserAgent)
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
	return parseMultistatus(resp.Body, remotePath, true)
}

func (c *WebDAVClient) targetURL(remotePath string) string {
	return strings.TrimRight(c.cfg.URL, "/") + (&url.URL{Path: remotePath}).EscapedPath()
}

// Stat returns properties of a single resource.
func (c *WebDAVClient) Stat(remotePath string, timeout time.Duration) (Entry, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	req, err := c.newRequest(ctx, "PROPFIND", remotePath)
	if err != nil {
		return Entry{}, err
	}
	req.Header.Set("Depth", "0")

	resp, err := c.httpClient().Do(req)
	if err != nil {
		return Entry{}, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusMultiStatus && resp.StatusCode != http.StatusOK {
		return Entry{}, &HTTPError{Status: resp.StatusCode, Method: "PROPFIND", Path: remotePath}
	}
	entries, err := parseMultistatus(resp.Body, remotePath, false)
	if err != nil {
		return Entry{}, err
	}
	if len(entries) == 0 {
		return Entry{}, &HTTPError{Status: http.StatusNotFound, Method: "PROPFIND", Path: remotePath}
	}
	return entries[0], nil
}

// send runs a request without a response body of interest; HTTP status >= 400 becomes an HTTPError.
func (c *WebDAVClient) send(method, remotePath string, timeout time.Duration, headers map[string]string, body io.Reader, size int64) error {
	// Deliberately detached from the app context: shutdown waits for active transfers.
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	req, err := c.newRequest(ctx, method, remotePath)
	if err != nil {
		return err
	}
	if body != nil {
		req.Body = io.NopCloser(body)
		req.ContentLength = size
	}
	for key, value := range headers {
		req.Header.Set(key, value)
	}

	resp, err := c.httpClient().Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)

	if resp.StatusCode >= 400 {
		return &HTTPError{Status: resp.StatusCode, Method: method, Path: remotePath}
	}
	return nil
}

func (c *WebDAVClient) PutFile(remotePath string, body io.Reader, size int64, timeout time.Duration) error {
	return c.send(http.MethodPut, remotePath, timeout, nil, body, size)
}

// Move overwrites the destination if it exists.
func (c *WebDAVClient) Move(source, destination string, timeout time.Duration) error {
	return c.send("MOVE", source, timeout, map[string]string{"Destination": c.targetURL(destination), "Overwrite": "T"}, nil, 0)
}

// Copy fails if the destination exists.
func (c *WebDAVClient) Copy(source, destination string, timeout time.Duration) error {
	return c.send("COPY", source, timeout, map[string]string{"Destination": c.targetURL(destination), "Overwrite": "F"}, nil, 0)
}

func (c *WebDAVClient) MakeCollection(remotePath string, timeout time.Duration) error {
	return c.send("MKCOL", remotePath, timeout, nil, nil, 0)
}

func (c *WebDAVClient) Delete(remotePath string, timeout time.Duration) error {
	return c.send(http.MethodDelete, remotePath, timeout, nil, nil, 0)
}

func (c *WebDAVClient) GetFile(remotePath string, timeout time.Duration, dst io.Writer) error {
	// Deliberately detached from the app context: shutdown waits for active downloads.
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	req, err := c.newRequest(ctx, http.MethodGet, remotePath)
	if err != nil {
		return err
	}

	resp, err := c.httpClient().Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return &HTTPError{Status: resp.StatusCode, Method: http.MethodGet, Path: remotePath}
	}
	_, err = io.Copy(dst, resp.Body)
	return err
}

type cancelBody struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func (b *cancelBody) Close() error {
	err := b.ReadCloser.Close()
	b.cancel()
	return err
}

// OpenFile starts a GET and returns the body; the timeout covers the whole read until Close.
func (c *WebDAVClient) OpenFile(remotePath string, timeout time.Duration) (io.ReadCloser, error) {
	// Deliberately detached from the app context: shutdown waits for active transfers.
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	req, err := c.newRequest(ctx, http.MethodGet, remotePath)
	if err != nil {
		cancel()
		return nil, err
	}
	resp, err := c.httpClient().Do(req)
	if err != nil {
		cancel()
		return nil, err
	}
	if resp.StatusCode >= 400 {
		resp.Body.Close()
		cancel()
		return nil, &HTTPError{Status: resp.StatusCode, Method: http.MethodGet, Path: remotePath}
	}
	return &cancelBody{ReadCloser: resp.Body, cancel: cancel}, nil
}

// CheckAndRecreate probes the server and replaces the HTTP client if the probe fails.
func (c *WebDAVClient) CheckAndRecreate() {
	logMessage("Checking client state", "warn")
	if _, err := c.ListDirectory(c.cfg.RemotePath, directoryRequestTimeout); err != nil {
		logMessage(fmt.Sprintf("ERROR: %v", err), "error")
		logMessage("Client check failed, recreating.", "warn")
		c.recreate()
		return
	}
	logMessage("Client is working", "warn")
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

func parseMultistatus(body io.Reader, requestPath string, skipSelf bool) ([]Entry, error) {
	var parsed multistatus
	if err := xml.NewDecoder(body).Decode(&parsed); err != nil {
		return nil, fmt.Errorf("failed to parse PROPFIND response: %w", err)
	}

	self := strings.TrimSuffix(requestPath, "/")
	var entries []Entry
	for _, response := range parsed.Responses {
		href, err := url.Parse(strings.TrimSpace(response.Href))
		if err != nil || href.Path == "" || (skipSelf && strings.TrimSuffix(href.Path, "/") == self) {
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
