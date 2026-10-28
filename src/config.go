package src

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const (
	directoryListDelay      = time.Second
	directoryRequestTimeout = 10 * time.Second
	reportInterval          = 30 * time.Second
	maxFileRetries          = 3
	fileDownloadDelay       = 250 * time.Millisecond
	defaultDownloadTimeout  = 60 * time.Second
	userAgent               = "Yandex.Disk/3.2.51 (Windows; 10.0)"
)

const (
	statusPending     = "pending"
	statusDownloading = "downloading"
	statusCompleted   = "completed"
	statusError       = "error"
)

type Config struct {
	URL            string
	Username       string
	Password       string
	RemotePath     string
	DestinationDir string
	StatePath      string
	MaxCPUs        int
	MemoryLimitMB  int
}

// LoadDotEnv does not override variables that are already set.
func LoadDotEnv(path string) {
	file, err := os.Open(path)
	if err != nil {
		return
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, value, found := strings.Cut(line, "=")
		if !found {
			continue
		}
		key = strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(key), "export "))
		value = strings.TrimSpace(value)
		if len(value) >= 2 && (value[0] == '"' || value[0] == '\'') && value[len(value)-1] == value[0] {
			value = value[1 : len(value)-1]
		}
		if _, exists := os.LookupEnv(key); !exists {
			os.Setenv(key, value)
		}
	}
	if err := scanner.Err(); err != nil {
		logMessage(fmt.Sprintf("Не удалось прочитать %s: %v", path, err), "error")
	}
}

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func envInt(key string, fallback int) (int, error) {
	value, err := strconv.Atoi(envOr(key, strconv.Itoa(fallback)))
	if err != nil || value < 0 {
		return 0, fmt.Errorf("%s должен быть целым числом не меньше 0", key)
	}
	return value, nil
}

func ReadConfig() (Config, error) {
	protocol := strings.TrimSuffix(envOr("WEBDAV_PROTOCOL", "http"), ":")
	host := envOr("WEBDAV_HOST", "localhost")
	port, err := strconv.Atoi(envOr("WEBDAV_PORT", "80"))

	if protocol != "http" && protocol != "https" {
		return Config{}, errors.New("WEBDAV_PROTOCOL должен быть http или https")
	}
	if err != nil || port < 1 || port > 65535 {
		return Config{}, errors.New("WEBDAV_PORT должен быть целым числом от 1 до 65535")
	}

	var segments []string
	for _, segment := range strings.Split(os.Getenv("WEBDAV_REMOTE_PATH"), "/") {
		if segment != "" {
			segments = append(segments, segment)
		}
	}
	remotePath := "/"
	if len(segments) > 0 {
		remotePath = "/" + strings.Join(segments, "/") + "/"
	}

	destinationDir, err := filepath.Abs(envOr("DESTINATION_DIR", "./downloads"))
	if err != nil {
		return Config{}, err
	}
	statePath, err := filepath.Abs(envOr("STATE_FILE", "./state.json"))
	if err != nil {
		return Config{}, err
	}

	maxCPUs, err := envInt("MAX_CPUS", 2)
	if err != nil {
		return Config{}, err
	}
	memoryLimitMB, err := envInt("MEMORY_LIMIT_MB", 256)
	if err != nil {
		return Config{}, err
	}

	return Config{
		URL:            fmt.Sprintf("%s://%s:%d", protocol, host, port),
		Username:       envOr("WEBDAV_USERNAME", "user"),
		Password:       envOr("WEBDAV_PASSWORD", "password"),
		RemotePath:     remotePath,
		DestinationDir: destinationDir,
		StatePath:      statePath,
		MaxCPUs:        maxCPUs,
		MemoryLimitMB:  memoryLimitMB,
	}, nil
}
