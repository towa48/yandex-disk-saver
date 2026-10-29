package src

import (
	"context"
	"fmt"
	"os"
	"sort"
	"sync"
	"time"
)

type App struct {
	cfg        Config
	ctx        context.Context
	store      *StateStore
	client     *WebDAVClient
	mainQueue  *Queue[string]
	errorQueue *Queue[string]
}

// sleep returns early once the app context is cancelled.
func (a *App) sleep(duration time.Duration) {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-a.ctx.Done():
	}
}

func (a *App) loadState() {
	logMessage("Загружаем состояние...", "trace")
	a.store.Load()

	logMessage("Формируем очередь загрузки...", "trace")
	a.store.With(func(s *State) {
		paths := make([]string, 0, len(s.Files))
		for remotePath := range s.Files {
			paths = append(paths, remotePath)
		}
		sort.Strings(paths)

		for _, remotePath := range paths {
			file := s.Files[remotePath]
			if file.Status == statusDownloading {
				file.Status = statusPending
			}
			if file.Status == statusPending {
				a.mainQueue.Push(remotePath)
			} else if file.Status == statusError {
				a.errorQueue.Push(remotePath)
			}
		}
	})

	logMessage(fmt.Sprintf("Состояние загружено. %d файлов в основной очереди и %d в очереди ошибок.",
		a.mainQueue.Len(), a.errorQueue.Len()), "info")
}

func (a *App) runMainQueue() {
	for {
		remotePath, ok := a.mainQueue.Pop()
		if !ok {
			break
		}
		if a.ctx.Err() != nil {
			continue
		}

		logMessage(fmt.Sprintf("%sMAIN:%s Processing file: %s. Main queue length: %d.",
			colorOrange, colorReset, remotePath, a.mainQueue.Len()), "info")

		var status string
		var size *int64
		a.store.With(func(s *State) {
			if file := s.Files[remotePath]; file != nil {
				status, size = file.Status, file.Size
			}
		})

		if status == statusPending {
			a.sleep(fileDownloadDelay)
			a.downloadFile(remotePath, calculateDownloadTimeout(size))
		}
	}

	logMessage(fmt.Sprintf("%sMAIN:%s Worker stopped. Main queue length: %d.",
		colorOrange, colorReset, a.mainQueue.Len()), "info")
	a.errorQueue.Close()
}

func (a *App) runErrorQueue() {
	for {
		remotePath, ok := a.errorQueue.Pop()
		if !ok {
			break
		}

		var status string
		var attempts int
		var size *int64
		a.store.With(func(s *State) {
			if file := s.Files[remotePath]; file != nil {
				status, attempts, size = file.Status, file.Attempts, file.Size
			}
		})
		if a.ctx.Err() != nil || status != statusError || attempts >= a.cfg.MaxFileRetries {
			continue
		}

		a.sleep(fileDownloadDelay)

		logMessage(fmt.Sprintf("%sRETRY:%s Processing file: %s. Error queue length: %d.",
			colorOrange, colorReset, remotePath, a.errorQueue.Len()), "info")
		a.downloadFile(remotePath, calculateDownloadTimeout(size))
	}

	logMessage(fmt.Sprintf("%sRETRY:%s Worker stopped. Error queue length: %d.",
		colorOrange, colorReset, a.errorQueue.Len()), "info")
}

func (a *App) runPeriodic(task func() error, interval time.Duration, label string) {
	for a.ctx.Err() == nil {
		if err := task(); err != nil {
			logMessage(fmt.Sprintf("%s: %v", label, err), "error")
		}
		a.sleep(interval)
	}
}

// Run works until ctx is cancelled, then waits for active downloads and saves the state.
func Run(ctx context.Context, cfg Config) error {
	if err := os.MkdirAll(cfg.DestinationDir, 0o755); err != nil {
		return err
	}

	app := &App{
		cfg:        cfg,
		ctx:        ctx,
		store:      NewStateStore(cfg.StatePath),
		client:     NewWebDAVClient(cfg),
		mainQueue:  NewQueue[string](),
		errorQueue: NewQueue[string](),
	}

	var tasks sync.WaitGroup
	start := func(fn func()) {
		tasks.Add(1)
		go func() {
			defer tasks.Done()
			fn()
		}()
	}

	start(app.runMainQueue)
	start(app.runErrorQueue)

	app.loadState()

	start(app.discoverRemoteFiles)
	start(func() { app.runPeriodic(app.writeReport, reportInterval, "Ошибка создания отчёта") })

	logMessage(fmt.Sprintf("Запущено. Сервер: %s; папка назначения: %s", cfg.URL, cfg.DestinationDir), "info")

	<-ctx.Done()
	app.mainQueue.Close()
	app.errorQueue.Close()

	tasks.Wait()
	logMessage("Остановка: сохраняю состояние...", "info")
	app.store.Save()
	return nil
}
