package src

import (
	"context"
	"fmt"
	"os"
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
	workers    sync.WaitGroup
	stop       context.CancelFunc
	reportMu   sync.Mutex
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

func (a *App) fileState(remotePath string) (status string, attempts int) {
	a.store.With(func(s *State) {
		if file := s.Files[remotePath]; file != nil {
			status, attempts = file.Status, file.Attempts
		}
	})
	return status, attempts
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

		if status, _ := a.fileState(remotePath); status != statusPending {
			continue
		}

		a.sleep(fileDownloadDelay)
		if a.attemptDownload(remotePath) {
			continue
		}
		if _, attempts := a.fileState(remotePath); attempts < a.cfg.MaxFileRetries {
			a.errorQueue.Push(remotePath)
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

		logMessage(fmt.Sprintf("%sRETRY:%s Processing file: %s. Error queue length: %d.",
			colorOrange, colorReset, remotePath, a.errorQueue.Len()), "info")

		for a.ctx.Err() == nil {
			status, attempts := a.fileState(remotePath)
			if status != statusError || attempts >= a.cfg.MaxFileRetries {
				break
			}
			a.sleep(fileDownloadDelay)
			if a.attemptDownload(remotePath) {
				break
			}
		}
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

// Run works until ctx is cancelled and waits for active downloads before returning.
func Run(ctx context.Context, cfg Config) error {
	if err := os.MkdirAll(cfg.DestinationDir, 0o755); err != nil {
		return err
	}

	ctx, stop := context.WithCancel(ctx)
	defer stop()

	app := &App{
		cfg:        cfg,
		ctx:        ctx,
		stop:       stop,
		store:      NewStateStore(),
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
	startWorker := func(fn func()) {
		app.workers.Add(1)
		start(func() {
			defer app.workers.Done()
			fn()
		})
	}

	startWorker(app.runMainQueue)
	startWorker(app.runErrorQueue)

	start(app.runPipeline)
	start(func() { app.runPeriodic(app.writeReport, reportInterval, "Ошибка создания отчёта") })

	logMessage(fmt.Sprintf("Запущено. Сервер: %s; папка назначения: %s", cfg.URL, cfg.DestinationDir), "info")

	<-ctx.Done()
	app.mainQueue.Close()
	app.errorQueue.Close()

	tasks.Wait()
	if err := app.writeReport(); err != nil {
		logMessage(fmt.Sprintf("Ошибка создания отчёта: %v", err), "error")
	}
	return nil
}
