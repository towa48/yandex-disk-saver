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
	stop       context.CancelFunc
	store      *StateStore
	client     *WebDAVClient
	src        Storage
	dst        Storage
	mainQueue  *Queue[string]
	errorQueue *Queue[string]
	workers    sync.WaitGroup
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

func (a *App) fileState(path string) (status string, attempts int) {
	a.store.With(func(s *State) {
		if file := s.Src.Files[path]; file != nil {
			status, attempts = file.Status, file.Attempts
		}
	})
	return status, attempts
}

func (a *App) runMainQueue() {
	for {
		path, ok := a.mainQueue.Pop()
		if !ok {
			break
		}
		if a.ctx.Err() != nil {
			continue
		}

		logMessage(fmt.Sprintf("%sMAIN:%s Processing file: %s. Main queue length: %d.",
			colorOrange, colorReset, path, a.mainQueue.Len()), "info")

		if status, _ := a.fileState(path); status != statusPending {
			continue
		}

		a.sleep(fileDownloadDelay)
		if a.attempt(path) {
			continue
		}
		if _, attempts := a.fileState(path); attempts < a.cfg.MaxFileRetries {
			a.errorQueue.Push(path)
		}
	}

	logMessage(fmt.Sprintf("%sMAIN:%s Worker stopped. Main queue length: %d.",
		colorOrange, colorReset, a.mainQueue.Len()), "info")
	a.errorQueue.Close()
}

func (a *App) runErrorQueue() {
	for {
		path, ok := a.errorQueue.Pop()
		if !ok {
			break
		}

		logMessage(fmt.Sprintf("%sRETRY:%s Processing file: %s. Error queue length: %d.",
			colorOrange, colorReset, path, a.errorQueue.Len()), "info")

		for a.ctx.Err() == nil {
			status, attempts := a.fileState(path)
			if status != statusError || attempts >= a.cfg.MaxFileRetries {
				break
			}
			a.sleep(fileDownloadDelay)
			if a.attempt(path) {
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

// Run works until ctx is cancelled or the sync cycle ends, and waits for active transfers before returning.
func Run(ctx context.Context, cfg Config) error {
	logMessage(fmt.Sprintf("Mode: %s", modeLabel(cfg.SyncMode)), "alert")

	if err := os.MkdirAll(cfg.DestinationDir, 0o755); err != nil {
		return err
	}
	diskRoot, err := safeJoin(cfg.DestinationDir, cfg.RemotePath)
	if err != nil {
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
	webdav := newWebDAVStorage(app)
	disk := newDiskStorage(ctx, diskRoot)
	if cfg.SyncMode == modeFSToWebDAV {
		app.src, app.dst = disk, webdav
	} else {
		app.src, app.dst = webdav, disk
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
	start(func() { app.runPeriodic(app.writeReport, reportInterval, "Report error") })

	logMessage(fmt.Sprintf("Started. Server: %s; destination folder: %s", cfg.URL, cfg.DestinationDir), "info")

	<-ctx.Done()
	app.mainQueue.Close()
	app.errorQueue.Close()

	tasks.Wait()
	if err := app.writeReport(); err != nil {
		logMessage(fmt.Sprintf("Report error: %v", err), "error")
	}
	return nil
}
