package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"yandex_disk_saver/src"
)

func main() {
	src.LoadDotEnv(".env")

	cfg, err := src.ReadConfig()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Не удалось запустить приложение: %v\n", err)
		os.Exit(1)
	}

	src.ApplyLimits(cfg)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := src.Run(ctx, cfg); err != nil {
		fmt.Fprintf(os.Stderr, "Не удалось запустить приложение: %v\n", err)
		os.Exit(1)
	}
}
