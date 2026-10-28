package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	loadDotEnv(".env")

	cfg, err := readConfig()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Не удалось запустить приложение: %v\n", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, cfg); err != nil {
		fmt.Fprintf(os.Stderr, "Не удалось запустить приложение: %v\n", err)
		os.Exit(1)
	}
}
