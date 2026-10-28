package main

import (
	"fmt"
	"time"
)

const (
	colorYellow = "\x1b[33m"
	colorReset  = "\x1b[0m"
	colorRed    = "\x1b[31m"
	colorOrange = "\x1b[38;5;214m"
	colorGrey   = "\x1b[90m"
)

func logMessage(message, level string) {
	switch level {
	case "error":
		message = colorRed + message + colorReset
	case "warn":
		message = colorOrange + message + colorReset
	case "trace":
		message = colorGrey + message + colorReset
	}
	fmt.Printf("%s[%s]%s %s\n", colorYellow, time.Now().UTC().Format("2006-01-02T15:04:05.000Z"), colorReset, message)
}
