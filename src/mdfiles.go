package src

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func mdCell(value string) string {
	return strings.ReplaceAll(strings.ReplaceAll(value, `\`, `\\`), "|", `\|`)
}

// splitMDRow splits a table row on unescaped pipes and unescapes the cells.
func splitMDRow(line string) []string {
	line = strings.TrimSpace(line)
	line = strings.TrimPrefix(line, "|")
	line = strings.TrimSuffix(line, "|")

	var cells []string
	var current strings.Builder
	runes := []rune(line)
	for i := 0; i < len(runes); i++ {
		switch {
		case runes[i] == '\\' && i+1 < len(runes):
			i++
			current.WriteRune(runes[i])
		case runes[i] == '|':
			cells = append(cells, strings.TrimSpace(current.String()))
			current.Reset()
		default:
			current.WriteRune(runes[i])
		}
	}
	return append(cells, strings.TrimSpace(current.String()))
}

func mdRow(cells ...string) string {
	escaped := make([]string, len(cells))
	for i, cell := range cells {
		escaped[i] = mdCell(cell)
	}
	return "| " + strings.Join(escaped, " | ") + " |\n"
}

func mdMegabytes(bytes int64) string {
	return fmt.Sprintf("%.2f", float64(bytes)/(1024*1024))
}

// appendRows creates the file with header if missing, then appends rows; with dedupe, paths already listed are skipped.
func (a *App) appendRows(name, header string, rows [][]string, dedupe bool) error {
	a.reportMu.Lock()
	defer a.reportMu.Unlock()

	target := filepath.Join(a.cfg.DestinationDir, name)
	existing, err := os.ReadFile(target)
	isNew := err != nil

	var content strings.Builder
	if isNew {
		content.WriteString(header)
	}
	for _, cells := range rows {
		line := mdRow(cells...)
		if dedupe && !isNew && strings.Contains(string(existing), "\n| "+mdCell(cells[0])+" |") {
			continue
		}
		content.WriteString(line)
	}
	if content.Len() == 0 {
		return nil
	}

	file, err := os.OpenFile(target, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer file.Close()
	_, err = file.WriteString(content.String())
	return err
}

// appendDownloadError adds a row to DOWNLOAD_ERROR.md, creating the file if missing.
func (a *App) appendDownloadError(remotePath string, size *int64, code string) {
	var bytes int64
	if size != nil {
		bytes = *size
	}
	header := "# Ошибки скачивания\n\nСписок файлов, которые не удалось скачать с WebDAV-сервера.\n\n" +
		"| Файл | Дата | Размер, МБ | Код ошибки |\n| --- | --- | --- | --- |\n"
	row := []string{remotePath, time.Now().Format("2006-01-02"), mdMegabytes(bytes), code}
	if err := a.appendRows(downloadErrorName, header, [][]string{row}, false); err != nil {
		logMessage(fmt.Sprintf("Не удалось записать %s: %v", downloadErrorName, err), "error")
	}
}
