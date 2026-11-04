package src

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const dateFormat = time.DateOnly

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
	title, description := "Download errors", "could not be downloaded from the WebDAV server"
	if a.cfg.SyncMode == modeFSToWebDAV {
		title, description = "Upload errors", "could not be uploaded to the WebDAV server"
	}
	header := "# " + title + "\n\nFiles that " + description + ".\n\n" +
		"| File | Date | Size, MB | Error code |\n| --- | --- | --- | --- |\n"
	row := []string{remotePath, time.Now().Format(dateFormat), mdMegabytes(bytes), code}
	if err := a.appendRows(downloadErrorName, header, [][]string{row}, false); err != nil {
		logMessage(fmt.Sprintf("Failed to write %s: %v", downloadErrorName, err), "error")
	}
}

// pruneTable drops table rows whose date cell (dateColumn) is older than cutoff; unparsable rows are kept.
func (a *App) pruneTable(name string, dateColumn int, cutoff time.Time) {
	a.reportMu.Lock()
	defer a.reportMu.Unlock()

	target := filepath.Join(a.cfg.DestinationDir, name)
	data, err := os.ReadFile(target)
	if err != nil {
		return
	}

	lines := strings.SplitAfter(string(data), "\n")
	kept := make([]string, 0, len(lines))
	removed := 0
	for _, line := range lines {
		if strings.HasPrefix(strings.TrimSpace(line), "|") {
			cells := splitMDRow(line)
			if dateColumn < len(cells) {
				if date, err := time.ParseInLocation(dateFormat, cells[dateColumn], time.Local); err == nil && date.Before(cutoff) {
					removed++
					continue
				}
			}
		}
		kept = append(kept, line)
	}
	if removed == 0 {
		return
	}
	if err := os.WriteFile(target, []byte(strings.Join(kept, "")), 0o644); err != nil {
		logMessage(fmt.Sprintf("Failed to update %s: %v", name, err), "error")
		return
	}
	logMessage(fmt.Sprintf("Removed %d expired records from %s", removed, name), "info")
}

// purgeOldRecords removes dated reports and old rows of the service md files after TrashRetentionDays.
func (a *App) purgeOldRecords() {
	cutoff := time.Now().AddDate(0, 0, -a.cfg.TrashRetentionDays)

	entries, err := os.ReadDir(a.cfg.DestinationDir)
	if err != nil {
		return
	}
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasPrefix(name, reportFilePrefix) || !strings.HasSuffix(name, reportFileExt) {
			continue
		}
		datePart := strings.TrimSuffix(strings.TrimPrefix(name, reportFilePrefix), reportFileExt)
		date, err := time.ParseInLocation("20060102", datePart, time.Local)
		if err != nil || !date.Before(cutoff) {
			continue
		}
		if err := os.Remove(filepath.Join(a.cfg.DestinationDir, name)); err != nil {
			logMessage(fmt.Sprintf("Failed to delete report %s: %v", name, err), "error")
			continue
		}
		logMessage(fmt.Sprintf("Deleted expired report: %s", name), "info")
	}

	a.pruneTable(downloadErrorName, 1, cutoff)
	a.pruneTable(replaceWarnName, 3, cutoff)
	a.pruneTable(deleteWarnName, 2, cutoff)
}
