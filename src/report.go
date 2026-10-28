package src

import (
	"fmt"
	"html"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

type folderSummary struct {
	total, completed, errors int
	bytes                    int64
}

func escapeHTML(value string) string {
	return strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;", `"`, "&quot;", "'", "&#39;").Replace(value)
}

// formatBytes renders megabytes in the ru-RU style: "1 234,5 МБ".
func formatBytes(bytes int64) string {
	text := strconv.FormatFloat(float64(bytes)/(1024*1024), 'f', 1, 64)
	text = strings.TrimSuffix(text, ".0")
	whole, fraction, _ := strings.Cut(text, ".")

	var grouped []string
	for len(whole) > 3 {
		grouped = append([]string{whole[len(whole)-3:]}, grouped...)
		whole = whole[:len(whole)-3]
	}
	grouped = append([]string{whole}, grouped...)

	result := strings.Join(grouped, "\u00a0")
	if fraction != "" {
		result += "," + fraction
	}
	return result + " МБ"
}

const reportTemplate = `<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Отчёт о скачивании WebDAV</title>
  <style>
    body{font:15px/1.5 system-ui,sans-serif;margin:32px;color:#202a35;background:#f5f7f8}
    main{max-width:1100px;margin:auto}h1{font-size:24px}p{color:#586675}
    table{width:100%;border-collapse:collapse;background:white}
    th,td{text-align:left;padding:10px 12px;border-bottom:1px solid #e3e8eb}
    th{background:#edf1f3}tr.complete{background:#e7f5e9}tr.error{background:#fde9e7}
    @media(max-width:600px){body{margin:16px}table{font-size:13px}th,td{padding:8px 6px}}
  </style>
</head>
<body><main>
  <h1>Отчёт о скачивании WebDAV</h1>
  <p>Обновлено: {{UPDATED}}. Файлов: {{COUNT}}.</p>
  <table><thead><tr><th>Папка (до 2 уровней)</th><th>Общий размер</th><th>Скачано</th><th>Файлов с ошибками</th></tr></thead>
  <tbody>{{ROWS}}</tbody></table>
</main></body></html>`

func (a *App) writeReport() error {
	type fileInfo struct {
		path     string
		size     int64
		status   string
		attempts int
	}

	var files []fileInfo
	a.store.With(func(s *State) {
		for remotePath, file := range s.Files {
			info := fileInfo{path: remotePath, status: file.Status, attempts: file.Attempts}
			if file.Size != nil {
				info.size = *file.Size
			}
			files = append(files, info)
		}
	})

	folders := map[string]*folderSummary{}
	for _, file := range files {
		var parts []string
		for _, part := range strings.Split(file.path, "/") {
			if part != "" {
				parts = append(parts, part)
			}
		}
		folderPaths := []string{"/"}
		if len(parts) > 0 {
			folderPaths = append(folderPaths, "/"+parts[0])
		}
		if len(parts) > 1 {
			folderPaths = append(folderPaths, "/"+parts[0]+"/"+parts[1])
		}

		for _, folderPath := range folderPaths {
			summary := folders[folderPath]
			if summary == nil {
				summary = &folderSummary{}
				folders[folderPath] = summary
			}
			summary.total++
			summary.bytes += file.size
			if file.status == statusCompleted {
				summary.completed++
			}
			if file.status == statusError || (file.status == statusDownloading && file.attempts > 0) {
				summary.errors++
			}
		}
	}

	folderPaths := make([]string, 0, len(folders))
	for folderPath := range folders {
		folderPaths = append(folderPaths, folderPath)
	}
	sort.Slice(folderPaths, func(i, j int) bool {
		left, right := strings.ToLower(folderPaths[i]), strings.ToLower(folderPaths[j])
		if left != right {
			return left < right
		}
		return folderPaths[i] < folderPaths[j]
	})

	var rows strings.Builder
	for _, folderPath := range folderPaths {
		summary := folders[folderPath]
		percent := 100
		if summary.total > 0 {
			percent = int(math.Round(float64(summary.completed) / float64(summary.total) * 100))
		}
		rowClass := ""
		if summary.errors > 0 {
			rowClass = "error"
		} else if summary.total > 0 && summary.completed == summary.total {
			rowClass = "complete"
		}
		depth, label := 0, "Корневая папка"
		if folderPath != "/" {
			depth = strings.Count(folderPath, "/")
			label = folderPath
		}

		if rows.Len() > 0 {
			rows.WriteString("\n")
		}
		fmt.Fprintf(&rows, `<tr class="%s"><td style="padding-left:%dpx">%s</td><td>%s</td><td>%d%% (%d/%d)</td><td>%d</td></tr>`,
			rowClass, depth*20+12, escapeHTML(label), formatBytes(summary.bytes), percent, summary.completed, summary.total, summary.errors)
	}
	if rows.Len() == 0 {
		rows.WriteString(`<tr><td colspan="4">Файлы не найдены</td></tr>`)
	}

	page := strings.NewReplacer(
		"{{UPDATED}}", html.EscapeString(time.Now().Format("02.01.2006, 15:04:05")),
		"{{COUNT}}", strconv.Itoa(len(files)),
		"{{ROWS}}", rows.String(),
	).Replace(reportTemplate)

	reportPath := filepath.Join(a.cfg.DestinationDir, "download-report.html")
	temporaryPath := reportPath + ".tmp"
	if err := os.MkdirAll(a.cfg.DestinationDir, 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(temporaryPath, []byte(page), 0o644); err != nil {
		return err
	}
	if err := os.Rename(temporaryPath, reportPath); err != nil {
		return err
	}
	logMessage(fmt.Sprintf("Отчёт обновлён: %s", reportPath), "info")
	return nil
}
