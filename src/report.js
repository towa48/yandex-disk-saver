"use strict";

const fs = require("node:fs/promises");
const path = require("node:path");

function escapeHtml(value) {
  return String(value).replace(/[&<>"']/g, (character) => ({
    "&": "&amp;",
    "<": "&lt;",
    ">": "&gt;",
    '"': "&quot;",
    "'": "&#39;",
  })[character]);
}

function formatBytes(bytes) {
  const megabytes = bytes / (1024 * 1024);
  return `${new Intl.NumberFormat("ru-RU", { maximumFractionDigits: 1 }).format(megabytes)} МБ`;
}

async function writeReport({ state, destinationDir, fileStatus, log }) {
  const folders = new Map();
  const files = Object.entries(state.files);

  for (const [remotePath, file] of files) {
    const parts = remotePath.split("/").filter(Boolean);
    const folderPaths = ["/"];
    if (parts.length > 0) folderPaths.push(`/${parts[0]}`);
    if (parts.length > 1) folderPaths.push(`/${parts[0]}/${parts[1]}`);

    for (const folderPath of folderPaths) {
      if (!folders.has(folderPath)) {
        folders.set(folderPath, { total: 0, completed: 0, bytes: 0, errors: 0 });
      }
      const summary = folders.get(folderPath);
      summary.total += 1;
      summary.bytes += file.size || 0;
      if (file.status === fileStatus.COMPLETED) summary.completed += 1;
      if (file.status === fileStatus.ERROR || (file.status === fileStatus.DOWNLOADING && file.attempts > 0)) {
        summary.errors += 1;
      }
    }
  }

  const sortedFolders = [...folders.entries()].sort(([left], [right]) =>
    left.localeCompare(right, "ru"));
  const rows = sortedFolders.map(([folderPath, summary]) => {
    const percent = summary.total ? Math.round((summary.completed / summary.total) * 100) : 100;
    const rowClass = summary.errors > 0
      ? "error"
      : summary.total > 0 && summary.completed === summary.total
        ? "complete"
        : "";
    const depth = folderPath === "/" ? 0 : folderPath.split("/").filter(Boolean).length;
    const label = folderPath === "/" ? "Корневая папка" : folderPath;

    return `<tr class="${rowClass}"><td style="padding-left:${depth * 20 + 12}px">${escapeHtml(label)}</td><td>${formatBytes(summary.bytes)}</td><td>${percent}% (${summary.completed}/${summary.total})</td><td>${summary.errors}</td></tr>`;
  }).join("\n");

  const html = `<!doctype html>
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
  <p>Обновлено: ${escapeHtml(new Date().toLocaleString("ru-RU"))}. Файлов: ${files.length}.</p>
  <table><thead><tr><th>Папка (до 2 уровней)</th><th>Общий размер</th><th>Скачано</th><th>Файлов с ошибками</th></tr></thead>
  <tbody>${rows || '<tr><td colspan="4">Файлы не найдены</td></tr>'}</tbody></table>
</main></body></html>`;

  const reportPath = path.join(destinationDir, "download-report.html");
  const temporaryPath = `${reportPath}.tmp`;
  await fs.mkdir(destinationDir, { recursive: true });
  await fs.writeFile(temporaryPath, html, "utf8");
  await fs.rename(temporaryPath, reportPath);
  log(`Отчёт обновлён: ${reportPath}`, "info");
}

module.exports = { writeReport };
