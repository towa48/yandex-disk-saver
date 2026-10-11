"use strict";

const fs = require("node:fs/promises");
const path = require("node:path");
const crypto = require("node:crypto");
const { createClient } = require("webdav");
const { loadavg } = require("node:os");
require("dotenv").config();

const DIRECTORY_LIST_DELAY_MS = 500;
const REPORT_INTERVAL_MS = 60_000;
const DISCOVERY_INTERVAL_MS = 60_000;
const RETRY_DELAYS_MS = [10_000, 20_000, 30_000];

function readConfig() {
  const protocol = (process.env.WEBDAV_PROTOCOL || "http").replace(/:$/, "");
  const host = process.env.WEBDAV_HOST || "localhost";
  const port = Number(process.env.WEBDAV_PORT || 80);

  if (!["http", "https"].includes(protocol)) {
    throw new Error("WEBDAV_PROTOCOL должен быть http или https");
  }
  if (!Number.isInteger(port) || port < 1 || port > 65_535) {
    throw new Error("WEBDAV_PORT должен быть целым числом от 1 до 65535");
  }

  const remotePath = `/${(process.env.WEBDAV_REMOTE_PATH || "")
    .split("/")
    .filter(Boolean)
    .join("/")}`;

  return {
    url: `${protocol}://${host}:${port}`,
    username: process.env.WEBDAV_USERNAME || "user",
    password: process.env.WEBDAV_PASSWORD || "password",
    remotePath: remotePath === "/" ? "/" : `${remotePath}/`,
    destinationDir: path.resolve(process.env.DESTINATION_DIR || "./downloads"),
    statePath: path.resolve(process.env.STATE_FILE || "./state.json"),
  };
}

const config = readConfig();
const client = createClient(config.url, {
  username: config.username,
  password: config.password,
});

const state = { version: 1, files: {} };
let stopping = false;
let saveChain = Promise.resolve();
let discoveryRunning = false;
const mainQueue = [];
const waiters = new Set();

function sleep(milliseconds) {
  return new Promise((resolve) => {
    const finish = () => {
      clearTimeout(timer);
      waiters.delete(finish);
      resolve();
    };
    const timer = setTimeout(finish, milliseconds);
    waiters.add(finish);
  });
}

function log(message) {
  console.log(`[${new Date().toISOString()}] ${message}`);
}

async function loadState() {
  try {
    const saved = JSON.parse(await fs.readFile(config.statePath, "utf8"));
    if (saved && saved.files && typeof saved.files === "object") {
      state.files = saved.files;
      state.version = saved.version || 1;
    }
  } catch (error) {
    if (error.code !== "ENOENT") {
      log(`Не удалось загрузить состояние: ${error.message}. Будет создано новое.`);
    }
  }

  for (const [remotePath, file] of Object.entries(state.files)) {
    if (file.status === "downloading") {
      file.status = "pending";
    }
    if (file.status === "pending") {
      mainQueue.push(remotePath);
    }
  }
}

function persistState() {
  var snapshot = JSON.stringify(state, null, 2);
  saveChain = saveChain
    .catch(e => log(`Не удалось сохранить состояние: ${e.message}`))
    .then(async () => {
      await fs.mkdir(path.dirname(config.statePath), { recursive: true });
      await fs.writeFile(config.statePath, snapshot, "utf8");
      log(`Состояние сохранено в ${config.statePath}`);
    })
    .catch((error) => log(`Не удалось сохранить состояние: ${error.message}`));
  return saveChain;
}

function getEntryPath(entry) {
  if (entry.filename) {
    return entry.filename.startsWith("/")
      ? path.posix.normalize(entry.filename)
      : path.posix.resolve(config.remotePath, entry.filename);
  }
  return path.posix.join(config.remotePath, entry.basename);
}

async function listRemoteFiles(onFile) {
  const files = [];
  const directories = [config.remotePath];
  const visited = new Set();

  while (directories.length > 0) {
    const directory = directories.pop();
    if (visited.has(directory)) continue;
    visited.add(directory);

    await sleep(DIRECTORY_LIST_DELAY_MS);
    const entries = await client.getDirectoryContents(directory);
    log(`directory contents for ${directory}: ${entries.length} entries`);
    for (const entry of entries) {
      const remotePath = getEntryPath(entry);
      if (entry.type === "directory") {
        directories.push(remotePath.endsWith("/") ? remotePath : `${remotePath}/`);
      } else if (entry.type === "file") {
        const parsedSize = Number(entry.size);
        const remoteFile = {
          remotePath,
          size: Number.isFinite(parsedSize) && parsedSize >= 0 ? parsedSize : null,
          etag: entry.etag || null,
          lastmod: entry.lastmod || null,
        };
        files.push(remoteFile);
        await onFile(remoteFile);
      }
    }
  }

  return files;
}

function metadataChanged(previous, remoteFile) {
  if (!previous) return false;
  if (previous.etag && remoteFile.etag) return previous.etag !== remoteFile.etag;
  if (previous.size !== null && remoteFile.size !== null && previous.size !== remoteFile.size) {
    return true;
  }
  return Boolean(previous.lastmod && remoteFile.lastmod && previous.lastmod !== remoteFile.lastmod);
}

async function discoverRemoteFiles() {
  if (discoveryRunning || stopping) return;
  discoveryRunning = true;
  try {
    const seen = new Set();

    const remoteFiles = await listRemoteFiles(async (remoteFile) => {
      const { remotePath } = remoteFile;
      seen.add(remotePath);
      const previous = state.files[remotePath];

      if (!previous) {
        state.files[remotePath] = {
          ...remoteFile,
          status: "pending",
          attempts: 0,
          nextRetryAt: null,
        };
        mainQueue.push(remotePath);
        await persistState();
      } else if (previous.status === "completed") {
        return;
      } else if (metadataChanged(previous, remoteFile)) {
        if (previous.status === "downloading") return;
        Object.assign(previous, remoteFile, {
          status: "pending",
          attempts: 0,
          nextRetryAt: null,
        });
        mainQueue.push(remotePath);
        await persistState();
      } else {
        const hasMetadataChanges = Object.entries(remoteFile)
          .some(([key, value]) => previous[key] !== value);
        if (hasMetadataChanges) {
          Object.assign(previous, remoteFile);
          await persistState();
        }
      }
    });

    for (const remotePath of Object.keys(state.files)) {
      if (!seen.has(remotePath) && state.files[remotePath].status !== "completed") {
        delete state.files[remotePath];
        await persistState();
      }
    }
    log(`Обнаружено файлов: ${remoteFiles.length}`);
  } catch (error) {
    log(`Ошибка сканирования WebDAV: ${error.message}`);
  } finally {
    discoveryRunning = false;
  }
}

function localPathFor(remotePath) {
  const segments = remotePath.split("/").filter(Boolean).map((segment) => {
    try {
      return decodeURIComponent(segment);
    } catch {
      return segment;
    }
  });

  if (segments.some((segment) => segment === "." || segment === ".." || /[\\/]/.test(segment))) {
    throw new Error(`Недопустимый путь WebDAV: ${remotePath}`);
  }

  const localPath = path.resolve(config.destinationDir, ...segments);
  const relativePath = path.relative(config.destinationDir, localPath);
  if (relativePath.startsWith("..") || path.isAbsolute(relativePath)) {
    throw new Error(`Путь выходит за папку назначения: ${remotePath}`);
  }
  return localPath;
}

function retryDelay(attempts) {
  return RETRY_DELAYS_MS[Math.min(attempts - 1, RETRY_DELAYS_MS.length - 1)];
}

async function downloadFile(remotePath) {
  const file = state.files[remotePath];
  if (!file) return;

  file.status = "downloading";
  await persistState();
  try {
    const destination = localPathFor(remotePath);
    await fs.mkdir(path.dirname(destination), { recursive: true });
    const contents = await client.getFileContents(remotePath, { format: "binary" });
    const buffer = Buffer.from(contents);
    const temporaryPath = `${destination}.part`;
    await fs.writeFile(temporaryPath, buffer);

    if (file.etag) {
      const expectedMd5 = file.etag.replace(/^W\//i, "").replace(/^"|"$/g, "").toLowerCase();
      const actualMd5 = crypto.createHash("md5").update(buffer).digest("hex");

      if (actualMd5 !== expectedMd5) {
        await fs.rm(temporaryPath, { force: true });
        await fs.rm(destination, { force: true });
        file.integrityFailures = (file.integrityFailures || 0) + 1;
        throw new Error(`MD5 не совпал: ожидался ${expectedMd5}, получен ${actualMd5}. Системная нагрузка: ${loadavg().join(", ")}`);
      }
    }

    await fs.rename(temporaryPath, destination);

    file.status = "completed";
    file.downloadedAt = new Date().toISOString();
    file.nextRetryAt = null;
    await persistState();
    log(`Скачан: ${remotePath}`);
  } catch (error) {
    file.status = "error";
    file.attempts = (file.attempts || 0) + 1;
    const delay = retryDelay(file.attempts);
    file.nextRetryAt = Date.now() + delay;

    await persistState();
    log(`Ошибка ${remotePath}: ${error.message}. Повтор через ${delay / 1000} сек.`);
  }
}

async function mainWorker() {
  while (!stopping) {
    const remotePath = mainQueue.shift();
    if (remotePath) {
      if (state.files[remotePath]?.status === "pending") {
        await downloadFile(remotePath);
      }
      continue;
    }

    await sleep(250);
  }
}

async function retryWorker() {
  while (!stopping) {
    const now = Date.now();
    const dueRetry = Object.entries(state.files).find(([, file]) =>
      file.status === "error" && (file.nextRetryAt || 0) <= now);

    if (dueRetry) {
      await downloadFile(dueRetry[0]);
      continue;
    }

    const nextRetryAt = Object.values(state.files).reduce((earliest, file) => {
      return file.status === "error"
        ? Math.min(earliest, file.nextRetryAt || 0)
        : earliest;
    }, Infinity);
    const waitTime = Number.isFinite(nextRetryAt)
      ? Math.min(1_000, Math.max(1, nextRetryAt - Date.now()))
      : 1_000;
    await sleep(waitTime);
  }
}

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
  return `${new Intl.NumberFormat("ru-RU", { maximumFractionDigits: 1 }).format(bytes)} Б`;
}

async function writeReport() {
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
      if (file.status === "completed") summary.completed += 1;
      if (file.status === "error" || (file.status === "downloading" && file.attempts > 0)) {
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

  const reportPath = path.join(config.destinationDir, "download-report.html");
  const temporaryPath = `${reportPath}.tmp`;
  await fs.mkdir(config.destinationDir, { recursive: true });
  await fs.writeFile(temporaryPath, html, "utf8");
  await fs.rename(temporaryPath, reportPath);
  log(`Отчёт обновлён: ${reportPath}`);
}

async function runPeriodic(task, interval, label) {
  while (!stopping) {
    try {
      await task();
    } catch (error) {
      log(`${label}: ${error.message}`);
    }
    await sleep(interval);
  }
}

async function shutdown() {
  if (stopping) return;
  stopping = true;
  for (const wake of waiters) wake();
  log("Остановка: сохраняю состояние...");
  await persistState();
  await Promise.allSettled(runningTasks);
  await persistState();
  process.exitCode = 0;
}

let runningTasks = [];

async function start() {
  await loadState();
  await fs.mkdir(config.destinationDir, { recursive: true });
  await persistState();
  await discoverRemoteFiles();
  await writeReport().catch((error) => log(`Ошибка создания отчёта: ${error.message}`));

  runningTasks = [
    mainWorker(),
    retryWorker(),
    runPeriodic(discoverRemoteFiles, DISCOVERY_INTERVAL_MS, "Ошибка сканирования"),
    runPeriodic(writeReport, REPORT_INTERVAL_MS, "Ошибка создания отчёта"),
  ];

  const stop = async () => {
    await shutdown();
  };
  process.once("SIGINT", stop);
  process.once("SIGTERM", stop);
  log(`Запущено. Сервер: ${config.url}; папка назначения: ${config.destinationDir}`);
}

start().catch((error) => {
  console.error(`Не удалось запустить приложение: ${error.message}`);
  process.exitCode = 1;
});