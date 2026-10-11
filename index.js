"use strict";

const fs = require("node:fs/promises");
const path = require("node:path");
const crypto = require("node:crypto");
const { createClient } = require("webdav");
const { loadavg } = require("node:os");
require("dotenv").config();

const FILE_STATUS = Object.freeze({
  PENDING: "pending",
  DOWNLOADING: "downloading",
  COMPLETED: "completed",
  ERROR: "error",
});

const DIRECTORY_LIST_DELAY_MS = 1000;
const REPORT_INTERVAL_MS = 30_000;
const RETRY_DELAYS_MS = [10_000, 20_000, 30_000];
const MAX_RETRIES = 10;

const YELLOW_COLOR = "\x1b[33m";
const RESET_COLOR = "\x1b[0m";
const RED_COLOR = "\x1b[31m";
const ORANGE_COLOR = "\x1b[38;5;214m";
const GREY_COLOR = "\x1b[90m";

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

function createWebDavClient() {
  return createClient(config.url, {
    username: config.username,
    password: config.password,
  });
}

var client = createWebDavClient();

const state = { version: 1, files: {}, folders: {} };
let stopping = false;
let saveChain = Promise.resolve();
let mainWorkerRunning = false;
let discoveryRunning = false;
const mainQueue = [];
const errorQueue = [];
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

function log(message, logLevel) {
  let result = message;
  switch(logLevel) {
    case "error":
      result = `${RED_COLOR}${message}${RESET_COLOR}`;
      break;
    case "warn":
      result = `${ORANGE_COLOR}${message}${RESET_COLOR}`;
      break;
    case "trace":
      result = `${GREY_COLOR}${message}${RESET_COLOR}`;
      break;
    case "info":
    default:
      result = message;
      break;
  }
  console.log(`${YELLOW_COLOR}[${new Date().toISOString()}]${RESET_COLOR} ${result}`);
}

function isRateLimitError(error) {
  return error?.status === 429 || error?.response?.status === 429;
}

async function loadState() {
  log(`Загружаем состояние...`, 'trace');
  try {
    const saved = JSON.parse(await fs.readFile(config.statePath, "utf8"));
    if (saved && saved.files && typeof saved.files === "object") {
      state.files = saved.files;
      state.version = saved.version || 1;
    }
  } catch (error) {
    if (error.code !== "ENOENT") {
      log(`Не удалось загрузить состояние: ${error.message}. Будет создано новое.`, 'error');
    }
  }

  log(`Формируем очередь загрузки...`, 'trace');

  for (const [remotePath, file] of Object.entries(state.files)) {
    if (file.status === FILE_STATUS.DOWNLOADING) {
      file.status = FILE_STATUS.PENDING;
    }
    if (file.status === FILE_STATUS.PENDING) {
      mainQueue.push(remotePath);
    } else if (file.status === FILE_STATUS.ERROR) {
      errorQueue.push(file);
    }
  }

  log(`Состояние загружено. ${mainQueue.length} файлов в очереди.`, 'info');
}

function persistState() {
  var snapshot = JSON.stringify(state, null, 2);
  saveChain = saveChain
    .catch(e => log(`Не удалось сохранить состояние: ${e.message}`, 'error'))
    .then(async () => {
      await fs.mkdir(path.dirname(config.statePath), { recursive: true });
      await fs.writeFile(config.statePath, snapshot, "utf8");
    })
    .catch((error) => log(`Не удалось сохранить состояние: ${error.message}`, 'error'));
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

async function getDirectoryContentsWithRecovery(directory) {
  try {
    return await client.getDirectoryContents(directory);
  } catch (requestError) {
    if (isRateLimitError(requestError)) {
        await sleep(DIRECTORY_LIST_DELAY_MS);
        return await getDirectoryContentsWithRecovery(directory);
    }

    log(`Ошибка запроса каталога ${directory}: ${requestError.message}. Проверяю подключение к этому же каталогу.`, 'error');
    try {
      const entries = await client.getDirectoryContents(directory);
      log(`Подключение WebDAV доступно; продолжаю с каталога ${directory}.`, 'info');
      return entries;
    } catch (checkError) {
      if (isRateLimitError(checkError)) {
        await sleep(DIRECTORY_LIST_DELAY_MS);
        return await getDirectoryContentsWithRecovery(directory);
      }

      log(`Каталог ${directory} недоступен: ${checkError.message}. Пересоздаю клиент.`, 'error');
      client = createWebDavClient();

      try {
        const entries = await client.getDirectoryContents(directory);
        log(`Подключение восстановлено; продолжаю с каталога ${directory}.`, 'info');
        return entries;
      } catch (reconnectError) {
        await sleep(DIRECTORY_LIST_DELAY_MS);
        return await getDirectoryContentsWithRecovery(directory);
      }
    }
  }
}

async function listRemoteFiles(remotePath, onFile) {
  const folder = state.folders[remotePath] || {
    path: remotePath,
    status: FILE_STATUS.PENDING,
    children: []
  };

  if (folder.status === FILE_STATUS.COMPLETED) return;

  await sleep(DIRECTORY_LIST_DELAY_MS);
  const entries = folder.status == FILE_STATUS.DOWNLOADING
    ? folder.children.map(path => ({ type: "directory", filename: path })) // only folders
    : await getDirectoryContentsWithRecovery(folder.path);

  folder.status = FILE_STATUS.DOWNLOADING;
  state.folders[remotePath] = folder;

  log(`directory contents for ${folder.path}: ${entries.length} entries`, 'info');

  for (const entry of entries) {
    const remotePath = getEntryPath(entry);
    if (entry.type === "directory") {
      const childrenPath = remotePath.endsWith("/") ? remotePath : `${remotePath}/`;
      folder.children.push(childrenPath);
    } else if (entry.type === "file") {
      const parsedSize = Number(entry.size);
      const remoteFile = {
        remotePath,
        size: Number.isFinite(parsedSize) && parsedSize >= 0 ? parsedSize : null,
        etag: entry.etag || null,
        lastmod: entry.lastmod || null,
      };
      await onFile(remoteFile);
    }
  }

  await persistState();

  for (const child of folder.children) {
    await listRemoteFiles(child, onFile);
  }

  folder.status = FILE_STATUS.COMPLETED;
  await persistState();
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

    await listRemoteFiles(config.remotePath, async (remoteFile) => {
      const { remotePath } = remoteFile;
      seen.add(remotePath);
      const previous = state.files[remotePath];

      if (!previous) {
        state.files[remotePath] = {
          ...remoteFile,
          status: FILE_STATUS.PENDING,
          attempts: 0,
          nextRetryAt: null,
        };
        mainQueue.push(remotePath);
        await persistState();
      } else if (previous.status === FILE_STATUS.COMPLETED) {
        return;
      } else if (metadataChanged(previous, remoteFile)) {
        if (previous.status === FILE_STATUS.DOWNLOADING) return;
        Object.assign(previous, remoteFile, {
          status: FILE_STATUS.PENDING,
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
      if (!seen.has(remotePath) && state.files[remotePath].status !== FILE_STATUS.COMPLETED) {
        delete state.files[remotePath];
        await persistState();
      }
    }
  } catch (error) {
    log(`Ошибка сканирования WebDAV: ${error.message}`, 'error');
    if (isRateLimitError(error)) {
      log("Получен HTTP 429; клиент не пересоздаётся.", 'warn');
    }
  } finally {
    discoveryRunning = false;
    log('=> Сканирование завершено.', 'info');
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
    throw new Error(`Недопустимый путь WebDAV: ${remotePath}`, 'error');
  }

  const localPath = path.resolve(config.destinationDir, ...segments);
  const relativePath = path.relative(config.destinationDir, localPath);
  if (relativePath.startsWith("..") || path.isAbsolute(relativePath)) {
    throw new Error(`Путь выходит за папку назначения: ${remotePath}`, 'error');
  }
  return localPath;
}

function retryDelay(attempts) {
  return RETRY_DELAYS_MS[Math.min(attempts - 1, RETRY_DELAYS_MS.length - 1)];
}

async function downloadFile(remotePath, timeout) {
  const file = state.files[remotePath];
  if (!file) return;

  const timeout2 = timeout || 60_000;
  log(`Начало скачивания: ${remotePath}. Таймаут: ${(timeout2/1000).toFixed(2)} сек.`, 'trace');

  file.status = FILE_STATUS.DOWNLOADING;
  await persistState();
  try {
    const destination = localPathFor(remotePath);
    await fs.mkdir(path.dirname(destination), { recursive: true });
    const contents = await client.getFileContents(remotePath, { format: "binary", timeout: timeout2 });
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

    file.status = FILE_STATUS.COMPLETED;
    file.downloadedAt = new Date().toISOString();
    file.nextRetryAt = null;

    // remove entry from errorQueue if it exists
    const errorIndex = errorQueue.indexOf(file);
    if (errorIndex !== -1) {
      errorQueue.splice(errorIndex, 1);
    }

    await persistState();
    log(`Скачан: ${remotePath}`, 'trace');
  } catch (error) {
    file.status = FILE_STATUS.ERROR;
    file.attempts = (file.attempts || 0) + 1;
    const delay = retryDelay(file.attempts);
    file.nextRetryAt = Date.now() + delay;

    errorQueue.push(file);

    await persistState();
    log(`Ошибка ${remotePath}: ${error.message}. Повтор через ${delay / 1000} сек.`, 'error');
  }
}

function calculateDownloadTimeout(fileSizeInBytes) {
  if (!fileSizeInBytes) {
    return null;
  }

  const minSpeedMbps = 2;
  const extraReserveMs = 5000;

  // 1. Translate from Mbps to bytes per second:
  // (Mbps * 1024 * 1024) / 8 bits = bytes per second
  const speedBytesPerSecond = (minSpeedMbps * 1024 * 1024) / 8;

  // 2. Calculate the pure download time in seconds:
  const downloadTimeSeconds = fileSizeInBytes / speedBytesPerSecond;

  // 3. Convert to milliseconds and add extra reserve for lags/connection:
  const timeoutMs = (downloadTimeSeconds * 1000) + extraReserveMs;

  return Math.round(timeoutMs);
}

async function mainWorker() {
  mainWorkerRunning = true;
  while (!stopping && (mainQueue.length > 0 || discoveryRunning)) {
    const remotePath = mainQueue.shift();
    if (remotePath) {
      const file = state.files[remotePath];
      if (file?.status === FILE_STATUS.PENDING) {
        const timeout = calculateDownloadTimeout(file?.size);
        await downloadFile(remotePath, timeout);
      }
      continue;
    }

    await sleep(250);
  }
  mainWorkerRunning = false;
  log(`=> Main worker stopped. Error queue length: ${errorQueue.length}`, 'info');
}

async function retryWorker() {
  while (!stopping && (errorQueue.length > 0 || mainWorkerRunning || discoveryRunning)) {
    const now = Date.now();

    const dueRetry = errorQueue.find(([, file]) =>
      file.status === FILE_STATUS.ERROR && (file.nextRetryAt || 0) <= now && file.attempts <= MAX_RETRIES);

    if (dueRetry) {
      const file = dueRetry[1];
      const timeout = calculateDownloadTimeout(file?.size);
      await downloadFile(dueRetry[0], timeout);

      // wait to the next entity
      await sleep(250);
      continue;
    }

    // wait for the queue
    await sleep(1000);
  }
  log(`=> Retry worker stopped. Error queue length: ${errorQueue.length}`, 'info');
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
  const megabytes = bytes / (1024 * 1024);
  return `${new Intl.NumberFormat("ru-RU", { maximumFractionDigits: 1 }).format(megabytes)} МБ`;
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
      if (file.status === FILE_STATUS.COMPLETED) summary.completed += 1;
      if (file.status === FILE_STATUS.ERROR || (file.status === FILE_STATUS.DOWNLOADING && file.attempts > 0)) {
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
  log(`Отчёт обновлён: ${reportPath}`, 'info');
}

async function runPeriodic(task, interval, label) {
  while (!stopping) {
    try {
      await task();
    } catch (error) {
      log(`${label}: ${error.message}`, 'error');
    }
    await sleep(interval);
  }
}

async function shutdown() {
  if (stopping) return;
  stopping = true;
  for (const wake of waiters) wake();
  log("Остановка: сохраняю состояние...", 'info');
  await persistState();
  await Promise.allSettled(runningTasks);
  await persistState();
  process.exitCode = 0;
}

let runningTasks = [];

async function start() {
  await loadState();
  await fs.mkdir(config.destinationDir, { recursive: true });

  const stop = async () => {
    await shutdown();
  };
  process.once("SIGINT", stop);
  process.once("SIGTERM", stop);

  const main = mainWorker(); 
  const retry = retryWorker();
  const discovery = discoverRemoteFiles();

  runningTasks.push(
    main,
    retry,
    discovery,
    runPeriodic(writeReport, REPORT_INTERVAL_MS, "Ошибка создания отчёта"),
  );

  log(`Запущено. Сервер: ${config.url}; папка назначения: ${config.destinationDir}`, 'info');

  // wait for the main threads
  await Promise.allSettled([ main, retry, discovery ]);

  if (stopping) return;
}

start().catch((error) => {
  console.error(`Не удалось запустить приложение: ${error.message}`);
  process.exitCode = 1;
});