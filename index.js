"use strict";

const fs = require("node:fs/promises");
const path = require("node:path");
const crypto = require("node:crypto");
const { createClient } = require("webdav");
const { Subject, concatMap, mergeMap } = require("rxjs");
const { loadavg } = require("node:os");
require("dotenv").config();

const FILE_STATUS = Object.freeze({
  PENDING: "pending",
  DOWNLOADING: "downloading",
  COMPLETED: "completed",
  ERROR: "error",
});

const DIRECTORY_LIST_DELAY_MS = 1000;
const DIRECTORY_REQUEST_TIMEOUT_MS = 10_000;
const REPORT_INTERVAL_MS = 30_000;
const MAX_FILE_RETRIES = 3;
const FILE_DOWNLOAD_TIMEOUT_MS = 250;
const FILE_DOWNLOAD_HEADERS = Object.freeze({
  "User-Agent": "Yandex.Disk/3.2.51 (Windows; 10.0)",
});

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
    headers: FILE_DOWNLOAD_HEADERS
  });
}

var client = createWebDavClient();
let webDavClientLock = Promise.resolve();

function withWebDavClientLock(task) {
  const result = webDavClientLock.then(task);
  webDavClientLock = result.catch(() => {});
  return result;
}

function getWebDavClient() {
  return withWebDavClientLock(() => client);
}

function recreateWebDavClient() {
  return withWebDavClientLock(() => {
    client = createWebDavClient();
    return client;
  });
}

async function getDirectoryContentsWithTimeout(remotePath) {
  const webDavClient = await getWebDavClient();

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), DIRECTORY_REQUEST_TIMEOUT_MS);
  try {
    return await webDavClient.getDirectoryContents(remotePath, { signal: controller.signal });
  } finally {
    clearTimeout(timeout);
  }
}

async function checkAndRecreateWebDavClient() {
  try {
    log(`Проверяю состояние клиента`, 'warn');
    await getDirectoryContentsWithTimeout(config.remotePath);
    log(`Клиент в рабочем состоянии`, 'warn');
  } catch (error) {
    log(`ERROR: ${error.message}`, 'error');
    log(`Ошибка при проверке клиента, пересоздаю.`, 'warn');
    await recreateWebDavClient();
  }
}

async function getFileContents(path, timeout) {
  const webDavClient = await getWebDavClient();
  try {
    const result = await webDavClient.getFileContents(path, {
      format: "binary",
      timeout,
    });
    return result;
  } catch (error) {
    if (error.status && error.status === 503) {
      // for the 503 set ERROR status for the file
      throw error;
    }

    // trying to recreate the WebDav client if the error is not a 503
    await checkAndRecreateWebDavClient();
    const newClient = await getWebDavClient();
    return await newClient.getFileContents(path, {
      format: "binary",
      timeout,
    });
  }
}

async function getDirectoryContents(path, retry = 0) {
  try {
    const result = await getDirectoryContentsWithTimeout(path);
    return result;
  } catch (error) {
    if (isRateLimitError(error) && retry < 3) {
      await sleep(DIRECTORY_LIST_DELAY_MS);
      return await getDirectoryContents(path, retry + 1);
    }

    log(`Ошибка запроса каталога ${path}: ${error.message}. Проверяю подключение к этому же каталогу.`, 'error');

    await checkAndRecreateWebDavClient();
    return await getDirectoryContentsWithTimeout(path);
  }
}

const state = { version: 1, files: {}, folders: {} };
let stopping = false;
let stateLock = Promise.resolve();
let discoveryRunning = false;

const mainQueue = new Subject();
let mainQueueLength = 0;
const errorQueue = new Subject();
let errorQueueLength = 0;

const waiters = new Set();
const downloadWaiters = new Set();
let activeDownloads = 0;

function waitForActiveDownloads() {
  if (activeDownloads === 0)
    return Promise.resolve();
  return new Promise((resolve) => downloadWaiters.add(resolve));
}

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

function enqueueFileDownload(remotePath) {
  mainQueueLength += 1;
  mainQueue.next(remotePath);
}

function getDownloadQueueLength() {
  return mainQueueLength;
}

function enqueueDownloadRetry(errorFile) {
  errorQueueLength += 1;
  errorQueue.next(errorFile);
}

function getErrorQueueLength() {
  return errorQueueLength;
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
    await withStateLock(() => {
      if (saved && saved.files && typeof saved.files === "object") {
        state.files = saved.files;
        state.version = saved.version || 1;
      }
      if (saved && saved.folders && typeof saved.folders === "object") {
        // hash
        const folders = saved.folders;

        // fix duplicate folder entries
        for(const path in folders) {
          const entry = folders[path];
          const children = entry.children || [];

          const unique = {};
          for (const folderPath of children) {
            if (!unique[folderPath]) {
              unique[folderPath] = true;
            }
          }

          entry.children = Object.keys(unique).sort();
          state.folders = folders;
        }
      }
    });
  } catch (error) {
    if (error.code !== "ENOENT") {
      log(`Не удалось загрузить состояние: ${error.message}. Будет создано новое.`, 'error');
    }
  }

  log(`Формируем очередь загрузки...`, 'trace');

  await withStateLock(async () => {
    for (const [remotePath, file] of Object.entries(state.files)) {
      if (file.status === FILE_STATUS.DOWNLOADING) {
        file.status = FILE_STATUS.PENDING;
      }
      if (file.status === FILE_STATUS.PENDING) {
        await enqueueFileDownload(remotePath);
      } else if (file.status === FILE_STATUS.ERROR) {
        await enqueueDownloadRetry(file);
      }
    }
  });

  const mainQueueLength = await getDownloadQueueLength();
  const errorQueueLength = await getErrorQueueLength();
  log(`Состояние загружено. ${mainQueueLength} файлов в основной очереди и ${errorQueueLength} в очереди ошибок.`, 'info');
}

// Mutex for `state`: tasks run strictly one after another; a task must not call these helpers itself.
function withStateLock(task) {
  const result = stateLock.then(task);
  stateLock = result.catch(() => {});
  return result;
}

// Serializes and writes state.json; call only while holding the lock.
async function writeStateFile() {
  try {
    const snapshot = JSON.stringify(state, null, 2);
    await fs.mkdir(path.dirname(config.statePath), { recursive: true });
    const temporaryPath = `${config.statePath}.tmp`;
    await fs.writeFile(temporaryPath, snapshot, "utf8");
    await fs.rename(temporaryPath, config.statePath);
  } catch (error) {
    log(`Не удалось сохранить состояние: ${error.message}`, 'error');
  }
}

function persistState() {
  return withStateLock(writeStateFile);
}

// Applies `mutate` and saves the result in one locked step.
function updateState(mutate) {
  return withStateLock(async () => {
    const result = mutate();
    await writeStateFile();
    return result;
  });
}

function getEntryPath(entry) {
  if (entry.filename) {
    return entry.filename.startsWith("/")
      ? path.posix.normalize(entry.filename)
      : path.posix.resolve(config.remotePath, entry.filename);
  }
  return path.posix.join(config.remotePath, entry.basename);
}

async function listRemoteFiles(remotePath, onFile) {
  const folder = state.folders[remotePath] || {
    path: remotePath,
    status: FILE_STATUS.PENDING,
    children: []
  };

  //if (state.folders[remotePath]) {
  //  log(`Folder ${remotePath} already exists in state with status '${folder.status}'.`, 'trace');
  //}

  if (folder.status === FILE_STATUS.COMPLETED) return;

  // Await only if server request needed
  if (folder.status != FILE_STATUS.DOWNLOADING) {
    await sleep(DIRECTORY_LIST_DELAY_MS);
  }

  const entries = folder.status == FILE_STATUS.DOWNLOADING
    ? folder.children.map(path => ({ type: "directory", filename: path })) // only folders
    : await getDirectoryContents(folder.path);

  await withStateLock(() => {
    folder.status = FILE_STATUS.DOWNLOADING;
    state.folders[remotePath] = folder;
  });

  log(`directory contents for ${folder.path}: ${entries.length} entries`, 'info');

  const childrenFolders = entries.filter(entry => entry.type === "directory").map(entry => {
    const remotePath = getEntryPath(entry);
    return remotePath.endsWith("/") ? remotePath : `${remotePath}/`;
  });

  const childrenFiles = entries.filter(entry => entry.type === "file").map(entry => {
    const remotePath = getEntryPath(entry);
    const parsedSize = Number(entry.size);
    const remoteFile = {
        remotePath,
        size: Number.isFinite(parsedSize) && parsedSize >= 0 ? parsedSize : null,
        etag: entry.etag || null,
        lastmod: entry.lastmod || null,
      };
    return remoteFile;
  });

  log(`children folders for ${folder.path}: ${childrenFolders.length} entries`, 'trace');

  await updateState(() => {
    for (const child of childrenFolders) {
      if (!folder.children.includes(child)) folder.children.push(child);
    }
    log(`children updated for ${folder.path}`, 'trace');
  });

  // Write folders
  await writeStateFile();

  log(`process files of ${folder.path}`, 'trace');
  for (const file of childrenFiles) {
    await onFile(file);
  }

  // Write files
  await writeStateFile();

  for (const child of folder.children) {
    log(`process next child for ${folder.path}: ${child}`, 'trace');
    await listRemoteFiles(child, onFile);
  }

  await updateState(() => {
    folder.status = FILE_STATUS.COMPLETED;
    log(`status completed for ${folder.path}`, 'trace');
  });
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
    await listRemoteFiles(config.remotePath, (remoteFile) => withStateLock(async () => {
      const { remotePath } = remoteFile;
      const previous = state.files[remotePath];

      if (!previous) {
        state.files[remotePath] = {
          ...remoteFile,
          status: FILE_STATUS.PENDING,
          attempts: 0,
          nextRetryAt: null,
        };
        await enqueueFileDownload(remotePath);
      } else if (previous.status === FILE_STATUS.COMPLETED) {
        return;
      } else if (metadataChanged(previous, remoteFile)) {
        if (previous.status === FILE_STATUS.DOWNLOADING) return;
        Object.assign(previous, remoteFile, {
          status: FILE_STATUS.PENDING,
          attempts: 0,
          nextRetryAt: null,
        });
        await enqueueFileDownload(remotePath);
      } else if (Object.entries(remoteFile).some(([key, value]) => previous[key] !== value)) {
        Object.assign(previous, remoteFile);
      }
    }));
  } catch (error) {
    log(`Ошибка сканирования WebDAV: ${error.message}`, 'error');
    if (isRateLimitError(error)) {
      log("Получен HTTP 429; клиент не пересоздаётся.", 'warn');
    }
  } finally {
    discoveryRunning = false;
    mainQueue.complete();
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

async function downloadFile(remotePath, timeout) {
  if (stopping)
    return;

  activeDownloads += 1;
  try {
    await performDownload(remotePath, timeout);
  } finally {
    activeDownloads -= 1;
    if (activeDownloads === 0) {
      for (const resolve of downloadWaiters) resolve();
      downloadWaiters.clear();
    }
  }
}

async function performDownload(remotePath, timeout) {
  let file = null;
  await withStateLock(() => {
    file = state.files[remotePath];
  });

  if (!file) return;

  const timeout2 = timeout || 60_000;
  log(`Начало скачивания: ${remotePath}. Таймаут: ${(timeout2/1000).toFixed(2)} сек.`, 'trace');

  await updateState(() => {
    file.status = FILE_STATUS.DOWNLOADING;
  });

  try {
    const destination = localPathFor(remotePath);
    await fs.mkdir(path.dirname(destination), { recursive: true });
    const contents = await getFileContents(remotePath, timeout2);
    const buffer = Buffer.from(contents);
    const temporaryPath = `${destination}.part`;
    await fs.writeFile(temporaryPath, buffer);

    if (file.etag) {
      const expectedMd5 = file.etag.replace(/^W\//i, "").replace(/^"|"$/g, "").toLowerCase();
      const actualMd5 = crypto.createHash("md5").update(buffer).digest("hex");

      if (actualMd5 !== expectedMd5) {
        await fs.rm(temporaryPath, { force: true });
        await fs.rm(destination, { force: true });
        await withStateLock(() => {
          file.integrityFailures = (file.integrityFailures || 0) + 1;
        });
        throw new Error(`MD5 не совпал: ожидался ${expectedMd5}, получен ${actualMd5}. Системная нагрузка: ${loadavg().join(", ")}`);
      }
    }

    await fs.rename(temporaryPath, destination);

    await updateState(() => {
      file.status = FILE_STATUS.COMPLETED;
      file.downloadedAt = new Date().toISOString();
      file.nextRetryAt = null;
    });

    log(`Скачан: ${remotePath}`, 'trace');
  } catch (error) {
    log(`Ошибка ${remotePath}: ${error.message}.`, 'error');

    await updateState(() => {
      file.status = FILE_STATUS.ERROR;
      file.attempts = (file.attempts || 0) + 1;
    });

    // append the file to the error queue if it hasn't reached the max retries yet
    if (file.attempts < MAX_FILE_RETRIES)
    {
      enqueueDownloadRetry(file);
    }
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

function subscribeToMainQueue() {
  mainQueue.pipe(concatMap(async (remotePath) => {
    if (stopping)
      return;

    mainQueueLength -= 1;
    const length = mainQueueLength;
    log(`${ORANGE_COLOR}MAIN:${RESET_COLOR} Processing file: ${remotePath}. Main queue length: ${length}.`, 'info');

    let file = null
    await withStateLock(() => {
      file = state.files[remotePath];
    });

    if (file?.status === FILE_STATUS.PENDING) {
      await sleep(FILE_DOWNLOAD_TIMEOUT_MS);

      const timeout = calculateDownloadTimeout(file?.size);
      await downloadFile(remotePath, timeout);
    }
  })).subscribe({
    error: (error) => {
      log(`${ORANGE_COLOR}MAIN:${RESET_COLOR} Worker encountered an error: ${error.message}`, 'error');
    },
    complete: async () => {
      const errorQueueLength = await getErrorQueueLength();
      log(`${ORANGE_COLOR}MAIN:${RESET_COLOR} Worker stopped. Error queue length: ${errorQueueLength}.`, 'info');
    
      mainQueue.unsubscribe();
      errorQueue.complete();
    },
  });
}

function subscribeToErrorQueue() {
  errorQueue.pipe(concatMap(async (file) => {
    if (stopping || file.status !== FILE_STATUS.ERROR || file.attempts >= MAX_FILE_RETRIES)
      return;

    await sleep(FILE_DOWNLOAD_TIMEOUT_MS);

    const length = await getErrorQueueLength();
    log(`${ORANGE_COLOR}RETRY:${RESET_COLOR} Processing file: ${file.remotePath}. Error queue length: ${length}.`, 'info');

    const timeout = calculateDownloadTimeout(file.size);
    await downloadFile(file.remotePath, timeout);
  })).subscribe({
    error: (error) => {
      log(`${ORANGE_COLOR}RETRY:${RESET_COLOR} Worker encountered an error: ${error.message}`, 'error');
    },
    complete: async () => {
      const length = await getErrorQueueLength();
      log(`${ORANGE_COLOR}RETRY:${RESET_COLOR} Worker stopped. Error queue length: ${length}.`, 'info');
    },
  });
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
  mainQueue.complete();
  errorQueue.complete();

  await waitForActiveDownloads();
  await Promise.allSettled(runningTasks);
  log("Остановка: сохраняю состояние...", 'info');
  await persistState();
  process.exitCode = 0;
}

let runningTasks = [];

async function start() {
  await fs.mkdir(config.destinationDir, { recursive: true });

  subscribeToMainQueue();
  subscribeToErrorQueue();

  await loadState();

  const stop = async () => {
    await shutdown();
  };

  process.once("SIGINT", stop);
  process.once("SIGTERM", stop);
  if (process.send) {
    process.once("message", async (message) => {
      if (message !== "shutdown") return;
      await stop();
      process.disconnect();
    });
  }

  const discovery = discoverRemoteFiles();

  runningTasks.push(
    discovery,
    runPeriodic(writeReport, REPORT_INTERVAL_MS, "Ошибка создания отчёта"),
  );

  log(`Запущено. Сервер: ${config.url}; папка назначения: ${config.destinationDir}`, 'info');

  // wait for the main threads
  await Promise.allSettled([ discovery ]);

  if (stopping) return;
}

start().catch((error) => {
  console.error(`Не удалось запустить приложение: ${error.message}`);
  process.exitCode = 1;
});