"use strict";

const fs = require("node:fs/promises");
const path = require("node:path");
const { Subject, concatMap, mergeMap } = require("rxjs");
const { writeReport } = require("./src/report");
const { createStateStore } = require("./src/state");
const { createDiscovery } = require("./src/discovery");
const { createDownloader, calculateDownloadTimeout } = require("./src/downloader");
const { createWebDavClientManager } = require("./src/webdavClient");
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

const webDavClientManager = createWebDavClientManager({ config, log, requestTimeoutMs: DIRECTORY_REQUEST_TIMEOUT_MS });

const stateStore = createStateStore({ statePath: config.statePath, log });
const { state, withStateLock, writeStateFile, persistState, updateState, loadStateFile } = stateStore;
let stopping = false;

const { discoverRemoteFiles } = createDiscovery({
  config,
  fileStatus: FILE_STATUS,
  listDelayMs: DIRECTORY_LIST_DELAY_MS,
  log,
  sleep,
  stateStore,
  webDavClientManager,
  enqueueFileDownload,
  isStopping: () => stopping,
  onComplete: () => mainQueue.complete(),
});

const { downloadFile, waitForActiveDownloads } = createDownloader({
  config,
  fileStatus: FILE_STATUS,
  log,
  maxRetries: MAX_FILE_RETRIES,
  stateStore,
  webDavClientManager,
  enqueueDownloadRetry,
  isStopping: () => stopping,
});

const mainQueue = new Subject();
let mainQueueLength = 0;
const errorQueue = new Subject();
let errorQueueLength = 0;

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

async function loadState() {
  log(`Загружаем состояние...`, 'trace');
  await loadStateFile();

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
    runPeriodic(
      () => writeReport({ state, destinationDir: config.destinationDir, fileStatus: FILE_STATUS, log }),
      REPORT_INTERVAL_MS, "Ошибка создания отчёта"),
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