"use strict";

const fs = require("node:fs/promises");
const path = require("node:path");
const crypto = require("node:crypto");
const { loadavg } = require("node:os");

function calculateDownloadTimeout(fileSizeInBytes) {
  if (!fileSizeInBytes) {
    return null;
  }

  const minSpeedMbps = 2;
  const extraReserveMs = 5000;

  // (Mbps * 1024 * 1024) / 8 bits = bytes per second
  const speedBytesPerSecond = (minSpeedMbps * 1024 * 1024) / 8;
  const downloadTimeSeconds = fileSizeInBytes / speedBytesPerSecond;
  const timeoutMs = (downloadTimeSeconds * 1000) + extraReserveMs;

  return Math.round(timeoutMs);
}

function createDownloader({
  config,
  fileStatus,
  log,
  maxRetries,
  stateStore,
  webDavClientManager,
  enqueueDownloadRetry,
  isStopping,
}) {
  const { state, withStateLock, updateState } = stateStore;
  const { getWebDavClient, checkAndRecreateWebDavClient } = webDavClientManager;
  const downloadWaiters = new Set();
  let activeDownloads = 0;

  function waitForActiveDownloads() {
    if (activeDownloads === 0)
      return Promise.resolve();
    return new Promise((resolve) => downloadWaiters.add(resolve));
  }

  async function getFileContents(remotePath, timeout) {
    const webDavClient = await getWebDavClient();
    try {
      return await webDavClient.getFileContents(remotePath, {
        format: "binary",
        timeout,
      });
    } catch (error) {
      if (error.status && error.status === 503) {
        // for the 503 set ERROR status for the file
        throw error;
      }

      // trying to recreate the WebDav client if the error is not a 503
      await checkAndRecreateWebDavClient();
      const newClient = await getWebDavClient();
      return await newClient.getFileContents(remotePath, {
        format: "binary",
        timeout,
      });
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

  async function performDownload(remotePath, timeout) {
    let file = null;
    await withStateLock(() => {
      file = state.files[remotePath];
    });

    if (!file) return;

    const timeout2 = timeout || 60_000;
    log(`Начало скачивания: ${remotePath}. Таймаут: ${(timeout2 / 1000).toFixed(2)} сек.`, "trace");

    await updateState(() => {
      file.status = fileStatus.DOWNLOADING;
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
        file.status = fileStatus.COMPLETED;
        file.downloadedAt = new Date().toISOString();
        file.nextRetryAt = null;
      });

      log(`Скачан: ${remotePath}`, "trace");
    } catch (error) {
      log(`Ошибка ${remotePath}: ${error.message}.`, "error");

      await updateState(() => {
        file.status = fileStatus.ERROR;
        file.attempts = (file.attempts || 0) + 1;
      });

      // append the file to the error queue if it hasn't reached the max retries yet
      if (file.attempts < maxRetries) {
        enqueueDownloadRetry(file);
      }
    }
  }

  async function downloadFile(remotePath, timeout) {
    if (isStopping())
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

  return { downloadFile, waitForActiveDownloads };
}

module.exports = { createDownloader, calculateDownloadTimeout };
