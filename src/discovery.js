"use strict";

const path = require("node:path");

function isRateLimitError(error) {
  return error?.status === 429 || error?.response?.status === 429;
}

function metadataChanged(previous, remoteFile) {
  if (!previous) return false;
  if (previous.etag && remoteFile.etag) return previous.etag !== remoteFile.etag;
  if (previous.size !== null && remoteFile.size !== null && previous.size !== remoteFile.size) {
    return true;
  }
  return Boolean(previous.lastmod && remoteFile.lastmod && previous.lastmod !== remoteFile.lastmod);
}

function createDiscovery({
  config,
  fileStatus,
  listDelayMs,
  log,
  sleep,
  stateStore,
  webDavClientManager,
  enqueueFileDownload,
  isStopping,
  onComplete,
}) {
  const { state, withStateLock, writeStateFile, updateState } = stateStore;
  const { getDirectoryContentsWithTimeout, checkAndRecreateWebDavClient } = webDavClientManager;
  let discoveryRunning = false;

  async function getDirectoryContents(remotePath, retry = 0) {
    try {
      return await getDirectoryContentsWithTimeout(remotePath);
    } catch (error) {
      if (isRateLimitError(error) && retry < 3) {
        await sleep(listDelayMs);
        return await getDirectoryContents(remotePath, retry + 1);
      }

      log(`Ошибка запроса каталога ${remotePath}: ${error.message}. Проверяю подключение к этому же каталогу.`, "error");

      await checkAndRecreateWebDavClient();
      return await getDirectoryContentsWithTimeout(remotePath);
    }
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
      status: fileStatus.PENDING,
      children: []
    };

    if (folder.status === fileStatus.COMPLETED) return;

    // Await only if server request needed
    if (folder.status != fileStatus.DOWNLOADING) {
      await sleep(listDelayMs);
    }

    const entries = folder.status == fileStatus.DOWNLOADING
      ? folder.children.map(childPath => ({ type: "directory", filename: childPath })) // only folders
      : await getDirectoryContents(folder.path);

    await withStateLock(() => {
      folder.status = fileStatus.DOWNLOADING;
      state.folders[remotePath] = folder;
    });

    log(`directory contents for ${folder.path}: ${entries.length} entries`, "info");

    const childrenFolders = entries.filter(entry => entry.type === "directory").map(entry => {
      const entryPath = getEntryPath(entry);
      return entryPath.endsWith("/") ? entryPath : `${entryPath}/`;
    });

    const childrenFiles = entries.filter(entry => entry.type === "file").map(entry => {
      const entryPath = getEntryPath(entry);
      const parsedSize = Number(entry.size);
      return {
        remotePath: entryPath,
        size: Number.isFinite(parsedSize) && parsedSize >= 0 ? parsedSize : null,
        etag: entry.etag || null,
        lastmod: entry.lastmod || null,
      };
    });

    log(`children folders for ${folder.path}: ${childrenFolders.length} entries`, "trace");

    await updateState(() => {
      for (const child of childrenFolders) {
        if (!folder.children.includes(child)) folder.children.push(child);
      }
      log(`children updated for ${folder.path}`, "trace");
    });

    // Write folders
    await writeStateFile();

    log(`process files of ${folder.path}`, "trace");
    for (const file of childrenFiles) {
      await onFile(file);
    }

    // Write files
    await writeStateFile();

    for (const child of folder.children) {
      log(`process next child for ${folder.path}: ${child}`, "trace");
      await listRemoteFiles(child, onFile);
    }

    await updateState(() => {
      folder.status = fileStatus.COMPLETED;
      log(`status completed for ${folder.path}`, "trace");
    });
  }

  async function discoverRemoteFiles() {
    if (discoveryRunning || isStopping()) return;
    discoveryRunning = true;
    try {
      await listRemoteFiles(config.remotePath, (remoteFile) => withStateLock(async () => {
        const { remotePath } = remoteFile;
        const previous = state.files[remotePath];

        if (!previous) {
          state.files[remotePath] = {
            ...remoteFile,
            status: fileStatus.PENDING,
            attempts: 0,
            nextRetryAt: null,
          };
          await enqueueFileDownload(remotePath);
        } else if (previous.status === fileStatus.COMPLETED) {
          return;
        } else if (metadataChanged(previous, remoteFile)) {
          if (previous.status === fileStatus.DOWNLOADING) return;
          Object.assign(previous, remoteFile, {
            status: fileStatus.PENDING,
            attempts: 0,
            nextRetryAt: null,
          });
          await enqueueFileDownload(remotePath);
        } else if (Object.entries(remoteFile).some(([key, value]) => previous[key] !== value)) {
          Object.assign(previous, remoteFile);
        }
      }));
    } catch (error) {
      log(`Ошибка сканирования WebDAV: ${error.message}`, "error");
      if (isRateLimitError(error)) {
        log("Получен HTTP 429; клиент не пересоздаётся.", "warn");
      }
    } finally {
      discoveryRunning = false;
      onComplete();
      log("=> Сканирование завершено.", "info");
    }
  }

  return { discoverRemoteFiles };
}

module.exports = { createDiscovery };
