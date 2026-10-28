"use strict";

const { createClient } = require("webdav");

const FILE_DOWNLOAD_HEADERS = Object.freeze({
  "User-Agent": "Yandex.Disk/3.2.51 (Windows; 10.0)",
});

function createWebDavClient({ url, username, password }) {
  return createClient(url, {
    username,
    password,
    headers: FILE_DOWNLOAD_HEADERS,
  });
}

function createWebDavClientManager({ config, log, requestTimeoutMs }) {
  let client = createWebDavClient(config);
  let lock = Promise.resolve();

  function withLock(task) {
    const result = lock.then(task);
    lock = result.catch(() => {});
    return result;
  }

  function getWebDavClient() {
    return withLock(() => client);
  }

  function recreateWebDavClient() {
    return withLock(() => {
      client = createWebDavClient(config);
      return client;
    });
  }

  async function getDirectoryContentsWithTimeout(remotePath) {
    const webDavClient = await getWebDavClient();

    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), requestTimeoutMs);
    try {
      return await webDavClient.getDirectoryContents(remotePath, { signal: controller.signal });
    } finally {
      clearTimeout(timeout);
    }
  }

  async function checkAndRecreateWebDavClient() {
    try {
      log(`Проверяю состояние клиента`, "warn");
      await getDirectoryContentsWithTimeout(config.remotePath);
      log(`Клиент в рабочем состоянии`, "warn");
    } catch (error) {
      log(`ERROR: ${error.message}`, "error");
      log(`Ошибка при проверке клиента, пересоздаю.`, "warn");
      await recreateWebDavClient();
    }
  }

  return { getWebDavClient, getDirectoryContentsWithTimeout, checkAndRecreateWebDavClient };
}

module.exports = { createWebDavClient, createWebDavClientManager };
