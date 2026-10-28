"use strict";

const fs = require("node:fs/promises");
const path = require("node:path");

function createStateStore({ statePath, log }) {
  const state = { version: 1, files: {}, folders: {} };
  let stateLock = Promise.resolve();

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
      await fs.mkdir(path.dirname(statePath), { recursive: true });
      const temporaryPath = `${statePath}.tmp`;
      await fs.writeFile(temporaryPath, snapshot, "utf8");
      await fs.rename(temporaryPath, statePath);
    } catch (error) {
      log(`Не удалось сохранить состояние: ${error.message}`, "error");
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

  async function loadStateFile() {
    try {
      const saved = JSON.parse(await fs.readFile(statePath, "utf8"));
      await withStateLock(() => {
        if (saved && saved.files && typeof saved.files === "object") {
          state.files = saved.files;
          state.version = saved.version || 1;
        }
        if (saved && saved.folders && typeof saved.folders === "object") {
          const folders = saved.folders;

          // fix duplicate folder entries
          for (const folderKey in folders) {
            const entry = folders[folderKey];
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
        log(`Не удалось загрузить состояние: ${error.message}. Будет создано новое.`, "error");
      }
    }
  }

  return { state, withStateLock, writeStateFile, persistState, updateState, loadStateFile };
}

module.exports = { createStateStore };
