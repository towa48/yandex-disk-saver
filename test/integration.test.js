"use strict";

const assert = require("node:assert/strict");
const crypto = require("node:crypto");
const fs = require("node:fs/promises");
const http = require("node:http");
const os = require("node:os");
const path = require("node:path");
const { spawn } = require("node:child_process");
const { test } = require("node:test");

test("downloads files, retries server errors, saves state, and stops promptly", async (t) => {
  const temporaryDirectory = await fs.mkdtemp(path.join(os.tmpdir(), "webdav-downloader-"));
  const failedRequests = [];
  let corruptFileRequests = 0;
  let child;
  const md5 = (contents) => crypto.createHash("md5").update(contents).digest("hex");

  const response = (href, properties) =>
    `<d:response><d:href>${href}</d:href><d:propstat><d:prop>${properties}</d:prop><d:status>HTTP/1.1 200 OK</d:status></d:propstat></d:response>`;
  const multistatus = (responses) =>
    `<?xml version="1.0"?><d:multistatus xmlns:d="DAV:">${responses.join("")}</d:multistatus>`;

  const server = http.createServer((request, responseStream) => {
    if (request.method === "PROPFIND") {
      responseStream.writeHead(207, { "Content-Type": "application/xml" });
      if (request.url === "/") {
        responseStream.end(multistatus([
          response("/", "<d:resourcetype><d:collection/></d:resourcetype>"),
          response("/folder/", "<d:resourcetype><d:collection/></d:resourcetype>"),
        ]));
      } else {
        responseStream.end(multistatus([
          response("/folder/ok.txt", `<d:resourcetype/><d:getcontentlength>2</d:getcontentlength><d:getetag>${md5("ok")}</d:getetag>`),
          response("/folder/fail.txt", "<d:resourcetype/><d:getcontentlength>4</d:getcontentlength><d:getetag>fail-v1</d:getetag>"),
          response("/folder/corrupt.txt", `<d:resourcetype/><d:getcontentlength>4</d:getcontentlength><d:getetag>${md5("good")}</d:getetag>`),
        ]));
      }
      return;
    }

    if (request.method === "GET" && request.url.endsWith("/corrupt.txt")) {
      corruptFileRequests += 1;
      const contents = corruptFileRequests === 1 ? "bad!" : "good";
      responseStream.writeHead(200, { "Content-Length": String(Buffer.byteLength(contents)) });
      responseStream.end(contents);
      return;
    }

    if (request.method === "GET" && request.url.endsWith("/fail.txt")) {
      failedRequests.push(Date.now());
      responseStream.writeHead(500);
      responseStream.end("intentional failure");
      return;
    }

    if (request.method === "GET") {
      responseStream.writeHead(200, { "Content-Length": "2" });
      responseStream.end("ok");
      return;
    }

    responseStream.writeHead(405).end();
  });

  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));

  try {
    const port = server.address().port;
    child = spawn(process.execPath, [path.resolve(__dirname, "..", "index.js")], {
      cwd: path.resolve(__dirname, ".."),
      env: {
        ...process.env,
        WEBDAV_PROTOCOL: "http",
        WEBDAV_HOST: "127.0.0.1",
        WEBDAV_PORT: String(port),
        WEBDAV_REMOTE_PATH: "/",
        DESTINATION_DIR: path.join(temporaryDirectory, "downloads"),
        STATE_FILE: path.join(temporaryDirectory, "state.json"),
      },
      stdio: ["ignore", "pipe", "pipe"],
    });

    let output = "";
    child.stdout.on("data", (chunk) => { output += chunk; });
    child.stderr.on("data", (chunk) => { output += chunk; });

    const deadline = Date.now() + 18_000;
    while (Date.now() < deadline && (failedRequests.length < 2 || corruptFileRequests < 2)) {
      if (child.exitCode !== null) throw new Error(`Приложение завершилось раньше времени: ${output}`);
      await new Promise((resolve) => setTimeout(resolve, 100));
    }

    assert.ok(failedRequests.length >= 2, `Повторная попытка не выполнена: ${output}`);
    assert.ok(corruptFileRequests >= 2, `Повторная загрузка после ошибки MD5 не выполнена: ${output}`);
    assert.ok(failedRequests[1] - failedRequests[0] >= 9_500, "Повтор должен начаться примерно через 10 секунд");
    assert.equal(await fs.readFile(path.join(temporaryDirectory, "downloads", "folder", "ok.txt"), "utf8"), "ok");
    assert.equal(await fs.readFile(path.join(temporaryDirectory, "downloads", "folder", "corrupt.txt"), "utf8"), "good");

    const startedAt = Date.now();
    const childExit = new Promise((resolve) => child.once("exit", resolve));
    child.kill("SIGINT");
    let shutdownTimer;
    await Promise.race([
      childExit,
      new Promise((_, reject) => {
        shutdownTimer = setTimeout(() => reject(new Error("Остановка приложения заняла больше 4 секунд")), 4_000);
      }),
    ]);
    clearTimeout(shutdownTimer);
    assert.ok(Date.now() - startedAt < 4_000);

    const saved = JSON.parse(await fs.readFile(path.join(temporaryDirectory, "state.json"), "utf8"));
    assert.equal(saved.files["/folder/ok.txt"].status, "completed");
    assert.equal(saved.files["/folder/fail.txt"].status, "error");
    assert.ok(saved.files["/folder/fail.txt"].attempts >= 2);
    assert.equal(saved.files["/folder/corrupt.txt"].etag, md5("good"));
    assert.equal(saved.files["/folder/corrupt.txt"].integrityFailures, 1);
    assert.equal(saved.files["/folder/corrupt.txt"].status, "completed");

    const report = await fs.readFile(path.join(temporaryDirectory, "downloads", "download-report.html"), "utf8");
    assert.match(report, /Папка \(до 2 уровней\)/);
    assert.match(report, /Общий размер/);
  } finally {
    if (child && child.exitCode === null) {
      child.kill("SIGKILL");
      await new Promise((resolve) => child.once("exit", resolve));
    }
    const serverClosed = new Promise((resolve) => server.close(resolve));
    server.closeAllConnections?.();
    await serverClosed;
    await fs.rm(temporaryDirectory, { recursive: true, force: true });
  }
});