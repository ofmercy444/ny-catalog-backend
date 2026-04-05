require("dotenv").config();
const { spawn } = require("child_process");

const LOOP_DELAY_MS = Number(process.env.LOOP_DELAY_MS || 1200000); // 20 min
const FAILURE_BACKOFF_MS = Number(process.env.FAILURE_BACKOFF_MS || 900000); // 15 min

let stopping = false;

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function runCrawlerOnce() {
  return new Promise((resolve) => {
    const startedAt = Date.now();
    console.log(`[scheduler] crawl start ${new Date(startedAt).toISOString()}`);

    const child = spawn(process.execPath, ["crawler.js"], {
      stdio: "inherit",
      env: process.env,
    });

    child.on("exit", (code, signal) => {
      const elapsed = Date.now() - startedAt;
      console.log(
        `[scheduler] crawl end code=${code} signal=${signal || "none"} elapsed_ms=${elapsed}`
      );
      resolve(code === 0);
    });

    child.on("error", (err) => {
      console.error("[scheduler] failed to start crawler:", err);
      resolve(false);
    });
  });
}

async function main() {
  console.log("[scheduler] loop started");
  while (!stopping) {
    const ok = await runCrawlerOnce();
    if (stopping) break;

    if (ok) {
      await sleep(LOOP_DELAY_MS);
    } else {
      await sleep(FAILURE_BACKOFF_MS);
    }
  }
  console.log("[scheduler] loop stopped");
}

process.on("SIGINT", () => {
  stopping = true;
});
process.on("SIGTERM", () => {
  stopping = true;
});

main().catch((err) => {
  console.error("[scheduler] fatal error:", err);
  process.exit(1);
});