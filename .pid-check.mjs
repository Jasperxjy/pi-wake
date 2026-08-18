import { spawn } from "node:child_process";

const sleeper = spawn(process.execPath, ["-e", "console.log(process.pid); setInterval(() => {}, 1000)"], { stdio: ["ignore", "pipe", "pipe"] });
const pid = await new Promise((resolve) => {
	sleeper.stdout.once("data", (chunk) => resolve(Number(String(chunk).trim())));
});
console.log("sleeper pid:", pid);

// Check from this process
try { process.kill(pid, 0); console.log("parent sees sleeper: ALIVE"); }
catch (e) { console.log("parent sees sleeper:", e.code); }

// Check from another child process
const checker = spawn(process.execPath, ["-e", `try { process.kill(${pid}, 0); console.log("checker sees sleeper: ALIVE"); } catch (e) { console.log("checker sees sleeper:", e.code); }`]);
checker.stdout.on("data", (c) => process.stdout.write(c));
checker.stderr.on("data", (c) => process.stdout.write(c));
await new Promise((r) => checker.on("close", r));

// Now check a definitely-dead pid
sleeper.kill("SIGKILL");
await new Promise((r) => sleeper.on("close", r));
await new Promise((r) => setTimeout(r, 300));
try { process.kill(pid, 0); console.log("after kill: ALIVE (bad!)"); }
catch (e) { console.log("after kill:", e.code); }
