/**
 * Builds the standalone `pi-wake-daemon` bin.
 *
 * The extension itself is loaded by Pi through jiti and ships as TypeScript. But
 * the daemon bin runs under plain `node`, which refuses type stripping for files
 * under `node_modules` (`ERR_UNSUPPORTED_NODE_MODULES_TYPE_STRIPPING`). It must
 * therefore ship as plain ESM JavaScript. The daemon's import graph is entirely
 * local (`./core.ts`, `./presence.ts`, `./lock.ts`, `./runtime.ts`), so this
 * script emits exactly those five files:
 *
 *   1. copy the sources to a project-local staging dir with relative `.ts`
 *      specifiers rewritten to `.js` (Node's native type stripping needs `.ts`
 *      specifiers; emitted ESM needs `.js`),
 *   2. run the project's own `tsc` on the staging dir (verbatimModuleSyntax off,
 *      so type-only imports/specifiers are elided instead of preserved),
 *   3. move the emitted `.js` files into `dist/`.
 */
import { cp, mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { spawnSync } from "node:child_process";
import path from "node:path";
import { fileURLToPath } from "node:url";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const srcDir = path.join(root, "extensions", "pi-wake");
const outDir = path.join(root, "dist");
const stageDir = path.join(root, ".build-staging");
const FILES = ["core.ts", "presence.ts", "lock.ts", "runtime.ts", "daemon.ts"];

const tscBin = path.join(root, "node_modules", ".bin", process.platform === "win32" ? "tsc.cmd" : "tsc");

await rm(stageDir, { recursive: true, force: true });
await rm(outDir, { recursive: true, force: true });
await mkdir(path.join(stageDir, "src"), { recursive: true });

for (const file of FILES) {
	let text = await readFile(path.join(srcDir, file), "utf8");
	// Local relative imports are written as ./X.ts for Node's native type
	// stripping; the emitted ESM must reference the .js files.
	text = text
		.replace(/(from\s+["']\.[^"']*?)\.ts(["'])/g, "$1.js$2")
		.replace(/(import\s*\(\s*["']\.[^"']*?)\.ts(["'])/g, "$1.js$2");
	await writeFile(path.join(stageDir, "src", file), text);
}

await writeFile(path.join(stageDir, "tsconfig.json"), JSON.stringify({
	compilerOptions: {
		module: "nodenext",
		moduleResolution: "nodenext",
		target: "es2023",
		strict: true,
		erasableSyntaxOnly: true,
		skipLibCheck: true,
		types: ["node"],
		outDir: "out",
		rootDir: "src",
		noEmit: false,
		declaration: false,
		sourceMap: false,
	},
	include: ["src"],
}));

// Run the compiler via its JS entry so no shell is involved (tsc.cmd needs a
// shell on Windows and unescaped args are a deprecation hazard).
const tscEntry = path.join(root, "node_modules", "typescript", "lib", "tsc.js");
const result = spawnSync(process.execPath, [tscEntry, "-p", path.join(stageDir, "tsconfig.json")], { encoding: "utf8" });
if (result.status !== 0) {
	await rm(stageDir, { recursive: true, force: true });
	throw new Error(`tsc build failed:\n${result.stdout}\n${result.stderr}`);
}

await mkdir(outDir, { recursive: true });
for (const file of FILES) {
	const out = path.join(outDir, file.replace(/\.ts$/, ".js"));
	await cp(path.join(stageDir, "out", file.replace(/\.ts$/, ".js")), out);
	const emitted = await readFile(out, "utf8");
	if (emitted.includes("import type ") || /from\s+["']\.[^"']*\.ts["']/.test(emitted) || /(^|[^A-Za-z])\btype\s+[A-Z][A-Za-z]*\s*=\s*[^=]/.test(emitted)) {
		throw new Error(`build output for ${file} still contains TypeScript-only syntax`);
	}
}
await rm(stageDir, { recursive: true, force: true });

console.log(`built dist: ${FILES.map((file) => file.replace(/\.ts$/, ".js")).join(", ")}`);
