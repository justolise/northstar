/**
 * download-nats.js  — postinstall script
 *
 * Downloads the nats-server binary for the current platform if it isn't
 * already present. Called automatically by `npm install` via the
 * "postinstall" script in package.json.
 *
 * Supports: linux-amd64, linux-arm64, darwin-amd64, darwin-arm64.
 */

import { existsSync } from 'fs';
import { execSync } from 'child_process';
import { platform, arch } from 'os';

const NATS_VERSION = 'v2.10.24';
const BINARY = './nats-server';

if (existsSync(BINARY)) {
  console.log('[setup] nats-server binary already present — skipping download.');
  process.exit(0);
}

const platMap = { linux: 'linux', darwin: 'darwin', win32: 'windows' };
const archMap  = { x64: 'amd64', arm64: 'arm64', arm: 'arm6' };

const p = platMap[platform()] ?? 'linux';
const a = archMap[arch()] ?? 'amd64';

const dirName  = `nats-server-${NATS_VERSION}-${p}-${a}`;
const tarName  = `${dirName}.tar.gz`;
const url      = `https://github.com/nats-io/nats-server/releases/download/${NATS_VERSION}/${tarName}`;

console.log(`[setup] Downloading NATS server ${NATS_VERSION} (${p}/${a})...`);
console.log(`[setup] Source: ${url}`);

try {
  // Download → extract → place binary
  execSync(
    `curl -sL "${url}" -o /tmp/${tarName} && ` +
    `tar xzf /tmp/${tarName} -C /tmp && ` +
    `cp /tmp/${dirName}/nats-server . && ` +
    `chmod +x ./nats-server && ` +
    `rm -rf /tmp/${tarName} /tmp/${dirName}`,
    { stdio: 'inherit' }
  );
  console.log('[setup] ✓ nats-server ready.');
} catch (err) {
  console.error('[setup] ✗ Download failed:', err.message);
  console.error('[setup] Download manually from: https://nats.io/download/');
  console.error('[setup] Place the binary as ./nats-server and re-run npm start.');
  // Non-fatal — user can still add binary manually
}
