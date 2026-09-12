// Runs only inside Actions. Online backups are authenticated-encrypted before
// upload. Keep the newest two checkpoints; delete only our own superseded ones.
import { DefaultArtifactClient } from '@actions/artifact';
import { spawn } from 'node:child_process';
import { mkdtemp, rm, appendFile, access } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';

const artifact = new DefaultArtifactClient();
const root = process.env.QUANTURA_RESEARCH_DIR;
const retained = [];
let sequence = 0;
let publishing;
function command(args) {
  return new Promise((resolve, reject) => {
    const child = spawn('python', args, { stdio: 'inherit' });
    child.on('error', reject);
    child.on('exit', code => code === 0 ? resolve() : reject(new Error('RESEARCH_PROCESS_FAILED')));
  });
}
async function checkpoint() {
  try { await access(path.join(root, 'research.sqlite3')); } catch { return; }
  const temporary = await mkdtemp(path.join(os.tmpdir(), 'quantura-checkpoint-'));
  const file = path.join(temporary, 'research.qra.enc');
  try {
    await command(['-m', 'market_research.artifact', 'checkpoint', '--source', root, '--output', file]);
    const name = `replay-checkpoint-${process.env.GITHUB_RUN_ID}-${process.env.GITHUB_RUN_ATTEMPT}-${++sequence}`;
    const uploaded = await artifact.uploadArtifact(name, [file], temporary, { retentionDays: 3, compressionLevel: 0 });
    if (!uploaded.id) throw new Error('CHECKPOINT_UPLOAD_FAILED');
    retained.push(name);
    await appendFile(process.env.GITHUB_OUTPUT, `checkpoint_artifact_id=${uploaded.id}\n`);
    await appendFile(process.env.GITHUB_STEP_SUMMARY, `\nEncrypted recoverable checkpoint: artifact **${uploaded.id}** (${name}); 3-day retention.\n`);
    // Latest two verified uploads retained; never remove another run's data.
    if (retained.length > 2) await artifact.deleteArtifact(retained.shift());
  } finally { await rm(temporary, { recursive: true, force: true }); }
}
function publish() {
  if (!publishing) publishing = checkpoint().finally(() => { publishing = undefined; });
  return publishing;
}
const timer = setInterval(() => publish().catch(() => console.error('CHECKPOINT_UPLOAD_FAILED; local checkpoint retained')), 45 * 60 * 1000);
const firstCheckpoint = setTimeout(() => publish().catch(() => console.error('INITIAL_CHECKPOINT_UPLOAD_FAILED')), 60 * 1000);
let failure;
try { await command(['-m', 'market_research.historical', ...process.argv.slice(2)]); }
catch (error) { failure = error; }
finally {
  clearInterval(timer);
  clearTimeout(firstCheckpoint);
  if (publishing) await publishing;
  await publish();
}
if (failure) process.exitCode = 1;
