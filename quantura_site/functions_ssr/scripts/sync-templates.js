const fs = require("fs/promises");
const path = require("path");

const sourceRoot = path.join(__dirname, "..", "..", "pages");
const destRoot = path.join(__dirname, "..", "templates");
const PROFILE_MARKER = "<!-- TERMINAL_PROFILE_PANEL -->";

const extractDashboardPanel = (html, name) => {
  const start = new RegExp(`<section\\b[^>]*\\bdata-panel="${name}"[^>]*>`, "i").exec(html);
  if (!start) throw new Error(`Missing archived account panel: ${name}`);
  const tags = /<\/?section\b[^>]*>/gi;
  tags.lastIndex = start.index;
  let depth = 0;
  for (let match = tags.exec(html); match; match = tags.exec(html)) {
    depth += /^<\/section/i.test(match[0]) ? -1 : 1;
    if (depth === 0) {
      return html.slice(start.index, tags.lastIndex).replace(
        /^<section\b[^>]*>/i,
        `<section class="terminal-profile-content${name === "auth" ? " auth-section" : ""}" id="${name}">`,
      )
        .replace("Sign in to unlock your dashboard", "Sign in to save your work")
        .replace("surface your order status in the dashboard.", "show your order status in Profile.")
        .replace("Switching workspaces reloads the research and tasks shared with that team.", "Switching workspaces reloads research shared with that team.");
    }
  }
  throw new Error(`Unclosed archived account panel: ${name}`);
};

const buildTerminalProfilePanel = (dashboardHtml) => {
  const groups = [
    { name: "auth", label: "Sign in or create an account", guest: true, open: true },
    { name: "profile", label: "Account settings", open: true },
    { name: "orders", label: "Orders and forecast requests" },
    { name: "collaboration", label: "Workspaces and collaborators" },
    { name: "developer", label: "API keys and developer access" },
  ];
  const sections = groups.map(({ name, label, guest, open }) => `
    <details class="terminal-profile-group" id="terminal-profile-${name}"${guest ? " data-profile-guest" : " data-profile-account hidden"}${open ? " open" : ""}>
      <summary>${label}</summary>
      ${extractDashboardPanel(dashboardHtml, name)}
    </details>`).join("");
  return `<!-- BEGIN_TERMINAL_PROFILE_PANEL -->
  <section class="panel hidden terminal-profile-panel" data-panel="profile" aria-labelledby="terminal-profile-title">
    <div class="terminal-profile-heading"><div class="eyebrow">Your account</div><h2 id="terminal-profile-title">Profile</h2><p class="small muted">Manage your account, requests, workspaces, and API access in Terminal.</p></div>
    ${sections}
    <div class="terminal-profile-alerts hidden" data-profile-account><a href="/screener#saved-alerts"><i class="iconoir-bell-notification" aria-hidden="true"></i><span>Saved alerts and notifications</span></a></div>
  </section>
  <!-- END_TERMINAL_PROFILE_PANEL -->`;
};

const walk = async (dir) => {
  const entries = await fs.readdir(dir, { withFileTypes: true });
  const out = [];
  for (const entry of entries) {
    const full = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      out.push(...(await walk(full)));
    } else {
      out.push(full);
    }
  }
  return out;
};

const main = async () => {
  // Overlay canonical generated pages. A developer's untracked template files
  // are not build output we own and must never be removed by synchronization.
  // Production deploys start from a clean git archive, not this local directory.
  await fs.mkdir(destRoot, { recursive: true });

  const files = await walk(sourceRoot);
  const dashboardHtml = await fs.readFile(path.join(sourceRoot, "dashboard.html"), "utf8");
  let copied = 0;
  for (const file of files) {
    if (!file.endsWith(".html")) continue;
    const rel = path.relative(sourceRoot, file);
    // Ignore editor/OS duplicate files such as "page 2.html". Only canonical
    // source pages belong in the deployable SSR template tree.
    if (/\s\d+\.html$/i.test(rel)) continue;
    const dest = path.join(destRoot, rel);
    await fs.mkdir(path.dirname(dest), { recursive: true });
    if (rel === "forecasting.html") {
      const source = await fs.readFile(file, "utf8");
      if (!source.includes(PROFILE_MARKER)) throw new Error("Forecasting template has no profile insertion marker");
      await fs.writeFile(dest, source.replace(PROFILE_MARKER, buildTerminalProfilePanel(dashboardHtml)));
    } else {
      await fs.copyFile(file, dest);
    }
    copied += 1;
  }

  // eslint-disable-next-line no-console
  console.log(`Quantura SSR: synced ${copied} HTML templates into ${destRoot}`);
};

if (require.main === module) main().catch((err) => {
  // eslint-disable-next-line no-console
  console.error(err);
  process.exitCode = 1;
});

module.exports = { buildTerminalProfilePanel, extractDashboardPanel };
