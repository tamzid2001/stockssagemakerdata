const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const {JSDOM} = require('jsdom');
const root = path.resolve(__dirname, '../..');

test('one accessible full-screen backtest builder follows the primary forecast action in both templates', () => {
  for (const folder of ['pages', 'functions_ssr/templates']) {
    const dom = new JSDOM(fs.readFileSync(path.join(root, folder, 'forecasting.html'), 'utf8'));
    const doc = dom.window.document;
    const button = doc.getElementById('backtest-open');
    const modal = doc.getElementById('backtest-dialog');
    assert.equal(button?.getAttribute('type'), 'button');
    assert.equal(button?.getAttribute('aria-controls'), 'backtest-dialog');
    assert.ok(button?.closest('.ensemble-primary-actions'));
    assert.equal(modal?.tagName, 'DIALOG');
    assert.equal(modal?.getAttribute('aria-labelledby'), 'backtest-title');
    assert.ok(modal?.querySelector('#backtest-form #backtest-run'));
    assert.ok(modal?.querySelector('#backtest-history-phase'));
    assert.ok(modal?.querySelector('#backtest-strategy-download'));
    dom.window.close();
  }
});

test('builder loads only on demand and does not send live orders', () => {
  const app = fs.readFileSync(path.join(root, 'public/app.js'), 'utf8');
  const builder = fs.readFileSync(path.join(root, 'public/backtest-builder.js'), 'utf8');
  const css = fs.readFileSync(path.join(root, 'public/professional.css'), 'utf8');
  assert.match(app, /import\("\/backtest-builder\.js\?v=/);
  assert.doesNotMatch(builder, /portfolio\/events\/orders|sendOrder|placeOrder/);
  assert.match(builder, /\/api\/v1\/backtests/);
  assert.match(css, /\.backtest-dialog\s*\{[^}]*100dvh/s);
});

test('forecast header uses browser locale and has no notifications or language control', () => {
  const app = fs.readFileSync(path.join(root, 'public/app.js'), 'utf8');
  for (const folder of ['pages', 'functions_ssr/templates']) {
    const dom = new JSDOM(fs.readFileSync(path.join(root, folder, 'forecasting.html'), 'utf8'));
    assert.equal(dom.window.document.querySelector('#language-select'), null);
    assert.equal(dom.window.document.querySelector('#header-notifications'), null);
    dom.window.close();
  }
  assert.match(app, /applyLanguagePreference\("auto", \{ persist: false \}\)/);
  assert.doesNotMatch(app, /ensureHeaderNotificationsCta/);
});

test('modal stacks quantile rules and queues the selected forecast through authenticated API bridge', async () => {
  const dom = new JSDOM(fs.readFileSync(path.join(root, 'pages/forecasting.html'), 'utf8'), { url: 'https://quantura.studio/forecasting' });
  const previousWindow = global.window, previousDocument = global.document;
  global.window = dom.window;
  global.document = dom.window.document;
  dom.window.HTMLDialogElement.prototype.showModal = function () { this.open = true; };
  dom.window.HTMLDialogElement.prototype.close = function () { this.open = false; };
  let submitted;
  dom.window.QuanturaBacktestBridge = {
    ensureSession: async () => {}, workspaceId: () => 'ws_test',
    source: () => ({ type: 'ticker', symbol: 'SPY', provider: 'auto' }),
    request: async (path, options) => {
      if (path.startsWith('/api/v1/ensemble-forecasts/models')) return { data: { models: [
        { id: 'prophet', name: 'Meta Prophet', available: true, default_weight: 1,
          minimum_observed_context: 2, quantile_support: { type: 'requested' } },
      ] } };
      if (path === '/api/v1/backtests') {
        submitted = { path, options };
        return { data: { backtest_id: 'bt_test', status: 'queued' } };
      }
      return { data: { id: 'bt_test', status: 'completed', provider: 'alpaca', fill_model: 'next_observed_bar_open',
        metrics: { forecast_windows: 2, matched_forecast_bars: 20, observed_bars: 148, trades: 0, wins: 0, losses: 0,
          win_rate_pct: null, net_pnl: 0, return_pct: 0, max_drawdown: 0, fees: 0,
          longest_winning_streak: 0, longest_losing_streak: 0 }, forecast_windows: [],
        trades: [], equity_curve: [{ timestamp: '2026-09-01T00:00:00Z', equity: 1000 }] } };
    },
  };
  try {
    const module = await import(`file://${path.join(root, 'public/backtest-builder.js')}`);
    module.openBacktest();
    assert.equal(dom.window.document.getElementById('backtest-dialog').open, true);
    await new Promise(resolve => setTimeout(resolve, 10));
    dom.window.document.querySelector('[data-add-backtest-rule="trailing_stop"]').click();
    assert.equal(dom.window.document.querySelectorAll('.backtest-rule').length, 4);
    dom.window.document.getElementById('backtest-frequency').value = '1Hour';
    dom.window.document.getElementById('backtest-end').value = '2026-09-01T15:30';
    dom.window.document.getElementById('backtest-form').dispatchEvent(new dom.window.Event('submit', { cancelable: true }));
    await new Promise(resolve => setTimeout(resolve, 30));
    assert.equal(submitted.path, '/api/v1/backtests');
    assert.equal(submitted.options.method, 'POST');
    assert.equal(submitted.options.body.source.symbol, 'SPY');
    assert.equal(submitted.options.body.source.frequency, '1Hour');
    assert.equal(submitted.options.body.strategy.schema_version, 2);
    assert.equal(submitted.options.body.strategy.type, 'quantile_rules');
    assert.deepEqual(submitted.options.body.strategy.rules.map(rule => rule.kind),
      ['entry', 'take_profit', 'stop_loss', 'trailing_stop']);
    assert.equal(submitted.options.body.forecast.models.prophet.weight, 1);
    assert.equal(submitted.options.body.replay.evaluation_windows, 2);
    assert.equal(submitted.options.body.workspace_id, 'ws_test');
    assert.match(dom.window.document.getElementById('backtest-status').textContent, /2 walk-forward forecasts/);
  } finally {
    global.window = previousWindow;
    global.document = previousDocument;
    dom.window.close();
  }
});
