/* Public-page measurement uses the same optional consent choice as GA4. */
(() => {
  if (window.QuanturaAds) return;
  const PIXEL_ID = 'U9kvJUnMS74Aq4mMBS2n7u'; // Public identifier, never an API key.
  const publicPage = () => /^(\/(?:about|pricing|contact|shop|blog)?\/?|\/blog\/[a-z0-9-]+\/?|\/shop\/[a-z0-9-]+\/?)$/i.test(location.pathname);
  const accepted = () => {
    try { return localStorage.getItem('quantura_cookie_consent') === 'accepted' && navigator.globalPrivacyControl !== true; }
    catch { return false; }
  };
  let initialized = false, viewed = false, ziftLoaded = false;
  const sent = new Set();
  const rawCookie = name => {
    const prefix = name + '=';
    const part = document.cookie.split(';').map(s => s.trim()).find(s => s.startsWith(prefix));
    return part ? part.slice(prefix.length) : ''; // Attribution is opaque: do not decode.
  };
  function loadScript(src, id) {
    if (document.getElementById(id)) return;
    const script = document.createElement('script');
    script.id = id; script.async = true; script.fetchPriority = 'low'; script.src = src;
    script.onerror = () => {};
    document.head.append(script);
  }
  function sync() {
    try {
      const allowed = publicPage() && accepted();
      if (!allowed) {
        if (initialized) window.oaiq('consent', false);
        // Zift has no documented consent API. A reload ends the loaded tag;
        // the saved preference prevents it from loading on the new document.
        if (ziftLoaded) location.reload();
        return;
      }
      if (!window.oaiq) {
        const q = function() { q.q.push(arguments); }; q.q = []; window.oaiq = q;
      }
      window.oaiq('consent', true);
      if (!initialized) {
        window.oaiq('init', { pixelId: PIXEL_ID, debug: false });
        initialized = true;
        loadScript('https://bzrcdn.openai.com/sdk/oaiq.min.js', 'quantura-openai-pixel');
      }
      if (!viewed) {
        window.oaiq('measure', 'page_viewed', { type: 'contents' }, { opt_out: true });
        viewed = true;
      }
      // Zift is limited to public URLs without potentially sensitive parameters.
      if (!ziftLoaded && !location.search && !location.hash) {
        loadScript('https://static.ziftsolutions.com/analytics/8a998abda0eb6fc001a0ef8c62c1053d.js', 'quantura-aws-marketplace-analytics');
        ziftLoaded = true;
      }
    } catch { /* Optional integrations fail independently of core features. */ }
  }
  function capture() {
    try {
      if (!publicPage() || !accepted()) return null;
      return { consent: 'granted', sourceUrl: location.origin + location.pathname,
        oppref: rawCookie('__oppref'), obref: rawCookie('__obref') };
    } catch { return null; }
  }
  function leadCreated(eventId) {
    try {
      if (!accepted() || !publicPage() || !/^lead_[A-Za-z0-9_-]{1,100}$/.test(eventId) || sent.has(eventId)) return;
      sync();
      window.oaiq('measure', 'lead_created', { type: 'customer_action' }, { event_id: eventId, opt_out: true });
      sent.add(eventId);
    } catch { /* A completed contact request remains successful. */ }
  }
  window.QuanturaAds = Object.freeze({ capture, leadCreated });
  document.addEventListener('quantura:consent-change', sync);
  window.addEventListener('storage', e => { if (e.key === 'quantura_cookie_consent') sync(); });
  function start() {
    const footer = document.querySelector('footer .footer-grid > div');
    if (footer && !document.getElementById('quantura-cookie-preferences')) {
      const button = document.createElement('button');
      button.id = 'quantura-cookie-preferences'; button.type = 'button';
      button.className = 'cta secondary small'; button.textContent = 'Cookie preferences';
      button.addEventListener('click', () => document.dispatchEvent(new Event('quantura:open-cookie-preferences')));
      footer.append(button);
    }
    sync();
  }
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', start, { once: true });
  else start();
})();
