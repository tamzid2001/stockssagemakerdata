/* Loaded only when Help is opened. Conversation lives in memory, never storage/analytics. */
(() => {
  "use strict";
  let dialog, list, input, status, submit, controller, generation = 0;
  let messages = [];
  function element(tag, text, className) {
    const node = document.createElement(tag); if (text) node.textContent = text;
    if (className) node.className = className; return node;
  }
  function addMessage(role, text, references = []) {
    const row = element("section", "", `support-message support-message-${role}`);
    row.append(element("strong", role === "user" ? "You" : "Quantura assistant"), element("p", text));
    if (references.length) {
      const links = element("div", "", "support-references");
      for (const reference of references) {
        // Defense in depth: no arbitrary model-produced URLs or HTML.
        try {
          const url = new URL(reference.url);
          if (url.protocol !== "https:" || !["quantura.studio", "quantura.mintlify.app"].includes(url.hostname) || url.username || url.password) continue;
          const link = element("a", reference.title); link.href = url.href;
          link.target = "_blank"; link.rel = "noopener noreferrer"; links.append(link);
        } catch { /* Invalid links are never rendered. */ }
      }
      row.append(links);
    }
    list.append(row); list.scrollTop = list.scrollHeight;
  }
  function clearChat() {
    generation++; controller?.abort(); controller = null; messages = []; list.replaceChildren();
    status.textContent = ""; input.value = ""; submit.disabled = false;
    addMessage("assistant", "How can I help with Q Forecast, workspaces, CSVs or API access? I can explain the product, but cannot inspect your account or change it.");
  }
  async function send(event) {
    event.preventDefault();
    const question = input.value.trim(); if (!question || controller) return;
    const user = window.firebase?.auth?.()?.currentUser;
    if (!user || user.isAnonymous) {
      status.textContent = "Sign in using the website's Sign in button to chat. Documentation and Contact remain available below.";
      return;
    }
    if (/(?:qnt_live_|sk-|hf_|mint_)[A-Za-z0-9_\-]{12,}|-----BEGIN [A-Z ]*PRIVATE KEY-----|Bearer\s+\S{20,}/i.test(question)) {
      status.textContent = "Please remove credentials before sending. Never share passwords, API keys or private data."; return;
    }
    const turn = generation;
    controller = new AbortController(); const request = controller;
    const timeout = setTimeout(() => request.abort(), 65000);
    submit.disabled = true; status.textContent = "Preparing an answer…";
    const outgoing = [...messages.slice(-8), { role: "user", content: question }];
    const userRowIndex = list.childElementCount;
    addMessage("user", question); input.value = "";
    try {
      const token = await user.getIdToken();
      const response = await fetch("/api/support/chat", { method: "POST", credentials: "same-origin", signal: request.signal,
        headers: { "Content-Type": "application/json", Authorization: `Bearer ${token}` }, body: JSON.stringify({ messages: outgoing }) });
      const payload = await response.json().catch(() => ({}));
      if (turn !== generation) return;
      if (!response.ok) throw new Error([401,422,429,503].includes(response.status) ? payload.error?.message || "Support is unavailable. Please retry." : "Support is unavailable. Please retry or contact us.");
      if (typeof payload.data?.answer !== "string" || !Array.isArray(payload.data?.references)) throw new Error("The reply could not be displayed. Please retry.");
      messages = [...outgoing, { role: "assistant", content: payload.data.answer }];
      addMessage("assistant", payload.data.answer, payload.data.references);
      status.textContent = "Answer based on Quantura product guidance. AI can make mistakes.";
    } catch (error) {
      if (turn !== generation) return;
      status.textContent = error.name === "AbortError" ? "The request timed out. Please retry or contact support." : error.message;
      list.children[userRowIndex]?.remove(); input.value = question;
    } finally {
      clearTimeout(timeout);
      if (turn === generation) { controller = null; submit.disabled = false; if (dialog.open) input.focus(); }
    }
  }
  function createDialog() {
    dialog = element("dialog", "", "support-dialog");
    dialog.id = "quantura-support"; dialog.setAttribute("aria-labelledby", "support-title");
    dialog.setAttribute("data-cs-mask", ""); dialog.setAttribute("data-cs-exclude", "");
    const header = element("header", "", "support-header");
    const heading = element("div"); const title = element("h2", "Quantura Support"); title.id = "support-title";
    heading.append(title, element("p", "AI assistant · GPT-5.6 Luna", "small"));
    const close = element("button", "Close", "cta secondary small"); close.type = "button";
    close.setAttribute("aria-label", "Close support assistant"); close.addEventListener("click", () => dialog.close());
    header.append(heading, close);
    list = element("div", "", "support-messages"); list.setAttribute("role", "log");
    list.setAttribute("aria-live", "polite"); list.setAttribute("aria-label", "Support conversation");
    const form = element("form", "", "support-form");
    const label = element("label", "Your product question"); label.htmlFor = "support-question";
    input = element("textarea"); input.id = "support-question"; input.rows = 2; input.maxLength = 1000;
    input.placeholder = "How do I download my workspace CSVs?";
    input.setAttribute("autocomplete", "off"); input.setAttribute("aria-describedby", "support-privacy");
    input.addEventListener("keydown", event => { if (event.key === "Enter" && !event.shiftKey && !event.isComposing) { event.preventDefault(); form.requestSubmit(); } });
    const actions = element("div", "", "support-actions");
    const clear = element("button", "Clear chat", "cta secondary small"); clear.type = "button"; clear.addEventListener("click", clearChat);
    submit = element("button", "Send question", "cta small"); submit.type = "submit";
    actions.append(clear, submit); form.append(label, input, actions); form.addEventListener("submit", send);
    status = element("p", "", "support-status small"); status.setAttribute("role", "status");
    const privacy = element("p", "Messages are sent to OpenAI when you press Send. Do not include secrets or private data.", "support-privacy small"); privacy.id = "support-privacy";
    const footer = element("footer", "", "support-footer");
    for (const [text, href] of [["Documentation", "https://quantura.mintlify.app/"], ["Contact support", "/contact"], ["Privacy", "/privacy"]]) {
      const link = element("a", text); link.href = href; footer.append(link);
    }
    dialog.append(header, list, form, status, privacy, footer); document.body.append(dialog); clearChat();
    // Closing/minimizing preserves the current conversation; Clear chat removes it.
    // A different signed-in identity must never inherit a previous user's messages.
    window.firebase?.auth?.()?.onAuthStateChanged?.(() => { if (dialog) clearChat(); });
  }
  window.QuanturaSupport = Object.freeze({ open(launcher) {
    if (!dialog) createDialog();
    if (!dialog.open) dialog.showModal();
    dialog.onclose = () => launcher?.focus(); input.focus();
  } });
})();
