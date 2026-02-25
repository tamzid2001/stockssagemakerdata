import {
  createApiClient,
  escapeHtml,
  formatRelativeTime,
  initAuth,
  normalizeTicker,
  track,
  waitForFirebase,
} from "./explore-common.js";

const refs = {
  authToggle: document.getElementById("profile-auth-toggle"),
  profileAvatar: document.getElementById("profile-avatar"),
  profileTitle: document.getElementById("profile-title"),
  profileSubtitle: document.getElementById("profile-subtitle"),
  posts: document.getElementById("profile-posts"),
  loadMore: document.getElementById("profile-load-more"),
  settingsSection: document.getElementById("notification-settings"),
  notifGlobal: document.getElementById("notif-global"),
  notifFollowing: document.getElementById("notif-following"),
  notifTickers: document.getElementById("notif-tickers"),
  notifEnableWebPush: document.getElementById("notif-enable-webpush"),
  notifSave: document.getElementById("notif-save"),
  notifStatus: document.getElementById("notif-status"),
  watchTickerInput: document.getElementById("watch-ticker-input"),
  watchTickerAdd: document.getElementById("watch-ticker-add"),
  watchTickerList: document.getElementById("watch-ticker-list"),
  followAuthorInput: document.getElementById("follow-author-input"),
  followAuthorAdd: document.getElementById("follow-author-add"),
  followAuthorList: document.getElementById("follow-author-list"),
};

const state = {
  authClient: null,
  api: null,
  user: null,
  viewedHandle: "",
  viewedUid: "",
  viewedProfile: null,
  ownProfile: false,
  cursor: null,
  loadingPosts: false,
  postMap: new Map(),
  settings: {
    notificationPrefs: {
      global: true,
      following: true,
      tickers: true,
    },
    follows: [],
    watchTickers: [],
  },
};

function setStatus(message, isError = false) {
  if (!refs.notifStatus) return;
  refs.notifStatus.textContent = message;
  refs.notifStatus.style.color = isError ? "#d83446" : "";
}

function setAuthButton() {
  if (!refs.authToggle) return;
  const user = state.user;
  refs.authToggle.textContent = !user || user.isAnonymous ? "Sign in" : "Sign out";
}

function resolveHandleFromPath() {
  const parts = window.location.pathname.split("/").filter(Boolean);
  if (parts[0] === "u" && parts[1]) return parts[1];
  return "";
}

function renderProfileHeader() {
  const profile = state.viewedProfile || {};
  const titleHandle = profile.handle || (state.ownProfile && state.user ? state.user.uid.slice(0, 8) : "profile");
  refs.profileTitle.textContent = `@${titleHandle}`;
  refs.profileSubtitle.textContent = state.ownProfile
    ? "Manage your Explore posts and notification subscriptions."
    : "Public profile posts";
  if (profile.photoURL) refs.profileAvatar.src = profile.photoURL;
}

function renderPosts(append = false) {
  if (!refs.posts) return;
  if (!append) refs.posts.innerHTML = "";

  const posts = Array.from(state.postMap.values());
  if (!posts.length) {
    refs.posts.innerHTML = `<div class="muted">No posts available.</div>`;
    refs.loadMore?.classList.add("hidden");
    return;
  }

  posts.forEach((post) => {
    if (refs.posts.querySelector(`[data-post-id="${CSS.escape(post.id)}"]`)) return;

    const row = document.createElement("article");
    row.className = "profile-post-row";
    row.dataset.postId = post.id;
    row.innerHTML = `
      <div>
        <h3>${escapeHtml(post.title || "Untitled")}</h3>
        <div class="muted">${formatRelativeTime(post.createdAtMs)} • ${escapeHtml(post.type || "post")} • ♥${Number(post.counts?.likes || 0)} · 💬${Number(post.counts?.comments || 0)} · ↻${Number(post.counts?.reposts || 0)}</div>
      </div>
      <div class="inline-controls">
        ${state.ownProfile ? `
          <select data-post-visibility="${escapeHtml(post.id)}">
            <option value="public" ${post.visibility === "public" ? "selected" : ""}>Public</option>
            <option value="unlisted" ${post.visibility === "unlisted" ? "selected" : ""}>Unlisted</option>
          </select>
          <button data-post-delete="${escapeHtml(post.id)}" type="button">Delete</button>
        ` : `<a class="link-btn" href="/explore?post=${encodeURIComponent(post.id)}">Open</a>`}
      </div>
    `;

    refs.posts.appendChild(row);
  });

  refs.loadMore?.classList.toggle("hidden", !state.cursor);
}

async function loadPosts(reset = false) {
  if (!state.api || !state.viewedUid || state.loadingPosts) return;
  state.loadingPosts = true;

  if (reset) {
    state.cursor = null;
    state.postMap.clear();
    renderPosts(false);
  }

  try {
    const params = new URLSearchParams();
    params.set("limit", "20");
    if (state.cursor) params.set("cursor", state.cursor);

    const response = await state.api.get(`/profile/${encodeURIComponent(state.viewedUid)}/posts?${params.toString()}`);
    const posts = Array.isArray(response.posts) ? response.posts : [];
    posts.forEach((post) => state.postMap.set(post.id, post));
    state.cursor = response.cursor || null;
    renderPosts(!reset);

    track("profile_posts_loaded", {
      owner: state.ownProfile,
      count: posts.length,
    });
  } catch (error) {
    if (refs.posts) refs.posts.innerHTML = `<div class="muted">${escapeHtml(error.message || "Unable to load posts.")}</div>`;
  } finally {
    state.loadingPosts = false;
  }
}

function renderSettings() {
  if (!state.ownProfile || !refs.settingsSection) {
    refs.settingsSection?.classList.add("hidden");
    return;
  }

  refs.settingsSection.classList.remove("hidden");
  refs.notifGlobal.checked = Boolean(state.settings.notificationPrefs.global);
  refs.notifFollowing.checked = Boolean(state.settings.notificationPrefs.following);
  refs.notifTickers.checked = Boolean(state.settings.notificationPrefs.tickers);

  refs.watchTickerList.innerHTML = state.settings.watchTickers
    .map(
      (ticker) => `<span class="token-chip">${escapeHtml(ticker)} <button type="button" data-watch-remove="${escapeHtml(ticker)}">×</button></span>`
    )
    .join("");

  refs.followAuthorList.innerHTML = state.settings.follows
    .map(
      (uid) => `<span class="token-chip">${escapeHtml(uid)} <button type="button" data-follow-remove="${escapeHtml(uid)}">×</button></span>`
    )
    .join("");
}

async function loadNotificationSettings() {
  if (!state.api || !state.ownProfile) return;
  try {
    const response = await state.api.get("/me/notification-settings");
    state.settings.notificationPrefs = response.notificationPrefs || state.settings.notificationPrefs;
    state.settings.follows = Array.isArray(response.follows) ? response.follows : [];
    state.settings.watchTickers = Array.isArray(response.watchTickers) ? response.watchTickers : [];
    renderSettings();
  } catch (error) {
    setStatus(error.message || "Unable to load notification settings.", true);
  }
}

async function registerWebPushToken() {
  if (!state.api) return;

  const firebase = await waitForFirebase();
  const messaging = firebase.messaging?.();
  if (!messaging) throw new Error("Messaging SDK unavailable.");
  if (!("Notification" in window)) throw new Error("This browser does not support notifications.");

  const permission = await Notification.requestPermission();
  if (permission !== "granted") throw new Error("Notification permission denied.");

  const config = await state.api.get("/notifications/config");
  const vapidPublicKey = String(config.vapidPublicKey || "").trim();
  if (!vapidPublicKey) throw new Error("Missing VAPID key configuration.");

  const registration = await navigator.serviceWorker.register("/firebase-messaging-sw.js");
  const token = await messaging.getToken({
    vapidKey: vapidPublicKey,
    serviceWorkerRegistration: registration,
  });

  if (!token) throw new Error("Unable to retrieve push token.");

  await state.api.post("/notifications/register-token", {
    token,
    platform: "web",
  });

  setStatus("Push is enabled for this browser.");
  track("profile_push_enabled", { platform: "web" });
}

async function saveNotificationPrefs() {
  if (!state.api) return;
  await state.api.post("/notifications/preferences", {
    global: Boolean(refs.notifGlobal.checked),
    following: Boolean(refs.notifFollowing.checked),
    tickers: Boolean(refs.notifTickers.checked),
  });
  setStatus("Notification preferences saved.");
  track("profile_notification_prefs_saved", {
    global: Boolean(refs.notifGlobal.checked),
    following: Boolean(refs.notifFollowing.checked),
    tickers: Boolean(refs.notifTickers.checked),
  });
}

async function resolveViewedProfile() {
  if (!state.api) return;

  state.viewedHandle = resolveHandleFromPath();
  if (state.viewedHandle) {
    const response = await state.api.get(`/profile/handle/${encodeURIComponent(state.viewedHandle)}`);
    state.viewedUid = response.uid;
    state.viewedProfile = response;
    state.ownProfile = Boolean(state.user && state.user.uid === state.viewedUid);
  } else {
    state.viewedUid = state.user?.uid || "";
    state.ownProfile = Boolean(state.user);
    state.viewedProfile = {
      uid: state.viewedUid,
      handle: state.user?.displayName || state.user?.email?.split("@")[0] || state.user?.uid?.slice(0, 8) || "profile",
      photoURL: state.user?.photoURL || "",
    };
  }

  renderProfileHeader();
  renderSettings();
}

function bindEvents() {
  refs.authToggle?.addEventListener("click", async () => {
    if (!state.authClient) return;
    try {
      if (!state.user || state.user.isAnonymous) await state.authClient.signInWithGoogle();
      else await state.authClient.signOut();
    } catch (error) {
      setStatus(error.message || "Auth action failed.", true);
    }
  });

  refs.loadMore?.addEventListener("click", async () => {
    await loadPosts(false);
  });

  refs.posts?.addEventListener("change", async (event) => {
    const select = event.target.closest("[data-post-visibility]");
    if (!select || !state.api) return;
    try {
      await state.api.patch(`/posts/${encodeURIComponent(select.dataset.postVisibility)}/visibility`, {
        visibility: select.value,
      });
      const post = state.postMap.get(select.dataset.postVisibility);
      if (post) {
        post.visibility = select.value;
        state.postMap.set(post.id, post);
      }
      track("profile_post_visibility_changed", { post_id: select.dataset.postVisibility, visibility: select.value });
    } catch (error) {
      setStatus(error.message || "Unable to update visibility.", true);
    }
  });

  refs.posts?.addEventListener("click", async (event) => {
    const deleteBtn = event.target.closest("[data-post-delete]");
    if (!deleteBtn || !state.api) return;
    if (!window.confirm("Delete this post and all related engagement?")) return;

    try {
      await state.api.delete(`/posts/${encodeURIComponent(deleteBtn.dataset.postDelete)}`);
      state.postMap.delete(deleteBtn.dataset.postDelete);
      renderPosts(false);
      track("profile_post_deleted", { post_id: deleteBtn.dataset.postDelete });
    } catch (error) {
      setStatus(error.message || "Unable to delete post.", true);
    }
  });

  refs.notifEnableWebPush?.addEventListener("click", async () => {
    try {
      setStatus("Registering web push token...");
      await registerWebPushToken();
    } catch (error) {
      setStatus(error.message || "Unable to enable push.", true);
    }
  });

  refs.notifSave?.addEventListener("click", async () => {
    try {
      await saveNotificationPrefs();
      await loadNotificationSettings();
    } catch (error) {
      setStatus(error.message || "Unable to save preferences.", true);
    }
  });

  refs.watchTickerAdd?.addEventListener("click", async () => {
    if (!state.api) return;
    const ticker = normalizeTicker(refs.watchTickerInput?.value || "");
    if (!ticker) return;
    try {
      await state.api.post(`/watch-tickers/${encodeURIComponent(ticker)}`, { watch: true });
      refs.watchTickerInput.value = "";
      await loadNotificationSettings();
      track("profile_watch_ticker_added", { ticker });
    } catch (error) {
      setStatus(error.message || "Unable to add ticker.", true);
    }
  });

  refs.followAuthorAdd?.addEventListener("click", async () => {
    if (!state.api) return;
    const authorUid = String(refs.followAuthorInput?.value || "").trim();
    if (!authorUid) return;
    try {
      await state.api.post(`/follows/${encodeURIComponent(authorUid)}`, { follow: true });
      refs.followAuthorInput.value = "";
      await loadNotificationSettings();
      track("profile_follow_author_added", { author_uid: authorUid });
    } catch (error) {
      setStatus(error.message || "Unable to follow author.", true);
    }
  });

  refs.watchTickerList?.addEventListener("click", async (event) => {
    const removeBtn = event.target.closest("[data-watch-remove]");
    if (!removeBtn || !state.api) return;
    try {
      const ticker = normalizeTicker(removeBtn.dataset.watchRemove);
      await state.api.post(`/watch-tickers/${encodeURIComponent(ticker)}`, { watch: false });
      await loadNotificationSettings();
      track("profile_watch_ticker_removed", { ticker });
    } catch (error) {
      setStatus(error.message || "Unable to remove ticker.", true);
    }
  });

  refs.followAuthorList?.addEventListener("click", async (event) => {
    const removeBtn = event.target.closest("[data-follow-remove]");
    if (!removeBtn || !state.api) return;
    try {
      const uid = String(removeBtn.dataset.followRemove || "").trim();
      await state.api.post(`/follows/${encodeURIComponent(uid)}`, { follow: false });
      await loadNotificationSettings();
      track("profile_follow_author_removed", { author_uid: uid });
    } catch (error) {
      setStatus(error.message || "Unable to remove follow.", true);
    }
  });
}

async function bootstrap() {
  state.authClient = await initAuth(async (user) => {
    state.user = user;
    setAuthButton();

    if (!state.api) return;

    try {
      await resolveViewedProfile();
      await loadPosts(true);
      if (state.ownProfile) await loadNotificationSettings();
    } catch (error) {
      if (refs.posts) refs.posts.innerHTML = `<div class="muted">${escapeHtml(error.message || "Unable to load profile.")}</div>`;
    }
  });

  state.api = await createApiClient(() => state.authClient.getAuthToken());
  bindEvents();
}

bootstrap().catch((error) => {
  if (refs.posts) refs.posts.innerHTML = `<div class="muted">${escapeHtml(error.message || "Profile initialization failed.")}</div>`;
});
