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
  profileName: document.getElementById("profile-name"),
  profileHandle: document.getElementById("profile-handle"),
  profileEmail: document.getElementById("profile-email"),
  profileVerifiedBadge: document.getElementById("profile-verified-badge"),
  profileShareButton: document.getElementById("profile-share-button"),
  profilePrivacySection: document.getElementById("profile-privacy-section"),
  profilePublicToggle: document.getElementById("profile-public-toggle"),
  profileEmailOptIn: document.getElementById("profile-email-optin"),
  profilePrivacySave: document.getElementById("profile-privacy-save"),
  profilePrivacyStatus: document.getElementById("profile-privacy-status"),
  posts: document.getElementById("profile-posts"),
  loadMore: document.getElementById("profile-load-more"),
  savedFoldersSection: document.getElementById("saved-folders-section"),
  savedFolderName: document.getElementById("saved-folder-name"),
  savedFolderCreate: document.getElementById("saved-folder-create"),
  savedFolderList: document.getElementById("saved-folder-list"),
  savedSearchForm: document.getElementById("saved-search-form"),
  savedSearchInput: document.getElementById("saved-search-input"),
  savedSearchResults: document.getElementById("saved-search-results"),
  savedFolderItemsTitle: document.getElementById("saved-folder-items-title"),
  savedFolderItems: document.getElementById("saved-folder-items"),
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
  folders: [],
  activeFolderId: "",
  folderItems: [],
  searchResults: [],
};

function setStatus(message, isError = false) {
  if (!refs.notifStatus) return;
  refs.notifStatus.textContent = message;
  refs.notifStatus.style.color = isError ? "#d83446" : "";
}

function setPrivacyStatus(message, isError = false) {
  if (!refs.profilePrivacyStatus) return;
  refs.profilePrivacyStatus.textContent = message;
  refs.profilePrivacyStatus.style.color = isError ? "#d83446" : "";
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

function currentProfileUrl() {
  const handle = String(state.viewedProfile?.handle || "").trim();
  if (!handle) return "";
  return `${window.location.origin}/u/${encodeURIComponent(handle)}`;
}

async function shareProfile() {
  const url = currentProfileUrl();
  if (!url) return;
  const title = state.viewedProfile?.name
    ? `${state.viewedProfile.name} on Quantura`
    : `@${state.viewedProfile?.handle || "profile"} on Quantura`;
  try {
    if (navigator.share) {
      await navigator.share({ title, url, text: "Quantura public profile" });
    } else if (navigator.clipboard?.writeText) {
      await navigator.clipboard.writeText(url);
      window.alert("Profile link copied.");
    }
  } catch {
    // Ignore cancelled share actions.
  }
}

function renderProfileHeader() {
  const profile = state.viewedProfile || {};
  const handle = String(profile.handle || "").trim() || "profile";

  if (refs.profileTitle) refs.profileTitle.childNodes[0].textContent = `@${handle} `;
  if (refs.profileSubtitle) {
    refs.profileSubtitle.textContent = state.ownProfile
      ? "Manage your public profile, posts, and saved folders."
      : "Public profile posts";
  }
  if (refs.profileName) refs.profileName.textContent = profile.name || "-";
  if (refs.profileHandle) refs.profileHandle.textContent = `@${handle}`;
  if (refs.profileEmail) refs.profileEmail.textContent = profile.emailVisible ? profile.email || "-" : "Hidden";
  if (refs.profileAvatar) refs.profileAvatar.src = profile.photoURL || "/assets/quantura-icon.svg";
  refs.profileVerifiedBadge?.classList.toggle("hidden", !profile.verified);
  refs.profilePrivacySection?.classList.toggle("hidden", !state.ownProfile);
  refs.savedFoldersSection?.classList.toggle("hidden", !state.ownProfile);

  if (state.ownProfile) {
    if (refs.profilePublicToggle) refs.profilePublicToggle.checked = Boolean(profile.publicProfile);
    if (refs.profileEmailOptIn) refs.profileEmailOptIn.checked = Boolean(profile.publicEmailOptIn);
  }
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

function renderSavedFolderList() {
  if (!refs.savedFolderList) return;
  refs.savedFolderList.innerHTML = "";
  if (!state.folders.length) {
    refs.savedFolderList.innerHTML = `<span class="muted">No folders yet.</span>`;
    return;
  }

  state.folders.forEach((folder) => {
    const button = document.createElement("button");
    button.type = "button";
    button.className = `token-chip ${state.activeFolderId === folder.id ? "active" : ""}`;
    button.dataset.folderSelect = folder.id;
    button.innerHTML = `${escapeHtml(folder.name)} <span>(${Number(folder.itemCount || 0)})</span>`;
    refs.savedFolderList.appendChild(button);
  });
}

function renderSavedSearchResults() {
  if (!refs.savedSearchResults) return;
  refs.savedSearchResults.innerHTML = "";
  if (!state.searchResults.length) {
    refs.savedSearchResults.innerHTML = `<div class="muted">No saved assets matched this query.</div>`;
    return;
  }

  const activeFolder = state.folders.find((folder) => folder.id === state.activeFolderId);
  const canSaveToFolder = Boolean(activeFolder && !activeFolder.isSystem);

  state.searchResults.forEach((item) => {
    const row = document.createElement("article");
    row.className = "saved-item-row";
    row.innerHTML = `
      <div class="saved-item-title">${escapeHtml(item.title || "Untitled")}</div>
      <div class="saved-item-meta">${escapeHtml(item.itemType || "item")}${item.ticker ? ` • ${escapeHtml(item.ticker)}` : ""}</div>
      ${item.subtitle ? `<div class="muted">${escapeHtml(item.subtitle)}</div>` : ""}
      <div class="inline-controls">
        <a class="link-btn" href="${escapeHtml(item.targetUrl || "/")}">Open</a>
        ${canSaveToFolder ? `<button type="button" data-save-item-type="${escapeHtml(item.itemType)}" data-save-source-id="${escapeHtml(item.sourceId)}">Save to folder</button>` : ""}
      </div>
    `;
    refs.savedSearchResults.appendChild(row);
  });
}

function renderFolderItems() {
  if (!refs.savedFolderItems) return;
  refs.savedFolderItems.innerHTML = "";
  const activeFolder = state.folders.find((folder) => folder.id === state.activeFolderId);
  if (refs.savedFolderItemsTitle) {
    refs.savedFolderItemsTitle.textContent = activeFolder ? `${activeFolder.name} items` : "Folder items";
  }

  if (!state.folderItems.length) {
    refs.savedFolderItems.innerHTML = `<div class="muted">No items in this folder.</div>`;
    return;
  }

  state.folderItems.forEach((item) => {
    const row = document.createElement("article");
    row.className = "saved-item-row";
    row.innerHTML = `
      <div class="saved-item-title">${escapeHtml(item.title || "Untitled")}</div>
      <div class="saved-item-meta">${escapeHtml(item.itemType || "item")}${item.ticker ? ` • ${escapeHtml(item.ticker)}` : ""}</div>
      ${item.subtitle ? `<div class="muted">${escapeHtml(item.subtitle)}</div>` : ""}
      <div class="inline-controls">
        <a class="link-btn" href="${escapeHtml(item.targetUrl || "/")}">Open</a>
        ${activeFolder && !activeFolder.isSystem ? `<button type="button" data-remove-item-type="${escapeHtml(item.itemType)}" data-remove-source-id="${escapeHtml(item.sourceId)}">Remove</button>` : ""}
      </div>
    `;
    refs.savedFolderItems.appendChild(row);
  });
}

async function loadFolders() {
  if (!state.api || !state.ownProfile) return;
  const response = await state.api.get("/saved/folders");
  state.folders = Array.isArray(response.folders) ? response.folders : [];
  if (!state.activeFolderId || !state.folders.some((folder) => folder.id === state.activeFolderId)) {
    const firstCustom = state.folders.find((folder) => !folder.isSystem);
    state.activeFolderId = (firstCustom || state.folders[0] || {}).id || "";
  }
  renderSavedFolderList();
  await loadActiveFolderItems();
}

async function loadActiveFolderItems() {
  if (!state.api || !state.ownProfile || !state.activeFolderId) {
    state.folderItems = [];
    renderFolderItems();
    return;
  }
  const response = await state.api.get(`/saved/folders/${encodeURIComponent(state.activeFolderId)}/items?limit=40`);
  state.folderItems = Array.isArray(response.items) ? response.items : [];
  renderFolderItems();
}

async function loadSavedSearch(query = "") {
  if (!state.api || !state.ownProfile) return;
  const url = `/saved/search?q=${encodeURIComponent(query)}&limit=40`;
  const response = await state.api.get(url);
  state.searchResults = Array.isArray(response.items) ? response.items : [];
  renderSavedSearchResults();
}

async function saveItemToActiveFolder(itemType, sourceId) {
  if (!state.api || !state.ownProfile || !state.activeFolderId) return;
  const folder = state.folders.find((row) => row.id === state.activeFolderId);
  if (!folder || folder.isSystem) {
    window.alert("Select a custom folder first.");
    return;
  }

  await state.api.post(`/saved/folders/${encodeURIComponent(state.activeFolderId)}/items`, {
    itemType,
    sourceId,
  });
  await loadFolders();
  track("profile_folder_item_saved", { item_type: itemType });
}

async function removeFolderItem(itemType, sourceId) {
  if (!state.api || !state.ownProfile || !state.activeFolderId) return;
  await state.api.delete(
    `/saved/folders/${encodeURIComponent(state.activeFolderId)}/items/${encodeURIComponent(itemType)}/${encodeURIComponent(sourceId)}`
  );
  await loadFolders();
  track("profile_folder_item_removed", { item_type: itemType });
}

async function createFolder() {
  if (!state.api || !state.ownProfile) return;
  const name = String(refs.savedFolderName?.value || "").trim();
  if (!name) return;
  await state.api.post("/saved/folders", { name });
  if (refs.savedFolderName) refs.savedFolderName.value = "";
  await loadFolders();
  track("profile_folder_created", { name });
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
    .map((ticker) => `<span class="token-chip">${escapeHtml(ticker)} <button type="button" data-watch-remove="${escapeHtml(ticker)}">×</button></span>`)
    .join("");

  refs.followAuthorList.innerHTML = state.settings.follows
    .map((uid) => `<span class="token-chip">${escapeHtml(uid)} <button type="button" data-follow-remove="${escapeHtml(uid)}">×</button></span>`)
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
    state.ownProfile = Boolean(state.user && !state.user.isAnonymous && state.user.uid === state.viewedUid);
  } else if (state.user && !state.user.isAnonymous) {
    const response = await state.api.get("/me/profile");
    state.viewedUid = response.uid;
    state.viewedProfile = response;
    state.ownProfile = true;
  } else {
    state.viewedUid = "";
    state.ownProfile = false;
    state.viewedProfile = {
      uid: "",
      handle: "guest",
      name: "Guest",
      emailVisible: false,
      photoURL: "",
      verified: false,
      publicProfile: false,
      publicEmailOptIn: false,
    };
  }

  renderProfileHeader();
  renderSettings();
}

async function saveProfileVisibility() {
  if (!state.api || !state.ownProfile) return;
  setPrivacyStatus("Saving visibility settings...");
  try {
    const response = await state.api.patch("/me/profile", {
      publicProfile: Boolean(refs.profilePublicToggle?.checked),
      publicEmailOptIn: Boolean(refs.profileEmailOptIn?.checked),
    });
    state.viewedProfile = response.profile || state.viewedProfile;
    renderProfileHeader();
    setPrivacyStatus("Visibility settings saved.");
    track("profile_visibility_saved", {
      public_profile: Boolean(refs.profilePublicToggle?.checked),
      public_email: Boolean(refs.profileEmailOptIn?.checked),
    });
  } catch (error) {
    setPrivacyStatus(error.message || "Unable to save visibility settings.", true);
  }
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

  refs.profileShareButton?.addEventListener("click", async () => {
    await shareProfile();
    track("profile_shared", { handle: state.viewedProfile?.handle || "" });
  });

  refs.profilePrivacySave?.addEventListener("click", async () => {
    await saveProfileVisibility();
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

  refs.savedFolderCreate?.addEventListener("click", async () => {
    try {
      await createFolder();
    } catch (error) {
      window.alert(error.message || "Unable to create folder.");
    }
  });

  refs.savedFolderList?.addEventListener("click", async (event) => {
    const button = event.target.closest("[data-folder-select]");
    if (!button) return;
    state.activeFolderId = button.dataset.folderSelect;
    renderSavedFolderList();
    await loadActiveFolderItems();
  });

  refs.savedSearchForm?.addEventListener("submit", async (event) => {
    event.preventDefault();
    try {
      await loadSavedSearch(String(refs.savedSearchInput?.value || "").trim());
    } catch (error) {
      window.alert(error.message || "Unable to search saved assets.");
    }
  });

  refs.savedSearchResults?.addEventListener("click", async (event) => {
    const button = event.target.closest("[data-save-item-type][data-save-source-id]");
    if (!button) return;
    try {
      await saveItemToActiveFolder(button.dataset.saveItemType, button.dataset.saveSourceId);
    } catch (error) {
      window.alert(error.message || "Unable to save item to folder.");
    }
  });

  refs.savedFolderItems?.addEventListener("click", async (event) => {
    const button = event.target.closest("[data-remove-item-type][data-remove-source-id]");
    if (!button) return;
    try {
      await removeFolderItem(button.dataset.removeItemType, button.dataset.removeSourceId);
    } catch (error) {
      window.alert(error.message || "Unable to remove folder item.");
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
      if (state.viewedUid) await loadPosts(true);
      if (state.ownProfile) {
        await loadNotificationSettings();
        await loadFolders();
        await loadSavedSearch("");
      }
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
