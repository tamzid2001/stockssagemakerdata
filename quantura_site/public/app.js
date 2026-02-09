(() => {
  const ADMIN_EMAIL = "tamzid257@gmail.com";
  const STRIPE_URL = "https://buy.stripe.com/8x24gze7a86K1zE9M20co08";
  const PRODUCT_DEFAULTS = {
    name: "Deep Forecast",
    price: 349,
    currency: "USD",
  };

  const ui = {
    headerAuth: document.getElementById("header-auth"),
    headerSignOut: document.getElementById("header-signout"),
    headerDashboard: document.getElementById("header-dashboard"),
    purchaseBtn: document.getElementById("purchase-btn"),
    purchaseNote: document.getElementById("purchase-note"),
    purchaseSuccess: document.getElementById("purchase-success"),
    stripeLink: document.getElementById("stripe-link"),
    emailForm: document.getElementById("email-auth-form"),
    emailInput: document.getElementById("auth-email"),
    passwordInput: document.getElementById("auth-password"),
    emailSignin: document.getElementById("email-signin"),
    emailCreate: document.getElementById("email-create"),
    emailMessage: document.getElementById("auth-email-message"),
    googleSignin: document.getElementById("google-signin"),
    userEmail: document.getElementById("user-email"),
    userProvider: document.getElementById("user-provider"),
    userStatus: document.getElementById("user-status"),
    dashboardCta: document.getElementById("dashboard-cta"),
    userOrders: document.getElementById("user-orders"),
    adminSection: document.getElementById("admin"),
    adminOrders: document.getElementById("admin-orders"),
    toast: document.getElementById("toast"),
  };

  const state = {
    user: null,
    unsubscribeOrders: null,
    unsubscribeAdmin: null,
  };

  const showToast = (message, variant = "default") => {
    if (!ui.toast) return;
    ui.toast.textContent = message;
    ui.toast.dataset.variant = variant;
    ui.toast.classList.add("show");
    window.clearTimeout(ui.toast._timeout);
    ui.toast._timeout = window.setTimeout(() => {
      ui.toast.classList.remove("show");
    }, 3200);
  };

  const setPurchaseState = (user) => {
    if (!ui.purchaseBtn || !ui.purchaseNote) return;
    if (user) {
      ui.purchaseBtn.disabled = false;
      ui.purchaseBtn.textContent = "Request Deep Forecast";
      ui.purchaseNote.textContent = "Orders appear in your dashboard instantly.";
    } else {
      ui.purchaseBtn.disabled = true;
      ui.purchaseBtn.textContent = "Sign in to purchase";
      ui.purchaseNote.textContent = "You must sign in to purchase. We use Firebase Auth to secure checkout.";
      ui.stripeLink?.classList.add("hidden");
      ui.purchaseSuccess?.classList.add("hidden");
    }
  };

  const setAuthUi = (user) => {
    const isAuthed = Boolean(user);
    ui.headerAuth?.classList.toggle("hidden", isAuthed);
    ui.headerSignOut?.classList.toggle("hidden", !isAuthed);
    ui.headerDashboard?.classList.toggle("hidden", !isAuthed);

    ui.userEmail.textContent = user?.email || "Not signed in";
    ui.userProvider.textContent = user?.providerData?.[0]?.providerId || "—";
    ui.userStatus.textContent = isAuthed ? "Member" : "Guest";
    ui.userStatus.classList.toggle("pill", true);
    ui.dashboardCta.classList.toggle("hidden", isAuthed);

    setPurchaseState(user);
  };

  const formatTimestamp = (value) => {
    if (!value) return "Processing";
    if (value.toDate) {
      return value.toDate().toLocaleString();
    }
    return new Date(value).toLocaleString();
  };

  const renderOrderList = (orders, container, opts = {}) => {
    if (!container) return;
    container.innerHTML = "";
    if (!orders.length) {
      container.innerHTML = "<p class=\"small\">No orders yet. Your Deep Forecast request will appear here.</p>";
      return;
    }

    orders.forEach((order) => {
      const card = document.createElement("div");
      card.className = "order-card";
      card.dataset.orderId = order.id;

      const status = order.status || "pending";
      const statusLabel = status.replace("_", " ");

      const adminTools = opts.admin
        ? `
          <div class="order-actions">
            <select class="status-select">
              ${["pending", "in_progress", "fulfilled", "cancelled"]
                .map((option) => `
                  <option value="${option}" ${option === status ? "selected" : ""}>
                    ${option.replace("_", " ")}
                  </option>
                `)
                .join("")}
            </select>
            <input class="input notes-input" placeholder="Fulfillment notes" value="${order.fulfillmentNotes || ""}" />
            <button class="cta small update-status" type="button">Update</button>
          </div>
        `
        : "";

      card.innerHTML = `
        <div class="order-header">
          <div>
            <div class="order-title">${order.product || PRODUCT_DEFAULTS.name}</div>
            <div class="small">Order ID: ${order.id}</div>
          </div>
          <span class="status ${status}">${statusLabel}</span>
        </div>
        <div class="order-meta">
          <div><strong>Requested</strong> ${formatTimestamp(order.createdAt)}</div>
          <div><strong>Price</strong> $${order.price || PRODUCT_DEFAULTS.price} ${order.currency || "USD"}</div>
          ${opts.admin ? `<div><strong>Client</strong> ${order.userEmail || "—"}</div>` : ""}
        </div>
        ${adminTools}
      `;

      container.appendChild(card);
    });
  };

  const startUserOrders = (db, user) => {
    if (state.unsubscribeOrders) state.unsubscribeOrders();
    if (!user) return;

    state.unsubscribeOrders = db
      .collection("orders")
      .where("userId", "==", user.uid)
      .orderBy("createdAt", "desc")
      .onSnapshot((snapshot) => {
        const orders = snapshot.docs.map((doc) => ({ id: doc.id, ...doc.data() }));
        renderOrderList(orders, ui.userOrders);
      });
  };

  const startAdminOrders = (db) => {
    if (state.unsubscribeAdmin) state.unsubscribeAdmin();
    state.unsubscribeAdmin = db
      .collection("orders")
      .orderBy("createdAt", "desc")
      .limit(100)
      .onSnapshot((snapshot) => {
        const orders = snapshot.docs.map((doc) => ({ id: doc.id, ...doc.data() }));
        renderOrderList(orders, ui.adminOrders, { admin: true });
      });
  };

  const ensureUserProfile = async (db, user) => {
    if (!user) return;
    const userRef = db.collection("users").doc(user.uid);
    await userRef.set(
      {
        email: user.email,
        name: user.displayName || "",
        provider: user.providerData?.[0]?.providerId || "email",
        updatedAt: firebase.firestore.FieldValue.serverTimestamp(),
      },
      { merge: true }
    );
  };

  const init = () => {
    if (typeof firebase === "undefined") {
      console.error("Firebase SDK not loaded.");
      return;
    }

    const auth = firebase.auth();
    const db = firebase.firestore();
    const functions = firebase.functions();

    auth.setPersistence(firebase.auth.Auth.Persistence.LOCAL).catch(() => {
      showToast("Unable to set auth persistence.", "warn");
    });

    ui.headerAuth?.addEventListener("click", () => {
      document.getElementById("auth")?.scrollIntoView({ behavior: "smooth" });
    });

    ui.headerSignOut?.addEventListener("click", async () => {
      await auth.signOut();
      showToast("Signed out.");
    });

    ui.emailForm?.addEventListener("submit", async (event) => {
      event.preventDefault();
      ui.emailMessage.textContent = "";
      try {
        await auth.signInWithEmailAndPassword(ui.emailInput.value, ui.passwordInput.value);
        showToast("Signed in successfully.");
      } catch (error) {
        ui.emailMessage.textContent = error.message;
      }
    });

    ui.emailCreate?.addEventListener("click", async () => {
      ui.emailMessage.textContent = "";
      try {
        await auth.createUserWithEmailAndPassword(ui.emailInput.value, ui.passwordInput.value);
        showToast("Account created.");
      } catch (error) {
        ui.emailMessage.textContent = error.message;
      }
    });

    ui.googleSignin?.addEventListener("click", async () => {
      ui.emailMessage.textContent = "";
      try {
        const provider = new firebase.auth.GoogleAuthProvider();
        await auth.signInWithPopup(provider);
        showToast("Signed in with Google.");
      } catch (error) {
        ui.emailMessage.textContent = error.message;
      }
    });

    ui.purchaseBtn?.addEventListener("click", async () => {
      if (!state.user) {
        showToast("Sign in to continue.", "warn");
        document.getElementById("auth")?.scrollIntoView({ behavior: "smooth" });
        return;
      }

      ui.purchaseBtn.disabled = true;
      ui.purchaseBtn.textContent = "Creating order...";

      try {
        const createOrder = functions.httpsCallable("create_order");
        const result = await createOrder({
          product: PRODUCT_DEFAULTS.name,
          price: PRODUCT_DEFAULTS.price,
          currency: PRODUCT_DEFAULTS.currency,
        });
        const orderId = result.data?.orderId;
        ui.purchaseSuccess.textContent = `Order ${orderId} created. Proceed to payment to finalize.`;
        ui.purchaseSuccess.classList.remove("hidden");
        ui.stripeLink.classList.remove("hidden");
        showToast("Order created. Proceed to payment.");
      } catch (error) {
        showToast(error.message || "Unable to create order.", "warn");
      } finally {
        ui.purchaseBtn.disabled = false;
        ui.purchaseBtn.textContent = "Request Deep Forecast";
      }
    });

    ui.stripeLink?.addEventListener("click", () => {
      window.open(STRIPE_URL, "_blank", "noreferrer");
    });

    ui.adminOrders?.addEventListener("click", async (event) => {
      const button = event.target.closest(".update-status");
      if (!button) return;
      const card = button.closest(".order-card");
      if (!card) return;

      const orderId = card.dataset.orderId;
      const statusSelect = card.querySelector(".status-select");
      const notesInput = card.querySelector(".notes-input");
      if (!orderId || !statusSelect) return;

      button.disabled = true;
      button.textContent = "Updating...";

      try {
        const updateOrder = functions.httpsCallable("update_order_status");
        await updateOrder({
          orderId,
          status: statusSelect.value,
          notes: notesInput?.value || "",
        });
        showToast("Order updated.");
      } catch (error) {
        showToast(error.message || "Unable to update order.", "warn");
      } finally {
        button.disabled = false;
        button.textContent = "Update";
      }
    });

    auth.onAuthStateChanged(async (user) => {
      state.user = user;
      setAuthUi(user);

      if (!user) {
        renderOrderList([], ui.userOrders);
        ui.adminSection.classList.add("hidden");
        if (state.unsubscribeOrders) state.unsubscribeOrders();
        if (state.unsubscribeAdmin) state.unsubscribeAdmin();
        return;
      }

      await ensureUserProfile(db, user);
      startUserOrders(db, user);

      if (user.email === ADMIN_EMAIL) {
        ui.adminSection.classList.remove("hidden");
        startAdminOrders(db);
      } else {
        ui.adminSection.classList.add("hidden");
        if (state.unsubscribeAdmin) state.unsubscribeAdmin();
      }
    });
  };

  window.addEventListener("load", init);
})();
