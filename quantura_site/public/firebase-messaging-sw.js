/* global importScripts, firebase, self, clients */

importScripts("/__/firebase/12.9.0/firebase-app-compat.js");
importScripts("/__/firebase/12.9.0/firebase-messaging-compat.js");
importScripts("/__/firebase/init.js?useEmulator=false");

let messaging = null;
try {
  messaging = firebase.messaging();
} catch (error) {
  messaging = null;
}

if (messaging) {
  messaging.onBackgroundMessage((payload) => {
    const notification = payload?.notification || {};
    const title = notification.title || "Quantura update";
    const options = {
      body: notification.body || "You have a new Quantura update.",
      icon: notification.icon || "/assets/quantura-icon.svg",
      badge: "/assets/quantura-icon.svg",
      data: payload?.data || {},
      tag: notification.tag || "quantura-webpush",
    };
    clients
      .matchAll({ type: "window", includeUncontrolled: true })
      .then((windowClients) => {
        windowClients.forEach((client) => {
          client.postMessage({
            type: "quantura_push_background",
            title,
            body: options.body,
            data: payload?.data || {},
          });
        });
      })
      .catch(() => {
        // Ignore client-post failures.
      });
    self.registration.showNotification(title, options);
  });
}

self.addEventListener("notificationclick", (event) => {
  event.notification.close();
  const targetUrl = event.notification?.data?.url || "/dashboard";
  event.waitUntil(clients.openWindow(targetUrl));
});
