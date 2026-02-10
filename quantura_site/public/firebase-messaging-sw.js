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
    self.registration.showNotification(title, options);
  });
}

self.addEventListener("notificationclick", (event) => {
  event.notification.close();
  const targetUrl = event.notification?.data?.url || "/dashboard";
  event.waitUntil(clients.openWindow(targetUrl));
});
