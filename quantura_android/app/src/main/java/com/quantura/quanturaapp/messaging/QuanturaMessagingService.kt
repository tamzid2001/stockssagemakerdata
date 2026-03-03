package com.quantura.quanturaapp.messaging

import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.PendingIntent
import android.content.Context
import android.content.Intent
import android.os.Build
import androidx.core.app.NotificationCompat
import com.google.firebase.messaging.FirebaseMessagingService
import com.google.firebase.messaging.RemoteMessage
import com.quantura.quanturaapp.MainActivity

class QuanturaMessagingService : FirebaseMessagingService() {

    override fun onNewToken(token: String) {
        super.onNewToken(token)
        QuanturaFcmTokenHolder.setToken(applicationContext, token)
    }

    override fun onMessageReceived(message: RemoteMessage) {
        super.onMessageReceived(message)
        val notification = message.notification
        val data = message.data
        val title = notification?.title ?: data["title"] ?: "Quantura"
        val body = notification?.body ?: data["body"] ?: "You have a new update."
        val url = data["url"] ?: data["path"] ?: "/dashboard"
        showNotification(title, body, url)
    }

    private fun showNotification(title: String, body: String, deepLinkPath: String) {
        val channelId = "quantura_push"
        createChannel(channelId)

        val baseUrl = "https://quantura.studio"
        val targetUrl = if (deepLinkPath.startsWith("http")) deepLinkPath else "$baseUrl${if (deepLinkPath.startsWith("/")) deepLinkPath else "/$deepLinkPath"}"

        val intent = Intent(this, MainActivity::class.java).apply {
            flags = Intent.FLAG_ACTIVITY_NEW_TASK or Intent.FLAG_ACTIVITY_CLEAR_TOP
            putExtra(EXTRA_DEEP_LINK_URL, targetUrl)
        }
        val pendingIntent = PendingIntent.getActivity(
            this,
            0,
            intent,
            PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_IMMUTABLE
        )

        val notification = NotificationCompat.Builder(this, channelId)
            .setSmallIcon(android.R.drawable.ic_popup_reminder)
            .setContentTitle(title)
            .setContentText(body)
            .setPriority(NotificationCompat.PRIORITY_HIGH)
            .setAutoCancel(true)
            .setContentIntent(pendingIntent)
            .build()

        val manager = getSystemService(Context.NOTIFICATION_SERVICE) as NotificationManager
        manager.notify(NOTIFICATION_ID, notification)
    }

    private fun createChannel(channelId: String) {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
            val channel = NotificationChannel(
                channelId,
                "Quantura Notifications",
                NotificationManager.IMPORTANCE_HIGH
            ).apply {
                description = "Quantura push notifications"
                enableVibration(true)
            }
            val manager = getSystemService(Context.NOTIFICATION_SERVICE) as NotificationManager
            manager.createNotificationChannel(channel)
        }
    }

    companion object {
        const val EXTRA_DEEP_LINK_URL = "quantura_deep_link_url"
        private const val NOTIFICATION_ID = 9001
    }
}
