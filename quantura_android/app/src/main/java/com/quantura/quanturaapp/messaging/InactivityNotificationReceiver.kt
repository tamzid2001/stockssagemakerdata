package com.quantura.quanturaapp.messaging

import android.Manifest
import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.PendingIntent
import android.content.BroadcastReceiver
import android.content.Context
import android.content.Intent
import android.content.pm.PackageManager
import android.os.Build
import androidx.core.app.NotificationCompat
import androidx.core.content.ContextCompat
import com.quantura.quanturaapp.MainActivity

class InactivityNotificationReceiver : BroadcastReceiver() {
    override fun onReceive(context: Context, intent: Intent?) {
        val action = intent?.action.orEmpty()
        if (action.isBlank()) return
        InactivityNotificationScheduler.rescheduleAction(context, action)

        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU) {
            val granted = ContextCompat.checkSelfPermission(context, Manifest.permission.POST_NOTIFICATIONS) == PackageManager.PERMISSION_GRANTED
            if (!granted) return
        }

        val (title, body, path, notificationId) = when (action) {
            InactivityNotificationScheduler.ACTION_WEEKLY -> Quad(
                "Weekly Quantura recap",
                "Review this week’s market updates and model outputs.",
                "/explore?source=inactive_weekly",
                29012
            )
            else -> Quad(
                "Come back to Quantura",
                "Your watchlist and forecasts may have moved. Open Quantura for a quick check.",
                "/forecasting?source=inactive_daily",
                29011
            )
        }

        val manager = context.getSystemService(Context.NOTIFICATION_SERVICE) as NotificationManager
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
            val channel = NotificationChannel(
                CHANNEL_ID,
                "Quantura Inactive Reminders",
                NotificationManager.IMPORTANCE_DEFAULT
            ).apply {
                description = "Daily and weekly reminders for inactive sessions"
            }
            manager.createNotificationChannel(channel)
        }

        val deepLink = if (path.startsWith("http")) path else "https://quantura.studio$path"
        val launchIntent = Intent(context, MainActivity::class.java).apply {
            flags = Intent.FLAG_ACTIVITY_NEW_TASK or Intent.FLAG_ACTIVITY_CLEAR_TOP
            putExtra(QuanturaMessagingService.EXTRA_DEEP_LINK_URL, deepLink)
        }
        val contentIntent = PendingIntent.getActivity(
            context,
            notificationId,
            launchIntent,
            PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_IMMUTABLE
        )

        val notification = NotificationCompat.Builder(context, CHANNEL_ID)
            .setSmallIcon(android.R.drawable.ic_popup_reminder)
            .setContentTitle(title)
            .setContentText(body)
            .setPriority(NotificationCompat.PRIORITY_DEFAULT)
            .setAutoCancel(true)
            .setContentIntent(contentIntent)
            .build()

        manager.notify(notificationId, notification)
    }

    private data class Quad(
        val title: String,
        val body: String,
        val path: String,
        val id: Int
    )

    companion object {
        private const val CHANNEL_ID = "quantura_inactive"
    }
}
