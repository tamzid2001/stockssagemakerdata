package com.quantura.quanturaapp.messaging

import android.Manifest
import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.PendingIntent
import android.content.Context
import android.content.Intent
import android.content.pm.PackageManager
import android.os.Build
import androidx.core.app.NotificationCompat
import androidx.core.content.ContextCompat
import com.google.firebase.auth.FirebaseAuth
import com.google.firebase.firestore.DocumentChange
import com.google.firebase.firestore.ListenerRegistration
import com.google.firebase.firestore.FirebaseFirestore
import com.quantura.quanturaapp.MainActivity
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

object NativePersonalizedNotificationManager {
    private const val CHANNEL_ID = "quantura_personalized"
    private const val CHANNEL_NAME = "Quantura Personalized Updates"
    private const val BASE_URL = "https://quantura.studio"

    private var authListener: FirebaseAuth.AuthStateListener? = null
    private var watchlistListener: ListenerRegistration? = null
    private var forecastListener: ListenerRegistration? = null
    private var aiLogListener: ListenerRegistration? = null
    private var activeUid: String = ""
    private val primedKeys = mutableSetOf<String>()
    private val lastSentAt = ConcurrentHashMap<String, Long>()
    private val notificationId = AtomicInteger(12000)
    private var started = false

    fun start(context: Context) {
        if (started) return
        started = true

        val appContext = context.applicationContext
        val auth = FirebaseAuth.getInstance()
        val listener = FirebaseAuth.AuthStateListener { firebaseAuth ->
            bindForUser(appContext, firebaseAuth.currentUser?.uid.orEmpty())
        }
        authListener = listener
        auth.addAuthStateListener(listener)
        bindForUser(appContext, auth.currentUser?.uid.orEmpty())
    }

    private fun bindForUser(context: Context, uidRaw: String) {
        val uid = uidRaw.trim()
        if (uid == activeUid) return
        clearListeners()
        activeUid = uid
        if (uid.isEmpty()) return

        val db = FirebaseFirestore.getInstance()

        watchlistListener = db.collection("users").document(uid).collection("watchlist")
            .orderBy("updatedAt", com.google.firebase.firestore.Query.Direction.DESCENDING)
            .limit(1)
            .addSnapshotListener { snapshot, _ ->
                handleSnapshot(
                    context = context,
                    key = "watchlist_$uid",
                    changes = snapshot?.documentChanges,
                    title = "Watchlist updated",
                    fallbackBody = "Your Quantura watchlist changed.",
                    deepLinkPath = "/watchlist"
                )
            }

        forecastListener = db.collection("forecast_requests")
            .whereEqualTo("userId", uid)
            .limit(1)
            .addSnapshotListener { snapshot, _ ->
                handleSnapshot(
                    context = context,
                    key = "forecast_$uid",
                    changes = snapshot?.documentChanges,
                    title = "Forecast update",
                    fallbackBody = "A forecast changed in your workspace.",
                    deepLinkPath = "/saved-forecasts"
                )
            }

        aiLogListener = db.collection("users").document(uid).collection("ai_logs")
            .orderBy("createdAt", com.google.firebase.firestore.Query.Direction.DESCENDING)
            .limit(1)
            .addSnapshotListener { snapshot, _ ->
                handleSnapshot(
                    context = context,
                    key = "ai_logs_$uid",
                    changes = snapshot?.documentChanges,
                    title = "AI log update",
                    fallbackBody = "New AI activity is available.",
                    deepLinkPath = "/dashboard"
                )
            }
    }

    private fun handleSnapshot(
        context: Context,
        key: String,
        changes: List<DocumentChange>?,
        title: String,
        fallbackBody: String,
        deepLinkPath: String,
    ) {
        if (!primedKeys.contains(key)) {
            primedKeys.add(key)
            return
        }
        val firstChange = changes?.firstOrNull() ?: return
        if (firstChange.type != DocumentChange.Type.ADDED && firstChange.type != DocumentChange.Type.MODIFIED) {
            return
        }

        val now = System.currentTimeMillis()
        val last = lastSentAt[key] ?: 0L
        if (now - last < 10_000L) return
        lastSentAt[key] = now

        val data = firstChange.document.data
        val explicitBody = (
            data["notificationText"] as? String
                ?: data["summary"] as? String
                ?: data["notes"] as? String
                ?: data["title"] as? String
                ?: ""
        ).trim()
        val body = if (explicitBody.isNotEmpty()) explicitBody else fallbackBody
        showNotification(context, title, body, deepLinkPath)
    }

    private fun showNotification(context: Context, title: String, body: String, deepLinkPath: String) {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU) {
            if (ContextCompat.checkSelfPermission(context, Manifest.permission.POST_NOTIFICATIONS) != PackageManager.PERMISSION_GRANTED) {
                return
            }
        }

        val manager = context.getSystemService(Context.NOTIFICATION_SERVICE) as NotificationManager
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
            val channel = NotificationChannel(CHANNEL_ID, CHANNEL_NAME, NotificationManager.IMPORTANCE_DEFAULT).apply {
                description = "User-specific watchlist and forecast updates"
            }
            manager.createNotificationChannel(channel)
        }

        val normalizedPath = if (deepLinkPath.startsWith("/")) deepLinkPath else "/$deepLinkPath"
        val targetUrl = "$BASE_URL$normalizedPath"
        val id = notificationId.incrementAndGet()
        val intent = Intent().apply {
            setClass(context, MainActivity::class.java)
            setPackage(context.packageName)
            flags = Intent.FLAG_ACTIVITY_NEW_TASK or Intent.FLAG_ACTIVITY_CLEAR_TOP
            putExtra(QuanturaMessagingService.EXTRA_DEEP_LINK_URL, targetUrl)
        }
        val pendingIntent = PendingIntent.getActivity(
            context,
            id,
            intent,
            PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_IMMUTABLE
        )

        val notification = NotificationCompat.Builder(context, CHANNEL_ID)
            .setSmallIcon(android.R.drawable.ic_popup_reminder)
            .setContentTitle(title)
            .setContentText(body)
            .setPriority(NotificationCompat.PRIORITY_DEFAULT)
            .setAutoCancel(true)
            .setContentIntent(pendingIntent)
            .build()

        manager.notify(id, notification)
    }

    private fun clearListeners() {
        watchlistListener?.remove()
        watchlistListener = null
        forecastListener?.remove()
        forecastListener = null
        aiLogListener?.remove()
        aiLogListener = null
    }
}
