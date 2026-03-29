package com.quantura.quanturaapp.messaging

import android.app.AlarmManager
import android.app.PendingIntent
import android.content.Context
import android.content.Intent
import android.os.Build
import android.util.Log

object InactivityNotificationScheduler {
    const val ACTION_DAILY = "com.quantura.quanturaapp.NOTIFY_INACTIVE_DAILY"
    const val ACTION_WEEKLY = "com.quantura.quanturaapp.NOTIFY_INACTIVE_WEEKLY"

    private const val TAG = "InactivityScheduler"
    private const val REQUEST_DAILY = 29001
    private const val REQUEST_WEEKLY = 29002
    private const val DAY_MS = 24L * 60L * 60L * 1000L

    fun reschedule(context: Context) {
        cancel(context)
        rescheduleAction(context, ACTION_DAILY)
        rescheduleAction(context, ACTION_WEEKLY)
    }

    fun cancel(context: Context) {
        val manager = context.getSystemService(Context.ALARM_SERVICE) as AlarmManager
        manager.cancel(buildPendingIntent(context, ACTION_DAILY, REQUEST_DAILY))
        manager.cancel(buildPendingIntent(context, ACTION_WEEKLY, REQUEST_WEEKLY))
    }

    fun rescheduleAction(context: Context, action: String) {
        val spec = scheduleSpecFor(action) ?: return
        schedule(context, action, spec.requestCode, spec.delayMs)
    }

    private fun schedule(context: Context, action: String, requestCode: Int, delayMs: Long) {
        val manager = context.getSystemService(Context.ALARM_SERVICE) as AlarmManager
        val triggerAt = System.currentTimeMillis() + delayMs
        val pendingIntent = buildPendingIntent(context, action, requestCode)
        try {
            when {
                Build.VERSION.SDK_INT >= Build.VERSION_CODES.S && manager.canScheduleExactAlarms() ->
                    manager.setExactAndAllowWhileIdle(AlarmManager.RTC_WAKEUP, triggerAt, pendingIntent)
                Build.VERSION.SDK_INT >= Build.VERSION_CODES.M ->
                    manager.setAndAllowWhileIdle(AlarmManager.RTC_WAKEUP, triggerAt, pendingIntent)
                else -> manager.set(AlarmManager.RTC_WAKEUP, triggerAt, pendingIntent)
            }
        } catch (securityException: SecurityException) {
            Log.w(TAG, "Exact alarm not permitted, falling back to inexact schedule for $action")
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.M) {
                manager.setAndAllowWhileIdle(AlarmManager.RTC_WAKEUP, triggerAt, pendingIntent)
            } else {
                manager.set(AlarmManager.RTC_WAKEUP, triggerAt, pendingIntent)
            }
        }
    }

    private fun buildPendingIntent(context: Context, action: String, requestCode: Int): PendingIntent {
        val intent = Intent().apply {
            setClass(context, InactivityNotificationReceiver::class.java)
            setPackage(context.packageName)
            this.action = action
        }
        return PendingIntent.getBroadcast(
            context,
            requestCode,
            intent,
            PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_IMMUTABLE
        )
    }

    private fun scheduleSpecFor(action: String): ScheduleSpec? =
        when (action) {
            ACTION_DAILY -> ScheduleSpec(REQUEST_DAILY, DAY_MS)
            ACTION_WEEKLY -> ScheduleSpec(REQUEST_WEEKLY, 7L * DAY_MS)
            else -> null
        }

    private data class ScheduleSpec(
        val requestCode: Int,
        val delayMs: Long,
    )
}
