package com.quantura.quanturaapp.messaging

import android.content.Context
import android.content.SharedPreferences

object QuanturaFcmTokenHolder {
    private const val PREFS_NAME = "quantura_fcm"
    private const val KEY_TOKEN = "fcm_token"

    private fun prefs(context: Context): SharedPreferences =
        context.getSharedPreferences(PREFS_NAME, Context.MODE_PRIVATE)

    fun setToken(context: Context, token: String) {
        prefs(context).edit().putString(KEY_TOKEN, token).apply()
    }

    fun getToken(context: Context): String? =
        prefs(context).getString(KEY_TOKEN, null)?.takeIf { it.isNotBlank() }
}
