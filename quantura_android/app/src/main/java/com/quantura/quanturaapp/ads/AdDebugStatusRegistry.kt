package com.quantura.quanturaapp.ads

import java.util.concurrent.ConcurrentHashMap

data class AdFormatStatusSnapshot(
    val format: String,
    val lastLoadStatus: String,
    val lastShowStatus: String,
)

object AdDebugStatusRegistry {
    private const val DEFAULT_LOAD_STATUS = "idle"
    private const val DEFAULT_SHOW_STATUS = "idle"

    private val loadStatusByFormat = ConcurrentHashMap<String, String>()
    private val showStatusByFormat = ConcurrentHashMap<String, String>()

    fun updateLoad(format: String, status: String) {
        loadStatusByFormat[format.lowercase()] = status
    }

    fun updateShow(format: String, status: String) {
        showStatusByFormat[format.lowercase()] = status
    }

    fun snapshot(formats: List<String>): List<AdFormatStatusSnapshot> {
        return formats.map { format ->
            val key = format.lowercase()
            AdFormatStatusSnapshot(
                format = key,
                lastLoadStatus = loadStatusByFormat[key] ?: DEFAULT_LOAD_STATUS,
                lastShowStatus = showStatusByFormat[key] ?: DEFAULT_SHOW_STATUS,
            )
        }
    }
}
