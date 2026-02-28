package com.quantura.quanturaapp.auth

import android.content.Context
import com.google.android.play.core.integrity.IntegrityManagerFactory
import com.google.android.play.core.integrity.IntegrityTokenRequest
import kotlinx.coroutines.suspendCancellableCoroutine
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException

class PlayIntegrityClient(context: Context) {
    private val integrityManager = IntegrityManagerFactory.create(context.applicationContext)

    suspend fun requestToken(nonce: String, cloudProjectNumber: Long? = null): String {
        val builder = IntegrityTokenRequest.builder().setNonce(nonce)
        if (cloudProjectNumber != null) {
            builder.setCloudProjectNumber(cloudProjectNumber)
        }
        val request = builder.build()

        return suspendCancellableCoroutine { continuation ->
            integrityManager.requestIntegrityToken(request)
                .addOnSuccessListener { response ->
                    val token = response.token().trim()
                    if (token.isEmpty()) {
                        continuation.resumeWithException(IllegalStateException("Play Integrity token is empty."))
                    } else {
                        continuation.resume(token)
                    }
                }
                .addOnFailureListener { error ->
                    continuation.resumeWithException(error)
                }
        }
    }
}
