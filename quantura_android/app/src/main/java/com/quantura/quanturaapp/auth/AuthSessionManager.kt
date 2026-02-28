package com.quantura.quanturaapp.auth

import android.util.Log
import com.google.firebase.auth.FirebaseAuth
import java.util.concurrent.atomic.AtomicBoolean

object AuthSessionManager {
    private const val TAG = "AuthSessionManager"

    private val started = AtomicBoolean(false)
    private val anonymousBootstrapInFlight = AtomicBoolean(false)
    private var listener: FirebaseAuth.AuthStateListener? = null

    fun start(firebaseReady: Boolean) {
        if (!firebaseReady) {
            Log.w(TAG, "Firebase is unavailable; auth session manager not started.")
            return
        }
        if (!started.compareAndSet(false, true)) return

        val auth = FirebaseAuth.getInstance()
        val authStateListener = FirebaseAuth.AuthStateListener { fbAuth ->
            handleAuthState(fbAuth)
        }
        listener = authStateListener
        auth.addAuthStateListener(authStateListener)
        handleAuthState(auth)
    }

    private fun handleAuthState(auth: FirebaseAuth) {
        val user = auth.currentUser
        if (user == null) {
            signInAnonymouslyIfNeeded(auth)
            return
        }
        Log.i(TAG, "Auth state changed uid=${user.uid} anonymous=${user.isAnonymous}")
    }

    private fun signInAnonymouslyIfNeeded(auth: FirebaseAuth) {
        if (!anonymousBootstrapInFlight.compareAndSet(false, true)) return
        Log.i(TAG, "No current user; bootstrapping anonymous sign-in.")
        auth.signInAnonymously()
            .addOnSuccessListener { result ->
                anonymousBootstrapInFlight.set(false)
                Log.i(TAG, "Anonymous sign-in succeeded uid=${result.user?.uid.orEmpty()}")
            }
            .addOnFailureListener { error ->
                anonymousBootstrapInFlight.set(false)
                Log.e(TAG, "Anonymous sign-in failed.", error)
            }
    }
}
