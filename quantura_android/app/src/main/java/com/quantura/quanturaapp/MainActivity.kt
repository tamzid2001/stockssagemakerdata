package com.quantura.quanturaapp

import android.Manifest
import android.content.Intent
import android.content.pm.PackageManager
import android.net.Uri
import android.os.Build
import android.os.Bundle
import android.util.Log
import android.webkit.WebChromeClient
import android.webkit.WebResourceRequest
import android.webkit.WebSettings
import android.webkit.WebView
import android.webkit.WebViewClient
import androidx.activity.ComponentActivity
import androidx.activity.compose.setContent
import androidx.activity.enableEdgeToEdge
import androidx.activity.result.contract.ActivityResultContracts
import androidx.compose.foundation.background
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Spacer
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.height
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.wrapContentHeight
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.material3.AlertDialog
import androidx.compose.material3.Button
import androidx.compose.material3.ButtonDefaults
import androidx.compose.material3.CircularProgressIndicator
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.OutlinedButton
import androidx.compose.material3.OutlinedTextField
import androidx.compose.material3.Surface
import androidx.compose.material3.Text
import androidx.compose.material3.TextButton
import androidx.compose.runtime.Composable
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.Brush
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.text.input.PasswordVisualTransformation
import androidx.compose.ui.text.style.TextAlign
import androidx.compose.ui.unit.dp
import androidx.compose.ui.viewinterop.AndroidView
import androidx.core.content.ContextCompat
import androidx.lifecycle.lifecycleScope
import com.google.android.gms.auth.api.signin.GoogleSignIn
import com.google.android.gms.auth.api.signin.GoogleSignInClient
import com.google.android.gms.auth.api.signin.GoogleSignInOptions
import com.google.android.gms.common.api.ApiException
import com.google.android.gms.tasks.OnCompleteListener
import com.google.firebase.auth.AuthCredential
import com.google.firebase.auth.EmailAuthProvider
import com.google.firebase.auth.FirebaseAuth
import com.google.firebase.auth.FirebaseAuthUserCollisionException
import com.google.firebase.auth.FirebaseUser
import com.google.firebase.auth.GoogleAuthProvider
import com.google.firebase.messaging.FirebaseMessaging
import com.quantura.quanturaapp.ads.AdManager
import com.quantura.quanturaapp.ads.BannerAdView
import com.quantura.quanturaapp.messaging.NativePersonalizedNotificationManager
import com.quantura.quanturaapp.messaging.QuanturaFcmTokenHolder
import com.quantura.quanturaapp.messaging.QuanturaMessagingService
import com.quantura.quanturaapp.web.QuanturaJavascriptBridge
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.suspendCancellableCoroutine
import kotlinx.coroutines.withContext
import org.json.JSONArray
import org.json.JSONObject
import java.net.HttpURLConnection
import java.net.URL
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException

private const val DEFAULT_START_URL = "https://quantura.studio/"

class MainActivity : ComponentActivity() {
    private val appContainer by lazy { (application as QuanturaApplication).container }
    private val firebaseAuth by lazy { FirebaseAuth.getInstance() }

    private var webViewRef: WebView? = null
    private var googleSignInClient: GoogleSignInClient? = null
    private var authStateListener: FirebaseAuth.AuthStateListener? = null

    private var lastSyncedUid: String = ""
    private var bridgeSyncRequired = false
    private var gateDismissedForSession = false
    private var anonymousBootstrapInFlight = false

    private var authGateVisible by mutableStateOf(true)
    private var authBusy by mutableStateOf(false)
    private var authErrorText by mutableStateOf("")
    private var emailDialogVisible by mutableStateOf(false)
    private var emailValue by mutableStateOf("")
    private var passwordValue by mutableStateOf("")

    private val googleSignInLauncher =
        registerForActivityResult(ActivityResultContracts.StartActivityForResult()) { result ->
            handleGoogleSignInResult(result.data)
        }

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)

        enableEdgeToEdge()
        requestNotificationPermission()
        fetchFcmToken()
        NativePersonalizedNotificationManager.start(this)
        registerAuthStateListener()

        lifecycleScope.launch {
            appContainer.remoteConfigManager.fetchAndActivate()
            appContainer.adManager.primeAds(this@MainActivity)
            appContainer.appOpenAdManager.loadAdIfNeeded()
        }

        val deepLinkUrl = intent?.getStringExtra(QuanturaMessagingService.EXTRA_DEEP_LINK_URL)
        val startUrl = deepLinkUrl ?: DEFAULT_START_URL

        setContent {
            MaterialTheme {
                Surface(modifier = Modifier.fillMaxSize()) {
                    Box(modifier = Modifier.fillMaxSize()) {
                        Column(modifier = Modifier.fillMaxSize()) {
                            QuanturaWebViewScreen(
                                modifier = Modifier.weight(1f),
                                activity = this@MainActivity,
                                startUrl = startUrl,
                                adManager = appContainer.adManager,
                                onNativeAuthMessage = { type, payload ->
                                    handleNativeAuthMessage(type, payload)
                                },
                                onReady = { webView ->
                                    webViewRef = webView
                                    appContainer.appOpenAdManager.setPresentationBlockedByAuthGate(authGateVisible)
                                    emitAuthStateToWeb(firebaseAuth.currentUser, idTokenFresh = false)
                                },
                            )
                            AndroidView(
                                modifier = Modifier.fillMaxWidth().wrapContentHeight(),
                                factory = { ctx ->
                                    BannerAdView(ctx).apply {
                                        loadAd(appContainer.remoteConfigManager)
                                    }
                                },
                            )
                        }

                        if (authGateVisible) {
                            NativeAuthGate(
                                isBusy = authBusy,
                                errorText = authErrorText,
                                onGoogle = { startGoogleSignInFlow(trigger = "auth_gate") },
                                onEmail = {
                                    authErrorText = ""
                                    emailDialogVisible = true
                                },
                                onNotNow = { continueAnonymouslyForNow() },
                            )
                        }

                        if (emailDialogVisible) {
                            EmailSignInDialog(
                                email = emailValue,
                                password = passwordValue,
                                isBusy = authBusy,
                                onEmailChanged = { emailValue = it },
                                onPasswordChanged = { passwordValue = it },
                                onDismiss = {
                                    if (!authBusy) emailDialogVisible = false
                                },
                                onContinue = {
                                    signInWithEmail(emailValue, passwordValue)
                                },
                            )
                        }
                    }
                }
            }
        }

        appContainer.appOpenAdManager.setPresentationBlockedByAuthGate(authGateVisible)
    }

    private fun registerAuthStateListener() {
        if (authStateListener != null) return
        authStateListener = FirebaseAuth.AuthStateListener { auth ->
            handleAuthStateChanged(auth.currentUser)
        }
        firebaseAuth.addAuthStateListener(authStateListener!!)
        handleAuthStateChanged(firebaseAuth.currentUser)
    }

    private fun handleAuthStateChanged(user: FirebaseUser?) {
        runOnUiThread {
            Log.i(
                "MainActivity",
                "[Auth][Android] state uid=${user?.uid.orEmpty()} anonymous=${user?.isAnonymous ?: false}"
            )
            emitAuthStateToWeb(user, idTokenFresh = false)

            if (user == null) {
                updateAuthGateVisibility(!gateDismissedForSession)
                ensureAnonymousSessionIfNeeded()
                return@runOnUiThread
            }

            if (user.isAnonymous) {
                updateAuthGateVisibility(!gateDismissedForSession)
                return@runOnUiThread
            }

            if (bridgeSyncRequired || authBusy) {
                updateAuthGateVisibility(true)
                return@runOnUiThread
            }

            updateAuthGateVisibility(false)
            if (lastSyncedUid != user.uid) {
                lastSyncedUid = user.uid
                syncWebSessionForUser(
                    user = user,
                    forceRefresh = false,
                    interactive = false,
                    source = "auth_state_listener",
                )
            }
        }
    }

    private fun ensureAnonymousSessionIfNeeded() {
        if (firebaseAuth.currentUser != null) return
        if (anonymousBootstrapInFlight) return

        anonymousBootstrapInFlight = true
        Log.i("MainActivity", "[Auth][Android] Bootstrapping anonymous session.")
        firebaseAuth.signInAnonymously()
            .addOnSuccessListener { result ->
                anonymousBootstrapInFlight = false
                Log.i("MainActivity", "[Auth][Android] Anonymous sign-in succeeded uid=${result.user?.uid.orEmpty()}")
                emitAuthStateToWeb(result.user, idTokenFresh = false)
            }
            .addOnFailureListener { error ->
                anonymousBootstrapInFlight = false
                Log.e("MainActivity", "[Auth][Android] Anonymous sign-in failed.", error)
            }
    }

    private fun updateAuthGateVisibility(visible: Boolean) {
        authGateVisible = visible
        appContainer.appOpenAdManager.setPresentationBlockedByAuthGate(visible)
    }

    private fun continueAnonymouslyForNow() {
        gateDismissedForSession = true
        bridgeSyncRequired = false
        authBusy = false
        authErrorText = ""
        updateAuthGateVisibility(false)
        emitAuthStateToWeb(firebaseAuth.currentUser, idTokenFresh = false)
    }

    private fun startGoogleSignInFlow(trigger: String) {
        val client = resolveGoogleSignInClient()
        if (client == null) {
            failInteractiveSignIn("Google sign-in is not configured (missing default_web_client_id).")
            return
        }
        beginInteractiveSignIn("google", trigger)
        googleSignInLauncher.launch(client.signInIntent)
    }

    private fun resolveGoogleSignInClient(): GoogleSignInClient? {
        googleSignInClient?.let { return it }

        val webClientResId = resources.getIdentifier("default_web_client_id", "string", packageName)
        if (webClientResId == 0) return null
        val webClientId = getString(webClientResId).trim()
        if (webClientId.isEmpty()) return null

        val signInOptions = GoogleSignInOptions.Builder(GoogleSignInOptions.DEFAULT_SIGN_IN)
            .requestIdToken(webClientId)
            .requestEmail()
            .build()
        return GoogleSignIn.getClient(this, signInOptions).also { googleSignInClient = it }
    }

    private fun handleGoogleSignInResult(data: Intent?) {
        val account = try {
            GoogleSignIn.getSignedInAccountFromIntent(data).getResult(ApiException::class.java)
        } catch (error: Exception) {
            failInteractiveSignIn(error.message ?: "Google sign-in failed.")
            return
        }

        val idToken = account.idToken?.trim().orEmpty()
        if (idToken.isEmpty()) {
            failInteractiveSignIn("Google did not return an ID token.")
            return
        }

        val credential = GoogleAuthProvider.getCredential(idToken, null)
        signInOrLinkWithCredential(provider = "google", credential = credential)
    }

    private fun signInWithEmail(emailRaw: String, passwordRaw: String) {
        val email = emailRaw.trim()
        val password = passwordRaw
        if (email.isEmpty() || password.isEmpty()) {
            authErrorText = "Enter both email and password."
            return
        }
        beginInteractiveSignIn("email", "auth_gate_email")
        val credential = EmailAuthProvider.getCredential(email, password)
        signInOrLinkWithCredential(provider = "email", credential = credential)
    }

    private fun beginInteractiveSignIn(provider: String, trigger: String) {
        Log.i("MainActivity", "[Auth][Android] Starting interactive sign-in provider=$provider trigger=$trigger")
        gateDismissedForSession = false
        bridgeSyncRequired = true
        authBusy = true
        authErrorText = ""
        updateAuthGateVisibility(true)
    }

    private fun signInOrLinkWithCredential(provider: String, credential: AuthCredential) {
        val currentUser = firebaseAuth.currentUser
        if (currentUser?.isAnonymous == true) {
            Log.i("MainActivity", "[Auth][Android] Attempting anonymous link provider=$provider")
            currentUser.linkWithCredential(credential)
                .addOnSuccessListener { authResult ->
                    completeInteractiveSignIn(provider, authResult.user)
                }
                .addOnFailureListener { error ->
                    val collisionCredential = (error as? FirebaseAuthUserCollisionException)?.updatedCredential
                    val fallbackCredential = collisionCredential ?: credential
                    if (collisionCredential != null) {
                        Log.w("MainActivity", "[Auth][Android] Link collision; using updated credential provider=$provider")
                    } else {
                        Log.w("MainActivity", "[Auth][Android] Link failed; trying sign-in fallback provider=$provider", error)
                    }
                    firebaseAuth.signInWithCredential(fallbackCredential)
                        .addOnSuccessListener { authResult ->
                            completeInteractiveSignIn(provider, authResult.user)
                        }
                        .addOnFailureListener { signInError ->
                            failInteractiveSignIn(signInError.message ?: "Native Firebase sign-in failed.")
                        }
                }
            return
        }

        firebaseAuth.signInWithCredential(credential)
            .addOnSuccessListener { authResult ->
                completeInteractiveSignIn(provider, authResult.user)
            }
            .addOnFailureListener { error ->
                failInteractiveSignIn(error.message ?: "Native Firebase sign-in failed.")
            }
    }

    private fun completeInteractiveSignIn(provider: String, user: FirebaseUser?) {
        if (user == null) {
            failInteractiveSignIn("Firebase user is unavailable after sign-in.")
            return
        }
        Log.i("MainActivity", "[Auth][Android] Native sign-in success provider=$provider uid=${user.uid}; syncing web session")
        syncWebSessionForUser(
            user = user,
            forceRefresh = true,
            interactive = true,
            source = provider,
        )
    }

    private fun failInteractiveSignIn(message: String) {
        Log.w("MainActivity", "[Auth][Android] Interactive sign-in failed: $message")
        authBusy = false
        bridgeSyncRequired = firebaseAuth.currentUser?.isAnonymous == false
        authErrorText = message
        updateAuthGateVisibility(true)
    }

    private fun syncWebSessionForUser(
        user: FirebaseUser,
        forceRefresh: Boolean,
        interactive: Boolean,
        source: String,
    ) {
        if (interactive) {
            authBusy = true
        }

        lifecycleScope.launch {
            try {
                val nativeIdToken = user.awaitIdToken(forceRefresh)
                val customToken = exchangeNativeIdTokenForCustomToken(nativeIdToken)
                injectCustomTokenIntoWeb(customToken)
                emitAuthStateToWeb(user, idTokenFresh = forceRefresh)
                Log.i("MainActivity", "[Auth][Android] Web custom token sync succeeded source=$source uid=${user.uid}")

                lastSyncedUid = user.uid
                bridgeSyncRequired = false
                authErrorText = ""
                emailDialogVisible = false
                if (interactive) {
                    authBusy = false
                    updateAuthGateVisibility(false)
                }
            } catch (error: Exception) {
                Log.e("MainActivity", "[Auth][Android] Web custom token sync failed source=$source", error)
                if (interactive) {
                    authBusy = false
                    bridgeSyncRequired = firebaseAuth.currentUser?.isAnonymous == false
                    authErrorText = "Signed in, but website sync failed: ${error.message ?: "unknown error"}"
                    updateAuthGateVisibility(true)
                }
            }
        }
    }

    private suspend fun exchangeNativeIdTokenForCustomToken(nativeIdToken: String): String {
        val idToken = nativeIdToken.trim()
        if (idToken.isEmpty()) throw IllegalStateException("Native ID token is empty.")

        return withContext(Dispatchers.IO) {
            val endpoint = "${resolveTrustedOrigin()}/api/auth/exchange"
            val connection = (URL(endpoint).openConnection() as HttpURLConnection).apply {
                requestMethod = "POST"
                connectTimeout = 30_000
                readTimeout = 30_000
                doOutput = true
                setRequestProperty("Content-Type", "application/json")
                setRequestProperty("Authorization", "Bearer $idToken")
            }
            try {
                connection.outputStream.use { output ->
                    output.write("{}".toByteArray(Charsets.UTF_8))
                }

                val status = connection.responseCode
                val body = runCatching {
                    val stream = if (status in 200..299) connection.inputStream else connection.errorStream
                    stream?.bufferedReader()?.use { it.readText() }.orEmpty()
                }.getOrDefault("")

                val payload = if (body.isNotBlank()) runCatching { JSONObject(body) }.getOrNull() else null
                if (status !in 200..299) {
                    val detail = payload?.optString("error")?.ifBlank { payload.optString("message") } ?: "Token exchange failed"
                    throw IllegalStateException("$detail ($status)")
                }

                val customToken = payload?.optString("customToken")?.trim().orEmpty()
                if (customToken.isEmpty()) {
                    throw IllegalStateException("Server returned an empty custom token.")
                }
                customToken
            } finally {
                connection.disconnect()
            }
        }
    }

    private fun resolveTrustedOrigin(): String {
        val parsed = runCatching { Uri.parse(webViewRef?.url ?: "") }.getOrNull()
        val host = parsed?.host?.lowercase()
        val scheme = parsed?.scheme?.lowercase().orEmpty().ifBlank { "https" }
        return if (isTrustedHost(host)) "$scheme://$host" else "https://quantura.studio"
    }

    private fun injectCustomTokenIntoWeb(customToken: String) {
        val escaped = customToken.escapeForSingleQuotedJs()
        emitJs(
            """
            window.__QUANTURA_PENDING_CUSTOM_TOKEN__='$escaped';
            if (window.__quanturaAuthBridge?.receiveCustomToken) {
                window.__quanturaAuthBridge.receiveCustomToken('$escaped');
            }
            window.dispatchEvent(new CustomEvent('quantura:native-custom-token', { detail: { type: 'CUSTOM_TOKEN' } }));
            """.trimIndent()
        )
    }

    private fun emitAuthStateToWeb(user: FirebaseUser?, idTokenFresh: Boolean) {
        val payload = JSONObject().apply {
            put("type", "AUTH_STATE")
            put("uid", user?.uid ?: "")
            put("isAnonymous", user?.isAnonymous ?: true)
            put("providers", providerIdsFor(user))
            put("idTokenFresh", idTokenFresh)
        }
        emitJs(
            """
            window.__QUANTURA_PENDING_AUTH_STATE__=${payload};
            if (window.__quanturaAuthBridge?.onNativeAuthState) {
                window.__quanturaAuthBridge.onNativeAuthState(${payload});
            }
            window.dispatchEvent(new CustomEvent('quantura:native-auth-state', { detail: ${payload} }));
            """.trimIndent()
        )
    }

    private fun providerIdsFor(user: FirebaseUser?): JSONArray {
        val providers = user?.providerData
            ?.mapNotNull { providerInfo -> providerInfo.providerId?.trim()?.takeIf { it.isNotEmpty() } }
            ?.distinct()
            ?.toMutableList()
            ?: mutableListOf()
        if (providers.isEmpty() && user?.isAnonymous == true) {
            providers.add("anonymous")
        }
        return JSONArray(providers)
    }

    private fun emitNativeFcmToken(token: String) {
        val escaped = token.escapeForSingleQuotedJs()
        emitJs(
            "window.__NATIVE_FCM_TOKEN__='$escaped';if(typeof window.__quanturaNativeTokenReady==='function')window.__quanturaNativeTokenReady('$escaped');"
        )
    }

    private fun emitJs(script: String) {
        webViewRef?.post {
            webViewRef?.evaluateJavascript(script, null)
        }
    }

    private fun handleNativeAuthMessage(typeRaw: String, payload: JSONObject) {
        when (typeRaw.trim().uppercase()) {
            "REQUEST_SIGN_IN" -> {
                Log.i("MainActivity", "[Auth][Android] REQUEST_SIGN_IN received from web.")
                gateDismissedForSession = false
                authErrorText = ""
                updateAuthGateVisibility(true)
                val provider = payload.optString("provider").trim().lowercase()
                if (provider == "google" && !authBusy) {
                    startGoogleSignInFlow(trigger = "web_request")
                } else if (provider == "email") {
                    emailDialogVisible = true
                }
            }

            "GET_AUTH_STATE" -> {
                Log.i("MainActivity", "[Auth][Android] GET_AUTH_STATE received from web.")
                emitAuthStateToWeb(firebaseAuth.currentUser, idTokenFresh = false)
            }

            "SIGN_OUT" -> {
                Log.i("MainActivity", "[Auth][Android] SIGN_OUT received from web.")
                signOutToAnonymous()
            }

            else -> {
                Log.w("MainActivity", "[Auth][Android] Unknown auth bridge type=$typeRaw")
            }
        }
    }

    private fun signOutToAnonymous() {
        try {
            firebaseAuth.signOut()
            resolveGoogleSignInClient()?.signOut()
        } catch (error: Exception) {
            Log.w("MainActivity", "[Auth][Android] Sign-out warning: ${error.message}")
        }
        bridgeSyncRequired = false
        gateDismissedForSession = false
        authBusy = false
        authErrorText = ""
        lastSyncedUid = ""
        updateAuthGateVisibility(true)
        emitAuthStateToWeb(firebaseAuth.currentUser, idTokenFresh = false)
        ensureAnonymousSessionIfNeeded()
    }

    private fun requestNotificationPermission() {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU) {
            if (ContextCompat.checkSelfPermission(this, Manifest.permission.POST_NOTIFICATIONS) != PackageManager.PERMISSION_GRANTED) {
                requestPermissions(arrayOf(Manifest.permission.POST_NOTIFICATIONS), REQUEST_NOTIFICATION_PERMISSION)
            }
        }
    }

    private fun fetchFcmToken() {
        FirebaseMessaging.getInstance().token.addOnCompleteListener(OnCompleteListener { task ->
            if (!task.isSuccessful) return@OnCompleteListener
            task.result?.let { token ->
                QuanturaFcmTokenHolder.setToken(this, token)
                emitNativeFcmToken(token)
            }
        })
    }

    override fun onPause() {
        webViewRef?.onPause()
        webViewRef?.pauseTimers()
        appContainer.adManager.onPause()
        super.onPause()
    }

    override fun onResume() {
        super.onResume()
        webViewRef?.onResume()
        webViewRef?.resumeTimers()
        appContainer.adManager.onResume(this)
    }

    override fun onDestroy() {
        authStateListener?.let { firebaseAuth.removeAuthStateListener(it) }
        authStateListener = null
        webViewRef?.removeJavascriptInterface("QuanturaBridge")
        webViewRef?.removeJavascriptInterface("quanturaAuth")
        webViewRef?.destroy()
        webViewRef = null
        super.onDestroy()
    }

    override fun onNewIntent(intent: Intent) {
        super.onNewIntent(intent)
        val deepLinkUrl = intent.getStringExtra(QuanturaMessagingService.EXTRA_DEEP_LINK_URL).orEmpty()
        if (deepLinkUrl.isNotBlank()) {
            val uri = runCatching { Uri.parse(deepLinkUrl) }.getOrNull()
            if (isTrustedUri(uri)) {
                webViewRef?.loadUrl(deepLinkUrl)
            } else if (uri != null) {
                runCatching { startActivity(Intent(Intent.ACTION_VIEW, uri)) }
            }
        }
    }

    companion object {
        private const val REQUEST_NOTIFICATION_PERMISSION = 9001
    }
}

@Composable
private fun QuanturaWebViewScreen(
    modifier: Modifier = Modifier,
    activity: ComponentActivity,
    startUrl: String,
    adManager: AdManager,
    onNativeAuthMessage: (type: String, payload: JSONObject) -> Unit,
    onReady: (WebView) -> Unit,
) {
    AndroidView(
        modifier = modifier.fillMaxSize(),
        factory = { context ->
            WebView(context).apply {
                settings.javaScriptEnabled = true
                settings.domStorageEnabled = true
                settings.mediaPlaybackRequiresUserGesture = false
                settings.javaScriptCanOpenWindowsAutomatically = false
                settings.setSupportMultipleWindows(false)
                settings.allowFileAccess = false
                settings.allowContentAccess = false
                settings.allowFileAccessFromFileURLs = false
                settings.allowUniversalAccessFromFileURLs = false
                if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.LOLLIPOP) {
                    settings.mixedContentMode = WebSettings.MIXED_CONTENT_NEVER_ALLOW
                }
                if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
                    settings.safeBrowsingEnabled = true
                }
                settings.userAgentString = "${settings.userAgentString} QuanturaAndroidApp/1.0"

                webChromeClient = WebChromeClient()
                webViewClient = object : WebViewClient() {
                    override fun shouldOverrideUrlLoading(view: WebView?, request: WebResourceRequest?): Boolean {
                        val uri = request?.url
                        if (request?.isForMainFrame == true) {
                            if (!isTrustedUri(uri)) {
                                uri?.let {
                                    runCatching {
                                        activity.startActivity(Intent(Intent.ACTION_VIEW, it))
                                    }
                                }
                                Log.w("MainActivity", "[WebView][Android] Blocked untrusted navigation uri=${uri}")
                                return true
                            }
                            val target = uri?.toString().orEmpty()
                            if (target.isNotBlank()) {
                                adManager.onPrimaryNavigation(activity, target)
                            }
                        }
                        return false
                    }

                    override fun onPageFinished(view: WebView?, url: String?) {
                        super.onPageFinished(view, url)
                        if (!isTrustedUri(Uri.parse(url ?: ""))) {
                            return
                        }
                        val token = QuanturaFcmTokenHolder.getToken(context)
                        evaluateJavascript(
                            """
                            window.__QUANTURA_NATIVE_APP__=true;
                            window.__QUANTURA_NATIVE_PLATFORM__='android';
                            window.__QUANTURA_NATIVE_AUTH_BRIDGE__=true;
                            window.dispatchEvent(new CustomEvent('quantura:native-runtime-ready',{detail:{platform:'android',authBridge:true}}));
                            """.trimIndent(),
                            null
                        )
                        if (!token.isNullOrBlank()) {
                            val escaped = token.escapeForSingleQuotedJs()
                            evaluateJavascript(
                                "window.__NATIVE_FCM_TOKEN__='$escaped';if(typeof window.__quanturaNativeTokenReady==='function')window.__quanturaNativeTokenReady('$escaped');",
                                null
                            )
                        }
                    }
                }

                val bridge = QuanturaJavascriptBridge(
                    activity = activity,
                    adManager = adManager,
                    onNativeAuthMessage = onNativeAuthMessage,
                )
                addJavascriptInterface(bridge, "QuanturaBridge")
                addJavascriptInterface(bridge, "quanturaAuth")

                val initialUrl = if (isTrustedUri(Uri.parse(startUrl))) startUrl else DEFAULT_START_URL
                loadUrl(initialUrl)
                onReady(this)
            }
        },
        update = { webView ->
            if (webView.url.isNullOrBlank()) {
                val target = if (isTrustedUri(Uri.parse(startUrl))) startUrl else DEFAULT_START_URL
                webView.loadUrl(target)
            }
            onReady(webView)
        },
    )
}

@Composable
private fun NativeAuthGate(
    isBusy: Boolean,
    errorText: String,
    onGoogle: () -> Unit,
    onEmail: () -> Unit,
    onNotNow: () -> Unit,
) {
    Box(
        modifier = Modifier
            .fillMaxSize()
            .background(
                Brush.verticalGradient(
                    colors = listOf(
                        Color(0xFF081B4A),
                        Color(0xFF103571),
                        Color(0xFF07163B),
                    )
                )
            )
            .padding(horizontal = 24.dp, vertical = 32.dp),
        contentAlignment = Alignment.Center,
    ) {
        Column(
            horizontalAlignment = Alignment.CenterHorizontally,
            verticalArrangement = Arrangement.spacedBy(12.dp),
        ) {
            Text(
                text = "Sign in to Quantura",
                color = Color.White,
                style = MaterialTheme.typography.headlineMedium,
                fontWeight = FontWeight.Bold,
            )
            Text(
                text = "Sync forecasts, save screeners, and unlock personalized alerts.",
                color = Color.White.copy(alpha = 0.86f),
                textAlign = TextAlign.Center,
                style = MaterialTheme.typography.bodyMedium,
                modifier = Modifier.padding(horizontal = 8.dp),
            )

            Spacer(modifier = Modifier.height(8.dp))

            Button(
                onClick = onGoogle,
                enabled = !isBusy,
                modifier = Modifier.fillMaxWidth(),
                shape = RoundedCornerShape(12.dp),
                colors = ButtonDefaults.buttonColors(
                    containerColor = Color.White,
                    contentColor = Color(0xFF111827),
                ),
            ) {
                Text("Continue with Google", fontWeight = FontWeight.SemiBold)
            }

            OutlinedButton(
                onClick = onEmail,
                enabled = !isBusy,
                modifier = Modifier.fillMaxWidth(),
                shape = RoundedCornerShape(12.dp),
                border = ButtonDefaults.outlinedButtonBorder.copy(width = 1.dp),
                colors = ButtonDefaults.outlinedButtonColors(contentColor = Color.White),
            ) {
                Text("Continue with Email", fontWeight = FontWeight.SemiBold)
            }

            TextButton(onClick = onNotNow, enabled = !isBusy) {
                Text("Not now", color = Color.White.copy(alpha = 0.92f))
            }

            if (errorText.isNotBlank()) {
                Text(
                    text = errorText,
                    color = Color(0xFFFFCDD2),
                    textAlign = TextAlign.Center,
                    style = MaterialTheme.typography.bodySmall,
                )
            }

            if (isBusy) {
                Spacer(modifier = Modifier.height(4.dp))
                CircularProgressIndicator(color = Color.White)
            }
        }
    }
}

@Composable
private fun EmailSignInDialog(
    email: String,
    password: String,
    isBusy: Boolean,
    onEmailChanged: (String) -> Unit,
    onPasswordChanged: (String) -> Unit,
    onDismiss: () -> Unit,
    onContinue: () -> Unit,
) {
    AlertDialog(
        onDismissRequest = onDismiss,
        title = { Text("Continue with Email") },
        text = {
            Column(verticalArrangement = Arrangement.spacedBy(8.dp)) {
                OutlinedTextField(
                    value = email,
                    onValueChange = onEmailChanged,
                    label = { Text("Email") },
                    singleLine = true,
                    enabled = !isBusy,
                    modifier = Modifier.fillMaxWidth(),
                )
                OutlinedTextField(
                    value = password,
                    onValueChange = onPasswordChanged,
                    label = { Text("Password") },
                    singleLine = true,
                    enabled = !isBusy,
                    visualTransformation = PasswordVisualTransformation(),
                    modifier = Modifier.fillMaxWidth(),
                )
                Text(
                    text = "If this email is new, Quantura links it to your current app session.",
                    style = MaterialTheme.typography.bodySmall,
                    color = MaterialTheme.colorScheme.onSurfaceVariant,
                )
            }
        },
        confirmButton = {
            TextButton(onClick = onContinue, enabled = !isBusy) {
                Text("Continue")
            }
        },
        dismissButton = {
            TextButton(onClick = onDismiss, enabled = !isBusy) {
                Text("Cancel")
            }
        },
    )
}

private suspend fun FirebaseUser.awaitIdToken(forceRefresh: Boolean): String =
    suspendCancellableCoroutine { continuation ->
        getIdToken(forceRefresh)
            .addOnSuccessListener { result ->
                val token = result.token?.trim().orEmpty()
                if (token.isEmpty()) {
                    continuation.resumeWithException(IllegalStateException("Firebase ID token is empty."))
                } else {
                    continuation.resume(token)
                }
            }
            .addOnFailureListener { error ->
                continuation.resumeWithException(error)
            }
    }

private fun String.escapeForSingleQuotedJs(): String =
    replace("\\", "\\\\")
        .replace("'", "\\'")
        .replace("\n", "\\n")
        .replace("\r", "\\r")

private fun isTrustedHost(host: String?): Boolean {
    val normalized = host?.trim()?.lowercase().orEmpty()
    if (normalized.isEmpty()) return false
    if (normalized == "quantura.studio") return true
    if (normalized == "www.quantura.studio") return true
    if (normalized == "quantura-e2e3d.web.app") return true
    if (normalized == "quantura-e2e3d.firebaseapp.com") return true
    if (normalized == "localhost") return true
    if (normalized == "127.0.0.1") return true
    return normalized.endsWith(".quantura.studio")
}

private fun isTrustedUri(uri: Uri?): Boolean {
    if (uri == null) return false
    val scheme = uri.scheme?.lowercase().orEmpty()
    if (scheme !in setOf("https", "http")) return false
    return isTrustedHost(uri.host)
}
