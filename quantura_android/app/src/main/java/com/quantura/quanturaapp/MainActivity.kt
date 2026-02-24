package com.quantura.quanturaapp

import android.Manifest
import android.content.Intent
import android.content.pm.PackageManager
import android.os.Build
import android.os.Bundle
import android.webkit.WebChromeClient
import android.webkit.WebResourceRequest
import android.webkit.WebView
import android.webkit.WebViewClient
import androidx.activity.ComponentActivity
import androidx.activity.compose.setContent
import androidx.activity.enableEdgeToEdge
import androidx.activity.result.contract.ActivityResultContracts
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.wrapContentHeight
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Surface
import androidx.compose.runtime.Composable
import androidx.compose.ui.Modifier
import androidx.compose.ui.viewinterop.AndroidView
import androidx.core.content.ContextCompat
import androidx.lifecycle.lifecycleScope
import com.google.android.gms.auth.api.signin.GoogleSignIn
import com.google.android.gms.auth.api.signin.GoogleSignInClient
import com.google.android.gms.auth.api.signin.GoogleSignInOptions
import com.google.android.gms.common.api.ApiException
import com.google.android.gms.tasks.OnCompleteListener
import com.google.firebase.auth.FirebaseAuth
import com.google.firebase.auth.GoogleAuthProvider
import com.google.firebase.messaging.FirebaseMessaging
import com.quantura.quanturaapp.ads.AdManager
import com.quantura.quanturaapp.ads.BannerAdView
import com.quantura.quanturaapp.messaging.QuanturaFcmTokenHolder
import com.quantura.quanturaapp.messaging.QuanturaMessagingService
import com.quantura.quanturaapp.messaging.NativePersonalizedNotificationManager
import com.quantura.quanturaapp.web.QuanturaJavascriptBridge
import kotlinx.coroutines.launch
import org.json.JSONObject

class MainActivity : ComponentActivity() {
    private val appContainer by lazy { (application as QuanturaApplication).container }
    private val firebaseAuth by lazy { FirebaseAuth.getInstance() }
    private var webViewRef: WebView? = null
    private var googleSignInClient: GoogleSignInClient? = null
    private var pendingNativeAuthRequestId: String? = null

    private val googleSignInLauncher =
        registerForActivityResult(ActivityResultContracts.StartActivityForResult()) { result ->
            val requestId = pendingNativeAuthRequestId
            pendingNativeAuthRequestId = null
            if (requestId.isNullOrBlank()) return@registerForActivityResult
            handleGoogleSignInResult(requestId, result.data)
        }

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)

        enableEdgeToEdge()
        requestNotificationPermission()
        fetchFcmToken()
        NativePersonalizedNotificationManager.start(this)

        lifecycleScope.launch {
            appContainer.remoteConfigManager.fetchAndActivate()
            appContainer.adManager.primeAds(this@MainActivity)
        }

        val deepLinkUrl = intent?.getStringExtra(QuanturaMessagingService.EXTRA_DEEP_LINK_URL)
        val loadUrl = deepLinkUrl ?: "https://quantura-e2e3d.web.app/"

        setContent {
            MaterialTheme {
                Surface(modifier = Modifier.fillMaxSize()) {
                    Column(modifier = Modifier.fillMaxSize()) {
                        QuanturaWebViewScreen(
                            modifier = Modifier.weight(1f),
                            activity = this@MainActivity,
                            startUrl = loadUrl,
                            adManager = appContainer.adManager,
                            onNativeAuthRequest = { provider, requestId ->
                                handleNativeAuthRequest(provider, requestId)
                            },
                            onNativeSignOutRequest = { requestId ->
                                handleNativeSignOut(requestId)
                            },
                            onReady = { webView -> webViewRef = webView },
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
                }
            }
        }
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

    private fun handleNativeAuthRequest(provider: String, requestId: String) {
        val cleanProvider = provider.trim().lowercase()
        val cleanRequestId = requestId.trim()
        if (cleanProvider.isEmpty() || cleanRequestId.isEmpty()) return

        if (cleanProvider != "google") {
            emitNativeAuthResult(
                requestId = cleanRequestId,
                provider = cleanProvider,
                ok = false,
                error = "Native $cleanProvider sign-in is not configured in this build."
            )
            return
        }

        val client = resolveGoogleSignInClient()
        if (client == null) {
            emitNativeAuthResult(
                requestId = cleanRequestId,
                provider = cleanProvider,
                ok = false,
                error = "Google sign-in is not configured (missing default_web_client_id)."
            )
            return
        }

        pendingNativeAuthRequestId = cleanRequestId
        googleSignInLauncher.launch(client.signInIntent)
    }

    private fun handleGoogleSignInResult(requestId: String, data: Intent?) {
        val account = try {
            GoogleSignIn.getSignedInAccountFromIntent(data).getResult(ApiException::class.java)
        } catch (error: Exception) {
            emitNativeAuthResult(
                requestId = requestId,
                provider = "google",
                ok = false,
                error = error.message ?: "Google sign-in failed."
            )
            return
        }

        val accountIdToken = account.idToken?.trim().orEmpty()
        if (accountIdToken.isEmpty()) {
            emitNativeAuthResult(
                requestId = requestId,
                provider = "google",
                ok = false,
                error = "Google did not return an ID token."
            )
            return
        }

        val credential = GoogleAuthProvider.getCredential(accountIdToken, null)
        firebaseAuth.signInWithCredential(credential)
            .addOnSuccessListener { authResult ->
                authResult.user?.getIdToken(true)
                    ?.addOnSuccessListener { tokenResult ->
                        val firebaseIdToken = tokenResult.token?.trim().orEmpty()
                        if (firebaseIdToken.isEmpty()) {
                            emitNativeAuthResult(
                                requestId = requestId,
                                provider = "google",
                                ok = false,
                                error = "Firebase did not return an ID token."
                            )
                            return@addOnSuccessListener
                        }
                        emitNativeAuthResult(
                            requestId = requestId,
                            provider = "google",
                            ok = true,
                            idToken = firebaseIdToken
                        )
                    }
                    ?.addOnFailureListener { error ->
                        emitNativeAuthResult(
                            requestId = requestId,
                            provider = "google",
                            ok = false,
                            error = error.message ?: "Unable to fetch Firebase ID token."
                        )
                    }
            }
            .addOnFailureListener { error ->
                emitNativeAuthResult(
                    requestId = requestId,
                    provider = "google",
                    ok = false,
                    error = error.message ?: "Native Firebase sign-in failed."
                )
            }
    }

    private fun handleNativeSignOut(requestId: String) {
        try {
            firebaseAuth.signOut()
            resolveGoogleSignInClient()?.signOut()
            if (requestId.isNotBlank()) {
                emitNativeAuthResult(requestId = requestId, provider = "google", ok = true)
            }
        } catch (error: Exception) {
            if (requestId.isNotBlank()) {
                emitNativeAuthResult(
                    requestId = requestId,
                    provider = "google",
                    ok = false,
                    error = error.message ?: "Native sign-out failed."
                )
            }
        }
    }

    private fun emitNativeAuthResult(
        requestId: String,
        provider: String,
        ok: Boolean,
        idToken: String = "",
        error: String = "",
    ) {
        val payload = JSONObject().apply {
            put("requestId", requestId)
            put("provider", provider)
            put("ok", ok)
            if (idToken.isNotBlank()) put("idToken", idToken)
            if (error.isNotBlank()) put("error", error)
        }
        emitJsEvent("quantura:native-auth-result", payload)
    }

    private fun emitNativeFcmToken(token: String) {
        val escaped = token.replace("\\", "\\\\").replace("'", "\\'")
        emitJs(
            "window.__NATIVE_FCM_TOKEN__='$escaped';if(typeof window.__quanturaNativeTokenReady==='function')window.__quanturaNativeTokenReady('$escaped');"
        )
    }

    private fun emitJsEvent(eventName: String, detail: JSONObject) {
        emitJs("window.dispatchEvent(new CustomEvent('$eventName',{detail:${detail.toString()}}));")
    }

    private fun emitJs(script: String) {
        webViewRef?.post {
            webViewRef?.evaluateJavascript(script, null)
        }
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

    companion object {
        private const val REQUEST_NOTIFICATION_PERMISSION = 9001
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
        webViewRef?.removeJavascriptInterface("QuanturaBridge")
        webViewRef?.destroy()
        webViewRef = null
        super.onDestroy()
    }

    override fun onNewIntent(intent: Intent) {
        super.onNewIntent(intent)
        val deepLinkUrl = intent.getStringExtra(QuanturaMessagingService.EXTRA_DEEP_LINK_URL).orEmpty()
        if (deepLinkUrl.isNotBlank()) {
            webViewRef?.loadUrl(deepLinkUrl)
        }
    }
}

@Composable
private fun QuanturaWebViewScreen(
    modifier: Modifier = Modifier,
    activity: ComponentActivity,
    startUrl: String,
    adManager: AdManager,
    onNativeAuthRequest: (provider: String, requestId: String) -> Unit,
    onNativeSignOutRequest: (requestId: String) -> Unit,
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
                settings.userAgentString = "${settings.userAgentString} QuanturaAndroidApp/1.0"

                webChromeClient = WebChromeClient()
                webViewClient = object : WebViewClient() {
                    override fun shouldOverrideUrlLoading(view: WebView?, request: WebResourceRequest?): Boolean = false

                    override fun onPageFinished(view: WebView?, url: String?) {
                        super.onPageFinished(view, url)
                        val token = QuanturaFcmTokenHolder.getToken(context)
                        evaluateJavascript(
                            """
                            window.__QUANTURA_NATIVE_APP__=true;
                            window.__QUANTURA_NATIVE_PLATFORM__='android';
                            window.dispatchEvent(new CustomEvent('quantura:native-runtime-ready',{detail:{platform:'android'}}));
                            """.trimIndent(),
                            null
                        )
                        if (!token.isNullOrBlank()) {
                            val escaped = token.replace("\\", "\\\\").replace("'", "\\'")
                            evaluateJavascript(
                                "window.__NATIVE_FCM_TOKEN__='$escaped';if(typeof window.__quanturaNativeTokenReady==='function')window.__quanturaNativeTokenReady('$escaped');",
                                null
                            )
                        }
                    }
                }

                addJavascriptInterface(
                    QuanturaJavascriptBridge(
                        activity = activity,
                        adManager = adManager,
                        onNativeAuthRequest = onNativeAuthRequest,
                        onNativeSignOutRequest = onNativeSignOutRequest,
                    ),
                    "QuanturaBridge"
                )
                loadUrl(startUrl)
                onReady(this)
            }
        },
        update = { webView ->
            if (webView.url.isNullOrBlank()) {
                webView.loadUrl(startUrl)
            }
            onReady(webView)
        },
    )
}
