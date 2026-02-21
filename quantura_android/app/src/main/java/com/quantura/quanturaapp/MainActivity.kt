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
import androidx.core.content.ContextCompat
import androidx.activity.enableEdgeToEdge
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.wrapContentHeight
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Surface
import androidx.compose.runtime.Composable
import androidx.compose.ui.Modifier
import androidx.compose.ui.Modifier
import androidx.compose.ui.viewinterop.AndroidView
import androidx.lifecycle.lifecycleScope
import com.google.android.gms.tasks.OnCompleteListener
import com.google.firebase.messaging.FirebaseMessaging
import com.quantura.quanturaapp.ads.AdManager
import com.quantura.quanturaapp.ads.BannerAdView
import com.quantura.quanturaapp.messaging.QuanturaFcmTokenHolder
import com.quantura.quanturaapp.messaging.QuanturaMessagingService
import com.quantura.quanturaapp.web.QuanturaJavascriptBridge
import kotlinx.coroutines.launch

class MainActivity : ComponentActivity() {
    private val appContainer by lazy { (application as QuanturaApplication).container }
    private var webViewRef: WebView? = null

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)

        enableEdgeToEdge()
        requestNotificationPermission()
        fetchFcmToken()

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
                            remoteConfigManager = appContainer.remoteConfigManager,
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
}

@Composable
private fun QuanturaWebViewScreen(
    modifier: Modifier = Modifier,
    activity: ComponentActivity,
    startUrl: String,
    adManager: AdManager,
    remoteConfigManager: com.quantura.quanturaapp.config.RemoteConfigManager,
    onReady: (WebView) -> Unit,
) {
    AndroidView(
        modifier = modifier,
        modifier = Modifier.fillMaxSize(),
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
                        if (!token.isNullOrBlank()) {
                            val escaped = token.replace("'", "\\'")
                            evaluateJavascript("window.__NATIVE_FCM_TOKEN__='$escaped';if(typeof window.__quanturaNativeTokenReady==='function')window.__quanturaNativeTokenReady('$escaped');", null)
                        }
                    }
                }

                addJavascriptInterface(QuanturaJavascriptBridge(activity, adManager), "QuanturaBridge")
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
