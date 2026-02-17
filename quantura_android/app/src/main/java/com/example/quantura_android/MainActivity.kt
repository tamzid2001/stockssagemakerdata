package com.example.quantura_android

import android.os.Bundle
import android.webkit.WebChromeClient
import android.webkit.WebResourceRequest
import android.webkit.WebView
import android.webkit.WebViewClient
import androidx.activity.ComponentActivity
import androidx.activity.compose.setContent
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Surface
import androidx.compose.runtime.Composable
import androidx.compose.ui.Modifier
import androidx.compose.ui.viewinterop.AndroidView
import androidx.lifecycle.lifecycleScope
import com.example.quantura_android.ads.AdManager
import com.example.quantura_android.web.QuanturaJavascriptBridge
import kotlinx.coroutines.launch

class MainActivity : ComponentActivity() {
    private val appContainer by lazy { (application as QuanturaApplication).container }
    private var webViewRef: WebView? = null

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)

        lifecycleScope.launch {
            appContainer.remoteConfigManager.fetchAndActivate()
            appContainer.adManager.primeAds(this@MainActivity)
        }

        setContent {
            MaterialTheme {
                Surface(modifier = Modifier.fillMaxSize()) {
                    QuanturaWebViewScreen(
                        activity = this@MainActivity,
                        startUrl = "https://quantura-e2e3d.web.app/",
                        adManager = appContainer.adManager,
                        onReady = { webView -> webViewRef = webView },
                    )
                }
            }
        }
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
    activity: ComponentActivity,
    startUrl: String,
    adManager: AdManager,
    onReady: (WebView) -> Unit,
) {
    AndroidView(
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
                    override fun shouldOverrideUrlLoading(view: WebView?, request: WebResourceRequest?): Boolean {
                        return false
                    }
                }

                // Inject Android bridge as window.QuanturaBridge for the web wrapper.
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
