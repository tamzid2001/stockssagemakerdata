package com.quantura.quanturaapp.iap

import android.app.Activity
import com.android.billingclient.api.AcknowledgePurchaseParams
import com.android.billingclient.api.BillingClient
import com.android.billingclient.api.BillingClientStateListener
import com.android.billingclient.api.BillingFlowParams
import com.android.billingclient.api.BillingResult
import com.android.billingclient.api.PendingPurchasesParams
import com.android.billingclient.api.ProductDetails
import com.android.billingclient.api.Purchase
import com.android.billingclient.api.PurchasesUpdatedListener
import com.android.billingclient.api.QueryProductDetailsParams
import com.android.billingclient.api.QueryPurchasesParams
import kotlinx.coroutines.CancellableContinuation
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withTimeoutOrNull
import kotlinx.coroutines.withContext
import kotlinx.coroutines.suspendCancellableCoroutine
import kotlin.coroutines.resume

/**
 * Play Billing scaffold:
 * - Initializes BillingClient
 * - Queries products
 * - Launches purchase flow
 * - Handles purchase updates
 *
 * TODO: wire server-side purchase verification before granting entitlements.
 */
class PlayBillingIapService(
    private val applicationContext: android.content.Context,
) : IapService, PurchasesUpdatedListener {

    companion object {
        private const val ENTITLEMENT_QUANTURA_PRO = "quantura_pro"
        const val DEFAULT_PRODUCT_ID = "quanturapro"
        private val PRODUCT_IDS = listOf(
            "goplan",
            "premium",
            "quanturapro",
            "quanturabusiness",
            "goplanyearly",
            "annualplusplan",
            "annualbusinessplan",
        )
        private val IOS_TO_ANDROID_ALIASES = mapOf(
            "pro" to "quanturapro",
            "businessplan" to "quanturabusiness",
            "annualgoplan" to "goplanyearly",
        )

        fun normalizeRequestedProductId(rawProductId: String): String {
            val trimmed = rawProductId.trim()
            if (trimmed.isEmpty()) return DEFAULT_PRODUCT_ID
            if (PRODUCT_IDS.contains(trimmed)) return trimmed
            val alias = IOS_TO_ANDROID_ALIASES[trimmed.lowercase()]
            return alias ?: DEFAULT_PRODUCT_ID
        }
    }

    @Volatile
    private var billingClient: BillingClient? = null
    @Volatile
    private var latestPurchases: List<Purchase> = emptyList()
    @Volatile
    private var pendingPurchaseContinuation: CancellableContinuation<IapService.PurchaseResult>? = null
    @Volatile
    private var pendingPurchaseProductId: String = ""

    suspend fun initialize(): Boolean = withContext(Dispatchers.Main) {
        val client = billingClient ?: BillingClient.newBuilder(applicationContext)
            .setListener(this@PlayBillingIapService)
            .enablePendingPurchases(
                PendingPurchasesParams.newBuilder()
                    .enableOneTimeProducts()
                    .build()
            )
            .build()
            .also { billingClient = it }

        suspendCancellableCoroutine { continuation ->
            client.startConnection(object : BillingClientStateListener {
                override fun onBillingSetupFinished(result: BillingResult) {
                    if (result.responseCode == BillingClient.BillingResponseCode.OK) {
                        continuation.resume(true)
                    } else {
                        continuation.resume(false)
                    }
                }

                override fun onBillingServiceDisconnected() {
                    // BillingClient auto-reconnect is handled by initialize() call sites.
                    if (continuation.isActive) continuation.resume(false)
                }
            })
        }
    }

    override suspend fun hasEntitlement(entitlementId: String): Boolean = withContext(Dispatchers.Main) {
        if (entitlementId != ENTITLEMENT_QUANTURA_PRO) return@withContext false
        val purchases = queryActivePurchases()
        purchases.any { purchase ->
            purchase.purchaseState == Purchase.PurchaseState.PURCHASED &&
                purchase.products.any { it in PRODUCT_IDS }
        }
    }

    override suspend fun getOfferings(): List<IapService.Offering> = withContext(Dispatchers.Main) {
        val details = queryProductDetails(PRODUCT_IDS)
        if (details.isEmpty()) return@withContext emptyList()
        details.map { product ->
            val offer = product.subscriptionOfferDetails?.firstOrNull()
            val price = offer?.pricingPhases?.pricingPhaseList?.firstOrNull()?.formattedPrice
                ?: product.oneTimePurchaseOfferDetails?.formattedPrice
                ?: "Unavailable"
            IapService.Offering(
                id = product.productId,
                title = product.title.ifBlank { product.name.ifBlank { "Quantura Pro" } },
                description = product.description.ifBlank { "Premium subscription" },
                price = price,
                productId = product.productId,
            )
        }
    }

    override suspend fun purchase(activity: Activity, productId: String): IapService.PurchaseResult =
        withContext(Dispatchers.Main) {
            val normalizedProductId = normalizeRequestedProductId(productId)
            val details = queryProductDetails(listOf(normalizedProductId)).firstOrNull()
                ?: return@withContext IapService.PurchaseResult.Error("Product $normalizedProductId not found.")
            if (pendingPurchaseContinuation?.isActive == true) {
                return@withContext IapService.PurchaseResult.Error("Another Google Play purchase is already in progress.")
            }

            val offerToken = details.subscriptionOfferDetails?.firstOrNull()?.offerToken
            val productDetailsParams = BillingFlowParams.ProductDetailsParams.newBuilder()
                .setProductDetails(details)
                .apply {
                    if (!offerToken.isNullOrBlank()) setOfferToken(offerToken)
                }
                .build()

            val flowParams = BillingFlowParams.newBuilder()
                .setProductDetailsParamsList(listOf(productDetailsParams))
                .build()
            withTimeoutOrNull(180_000L) {
                suspendCancellableCoroutine<IapService.PurchaseResult> { continuation ->
                    pendingPurchaseContinuation = continuation
                    pendingPurchaseProductId = normalizedProductId
                    continuation.invokeOnCancellation {
                        clearPendingPurchase(continuation)
                    }

                    val client = billingClient
                    if (client == null) {
                        resumePendingPurchase(IapService.PurchaseResult.Error("Billing client is not initialized."))
                        return@suspendCancellableCoroutine
                    }

                    val result = client.launchBillingFlow(activity, flowParams)
                    when (result.responseCode) {
                        BillingClient.BillingResponseCode.OK -> Unit
                        BillingClient.BillingResponseCode.USER_CANCELED ->
                            resumePendingPurchase(IapService.PurchaseResult.Cancelled(normalizedProductId))
                        else ->
                            resumePendingPurchase(
                                IapService.PurchaseResult.Error(
                                    result.debugMessage.ifBlank { "Unable to start purchase flow." }
                                )
                            )
                    }
                }
            } ?: IapService.PurchaseResult.Error("Timed out waiting for Google Play purchase confirmation.")
        }

    override suspend fun restorePurchases(): Boolean = withContext(Dispatchers.Main) {
        val purchases = queryActivePurchases()
        purchases.isNotEmpty()
    }

    override fun onPurchasesUpdated(result: BillingResult, purchases: MutableList<Purchase>?) {
        if (result.responseCode == BillingClient.BillingResponseCode.USER_CANCELED) {
            resumePendingPurchase(IapService.PurchaseResult.Cancelled(pendingPurchaseProductId.ifBlank { DEFAULT_PRODUCT_ID }))
            return
        }
        if (result.responseCode != BillingClient.BillingResponseCode.OK) {
            resumePendingPurchase(
                IapService.PurchaseResult.Error(result.debugMessage.ifBlank { "Google Play purchase failed." })
            )
            return
        }
        latestPurchases = purchases?.toList().orEmpty()
        if (latestPurchases.isEmpty()) {
            resumePendingPurchase(IapService.PurchaseResult.Error("Google Play did not return a purchase record."))
            return
        }

        val purchase = latestPurchases.firstOrNull { candidate ->
            val expectedProductId = pendingPurchaseProductId
            expectedProductId.isBlank() || candidate.products.any { it == expectedProductId }
        } ?: latestPurchases.first()
        val resolvedProductId = purchase.products.firstOrNull { it in PRODUCT_IDS }
            ?: pendingPurchaseProductId.ifBlank { DEFAULT_PRODUCT_ID }
        val resolvedOrderId = purchase.orderId?.trim().orEmpty()

        // Scaffold only: acknowledge unacknowledged purchases to avoid automatic refunds in test flows.
        latestPurchases.forEach { purchase ->
            if (purchase.purchaseState == Purchase.PurchaseState.PURCHASED && !purchase.isAcknowledged) {
                billingClient?.acknowledgePurchase(
                    AcknowledgePurchaseParams.newBuilder().setPurchaseToken(purchase.purchaseToken).build()
                ) { _ -> }
            }
        }

        when (purchase.purchaseState) {
            Purchase.PurchaseState.PURCHASED ->
                resumePendingPurchase(IapService.PurchaseResult.Success(resolvedProductId, resolvedOrderId))
            Purchase.PurchaseState.PENDING ->
                resumePendingPurchase(IapService.PurchaseResult.Pending(resolvedProductId, resolvedOrderId))
            else ->
                resumePendingPurchase(IapService.PurchaseResult.Error("Google Play purchase did not complete."))
        }
    }

    private fun resumePendingPurchase(result: IapService.PurchaseResult) {
        val continuation = pendingPurchaseContinuation
        pendingPurchaseContinuation = null
        pendingPurchaseProductId = ""
        if (continuation?.isActive == true) {
            continuation.resume(result)
        }
    }

    private fun clearPendingPurchase(target: CancellableContinuation<IapService.PurchaseResult>) {
        if (pendingPurchaseContinuation === target) {
            pendingPurchaseContinuation = null
            pendingPurchaseProductId = ""
        }
    }

    private suspend fun queryProductDetails(productIds: List<String>): List<ProductDetails> {
        if (productIds.isEmpty()) return emptyList()
        if (!initialize()) return emptyList()

        val queryProducts = productIds.map { id ->
            QueryProductDetailsParams.Product.newBuilder()
                .setProductId(id)
                .setProductType(BillingClient.ProductType.SUBS)
                .build()
        }

        val params = QueryProductDetailsParams.newBuilder()
            .setProductList(queryProducts)
            .build()

        return suspendCancellableCoroutine { continuation ->
            val client = billingClient
            if (client == null) {
                continuation.resume(emptyList())
                return@suspendCancellableCoroutine
            }
            client.queryProductDetailsAsync(params) { billingResult, productDetailsResult ->
                if (billingResult.responseCode == BillingClient.BillingResponseCode.OK) {
                    continuation.resume(productDetailsResult.productDetailsList)
                } else {
                    continuation.resume(emptyList())
                }
            }
        }
    }

    private suspend fun queryActivePurchases(): List<Purchase> {
        if (!initialize()) return emptyList()
        return suspendCancellableCoroutine { continuation ->
            val client = billingClient
            if (client == null) {
                continuation.resume(emptyList())
                return@suspendCancellableCoroutine
            }
            val params = QueryPurchasesParams.newBuilder()
                .setProductType(BillingClient.ProductType.SUBS)
                .build()
            client.queryPurchasesAsync(params) { result, purchases ->
                if (result.responseCode == BillingClient.BillingResponseCode.OK) {
                    continuation.resume(purchases ?: emptyList())
                } else {
                    continuation.resume(emptyList())
                }
            }
        }
    }
}
