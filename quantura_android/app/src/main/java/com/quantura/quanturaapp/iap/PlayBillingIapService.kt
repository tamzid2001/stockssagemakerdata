package com.quantura.quanturaapp.iap

import android.app.Activity
import com.android.billingclient.api.AcknowledgePurchaseParams
import com.android.billingclient.api.BillingClient
import com.android.billingclient.api.BillingClientStateListener
import com.android.billingclient.api.BillingFlowParams
import com.android.billingclient.api.BillingResult
import com.android.billingclient.api.ProductDetails
import com.android.billingclient.api.Purchase
import com.android.billingclient.api.PurchasesUpdatedListener
import com.android.billingclient.api.QueryProductDetailsParams
import com.android.billingclient.api.QueryPurchasesParams
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import kotlin.coroutines.resume
import kotlinx.coroutines.suspendCancellableCoroutine

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
        private val PRODUCT_IDS = listOf("quantura_pro_monthly")
    }

    @Volatile
    private var billingClient: BillingClient? = null
    @Volatile
    private var latestPurchases: List<Purchase> = emptyList()

    suspend fun initialize(): Boolean = withContext(Dispatchers.Main) {
        val client = billingClient ?: BillingClient.newBuilder(applicationContext)
            .setListener(this@PlayBillingIapService)
            .enablePendingPurchases()
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
            val details = queryProductDetails(listOf(productId)).firstOrNull()
                ?: return@withContext IapService.PurchaseResult.Error("Product $productId not found.")

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

            val result = billingClient?.launchBillingFlow(activity, flowParams)
                ?: return@withContext IapService.PurchaseResult.Error("Billing client is not initialized.")

            when (result.responseCode) {
                BillingClient.BillingResponseCode.OK -> IapService.PurchaseResult.Success(productId)
                BillingClient.BillingResponseCode.USER_CANCELED -> IapService.PurchaseResult.Cancelled(productId)
                else -> IapService.PurchaseResult.Error(result.debugMessage.ifBlank { "Unable to start purchase flow." })
            }
        }

    override suspend fun restorePurchases(): Boolean = withContext(Dispatchers.Main) {
        val purchases = queryActivePurchases()
        purchases.isNotEmpty()
    }

    override fun onPurchasesUpdated(result: BillingResult, purchases: MutableList<Purchase>?) {
        if (result.responseCode != BillingClient.BillingResponseCode.OK) return
        latestPurchases = purchases?.toList().orEmpty()

        // Scaffold only: acknowledge unacknowledged purchases to avoid automatic refunds in test flows.
        latestPurchases.forEach { purchase ->
            if (purchase.purchaseState == Purchase.PurchaseState.PURCHASED && !purchase.isAcknowledged) {
                billingClient?.acknowledgePurchase(
                    AcknowledgePurchaseParams.newBuilder().setPurchaseToken(purchase.purchaseToken).build()
                ) { _ -> }
            }
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
            client.queryProductDetailsAsync(params) { billingResult, productDetailsList ->
                if (billingResult.responseCode == BillingClient.BillingResponseCode.OK) {
                    continuation.resume(productDetailsList.orEmpty())
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
                    continuation.resume(purchases.orEmpty())
                } else {
                    continuation.resume(emptyList())
                }
            }
        }
    }
}
