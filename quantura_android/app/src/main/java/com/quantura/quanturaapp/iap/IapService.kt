package com.quantura.quanturaapp.iap

import android.app.Activity

/**
 * In-App Purchase service interface.
 * Implement with Play Billing for Android, StoreKit for iOS.
 *
 * Before use: Create product IDs in Play Console, add license testers.
 * See docs/release-checklist.md for setup steps.
 */
interface IapService {
    /**
     * Fetched product offerings (subscriptions, one-time).
     */
    data class Offering(
        val id: String,
        val title: String,
        val description: String,
        val price: String,
        val productId: String,
    )

    /**
     * Purchase result.
     */
    sealed class PurchaseResult {
        data class Success(val productId: String) : PurchaseResult()
        data class Cancelled(val productId: String) : PurchaseResult()
        data class Error(val message: String) : PurchaseResult()
    }

    /**
     * Check if user has an active entitlement (e.g. pro subscription).
     */
    suspend fun hasEntitlement(entitlementId: String): Boolean

    /**
     * Fetch available offerings.
     */
    suspend fun getOfferings(): List<Offering>

    /**
     * Launch purchase flow. Must be called from an Activity.
     */
    suspend fun purchase(activity: Activity, productId: String): PurchaseResult

    /**
     * Restore previous purchases (e.g. after reinstall).
     */
    suspend fun restorePurchases(): Boolean
}
