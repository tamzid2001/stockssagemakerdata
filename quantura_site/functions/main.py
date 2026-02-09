from __future__ import annotations

import os
from typing import Any

import firebase_admin
from firebase_admin import credentials, firestore
from firebase_functions import https_fn
from firebase_functions.options import set_global_options

set_global_options(max_instances=10)

SERVICE_ACCOUNT_PATH = os.environ.get(
    "SERVICE_ACCOUNT_PATH",
    os.path.join(os.path.dirname(__file__), "serviceAccountKey.json"),
)
ADMIN_EMAIL = "tamzid257@gmail.com"
ALLOWED_STATUSES = {"pending", "in_progress", "fulfilled", "cancelled"}

if not firebase_admin._apps:
    cred = credentials.Certificate(SERVICE_ACCOUNT_PATH)
    firebase_admin.initialize_app(cred)

db = firestore.client()


def _require_auth(req: https_fn.CallableRequest) -> dict[str, Any]:
    if req.auth is None:
        raise https_fn.HttpsError(
            https_fn.FunctionsErrorCode.UNAUTHENTICATED,
            "Sign in is required to perform this action.",
        )
    return req.auth.token or {}


@https_fn.on_call()
def create_order(req: https_fn.CallableRequest) -> dict[str, Any]:
    token = _require_auth(req)
    data = req.data or {}

    product = str(data.get("product") or "Deep Forecast")
    currency = str(data.get("currency") or "USD")
    price = data.get("price")
    if not isinstance(price, (int, float)):
        price = 349

    order = {
        "userId": req.auth.uid,
        "userEmail": token.get("email"),
        "product": product,
        "price": price,
        "currency": currency,
        "status": "pending",
        "fulfillmentNotes": "",
        "createdAt": firestore.SERVER_TIMESTAMP,
        "updatedAt": firestore.SERVER_TIMESTAMP,
    }

    doc_ref = db.collection("orders").document()
    doc_ref.set(order)

    return {"orderId": doc_ref.id}


@https_fn.on_call()
def update_order_status(req: https_fn.CallableRequest) -> dict[str, Any]:
    token = _require_auth(req)
    email = token.get("email")

    if email != ADMIN_EMAIL:
        raise https_fn.HttpsError(
            https_fn.FunctionsErrorCode.PERMISSION_DENIED,
            "Admin access required.",
        )

    data = req.data or {}
    order_id = data.get("orderId")
    status = data.get("status")
    notes = data.get("notes", "")

    if not order_id:
        raise https_fn.HttpsError(
            https_fn.FunctionsErrorCode.INVALID_ARGUMENT,
            "Order ID is required.",
        )

    if status not in ALLOWED_STATUSES:
        raise https_fn.HttpsError(
            https_fn.FunctionsErrorCode.INVALID_ARGUMENT,
            "Invalid order status.",
        )

    order_ref = db.collection("orders").document(order_id)
    snapshot = order_ref.get()
    if not snapshot.exists:
        raise https_fn.HttpsError(
            https_fn.FunctionsErrorCode.NOT_FOUND,
            "Order not found.",
        )

    update_payload: dict[str, Any] = {
        "status": status,
        "fulfillmentNotes": notes,
        "updatedAt": firestore.SERVER_TIMESTAMP,
    }

    if status == "fulfilled":
        update_payload["fulfilledAt"] = firestore.SERVER_TIMESTAMP

    order_ref.update(update_payload)

    return {"orderId": order_id, "status": status}
