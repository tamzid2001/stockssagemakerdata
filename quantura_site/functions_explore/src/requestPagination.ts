import { FieldPath, Timestamp, type Firestore } from "firebase-admin/firestore";

type Position = { v: 1; user: string; id: string; seconds: number; nanoseconds: number };
export function decodeRequestCursor(value: unknown, user: string): Position | null {
  if (value === undefined || value === "") return null;
  try {
    if (typeof value !== "string" || value.length > 2048 || !/^[A-Za-z0-9_-]+$/.test(value)) throw new Error();
    const p = JSON.parse(Buffer.from(value, "base64url").toString("utf8"));
    if (p.v !== 1 || p.user !== user || typeof p.id !== "string" || !p.id || p.id.includes("/") || p.id.length > 1500 || !Number.isInteger(p.seconds) || !Number.isInteger(p.nanoseconds)) throw new Error();
    new Timestamp(p.seconds, p.nanoseconds);
    return p;
  } catch { throw new Error("invalid_request_cursor"); }
}

// Scope the query to the authenticated user. The cursor is a position, never
// a document path or an authorization grant. Timestamp + ID also handles ties.
export async function scanRequestPage(db: Firestore, user: string, cursor: unknown, limit: number) {
  const position = decodeRequestCursor(cursor, user);
  let query = db.collection("users").doc(user).collection("requests")
    .orderBy("updatedAt", "desc").orderBy(FieldPath.documentId(), "desc");
  if (position) query = query.startAfter(new Timestamp(position.seconds, position.nanoseconds), position.id);
  const scanLimit = Math.max(1, Math.min(40, limit));
  const snapshot = await query.limit(scanLimit + 1).get();
  return { docs: snapshot.docs.slice(0, scanLimit), more: snapshot.size > scanLimit };
}

// Advance over deleted/nonmatching entries as well as returned rows. An empty
// filtered page can still have a continuation; it must not hide older records.
export function selectRequestPage<T>(docs: Array<{ id: string; data: Record<string, unknown> }>, user: string, limit: number, more: boolean, project: (doc: { id: string; data: Record<string, unknown> }) => T | null) {
  const items: T[] = [];
  let consumed = 0;
  for (const doc of docs) {
    consumed++;
    const item = project(doc);
    if (item !== null) items.push(item);
    if (items.length === limit) break;
  }
  const last = docs[consumed - 1];
  const hasMore = Boolean(last) && (more || consumed < docs.length);
  const time = last?.data.updatedAt as Timestamp;
  const nextCursor = hasMore ? Buffer.from(JSON.stringify({v: 1, user, id: last.id, seconds: time.seconds, nanoseconds: time.nanoseconds} satisfies Position)).toString("base64url") : null;
  return { items, next_cursor: nextCursor, has_more: hasMore };
}
