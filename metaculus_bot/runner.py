from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
from collections import Counter, defaultdict, deque
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .api import ApiError, Metaculus, RateLimited, competition_policy, eligible_post, eligible_project, post_projects, utcnow
from .checkpoint import Checkpoint, PrivateNoteLimit
from .llm import FreeLLM, FreeQuota
from .questions import already_submitted, context, payload, unpack
from .research import evidence
from .time_series import definition, forecast

MARKER = "QUANTURA_BOT_RECORD_V1"
NOTE_MARKER = "QUANTURA_BOT_NOTE_V2"


def question_hash(question):
    return hashlib.sha256(json.dumps(context(question), sort_keys=True).encode()).hexdigest()


def log(event: str, **fields):
    print(json.dumps({"event": event, **fields}, sort_keys=True), flush=True)


def find_question(api, post_id, question_id):
    return next((q for q in unpack(api.get(f"posts/{post_id}/")) if q.id_of_question == question_id), None)


def saved_record(comments: list[dict], user_id: int, question_id: int) -> dict | None:
    for comment in reversed(comments):
        if (comment.get("author") or {}).get("id") != user_id:
            continue
        text = comment.get("text") or ""
        match = re.search(r'<!-- ' + MARKER + r' (\{.*\}) -->', text, re.DOTALL)
        if match:
            try:
                record = json.loads(match[1])
                if record["question_id"] == question_id:
                    return record
            except (json.JSONDecodeError, KeyError, TypeError):
                continue
    return None


def answer_hash(record):
    return hashlib.sha256(json.dumps(record["answer"], sort_keys=True, allow_nan=False).encode()).hexdigest()


def saved_note(comments, user_id, question_id):
    for comment in reversed(comments):
        if (comment.get("author") or {}).get("id") != user_id:
            continue
        match = re.search(r'<!-- ' + NOTE_MARKER + r' (\{[^\n]*\}) -->', comment.get("text") or "")
        if match:
            try:
                note = json.loads(match[1])
                if note.get("question_id") == question_id:
                    return note
            except (json.JSONDecodeError, TypeError):
                continue
    return None


def concise_reasoning(text):
    # Bound the posted explanation, including legacy pending generations. Do
    # not regenerate or adjust a forecast just to shorten its private note.
    original = " ".join(str(text).split())
    short = " ".join(original.split()[:100])[:1000]
    if len(short) < len(original):
        endings = [m.end() for m in re.finditer(r'[.!?](?:\s|$)', short)]
        short = short[:endings[-1]].strip() if endings and endings[-1] > len(short) / 2 else short[:999].rsplit(" ", 1)[0] + "…"
    return short


def record_text(record: dict) -> str:
    answer = record["answer"]
    models = str(answer.get("model") or ", ".join(answer.get("models", [])) or answer.get("engine", "free_llm"))[:120]
    urls = list(dict.fromkeys(s["url"] for s in answer.get("sources", []) if isinstance(s.get("url"), str) and len(s["url"]) <= 300))[:3]
    sources = "\n\nSources: " + "; ".join(urls) if urls else ""
    note = {"question_id": record["question_id"], "answer_sha256": answer_hash(record)}
    return (f"Automated forecast ({models}).\n\n"
            f"{concise_reasoning(answer.get('reasoning') or answer.get('reason', ''))}{sources}\n\n"
            f"<!-- {NOTE_MARKER} {json.dumps(note, sort_keys=True, separators=(',', ':'))} -->")


def question_order(questions, memberships, handled, replay, now=None):
    """Recover exact answers first; protect short deadlines; balance the backlog.

    Selection depends on coverage and closing times, never forecast probabilities
    or scores. A question shared by multiple tournaments is processed once.
    """
    now = now or utcnow()
    key = lambda q: (q.close_time or datetime.max.replace(tzinfo=timezone.utc), q.id_of_question)
    ordered = sorted(questions, key=key)
    first = [q for q in ordered if q.id_of_question in replay]
    urgent = [q for q in ordered if q.id_of_question not in handled and q.id_of_question not in replay and
              q.close_time and q.close_time <= now + timedelta(hours=2)]
    selected = {q.id_of_question for q in first + urgent}
    queues, coverage = defaultdict(deque), Counter()
    for q in ordered:
        members = memberships[q.id_of_question]
        if q.id_of_question in handled or q.id_of_question in selected:
            coverage.update(members)
        else:
            for member in members:
                queues[member].append(q)
    balanced = []
    while queues:
        for member in list(queues):
            while queues[member] and queues[member][0].id_of_question in selected:
                queues[member].popleft()
            if not queues[member]:
                del queues[member]
        if not queues:
            break
        member = min(queues, key=lambda p: (coverage[p], key(queues[p][0]), str(p)))
        q = queues[member].popleft()
        selected.add(q.id_of_question)
        coverage.update(memberships[q.id_of_question])
        balanced.append(q)
    return first + urgent + balanced + [q for q in ordered if q.id_of_question not in selected]


def submit(api, question, record):
    fresh = find_question(api, question.id_of_post, question.id_of_question)
    if fresh is None:
        return "closed_before_submission"
    if already_submitted(fresh):
        return "already_submitted"
    if record["answer"].get("abstain"):
        return "abstained"
    # Re-validate against the latest exact criteria and scale. An edited question
    # must not receive a distribution calculated for its previous definition.
    if question_hash(fresh) != record["question_sha256"]:
        return "criteria_changed_deferred"
    body = payload(fresh, record["answer"])
    try:
        api.post("questions/forecast/", [body])
    except ApiError as error:
        if error.service == "METACULUS_WRITE_UNCERTAIN" or error.status in {408, 500, 502, 503, 504}:
            # Never immediately repost a possibly accepted write. Verify own
            # history; the next run can retry the SAME persisted generation.
            if question.id_of_question in api.own_forecasts([question.id_of_question]):
                return "submitted_reconciled"
        raise
    # Feed metadata can lag; the authenticated own-history endpoint is the
    # authoritative receipt. Never call an unconfirmed write a confirmed fill.
    return ("submitted" if question.id_of_question in api.own_forecasts([question.id_of_question])
            else "submission_unconfirmed")


def run(args, api=None, llm_factory=FreeLLM):
    if getattr(args, "submit", False) and os.environ.get("METACULUS_SUBMISSIONS_APPROVED") != "true":
        raise RuntimeError("METACULUS_SUBMISSIONS_NOT_APPROVED")
    checkpoint = Checkpoint(getattr(args, "checkpoint", None), os.environ.get("METACULUS_TOKEN"))
    if getattr(args, "submit", False) and not checkpoint.path:
        raise RuntimeError("SUBMISSION_CHECKPOINT_REQUIRED")
    if checkpoint.remaining():
        raise RateLimited(checkpoint.remaining())
    api = api or Metaculus(os.environ["METACULUS_TOKEN"])
    identity = api.identity()
    expected_bot = os.environ.get("METACULUS_BOT_ID")
    if expected_bot and str(identity["id"]) != expected_bot:
        raise RuntimeError("METACULUS_BOT_ID_MISMATCH")
    if args.submit and os.environ.get("METACULUS_PARTICIPATION_FORM_CONFIRMED") != "true":
        raise RuntimeError("PARTICIPATION_FORM_CONFIRMATION_REQUIRED")
    registry = json.loads(Path(__file__).with_name("series_registry.json").read_text())
    post_ids = getattr(args, "post_ids", [])
    if post_ids:
        # The first job already selected these posts. Recheck their current
        # tournament eligibility and forecast permissions without scanning all
        # tournaments a second time.
        discovered_posts = [api.get(f"posts/{i}/") for i in sorted(set(post_ids))]
        pairs = [(p["id"], post, p) for post in discovered_posts if eligible_post(post, args.scope)
                 for p in post_projects(post) if (p.get("slug") == "bot-testing-area" if args.scope == "test" else eligible_project(p))]
        discovered_posts = [(project_id, post) for project_id, post, _ in pairs]
        projects = list({p["id"]: {**p, "name": p.get("name") or str(p["id"])} for _, _, p in pairs}.values())
    else:
        projects = ([{"id": "bot-testing-area", "name": "Bot testing area"}] if args.scope == "test" else api.competitions())
        discovered_posts = [(project["id"], post) for project in projects for post in api.posts(project["id"])]
    log("metaculus_discovery", bot=identity["username"], bot_id=identity["id"], engine=args.engine,
        competitions=[{"id": p["id"], "name": p["name"], **competition_policy(p)} for p in projects], free_only=True)
    questions, seen, memberships = [], set(), defaultdict(set)
    counts = Counter()
    for project_id, post in discovered_posts:
        try:
            parsed = unpack(post)
        except (ValueError, KeyError, TypeError):
            counts["schema_deferred"] += 1
            continue
        for q in parsed:
            if args.question_ids and q.id_of_question not in args.question_ids:
                continue
            memberships[q.id_of_question].add(project_id)
            if q.id_of_question not in seen:
                seen.add(q.id_of_question)
                questions.append(q)
    # Forecast short-lived/new questions first, then persistent backlog. All
    # open pages and group members are discovered, even beyond the batch limit.
    questions.sort(key=lambda q: (q.close_time or datetime.max.replace(tzinfo=timezone.utc), q.id_of_question))
    own = api.own_forecasts([q.id_of_question for q in questions])
    for question_id in own:
        if checkpoint.get(question_id):
            checkpoint.remove(question_id)
    specs = {q.id_of_question: definition(q, registry) for q in questions}
    llm, time_series_ids, attempted, submitted_count, note_attempts, summaries = None, [], 0, 0, 0, []
    if args.engine == "llm":
        time_series_ids = [q.id_of_question for q in questions if specs[q.id_of_question] and q.id_of_question not in own]
    candidates = [q for q in questions if q.id_of_question not in own and not already_submitted(q)
                  and bool(specs[q.id_of_question]) == (args.engine == "time-series")]
    # One paginated own-comments read per job, shared by all posts/group children.
    # Saved abstentions do not consume the new-question budget or starve backlog.
    comments = api.comments(None, identity["id"]) if candidates else []
    records = {q.id_of_question: saved_record(comments, identity["id"], q.id_of_question) for q in candidates}
    notes = {q.id_of_question: saved_note(comments, identity["id"], q.id_of_question) for q in candidates}
    handled = own | {q.id_of_question for q in questions if already_submitted(q)} | {i for i, r in records.items() if r or checkpoint.get(i) or notes[i]}
    replay = {q.id_of_question for q in candidates if
              (record := records[q.id_of_question] or checkpoint.get(q.id_of_question)) and not record["answer"].get("abstain")}
    questions = question_order(questions, memberships, handled, replay)
    if args.engine == "llm":
        time_series_ids = [q.id_of_question for q in questions if specs[q.id_of_question] and q.id_of_question not in own]
    coverage = [{"id": p["id"], "name": p["name"], **competition_policy(p),
                 "open_questions": sum(p["id"] in memberships[q.id_of_question] for q in questions),
                 "previously_submitted": sum(p["id"] in memberships[i] for i in own),
                 "submitted_this_run": 0, "reasoning_notes_saved_this_run": 0} for p in projects]
    for row in coverage:
        log("metaculus_tournament_coverage", **row)
    for q in questions:
        if args.question_ids and q.id_of_question not in args.question_ids:
            continue
        if q.id_of_question in own or already_submitted(q):
            counts["already_submitted"] += 1
            continue
        spec = specs[q.id_of_question]
        if args.engine == "llm" and spec:
            counts["time_series_queued"] += 1
            continue
        if args.engine == "time-series" and not spec:
            continue
        record = records.get(q.id_of_question)
        persisted = record is not None
        if persisted:
            checkpoint.remove(q.id_of_question)
        record = record or checkpoint.get(q.id_of_question)
        note = notes.get(q.id_of_question)
        if note and not record:
            counts["note_without_exact_recovery_deferred"] += 1
            continue
        if submitted_count >= args.max_questions or (not record and attempted >= args.max_questions):
            counts["batch_deferred"] += 1
            continue
        status = "deferred"
        stage = "comments_read"
        try:
            if not record:
                attempted += 1
                if args.engine == "time-series":
                    stage = "time_series_inference"
                    answer = forecast(q, spec, args.ensemble_python)
                else:
                    stage = "free_llm_generation"
                    if llm is None:
                        llm = llm_factory(os.environ["OPENROUTER_API_KEY"])
                    answer = llm.forecast(q, evidence(q))
                record = {"question_id": q.id_of_question, "generated_at": utcnow().isoformat(), "answer": answer,
                          "question_sha256": question_hash(q)}
                checkpoint.put(record)
            if record["answer"].get("abstain"):
                status = "abstained"
                # Preserve the decision privately; no comment or repeated model
                # call is needed for an abstention.
            else:
                stage = "distribution_validation"
                payload(q, record["answer"])
                if note and note.get("answer_sha256") != answer_hash(record):
                    raise RuntimeError("PRIVATE_NOTE_RECOVERY_MISMATCH")
            if args.submit and not record["answer"].get("abstain") and not persisted and not note:
                stage = "reasoning_write"
                if record.get("note_write_state"):
                    raise RuntimeError("PRIVATE_NOTE_CONFIRMATION_PENDING")
                if note_attempts >= 2:
                    raise PrivateNoteLimit("PRIVATE_NOTE_BATCH_LIMIT")
                text = record_text(record)
                checkpoint.reserve_note_attempt()
                note_attempts += 1
                record["note_write_state"] = "uncertain"
                checkpoint.put(record)
                # An encrypted checkpoint preserves this exact answer even if
                # the private-note request is throttled or its response is lost.
                try:
                    api.post("comments/create/", {"text": text, "on_post": q.id_of_post,
                             "parent": None, "included_forecast": False, "is_private": True})
                except ApiError as error:
                    if error.status in {400, 401, 403, 404, 429}:
                        record.pop("note_write_state", None)
                        checkpoint.put(record)
                    raise
                record["note_write_state"] = "confirmed"
                checkpoint.put(record)
                # Keep the exact answer in the encrypted checkpoint until the
                # forecast is confirmed. Posted notes contain no recovery JSON.
                for row in coverage:
                    if row["id"] in memberships[q.id_of_question]:
                        row["reasoning_notes_saved_this_run"] += 1
            if not record["answer"].get("abstain"):
                stage = "forecast_submission"
                status = submit(api, q, record) if args.submit else "validated_not_submitted"
                if status in {"submitted", "submitted_reconciled", "submission_unconfirmed"}:
                    submitted_count += 1
                if status in {"submitted", "submitted_reconciled"}:
                    checkpoint.remove(q.id_of_question)
                    for row in coverage:
                        if row["id"] in memberships[q.id_of_question]:
                            row["submitted_this_run"] += 1
        except RateLimited as error:
            checkpoint.defer(error.retry_after)
            counts["api_rate_limit_deferred"] += 1
            time_series_ids = []
            log("metaculus_api_capacity", retry_after_seconds=round(error.retry_after, 1), question_id=q.id_of_question,
                status="deferred_until_next_scheduled_run")
            break
        except FreeQuota as error:
            counts["free_quota_deferred"] += 1
            log("metaculus_free_capacity", status=str(error), question_id=q.id_of_question, paid_fallback=False)
            break
        except PrivateNoteLimit as error:
            counts["private_note_limit_deferred"] += 1
            log("metaculus_private_note_capacity", error_code=str(error), question_id=q.id_of_question,
                status="deferred_until_next_capacity_window")
            time_series_ids = []
            break
        except (ApiError, ValueError, KeyError, TypeError, RuntimeError) as error:
            if isinstance(error, ApiError) and error.status in {401, 403}:
                # A permission denial stops the entire batch, not just this
                # question. Repeated writes to a restricted account add no value.
                raise
            # Never log response bodies, prompts, headers, credentials, or private
            # generated distributions in public Actions logs/artifacts.
            status = "deferred_error"
            log("metaculus_question_error", question_id=q.id_of_question, error_type=type(error).__name__,
                stage=stage, error_code=error.service if isinstance(error, ApiError) else
                str(error.args[0]) if error.args and re.fullmatch(r'[A-Za-z0-9_]{1,80}',str(error.args[0])) else None,
                upstream_status=error.status if isinstance(error, ApiError) else None)
        counts[status] += 1
        summary = {"question_id": q.id_of_question, "post_id": q.id_of_post, "type": q.question_type,
                   "engine": "quantura_time_series" if spec else "free_llm", "status": status,
                   "url": q.page_url, "competition_ids": sorted(memberships[q.id_of_question], key=str)}
        summaries.append(summary)
        log("metaculus_question", **summary)
    result = {"bot": identity["username"], "at": utcnow().isoformat(), "free_only": True,
              "discovered_open_questions": len(questions), "counts": dict(counts), "questions": summaries,
              "time_series_question_ids": time_series_ids, "competitions": coverage}
    time_series_post_ids = sorted({q.id_of_post for q in questions if q.id_of_question in time_series_ids[:args.max_questions]})
    result["time_series_post_ids"] = time_series_post_ids
    checkpoint.save()
    write_report(args, result)
    return result


def write_report(args, result):
    Path(args.report).parent.mkdir(parents=True, exist_ok=True)
    Path(args.report).write_text(json.dumps(result, indent=2) + "\n")
    if os.environ.get("GITHUB_OUTPUT"):
        with open(os.environ["GITHUB_OUTPUT"], "a") as output:
            output.write("time_series_ids=" + json.dumps(result.get("time_series_question_ids", [])[:args.max_questions]) + "\n")
            output.write("time_series_post_ids=" + json.dumps(result.get("time_series_post_ids", [])) + "\n")
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a") as summary:
            summary.write(f"## Quantura Metaculus — {args.engine}\n\nBot: {result['bot']}. Free LLMs only.\n\n"
                          f"Discovered {result['discovered_open_questions']} open questions.\n\n| Status | Count |\n|---|---:|\n")
            for status, count in sorted(result["counts"].items()):
                summary.write(f"| {status} | {count} |\n")
            if result.get("competitions"):
                summary.write("\n### Tournament coverage\n\n| Tournament | Bot prize eligibility | Open | Previously submitted | New confirmed | New reasoning notes |\n|---|---|---:|---:|---:|---:|\n")
                for row in result["competitions"]:
                    summary.write(f"| {row['name']} | {row['bot_prize_eligibility']} | {row['open_questions']} | {row['previously_submitted']} | {row['submitted_this_run']} | {row['reasoning_notes_saved_this_run']} |\n")
    log("metaculus_summary", **{k: v for k, v in result.items() if k != "questions"})


def main():
    parser = argparse.ArgumentParser(description="Discover and forecast bot-eligible Metaculus competitions")
    parser.add_argument("--scope", choices=["test", "competitions"], default="test")
    parser.add_argument("--engine", choices=["llm", "time-series"], default="llm")
    parser.add_argument("--submit", action="store_true")
    parser.add_argument("--max-questions", type=int, default=4)
    parser.add_argument("--question-ids", default="[]")
    parser.add_argument("--post-ids", default="[]")
    parser.add_argument("--checkpoint", default=None)
    parser.add_argument("--ensemble-python", default=".ensemble-env/bin/python")
    parser.add_argument("--report", default="artifacts/metaculus-summary.json")
    args = parser.parse_args()
    args.question_ids = json.loads(args.question_ids)
    args.post_ids = json.loads(args.post_ids)
    if (not 1 <= args.max_questions <= 20 or any(not isinstance(ids, list) or len(ids) > 100 or
            any(type(i) is not int or i <= 0 for i in ids) for ids in [args.question_ids, args.post_ids])):
        parser.error("Invalid batch size or question IDs")
    try:
        run(args)
    except RateLimited as error:
        checkpoint = Checkpoint(args.checkpoint, os.environ.get("METACULUS_TOKEN"))
        checkpoint.defer(error.retry_after)
        # Quota exhaustion is recoverable. Preserve a visible operational report
        # and let the next scheduled run resume exact saved generations.
        write_report(args, {"bot": "Quantura", "at": utcnow().isoformat(), "free_only": True,
                            "discovered_open_questions": 0, "counts": {"api_rate_limit_deferred": 1},
                            "questions": [], "retry_after_seconds": round(error.retry_after, 1)})
    except (ApiError, FreeQuota, RuntimeError, KeyError) as error:
        log("metaculus_worker_failed", error_type=type(error).__name__,
            upstream_status=error.status if isinstance(error, ApiError) else None,
            error_code=error.service if isinstance(error, ApiError) else
            str(error.args[0]) if error.args and re.fullmatch(r'[A-Za-z0-9_]{1,80}', str(error.args[0])) else None,
            recovery="operator_review_required" if isinstance(error, ApiError) and error.status in {401, 403} else None)
        raise SystemExit(1)


if __name__ == "__main__":
    main()
