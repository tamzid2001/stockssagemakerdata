from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

from .api import ApiError, Metaculus, utcnow
from .llm import FreeLLM, FreeQuota
from .questions import already_submitted, context, payload, unpack
from .research import evidence
from .time_series import definition, forecast

MARKER = "QUANTURA_BOT_RECORD_V1"


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


def record_text(record: dict) -> str:
    answer = record["answer"]
    sources = "\n".join(f"- {s['url']} (SHA256 {s['sha256']})" for s in answer.get("sources", []))
    return (f"## Quantura autonomous forecast\n\nQuestion ID: {record['question_id']}\n\n"
            f"Generated UTC: {record['generated_at']}\n\nEngine: {answer.get('engine', 'free_llm')}\n\n"
            f"Model(s): {answer.get('model') or ', '.join(answer.get('models', []))}\n\n"
            f"{answer.get('reasoning') or answer.get('reason', 'Abstained')}\n\n"
            f"Sources actually retrieved:\n{sources or 'Question background and resolution criteria only; no external source retrieved.'}\n\n"
            "This is an automated research estimate, with uncertainty and no human adjustment.\n\n"
            f"<!-- {MARKER} {json.dumps(record, sort_keys=True, separators=(',', ':'))} -->")


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
            latest = find_question(api, question.id_of_post, question.id_of_question)
            if latest and already_submitted(latest):
                return "submitted_reconciled"
        raise
    return "submitted"


def run(args, api=None, llm_factory=FreeLLM):
    api = api or Metaculus(os.environ["METACULUS_TOKEN"])
    identity = api.identity()
    if args.submit and os.environ.get("METACULUS_PARTICIPATION_FORM_CONFIRMED") != "true":
        raise RuntimeError("PARTICIPATION_FORM_CONFIRMATION_REQUIRED")
    registry = json.loads(Path(__file__).with_name("series_registry.json").read_text())
    projects = ([{"id": "bot-testing-area", "name": "Bot testing area"}] if args.scope == "test" else api.competitions())
    log("metaculus_discovery", bot=identity["username"], bot_id=identity["id"], engine=args.engine,
        competitions=[{"id": p["id"], "name": p["name"]} for p in projects], free_only=True)
    questions, seen = [], set()
    counts = Counter()
    for project in projects:
        for post in api.posts(project["id"]):
            try:
                parsed = unpack(post)
            except (ValueError, KeyError, TypeError):
                counts["schema_deferred"] += 1
                continue
            for q in parsed:
                if q.id_of_question not in seen:
                    seen.add(q.id_of_question)
                    questions.append(q)
    # Forecast short-lived/new questions first, then persistent backlog. All
    # open pages and group members are discovered, even beyond the batch limit.
    questions.sort(key=lambda q: (q.close_time or datetime.max.replace(tzinfo=timezone.utc), q.id_of_question))
    specs = {q.id_of_question: definition(q, registry) for q in questions}
    llm, time_series_ids, attempted, summaries = None, [], 0, []
    if args.engine == "llm":
        time_series_ids = [q.id_of_question for q in questions if specs[q.id_of_question] and not already_submitted(q)]
    for q in questions:
        if args.question_ids and q.id_of_question not in args.question_ids:
            continue
        if already_submitted(q):
            counts["already_submitted"] += 1
            continue
        spec = specs[q.id_of_question]
        if args.engine == "llm" and spec:
            counts["time_series_queued"] += 1
            continue
        if args.engine == "time-series" and not spec:
            continue
        if attempted >= args.max_questions:
            counts["batch_deferred"] += 1
            continue
        attempted += 1
        status = "deferred"
        try:
            comments = api.comments(q.id_of_post, identity["id"])
            record = saved_record(comments, identity["id"], q.id_of_question)
            if not record:
                if args.engine == "time-series":
                    answer = forecast(q, spec, args.ensemble_python)
                else:
                    if llm is None:
                        llm = llm_factory(os.environ["OPENROUTER_API_KEY"])
                    answer = llm.forecast(q, evidence(q))
                record = {"question_id": q.id_of_question, "generated_at": utcnow().isoformat(), "answer": answer,
                          "question_sha256": question_hash(q)}
                if args.submit:
                    # Persist the exact generation before forecasting; this also
                    # ensures every submitted forecast has its required reasoning.
                    api.post("comments/create/", {"text": record_text(record), "on_post": q.id_of_post,
                             "parent": None, "included_forecast": False, "is_private": True})
            if record["answer"].get("abstain"):
                status = "abstained"
            else:
                payload(q, record["answer"])
                status = submit(api, q, record) if args.submit else "validated_not_submitted"
        except FreeQuota as error:
            counts["free_quota_deferred"] += 1
            log("metaculus_free_capacity", status=str(error), question_id=q.id_of_question, paid_fallback=False)
            break
        except (ApiError, ValueError, KeyError, TypeError, RuntimeError) as error:
            # Never log response bodies, prompts, headers, credentials, or private
            # generated distributions in public Actions logs/artifacts.
            status = "deferred_error"
            log("metaculus_question_error", question_id=q.id_of_question, error_type=type(error).__name__,
                upstream_status=error.status if isinstance(error, ApiError) else None)
        counts[status] += 1
        summary = {"question_id": q.id_of_question, "post_id": q.id_of_post, "type": q.question_type,
                   "engine": "quantura_time_series" if spec else "free_llm", "status": status,
                   "url": q.page_url}
        summaries.append(summary)
        log("metaculus_question", **summary)
    result = {"bot": identity["username"], "at": utcnow().isoformat(), "free_only": True,
              "discovered_open_questions": len(questions), "counts": dict(counts), "questions": summaries,
              "time_series_question_ids": time_series_ids}
    Path(args.report).parent.mkdir(parents=True, exist_ok=True)
    Path(args.report).write_text(json.dumps(result, indent=2) + "\n")
    if os.environ.get("GITHUB_OUTPUT"):
        with open(os.environ["GITHUB_OUTPUT"], "a") as output:
            output.write("time_series_ids=" + json.dumps(time_series_ids[:args.max_questions]) + "\n")
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a") as summary:
            summary.write(f"## Quantura Metaculus — {args.engine}\n\nBot: {identity['username']}. Free LLMs only.\n\n"
                          f"Discovered {len(questions)} open questions.\n\n| Status | Count |\n|---|---:|\n")
            for status, count in sorted(counts.items()):
                summary.write(f"| {status} | {count} |\n")
    log("metaculus_summary", **{k: v for k, v in result.items() if k != "questions"})
    return result


def main():
    parser = argparse.ArgumentParser(description="Discover and forecast bot-eligible Metaculus competitions")
    parser.add_argument("--scope", choices=["test", "competitions"], default="test")
    parser.add_argument("--engine", choices=["llm", "time-series"], default="llm")
    parser.add_argument("--submit", action="store_true")
    parser.add_argument("--max-questions", type=int, default=4)
    parser.add_argument("--question-ids", default="[]")
    parser.add_argument("--ensemble-python", default=".ensemble-env/bin/python")
    parser.add_argument("--report", default="artifacts/metaculus-summary.json")
    args = parser.parse_args()
    args.question_ids = json.loads(args.question_ids)
    if not 1 <= args.max_questions <= 20 or not isinstance(args.question_ids, list) or any(type(i) is not int for i in args.question_ids):
        parser.error("Invalid batch size or question IDs")
    try:
        run(args)
    except (ApiError, FreeQuota, RuntimeError, KeyError) as error:
        log("metaculus_worker_failed", error_type=type(error).__name__,
            upstream_status=error.status if isinstance(error, ApiError) else None)
        raise SystemExit(1)


if __name__ == "__main__":
    main()
