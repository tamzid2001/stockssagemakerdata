from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable

from .types import AnalysisMode


@dataclass(frozen=True)
class RagDocument:
    document_id: str
    text: str
    publication_timestamp: str
    ingestion_timestamp: str
    source: str
    ticker: str | None = None
    document_type: str = "research"


def _utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed.astimezone(timezone.utc)


class PointInTimeRetriever:
    def __init__(self, documents: Iterable[RagDocument], *, max_results: int = 5):
        self.documents = list(documents)
        self.max_results = max(0, int(max_results))

    def retrieve(self, query: str, *, ticker: str, as_of: str, mode: AnalysisMode) -> list[dict]:
        cutoff = _utc(as_of)
        query_terms = {token.lower() for token in query.split() if len(token) > 2}
        candidates: list[tuple[int, RagDocument]] = []
        for document in self.documents:
            publication = _utc(document.publication_timestamp)
            ingestion = _utc(document.ingestion_timestamp)
            if mode is AnalysisMode.BACKTEST and (publication > cutoff or ingestion > cutoff):
                continue
            if document.ticker and document.ticker.upper() != ticker.upper():
                continue
            haystack = document.text.lower()
            score = sum(term in haystack for term in query_terms) + (2 if document.ticker else 0)
            if score:
                candidates.append((score, document))
        candidates.sort(key=lambda item: (-item[0], item[1].publication_timestamp, item[1].document_id))
        return [
            {
                "document_id": document.document_id,
                "excerpt": document.text[:1200],
                "publication_timestamp": document.publication_timestamp,
                "ingestion_timestamp": document.ingestion_timestamp,
                "source": document.source,
                "ticker": document.ticker,
                "document_type": document.document_type,
            }
            for _, document in candidates[: self.max_results]
        ]
