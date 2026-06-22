import json
import math
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
INDEX_DIR = ROOT / "ai-agent" / "rag-index"
INDEX_FILE = INDEX_DIR / "chunks.jsonl"

STOP_WORDS = {
    "about",
    "after",
    "also",
    "and",
    "are",
    "can",
    "does",
    "for",
    "from",
    "give",
    "how",
    "into",
    "the",
    "this",
    "what",
    "when",
    "where",
    "which",
    "with",
}


def retrieve_project_context(question: str, limit: int = 5) -> list[dict[str, str | int | float]]:
    if not INDEX_FILE.exists():
        return []

    query_terms = tokenize(question)
    if not query_terms:
        return []

    scored_chunks: list[dict[str, str | int | float]] = []
    with INDEX_FILE.open("r", encoding="utf-8") as stream:
        for line in stream:
            chunk = json.loads(line)
            chunk_terms = chunk.get("terms", {})
            score = score_chunk(query_terms, chunk_terms)
            if score <= 0:
                continue

            scored_chunks.append(
                {
                    "score": round(score, 4),
                    "path": chunk["path"],
                    "start_line": chunk["start_line"],
                    "end_line": chunk["end_line"],
                    "text": chunk["text"],
                }
            )

    scored_chunks.sort(key=lambda item: item["score"], reverse=True)
    return scored_chunks[:limit]


def format_rag_context(chunks: list[dict[str, str | int | float]]) -> str:
    if not chunks:
        return ""

    sections: list[str] = []
    for index, chunk in enumerate(chunks, start=1):
        sections.append(
            f"[Source {index}: {chunk['path']}:{chunk['start_line']}-{chunk['end_line']}]\n"
            f"{chunk['text']}"
        )
    return "\n\n".join(sections)


def format_rag_sources(chunks: list[dict[str, str | int | float]]) -> str:
    if not chunks:
        return ""
    return "\n".join(
        f"- {chunk['path']}:{chunk['start_line']}-{chunk['end_line']}" for chunk in chunks
    )


def has_rag_index() -> bool:
    return INDEX_FILE.exists()


def tokenize(text: str) -> list[str]:
    tokens = re.findall(r"[a-zA-Z][a-zA-Z0-9_]+", text.lower())
    return [token for token in tokens if len(token) >= 3 and token not in STOP_WORDS]


def score_chunk(query_terms: list[str], chunk_terms: dict[str, int]) -> float:
    score = 0.0
    unique_query_terms = set(query_terms)
    chunk_length = max(sum(chunk_terms.values()), 1)

    for term in unique_query_terms:
        count = chunk_terms.get(term, 0)
        if count:
            score += 1.0 + math.log(count)

    # Slightly prefer focused chunks over huge chunks.
    return score / math.log(chunk_length + 10)
