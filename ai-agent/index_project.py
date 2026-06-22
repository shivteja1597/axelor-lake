import json
import os
from pathlib import Path

from tools.rag_tools import INDEX_DIR, INDEX_FILE, tokenize

ROOT = Path(__file__).resolve().parents[1]

INCLUDE_PATHS = [
    "modules/axelor-lake/src/main/java",
    "modules/axelor-lake/src/main/resources/views",
    "modules/axelor-lake/src/main/resources/domains",
    "dbt_lakehouse",
    "python-ml",
    "ai-agent/generated-docs",
    "ai-agent/tools",
]

INCLUDE_FILES = [
    "Jenkinsfile",
    "README.md",
    "ai-agent/README.md",
    "ai-agent/app.py",
]

ALLOWED_SUFFIXES = {
    ".java",
    ".xml",
    ".py",
    ".sql",
    ".yml",
    ".yaml",
    ".properties",
    ".md",
}

SKIP_DIRS = {
    ".git",
    ".gradle",
    ".venv",
    "__pycache__",
    "build",
    "dbt_venv",
    "node_modules",
    "rag-index",
}

SKIP_FILES = {
    ".env",
}

MAX_CHARS = 1400
OVERLAP_LINES = 4


def main() -> int:
    INDEX_DIR.mkdir(parents=True, exist_ok=True)
    files = collect_files()
    chunks = []

    for path in files:
        chunks.extend(chunk_file(path))

    with INDEX_FILE.open("w", encoding="utf-8") as stream:
        for chunk in chunks:
            stream.write(json.dumps(chunk, ensure_ascii=True) + "\n")

    print(f"Indexed {len(files)} files into {len(chunks)} chunks.")
    print(f"Index file: {INDEX_FILE.relative_to(ROOT)}")
    return 0


def collect_files() -> list[Path]:
    files: list[Path] = []

    for relative_dir in INCLUDE_PATHS:
        root = ROOT / relative_dir
        if not root.exists():
            continue
        for directory, dirnames, filenames in os.walk(root):
            dirnames[:] = [name for name in dirnames if name not in SKIP_DIRS]
            for filename in filenames:
                path = Path(directory) / filename
                if should_include_file(path):
                    files.append(path)

    for relative_file in INCLUDE_FILES:
        path = ROOT / relative_file
        if path.exists() and should_include_file(path):
            files.append(path)

    return sorted(set(files), key=lambda item: str(item))


def should_include_file(path: Path) -> bool:
    if path.name in SKIP_FILES:
        return False
    if path.suffix.lower() not in ALLOWED_SUFFIXES:
        return False
    return not any(part in SKIP_DIRS for part in path.parts)


def chunk_file(path: Path) -> list[dict[str, object]]:
    try:
        lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
    except OSError:
        return []

    chunks: list[dict[str, object]] = []
    start = 0
    while start < len(lines):
        text_lines: list[str] = []
        end = start
        char_count = 0

        while end < len(lines) and char_count < MAX_CHARS:
            line = lines[end]
            text_lines.append(line)
            char_count += len(line) + 1
            end += 1

        text = "\n".join(text_lines).strip()
        if text:
            terms = term_counts(text)
            chunks.append(
                {
                    "path": str(path.relative_to(ROOT)),
                    "start_line": start + 1,
                    "end_line": end,
                    "text": text,
                    "terms": terms,
                }
            )

        if end >= len(lines):
            break
        start = max(end - OVERLAP_LINES, start + 1)

    return chunks


def term_counts(text: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for token in tokenize(text):
        counts[token] = counts.get(token, 0) + 1
    return counts


if __name__ == "__main__":
    raise SystemExit(main())
