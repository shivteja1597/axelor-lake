import os
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
GENERATED_DOCS_DIR = ROOT / "ai-agent" / "generated-docs"

ALLOWED_SUFFIXES = {".java", ".xml", ".py", ".sql", ".yml", ".yaml", ".properties", ".md"}
STOP_WORDS = {
    "about",
    "code",
    "file",
    "find",
    "for",
    "how",
    "show",
    "the",
    "where",
    "which",
}
SKIP_DIRS = {
    ".git",
    ".gradle",
    ".venv",
    "build",
    "dbt_venv",
    "node_modules",
    "__pycache__",
}


def search_project_files(query: str, limit: int = 8) -> list[dict[str, str]]:
    terms = build_search_terms(query)
    if not terms:
        return []

    results: list[dict[str, str]] = []
    for directory, dirnames, filenames in os.walk(ROOT):
        dirnames[:] = [name for name in dirnames if name not in SKIP_DIRS]

        for filename in filenames:
            if len(results) >= limit:
                return results

            path = Path(directory) / filename
            if path.suffix.lower() not in ALLOWED_SUFFIXES:
                continue

            try:
                lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
            except OSError:
                continue

            for line_number, line in enumerate(lines, start=1):
                lowered = line.lower()
                if all(term in lowered for term in terms[:3]):
                    results.append(
                        {
                            "path": str(path.relative_to(ROOT)),
                            "line": str(line_number),
                            "text": line.strip()[:240],
                        }
                    )
                    break

    return results


def build_search_terms(query: str) -> list[str]:
    terms: list[str] = []
    for raw_term in query.lower().replace("?", " ").replace(":", " ").split():
        term = raw_term.strip(".,'\"()[]{}")
        if len(term) < 3 or term in STOP_WORDS:
            continue
        if term.endswith("ed") and len(term) > 5:
            term = term[:-2]
        if term.endswith("ing") and len(term) > 6:
            term = term[:-3]
        terms.append(term)
    return terms


def format_search_results(results: list[dict[str, str]]) -> str:
    if not results:
        return "I could not find matching project files for that query."
    return "\n".join(
        f"{item['path']}:{item['line']} - {item['text']}" for item in results
    )


def create_api_documentation(target_name: str) -> str:
    match = find_java_type(target_name)
    if match is None:
        return f"I could not find a Java service matching {target_name}."

    source_path = match["path"]
    source = source_path.read_text(encoding="utf-8", errors="ignore")
    java_type = parse_java_type(source)
    methods = extract_public_methods(source)
    implementations = find_implementations(java_type["name"])

    GENERATED_DOCS_DIR.mkdir(parents=True, exist_ok=True)
    doc_path = GENERATED_DOCS_DIR / f"{java_type['name']}-api.md"
    doc_path.write_text(
        build_api_doc(source_path, java_type, methods, implementations),
        encoding="utf-8",
    )

    return (
        f"Created API documentation for {java_type['name']}.\n"
        f"File: {doc_path.relative_to(ROOT)}\n"
        f"Source: {source_path.relative_to(ROOT)}"
    )


def create_project_flow_documentation() -> str:
    GENERATED_DOCS_DIR.mkdir(parents=True, exist_ok=True)
    doc_path = GENERATED_DOCS_DIR / "Axelor-Lakehouse-Project-Flow.md"
    doc_path.write_text(build_project_flow_doc(), encoding="utf-8")
    return (
        "Created project flow documentation.\n"
        f"File: {doc_path.relative_to(ROOT)}"
    )


def build_project_flow_doc() -> str:
    return """# Axelor Lakehouse Project Flow

## Overview
This project is an Axelor lakehouse module where Java/Axelor handles the application flow, MinIO stores lakehouse files, Jenkins runs the processing pipeline, Python performs customer risk ML, and PostgreSQL stores published prediction data for dashboards.

## End-to-End Customer CSV Flow
1. User opens Axelor UI menu `Lake > Upload Customer Data`.
2. User uploads a customer CSV for dataset `customer_profile`.
3. `LakehouseController` receives the UI action.
4. `LakehouseServiceImpl` validates and uploads the raw CSV to MinIO.
5. Raw files are stored under MinIO bucket/path like `lake-raw/customer_profile/`.
6. Java triggers Jenkins job `open-suite-lake-pipeline` with `TABLE_NAME=customer_profile`.
7. Jenkins detects dataset type as `customer_profile`.
8. Jenkins runs dbt transformation for customer staging.
9. Jenkins runs `python-ml/customer_risk.py` for ML prediction.
10. Python writes `customer_predictions.parquet` to MinIO analytics storage.
11. Jenkins calls Axelor publish endpoint `/axelor-erp/ws/lake/customer-predictions/publish`.
12. Java reads the prediction parquet using PgDuckDB/DuckDB integration.
13. Java publishes rows into PostgreSQL table `lake_lake_customer_prediction`.
14. Axelor dashboards and AI Agent read customer prediction data from PostgreSQL.

## Component Responsibilities
- Axelor UI: Upload forms, dashboard views, AI Agent screen, menu navigation.
- Java controllers: Receive UI actions and publish callbacks.
- Java services: MinIO upload, Jenkins trigger, metadata handling, PostgreSQL publish step.
- MinIO: Stores raw CSV and generated parquet artifacts.
- Jenkins: Orchestrates dbt, ML prediction, and publish callback.
- dbt: Transforms raw customer data into staged lakehouse data.
- Python ML: Trains/runs customer churn risk prediction only.
- PostgreSQL: Stores published prediction rows used by dashboards and AI Agent.

## Important Files
- `modules/axelor-lake/src/main/java/com/axelor/lake/web/LakehouseController.java`
- `modules/axelor-lake/src/main/java/com/axelor/lake/service/LakehouseService.java`
- `modules/axelor-lake/src/main/java/com/axelor/lake/service/LakehouseServiceImpl.java`
- `modules/axelor-lake/src/main/java/com/axelor/lake/web/LakehousePublishEndpoint.java`
- `modules/axelor-lake/src/main/resources/views/LakehouseUpload.xml`
- `modules/axelor-lake/src/main/resources/views/LakeDashboard.xml`
- `Jenkinsfile`
- `python-ml/customer_risk.py`
- `ai-agent/app.py`

## Data Storage
- Raw CSV: MinIO `lake-raw`.
- Prediction parquet: MinIO analytics/artifact location.
- Published dashboard data: PostgreSQL `lake_lake_customer_prediction`.
- Lakehouse metadata/sync status: PostgreSQL `lake_lakehouse_table`.

## Dashboard Flow
1. Prediction pipeline finishes.
2. Publish endpoint loads prediction parquet into PostgreSQL.
3. Dashboard queries PostgreSQL prediction records.
4. Summary cards show active customers, high/medium/low risk counts, and churn percentages.
5. Prediction table shows customer-level prediction rows.

## AI Agent Flow
1. User asks a project question in Axelor AI Agent screen.
2. Axelor Java controller calls `ai-agent/ask.py`.
3. Python agent routes safe database prompts to PostgreSQL tools.
4. Project/code prompts use local project search tools.
5. Documentation prompts can write Markdown only under `ai-agent/generated-docs/`.
6. Unrelated questions are refused.

## Current Safe Agent Write Scope
The AI Agent can create generated Markdown documentation only in:

`ai-agent/generated-docs/`

It should not modify Java source, XML views, Jenkins files, credentials, or database data unless a separate permission is explicitly added.
"""


def find_java_type(target_name: str) -> dict[str, Path] | None:
    normalized_target = normalize_type_name(target_name)
    exact_matches: list[Path] = []
    suffix_matches: list[Path] = []
    contains_matches: list[Path] = []

    for path in iter_source_files(".java"):
        try:
            source = path.read_text(encoding="utf-8", errors="ignore")
        except OSError:
            continue

        java_type = parse_java_type(source)
        if not java_type["name"]:
            continue

        normalized_name = normalize_type_name(java_type["name"])
        if normalized_name == normalized_target:
            exact_matches.append(path)
        elif normalized_name.endswith(normalized_target):
            suffix_matches.append(path)
        elif normalized_target in normalized_name:
            contains_matches.append(path)

    candidates = exact_matches or suffix_matches or contains_matches
    if not candidates:
        return None

    candidates.sort(key=lambda path: (len(path.name), str(path)))
    return {"path": candidates[0]}


def iter_source_files(suffix: str):
    for directory, dirnames, filenames in os.walk(ROOT):
        dirnames[:] = [name for name in dirnames if name not in SKIP_DIRS]
        for filename in filenames:
            path = Path(directory) / filename
            if path.suffix.lower() == suffix:
                yield path


def parse_java_type(source: str) -> dict[str, str]:
    package_match = re.search(r"^\s*package\s+([\w.]+);", source, flags=re.MULTILINE)
    type_match = re.search(
        r"\b(public\s+)?(interface|class|enum)\s+([A-Za-z_][A-Za-z0-9_]*)",
        source,
    )
    return {
        "package": package_match.group(1) if package_match else "",
        "kind": type_match.group(2) if type_match else "type",
        "name": type_match.group(3) if type_match else "",
    }


def extract_public_methods(source: str) -> list[str]:
    methods: list[str] = []
    source_without_comments = remove_java_comments(source)
    java_type = parse_java_type(source_without_comments)
    is_interface = java_type["kind"] == "interface"
    lines = source_without_comments.splitlines()
    index = 0
    while index < len(lines):
        stripped = lines[index].strip()
        if is_method_signature_start(stripped, is_interface):
            signature_parts = [stripped]
            while not signature_parts[-1].endswith((";", "{")) and index + 1 < len(lines):
                index += 1
                signature_parts.append(lines[index].strip())
            signature = " ".join(signature_parts)
            signature = signature.rstrip("{").rstrip(";").strip()
            if "(" in signature and ")" in signature:
                methods.append(signature)
        index += 1
    return methods


def remove_java_comments(source: str) -> str:
    without_block_comments = re.sub(r"/\*.*?\*/", "", source, flags=re.DOTALL)
    return re.sub(r"//.*", "", without_block_comments)


def is_method_signature_start(line: str, is_interface: bool) -> bool:
    if not line or "(" not in line:
        return False
    if line.startswith(("public class", "public interface", "public enum")):
        return False
    if line.startswith(("if ", "for ", "while ", "switch ", "catch ")):
        return False
    if line.startswith("@"):
        return False
    if line.startswith("public "):
        return True
    return is_interface and not line.startswith(("import ", "package ")) and "=" not in line


def find_implementations(type_name: str) -> list[Path]:
    implementations: list[Path] = []
    if not type_name:
        return implementations

    pattern = re.compile(rf"\bimplements\s+{re.escape(type_name)}\b")
    for path in iter_source_files(".java"):
        try:
            source = path.read_text(encoding="utf-8", errors="ignore")
        except OSError:
            continue
        if pattern.search(source):
            implementations.append(path)
    implementations.sort(key=lambda path: str(path))
    return implementations


def build_api_doc(
    source_path: Path,
    java_type: dict[str, str],
    methods: list[str],
    implementations: list[Path],
) -> str:
    lines = [
        f"# {java_type['name']} API Documentation",
        "",
        "## Overview",
        f"- Type: `{java_type['kind']}`",
        f"- Package: `{java_type['package']}`",
        f"- Source: `{source_path.relative_to(ROOT)}`",
        "",
        "## Responsibilities",
        "- This documentation was generated from the Java source file.",
        "- Review business rules in the implementation before exposing these methods externally.",
        "",
        "## Public Methods",
    ]

    if methods:
        for method in methods:
            lines.append(f"- `{method}`")
    else:
        lines.append("- No public methods found.")

    lines.extend(["", "## Implementations"])
    if implementations:
        for implementation in implementations:
            lines.append(f"- `{implementation.relative_to(ROOT)}`")
    else:
        lines.append("- No implementation class found.")

    lines.extend(
        [
            "",
            "## Notes",
            "- Generated by the Axelor Lakehouse AI Agent.",
            "- Safe write scope: `ai-agent/generated-docs/` only.",
        ]
    )
    return "\n".join(lines) + "\n"


def normalize_type_name(value: str) -> str:
    return re.sub(r"[^a-z0-9]", "", value.lower())
