import csv
import json
import re
import shutil
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path


# Project paths work on Windows and inside Docker
DAGS_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = DAGS_DIR / "data"
INPUT_DIR = DATA_DIR / "input"
PROCESSED_DIR = DATA_DIR / "processed"
REJECTED_DIR = DATA_DIR / "rejected"
ARCHIVE_DIR = DATA_DIR / "archive"
CONFIG_FILE = DAGS_DIR / "config" / "cleaning_rules.json"
REPORTS_DIR = DAGS_DIR / "reports"


def normalize_name(name):
    """Convert names like 'Order ID' to 'order_id'."""
    name = re.sub(r"[^a-zA-Z0-9]+", "_", str(name).strip())
    return name.strip("_").lower() or "unnamed_column"


def read_csv_file(file_path):
    """Read a CSV and normalize its column names."""
    with open(file_path, "r", encoding="utf-8-sig", newline="") as file:
        reader = csv.DictReader(file)
        original_headers = reader.fieldnames

        if not original_headers:
            raise ValueError("CSV file is empty or has no header row.")

        headers = [normalize_name(h or "") for h in original_headers]

        if len(headers) != len(set(headers)):
            raise ValueError(
                "Two or more columns have the same name after normalization."
            )

        rows = []

        for raw_row in reader:
            row = {}
            for original, normalized in zip(original_headers, headers):
                value = raw_row.get(original)
                row[normalized] = "" if value is None else str(value).strip()
            rows.append(row)

    return headers, rows


def write_csv(file_path, headers, rows):
    """Write rows to a CSV file, including a header for empty outputs."""
    with open(file_path, "w", encoding="utf-8-sig", newline="") as file:
        writer = csv.DictWriter(
            file, fieldnames=headers, extrasaction="ignore"
        )
        writer.writeheader()
        writer.writerows(rows)


def normalize_department(row):
    """Example custom cleaner for employee department names."""
    department = row.get("department", "").strip()

    mappings = {
        "information technology": "IT",
        "human resources": "HR",
        "human resource": "HR",
        "finance": "Finance",
    }

    if department:
        row["department"] = mappings.get(
            department.lower(), department.title()
        )

    return row


# Add more custom cleaners here when needed.
CUSTOM_CLEANERS = {
    "normalize_department": normalize_department,
}


def find_dataset_config(file_path, configs):
    """
    Find rules by filename.
    sales.csv and sales_2026.csv can both use the 'sales' configuration.
    """
    stem = normalize_name(file_path.stem)

    if stem in configs:
        return stem

    matches = [
        name for name in configs
        if stem.startswith(name + "_")
    ]

    return max(matches, key=len) if matches else None


def archive_input_file(file_path):
    """Move a successfully processed input file to the archive."""
    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)

    destination = ARCHIVE_DIR / file_path.name

    if destination.exists():
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        destination = ARCHIVE_DIR / (
            f"{file_path.stem}_{timestamp}{file_path.suffix}"
        )

    shutil.move(str(file_path), str(destination))
    return destination.name


def process_file(file_path, dataset_name, rules):
    headers, rows = read_csv_file(file_path)

    # Check required columns.
    required_columns = [
        normalize_name(c) for c in rules.get("required_columns", [])
    ]
    missing_columns = [c for c in required_columns if c not in headers]

    if missing_columns:
        raise ValueError(
            f"Missing required columns: {missing_columns}"
        )

    # Normalize column names in the configuration too.
    required_values = [
        normalize_name(c) for c in rules.get("required_values", [])
    ]
    numeric_columns = [
        normalize_name(c) for c in rules.get("numeric_columns", [])
    ]
    positive_columns = [
        normalize_name(c) for c in rules.get("positive_columns", [])
    ]
    duplicate_columns = [
        normalize_name(c) for c in rules.get("drop_duplicates_on", [])
    ]
    fill_missing = {
        normalize_name(k): v
        for k, v in rules.get("fill_missing", {}).items()
    }

    # Validate that configured rules refer to existing columns.
    configured_columns = (
        required_values
        + numeric_columns
        + positive_columns
        + duplicate_columns
    )

    for column in configured_columns:
        if column not in headers:
            raise ValueError(
                f"Cleaning rules refer to missing column: '{column}'"
            )

    # Apply missing-value replacements, then optional custom cleaning.
    custom_name = rules.get("custom_cleaner")
    custom_function = None

    if custom_name:
        custom_function = CUSTOM_CLEANERS.get(custom_name)
        if custom_function is None:
            raise ValueError(
                f"Unknown custom cleaner: '{custom_name}'"
            )

    for row in rows:
        for column, replacement in fill_missing.items():
            if column in row and row[column] == "":
                row[column] = str(replacement)

        if custom_function:
            row = custom_function(row)

    # Validate required values and numeric fields.
    reasons = [[] for _ in rows]

    for index, row in enumerate(rows):
        for column in required_values:
            if row.get(column, "") == "":
                reasons[index].append(f"missing_required_value:{column}")

        for column in numeric_columns:
            value = row.get(column, "")

            # Empty values are handled by required_values or positive_columns.
            if value == "":
                continue

            try:
                number = Decimal(value)
                if not number.is_finite():
                    raise InvalidOperation
            except (InvalidOperation, ValueError):
                reasons[index].append(f"not_numeric:{column}")

        for column in positive_columns:
            value = row.get(column, "")

            if value == "":
                reasons[index].append(f"missing_positive_value:{column}")
                continue

            try:
                number = Decimal(value)
                if not number.is_finite():
                    raise InvalidOperation
                if number <= 0:
                    reasons[index].append(f"not_positive:{column}")
            except (InvalidOperation, ValueError):
                if column not in numeric_columns:
                    reasons[index].append(f"not_numeric:{column}")

    # Detect duplicates among rows that passed the other validations.
    seen = set()

    if duplicate_columns:
        for index, row in enumerate(rows):
            if reasons[index]:
                continue

            key = tuple(row.get(c, "") for c in duplicate_columns)

            # Don't treat records with incomplete duplicate keys as duplicates.
            if any(value == "" for value in key):
                continue

            if key in seen:
                reasons[index].append(
                    "duplicate_on:" + ",".join(duplicate_columns)
                )
            else:
                seen.add(key)

    accepted_rows = []
    rejected_rows = []

    for row, row_reasons in zip(rows, reasons):
        if row_reasons:
            rejected_row = row.copy()
            rejected_row["rejection_reason"] = "; ".join(row_reasons)
            rejected_rows.append(rejected_row)
        else:
            accepted_rows.append(row)

    # Save accepted and rejected records separately.
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    REJECTED_DIR.mkdir(parents=True, exist_ok=True)

    cleaned_file = PROCESSED_DIR / f"{file_path.stem}_cleaned.csv"
    rejected_file = REJECTED_DIR / f"{file_path.stem}_rejected.csv"

    write_csv(cleaned_file, headers, accepted_rows)
    write_csv(
        rejected_file,
        headers + ["rejection_reason"],
        rejected_rows,
    )

    archived_name = archive_input_file(file_path)

    return {
        "input_file": file_path.name,
        "dataset": dataset_name,
        "status": (
            "processed_with_rejections"
            if rejected_rows else "processed"
        ),
        "input_rows": len(rows),
        "accepted_rows": len(accepted_rows),
        "rejected_rows": len(rejected_rows),
        "duplicate_rows": sum(
            any(reason.startswith("duplicate_on:") for reason in row_reasons)
            for row_reasons in reasons
        ),
        "cleaned_file": str(cleaned_file),
        "rejected_file": str(rejected_file),
        "archived_file": archived_name,
    }


def process_all_files():
    for directory in (
        INPUT_DIR, PROCESSED_DIR, REJECTED_DIR,
        ARCHIVE_DIR, REPORTS_DIR
    ):
        directory.mkdir(parents=True, exist_ok=True)

    with open(CONFIG_FILE, "r", encoding="utf-8-sig") as file:
        raw_configs = json.load(file)

    configs = {
        normalize_name(name): rules
        for name, rules in raw_configs.items()
    }

    run_time = datetime.now().astimezone().isoformat()
    results = []

    input_files = sorted(INPUT_DIR.glob("*.csv"))

    if not input_files:
        print("No CSV files found in the input folder.")

    for file_path in input_files:
        dataset_name = find_dataset_config(file_path, configs)

        if dataset_name is None:
            results.append({
                "input_file": file_path.name,
                "status": "skipped_unknown_dataset",
                "error": "No matching entry found in cleaning_rules.json",
            })
            print(f"SKIPPED: {file_path.name} — no matching rules.")
            continue

        try:
            result = process_file(
                file_path,
                dataset_name,
                configs[dataset_name],
            )
            results.append(result)

            print(
                f"{result['status'].upper()}: {file_path.name} | "
                f"accepted={result['accepted_rows']} | "
                f"rejected={result['rejected_rows']}"
            )

        except Exception as error:
            results.append({
                "input_file": file_path.name,
                "dataset": dataset_name,
                "status": "failed",
                "error": str(error),
            })
            print(f"FAILED: {file_path.name} — {error}")

    report = {
        "run_time": run_time,
        "files_found": len(input_files),
        "files_processed": sum(
            r["status"] in ("processed", "processed_with_rejections")
            for r in results
        ),
        "files_failed_or_skipped": sum(
            r["status"] in ("failed", "skipped_unknown_dataset")
            for r in results
        ),
        "files": results,
    }

    # Keep the latest report and a timestamped copy.
    latest_report = REPORTS_DIR / "data_quality_report.json"
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    dated_report = REPORTS_DIR / f"data_quality_{timestamp}.json"

    for report_path in (latest_report, dated_report):
        with open(report_path, "w", encoding="utf-8") as file:
            json.dump(report, file, indent=2, ensure_ascii=False)

    print(f"\nReport saved to: {latest_report}")


if __name__ == "__main__":
    process_all_files()