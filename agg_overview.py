#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
hive_migration_overview_single_file.py

Generate a single synthetic migration overview file from an aggregated Hive tables CSV.
"""

from __future__ import annotations

import csv
from collections import Counter, defaultdict
from pathlib import Path


# ---------------------------------------------------------------------
# Default paths
# ---------------------------------------------------------------------

INPUT_CSV = Path("/path/to/inwi_hive_tables_aggregated.csv")
OUTPUT_MD = INPUT_CSV.parent / "inwi_hive_migration_overview.md"


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------

def human_readable_bytes(num: int) -> str:
    value = float(num)
    for unit in ["B", "KB", "MB", "GB", "TB", "PB"]:
        if value < 1024:
            return f"{value:.2f}{unit}"
        value /= 1024
    return f"{value:.2f}EB"


def safe_int(value, default=0):
    try:
        if value is None or value == "":
            return default
        return int(value)
    except Exception:
        return default


def read_pipe_csv(path: Path) -> list[dict]:
    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f, delimiter="|")
        return list(reader)


def storage_profile(row: dict) -> str:
    table_type = (row.get("table_type") or "").strip()
    input_format = (row.get("input_format") or "").strip()
    output_format = (row.get("output_format") or "").strip()
    serde_lib = (row.get("serde_lib") or "").strip()

    if table_type == "VIRTUAL_VIEW":
        return "VIEW"

    joined = " ".join([input_format, output_format, serde_lib]).lower()

    if "kudu" in joined:
        return "KUDU"
    if "parquet" in joined:
        return "PARQUET"
    if "orc" in joined:
        return "ORC"
    if "avro" in joined:
        return "AVRO"
    if "text" in joined or "lazy" in joined:
        return "TEXT"
    if "sequencefile" in joined:
        return "SEQUENCEFILE"
    if "rcfile" in joined:
        return "RCFILE"

    if input_format or output_format or serde_lib:
        return "OTHER"
    return "UNKNOWN"


def markdown_table(headers: list[str], rows: list[list]) -> str:
    out = []
    out.append("| " + " | ".join(headers) + " |")
    out.append("| " + " | ".join(["---"] * len(headers)) + " |")
    for row in rows:
        out.append("| " + " | ".join(str(x) for x in row) + " |")
    return "\n".join(out)


def top_n(items, key_func, n=20, reverse=True):
    return sorted(items, key=key_func, reverse=reverse)[:n]


# ---------------------------------------------------------------------
# Core
# ---------------------------------------------------------------------

def build_overview(rows: list[dict]) -> str:
    total_objects = len(rows)

    total_size_bytes = 0
    total_file_count = 0
    total_partition_count = 0

    table_type_counter = Counter()
    storage_counter = Counter()

    internal_tables = 0
    external_tables = 0
    virtual_views = 0
    physical_tables = 0

    empty_or_zero_tables = []
    small_files_candidates = []

    by_database = defaultdict(lambda: {
        "objects": 0,
        "physical_tables": 0,
        "views": 0,
        "managed_tables": 0,
        "external_tables": 0,
        "partitioned_tables": 0,
        "size_bytes": 0,
        "file_count": 0,
        "partition_count": 0,
    })

    by_storage = defaultdict(lambda: {
        "objects": 0,
        "physical_tables": 0,
        "views": 0,
        "size_bytes": 0,
        "file_count": 0,
        "partition_count": 0,
    })

    enriched_rows = []

    for row in rows:
        database = (row.get("database") or "").strip()
        table = (row.get("table") or "").strip()
        table_type = (row.get("table_type") or "").strip()

        partition_count = safe_int(row.get("partition_count"))
        file_count = safe_int(row.get("file_count"))
        size_bytes = safe_int(row.get("size_bytes"))

        sp = storage_profile(row)
        avg_file_size = size_bytes / file_count if file_count > 0 else 0

        total_size_bytes += size_bytes
        total_file_count += file_count
        total_partition_count += partition_count

        table_type_counter[table_type or "UNKNOWN"] += 1
        storage_counter[sp] += 1

        db = by_database[database]
        db["objects"] += 1
        db["size_bytes"] += size_bytes
        db["file_count"] += file_count
        db["partition_count"] += partition_count

        st = by_storage[sp]
        st["objects"] += 1
        st["size_bytes"] += size_bytes
        st["file_count"] += file_count
        st["partition_count"] += partition_count

        if table_type == "VIRTUAL_VIEW":
            virtual_views += 1
            db["views"] += 1
            st["views"] += 1
        else:
            physical_tables += 1
            db["physical_tables"] += 1
            st["physical_tables"] += 1

            if table_type == "MANAGED_TABLE":
                internal_tables += 1
                db["managed_tables"] += 1
            elif table_type == "EXTERNAL_TABLE":
                external_tables += 1
                db["external_tables"] += 1

            if partition_count > 0:
                db["partitioned_tables"] += 1

            if size_bytes == 0 or file_count == 0:
                empty_or_zero_tables.append(
                    (database, table, table_type, sp, partition_count, file_count, size_bytes)
                )

            if file_count > 0 and size_bytes > 0 and avg_file_size < 16 * 1024 * 1024:
                small_files_candidates.append(
                    (database, table, table_type, sp, partition_count, file_count, size_bytes, avg_file_size)
                )

        enriched_rows.append({
            "database": database,
            "table": table,
            "table_type": table_type,
            "storage_profile": sp,
            "partition_count": partition_count,
            "file_count": file_count,
            "size_bytes": size_bytes,
            "avg_file_size": avg_file_size,
        })

    database_count = len(by_database)
    avg_files_per_table = total_file_count / physical_tables if physical_tables else 0
    avg_size_per_table = total_size_bytes / physical_tables if physical_tables else 0
    avg_partitions_per_table = total_partition_count / physical_tables if physical_tables else 0

    partitioned_tables_count = sum(
        1 for r in enriched_rows
        if r["table_type"] != "VIRTUAL_VIEW" and r["partition_count"] > 0
    )

    kudu_tables = sum(
        1 for r in enriched_rows
        if r["table_type"] != "VIRTUAL_VIEW" and r["storage_profile"] == "KUDU"
    )

    parquet_tables = sum(
        1 for r in enriched_rows
        if r["table_type"] != "VIRTUAL_VIEW" and r["storage_profile"] == "PARQUET"
    )

    text_tables = sum(
        1 for r in enriched_rows
        if r["table_type"] != "VIRTUAL_VIEW" and r["storage_profile"] == "TEXT"
    )

    orc_tables = sum(
        1 for r in enriched_rows
        if r["table_type"] != "VIRTUAL_VIEW" and r["storage_profile"] == "ORC"
    )

    avro_tables = sum(
        1 for r in enriched_rows
        if r["table_type"] != "VIRTUAL_VIEW" and r["storage_profile"] == "AVRO"
    )

    large_tables_over_1tb = sum(
        1 for r in enriched_rows
        if r["table_type"] != "VIRTUAL_VIEW" and r["size_bytes"] >= 1024 ** 4
    )

    highly_partitioned_tables = sum(
        1 for r in enriched_rows
        if r["table_type"] != "VIRTUAL_VIEW" and r["partition_count"] >= 1000
    )

    high_file_count_tables = sum(
        1 for r in enriched_rows
        if r["table_type"] != "VIRTUAL_VIEW" and r["file_count"] >= 10000
    )

    global_metrics_rows = [
        ["Databases", database_count],
        ["Objects (all)", total_objects],
        ["Physical tables", physical_tables],
        ["Virtual views", virtual_views],
        ["Managed tables", internal_tables],
        ["External tables", external_tables],
        ["Total size", human_readable_bytes(total_size_bytes)],
        ["Total files", total_file_count],
        ["Total partitions", total_partition_count],
        ["Partitioned physical tables", partitioned_tables_count],
        ["Average size per physical table", human_readable_bytes(int(avg_size_per_table))],
        ["Average files per physical table", f"{avg_files_per_table:.2f}"],
        ["Average partitions per physical table", f"{avg_partitions_per_table:.2f}"],
        ["Zero size / zero file physical tables", len(empty_or_zero_tables)],
        ["Small files candidates (<16MB avg file)", len(small_files_candidates)],
        ["Kudu tables", kudu_tables],
        ["Parquet tables", parquet_tables],
        ["Text tables", text_tables],
        ["ORC tables", orc_tables],
        ["Avro tables", avro_tables],
        ["Large tables >= 1TB", large_tables_over_1tb],
        ["Highly partitioned tables >= 1000 partitions", highly_partitioned_tables],
        ["High file count tables >= 10000 files", high_file_count_tables],
    ]

    table_type_rows = []
    for k, v in sorted(table_type_counter.items(), key=lambda x: x[1], reverse=True):
        pct = (v / total_objects * 100) if total_objects else 0
        table_type_rows.append([k, v, f"{pct:.2f}%"])

    storage_rows = []
    for sp, vals in sorted(by_storage.items(), key=lambda x: x[1]["size_bytes"], reverse=True):
        avg_file_size = vals["size_bytes"] / vals["file_count"] if vals["file_count"] else 0
        storage_rows.append([
            sp,
            vals["objects"],
            vals["physical_tables"],
            vals["views"],
            human_readable_bytes(vals["size_bytes"]),
            vals["file_count"],
            vals["partition_count"],
            human_readable_bytes(int(avg_file_size)),
        ])

    db_rows = []
    for db_name, vals in sorted(by_database.items(), key=lambda x: x[1]["size_bytes"], reverse=True):
        avg_file_size = vals["size_bytes"] / vals["file_count"] if vals["file_count"] else 0
        db_rows.append([
            db_name,
            vals["objects"],
            vals["physical_tables"],
            vals["views"],
            vals["managed_tables"],
            vals["external_tables"],
            vals["partitioned_tables"],
            human_readable_bytes(vals["size_bytes"]),
            vals["file_count"],
            vals["partition_count"],
            human_readable_bytes(int(avg_file_size)),
        ])

    top_db_by_size_rows = [
        [db_name, human_readable_bytes(vals["size_bytes"]), vals["physical_tables"], vals["views"], vals["file_count"], vals["partition_count"]]
        for db_name, vals in top_n(list(by_database.items()), key_func=lambda x: x[1]["size_bytes"], n=20)
    ]

    top_db_by_tables_rows = [
        [db_name, vals["physical_tables"], vals["views"], human_readable_bytes(vals["size_bytes"]), vals["file_count"], vals["partition_count"]]
        for db_name, vals in top_n(list(by_database.items()), key_func=lambda x: x[1]["physical_tables"], n=20)
    ]

    top_tables_by_size_rows = [
        [r["database"], r["table"], r["table_type"], r["storage_profile"], human_readable_bytes(r["size_bytes"]), r["file_count"], r["partition_count"]]
        for r in top_n(
            [r for r in enriched_rows if r["table_type"] != "VIRTUAL_VIEW"],
            key_func=lambda x: x["size_bytes"],
            n=30
        )
    ]

    top_tables_by_files_rows = [
        [r["database"], r["table"], r["table_type"], r["storage_profile"], r["file_count"], human_readable_bytes(r["size_bytes"]), human_readable_bytes(int(r["avg_file_size"]))]
        for r in top_n(
            [r for r in enriched_rows if r["table_type"] != "VIRTUAL_VIEW"],
            key_func=lambda x: x["file_count"],
            n=30
        )
    ]

    top_tables_by_partitions_rows = [
        [r["database"], r["table"], r["table_type"], r["storage_profile"], r["partition_count"], human_readable_bytes(r["size_bytes"]), r["file_count"]]
        for r in top_n(
            [r for r in enriched_rows if r["table_type"] != "VIRTUAL_VIEW"],
            key_func=lambda x: x["partition_count"],
            n=30
        )
    ]

    empty_rows = [
        [db, table, table_type, sp, partition_count, file_count, human_readable_bytes(size_bytes)]
        for db, table, table_type, sp, partition_count, file_count, size_bytes in top_n(
            empty_or_zero_tables,
            key_func=lambda x: (x[6], x[5], x[4]),
            n=50
        )
    ]

    small_files_rows = [
        [db, table, table_type, sp, partition_count, file_count, human_readable_bytes(size_bytes), human_readable_bytes(int(avg_file_size))]
        for db, table, table_type, sp, partition_count, file_count, size_bytes, avg_file_size in top_n(
            small_files_candidates,
            key_func=lambda x: (x[5], x[6]),
            n=50
        )
    ]

    observations = []
    observations.append(f"- Inventory contains **{database_count} databases**, **{physical_tables} physical tables** and **{virtual_views} views**.")
    observations.append(f"- Total physical data volume is **{human_readable_bytes(total_size_bytes)}** across **{total_file_count:,} files** and **{total_partition_count:,} partitions**.")
    observations.append(f"- Managed tables: **{internal_tables}**. External tables: **{external_tables}**. Views should be handled separately from physical data migration.")

    if parquet_tables > 0:
        observations.append(f"- Parquet is a major format with **{parquet_tables} tables**, which is favorable for migration and modern analytics engines.")
    if text_tables > 0:
        observations.append(f"- Text-based storage remains significant with **{text_tables} tables**. These may require extra attention for schema enforcement, SerDe behavior, and performance.")
    if kudu_tables > 0:
        observations.append(f"- **{kudu_tables} Kudu tables** detected. Kudu must be handled as a dedicated migration stream.")
    if len(small_files_candidates) > 0:
        observations.append(f"- **{len(small_files_candidates)} tables** are candidates for a small-files issue (average file size < 16 MB).")
    if highly_partitioned_tables > 0:
        observations.append(f"- **{highly_partitioned_tables} tables** have at least 1000 partitions.")
    if large_tables_over_1tb > 0:
        observations.append(f"- **{large_tables_over_1tb} tables** are larger than 1 TB and should be prioritized.")
    if len(empty_or_zero_tables) > 0:
        observations.append(f"- **{len(empty_or_zero_tables)} physical tables** have zero size or zero files and should be reviewed before migration.")

    md = []
    md.append("# Hive Migration Overview")
    md.append("")
    md.append(f"Input file: `{INPUT_CSV}`")
    md.append("")
    md.append("Synthetic overview generated from aggregated Hive inventory.")
    md.append("")

    md.append("## 1. Global Metrics")
    md.append("")
    md.append(markdown_table(["Metric", "Value"], global_metrics_rows))
    md.append("")

    md.append("## 2. Object Breakdown by Table Type")
    md.append("")
    md.append(markdown_table(["Table Type", "Count", "Percent"], table_type_rows))
    md.append("")

    md.append("## 3. Breakdown by Storage Profile")
    md.append("")
    md.append(markdown_table([
        "Storage Profile", "Objects", "Physical Tables", "Views",
        "Total Size", "Files", "Partitions", "Avg File Size"
    ], storage_rows))
    md.append("")

    md.append("## 4. Database Overview")
    md.append("")
    md.append(markdown_table([
        "Database", "Objects", "Physical Tables", "Views", "Managed",
        "External", "Partitioned Tables", "Total Size", "Files",
        "Partitions", "Avg File Size"
    ], db_rows))
    md.append("")

    md.append("## 5. Top Databases by Size")
    md.append("")
    md.append(markdown_table([
        "Database", "Total Size", "Physical Tables", "Views", "Files", "Partitions"
    ], top_db_by_size_rows))
    md.append("")

    md.append("## 6. Top Databases by Number of Physical Tables")
    md.append("")
    md.append(markdown_table([
        "Database", "Physical Tables", "Views", "Total Size", "Files", "Partitions"
    ], top_db_by_tables_rows))
    md.append("")

    md.append("## 7. Top Tables by Size")
    md.append("")
    md.append(markdown_table([
        "Database", "Table", "Table Type", "Storage", "Size", "Files", "Partitions"
    ], top_tables_by_size_rows))
    md.append("")

    md.append("## 8. Top Tables by File Count")
    md.append("")
    md.append(markdown_table([
        "Database", "Table", "Table Type", "Storage", "Files", "Size", "Avg File Size"
    ], top_tables_by_files_rows))
    md.append("")

    md.append("## 9. Top Tables by Partition Count")
    md.append("")
    md.append(markdown_table([
        "Database", "Table", "Table Type", "Storage", "Partitions", "Size", "Files"
    ], top_tables_by_partitions_rows))
    md.append("")

    md.append("## 10. Empty or Zero-Size Physical Tables")
    md.append("")
    if empty_rows:
        md.append(markdown_table([
            "Database", "Table", "Table Type", "Storage", "Partitions", "Files", "Size"
        ], empty_rows))
    else:
        md.append("No empty or zero-size physical tables detected.")
    md.append("")

    md.append("## 11. Small Files Candidates")
    md.append("")
    md.append("Criteria: physical tables with data and average file size < 16 MB.")
    md.append("")
    if small_files_rows:
        md.append(markdown_table([
            "Database", "Table", "Table Type", "Storage", "Partitions", "Files", "Size", "Avg File Size"
        ], small_files_rows))
    else:
        md.append("No small-files candidates detected.")
    md.append("")

    md.append("## 12. Migration Observations")
    md.append("")
    md.extend(observations)
    md.append("")

    return "\n".join(md)


# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------

def main():
    if not INPUT_CSV.exists():
        raise FileNotFoundError(f"Input file not found: {INPUT_CSV}")

    rows = read_pipe_csv(INPUT_CSV)
    report = build_overview(rows)

    with open(OUTPUT_MD, "w", encoding="utf-8") as f:
        f.write(report)

    print(f"Overview written to: {OUTPUT_MD}")


if __name__ == "__main__":
    main()