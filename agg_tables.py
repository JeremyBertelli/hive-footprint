#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
from collections import defaultdict

INPUT_FILE = "/path/to/outputs/hive_storage_locations_with_sizes.csv"
OUTPUT_FILE = "/path/tp/outputs/hive_tables_aggregated.csv"


def human_readable(num: int) -> str:
    value = float(num)
    for unit in ["B", "KB", "MB", "GB", "TB", "PB"]:
        if value < 1024:
            return f"{value:.2f}{unit}"
        value /= 1024
    return f"{value:.2f}EB"


def detect_delimiter(path: str) -> str:
    with open(path, "r", encoding="utf-8", newline="") as f:
        first_line = f.readline()
    if "|" in first_line:
        return "|"
    if "\t" in first_line:
        return "\t"
    return ","


def main():
    delim = detect_delimiter(INPUT_FILE)

    agg = defaultdict(lambda: {
        "database": "",
        "table": "",
        "table_type": "",
        "input_format": "",
        "output_format": "",
        "serde_lib": "",
        "partition_count": 0,
        "file_count": 0,
        "size_bytes": 0,
    })

    with open(INPUT_FILE, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f, delimiter=delim)

        for row in reader:
            key = (
                row["database"],
                row["table"],
                row["table_type"],
                row["input_format"],
                row["output_format"],
                row["serde_lib"],
            )

            entry = agg[key]
            entry["database"] = row["database"]
            entry["table"] = row["table"]
            entry["table_type"] = row["table_type"]
            entry["input_format"] = row["input_format"]
            entry["output_format"] = row["output_format"]
            entry["serde_lib"] = row["serde_lib"]

            if row["location_level"] == "PARTITION":
                entry["partition_count"] += 1

            entry["file_count"] += int(row["file_count"] or 0)
            entry["size_bytes"] += int(row["size_bytes"] or 0)

    with open(OUTPUT_FILE, "w", encoding="utf-8", newline="") as f:
        fieldnames = [
            "database",
            "table",
            "table_type",
            "input_format",
            "output_format",
            "serde_lib",
            "partition_count",
            "file_count",
            "size_bytes",
            "size_human",
        ]
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter="|")
        writer.writeheader()

        for _, row in sorted(agg.items(), key=lambda x: (x[1]["database"], x[1]["table"])):
            row["size_human"] = human_readable(row["size_bytes"])
            writer.writerow(row)

    print(f"Written: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()