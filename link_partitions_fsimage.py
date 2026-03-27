#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
from collections import defaultdict
from urllib.parse import urlparse
from tqdm import tqdm

FSIMAGE_FILE = "/path/to/entries/fsimage.csv"
HIVE_FILE = "/path/to/entries/hive_storage_locations.csv"
OUTPUT_FILE = "/path/to/outputs/hive_storage_locations_with_sizes.csv"


def normalize_hdfs_location(location: str) -> str:
    location = location.strip()
    if location.startswith("hdfs://"):
        return urlparse(location).path.rstrip("/") or "/"
    return location.rstrip("/") or "/"


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
    if "\t" in first_line:
        return "\t"
    if "|" in first_line:
        return "|"
    return ","


def count_data_lines(path: str) -> int:
    with open(path, "r", encoding="utf-8", newline="") as f:
        return max(sum(1 for _ in f) - 1, 0)


def parent_directories(file_path: str):
    """
    For /a/b/c/file.parquet returns:
    /a/b/c
    /a/b
    /a
    /
    """
    parts = [p for p in file_path.strip("/").split("/") if p]
    if not parts:
        return ["/"]

    dirs = []
    if len(parts) == 1:
        dirs.append("/")
        return dirs

    for i in range(len(parts) - 1, 0, -1):
        dirs.append("/" + "/".join(parts[:i]))
    dirs.append("/")
    return dirs


def main():
    fs_delim = detect_delimiter(FSIMAGE_FILE)
    hive_delim = detect_delimiter(HIVE_FILE)

    fs_total = count_data_lines(FSIMAGE_FILE)

    dir_file_count = defaultdict(int)
    dir_size_bytes = defaultdict(int)

    with open(FSIMAGE_FILE, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f, delimiter=fs_delim)

        path_col = None
        for candidate in ["Path", "path", "ath"]:
            if candidate in reader.fieldnames:
                path_col = candidate
                break

        if path_col is None:
            raise ValueError(
                f"Impossible de trouver la colonne Path dans {FSIMAGE_FILE}. Colonnes: {reader.fieldnames}"
            )

        for row in tqdm(reader, total=fs_total, desc="Reading FSImage", unit="rows"):
            perm = row.get("Permission", "")
            if not perm.startswith("-"):
                continue

            fs_path = (row.get(path_col) or "").rstrip("/")
            if not fs_path:
                continue

            size = int(row.get("FileSize", "0") or 0)

            for directory in parent_directories(fs_path):
                dir_file_count[directory] += 1
                dir_size_bytes[directory] += size

    with open(HIVE_FILE, "r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f, delimiter=hive_delim)
        hive_rows = [row for row in reader if len(row) >= 9]

    results = []
    physical_table_types = {"MANAGED_TABLE", "EXTERNAL_TABLE"}

    for row in tqdm(hive_rows, desc="Processing Hive locations", unit="rows"):
        database = row[0]
        table = row[1]
        table_type = row[2]
        input_format = row[3]
        output_format = row[4]
        serde_lib = row[5]
        location_level = row[6]
        partition_name = row[7]
        location = row[8]

        if table_type not in physical_table_types:
            file_count = 0
            size_bytes = 0
        else:
            base_path = normalize_hdfs_location(location) if location else ""
            file_count = dir_file_count.get(base_path, 0) if base_path else 0
            size_bytes = dir_size_bytes.get(base_path, 0) if base_path else 0

        results.append({
            "database": database,
            "table": table,
            "table_type": table_type,
            "input_format": input_format,
            "output_format": output_format,
            "serde_lib": serde_lib,
            "location_level": location_level,
            "partition_name": partition_name,
            "location": location,
            "file_count": file_count,
            "size_bytes": size_bytes,
            "size_human": human_readable(size_bytes),
        })

    with open(OUTPUT_FILE, "w", encoding="utf-8", newline="") as f:
        fieldnames = [
            "database",
            "table",
            "table_type",
            "input_format",
            "output_format",
            "serde_lib",
            "location_level",
            "partition_name",
            "location",
            "file_count",
            "size_bytes",
            "size_human",
        ]
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter="|")
        writer.writeheader()
        writer.writerows(results)

    print(f"Written: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()