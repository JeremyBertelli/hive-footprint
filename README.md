# hive-footprint

## Overview

This project computes HDFS storage usage for Hive tables without querying the NameNode.

It uses:
- Hive Metastore (MySQL) to get table locations
- FSImage to get file metadata
- Python scripts to link and aggregate results

---

## Steps

### 1. Extract Hive tables

Run on cluster:

```bash
./list_tables.sh
```

This runs `list_tables.sql` on the Hive Metastore and produces:

```
entries/hive_storage_locations.tsv
```

---

### 2. Extract FSImage

On NameNode:

```bash
hdfs dfsadmin -fetchImage /tmp/fsimage
hdfs oiv -i /tmp/fsimage -o fsimage.csv -p Delimited
```

Move result to:

```
entries/fsimage.csv
```

---

### 3. Link Hive locations with FSImage

```bash
python3 link_partitions_fsimage.py
```

Output:

```
entries/hive_storage_locations_with_sizes.csv
```

Adds:
- file_count
- size_bytes
- size_human

---

### 4. Aggregate per table

```bash
python3 agg_tables.py
```

Output:

```
entries/hive_tables_aggregated.csv
```

Adds:
- total file count per table
- total size per table
- partition count

---

## Output files

### Detailed (per table / partition)

```
hive_storage_locations_with_sizes.csv
```

### Aggregated (per table)

```
hive_tables_aggregated.csv
```

---

## Notes

- FSImage is a snapshot (not real-time)
- Hive export and FSImage should be close in time
- No load is generated on the NameNode
