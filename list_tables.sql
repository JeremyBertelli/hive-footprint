SELECT
  CONCAT(
    d.NAME, '|',
    t.TBL_NAME, '|',
    t.TBL_TYPE, '|',
    IFNULL(s.INPUT_FORMAT, ''), '|',
    IFNULL(s.OUTPUT_FORMAT, ''), '|',
    IFNULL(sd.SLIB, ''), '|',
    'TABLE', '|',
    '', '|',
    IFNULL(s.LOCATION, '')
  )
FROM DBS d
JOIN TBLS t
  ON t.DB_ID = d.DB_ID
LEFT JOIN SDS s
  ON t.SD_ID = s.SD_ID
LEFT JOIN SERDES sd
  ON s.SERDE_ID = sd.SERDE_ID
LEFT JOIN PARTITIONS p
  ON p.TBL_ID = t.TBL_ID
WHERE p.TBL_ID IS NULL

UNION ALL

SELECT
  CONCAT(
    d.NAME, '|',
    t.TBL_NAME, '|',
    t.TBL_TYPE, '|',
    IFNULL(s.INPUT_FORMAT, ''), '|',
    IFNULL(s.OUTPUT_FORMAT, ''), '|',
    IFNULL(sd.SLIB, ''), '|',
    'PARTITION', '|',
    p.PART_NAME, '|',
    IFNULL(s.LOCATION, '')
  )
FROM DBS d
JOIN TBLS t
  ON t.DB_ID = d.DB_ID
JOIN PARTITIONS p
  ON p.TBL_ID = t.TBL_ID
JOIN SDS s
  ON p.SD_ID = s.SD_ID
LEFT JOIN SERDES sd
  ON s.SERDE_ID = sd.SERDE_ID

ORDER BY 1;