# Parquet-tool
Console tool for viewing content of parquet files

Usage: ./pq -q "select * where key >= 4.0 and time <=30 " test.pq
Output:
┌──────────────────────────┬─────────────────────────┬─────────────────────────┐
│key                       │time                     │value                    │
├──────────────────────────┼─────────────────────────┼─────────────────────────┤
│5                         │30                       │5555                     │
└──────────────────────────┴─────────────────────────┴─────────────────────────┘

Work in progress..
