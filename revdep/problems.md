# arkdb (0.0.18)

* GitHub: <https://github.com/ropensci/arkdb>
* Email: <mailto:cboettig@gmail.com>
* GitHub mirror: <https://github.com/cran/arkdb>

Run `revdepcheck::cloud_details(, "arkdb")` for more info

## Newly broken

*   checking tests ... ERROR
     ```
     ...
       > 
       > test_check("arkdb")
       [1] "Testing using backend duckdb_connection"
       Saving _problems/test-streamable-44.R
       [ FAIL 1 | WARN 6 | SKIP 8 | PASS 53 ]
       
       ══ Skipped tests (8) ═══════════════════════════════════════════════════════════
       • On CRAN (6): 'test-arkdb.R:287:3', 'test-bulk-import.R:6:3',
         'test-bulk-import.R:72:3', 'test-errors.R:12:3', 'test-errors.R:34:3',
         'test-errors.R:53:3'
       • {MonetDBLite} is not installed (2): 'test-arkdb.R:132:3',
         'test-local_db.R:24:3'
       
       ══ Failed tests ════════════════════════════════════════════════════════════════
       ── Error ('test-streamable.R:44:3'): streamable_vroom ──────────────────────────
       Error in `write_tsv(x = x, path = path, append = omit_header)`: unused argument (path = path)
       Backtrace:
           ▆
        1. └─arkdb (local) test_stream(streamable_vroom()) at test-streamable.R:44:3
        2.   └─stream$write(data, con, omit_header = FALSE) at test-streamable.R:12:3
       
       [ FAIL 1 | WARN 6 | SKIP 8 | PASS 53 ]
       Error:
       ! Test failures.
       Execution halted
     ```

