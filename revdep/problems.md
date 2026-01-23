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

# FORTLS (1.6.2)

* GitHub: <https://github.com/Molina-Valero/FORTLS>
* Email: <mailto:jamolinavalero@gmail.com>
* GitHub mirror: <https://github.com/cran/FORTLS>

Run `revdepcheck::cloud_details(, "FORTLS")` for more info

## Newly broken

*   checking re-building of vignette outputs ... ERROR
     ```
     ...
     Content type 'application/binary' length 12898220 bytes (12.3 MB)
     ==================================================
     downloaded 12.3 MB
     
     
     Quitting from tree_level.Rmd:54-61 [unnamed-chunk-3]
     ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
     <error/rlang_error>
     Error in `vroom::vroom_write()`:
     ! unused argument (path = file.path(dir.result, .data.red$file[1]))
     ---
     Backtrace:
         ▆
      1. └─FORTLS::normalize(...)
     ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
     
     Error: processing vignette 'tree_level.Rmd' failed with diagnostics:
     unused argument (path = file.path(dir.result, .data.red$file[1]))
     --- failed re-building ‘tree_level.Rmd’
     
     SUMMARY: processing the following file failed:
       ‘tree_level.Rmd’
     
     Error: Vignette re-building failed.
     Execution halted
     ```

# readr (2.1.6)

* GitHub: <https://github.com/tidyverse/readr>
* Email: <mailto:jenny@posit.co>
* GitHub mirror: <https://github.com/cran/readr>

Run `revdepcheck::cloud_details(, "readr")` for more info

## Newly broken

*   checking tests ... ERROR
     ```
     ...
         'test-read-fwf.R:142:3', 'test-read-fwf.R:174:3', 'test-read-fwf.R:184:3',
         'test-read-lines.R:89:3', 'test-write.R:11:3'
       
       ══ Failed tests ════════════════════════════════════════════════════════════════
       ── Error ('test-write.R:196:3'): Can change the escape behavior for quotes ─────
       Error in `vroom::vroom_format(x, delim = delim, eol = eol, col_names = col_names, na = na, quote = quote, escape = escape)`: `escape` must be one of "double", "backslash", or "none", not "invalid".
       Backtrace:
            ▆
         1. ├─testthat::expect_error(...) at test-write.R:196:3
         2. │ └─testthat:::expect_condition_matching_(...)
         3. │   └─testthat:::quasi_capture(...)
         4. │     ├─testthat (local) .capture(...)
         5. │     │ └─base::withCallingHandlers(...)
         6. │     └─rlang::eval_bare(quo_get_expr(.quo), quo_get_env(.quo))
         7. ├─readr::format_delim(df, "\t", escape = "invalid")
         8. │ └─vroom::vroom_format(escape = escape)
         9. │   └─rlang::arg_match(escape)
        10. │     └─rlang::arg_match0(arg, values, error_arg, error_call = error_call)
        11. └─rlang:::stop_arg_match(w, values = x, error_arg = y, error_call = z)
        12.   └─rlang::abort(msg, call = error_call, arg = error_arg)
       
       [ FAIL 1 | WARN 0 | SKIP 37 | PASS 713 ]
       Error:
       ! Test failures.
       Execution halted
     ```

