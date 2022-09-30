# readr

<details>

* Version: 2.1.2
* GitHub: https://github.com/tidyverse/readr
* Source code: https://github.com/cran/readr
* Date/Publication: 2022-01-30 22:30:02 UTC
* Number of recursive dependencies: 73

Run `cloud_details(, "readr")` for more info

</details>

## Newly broken

*   checking tests ... ERROR
    ```
      Running ‘first_edition.R’
      Running ‘second_edition.R’
    Running the tests in ‘tests/second_edition.R’ failed.
    Last 13 lines of output:
      ══ Failed tests ════════════════════════════════════════════════════════════════
      ── Failure (test-write.R:11:3): a literal NA is quoted ─────────────────────────
      format_csv(data.frame(x = "NA")) (`actual`) not equal to "x\n\"NA\"\n" (`expected`).
      
      `lines(actual)`:   "x" "NA"     ""
      `lines(expected)`: "x" "\"NA\"" ""
      ── Failure (test-write.R:16:3): na argument modifies how missing values are written ──
      format_csv(df, na = ".") (`actual`) not equal to "x,y\n.,1\nx,2\n\".\",.\n" (`expected`).
      
      `lines(actual)`:   "x,y" ".,1" "x,2" ".,."     ""
      `lines(expected)`: "x,y" ".,1" "x,2" "\".\",." ""
      
      [ FAIL 2 | WARN 0 | SKIP 36 | PASS 709 ]
      Error: Test failures
      Execution halted
    ```

## In both

*   checking installed package size ... NOTE
    ```
      installed size is 10.8Mb
      sub-directories of 1Mb or more:
        libs   9.9Mb
    ```

