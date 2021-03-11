# sp_BlitzCache Checks by Priority

This table lists all checks ordered by priority. 

Before adding a new check, make sure to add a Github issue for it first, and have a group discussion about its priority, description, and findings URL.

If you want to change anything about a check - the priority, finding, URL, or ID - open a Github issue first. The relevant scripts have to be updated too.

CURRENT HIGH CHECKID: 69
If you want to add a new check, start at 70

| Priority | FindingsGroup | Finding | URL | CheckID | Expert Mode |
|----------|---------------------------------|---------------------------------------|-------------------------------------------------|----------|-------------|
| 10 | Execution Plans | Forced Serialization | https://www.brentozar.com/blitzcache/forced-serialization/ | 25 | No |
| 10 | Large USERSTORE_TOKENPERM cache | Using Over 10% of the Buffer Pool | https://www.brentozar.com/go/userstore | 69 | No |
| 50 | Complexity | High Compile CPU | https://www.brentozar.com/blitzcache/high-compilers/ | 64 | No |
| 50 | Complexity | High Compile Memory | https://www.brentozar.com/blitzcache/high-compilers/ | 65 | No |
| 50 | Execution Plans | Compilation timeout | https://www.brentozar.com/blitzcache/compilation-timeout/ | 18 | No |
| 50 | Execution Plans | Compile Memory Limit Exceeded | https://www.brentozar.com/blitzcache/compile-memory-limit-exceeded/ | 19 | No |
| 50 | Execution Plans | No join predicate | https://www.brentozar.com/blitzcache/no-join-predicate/ | 20 | No |
| 50 | Execution Plans | Plan Warnings | https://www.brentozar.com/blitzcache/query-plan-warnings/ | 8 | No |
| 50 | Functions | Computed Column UDF | https://www.brentozar.com/blitzcache/computed-columns-referencing-functions/ | 42 | Yes |
| 50 | Functions | Filter UDF | https://www.brentozar.com/blitzcache/compute-scalar-functions/ | 44 | Yes |
| 50 | Non-SARGable queries | Queries may have non-SARGable predicates |https://www.brentozar.com/go/sargable| 62 | No |
| 50 | Parameterization | Forced Parameterization | https://www.brentozar.com/blitzcache/forced-parameterization/ | 5 | No |
| 50 | Parameterization | Forced Plan | https://www.brentozar.com/blitzcache/forced-plans/ | 3 | No |
| 50 | Parameterization | Parameter Sniffing | https://www.brentozar.com/blitzcache/parameter-sniffing/ | 2 | No |
| 50 | Performance | Function Join | https://www.brentozar.com/blitzcache/tvf-join/ | 17 | Yes |
| 50 | Performance | Implicit Conversions | https://www.brentozar.com/go/implicit | 14 | No |
| 50 | Performance | Long Running Query | https://www.brentozar.com/blitzcache/long-running-queries/ | 9 | No |
| 50 | Performance | Missing Indexes | https://www.brentozar.com/blitzcache/missing-index-request/ | 10 | No |
| 50 | Selects w/ Writes | Read queries are causing writes | https://dba.stackexchange.com/questions/191825/ | 66 | No |
| 100 | Complexity | Long Compile Time | https://www.brentozar.com/blitzcache/high-compilers/ | No |
| 100 | Complexity | Many to Many Merge | Blog not published yet | 61 | Yes |
| 100 | Complexity | Row Estimate Mismatch | https://www.brentozar.com/blitzcache/bad-estimates/ | 56 | Yes |
| 100 | Compute Scalar That References A CLR Function | Calls CLR Functions |  https://www.brentozar.com/blitzcache/compute-scalar-functions/| 31 | Yes |
| 100 | Compute Scalar That References A Function | Calls Functions |  https://www.brentozar.com/blitzcache/compute-scalar-functions/| 31 | Yes |
| 100 | Execution Pattern | Frequently Execution | https://www.brentozar.com/blitzcache/frequently-executed-queries/ | 1 | No |
| 100 | Execution Plans | Expensive Key Lookup | https://www.brentozar.com/blitzcache/expensive-key-lookups/ | 26 | No |
| 100 | Execution Plans | Expensive Remote Query | https://www.brentozar.com/blitzcache/expensive-remote-query/ | 28 |  |
| 100 | Execution Plans | Expensive Sort | https://www.brentozar.com/blitzcache/expensive-sorts/ | 43 | No |
| 100 | Execution Plans | Trivial Plans | https://www.brentozar.com/blitzcache/trivial-plans | 24 | No |
| 100 | Functions | MSTVFs | https://www.brentozar.com/blitzcache/tvf-join/ | 60 | No |
| 100 | Indexes | \>= 5 Indexes Modified | https://www.brentozar.com/blitzcache/many-indexes-modified/ | 45 | Yes |
| 100 | Indexes | ColumnStore Row Mode | https://www.brentozar.com/blitzcache/columnstore-indexes-operating-row-mode/ | 41 | Yes |
| 100 | Indexes | Forced Indexes | https://www.brentozar.com/blitzcache/optimizer-forcing/ | 39 | Yes |
| 100 | Indexes | Forced Seeks/Scans | https://www.brentozar.com/blitzcache/optimizer-forcing/ | 40 | Yes |
| 100 | Indexes | Table Scans (Heaps) | https://www.brentozar.com/archive/2012/05/video-heaps/ | 37 | No |
| 100 | Memory Grant | Unused Memory Grant | https://www.brentozar.com/blitzcache/unused-memory-grants/ | 30 | No |
| 100 | Parameterization | Unparameterized Query | https://www.brentozar.com/blitzcache/unparameterized-queries | 23 | Yes |
| 100 | Performance | Frequently executed operators | https://www.brentozar.com/blitzcache/busy-loops/ | 16 | Yes |
| 100 | Performance | Unmatched Indexes | https://www.brentozar.com/blitzcache/unmatched-indexes | 22 | No |
| 100 | Statistics | Columns With No Statistics | https://www.brentozar.com/blitzcache/columns-no-statistics/ | 35 | No |
| 100 | Table Variables detected | Beware nasty side effects | https://www.brentozar.com/blitzcache/table-variables/ | 33 | No |
| 100 | TempDB | >500mb Spills | https://www.brentozar.com/blitzcache/tempdb-spills/ | 59 | No |
| 100 | Warnings | Operator Warnings | https://www.brentozar.com/blitzcache/query-plan-warnings/ | 36 | Yes |
| 150 | Blocking | Long Running Low CPU | https://www.brentozar.com/blitzcache/long-running-low-cpu/ | 50 | No |
| 150 | Complexity | Index DML | https://www.brentozar.com/blitzcache/index-dml/ | 48 | Yes |
| 150 | Complexity | Low Cost High CPU | https://www.brentozar.com/blitzcache/low-cost-high-cpu/ | 51 | No |
| 150 | Complexity | Table DML | https://www.brentozar.com/blitzcache/table-dml/ | 49 | Yes |
| 150 | Statistics | Statistics used have > 100k modifications in the last 7 days | https://www.brentozar.com/blitzcache/stale-statistics/ | 52 | No |
| 150 | Indexes | Expensive Index Spool | https://www.brentozar.com/blitzcache/eager-index-spools/ | 54 | No |
| 150 | Indexes | Expensive Index Spool | https://sqlperformance.com/2019/09/sql-performance/nested-loops-joins-performance-spools | 67 | No |
| 150 | Indexes | Large Index Row Spool | https://www.brentozar.com/blitzcache/eager-index-spools/ | 55 | No |
| 150 | Indexes | Large Index Row Spool | https://sqlperformance.com/2019/09/sql-performance/nested-loops-joins-performance-spools | 68 | No |
| 200 | Cardinality | Downlevel CE | https://www.brentozar.com/blitzcache/legacy-cardinality-estimator/ | 13 | No |
| 200 | Complexity | Adaptive Joins | https://www.brentozar.com/blitzcache/adaptive-joins/ | 53 | No |
| 200 | Complexity | Row Goals | https://www.brentozar.com/go/rowgoals/ | 58 | Yes |
| 200 | Complexity | Row Level Security | https://www.brentozar.com/blitzcache/row-level-security/ | 46 | Yes |
| 200 | Complexity | Spatial Index | https://www.brentozar.com/blitzcache/spatial-indexes/ | 47 | Yes |
| 200 | Cursors | Cursor | https://www.brentozar.com/blitzcache/cursors-found-slow-queries/ | 4 | No |
| 200 | Cursors | Dynamic Cursors | https://www.brentozar.com/blitzcache/cursors-found-slow-queries/ | 4 | No |
| 200 | Cursors | Fast Forward Cursors | https://www.brentozar.com/blitzcache/cursors-found-slow-queries/ | 4 | No |
| 200 | Cursors | Non-forward Only Cursors | https://www.brentozar.com/blitzcache/cursors-found-slow-queries/ | 4 | No |
| 200 | Cursors | Optimistic Cursors | https://www.brentozar.com/blitzcache/cursors-found-slow-queries/ | 4 | No |
| 200 | Database Level Statistics | Database has stats updated 7 days ago with more than 100k modifications | https://www.brentozar.com/blitzcache/stale-statistics/ | 997 | No |
| 200 | Execution Plans | Multiple Plans | https://www.brentozar.com/blitzcache/multiple-plans/ | 21 | No |
| 200 | Execution Plans | Nearly Parallel | https://www.brentozar.com/blitzcache/query-cost-near-cost-threshold-parallelism/ | 7 | No |
| 200 | Execution Plans | Parallel | https://www.brentozar.com/blitzcache/parallel-plans-detected/ | 6 | No |
| 200 | Indexes | Backwards Scans | https://www.brentozar.com/blitzcache/backwards-scans/ | 38 | Yes |
| 200 | Is Paul White Electric? | This query has a Switch operator in it! | https://www.sql.kiwi/2013/06/hello-operator-my-switch-is-bored.html | 57 | Yes |
| 200 | Trace Flags | Session Level Trace Flags Enabled | https://www.brentozar.com/blitz/trace-flags-enabled-globally/ | 29 | No |
| 254 | Plan Cache Information | Breaks cache down by creation date (24/4/1 hrs) | None | 999 | No |
| 255 | Global Trace Flags Enabled | You have Global Trace Flags enabled on your server | https://www.brentozar.com/blitz/trace-flags-enabled-globally/ | 1000 | No |
| 255 | Need more help? | Paste your plan on the internet! | http://pastetheplan.com | 2147483646 | No |
| 255 | Thanks for using sp_BlitzCache! | From Your Community Volunteers | http://FirstResponderKit.org | 2147483647 | No |


## Blank row for the future
|  |  |  |  |  |  |




