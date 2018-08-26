# sp_BlitzCache Checks by Priority

This table lists all checks ordered by priority. 

Before adding a new check, make sure to add a Github issue for it first, and have a group discussion about its priority, description, and findings URL.

If you want to change anything about a check - the priority, finding, URL, or ID - open a Github issue first. The relevant scripts have to be updated too.

CURRENT HIGH CHECKID: 61
If you want to add a new check, start at 62

| Priority | FindingsGroup | Finding | URL | CheckID | Expert Mode | 
|----------|---------------------------------|---------------------------------------|-------------------------------------------------|----------|-------------|
| 100 | Execution Pattern | Frequently Executed Queries | http://brentozar.com/blitzcache/frequently-executed-queries/ | 1 | No |
| 50 | Parameterization | Parameter Sniffing | http://brentozar.com/blitzcache/parameter-sniffing/ | 2 | No |
| 50 | Parameterization | Forced Plans | http://brentozar.com/blitzcache/forced-plans/ | 3 | No |
| 200 | Cursors | Cursors | http://brentozar.com/blitzcache/cursors-found-slow-queries/ | 4 | No |
| 200 | Cursors | Optimistic Cursors | http://brentozar.com/blitzcache/cursors-found-slow-queries/ | 4 | No |
| 200 | Cursors | Non-forward Only Cursors | http://brentozar.com/blitzcache/cursors-found-slow-queries/ | 4 | No |
| 200 | Cursors | Dynamic Cursors | http://brentozar.com/blitzcache/cursors-found-slow-queries/ | 4 | No |
| 200 | Cursors | Fast Forward Cursors | http://brentozar.com/blitzcache/cursors-found-slow-queries/ | 4 | No |
| 50 | Parameterization | Forced Parameterization | http://brentozar.com/blitzcache/forced-parameterization/ | 5 | No |
| 200 | Execution Plans | Parallelism | http://brentozar.com/blitzcache/parallel-plans-detected/ | 6 | No |
| 200 | Execution Plans | Nearly Parallel | http://brentozar.com/blitzcache/query-cost-near-cost-threshold-parallelism/ | 7 | No |
| 50 | Execution Plans | Query Plan Warnings | http://brentozar.com/blitzcache/query-plan-warnings/ | 8 | No |
| 50 | Performance | Long Running Queries | http://brentozar.com/blitzcache/long-running-queries/ | 9 | No |
| 50 | Performance | Missing Index Request | http://brentozar.com/blitzcache/missing-index-request/ | 10 | No |
| 200 | Cardinality | Legacy Cardinality Estimator in Use | http://brentozar.com/blitzcache/legacy-cardinality-estimator/ | 13 | No |
| 50 | Performance | Implicit Conversions | http://brentozar.com/go/implicit | 14 | No |
| 100 | Performance | Frequently executed operators | http://brentozar.com/blitzcache/busy-loops/ | 16 | Yes |
| 50 | Performance | Joining to table valued functions | http://brentozar.com/blitzcache/tvf-join/ | 17 | Yes |
| 50 | Execution Plans | Compilation timeout | http://brentozar.com/blitzcache/compilation-timeout/ | 18 | No |
| 50 | Execution Plans | Compilation memory limit exceeded | http://brentozar.com/blitzcache/compile-memory-limit-exceeded/ | 19 | No |
| 50 | Execution Plans | No join predicate | http://brentozar.com/blitzcache/no-join-predicate/ | 20 | No |
| 200 | Execution Plans | Multiple execution plans | http://brentozar.com/blitzcache/multiple-plans/ | 21 | No |
| 100 | Performance | Unmatched indexes | http://brentozar.com/blitzcache/unmatched-indexes | 22 | No |
| 100 | Parameterization | Unparameterized queries | http://brentozar.com/blitzcache/unparameterized-queries | 23 | Yes |
| 100 | Execution Plans | Trivial Plans | http://brentozar.com/blitzcache/trivial-plans | 24 | No |
| 10 | Execution Plans | Forced Serialization | http://www.brentozar.com/blitzcache/forced-serialization/ | 25 | No |
| 100 | Execution Plans | Expensive Key Lookups | http://www.brentozar.com/blitzcache/expensive-key-lookups/ | 26 | No |
| 100 | Execution Plans | Expensive Remote Query | http://www.brentozar.com/blitzcache/expensive-remote-query/ | 28 |  |
| 200 | Trace Flags | Session Level Trace Flags Enabled | https://www.brentozar.com/blitz/trace-flags-enabled-globally/ | 29 | No |
| 100 | Unused memory grants | Queries are asking for more memory than they're using | https://www.brentozar.com/blitzcache/unused-memory-grants/ | 30 | No |
| 100 | Compute Scalar That References A Function | This could be trouble if you''re using Scalar Functions or MSTVFs |  https://www.brentozar.com/blitzcache/compute-scalar-functions/| 31 | Yes |
| 100 | Compute Scalar That References A CLR Function | TThis could be trouble if your CLR functions perform data access |  https://www.brentozar.com/blitzcache/compute-scalar-functions/| 31 | Yes |
| 100 | Table Variables detected | Beware nasty side effects | https://www.brentozar.com/blitzcache/table-variables/ | 33 | No |
| 100 | Columns with no statistics | Poor cardinality estimates may ensue | https://www.brentozar.com/blitzcache/columns-no-statistics/ | 35 | No |
| 100 | Operator Warnings | SQL is throwing operator level plan warnings | http://brentozar.com/blitzcache/query-plan-warnings/ | 36 | Yes |
| 100 | Table Scans | Your database has HEAPs | https://www.brentozar.com/archive/2012/05/video-heaps/ | 37 | No |
| 200 | Backwards Scans | Indexes are being read backwards | https://www.brentozar.com/blitzcache/backwards-scans/ | 38 | Yes |
| 100 | Index forcing | Someone is using hints to force index usage | https://www.brentozar.com/blitzcache/optimizer-forcing/ | 39 | Yes |
| 100 | Seek/Scan forcing | Someone is using hints to force index seeks/scans | https://www.brentozar.com/blitzcache/optimizer-forcing/ | 40 | Yes |
| 100 | ColumnStore indexes operating in Row Mode | Batch Mode is optimal for ColumnStore indexes | https://www.brentozar.com/blitzcache/columnstore-indexes-operating-row-mode/ | 41 | Yes |
| 50 | Computed Columns Referencing Scalar UDFs | This makes a whole lot of stuff run serially | https://www.brentozar.com/blitzcache/computed-columns-referencing-functions/ | 42 | Yes |
| 100 | Execution Plan | Expensive Sort | http://www.brentozar.com/blitzcache/expensive-sorts/ | 43 | No |
| 50 | Filters Referencing Scalar UDFs | This forces serialization | https://www.brentozar.com/blitzcache/compute-scalar-functions/ | 44 | Yes |
| 100 | Many Indexes Modified | Write Queries Are Hitting >= 5 Indexes | https://www.brentozar.com/blitzcache/many-indexes-modified/ | 45 | Yes |
| 200 | Plan Confusion | Row Level Security is in use | https://www.brentozar.com/blitzcache/row-level-security/ | 46 | Yes |
| 200 | Spatial Abuse | You hit a Spatial Index | https://www.brentozar.com/blitzcache/spatial-indexes/ | 47 | Yes |
| 150 | Index DML | Indexes were created or dropped | https://www.brentozar.com/blitzcache/index-dml/ | 48 | Yes |
| 150 | Table DML | Tables were created or dropped | https://www.brentozar.com/blitzcache/table-dml/ | 49 | Yes |
| 150 | Long Running Low CPU | You have a query that runs for much longer than it uses CPU | https://www.brentozar.com/blitzcache/long-running-low-cpu/ | 50 | No |
| 150 | Low Cost Query With High CPU | You have a low cost query that uses a lot of CPU | https://www.brentozar.com/blitzcache/low-cost-high-cpu/ | 51 | No |
| 150 | Biblical Statistics | Statistics used in queries are >7 days old with >100k modifications | https://www.brentozar.com/blitzcache/stale-statistics/ | 52 | No |
| 200 | Adaptive joins | Living in the future | https://www.brentozar.com/blitzcache/adaptive-joins/ | 53 | No |
| 150 | Expensive Index Spool | You have an index spool, this is usually a sign that there's an index missing somewhere. | https://www.brentozar.com/blitzcache/eager-index-spools/ | 54 | No |
| 150 | Index Spools Many Rows | You have an index spool that spools more rows than the query returns | https://www.brentozar.com/blitzcache/eager-index-spools/ | 55 | No |
| 100 | Potentially bad cardinality estimates | Estimated rows are different from average rows by a factor of 10000 | https://www.brentozar.com/blitzcache/bad-estimates/ | 56 | Yes |
| 200 | Is Paul White Electric? | This query has a Switch operator in it! | http://sqlblog.com/blogs/paul_white/archive/2013/06/11/hello-operator-my-switch-is-bored.aspx | 998 | Yes |
| 200 | Database Level Statistics | Database has stats updated 7 days ago with more than 100k modifications | https://www.brentozar.com/blitzcache/stale-statistics/ | 999 | No |
| 200 | Row Goals | This query had row goals introduced | https://www.brentozar.com/go/rowgoals/ | 58 | Yes |
| 100 | tempdb Spills | This query spills >500mb to tempdb on average | https://www.brentozar.com/blitzcache/tempdb-spills/ | 59 | No |
| 100 | MSTVFs | These have many of the same problems scalar UDFs have | http://brentozar.com/blitzcache/tvf-join/ | 60 | No |
| 100 | Many to Many Merge | These use secret worktables that could be doing lots of reads | Blog not published yet | 61 | Yes |
| 50 | Non-SARGable queries | Queries may have non-SARGable predicates |http://brentozar.com/go/sargable| http://brentozar.com/go/sargable | No |
| 254 | Plan Cache Information | Breaks cache down by creation date (24/4/1 hrs) | None | 999 | No |
| 255 | Global Trace Flags Enabled | You have Global Trace Flags enabled on your server | https://www.brentozar.com/blitz/trace-flags-enabled-globally/ | 1000 | No |
| 255 | Need more help? | Paste your plan on the internet! | http://pastetheplan.com | 2147483646 | No |
| 255 | Thanks for using sp_BlitzCache! | From Your Community Volunteers | http://FirstResponderKit.org | 2147483647 | No |


## Blank row for the future
|  |  |  |  |  |  |




