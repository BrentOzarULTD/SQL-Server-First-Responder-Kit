# sp_BlitzFirst Checks by Priority

This table lists all checks ordered by priority. 

Before adding a new check, make sure to add a Github issue for it first, and have a group discussion about its priority, description, and findings URL.

If you want to change anything about a check - the priority, finding, URL, or ID - open a Github issue first. The relevant scripts have to be updated too.

CURRENT HIGH CHECKID: 43
If you want to add a new check, start at 44

| Priority | FindingsGroup | Finding | URL | CheckID |
|----------|---------------------------------|---------------------------------------|-------------------------------------------------|----------|
| 0 | Outdated sp_BlitzFirst | sp_BlitzFirst is Over 6 Months Old | http://FirstResponderKit.org/ | 27 |
| 0 | Outdated or Missing sp_BlitzCache | Update Your sp_BlitzCache | http://FirstResponderKit.org/ | 36 |
| 1 | Logged Message | Logged from sp_BlitzFirst | http://FirstResponderKit.org | 38 |
| 1 | Maintenance Tasks Running | Backup Running | https://BrentOzar.com/askbrent/backups | 1 |
| 1 | Maintenance Tasks Running | DBCC CHECK* Running | https://BrentOzar.com/askbrent/dbcc | 2 |
| 1 | Maintenance Tasks Running | Restore Running | https://BrentOzar.com/askbrent/backups | 3 |
| 1 | Query Problems | Long-Running Query Blocking Others | https://BrentOzar.com/go/blocking | 5 |
| 1 | Query Problems | Query Rolling Back | https://BrentOzar.com/go/rollback | 9 |
| 1 | Query Problems | Sleeping Query with Open Transactions | https://BrentOzar.com/go/sleeping | 8 |
| 1 | SQL Server Internal Maintenance | Data File Growing | https://BrentOzar.com/go/instant | 4 |
| 1 | SQL Server Internal Maintenance | Log File Growing | https://BrentOzar.com/go/logsize | 13 |
| 1 | SQL Server Internal Maintenance | Log File Shrinking | https://BrentOzar.com/go/logsize | 14 |
| 10 | Server Performance | Poison Wait Detected | https://BrentOzar.com/go/poison | 30 |
| 10 | Server Performance | Target Memory Lower Than Max | https://BrentOzar.com/go/target | 35 |
| 40 | Table Problems | Forwarded Fetches/Sec High | https://BrentOzar.com/go/fetch | 29 |
| 50 | In-Memory OLTP | Garbage Collection in Progress | https://BrentOzar.com/go/garbage | 31 |
| 50 | Query Problems | Compilations/Sec High | https://BrentOzar.com/go/compile | 15 |
| 50 | Query Problems | Plan Cache Erased Recently | https://BrentOzar.com/go/freeproccache | 7 |
| 50 | Query Problems | Re-Compilations/Sec High | https://BrentOzar.com/go/recompile | 16 |
| 50 | Query Problems | ImplicitTransactions | https://www.brentozar.com/go/ImplicitTransactions/ | 37 |
| 50 | Server Performance | High CPU Utilization | https://BrentOzar.com/go/cpu | 24 |
| 50 | Server Performance | High CPU Utilization - Non SQL Processes | https://BrentOzar.com/go/cpu | 28 |
| 50 | Server Performance | Page Life Expectancy Low | https://BrentOzar.com/go/ple | 10 |
| 50 | Server Performance | Slow Data File Reads | https://BrentOzar.com/go/slow | 11 |
| 50 | Server Performance | Slow Log File Writes | https://BrentOzar.com/go/slow | 12 |
| 50 | Server Performance | Too Much Free Memory | https://BrentOzar.com/go/freememory | 34 |
| 50 | Server Performance | Memory Grants pending | https://www.brentozar.com/blitz/memory-grants | 39 |
| 100 | In-Memory OLTP | Transactions aborted | https://BrentOzar.com/go/aborted | 32 |
| 100 | Query Problems | Suboptimal Plans/Sec High | https://BrentOzar.com/go/suboptimal | 33 |
| 100 | Query Problems | Bad Estimates | https://brentozar.com/go/skewedup | 42 |
| 100 | Query Problems | Skewed Parallelism | https://brentozar.com/go/skewedup | 43 |
| 200 | Wait Stats | (One per wait type) | https://BrentOzar.com/sql/wait-stats/#(waittype) | 6 |
| 210 | Query Stats | Plan Cache Analysis Skipped | https://BrentOzar.com/go/topqueries | 18 |
| 210 | Query Stats | Top Resource-Intensive Queries | https://BrentOzar.com/go/topqueries | 17 |
| 250 | Server Info | Batch Requests per Second | https://BrentOzar.com/go/measure | 19 |
| 250 | Server Info | Re-Compiles per Second | https://BrentOzar.com/go/measure | 26 |
| 250 | Server Info | SQL Compilations/sec | https://BrentOzar.com/go/measure | 25 |
| 250 | Server Info | Wait Time per Core per Second | https://BrentOzar.com/go/measure | 20 |
| 251 | Server Info | CPU Utilization |  | 23 |
| 251 | Server Info | Database Count |  | 22 |
| 251 | Server Info | Database Size, Total GB |  | 21 |
| 251 | Server Info | Memory Grant/Workspace info |  | 40 |
