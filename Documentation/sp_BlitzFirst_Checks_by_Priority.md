# sp_BlitzFirst Checks by Priority

This table lists all checks ordered by priority. 

Before adding a new check, make sure to add a Github issue for it first, and have a group discussion about its priority, description, and findings URL.

If you want to change anything about a check - the priority, finding, URL, or ID - open a Github issue first. The relevant scripts have to be updated too.

CURRENT HIGH CHECKID: 47
If you want to add a new check, start at 48

| Priority | FindingsGroup | Finding | URL | CheckID |
|----------|---------------------------------|---------------------------------------|-------------------------------------------------|----------|
| 0 | Outdated sp_BlitzFirst | sp_BlitzFirst is Over 6 Months Old | http://FirstResponderKit.org/ | 27 |
| 0 | Outdated or Missing sp_BlitzCache | Update Your sp_BlitzCache | http://FirstResponderKit.org/ | 36 |
| 1 | Logged Message | Logged from sp_BlitzFirst | http://FirstResponderKit.org | 38 |
| 1 | Maintenance Tasks Running | Backup Running | https://www.brentozar.com/askbrent/backups | 1 |
| 1 | Maintenance Tasks Running | DBCC CHECK* Running | https://www.brentozar.com/askbrent/dbcc | 2 |
| 1 | Maintenance Tasks Running | Restore Running | https://www.brentozar.com/askbrent/backups | 3 |
| 1 | Query Problems | Long-Running Query Blocking Others | https://www.brentozar.com/go/blocking | 5 |
| 1 | Query Problems | Query Rolling Back | https://www.brentozar.com/go/rollback | 9 |
| 1 | Query Problems | Sleeping Query with Open Transactions | https://www.brentozar.com/go/sleeping | 8 |
| 1 | SQL Server Internal Maintenance | Data File Growing | https://www.brentozar.com/go/instant | 4 |
| 1 | SQL Server Internal Maintenance | Log File Growing | https://www.brentozar.com/go/logsize | 13 |
| 1 | SQL Server Internal Maintenance | Log File Shrinking | https://www.brentozar.com/go/logsize | 14 |
| 10 | Server Performance | Poison Wait Detected | https://www.brentozar.com/go/poison | 30 |
| 10 | Server Performance | Target Memory Lower Than Max | https://www.brentozar.com/go/target | 35 |
| 10 | Azure Performance | Database is Maxed Out | https://www.brentozar.com/go/maxedout | 41 |
| 40 | Table Problems | Forwarded Fetches/Sec High | https://www.brentozar.com/go/fetch | 29 |
| 50 | In-Memory OLTP | Garbage Collection in Progress | https://www.brentozar.com/go/garbage | 31 |
| 50 | Query Problems | Compilations/Sec High | https://www.brentozar.com/go/compile | 15 |
| 50 | Query Problems | Implicit Transactions | https://www.brentozar.com/go/ImplicitTransactions/ | 37 |
| 50 | Query Problems | Memory Leak in USERSTORE_TOKENPERM Cache | https://www.brentozar.com/go/userstore | 45 |
| 50 | Query Problems | Plan Cache Erased Recently | https://www.brentozar.com/go/freeproccache | 7 |
| 50 | Query Problems | Re-Compilations/Sec High | https://www.brentozar.com/go/recompile | 16 |
| 50 | Query Problems | Statistics Updated Recently | https://www.brentozar.com/go/stats | 44 |
| 50 | Query Problems | High Percentage Of Runnable Queries | https://erikdarlingdata.com/go/RunnableQueue/ | 47 |
| 50 | Server Performance | High CPU Utilization | https://www.brentozar.com/go/cpu | 24 |
| 50 | Server Performance | High CPU Utilization - Non SQL Processes | https://www.brentozar.com/go/cpu | 28 |
| 50 | Server Performance | Slow Data File Reads | https://www.brentozar.com/go/slow | 11 |
| 50 | Server Performance | Slow Log File Writes | https://www.brentozar.com/go/slow | 12 |
| 50 | Server Performance | Too Much Free Memory | https://www.brentozar.com/go/freememory | 34 |
| 50 | Server Performance | Memory Grants pending | https://www.brentozar.com/blitz/memory-grants | 39 |
| 100 | In-Memory OLTP | Transactions aborted | https://www.brentozar.com/go/aborted | 32 |
| 100 | Query Problems | Suboptimal Plans/Sec High | https://www.brentozar.com/go/suboptimal | 33 |
| 100 | Query Problems | Bad Estimates | https://www.brentozar.com/go/skewedup | 42 |
| 100 | Query Problems | Skewed Parallelism | https://www.brentozar.com/go/skewedup | 43 |
| 100 | Query Problems | Query with a memory grant exceeding @MemoryGrantThresholdPct | https://www.brentozar.com/memory-grants-sql-servers-public-toilet/ | 46 |
| 200 | Wait Stats | (One per wait type) | https://www.brentozar.com/sql/wait-stats/#(waittype) | 6 |
| 210 | Query Stats | Plan Cache Analysis Skipped | https://www.brentozar.com/go/topqueries | 18 |
| 210 | Query Stats | Top Resource-Intensive Queries | https://www.brentozar.com/go/topqueries | 17 |
| 250 | Server Info | Batch Requests per Second | https://www.brentozar.com/go/measure | 19 |
| 250 | Server Info | Re-Compiles per Second | https://www.brentozar.com/go/measure | 26 |
| 250 | Server Info | SQL Compilations/sec | https://www.brentozar.com/go/measure | 25 |
| 250 | Server Info | Wait Time per Core per Second | https://www.brentozar.com/go/measure | 20 |
| 251 | Server Info | CPU Utilization |  | 23 |
| 251 | Server Info | Database Count |  | 22 |
| 251 | Server Info | Database Size, Total GB |  | 21 |
| 251 | Server Info | Memory Grant/Workspace info |  | 40 |
