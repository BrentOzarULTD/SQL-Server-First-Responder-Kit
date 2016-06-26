# sp_BlitzFirst Checks by Priority

This table lists all checks ordered by priority. 

Before adding a new check, make sure to add a Github issue for it first, and have a group discussion about its priority, description, and findings URL.

If you want to change anything about a check - the priority, finding, URL, or ID - open a Github issue first. The relevant scripts have to be updated too.

| Priority | FindingsGroup | Finding | URL | CheckID |
|----------|---------------------------------|---------------------------------------|-------------------------------------------------|----------|
| 1 | Maintenance Tasks Running | Backup Running | http://BrentOzar.com/askbrent/backups | 1 |
| 1 | Maintenance Tasks Running | DBCC Running | http://BrentOzar.com/askbrent/dbcc | 2 |
| 1 | Maintenance Tasks Running | Restore Running | http://BrentOzar.com/askbrent/backups | 3 |
| 1 | Outdated sp_AskBrent | sp_AskBrent is Over 6 Months Old | http://BrentOzar.com/askbrent/ | 27 |
| 1 | Query Problems | Long-Running Query Blocking Others | http://BrentOzar.com/go/blocking | 5 |
| 1 | Query Problems | Query Rolling Back | http://BrentOzar.com/go/rollback | 9 |
| 1 | Query Problems | Sleeping Query with Open Transactions | http://BrentOzar.com/go/sleeping | 8 |
| 1 | SQL Server Internal Maintenance | Data File Growing | http://BrentOzar.com/go/instant | 4 |
| 1 | SQL Server Internal Maintenance | Log File Growing | http://BrentOzar.com/go/logsize | 13 |
| 1 | SQL Server Internal Maintenance | Log File Shrinking | http://BrentOzar.com/go/logsize | 14 |
| 50 | Query Problems | Compilations/Sec High | http://BrentOzar.com/go/compile | 15 |
| 50 | Query Problems | Plan Cache Erased Recently | http://BrentOzar.com/go/freeproccache | 7 |
| 50 | Query Problems | Re-Compilations/Sec High | http://BrentOzar.com/go/recompile | 16 |
| 50 | Server Performance | High CPU Utilization | http://BrentOzar.com/go/cpu | 24 |
| 50 | Server Performance | Page Life Expectancy Low | http://BrentOzar.com/go/ple | 10 |
| 50 | Server Performance | Slow Data File Reads | http://BrentOzar.com/go/slow | 11 |
| 50 | Server Performance | Slow Log File Writes | http://BrentOzar.com/go/slow | 12 |
| 200 | Wait Stats | (One per wait type) | http://BrentOzar.com/sql/wait-stats/#(waittype) | 6 |
| 210 | Query Stats | Plan Cache Analysis Skipped | http://BrentOzar.com/go/topqueries | 18 |
| 210 | Query Stats | Top Resource-Intensive Queries | http://BrentOzar.com/go/topqueries | 17 |
| 250 | Server Info | Batch Requests per Second | http://BrentOzar.com/go/measure | 19 |
| 250 | Server Info | Re-Compiles per Second | http://BrentOzar.com/go/measure | 26 |
| 250 | Server Info | SQL Compilations/sec | http://BrentOzar.com/go/measure | 25 |
| 250 | Server Info | Wait Time per Core per Second | http://BrentOzar.com/go/measure | 20 |
| 251 | Server Info | CPU Utilization |  | 23 |
| 251 | Server Info | Database Count |  | 22 |
| 251 | Server Info | Database Size, Total GB |  | 21 |
