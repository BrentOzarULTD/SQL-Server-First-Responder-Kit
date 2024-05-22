# sp_Blitz Checks by Priority

This table lists all checks ordered by priority. 

Before adding a new check, make sure to add a Github issue for it first, and have a group discussion about its priority, description, and findings URL.

If you want to change anything about a check - the priority, finding, URL, or ID - open a Github issue first. The relevant scripts have to be updated too.

CURRENT HIGH CHECKID: 261.
If you want to add a new one, start at 262.

| Priority | FindingsGroup | Finding | URL | CheckID |
|----------|-----------------------------|---------------------------------------------------------|------------------------------------------------------------------------|----------|
| 0 | Outdated sp_Blitz | sp_Blitz is Over 6 Months Old | https://www.BrentOzar.com/blitz/ | 155 |
| 0 | Informational | @CheckUserDatabaseObjects Disabled | https://www.BrentOzar.com/blitz/ | 201 |
| 0 | Informational | @CheckUserDatabaseObjects Disabled | https://www.BrentOzar.com/blitz/ | 204 |
| 0 | Informational | Some Checks Skipped | https://www.BrentOzar.com/blitz/ | 223 |
| 1 | Backup | Backing Up to Same Drive Where Databases Reside | https://www.BrentOzar.com/go/backup | 93 |
| 1 | Backup | Backups Not Performed Recently | https://www.BrentOzar.com/go/nobak | 1 |
| 1 | Backup | Encryption Certificate Not Backed Up Recently | https://www.BrentOzar.com/go/tde | 202 |
| 1 | Backup | Full Recovery Mode w/o Log Backups | https://www.BrentOzar.com/go/biglogs | 2 |
| 1 | Backup | Log Backups to NUL | https://www.BrentOzar.com/go/nul | 256 |
| 1 | Backup | TDE Certificate Not Backed Up Recently | https://www.BrentOzar.com/go/tde | 119 |
| 1 | Corruption | Database Corruption Detected | https://www.BrentOzar.com/go/repair | 34 |
| 1 | Corruption | Database Corruption Detected | https://www.BrentOzar.com/go/repair | 89 |
| 1 | Corruption | Database Corruption Detected | https://www.BrentOzar.com/go/repair | 90 |
| 1 | Performance | Memory Dangerously Low | https://www.BrentOzar.com/go/max | 51 |
| 1 | Performance | Memory Dangerously Low in NUMA Nodes | https://www.BrentOzar.com/go/max | 159 |
| 1 | Reliability | Evaluation Edition | https://www.BrentOzar.com/go/workgroup | 229 |
| 1 | Reliability | Last good DBCC CHECKDB over 2 weeks old | https://www.BrentOzar.com/go/checkdb | 68 |
| 1 | Security | Dangerous Service Account | https://vladdba.com/SQLServerSvcAccount | 258 |
| 1 | Security | Dangerous Service Account | https://vladdba.com/SQLServerSvcAccount | 259 |
| 1 | Security | Dangerous Service Account | https://vladdba.com/SQLServerSvcAccount | 260 |
| 1 | Security | Dangerous Service Account | https://vladdba.com/SQLServerSvcAccount | 261 |
| 5 | Monitoring | Disabled Internal Monitoring Features | https://msdn.microsoft.com/en-us/library/ms190737.aspx | 177 |
| 5 | Reliability | Dangerous Third Party Modules | https://support.microsoft.com/en-us/kb/2033238 | 179 |
| 5 | Reliability | Priority Boost Enabled | https://www.BrentOzar.com/go/priorityboost | 126 |
| 10 | Performance | 32-bit SQL Server Installed | https://www.BrentOzar.com/go/32bit | 154 |
| 10 | Performance | Auto-Close Enabled | https://www.BrentOzar.com/go/autoclose | 12 |
| 10 | Performance | Auto-Shrink Enabled | https://www.BrentOzar.com/go/autoshrink | 13 |
| 10 | Performance | Auto-Shrink Ran Recently| https://www.BrentOzar.com/go/autoshrink | 206 |
| 10 | Performance | CPU Schedulers Offline | https://www.BrentOzar.com/go/schedulers | 101 |
| 10 | Performance | CPU w/Odd Number of Cores | https://www.BrentOzar.com/go/oddity | 198 |
| 10 | Performance | DBCC DROPCLEANBUFFERS Ran Recently | https://www.BrentOzar.com/go/dbcc | 207 |
| 10 | Performance | DBCC FREEPROCCACHE Ran Recently | https://www.BrentOzar.com/go/dbcc | 208 |
| 10 | Performance | DBCC SHRINK% Ran Recently | https://www.BrentOzar.com/go/dbcc | 210 |
| 10 | Performance | High Memory Use for In-Memory OLTP (Hekaton) | https://www.BrentOzar.com/go/hekaton | 145 |
| 10 | Performance | Memory Nodes Offline | https://www.BrentOzar.com/go/schedulers | 110 |
| 10 | Performance | Plan Cache Erased Recently | https://www.BrentOzar.com/askbrent/plan-cache-erased-recently/ | 125 |
| 10 | Reliability | DBCC WRITEPAGE Used Recently | https://www.BrentOzar.com/go/dbcc | 209 |
| 10 | Reliability | Server restarted in last 24 hours | | 221 |
| 20 | Reliability | Dangerous Build of SQL Server (Corruption) | http://sqlperformance.com/2014/06/sql-indexes/hotfix-sql-2012-rebuilds | 129 |
| 20 | Reliability | Dangerous Build of SQL Server (Security) | https://technet.microsoft.com/en-us/library/security/MS14-044 | 157 |
| 20 | Reliability | Databases in Unusual States | https://www.BrentOzar.com/go/repair | 102 |
| 20 | Reliability | Memory Dumps Have Occurred | https://www.BrentOzar.com/go/dump | 171 |
| 20 | Reliability | No Failover Cluster Nodes Available | https://www.BrentOzar.com/go/node | 184 |
| 20 | Reliability | Query Store Cleanup Disabled | https://www.BrentOzar.com/go/cleanup | 182 |
| 20 | Reliability | Unsupported Build of SQL Server | https://www.BrentOzar.com/go/unsupported | 128 |
| 20 | Reliability | User Databases on C Drive | https://www.BrentOzar.com/go/cdrive | 26 |
| 20 | Reliability | TempDB on C Drive | https://www.BrentOzar.com/go/cdrive | 25 |
| 50 | DBCC Events | Overall Events | https://www.BrentOzar.com/go/dbcc | 203 |
| 50 | Performance | File Growths Slow | https://www.BrentOzar.com/go/filegrowth | 151 |
| 50 | Performance | Instant File Initialization Not Enabled | https://www.BrentOzar.com/go/instant | 192 |
| 50 | Performance | Memory Leak in USERSTORE_TOKENPERM Cache | https://www.BrentOzar.com/go/userstore| 233 |
| 50 | Performance | Poison Wait Detected | https://www.BrentOzar.com/go/poison | 107 |
| 50 | Performance | Poison Wait Detected: CMEMTHREAD & NUMA | https://www.BrentOzar.com/go/poison | 162 |
| 50 | Performance | Poison Wait Detected: Serializable Locking | https://www.BrentOzar.com/go/serializable | 121 |
| 50 | Performance | Recovery Interval Not Optimal| https://sqlperformance.com/2020/05/system-configuration/0-to-60-switching-to-indirect-checkpoints | 257 |
| 50 | Performance | Snapshotting Too Many Databases | https://www.BrentOzar.com/go/toomanysnaps | 236 |
| 50 | Performance | Too Much Free Memory | https://www.BrentOzar.com/go/freememory | 165 |
| 50 | Performance | Wait Stats Cleared Recently| | 205 |
| 50 | Reliability | Full Text Indexes Not Updating | https://www.BrentOzar.com/go/fulltext | 113 |
| 50 | Reliability | Page Verification Not Optimal | https://www.BrentOzar.com/go/torn | 14 |
| 50 | Reliability | Possibly Broken Log Shipping | https://www.BrentOzar.com/go/shipping | 111 |
| 50 | Reliability | TempDB File Error | https://www.BrentOzar.com/go/tempdboops | 191 |
| 50 | Reliability | Transaction Log Larger than Data File | https://www.BrentOzar.com/go/biglog | 75 |
| 50 | Reliability | Default Trace File Error | https://www.brentozar.com/go/defaulttrace | 199 |
| 100 | In-Memory OLTP (Hekaton) | Transaction Errors | https://www.BrentOzar.com/go/hekaton | 147 |
| 100 | Features | Missing Features (2016 SP1) | https://www.BrentOzar.com/ | 189 |
| 100 | Features | Missing Features (2017 CU3) | https://www.BrentOzar.com/ | 216 |
| 100 | Performance | Change Tracking Enabled | https://www.BrentOzar.com/go/tracking | 112 |
| 100 | Performance | Fill Factor Changed | https://www.BrentOzar.com/go/fillfactor | 60 |
| 100 | Performance | High Number of Cached Plans | https://www.BrentOzar.com/go/planlimits | 161 |
| 100 | Performance | Indexes Disabled | https://www.BrentOzar.com/go/ixoff | 47 |
| 100 | Performance | Many Plans for One Query | https://www.BrentOzar.com/go/parameterization | 160 |
| 100 | Performance | Max Memory Set Too High | https://www.BrentOzar.com/go/max | 50 |
| 100 | Performance | Memory Pressure Affecting Queries | https://www.BrentOzar.com/go/grants | 117 |
| 100 | Performance | Partitioned database with non-aligned indexes | https://www.BrentOzar.com/go/aligned | 72 |
| 100 | Performance | Repetitive Maintenance Tasks | https://ola.hallengren.com | 181 |
| 100 | Performance | Resource Governor Enabled | https://www.BrentOzar.com/go/rg | 10 |
| 100 | Performance | Server Triggers Enabled | https://www.BrentOzar.com/go/logontriggers/ | 11 |
| 100 | Performance | Shrink Database Job | https://www.BrentOzar.com/go/autoshrink | 79 |
| 100 | Performance | Shrink Database Step In Maintenance Plan | https://www.BrentOzar.com/go/autoshrink | 180 |
| 100 | Performance | Single-Use Plans in Procedure Cache | https://www.BrentOzar.com/go/single | 35 |
| 100 | Performance | Stored Procedure WITH RECOMPILE | https://www.BrentOzar.com/go/recompile | 78 |
| 100 | Performance | Unusual SQL Server Edition | https://www.BrentOzar.com/go/workgroup | 97 |
| 100 | Performance | Implicit Transactions | https://www.brentozar.com/go/ImplicitTransactions/ | 215 |
| 100 | Reliability | Cumulative Update Available | https://SQLServerUpdates.com | 217 |
| 100 | Reliability | Plan Guides Failing | https://www.BrentOzar.com/go/guides | 164 |
| 100 | Reliability | SQL Server Update May Fail | https://desertdba.com/failovers-cant-serve-two-masters/ | 234 |
| 110 | Performance | Active Tables Without Clustered Indexes | https://www.BrentOzar.com/go/heaps | 38 |
| 110 | Performance | Auto-Create Stats Disabled | https://www.BrentOzar.com/go/acs | 15 |
| 110 | Performance | Auto-Update Stats Disabled | https://www.BrentOzar.com/go/aus | 16 |
| 110 | Performance | Infinite merge replication metadata retention period | https://www.BrentOzar.com/go/merge | 99 |
| 110 | Performance | Parallelism Rocket Surgery | https://www.BrentOzar.com/go/makeparallel | 115 |
| 110 | Performance | Plan Guides Enabled | https://www.BrentOzar.com/go/guides | 95 |
| 110 | Performance | Statistics Without Histograms | https://www.BrentOzar.com/go/brokenstats | 220 |
| 120 | Query Plans | Cursor | https://www.BrentOzar.com/go/cursor | 66 |
| 120 | Query Plans | Implicit Conversion | https://www.BrentOzar.com/go/implicit | 63 |
| 120 | Query Plans | Implicit Conversion Affecting Cardinality | https://www.BrentOzar.com/go/implicit | 64 |
| 120 | Query Plans | Missing Index | https://www.BrentOzar.com/go/missingindex | 65 |
| 120 | Query Plans | RID or Key Lookups | https://www.BrentOzar.com/go/lookup | 118 |
| 120 | Query Plans | Scalar UDFs | https://www.BrentOzar.com/go/functions | 67 |
| 150 | Performance | Check Constraint Not Trusted | https://www.BrentOzar.com/go/trust | 56 |
| 150 | Performance | Deadlocks Happening Daily | https://www.BrentOzar.com/go/deadlocks | 124 |
| 150 | Performance | Forced Parameterization On | https://www.BrentOzar.com/go/forced | 18 |
| 150 | Performance | Foreign Keys Not Trusted | https://www.BrentOzar.com/go/trust | 48 |
| 150 | Performance | Leftover Fake Indexes From Wizards | https://www.BrentOzar.com/go/hypo | 46 |
| 150 | Performance | Objects created with dangerous SET Options | https://www.BrentOzar.com/go/badset | 218 |
| 150 | Performance | Queries Forcing Join Hints | https://www.BrentOzar.com/go/hints | 45 |
| 150 | Performance | Queries Forcing Order Hints | https://www.BrentOzar.com/go/hints | 44 |
| 150 | Performance | Slow Storage Reads on Drive <DRIVELETTER> | https://www.BrentOzar.com/go/slow | 36 |
| 150 | Performance | Slow Storage Writes on Drive <DRIVELETTER> | https://www.BrentOzar.com/go/slow | 37 |
| 150 | Performance | Stats Updated Asynchronously | https://www.BrentOzar.com/go/asyncstats | 17 |
| 150 | Performance | Triggers on Tables | https://www.BrentOzar.com/go/trig | 32 |
| 150 | Performance | Inconsistent Query Store metadata |  | 235 |
| 170 | File Configuration | File growth set to 1MB | https://www.BrentOzar.com/go/percentgrowth | 158 |
| 170 | File Configuration | File growth set to percent | https://www.BrentOzar.com/go/percentgrowth | 82 |
| 170 | File Configuration | High VLF Count | https://www.BrentOzar.com/go/vlf | 69 |
| 170 | File Configuration | Multiple Log Files on One Drive | https://www.BrentOzar.com/go/manylogs | 41 |
| 170 | File Configuration | System Database on C Drive | https://www.BrentOzar.com/go/drivec | 24 |
| 170 | File Configuration | TempDB Has >16 Data Files | https://www.BrentOzar.com/go/tempdb | 175 |
| 170 | File Configuration | TempDB Only Has 1 Data File | https://www.BrentOzar.com/go/tempdb | 40 |
| 170 | File Configuration | TempDB Unevenly Sized Data Files | https://www.BrentOzar.com/go/tempdb | 183 |
| 170 | File Configuration | Uneven File Growth Settings in One Filegroup | https://www.BrentOzar.com/go/grow | 42 |
| 170 | Reliability | Database Files on Network File Shares | https://www.BrentOzar.com/go/nas | 148 |
| 170 | Reliability | Database Files Stored in Azure | https://www.BrentOzar.com/go/azurefiles | 149 |
| 170 | Reliability | Database Snapshot Online | https://www.BrentOzar.com/go/snapshot | 77 |
| 170 | Reliability | Errors Logged Recently in the Default Trace | https://www.BrentOzar.com/go/defaulttrace | 150 |
| 170 | Reliability | Max File Size Set | https://www.BrentOzar.com/go/maxsize | 80 |
| 170 | Reliability | Remote Admin Connections Disabled | https://www.BrentOzar.com/go/dac | 100 |
| 200 | Backup | MSDB Backup History Not Purged | https://www.BrentOzar.com/go/history | 3 |
| 200 | Backup | MSDB Backup History Purged Too Frequently | https://www.BrentOzar.com/go/history | 186 |
| 200 | Informational | @@Servername not set | https://www.BrentOzar.com/go/servername | 70 |
| 200 | Informational | Agent Jobs Starting Simultaneously | https://www.BrentOzar.com/go/busyagent | 123 |
| 200 | Informational | Backup Compression Default Off | https://www.BrentOzar.com/go/backup | 116 |
| 200 | Informational | Cluster Node | https://www.BrentOzar.com/go/node | 53 |
| 200 | Informational | Collation different than tempdb | https://www.BrentOzar.com/go/collate | 76 |
| 200 | Informational | Database Encrypted | https://www.BrentOzar.com/go/tde | 21 |
| 200 | Informational | Date Correlation On | https://www.BrentOzar.com/go/corr | 20 |
| 200 | Informational | Linked Server Configured | https://www.BrentOzar.com/go/link | 49 |
| 200 | Informational | Replication In Use | https://www.BrentOzar.com/go/repl | 19 |
| 200 | Informational | Tables in the Master Database | https://www.BrentOzar.com/go/mastuser | 27 |
| 200 | Informational | Tables in the Model Database | https://www.BrentOzar.com/go/model | 29 |
| 200 | Informational | Tables in the MSDB Database | https://www.BrentOzar.com/go/msdbuser | 28 |
| 200 | Informational | TraceFlag On | https://www.BrentOzar.com/go/traceflags/ | 74 |
| 200 | Licensing | Enterprise Edition Features In Use | https://www.BrentOzar.com/go/ee | 33 |
| 200 | Licensing | Non-Production License | https://www.BrentOzar.com/go/licensing | 173 |
| 200 | Monitoring | Agent Jobs Without Failure Emails | https://www.BrentOzar.com/go/alerts | 94 |
| 200 | Monitoring | Alerts Configured without Follow Up | https://www.BrentOzar.com/go/alert | 59 |
| 200 | Monitoring | Alerts Disabled | https://www.BrentOzar.com/go/alerts/ | 98 |
| 200 | Monitoring | Alerts Without Event Descriptions | https://www.brentozar.com/go/alert | 219 |
| 200 | Monitoring | Extended Events Hyperextension | https://www.BrentOzar.com/go/xe | 176 |
| 200 | Monitoring | No Alerts for Corruption | https://www.BrentOzar.com/go/alert | 96 |
| 200 | Monitoring | No Alerts for Sev 19-25 | https://www.BrentOzar.com/go/alert | 61 |
| 200 | Monitoring | No failsafe operator configured | https://www.BrentOzar.com/go/failsafe | 73 |
| 200 | Monitoring | No Operators Configured/Enabled | https://www.BrentOzar.com/go/op | 31 |
| 200 | Monitoring | Not All Alerts Configured | https://www.BrentOzar.com/go/alert | 30 |
| 200 | Non-Active Server Config | Config Not Running at Set Value | https://www.BrentOzar.com/blitz/sp_configure/ | 81 |
| 200 | Non-Default Server Config | access check cache bucket count | https://www.BrentOzar.com/go/conf | 1001 |
| 200 | Non-Default Server Config | access check cache quota | https://www.BrentOzar.com/go/conf | 1002 |
| 200 | Non-Default Server Config | Ad Hoc Distributed Queries | https://www.BrentOzar.com/go/conf | 1003 |
| 200 | Non-Default Server Config | affinity I/O mask | https://www.BrentOzar.com/go/conf | 1004 |
| 200 | Non-Default Server Config | affinity mask | https://www.BrentOzar.com/go/conf | 1005 |
| 200 | Non-Default Server Config | affinity64 I/O mask | https://www.BrentOzar.com/go/conf | 1067 |
| 200 | Non-Default Server Config | affinity64 mask | https://www.BrentOzar.com/go/conf | 1066 |
| 200 | Non-Default Server Config | Agent XPs | https://www.BrentOzar.com/go/conf | 1071 |
| 200 | Non-Default Server Config | allow updates | https://www.BrentOzar.com/go/conf | 1007 |
| 200 | Non-Default Server Config | awe enabled | https://www.BrentOzar.com/go/conf | 1008 |
| 200 | Non-Default Server Config | backup checksum default | https://www.BrentOzar.com/go/conf | 1070 |
| 200 | Non-Default Server Config | backup compression default | https://www.BrentOzar.com/go/conf | 1073 |
| 200 | Non-Default Server Config | blocked process threshold | https://www.BrentOzar.com/go/conf | 1009 |
| 200 | Non-Default Server Config | c2 audit mode | https://www.BrentOzar.com/go/conf | 1010 |
| 200 | Non-Default Server Config | clr enabled | https://www.BrentOzar.com/go/conf | 1011 |
| 200 | Non-Default Server Config | common criteria compliance enabled | https://www.BrentOzar.com/go/conf | 1074 |
| 200 | Non-Default Server Config | contained database authentication | https://www.BrentOzar.com/go/conf | 1068 |
| 200 | Non-Default Server Config | cost threshold for parallelism | https://www.BrentOzar.com/go/conf | 1012 |
| 200 | Non-Default Server Config | cross db ownership chaining | https://www.BrentOzar.com/go/conf | 1013 |
| 200 | Non-Default Server Config | cursor threshold | https://www.BrentOzar.com/go/conf | 1014 |
| 200 | Non-Default Server Config | Database Mail XPs | https://www.BrentOzar.com/go/conf | 1072 |
| 200 | Non-Default Server Config | default full-text language | https://www.BrentOzar.com/go/conf | 1016 |
| 200 | Non-Default Server Config | default language | https://www.BrentOzar.com/go/conf | 1017 |
| 200 | Non-Default Server Config | default trace enabled | https://www.BrentOzar.com/go/conf | 1018 |
| 200 | Non-Default Server Config | disallow results from triggers | https://www.BrentOzar.com/go/conf | 1019 |
| 200 | Non-Default Server Config | EKM provider enabled | https://www.BrentOzar.com/go/conf | 1075 |
| 200 | Non-Default Server Config | filestream access level | https://www.BrentOzar.com/go/conf | 1076 |
| 200 | Non-Default Server Config | fill factor (%) | https://www.BrentOzar.com/go/conf | 1020 |
| 200 | Non-Default Server Config | ft crawl bandwidth (max) | https://www.BrentOzar.com/go/conf | 1021 |
| 200 | Non-Default Server Config | ft crawl bandwidth (min) | https://www.BrentOzar.com/go/conf | 1022 |
| 200 | Non-Default Server Config | ft notify bandwidth (max) | https://www.BrentOzar.com/go/conf | 1023 |
| 200 | Non-Default Server Config | ft notify bandwidth (min) | https://www.BrentOzar.com/go/conf | 1024 |
| 200 | Non-Default Server Config | in-doubt xact resolution | https://www.BrentOzar.com/go/conf | 1026 |
| 200 | Non-Default Server Config | index create memory (KB) | https://www.BrentOzar.com/go/conf | 1025 |
| 200 | Non-Default Server Config | lightweight pooling | https://www.BrentOzar.com/go/conf | 1027 |
| 200 | Non-Default Server Config | locks | https://www.BrentOzar.com/go/conf | 1028 |
| 200 | Non-Default Server Config | max degree of parallelism | https://www.BrentOzar.com/go/conf | 1029 |
| 200 | Non-Default Server Config | max full-text crawl range | https://www.BrentOzar.com/go/conf | 1030 |
| 200 | Non-Default Server Config | max server memory (MB) | https://www.BrentOzar.com/go/conf | 1031 |
| 200 | Non-Default Server Config | max text repl size (B) | https://www.BrentOzar.com/go/conf | 1032 |
| 200 | Non-Default Server Config | max worker threads | https://www.BrentOzar.com/go/conf | 1033 |
| 200 | Non-Default Server Config | media retention | https://www.BrentOzar.com/go/conf | 1034 |
| 200 | Non-Default Server Config | min memory per query (KB) | https://www.BrentOzar.com/go/conf | 1035 |
| 200 | Non-Default Server Config | min server memory (MB) | https://www.BrentOzar.com/go/conf | 1036 |
| 200 | Non-Default Server Config | nested triggers | https://www.BrentOzar.com/go/conf | 1037 |
| 200 | Non-Default Server Config | network packet size (B) | https://www.BrentOzar.com/go/conf | 1038 |
| 200 | Non-Default Server Config | Ole Automation Procedures | https://www.BrentOzar.com/go/conf | 1039 |
| 200 | Non-Default Server Config | open objects | https://www.BrentOzar.com/go/conf | 1040 |
| 200 | Non-Default Server Config | optimize for ad hoc workloads | https://www.BrentOzar.com/go/conf | 1041 |
| 200 | Non-Default Server Config | PH timeout (s) | https://www.BrentOzar.com/go/conf | 1042 |
| 200 | Non-Default Server Config | precompute rank | https://www.BrentOzar.com/go/conf | 1043 |
| 200 | Non-Default Server Config | priority boost | https://www.BrentOzar.com/go/conf | 1044 |
| 200 | Non-Default Server Config | query governor cost limit | https://www.BrentOzar.com/go/conf | 1045 |
| 200 | Non-Default Server Config | query wait (s) | https://www.BrentOzar.com/go/conf | 1046 |
| 200 | Non-Default Server Config | recovery interval (min) | https://www.BrentOzar.com/go/conf | 1047 |
| 200 | Non-Default Server Config | remote access | https://www.BrentOzar.com/go/conf | 1048 |
| 200 | Non-Default Server Config | remote admin connections | https://www.BrentOzar.com/go/conf | 1049 |
| 200 | Non-Default Server Config | remote login timeout (s) | https://www.BrentOzar.com/go/conf | 1069 |
| 200 | Non-Default Server Config | remote proc trans | https://www.BrentOzar.com/go/conf | 1050 |
| 200 | Non-Default Server Config | remote query timeout (s) | https://www.BrentOzar.com/go/conf | 1051 |
| 200 | Non-Default Server Config | Replication XPs | https://www.BrentOzar.com/go/conf | 1052 |
| 200 | Non-Default Server Config | RPC parameter data validation | https://www.BrentOzar.com/go/conf | 1053 |
| 200 | Non-Default Server Config | scan for startup procs | https://www.BrentOzar.com/go/conf | 1054 |
| 200 | Non-Default Server Config | server trigger recursion | https://www.BrentOzar.com/go/conf | 1055 |
| 200 | Non-Default Server Config | set working set size | https://www.BrentOzar.com/go/conf | 1056 |
| 200 | Non-Default Server Config | show advanced options | https://www.BrentOzar.com/go/conf | 1057 |
| 200 | Non-Default Server Config | SMO and DMO XPs | https://www.BrentOzar.com/go/conf | 1058 |
| 200 | Non-Default Server Config | SQL Mail XPs | https://www.BrentOzar.com/go/conf | 1059 |
| 200 | Non-Default Server Config | transform noise words | https://www.BrentOzar.com/go/conf | 1060 |
| 200 | Non-Default Server Config | two digit year cutoff | https://www.BrentOzar.com/go/conf | 1061 |
| 200 | Non-Default Server Config | user connections | https://www.BrentOzar.com/go/conf | 1062 |
| 200 | Non-Default Server Config | user options | https://www.BrentOzar.com/go/conf | 1063 |
| 200 | Non-Default Server Config | Web Assistant Procedures | https://www.BrentOzar.com/go/conf | 1064 |
| 200 | Non-Default Server Config | xp_cmdshell | https://www.BrentOzar.com/go/conf | 1065 |
| 200 | Performance | Buffer Pool Extensions Enabled | https://www.BrentOzar.com/go/bpe | 174 |
| 200 | Performance | Default Parallelism Settings | https://www.BrentOzar.com/go/cxpacket | 188 |
| 200 | Performance | In-Memory OLTP (Hekaton) In Use | https://www.BrentOzar.com/go/hekaton | 146 |
| 200 | Performance | Non-Dynamic Memory | https://www.BrentOzar.com/go/memory | 190 |
| 200 | Performance | Old Compatibility Level | https://www.BrentOzar.com/go/compatlevel | 62 |
| 200 | Performance | Query Store Disabled | https://www.BrentOzar.com/go/querystore | 163 |
| 200 | Performance | Snapshot Backups Occurring | https://www.BrentOzar.com/go/snaps | 178 |
| 200 | Performance | User-Created Statistics In Place | https://www.BrentOzar.com/go/userstats | 122 |
| 200 | Performance | SSAS/SSIS/SSRS Installed | https://www.BrentOzar.com/go/services | 224 |
| 200 | Reliability | Extended Stored Procedures in Master | https://www.BrentOzar.com/go/clr | 105 |
| 200 | Reliability | Resumable Index Operation Paused | https://www.BrentOzar.com/go/resumable | 225 |
| 200 | Surface Area | Endpoints Configured | https://www.BrentOzar.com/go/endpoints/ | 9 |
| 210 | Non-Default Database Config | ANSI NULL Default Enabled | https://www.BrentOzar.com/go/dbdefaults | 135 |
| 210 | Non-Default Database Config | Auto Create Stats Incremental Enabled | https://www.BrentOzar.com/go/dbdefaults | 134 |
| 210 | Non-Default Database Config | Change Data Capture Enabled | https://www.BrentOzar.com/go/dbdefaults | 140 |
| 210 | Non-Default Database Config | Containment Enabled | https://www.BrentOzar.com/go/dbdefaults | 141 |
| 210 | Non-Default Database Config | Delayed Durability Enabled | https://www.BrentOzar.com/go/dbdefaults | 143 |
| 210 | Non-Default Database Config | Forced Parameterization Enabled | https://www.BrentOzar.com/go/dbdefaults | 138 |
| 210 | Non-Default Database Config | Memory Optimized Enabled | https://www.BrentOzar.com/go/dbdefaults | 144 |
| 210 | Non-Default Database Config | Read Committed Snapshot Isolation Enabled | https://www.BrentOzar.com/go/dbdefaults | 133 |
| 210 | Non-Default Database Config | Recursive Triggers Enabled | https://www.BrentOzar.com/go/dbdefaults | 136 |
| 210 | Non-Default Database Config | Snapshot Isolation Enabled | https://www.BrentOzar.com/go/dbdefaults | 132 |
| 210 | Non-Default Database Config | Supplemental Logging Enabled | https://www.BrentOzar.com/go/dbdefaults | 131 |
| 210 | Non-Default Database Config | Target Recovery Time Changed | https://www.BrentOzar.com/go/dbdefaults | 142 |
| 210 | Non-Default Database Config | Trustworthy Enabled | https://www.BrentOzar.com/go/dbdefaults | 137 |
| 210 | Non-Default Database Config | Broker Enabled | https://www.BrentOzar.com/go/dbdefaults | 230 |
| 210 | Non-Default Database Config | Honor Broker Priority Enabled | https://www.BrentOzar.com/go/dbdefaults | 231 |
| 210 | Non-Default Database Scoped Config | MAXDOP | https://www.BrentOzar.com/go/dbscope | 194 |
| 210 | Non-Default Database Scoped Config | Legacy CE | https://www.BrentOzar.com/go/dbscope | 195 |
| 210 | Non-Default Database Scoped Config | Parameter Sniffing | https://www.BrentOzar.com/go/dbscope | 196 |
| 210 | Non-Default Database Scoped Config | Query Optimizer Hotfixes | https://www.BrentOzar.com/go/dbscope | 197 |
| 230 | Security | Control Server Permissions | https://www.BrentOzar.com/go/sa | 104 |
| 230 | Security | Database Owner <> SA | https://www.BrentOzar.com/go/owndb | 55 |
| 230 | Security | Database Owner is Unknown |  | 213 |
| 230 | Security | Elevated Permissions on a Database | https://www.BrentOzar.com/go/elevated | 86 |
| 230 | Security | Endpoints Owned by Users | https://www.BrentOzar.com/go/owners | 187 |
| 230 | Security | Jobs Owned By Users | https://www.BrentOzar.com/go/owners | 6 |
| 230 | Security | Security Admins | https://www.BrentOzar.com/go/sa | 5 |
| 230 | Security | Server Audits Running | https://www.BrentOzar.com/go/audits | 8 |
| 230 | Security | SQL Agent Job Runs at Startup | https://www.BrentOzar.com/go/startup | 57 |
| 230 | Security | Stored Procedure Runs at Startup | https://www.BrentOzar.com/go/startup | 7 |
| 230 | Security | Sysadmins | https://www.BrentOzar.com/go/sa | 4 |
| 230 | Security | Invalid Active Directory Accounts | | 2301|
| 240 | Wait Stats | No Significant Waits Detected | https://www.BrentOzar.com/go/waits | 153 |
| 240 | Wait Stats | Top Wait Stats | https://www.BrentOzar.com/go/waits | 152 |
| 240 | Wait Stats | Wait Stats Have Been Cleared | https://www.BrentOzar.com/go/waits | 185 |
| 250 | Informational | SQL Server Agent is running under an NT Service account | https://www.BrentOzar.com/go/setup | 170 |
| 250 | Informational | SQL Server is running under an NT Service account | https://www.BrentOzar.com/go/setup | 169 |
| 250 | Server Info | Agent is Currently Offline |  | 167 |
| 250 | Server Info | Azure Managed Instance | https://www.BrenOzar.com/go/azurevm | 222 |
| 250 | Server Info | Container | https://www.BrentOzar.com/go/virtual | 214 |
| 250 | Server Info | Data Size |  | 232 |
| 250 | Server Info | Default Trace Contents | https://www.BrentOzar.com/go/trace | 106 |
| 250 | Server Info | Drive Space |  | 92 |
| 250 | Server Info | Full-text Filter Daemon is Currently Offline |  | 168 |
| 250 | Server Info | Hardware |  | 84 |
| 250 | Server Info | Hardware - NUMA Config |  | 114 |
| 250 | Server Info | Instant File Initialization Enabled | https://www.BrentOzar.com/go/instant | 193 |
| 250 | Server Info | Locked Pages in Memory Enabled | https://www.BrentOzar.com/go/lpim | 166 |
| 250 | Server Info | Server Name | https://www.BrentOzar.com/go/servername | 130 |
| 250 | Server Info | Services |  | 83 |
| 250 | Server Info | SQL Server Last Restart |  | 88 |
| 250 | Server Info | Server Last Restart |  | 91 |
| 250 | Server Info | SQL Server Service |  | 85 |
| 250 | Server Info | Virtual Server | https://www.BrentOzar.com/go/virtual | 103 |
| 250 | Server Info | Windows Version |  | 172 |
| 250 | Server Info | Power Plan |  | 211 |
| 250 | Server Info | Stacked Instances | https://www.brentozar.com/go/babygotstacked/ | 212 |
| 253 | First Responder Kit | Version Check Failed | http://FirstResponderKit.org | 226 |
| 253 | First Responder Kit | Component Missing | http://FirstResponderKit.org | 227 |
| 253 | First Responder Kit | Component Outdated | http://FirstResponderKit.org | 228 |
| 254 | Rundate | (Current Date) |  | 156 |
| 255 | Thanks! | From Your Community Volunteers |  | -1 |
