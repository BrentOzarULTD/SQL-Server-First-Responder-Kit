# sp_Blitz Checks by Priority

This table lists all checks ordered by priority. 

Before adding a new check, make sure to add a Github issue for it first, and have a group discussion about its priority, description, and findings URL.

If you want to change anything about a check - the priority, finding, URL, or ID - open a Github issue first. The relevant scripts have to be updated too.

| Priority | FindingsGroup | Finding | URL | CheckID |
|----------|-----------------------------|---------------------------------------------------------|------------------------------------------------------------------------|----------|
| 0 | Outdated sp_Blitz | sp_Blitz is Over 6 Months Old | http://BrentOzar.com/blitz/ | 155 |
| 1 | Backup | Backing Up to Same Drive Where Databases Reside | http://BrentOzar.com/go/backup | 93 |
| 1 | Backup | Backups Not Performed Recently | http://BrentOzar.com/go/nobak | 1 |
| 1 | Backup | Full Recovery Mode w/o Log Backups | http://BrentOzar.com/go/biglogs | 2 |
| 1 | Backup | TDE Certificate Not Backed Up Recently | http://BrentOzar.com/go/tde | 119 |
| 1 | Corruption | Database Corruption Detected | http://BrentOzar.com/go/repair | 34 |
| 1 | Corruption | Database Corruption Detected | http://BrentOzar.com/go/repair | 89 |
| 1 | Corruption | Database Corruption Detected | http://BrentOzar.com/go/repair | 90 |
| 1 | Performance | Memory Dangerously Low | http://BrentOzar.com/go/max | 51 |
| 1 | Performance | Memory Dangerously Low in NUMA Nodes | http://BrentOzar.com/go/max | 159 |
| 1 | Reliability | Last good DBCC CHECKDB over 2 weeks old | http://BrentOzar.com/go/checkdb | 68 |
| 5 | Monitoring | Disabled Internal Monitoring Features | https://msdn.microsoft.com/en-us/library/ms190737.aspx | 177 |
| 5 | Reliability | Dangerous Third Party Modules | https://support.microsoft.com/en-us/kb/2033238 | 179 |
| 5 | Reliability | Priority Boost Enabled | http://BrentOzar.com/go/priorityboost | 126 |
| 10 | Performance | 32-bit SQL Server Installed | http://BrentOzar.com/go/32bit | 154 |
| 10 | Performance | Auto-Close Enabled | http://BrentOzar.com/go/autoclose | 12 |
| 10 | Performance | Auto-Shrink Enabled | http://BrentOzar.com/go/autoshrink | 13 |
| 10 | Performance | CPU Schedulers Offline | http://BrentOzar.com/go/schedulers | 101 |
| 10 | Performance | High Memory Use for In-Memory OLTP (Hekaton) | http://BrentOzar.com/go/hekaton | 145 |
| 10 | Performance | Memory Nodes Offline | http://BrentOzar.com/go/schedulers | 110 |
| 10 | Performance | Plan Cache Erased Recently | http://BrentOzar.com/askbrent/plan-cache-erased-recently/ | 125 |
| 10 | Performance | Query Store Disabled | http://BrentOzar.com/go/querystore | 163 |
| 20 | Reliability | Dangerous Build of SQL Server (Corruption) | http://sqlperformance.com/2014/06/sql-indexes/hotfix-sql-2012-rebuilds | 129 |
| 20 | Reliability | Dangerous Build of SQL Server (Security) | https://technet.microsoft.com/en-us/library/security/MS14-044 | 157 |
| 20 | Reliability | Databases in Unusual States | http://BrentOzar.com/go/repair | 102 |
| 20 | Reliability | Memory Dumps Have Occurred | http://BrentOzar.com/go/dump | 171 |
| 20 | Reliability | No Failover Cluster Nodes Available | http://BrentOzar.com/go/node | 184 |
| 20 | Reliability | Plan Guides Failing | http://BrentOzar.com/go/guides | 164 |
| 20 | Reliability | Query Store Cleanup Disabled | http://BrentOzar.com/go/cleanup | 182 |
| 20 | Reliability | Unsupported Build of SQL Server | http://BrentOzar.com/go/unsupported | 128 |
| 20 | Reliability | User Databases on C Drive | http://BrentOzar.com/go/cdrive | 26 |
| 20 | Reliability | TempDB on C Drive | http://BrentOzar.com/go/cdrive | 25 |
| 50 | Performance | Instant File Initialization Not Enabled | http://BrentOzar.com/go/instant | 192 |
| 50 | Performance | Log File Growths Slow | http://BrentOzar.com/go/filegrowth | 151 |
| 50 | Performance | Poison Wait Detected: CMEMTHREAD & NUMA | http://BrentOzar.com/go/poison | 162 |
| 50 | Performance | Poison Wait Detected: RESOURCE_SEMAPHORE | http://BrentOzar.com/go/poison | 108 |
| 50 | Performance | Poison Wait Detected: RESOURCE_SEMAPHORE_QUERY_COMPILE | http://BrentOzar.com/go/poison | 109 |
| 50 | Performance | Poison Wait Detected: Serializable Locking | http://BrentOzar.com/go/serializable | 121 |
| 50 | Performance | Poison Wait Detected: THREADPOOL | http://BrentOzar.com/go/poison | 107 |
| 50 | Performance | Too Much Free Memory | http://BrentOzar.com/go/freememory | 165 |
| 50 | Reliability | Database Snapshot Online | http://BrentOzar.com/go/snapshot | 77 |
| 50 | Reliability | Errors Logged Recently in the Default Trace | http://BrentOzar.com/go/defaulttrace | 150 |
| 50 | Reliability | Full Text Indexes Not Updating | http://BrentOzar.com/go/fulltext | 113 |
| 50 | Reliability | Page Verification Not Optimal | http://BrentOzar.com/go/torn | 14 |
| 50 | Reliability | Possibly Broken Log Shipping | http://BrentOzar.com/go/shipping | 111 |
| 50 | Reliability | Remote Admin Connections Disabled | http://BrentOzar.com/go/dac | 100 |
| 50 | Reliability | TempDB File Error | http://BrentOzar.com/go/tempdboops | 191 |
| 50 | Reliability | Transaction Log Larger than Data File | http://BrentOzar.com/go/biglog | 75 |
| 100 | In-Memory OLTP (Hekaton) | Transaction Errors | http://BrentOzar.com/go/hekaton | 147 |
| 100 | Features | Missing Features | http://BrentOzar.com/ | 189 |
| 100 | Performance | Change Tracking Enabled | http://BrentOzar.com/go/tracking | 112 |
| 100 | Performance | Fill Factor Changed | http://brentozar.com/go/fillfactor | 60 |
| 100 | Performance | High Number of Cached Plans | http://BrentOzar.com/go/planlimits | 161 |
| 100 | Performance | Indexes Disabled | http://BrentOzar.com/go/ixoff | 47 |
| 100 | Performance | Many Plans for One Query | http://BrentOzar.com/go/parameterization | 160 |
| 100 | Performance | Max Memory Set Too High | http://BrentOzar.com/go/max | 50 |
| 100 | Performance | Memory Pressure Affecting Queries | http://BrentOzar.com/go/grants | 117 |
| 100 | Performance | Partitioned database with non-aligned indexes | http://BrentOzar.com/go/aligned | 72 |
| 100 | Performance | Repetitive Maintenance Tasks | https://ola.hallengren.com | 181 |
| 100 | Performance | Resource Governor Enabled | http://BrentOzar.com/go/rg | 10 |
| 100 | Performance | Server Triggers Enabled | http://BrentOzar.com/go/logontriggers/ | 11 |
| 100 | Performance | Shrink Database Job | http://BrentOzar.com/go/autoshrink | 79 |
| 100 | Performance | Shrink Database Step In Maintenance Plan | http://BrentOzar.com/go/autoshrink | 180 |
| 100 | Performance | Single-Use Plans in Procedure Cache | http://BrentOzar.com/go/single | 35 |
| 100 | Performance | Stored Procedure WITH RECOMPILE | http://BrentOzar.com/go/recompile | 78 |
| 100 | Performance | Unusual SQL Server Edition | http://BrentOzar.com/go/workgroup | 97 |
| 110 | Performance | Active Tables Without Clustered Indexes | http://BrentOzar.com/go/heaps | 38 |
| 110 | Performance | Auto-Create Stats Disabled | http://BrentOzar.com/go/acs | 15 |
| 110 | Performance | Auto-Update Stats Disabled | http://BrentOzar.com/go/aus | 16 |
| 110 | Performance | Infinite merge replication metadata retention period | http://BrentOzar.com/go/merge | 99 |
| 110 | Performance | Parallelism Rocket Surgery | http://BrentOzar.com/go/makeparallel | 115 |
| 110 | Performance | Plan Guides Enabled | http://BrentOzar.com/go/guides | 95 |
| 120 | Query Plans | Cursor | http://BrentOzar.com/go/cursor | 66 |
| 120 | Query Plans | Implicit Conversion | http://BrentOzar.com/go/implicit | 63 |
| 120 | Query Plans | Implicit Conversion Affecting Cardinality | http://BrentOzar.com/go/implicit | 64 |
| 120 | Query Plans | Missing Index | http://BrentOzar.com/go/missingindex | 65 |
| 120 | Query Plans | RID or Key Lookups | http://BrentOzar.com/go/lookup | 118 |
| 120 | Query Plans | Scalar UDFs | http://BrentOzar.com/go/functions | 67 |
| 150 | Performance | Check Constraint Not Trusted | http://BrentOzar.com/go/trust | 56 |
| 150 | Performance | Deadlocks Happening Daily | http://BrentOzar.com/go/deadlocks | 124 |
| 150 | Performance | Forced Parameterization On | http://BrentOzar.com/go/forced | 18 |
| 150 | Performance | Foreign Keys Not Trusted | http://BrentOzar.com/go/trust | 48 |
| 150 | Performance | Inactive Tables Without Clustered Indexes | http://BrentOzar.com/go/heaps | 39 |
| 150 | Performance | Leftover Fake Indexes From Wizards | http://BrentOzar.com/go/hypo | 46 |
| 150 | Performance | Queries Forcing Join Hints | http://BrentOzar.com/go/hints | 45 |
| 150 | Performance | Queries Forcing Order Hints | http://BrentOzar.com/go/hints | 44 |
| 150 | Performance | Slow Storage Reads on Drive <DRIVELETTER> | http://BrentOzar.com/go/slow | 36 |
| 150 | Performance | Slow Storage Writes on Drive <DRIVELETTER> | http://BrentOzar.com/go/slow | 37 |
| 150 | Performance | Stats Updated Asynchronously | http://BrentOzar.com/go/asyncstats | 17 |
| 150 | Performance | Triggers on Tables | http://BrentOzar.com/go/trig | 32 |
| 170 | File Configuration | File growth set to 1MB | http://BrentOzar.com/go/percentgrowth | 158 |
| 170 | File Configuration | File growth set to percent | http://brentozar.com/go/percentgrowth | 82 |
| 170 | File Configuration | High VLF Count | http://BrentOzar.com/go/vlf | 69 |
| 170 | File Configuration | Multiple Log Files on One Drive | http://BrentOzar.com/go/manylogs | 41 |
| 170 | File Configuration | System Database on C Drive | http://BrentOzar.com/go/drivec | 24 |
| 170 | File Configuration | TempDB Has >16 Data Files | http://BrentOzar.com/go/tempdb | 175 |
| 170 | File Configuration | TempDB Only Has 1 Data File | http://BrentOzar.com/go/tempdb | 40 |
| 170 | File Configuration | TempDB Unevenly Sized Data Files | http://BrentOzar.com/go/tempdb | 183 |
| 170 | File Configuration | Uneven File Growth Settings in One Filegroup | http://BrentOzar.com/go/grow | 42 |
| 170 | Reliability | Database Files on Network File Shares | http://BrentOzar.com/go/nas | 148 |
| 170 | Reliability | Database Files Stored in Azure | http://BrentOzar.com/go/azurefiles | 149 |
| 170 | Reliability | Max File Size Set | http://BrentOzar.com/go/maxsize | 80 |
| 200 | Backup | Backing Up Unneeded Database | http://BrentOzar.com/go/reportservertempdb | 127 |
| 200 | Backup | MSDB Backup History Not Purged | http://BrentOzar.com/go/history | 3 |
| 200 | Backup | MSDB Backup History Purged Too Frequently | http://BrentOzar.com/go/history | 186 |
| 200 | Informational | @@Servername not set | http://BrentOzar.com/go/servername | 70 |
| 200 | Informational | Agent Jobs Starting Simultaneously | http://BrentOzar.com/go/busyagent | 123 |
| 200 | Informational | Backup Compression Default Off | http://BrentOzar.com/go/backup | 116 |
| 200 | Informational | Cluster Node | http://BrentOzar.com/go/node | 53 |
| 200 | Informational | Collation different than tempdb | http://BrentOzar.com/go/collate | 76 |
| 200 | Informational | Database Collation Mismatch | http://BrentOzar.com/go/collate | 58 |
| 200 | Informational | Database Encrypted | http://BrentOzar.com/go/tde | 21 |
| 200 | Informational | Date Correlation On | http://BrentOzar.com/go/corr | 20 |
| 200 | Informational | Linked Server Configured | http://BrentOzar.com/go/link | 49 |
| 200 | Informational | Replication In Use | http://BrentOzar.com/go/repl | 19 |
| 200 | Informational | Tables in the Master Database | http://BrentOzar.com/go/mastuser | 27 |
| 200 | Informational | Tables in the Model Database | http://BrentOzar.com/go/model | 29 |
| 200 | Informational | Tables in the MSDB Database | http://BrentOzar.com/go/msdbuser | 28 |
| 200 | Informational | TraceFlag On | http://www.BrentOzar.com/go/traceflags/ | 74 |
| 200 | Licensing | Enterprise Edition Features In Use | http://BrentOzar.com/go/ee | 33 |
| 200 | Licensing | Non-Production License | http://BrentOzar.com/go/licensing | 173 |
| 200 | Monitoring | Agent Jobs Without Failure Emails | http://BrentOzar.com/go/alerts | 94 |
| 200 | Monitoring | Alerts Configured without Follow Up | http://BrentOzar.com/go/alert | 59 |
| 200 | Monitoring | Alerts Disabled | http://www.BrentOzar.com/go/alerts/ | 98 |
| 200 | Monitoring | Extended Events Hyperextension | http://BrentOzar.com/go/xe | 176 |
| 200 | Monitoring | No Alerts for Corruption | http://BrentOzar.com/go/alert | 96 |
| 200 | Monitoring | No Alerts for Sev 19-25 | http://BrentOzar.com/go/alert | 61 |
| 200 | Monitoring | No failsafe operator configured | http://BrentOzar.com/go/failsafe | 73 |
| 200 | Monitoring | No Operators Configured/Enabled | http://BrentOzar.com/go/op | 31 |
| 200 | Monitoring | Not All Alerts Configured | http://BrentOzar.com/go/alert | 30 |
| 200 | Non-Active Server Config | Config Not Running at Set Value | http://www.BrentOzar.com/blitz/sp_configure/ | 81 |
| 200 | Non-Default Server Config | access check cache bucket count | http://BrentOzar.com/go/conf | 1001 |
| 200 | Non-Default Server Config | access check cache quota | http://BrentOzar.com/go/conf | 1002 |
| 200 | Non-Default Server Config | Ad Hoc Distributed Queries | http://BrentOzar.com/go/conf | 1003 |
| 200 | Non-Default Server Config | affinity I/O mask | http://BrentOzar.com/go/conf | 1004 |
| 200 | Non-Default Server Config | affinity mask | http://BrentOzar.com/go/conf | 1005 |
| 200 | Non-Default Server Config | affinity64 I/O mask | http://BrentOzar.com/go/conf | 1067 |
| 200 | Non-Default Server Config | affinity64 mask | http://BrentOzar.com/go/conf | 1066 |
| 200 | Non-Default Server Config | Agent XPs | http://BrentOzar.com/go/conf | 1071 |
| 200 | Non-Default Server Config | allow updates | http://BrentOzar.com/go/conf | 1007 |
| 200 | Non-Default Server Config | awe enabled | http://BrentOzar.com/go/conf | 1008 |
| 200 | Non-Default Server Config | backup checksum default | http://BrentOzar.com/go/conf | 1070 |
| 200 | Non-Default Server Config | backup compression default | http://BrentOzar.com/go/conf | 1073 |
| 200 | Non-Default Server Config | blocked process threshold | http://BrentOzar.com/go/conf | 1009 |
| 200 | Non-Default Server Config | c2 audit mode | http://BrentOzar.com/go/conf | 1010 |
| 200 | Non-Default Server Config | clr enabled | http://BrentOzar.com/go/conf | 1011 |
| 200 | Non-Default Server Config | common criteria compliance enabled | http://BrentOzar.com/go/conf | 1074 |
| 200 | Non-Default Server Config | contained database authentication | http://BrentOzar.com/go/conf | 1068 |
| 200 | Non-Default Server Config | cost threshold for parallelism | http://BrentOzar.com/go/conf | 1012 |
| 200 | Non-Default Server Config | cross db ownership chaining | http://BrentOzar.com/go/conf | 1013 |
| 200 | Non-Default Server Config | cursor threshold | http://BrentOzar.com/go/conf | 1014 |
| 200 | Non-Default Server Config | Database Mail XPs | http://BrentOzar.com/go/conf | 1072 |
| 200 | Non-Default Server Config | default full-text language | http://BrentOzar.com/go/conf | 1016 |
| 200 | Non-Default Server Config | default language | http://BrentOzar.com/go/conf | 1017 |
| 200 | Non-Default Server Config | default trace enabled | http://BrentOzar.com/go/conf | 1018 |
| 200 | Non-Default Server Config | disallow results from triggers | http://BrentOzar.com/go/conf | 1019 |
| 200 | Non-Default Server Config | EKM provider enabled | http://BrentOzar.com/go/conf | 1075 |
| 200 | Non-Default Server Config | filestream access level | http://BrentOzar.com/go/conf | 1076 |
| 200 | Non-Default Server Config | fill factor (%) | http://BrentOzar.com/go/conf | 1020 |
| 200 | Non-Default Server Config | ft crawl bandwidth (max) | http://BrentOzar.com/go/conf | 1021 |
| 200 | Non-Default Server Config | ft crawl bandwidth (min) | http://BrentOzar.com/go/conf | 1022 |
| 200 | Non-Default Server Config | ft notify bandwidth (max) | http://BrentOzar.com/go/conf | 1023 |
| 200 | Non-Default Server Config | ft notify bandwidth (min) | http://BrentOzar.com/go/conf | 1024 |
| 200 | Non-Default Server Config | in-doubt xact resolution | http://BrentOzar.com/go/conf | 1026 |
| 200 | Non-Default Server Config | index create memory (KB) | http://BrentOzar.com/go/conf | 1025 |
| 200 | Non-Default Server Config | lightweight pooling | http://BrentOzar.com/go/conf | 1027 |
| 200 | Non-Default Server Config | locks | http://BrentOzar.com/go/conf | 1028 |
| 200 | Non-Default Server Config | max degree of parallelism | http://BrentOzar.com/go/conf | 1029 |
| 200 | Non-Default Server Config | max full-text crawl range | http://BrentOzar.com/go/conf | 1030 |
| 200 | Non-Default Server Config | max server memory (MB) | http://BrentOzar.com/go/conf | 1031 |
| 200 | Non-Default Server Config | max text repl size (B) | http://BrentOzar.com/go/conf | 1032 |
| 200 | Non-Default Server Config | max worker threads | http://BrentOzar.com/go/conf | 1033 |
| 200 | Non-Default Server Config | media retention | http://BrentOzar.com/go/conf | 1034 |
| 200 | Non-Default Server Config | min memory per query (KB) | http://BrentOzar.com/go/conf | 1035 |
| 200 | Non-Default Server Config | min server memory (MB) | http://BrentOzar.com/go/conf | 1036 |
| 200 | Non-Default Server Config | nested triggers | http://BrentOzar.com/go/conf | 1037 |
| 200 | Non-Default Server Config | network packet size (B) | http://BrentOzar.com/go/conf | 1038 |
| 200 | Non-Default Server Config | Ole Automation Procedures | http://BrentOzar.com/go/conf | 1039 |
| 200 | Non-Default Server Config | open objects | http://BrentOzar.com/go/conf | 1040 |
| 200 | Non-Default Server Config | optimize for ad hoc workloads | http://BrentOzar.com/go/conf | 1041 |
| 200 | Non-Default Server Config | PH timeout (s) | http://BrentOzar.com/go/conf | 1042 |
| 200 | Non-Default Server Config | precompute rank | http://BrentOzar.com/go/conf | 1043 |
| 200 | Non-Default Server Config | priority boost | http://BrentOzar.com/go/conf | 1044 |
| 200 | Non-Default Server Config | query governor cost limit | http://BrentOzar.com/go/conf | 1045 |
| 200 | Non-Default Server Config | query wait (s) | http://BrentOzar.com/go/conf | 1046 |
| 200 | Non-Default Server Config | recovery interval (min) | http://BrentOzar.com/go/conf | 1047 |
| 200 | Non-Default Server Config | remote access | http://BrentOzar.com/go/conf | 1048 |
| 200 | Non-Default Server Config | remote admin connections | http://BrentOzar.com/go/conf | 1049 |
| 200 | Non-Default Server Config | remote login timeout (s) | http://BrentOzar.com/go/conf | 1069 |
| 200 | Non-Default Server Config | remote proc trans | http://BrentOzar.com/go/conf | 1050 |
| 200 | Non-Default Server Config | remote query timeout (s) | http://BrentOzar.com/go/conf | 1051 |
| 200 | Non-Default Server Config | Replication XPs | http://BrentOzar.com/go/conf | 1052 |
| 200 | Non-Default Server Config | RPC parameter data validation | http://BrentOzar.com/go/conf | 1053 |
| 200 | Non-Default Server Config | scan for startup procs | http://BrentOzar.com/go/conf | 1054 |
| 200 | Non-Default Server Config | server trigger recursion | http://BrentOzar.com/go/conf | 1055 |
| 200 | Non-Default Server Config | set working set size | http://BrentOzar.com/go/conf | 1056 |
| 200 | Non-Default Server Config | show advanced options | http://BrentOzar.com/go/conf | 1057 |
| 200 | Non-Default Server Config | SMO and DMO XPs | http://BrentOzar.com/go/conf | 1058 |
| 200 | Non-Default Server Config | SQL Mail XPs | http://BrentOzar.com/go/conf | 1059 |
| 200 | Non-Default Server Config | transform noise words | http://BrentOzar.com/go/conf | 1060 |
| 200 | Non-Default Server Config | two digit year cutoff | http://BrentOzar.com/go/conf | 1061 |
| 200 | Non-Default Server Config | user connections | http://BrentOzar.com/go/conf | 1062 |
| 200 | Non-Default Server Config | user options | http://BrentOzar.com/go/conf | 1063 |
| 200 | Non-Default Server Config | Web Assistant Procedures | http://BrentOzar.com/go/conf | 1064 |
| 200 | Non-Default Server Config | xp_cmdshell | http://BrentOzar.com/go/conf | 1065 |
| 200 | Performance | Buffer Pool Extensions Enabled | http://BrentOzar.com/go/bpe | 174 |
| 200 | Performance | Default Parallelism Settings | http://BrentOzar.com/go/cxpacket | 188 |
| 200 | Performance | In-Memory OLTP (Hekaton) In Use | http://BrentOzar.com/go/hekaton | 146 |
| 200 | Performance | Old Compatibility Level | http://BrentOzar.com/go/compatlevel | 62 |
| 200 | Performance | Snapshot Backups Occurring | http://BrentOzar.com/go/snaps | 178 |
| 200 | Performance | User-Created Statistics In Place | http://BrentOzar.com/go/userstats | 122 |
| 200 | Performance | Non-Dynamic Memory | http://BrentOzar.com/go/memory | 190 |
| 200 | Reliability | Extended Stored Procedures in Master | http://BrentOzar.com/go/clr | 105 |
| 200 | Surface Area | Endpoints Configured | http://BrentOzar.com/go/endpoints/ | 9 |
| 210 | Non-Default Database Config | ANSI NULL Default Enabled | http://BrentOzar.com/go/dbdefaults | 135 |
| 210 | Non-Default Database Config | Auto Create Stats Incremental Enabled | http://BrentOzar.com/go/dbdefaults | 134 |
| 210 | Non-Default Database Config | Change Data Capture Enabled | http://BrentOzar.com/go/dbdefaults | 140 |
| 210 | Non-Default Database Config | Containment Enabled | http://BrentOzar.com/go/dbdefaults | 141 |
| 210 | Non-Default Database Config | Delayed Durability Enabled | http://BrentOzar.com/go/dbdefaults | 143 |
| 210 | Non-Default Database Config | Forced Parameterization Enabled | http://BrentOzar.com/go/dbdefaults | 138 |
| 210 | Non-Default Database Config | Memory Optimized Enabled | http://BrentOzar.com/go/dbdefaults | 144 |
| 210 | Non-Default Database Config | Query Store Enabled | http://BrentOzar.com/go/dbdefaults | 139 |
| 210 | Non-Default Database Config | Read Committed Snapshot Isolation Enabled | http://BrentOzar.com/go/dbdefaults | 133 |
| 210 | Non-Default Database Config | Recursive Triggers Enabled | http://BrentOzar.com/go/dbdefaults | 136 |
| 210 | Non-Default Database Config | Snapshot Isolation Enabled | http://BrentOzar.com/go/dbdefaults | 132 |
| 210 | Non-Default Database Config | Supplemental Logging Enabled | http://BrentOzar.com/go/dbdefaults | 131 |
| 210 | Non-Default Database Config | Target Recovery Time Changed | http://BrentOzar.com/go/dbdefaults | 142 |
| 210 | Non-Default Database Config | Trustworthy Enabled | http://BrentOzar.com/go/dbdefaults | 137 |
| 210 | Non-Default Database Scoped Config | MAXDOP | http://BrentOzar.com/go/dbscope | 194 |
| 210 | Non-Default Database Scoped Config | Legacy CE | http://BrentOzar.com/go/dbscope | 195 |
| 210 | Non-Default Database Scoped Config | Parameter Sniffing | http://BrentOzar.com/go/dbscope | 196 |
| 210 | Non-Default Database Scoped Config | Query Optimizer Hotfixes | http://BrentOzar.com/go/dbscope | 197 |
| 230 | Security | Control Server Permissions | http://BrentOzar.com/go/sa | 104 |
| 230 | Security | Database Owner <> SA | http://BrentOzar.com/go/owndb | 55 |
| 230 | Security | Elevated Permissions on a Database | http://BrentOzar.com/go/elevated | 86 |
| 230 | Security | Endpoints Owned by Users | http://BrentOzar.com/go/owners | 187 |
| 230 | Security | Jobs Owned By Users | http://BrentOzar.com/go/owners | 6 |
| 230 | Security | Security Admins | http://BrentOzar.com/go/sa | 5 |
| 230 | Security | Server Audits Running | http://BrentOzar.com/go/audits | 8 |
| 230 | Security | SQL Agent Job Runs at Startup | http://BrentOzar.com/go/startup | 57 |
| 230 | Security | Stored Procedure Runs at Startup | http://BrentOzar.com/go/startup | 7 |
| 230 | Security | Sysadmins | http://BrentOzar.com/go/sa | 4 |
| 240 | Wait Stats | No Significant Waits Detected | http://BrentOzar.com/go/waits | 153 |
| 240 | Wait Stats | Top Wait Stats | http://BrentOzar.com/go/waits | 152 |
| 240 | Wait Stats | Wait Stats Have Been Cleared | http://BrentOzar.com/go/waits | 185 |
| 250 | Informational | SQL Server Agent is running under an NT Service account | http://BrentOzar.com/go/setup | 170 |
| 250 | Informational | SQL Server is running under an NT Service account | http://BrentOzar.com/go/setup | 169 |
| 250 | Server Info | Agent is Currently Offline |  | 167 |
| 250 | Server Info | Default Trace Contents | http://BrentOzar.com/go/trace | 106 |
| 250 | Server Info | Drive Space |  | 92 |
| 250 | Server Info | Full-text Filter Daemon is Currently Offline |  | 168 |
| 250 | Server Info | Hardware |  | 84 |
| 250 | Server Info | Hardware - NUMA Config |  | 114 |
| 250 | Server Info | Instant File Initialization Enabled | http://BrentOzar.com/go/instant | 193 |
| 250 | Server Info | Locked Pages in Memory Enabled | http://BrentOzar.com/go/lpim | 166 |
| 250 | Server Info | Server Name | http://BrentOzar.com/go/servername | 130 |
| 250 | Server Info | Services |  | 83 |
| 250 | Server Info | SQL Server Last Restart |  | 88 |
| 250 | Server Info | Server Last Restart |  | 91 |
| 250 | Server Info | SQL Server Service |  | 85 |
| 250 | Server Info | Virtual Server | http://BrentOzar.com/go/virtual | 103 |
| 250 | Server Info | Windows Version |  | 172 |
| 254 | Rundate | (Current Date) |  | 156 |
