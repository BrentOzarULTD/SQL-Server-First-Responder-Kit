# sp_BlitzIndex Checks by Priority

This table lists all checks ordered by priority. 

Before adding a new check, make sure to add a Github issue for it first, and have a group discussion about its priority, description, and findings URL.

If you want to change anything about a check - the priority, finding, URL, or ID - open a Github issue first. The relevant scripts have to be updated too.

CURRENT HIGH CHECKID: 124
If you want to add a new check, start at 125.

| Priority | FindingsGroup           | Finding                                                         | URL                                                              | CheckID |
| -------- | ----------------------- | --------------------------------------------------------------- | ---------------------------------------------------------------- | ------- |
| 10       | Over-Indexing           | Many NC Indexes on a Single Table                               | https://www.brentozar.com/go/IndexHoarder                        | 20      |
| 10       | Over-Indexing           | Unused NC Index with High Writes                                | https://www.brentozar.com/go/IndexHoarder                        | 22      |
| 10       | Resumable Indexing      | Resumable Index Operation Paused                                | https://www.BrentOzar.com/go/resumable                           | 122     |
| 10       | Resumable Indexing      | Resumable Index Operation Running                               | https://www.BrentOzar.com/go/resumable                           | 123     |
| 20       | Redundant Indexes       | Duplicate Keys                                                  | https://www.brentozar.com/go/duplicateindex                      | 1       |
| 30       | Redundant Indexes       | Approximate Duplicate Keys                                      | https://www.brentozar.com/go/duplicateindex                      | 2       |
| 40       | Index Suggestion        | High Value Missing Index                                        | https://www.brentozar.com/go/indexaphobia                        | 50      |
| 70       | Locking-Prone Indexes   | Total Lock Time with Long Average Waits                         | https://www.brentozar.com/go/aggressiveindexes                   | 11      |
| 70       | Locking-Prone Indexes   | Total Lock Time with Short Average Waits                        | https://www.brentozar.com/go/aggressiveindexes                   | 12      |
| 80       | Abnormal Design Pattern | Columnstore Indexes with Trace Flag 834                         | https://support.microsoft.com/en-us/kb/3210239                   | 72      |
| 80       | Abnormal Design Pattern | Identity Column Near End of Range                               | https://www.brentozar.com/go/AbnormalPsychology                  | 68      |
| 80       | Abnormal Design Pattern | Filter Columns Not In Index Definition                          | https://www.brentozar.com/go/IndexFeatures                       | 34      |
| 80       | Abnormal Design Pattern | History Table With NonClustered Index                           | https://sqlserverfast.com/blog/hugo/2023/09/an-update-on-merge/  | 124     |
| 90       | Statistics Warnings     | Low Sampling Rates                                              | https://www.brentozar.com/go/stats                               | 91      |
| 90       | Statistics Warnings     | Statistics Not Updated Recently                                 | https://www.brentozar.com/go/stats                               | 90      |
| 90       | Statistics Warnings     | Statistics with NO RECOMPUTE                                    | https://www.brentozar.com/go/stats                               | 92      |
| 100      | Over-Indexing           | NC index with High Writes:Reads                                 | https://www.brentozar.com/go/IndexHoarder                        | 48      |
| 100      | Indexes Worth Reviewing | Heap with a Nonclustered Primary Key                            | https://www.brentozar.com/go/SelfLoathing                        | 47      |
| 100      | Indexes Worth Reviewing | Heap with Forwarded Fetches                                     | https://www.brentozar.com/go/SelfLoathing                        | 43      |
| 100      | Indexes Worth Reviewing | Large Active Heap                                               | https://www.brentozar.com/go/SelfLoathing                        | 44      |
| 100      | Indexes Worth Reviewing | Low Fill Factor on Clustered Index                              | https://www.brentozar.com/go/SelfLoathing                        | 40      |
| 100      | Indexes Worth Reviewing | Low Fill Factor on Nonclustered Index                           | https://www.brentozar.com/go/SelfLoathing                        | 40      |
| 100      | Indexes Worth Reviewing | Medium Active Heap                                              | https://www.brentozar.com/go/SelfLoathing                        | 45      |
| 100      | Indexes Worth Reviewing | Small Active Heap                                               | https://www.brentozar.com/go/SelfLoathing                        | 46      |
| 100      | Forced Serialization    | Computed Column with Scalar UDF                                 | https://www.brentozar.com/go/serialudf                           | 99      |
| 100      | Forced Serialization    | Check Constraint with Scalar UDF                                | https://www.brentozar.com/go/computedscalar                      | 94      |
| 150      | Abnormal Design Pattern | Cascading Updates or Deletes                                    | https://www.brentozar.com/go/AbnormalPsychology                  | 71      |
| 150      | Abnormal Design Pattern | Unindexed Foreign Keys                                          | https://www.brentozar.com/go/AbnormalPsychology                  | 72      |
| 150      | Abnormal Design Pattern | Columnstore Index                                               | https://www.brentozar.com/go/AbnormalPsychology                  | 61      |
| 150      | Abnormal Design Pattern | Column Collation Does Not Match Database Collation              | https://www.brentozar.com/go/AbnormalPsychology                  | 69      |
| 150      | Abnormal Design Pattern | Compressed Index                                                | https://www.brentozar.com/go/AbnormalPsychology                  | 63      |
| 150      | Abnormal Design Pattern | In-Memory OLTP                                                  | https://www.brentozar.com/go/AbnormalPsychology                  | 73      |
| 150      | Abnormal Design Pattern | Non-Aligned Index on a Partitioned Table                        | https://www.brentozar.com/go/AbnormalPsychology                  | 65      |
| 150      | Abnormal Design Pattern | Partitioned Index                                               | https://www.brentozar.com/go/AbnormalPsychology                  | 64      |
| 150      | Abnormal Design Pattern | Spatial Index                                                   | https://www.brentozar.com/go/AbnormalPsychology                  | 62      |
| 150      | Abnormal Design Pattern | XML Index                                                       | https://www.brentozar.com/go/AbnormalPsychology                  | 60      |
| 150      | Over-Indexing           | Approximate: Wide Indexes (7 or More Columns)                   | https://www.brentozar.com/go/IndexHoarder                        | 23      |
| 150      | Over-Indexing           | More Than 5 Percent NC Indexes Are Unused                       | https://www.brentozar.com/go/IndexHoarder                        | 21      |
| 150      | Over-Indexing           | Non-Unique Clustered Index                                      | https://www.brentozar.com/go/IndexHoarder                        | 28      |
| 150      | Over-Indexing           | Unused NC Index with Low Writes                                 | https://www.brentozar.com/go/IndexHoarder                        | 29      |
| 150      | Over-Indexing           | Wide Clustered Index (>3 columns or >16 bytes)                  | https://www.brentozar.com/go/IndexHoarder                        | 24      |
| 150      | Indexes Worth Reviewing | Disabled Index                                                  | https://www.brentozar.com/go/SelfLoathing                        | 42      |
| 150      | Indexes Worth Reviewing | Hypothetical Index                                              | https://www.brentozar.com/go/SelfLoathing                        | 41      |
| 200      | Abnormal Design Pattern | Identity Column Using a Negative Seed or Increment Other Than 1 | https://www.brentozar.com/go/AbnormalPsychology                  | 74      |
| 200      | Abnormal Design Pattern | Recently Created Tables/Indexes (1 week)                        | https://www.brentozar.com/go/AbnormalPsychology                  | 66      |
| 200      | Abnormal Design Pattern | Recently Modified Tables/Indexes (2 days)                       | https://www.brentozar.com/go/AbnormalPsychology                  | 67      |
| 200      | Abnormal Design Pattern | Replicated Columns                                              | https://www.brentozar.com/go/AbnormalPsychology                  | 70      |
| 200      | Abnormal Design Pattern | Temporal Tables                                                 | https://www.brentozar.com/go/AbnormalPsychology                  | 110     |
| 200      | Repeated Calculations   | Computed Columns Not Persisted                                  | https://www.brentozar.com/go/serialudf                           | 100     |
| 200      | Statistics Warnings     | Statistics With Filters                                         | https://www.brentozar.com/go/stats                               | 93      |
| 200      | Over-Indexing           | High Ratio of Nulls                                             | https://www.brentozar.com/go/IndexHoarder                        | 25      |
| 200      | Over-Indexing           | High Ratio of Strings                                           | https://www.brentozar.com/go/IndexHoarder                        | 27      |
| 200      | Over-Indexing           | Wide Tables: 35+ cols or > 2000 non-LOB bytes                   | https://www.brentozar.com/go/IndexHoarder                        | 26      |
| 200      | Indexes Worth Reviewing | Heaps with Deletes                                              | https://www.brentozar.com/go/SelfLoathing                        | 49      |
| 200      | High Workloads          | Scan-a-lots (index-usage-stats)                                 | https://www.brentozar.com/go/Workaholics                         | 80      |
| 200      | High Workloads          | Top Recent Accesses (index-op-stats)                            | https://www.brentozar.com/go/Workaholics                         | 81      |
| 250      | Omitted Index Features  | Few Indexes Use Includes                                        | https://www.brentozar.com/go/IndexFeatures                       | 31      |
| 250      | Omitted Index Features  | No Filtered Indexes or Indexed Views                            | https://www.brentozar.com/go/IndexFeatures                       | 32      |
| 250      | Omitted Index Features  | No Indexes Use Includes                                         | https://www.brentozar.com/go/IndexFeatures                       | 30      |
| 250      | Omitted Index Features  | Potential Filtered Index (Based on Column Name)                 | https://www.brentozar.com/go/IndexFeatures                       | 33      |
| 250      | Specialized Indexes     | Optimized For Sequential Keys                                   |                                                                  | 121     |
