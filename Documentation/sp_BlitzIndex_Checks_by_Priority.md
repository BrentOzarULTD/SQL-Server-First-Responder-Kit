# sp_BlitzIndex Checks by Priority

This table lists all checks ordered by priority. 

Before adding a new check, make sure to add a Github issue for it first, and have a group discussion about its priority, description, and findings URL.

If you want to change anything about a check - the priority, finding, URL, or ID - open a Github issue first. The relevant scripts have to be updated too.

CURRENT HIGH CHECKID: 121
If you want to add a new check, start at 122.

| Priority | FindingsGroup | Finding | URL | CheckID |
|----------|---------------------------------|---------------------------------------|-------------------------------------------------|----------|
| 10 | Index Hoarder | Many NC Indexes on a Single Table | https://www.brentozar.com/go/IndexHoarder | 20 |
| 10 | Index Hoarder | Unused NC Index with High Writes | https://www.brentozar.com/go/IndexHoarder | 22 |
| 20 | Multiple Index Personalities | Duplicate Keys | https://www.brentozar.com/go/duplicateindex | 1 |
| 30 | Multiple Index Personalities | Borderline Duplicate Keys | https://www.brentozar.com/go/duplicateindex | 2 |
| 40 | Indexaphobia | High Value Missing Index | https://www.brentozar.com/go/indexaphobia | 50 |
| 70 | Aggressive Indexes | Total Lock Time with Long Average Waits | https://www.brentozar.com/go/aggressiveindexes | 11 |
| 70 | Aggressive Indexes | Total Lock Time with Short Average Waits | https://www.brentozar.com/go/aggressiveindexes | 12 |
| 80 | Abnormal Psychology | Columnstore Indexes with Trace Flag 834 | https://support.microsoft.com/en-us/kb/3210239 | 72 |
| 80 | Abnormal Psychology | Identity Column Near End of Range | https://www.brentozar.com/go/AbnormalPsychology | 68 |
| 80 | Abnormal Psychology | Filter Columns Not In Index Definition | https://www.brentozar.com/go/IndexFeatures | 34 |
| 90 | Functioning Statistaholics | Low Sampling Rates | https://www.brentozar.com/go/stats | 91 |
| 90 | Functioning Statistaholics | Statistics Not Updated Recently | https://www.brentozar.com/go/stats | 90 |
| 90 | Functioning Statistaholics | Statistics with NO RECOMPUTE | https://www.brentozar.com/go/stats | 92 |
| 100 | Index Hoarder | NC index with High Writes:Reads | https://www.brentozar.com/go/IndexHoarder | 48 |
| 100 | Self Loathing Indexes | Heap with a Nonclustered Primary Key | https://www.brentozar.com/go/SelfLoathing | 47 |
| 100 | Self Loathing Indexes | Heap with Forwarded Fetches | https://www.brentozar.com/go/SelfLoathing | 43 |
| 100 | Self Loathing Indexes | Large Active Heap | https://www.brentozar.com/go/SelfLoathing | 44 |
| 100 | Self Loathing Indexes | Low Fill Factor on Clustered Index | https://www.brentozar.com/go/SelfLoathing | 40 |
| 100 | Self Loathing Indexes | Low Fill Factor on Nonclustered Index | https://www.brentozar.com/go/SelfLoathing | 40 |
| 100 | Self Loathing Indexes | Medium Active Heap | https://www.brentozar.com/go/SelfLoathing | 45 |
| 100 | Self Loathing Indexes | Small Active Heap | https://www.brentozar.com/go/SelfLoathing | 46 |
| 100 | Serial Forcer | Computed Column with Scalar UDF | https://www.brentozar.com/go/serialudf | 99 |
| 100 | Serial Forcer | Check Constraint with Scalar UDF | https://www.brentozar.com/go/computedscalar | 94 |
| 150 | Abnormal Psychology | Cascading Updates or Deletes | https://www.brentozar.com/go/AbnormalPsychology | 71 |
| 150 | Abnormal Psychology | Unindexed Foreign Keys | https://www.brentozar.com/go/AbnormalPsychology | 72 |
| 150 | Abnormal Psychology | Columnstore Index | https://www.brentozar.com/go/AbnormalPsychology | 61 |
| 150 | Abnormal Psychology | Column Collation Does Not Match Database Collation| https://www.brentozar.com/go/AbnormalPsychology | 69 |
| 150 | Abnormal Psychology | Compressed Index | https://www.brentozar.com/go/AbnormalPsychology | 63 |
| 150 | Abnormal Psychology | In-Memory OLTP | https://www.brentozar.com/go/AbnormalPsychology | 73 |
| 150 | Abnormal Psychology | Non-Aligned Index on a Partitioned Table | https://www.brentozar.com/go/AbnormalPsychology | 65 |
| 150 | Abnormal Psychology | Partitioned Index | https://www.brentozar.com/go/AbnormalPsychology | 64 |
| 150 | Abnormal Psychology | Spatial Index | https://www.brentozar.com/go/AbnormalPsychology | 62 |
| 150 | Abnormal Psychology | XML Index | https://www.brentozar.com/go/AbnormalPsychology | 60 |
| 150 | Index Hoarder | Borderline: Wide Indexes (7 or More Columns) | https://www.brentozar.com/go/IndexHoarder | 23 |
| 150 | Index Hoarder | More Than 5 Percent NC Indexes Are Unused | https://www.brentozar.com/go/IndexHoarder | 21 |
| 150 | Index Hoarder | Non-Unique Clustered Index | https://www.brentozar.com/go/IndexHoarder | 28 |
| 150 | Index Hoarder | Unused NC Index with Low Writes | https://www.brentozar.com/go/IndexHoarder | 29 |
| 150 | Index Hoarder | Wide Clustered Index (>3 columns or >16 bytes) | https://www.brentozar.com/go/IndexHoarder | 24 |
| 150 | Self Loathing Indexes | Disabled Index | https://www.brentozar.com/go/SelfLoathing | 42 |
| 150 | Self Loathing Indexes | Hypothetical Index | https://www.brentozar.com/go/SelfLoathing | 41 |
| 200 | Abnormal Psychology | Identity Column Using a Negative Seed or Increment Other Than 1 | https://www.brentozar.com/go/AbnormalPsychology | 74 |
| 200 | Abnormal Psychology | Recently Created Tables/Indexes (1 week) | https://www.brentozar.com/go/AbnormalPsychology | 66 |
| 200 | Abnormal Psychology | Recently Modified Tables/Indexes (2 days) | https://www.brentozar.com/go/AbnormalPsychology | 67 |
| 200 | Abnormal Psychology | Replicated Columns | https://www.brentozar.com/go/AbnormalPsychology | 70 |
| 200 | Abnormal Psychology | Temporal Tables | https://www.brentozar.com/go/AbnormalPsychology | 110 |
| 200 | Cold Calculators | Definition Defeatists | https://www.brentozar.com/go/serialudf | 100 |
| 200 | Functioning Statistaholics | Filter Fixation | https://www.brentozar.com/go/stats | 93 |
| 200 | Index Hoarder | Addicted to Nulls | https://www.brentozar.com/go/IndexHoarder | 25 |
| 200 | Index Hoarder | Addicted to Strings | https://www.brentozar.com/go/IndexHoarder | 27 |
| 200 | Index Hoarder | Wide Tables: 35+ cols or > 2000 non-LOB bytes | https://www.brentozar.com/go/IndexHoarder | 26 |
| 200 | Self Loathing Indexes | Heaps with Deletes | https://www.brentozar.com/go/SelfLoathing | 49 |
| 200 | Workaholics | Scan-a-lots (index-usage-stats) | https://www.brentozar.com/go/Workaholics | 80 |
| 200 | Workaholics | Top Recent Accesses (index-op-stats) | https://www.brentozar.com/go/Workaholics | 81 |
| 250 | Feature-Phobic Indexes | Few Indexes Use Includes | https://www.brentozar.com/go/IndexFeatures | 31 |
| 250 | Feature-Phobic Indexes | No Filtered Indexes or Indexed Views | https://www.brentozar.com/go/IndexFeatures | 32 |
| 250 | Feature-Phobic Indexes | No Indexes Use Includes | https://www.brentozar.com/go/IndexFeatures | 30 |
| 250 | Feature-Phobic Indexes | Potential Filtered Index (Based on Column Name) | https://www.brentozar.com/go/IndexFeatures | 33 |
| 250 | Medicated Indexes | Optimized For Sequential Keys |  | 121 |
