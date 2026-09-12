# sp_BlitzBackups Checks by Priority

This table lists all checks ordered by priority.

Before adding a new check, make sure to add a Github issue for it first, and have a group discussion about its priority, and description.

If you want to change anything about a check - the priority, finding, or ID - open a Github issue first. The relevant scripts have to be updated too.

CURRENT HIGH CHECKID: 16.
If you want to add a new one, start at 17.

| Priority | Finding                                | CheckID |
| -------: | :------------------------------------- | ------: |
|       10 | Recovery model switched                |      11 |
|       10 | Backup to discard device without COPY_ONLY |      14 |
|       10 | Damaged backups                        |       8 |
|       20 | No CHECKSUMS                           |       7 |
|       20 | Backup to discard device                   |      14 |
|       50 | RTO estimate unavailable              |      15 |
|       50 | Single user mode backups               |       6 |
|       50 | Big Diffs/Logs                         |      13 |
|       80 | Uncompressed backups                   |      12 |
|      100 | Recovery fork metadata missing        |      16 |
|      100 | Non-Agent backups taken                |       1 |
|      100 | Compatibility level changing           |       2 |
|      100 | Password backups                       |       3 |
|      100 | Encrypted backups                      |       9 |
|      100 | Bulk logged backups                    |      10 |
|      150 | Read only state backups                |       5 |
|      200 | Snapshot backups                       |       4 |

Check 15 leaves RTO unknown when the candidate history contains multiple known recovery forks, a regular log written to a discard device or marked damaged, missing media or integrity metadata, or no usable full for its log endpoint. Candidate history includes the reporting window and its preceding usable full backup; discarded or damaged copy-only, full, and differential alternatives are ignored. Centralized history without backupmediafamily remains readable, but cannot receive a verified RTO estimate until media metadata is available.

RTO is a worst-case estimate for the requested interval, not only the latest restore point. A later full within that interval does not make an earlier discard-log gap restorable, so the interval remains unavailable rather than reporting only its later healthy portion.

Check 16 warns that legacy history lacks fork identifiers. LSN-based estimates remain available, but fork compatibility cannot be verified. New centralized-history pushes preserve the required metadata. See issue #4115.

For merged-source centralized history, instance-local media IDs cannot identify which source owns a media-family row. RTO remains unavailable for these ambiguous mappings.
