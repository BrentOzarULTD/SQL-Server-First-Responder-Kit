# sp_BlitzBackups Checks by Priority

This table lists all checks ordered by priority.

Before adding a new check, make sure to add a Github issue for it first, and have a group discussion about its priority, and description.

If you want to change anything about a check - the priority, finding, or ID - open a Github issue first. The relevant scripts have to be updated too.

CURRENT HIGH CHECKID: 14.
If you want to add a new one, start at 15.

| Priority | Finding                                | CheckID |
| -------: | :------------------------------------- | ------: |
|       10 | Recovery model switched                |      11 |
|       10 | Backup to NUL device without COPY_ONLY |      14 |
|       10 | Damaged backups                        |       8 |
|       20 | No CHECKSUMS                           |       7 |
|       20 | Backup to NUL device                   |      14 |
|       50 | Single user mode backups               |       6 |
|       50 | Big Diffs/Logs                         |      13 |
|       80 | Uncompressed backups                   |      12 |
|      100 | Non-Agent backups taken                |       1 |
|      100 | Compatibility level changing           |       2 |
|      100 | Password backups                       |       3 |
|      100 | Encrypted backups                      |       9 |
|      100 | Bulk logged backups                    |      10 |
|      150 | Read only state backups                |       5 |
|      200 | Snapshot backups                       |       4 |
