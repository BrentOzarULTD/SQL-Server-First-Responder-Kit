/*
sp_DatabaseRestore — SQL injection regression tests
=====================================================

These cases cover inputs that, prior to the hardening in PR #3980,
could have been abused by a caller with EXECUTE permission on the
proc to inject T-SQL or to inject shell commands via xp_cmdshell.

Threat model: a caller who has EXECUTE on sp_DatabaseRestore but is
not sysadmin should not be able to escalate to arbitrary T-SQL or
shell command execution by passing crafted parameter values.

How to run
----------
sp_DatabaseRestore + dbo.CommandExecute (Ola Hallengren's Maintenance
Solution) must be installed in the current database. None of the C:\
paths below need to actually exist — the validation gate fires (or
doesn't) before xp_dirtree / xp_cmdshell are invoked, and the
dynamic-RESTORE strings produced by @Debug = 1 are only PRINTed.

Each test wraps the EXEC in BEGIN TRY / BEGIN CATCH so one failure
doesn't stop the suite. The expected outcome is annotated above
each test as either:
   BLOCKED   — the validation gate must RAISERROR + RETURN
   PASSES    — the validation gate must let the input through
   NEUTRAL   — the input must be quoted/escaped so it cannot break
               out of its surrounding SQL literal or identifier

The "logic-only" cases at the bottom mimic the proc's PARSENAME +
QUOTENAME / single-quote-doubling without needing a real backup
folder, so the schema-resolution and identifier-quoting behaviour
can still be exercised on a fresh test rig.

Paths use C:\ so this works on any test rig without special storage.
*/


PRINT '====================================================';
PRINT 'PART 1 - Path-shape validation gate';
PRINT 'Each path-shaped parameter that contains a character with no';
PRINT 'legitimate use in a Windows path (and high command-injection';
PRINT 'risk) must be rejected before the proc reaches xp_cmdshell';
PRINT 'or dynamic-SQL construction.';
PRINT '====================================================';
GO

PRINT '--- 1.1 BLOCKED: ampersand in @BackupPathFull (cmd shell metachar) ---';
BEGIN TRY
    EXEC dbo.sp_DatabaseRestore @Database = 'AnyDB',
                                @BackupPathFull = 'C:\Backups\& whoami &\',
                                @Execute = 'N', @Debug = 1;
    PRINT '*** REGRESSION: not blocked ***';
END TRY BEGIN CATCH PRINT 'BLOCKED: ' + ERROR_MESSAGE(); END CATCH;
GO

PRINT '--- 1.2 BLOCKED: semicolon + nested injection in @BackupPathFull ---';
BEGIN TRY
    EXEC dbo.sp_DatabaseRestore @Database = 'AnyDB',
                                @BackupPathFull = 'C:\Backups\; xp_cmdshell ''calc''; --\',
                                @Execute = 'N', @Debug = 1;
    PRINT '*** REGRESSION: not blocked ***';
END TRY BEGIN CATCH PRINT 'BLOCKED: ' + ERROR_MESSAGE(); END CATCH;
GO

PRINT '--- 1.3 BLOCKED: pipe in @StandbyUndoPath ---';
BEGIN TRY
    EXEC dbo.sp_DatabaseRestore @Database = 'AnyDB',
                                @BackupPathFull = 'C:\Backups\',
                                @StandbyUndoPath = 'C:\Standby\| dir |\',
                                @Execute = 'N', @Debug = 1;
    PRINT '*** REGRESSION: not blocked ***';
END TRY BEGIN CATCH PRINT 'BLOCKED: ' + ERROR_MESSAGE(); END CATCH;
GO

PRINT '--- 1.4 BLOCKED: caret in @MoveDataDrive ---';
BEGIN TRY
    EXEC dbo.sp_DatabaseRestore @Database = 'AnyDB',
                                @BackupPathFull = 'C:\Backups\',
                                @MoveDataDrive = 'C:\Data^evil\',
                                @Execute = 'N', @Debug = 1;
    PRINT '*** REGRESSION: not blocked ***';
END TRY BEGIN CATCH PRINT 'BLOCKED: ' + ERROR_MESSAGE(); END CATCH;
GO

PRINT '--- 1.5 BLOCKED: < in @FileNamePrefix ---';
BEGIN TRY
    EXEC dbo.sp_DatabaseRestore @Database = 'AnyDB',
                                @BackupPathFull = 'C:\Backups\',
                                @FileNamePrefix = 'pref<x',
                                @Execute = 'N', @Debug = 1;
    PRINT '*** REGRESSION: not blocked ***';
END TRY BEGIN CATCH PRINT 'BLOCKED: ' + ERROR_MESSAGE(); END CATCH;
GO

PRINT '--- 1.6 BLOCKED: > in @MoveLogDrive ---';
BEGIN TRY
    EXEC dbo.sp_DatabaseRestore @Database = 'AnyDB',
                                @BackupPathFull = 'C:\Backups\',
                                @MoveLogDrive = 'C:\Logs>x\',
                                @Execute = 'N', @Debug = 1;
    PRINT '*** REGRESSION: not blocked ***';
END TRY BEGIN CATCH PRINT 'BLOCKED: ' + ERROR_MESSAGE(); END CATCH;
GO

PRINT '--- 1.7 BLOCKED: double-quote in @BackupPathDiff ---';
BEGIN TRY
    EXEC dbo.sp_DatabaseRestore @Database = 'AnyDB',
                                @BackupPathFull = 'C:\Backups\',
                                @BackupPathDiff = 'C:\Diff\"& whoami &"\',
                                @Execute = 'N', @Debug = 1;
    PRINT '*** REGRESSION: not blocked ***';
END TRY BEGIN CATCH PRINT 'BLOCKED: ' + ERROR_MESSAGE(); END CATCH;
GO

PRINT '--- 1.8 BLOCKED: NUL byte in @BackupPathLog ---';
PRINT '   (the LIKE pattern uses COLLATE Latin1_General_BIN2 so NUL is detected;';
PRINT '    under the default collation NUL is silently dropped from both sides)';
DECLARE @PathWithNul nvarchar(260) = N'C:\Logs\' + NCHAR(0) + N'evil\';
BEGIN TRY
    EXEC dbo.sp_DatabaseRestore @Database = 'AnyDB',
                                @BackupPathFull = 'C:\Backups\',
                                @BackupPathLog = @PathWithNul,
                                @Execute = 'N', @Debug = 1;
    PRINT '*** REGRESSION: not blocked ***';
END TRY BEGIN CATCH PRINT 'BLOCKED: ' + ERROR_MESSAGE(); END CATCH;
GO

PRINT '--- 1.9 BLOCKED: CR in @BackupPathFull ---';
DECLARE @PathWithCR nvarchar(260) = N'C:\Backups\' + NCHAR(13) + N'evil\';
BEGIN TRY
    EXEC dbo.sp_DatabaseRestore @Database = 'AnyDB',
                                @BackupPathFull = @PathWithCR,
                                @Execute = 'N', @Debug = 1;
    PRINT '*** REGRESSION: not blocked ***';
END TRY BEGIN CATCH PRINT 'BLOCKED: ' + ERROR_MESSAGE(); END CATCH;
GO

PRINT '--- 1.10 BLOCKED: LF in @MoveFilestreamDrive ---';
DECLARE @PathWithLF nvarchar(260) = N'C:\FS\' + NCHAR(10) + N'evil\';
BEGIN TRY
    EXEC dbo.sp_DatabaseRestore @Database = 'AnyDB',
                                @BackupPathFull = 'C:\Backups\',
                                @MoveFilestreamDrive = @PathWithLF,
                                @Execute = 'N', @Debug = 1;
    PRINT '*** REGRESSION: not blocked ***';
END TRY BEGIN CATCH PRINT 'BLOCKED: ' + ERROR_MESSAGE(); END CATCH;
GO


PRINT '====================================================';
PRINT 'PART 2 - Identifier validation';
PRINT '@RunStoredProcAfterRestore must be a 1- or 2-part name. The';
PRINT 'proc rejects 3- and 4-part names so a caller cannot execute';
PRINT 'cross-server / cross-database procs by smuggling extra dots.';
PRINT '====================================================';
GO

PRINT '--- 2.1 BLOCKED: 3-part @RunStoredProcAfterRestore ---';
BEGIN TRY
    EXEC dbo.sp_DatabaseRestore @Database = 'AnyDB',
                                @BackupPathFull = 'C:\Backups\',
                                @RunStoredProcAfterRestore = 'targetdb.dbo.MyProc',
                                @Execute = 'N', @Debug = 1;
    PRINT '*** REGRESSION: not blocked ***';
END TRY BEGIN CATCH PRINT 'BLOCKED: ' + ERROR_MESSAGE(); END CATCH;
GO

PRINT '--- 2.2 BLOCKED: 4-part (linked-server) @RunStoredProcAfterRestore ---';
BEGIN TRY
    EXEC dbo.sp_DatabaseRestore @Database = 'AnyDB',
                                @BackupPathFull = 'C:\Backups\',
                                @RunStoredProcAfterRestore = 'attackerSrv.targetdb.dbo.MyProc',
                                @Execute = 'N', @Debug = 1;
    PRINT '*** REGRESSION: not blocked ***';
END TRY BEGIN CATCH PRINT 'BLOCKED: ' + ERROR_MESSAGE(); END CATCH;
GO


PRINT '====================================================';
PRINT 'PART 3 - Regression: legitimate inputs must still pass';
PRINT 'These should pass the validation gate. They will fail later';
PRINT 'with "(FULL) No rows were returned for that database in path"';
PRINT 'because no real backups exist at C:\Backups\ — that is the';
PRINT 'expected outcome and means the validation gate did not block.';
PRINT '====================================================';
GO

PRINT '--- 3.1 PASSES: ordinary path ---';
BEGIN TRY
    EXEC dbo.sp_DatabaseRestore @Database = 'AnyDB',
                                @BackupPathFull = 'C:\Backups\',
                                @Execute = 'N', @Debug = 1;
END TRY BEGIN CATCH PRINT 'EXPECTED LATE FAILURE: ' + ERROR_MESSAGE(); END CATCH;
GO

PRINT '--- 3.2 PASSES: path with apostrophe (legal but rare on Windows) ---';
PRINT '   (validation gate must allow it; downstream sites must escape it)';
BEGIN TRY
    EXEC dbo.sp_DatabaseRestore @Database = 'AnyDB',
                                @BackupPathFull = 'C:\Backups\It''s Friday\',
                                @Execute = 'N', @Debug = 1;
END TRY BEGIN CATCH PRINT 'EXPECTED LATE FAILURE: ' + ERROR_MESSAGE(); END CATCH;
GO

PRINT '--- 3.3 PASSES: @Database with ampersand (database identifier, not a path) ---';
PRINT '   (DB names can legally contain & ; etc.; @Database is intentionally NOT';
PRINT '    in the path-shape gate — it gets single-quote-escaped at concat sites)';
BEGIN TRY
    EXEC dbo.sp_DatabaseRestore @Database = 'My&DB',
                                @BackupPathFull = 'C:\Backups\',
                                @Execute = 'N', @Debug = 1;
END TRY BEGIN CATCH PRINT 'EXPECTED LATE FAILURE: ' + ERROR_MESSAGE(); END CATCH;
GO

PRINT '--- 3.4 PASSES: 1-part @RunStoredProcAfterRestore ---';
BEGIN TRY
    EXEC dbo.sp_DatabaseRestore @Database = 'AnyDB',
                                @BackupPathFull = 'C:\Backups\',
                                @RunStoredProcAfterRestore = 'MyProc',
                                @Execute = 'N', @Debug = 1;
END TRY BEGIN CATCH PRINT 'EXPECTED LATE FAILURE: ' + ERROR_MESSAGE(); END CATCH;
GO

PRINT '--- 3.5 PASSES: 2-part @RunStoredProcAfterRestore ---';
BEGIN TRY
    EXEC dbo.sp_DatabaseRestore @Database = 'AnyDB',
                                @BackupPathFull = 'C:\Backups\',
                                @RunStoredProcAfterRestore = 'dbo.MyProc',
                                @Execute = 'N', @Debug = 1;
END TRY BEGIN CATCH PRINT 'EXPECTED LATE FAILURE: ' + ERROR_MESSAGE(); END CATCH;
GO

PRINT '--- 3.6 PASSES: @RunStoredProcAfterRestore with leading/trailing whitespace ---';
PRINT '   (the proc trims and collapses whitespace around dots before PARSENAME so';
PRINT '    pre-hardening callers using ''  MyProc  '' or ''dbo. MyProc'' still work)';
BEGIN TRY
    EXEC dbo.sp_DatabaseRestore @Database = 'AnyDB',
                                @BackupPathFull = 'C:\Backups\',
                                @RunStoredProcAfterRestore = '  MyProc  ',
                                @Execute = 'N', @Debug = 1;
END TRY BEGIN CATCH PRINT 'EXPECTED LATE FAILURE: ' + ERROR_MESSAGE(); END CATCH;
GO

PRINT '--- 3.7 PASSES: @RunStoredProcAfterRestore with whitespace around the dot ---';
BEGIN TRY
    EXEC dbo.sp_DatabaseRestore @Database = 'AnyDB',
                                @BackupPathFull = 'C:\Backups\',
                                @RunStoredProcAfterRestore = 'dbo. MyProc',
                                @Execute = 'N', @Debug = 1;
END TRY BEGIN CATCH PRINT 'EXPECTED LATE FAILURE: ' + ERROR_MESSAGE(); END CATCH;
GO

PRINT '--- 3.8 PASSES: bracketed identifiers with whitespace around the dot ---';
BEGIN TRY
    EXEC dbo.sp_DatabaseRestore @Database = 'AnyDB',
                                @BackupPathFull = 'C:\Backups\',
                                @RunStoredProcAfterRestore = '[dbo]. [My Proc]',
                                @Execute = 'N', @Debug = 1;
END TRY BEGIN CATCH PRINT 'EXPECTED LATE FAILURE: ' + ERROR_MESSAGE(); END CATCH;
GO


PRINT '====================================================';
PRINT 'PART 4 - Logic-only verification (mimics what the proc does)';
PRINT 'These do not call the proc; they reproduce the proc''s';
PRINT 'PARSENAME + QUOTENAME / single-quote-doubling so you can see';
PRINT 'on a fresh rig (without real backups) that injected payloads';
PRINT 'are quoted into harmless identifiers / string literals.';
PRINT '====================================================';
GO

PRINT '--- 4.1 NEUTRAL: @RunStoredProcAfterRestore with embedded ;DROP TABLE ---';
PRINT '   Input is wrapped in a single bracketed identifier; SQL Server treats';
PRINT '   the entire payload as one (non-existent) procedure name and the EXEC';
PRINT '   simply fails to find it — no statement break, no DROP runs.';
DECLARE @Input nvarchar(260) = N'dbo.MyProc; DROP TABLE foo; --';
DECLARE @Schema sysname = NULLIF(PARSENAME(@Input, 2), N'');
DECLARE @Proc   sysname = PARSENAME(@Input, 1);
DECLARE @Db     nvarchar(128) = QUOTENAME('TargetDB');
PRINT 'Input:  ' + @Input;
PRINT 'Output: EXEC ' + @Db + N'.' + ISNULL(QUOTENAME(@Schema), N'') + N'.' + QUOTENAME(@Proc);
GO

PRINT '--- 4.2 NEUTRAL: @DatabaseOwner with bracket-injection ---';
PRINT '   QUOTENAME doubles the inner ] so the entire string becomes one';
PRINT '   bracketed identifier; the EXISTS check on syslogins fails to find';
PRINT '   the login and the proc just PRINTs "not a valid Login".';
DECLARE @Owner sysname = N'sa]; DROP TABLE foo; --';
DECLARE @TargetDb nvarchar(128) = QUOTENAME('TargetDB');
PRINT 'Input:  ' + @Owner;
PRINT 'Output: ALTER AUTHORIZATION ON DATABASE::' + @TargetDb + N' TO ' + QUOTENAME(@Owner);
GO

PRINT '--- 4.3 NEUTRAL: 1-part proc name uses 3-part db..proc form ---';
PRINT '   Without the second dot, [TargetDB].[MyProc] would be parsed as';
PRINT '   schema=TargetDB.proc=MyProc in the *current* DB, not a proc in';
PRINT '   the restored DB.';
DECLARE @In nvarchar(260) = N'MyProc';
DECLARE @S sysname = NULLIF(PARSENAME(@In, 2), N'');
DECLARE @P sysname = PARSENAME(@In, 1);
DECLARE @D nvarchar(128) = QUOTENAME('TargetDB');
PRINT 'Input:  ' + @In;
PRINT 'Output: EXEC ' + @D + N'.' + ISNULL(QUOTENAME(@S), N'') + N'.' + QUOTENAME(@P);
GO

PRINT '--- 4.4 NEUTRAL: @StandbyUndoPath with apostrophe gets quote-doubled in dynamic RESTORE ---';
PRINT '   The path itself is preserved verbatim; what doubles is the apostrophe';
PRINT '   inside the SQL string literal. After SQL parses the literal, the value';
PRINT '   is a path containing one apostrophe, which is what the user supplied.';
DECLARE @StandbyPath nvarchar(max) = N'C:\Standby\It''s\';
DECLARE @TestDb nvarchar(128) = N'TestDB';
PRINT 'Input @StandbyUndoPath: ' + @StandbyPath;
PRINT 'Generated STANDBY clause: STANDBY = ''' + REPLACE(@StandbyPath, N'''', N'''''') + REPLACE(@TestDb, N'''', N'''''') + 'Undo.ldf''';
GO

PRINT '--- 4.5 NEUTRAL: MOVE clause with apostrophe in @MoveDataDrive ---';
PRINT '   The path-validation gate allows apostrophes (legal in Windows paths) so';
PRINT '   they reach the @MoveOption builder, which embeds them in MOVE ''logical''';
PRINT '   TO ''physical'' literals. The inline REPLACE doubles the apostrophe so';
PRINT '   the literal still terminates correctly.';
DECLARE @FilesTbl TABLE (LogicalName nvarchar(128), TargetPhysicalName nvarchar(260), PhysicalName nvarchar(260));
INSERT @FilesTbl VALUES (N'TestData', N'C:\It''s\Data\Test.mdf', N'D:\old\Test.mdf');
DECLARE @MoveOptOut nvarchar(max) = N'';
SELECT @MoveOptOut = @MoveOptOut + N', MOVE ''' + REPLACE(LogicalName, N'''', N'''''') + N''' TO ''' + REPLACE(TargetPhysicalName, N'''', N'''''') + ''''
FROM @FilesTbl;
PRINT '@MoveOption fragment: ' + @MoveOptOut;
GO

PRINT '--- 4.6 NEUTRAL: nested-EXEC RESTORE HEADERONLY needs four-quote escape ---';
PRINT '   The HEADERONLY/FILELISTONLY templates are EXEC(''RESTORE ... DISK=''''<path>'''''')';
PRINT '   so the path crosses two SQL-parser layers. A single apostrophe in the';
PRINT '   path needs to become four single quotes (REPLICATE(N'''''''', 4)) to';
PRINT '   survive both layers and reappear as one apostrophe in the inner literal.';
DECLARE @Tpl nvarchar(4000) = N'EXEC (''RESTORE HEADERONLY FROM DISK=''''{Path}'''''')';
DECLARE @Path nvarchar(max) = N'C:\Backups\It''s\full.bak';
DECLARE @Sql  nvarchar(max) = REPLACE(@Tpl, N'{Path}', REPLACE(@Path, N'''', REPLICATE(N'''', 4)));
PRINT 'Input @Path:   ' + @Path;
PRINT 'Outer @sql:    ' + @Sql;
PRINT '(after the outer EXEC parses @sql, the inner RESTORE sees a literal';
PRINT '''C:\Backups\It''s\full.bak'' — one apostrophe, exactly as supplied)';
GO


PRINT '====================================================';
PRINT 'Done. Suite passed if every BLOCKED test produced "BLOCKED:"';
PRINT 'and every PASSES test produced "EXPECTED LATE FAILURE: (FULL)';
PRINT 'No rows were returned..." (or completed without raising).';
PRINT '====================================================';
GO
