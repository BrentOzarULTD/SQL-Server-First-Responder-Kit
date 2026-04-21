SET ANSI_NULLS ON;
SET ANSI_PADDING ON;
SET ANSI_WARNINGS ON;
SET ARITHABORT ON;
SET CONCAT_NULL_YIELDS_NULL ON;
SET QUOTED_IDENTIFIER ON;
SET STATISTICS IO OFF;
SET STATISTICS TIME OFF;
GO

IF (
SELECT
  CASE
     WHEN CAST(SERVERPROPERTY('EngineEdition') AS INT) IN (5, 6, 8) THEN 1 /* Azure SQL DB, MI, Synapse */
     WHEN CONVERT(NVARCHAR(128), SERVERPROPERTY ('PRODUCTVERSION')) LIKE '8%' THEN 0
     WHEN CONVERT(NVARCHAR(128), SERVERPROPERTY ('PRODUCTVERSION')) LIKE '9%' THEN 0
     WHEN CONVERT(NVARCHAR(128), SERVERPROPERTY ('PRODUCTVERSION')) LIKE '10%' THEN 0
     WHEN CONVERT(NVARCHAR(128), SERVERPROPERTY ('PRODUCTVERSION')) LIKE '11%' THEN 0
     WHEN CONVERT(NVARCHAR(128), SERVERPROPERTY ('PRODUCTVERSION')) LIKE '12%' THEN 0
     WHEN CONVERT(NVARCHAR(128), SERVERPROPERTY ('PRODUCTVERSION')) LIKE '13%' THEN 0
     ELSE 1
  END
) = 0
BEGIN
    DECLARE @msg VARCHAR(8000);
    SELECT @msg = 'Sorry, sp_BlitzPlanCompare doesn''t work on versions of SQL prior to 2017.' + REPLICATE(CHAR(13), 7933);
    PRINT @msg;
    RETURN;
END;

IF OBJECT_ID('dbo.sp_BlitzPlanCompare') IS NULL
    EXEC ('CREATE PROCEDURE dbo.sp_BlitzPlanCompare AS RETURN 0;');
GO

ALTER PROCEDURE dbo.sp_BlitzPlanCompare
    /* Plan identifiers - at least one required (except in mode 2 where @CompareToXML carries its own). */
    @QueryPlanHash    BINARY(8)     = NULL,  /* most specific; one cached plan exactly */
    @QueryHash        BINARY(8)     = NULL,  /* stable across servers; usually narrows to 1-few plans */
    @StoredProcName   NVARCHAR(400) = NULL,  /* accepts 'proc', 'schema.proc', 'db.schema.proc'; resolved via OBJECT_ID() */
    /* Narrowing - optional on its own, must accompany one of the above */
    @DatabaseName     SYSNAME       = NULL,  /* scopes plan lookup to this database; required with @StoredProcName on Azure SQL DB */
    /* Comparison source - exactly one (or none for mode 1) */
    @CompareToXML     XML           = NULL,  /* mode 2: snapshot XML from another server */
    @LinkedServer     SYSNAME       = NULL,  /* mode 3: call sp_BlitzPlanCompare on the linked server */
    /* Misc */
    @Help             BIT           = 0,
    @Debug            BIT           = 0,
    @Version          VARCHAR(30)   = NULL OUTPUT,
    @VersionDate      DATETIME      = NULL OUTPUT,
    @VersionCheckMode BIT           = 0
WITH RECOMPILE
AS
BEGIN

SET NOCOUNT ON;
SET STATISTICS XML OFF;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT @Version = '8.32', @VersionDate = '20260407';

IF (@VersionCheckMode = 1)
BEGIN
    RETURN;
END;

DECLARE @nl NVARCHAR(2) = NCHAR(13) + NCHAR(10);

IF @Help = 1
BEGIN
    PRINT '
    sp_BlitzPlanCompare from http://FirstResponderKit.org

    This script compares two SQL Servers (Local and Remote) to identify differences
    that may explain why the same query plan performs differently between them. It
    examines stats, indexes, row counts, sniffed parameters, configuration, hardware,
    and live state for the objects actually referenced by the plan.

    Three operating modes:
      1. plan identifier(s) only          -> emit a snapshot XML you copy/paste to the other server
      2. @CompareToXML only               -> diff this server against the snapshot XML you pasted in
      3. plan identifier(s) + @LinkedServer -> diff this server against the linked server in one call

    Plan identifiers (at least one required for modes 1 and 3):
      - @QueryPlanHash   - most specific; one cached plan exactly
      - @QueryHash       - stable across servers; usually narrows to 1-few plans
      - @StoredProcName  - human-friendly: "usp_Foo", "dbo.usp_Foo", "[db].[schema].[usp_Foo]"
      - @DatabaseName    - narrows any of the above (cannot stand alone)

    If the identifier(s) you provide resolve to multiple cached plans, sp_BlitzPlanCompare
    returns the candidates with set_options + query text snippet so you can re-run with a
    specific @QueryPlanHash.

    Linked-server mode requires:
      - sp_BlitzPlanCompare installed on BOTH servers
      - RPC OUT enabled on the linked server
      - The local server is NOT Azure SQL DB or Synapse (no linked server support)

    Plan source: prefers the actual execution plan from sys.dm_exec_query_plan_stats
    (richer runtime info: actual memory grant used, spills that fired, per-operator wait
    stats) and falls back to the cached estimated plan from sys.dm_exec_query_plan. To get
    the actual plan, enable LAST_QUERY_PLAN_STATS on the database:
        ALTER DATABASE SCOPED CONFIGURATION SET LAST_QUERY_PLAN_STATS = ON;
    LAST_QUERY_PLAN_STATS requires SQL Server 2019+ or Azure SQL DB.

    Known limitations of this version:
     - v0.01 covers core differences only. Resource Governor, triggers, FKs,
       collation drift, datatype drift, fragmentation, fill factor, and index
       compression are deferred to a later version.
     - Statistics comparison uses header data only (last update, mod counter, sample).
       Histogram comparison is deferred to a later version.
     - Plan lookup uses the plan cache only; Query Store fallback is not implemented.

    Changes - for the full list of improvements and fixes in this version, see:
    https://github.com/BrentOzarULTD/SQL-Server-First-Responder-Kit/



    MIT License

    Copyright (c) Brent Ozar Unlimited

    Permission is hereby granted, free of charge, to any person obtaining a copy
    of this software and associated documentation files (the "Software"), to deal
    in the Software without restriction, including without limitation the rights
    to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
    copies of the Software, and to permit persons to whom the Software is
    furnished to do so, subject to the following conditions:

    The above copyright notice and this permission notice shall be included in all
    copies or substantial portions of the Software.

    THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
    IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
    FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
    AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
    LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
    OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
    SOFTWARE.
    ';

    SELECT N'@QueryPlanHash' AS [Parameter Name],
           N'BINARY(8)'      AS [Data Type],
           N'The query_plan_hash of a cached plan. Most specific identifier. Modes 1 and 3.' AS [Parameter Description]
    UNION ALL
    SELECT N'@QueryHash',
           N'BINARY(8)',
           N'The query_hash (logical query fingerprint). Stable across servers. Usually narrows to 1-few plans - if >1, you get a disambiguation result set to pick a @QueryPlanHash.'
    UNION ALL
    SELECT N'@StoredProcName',
           N'NVARCHAR(400)',
           N'A proc name, either bare ("usp_Foo"), schema-qualified ("dbo.usp_Foo"), or three-part ("[db].[schema].[usp_Foo]"). Resolved via OBJECT_ID() in either the current database or @DatabaseName. Multi-statement procs usually return multiple plans -> disambiguation result set.'
    UNION ALL
    SELECT N'@DatabaseName',
           N'SYSNAME',
           N'Scopes the plan lookup (and @StoredProcName resolution) to this database. Cannot stand alone - must accompany one of the identifier parameters.'
    UNION ALL
    SELECT N'@CompareToXML',
           N'XML',
           N'Snapshot XML produced by a prior mode-1 run on the other server. Supplying this triggers compare-mode (mode 2).'
    UNION ALL
    SELECT N'@LinkedServer',
           N'SYSNAME',
           N'Name of a configured linked server (with RPC OUT) to call sp_BlitzPlanCompare on remotely. Combine with a plan identifier for mode 3.'
    UNION ALL
    SELECT N'@Help',
           N'BIT',
           N'Displays this help message.'
    UNION ALL
    SELECT N'@Debug',
           N'BIT',
           N'When 1, prints intermediate progress messages and the dynamic SQL used for cross-database snapshots.'
    UNION ALL
    SELECT N'@Version',
           N'VARCHAR(30) OUTPUT',
           N'Returns the version string.'
    UNION ALL
    SELECT N'@VersionDate',
           N'DATETIME OUTPUT',
           N'Returns the version build date.'
    UNION ALL
    SELECT N'@VersionCheckMode',
           N'BIT',
           N'When 1, the procedure sets @Version and @VersionDate and returns immediately.';

    RETURN;
END;

/* =====================================================================
   Variable declarations
   ===================================================================== */
DECLARE @Mode             TINYINT,
        @EngineEdition    INT      = CAST(SERVERPROPERTY('EngineEdition')  AS INT),
        @ProductMajor     INT      = CAST(LEFT(CAST(SERVERPROPERTY('ProductVersion') AS NVARCHAR(128)),
                                          CHARINDEX('.', CAST(SERVERPROPERTY('ProductVersion') AS NVARCHAR(128))) - 1) AS INT),
        @LocalServerName  NVARCHAR(256) = CAST(SERVERPROPERTY('ServerName') AS NVARCHAR(256)),
        @QueryHashBin     BINARY(8),     /* the query_hash of the resolved plan (or from mode 2 XML) */
        @MatchCount       INT,
        @PlanHandle       VARBINARY(64),
        @SqlHandle        VARBINARY(64),
        @StmtStartOffset  INT,
        @StmtEndOffset    INT,
        @PlanXml          XML,
        @SnapshotXml      XML,
        @sql              NVARCHAR(MAX),
        @ErrNumber        INT,
        @ErrMessage       NVARCHAR(4000);

/* =====================================================================
   Parameter validation and mode dispatch
   ===================================================================== */

/* At least one way of identifying the plan. @CompareToXML carries its own hashes
   inside the XML payload so it counts as an identifier for mode 2. */
DECLARE @HasIdentifier BIT =
    CASE WHEN @QueryPlanHash  IS NOT NULL
           OR @QueryHash      IS NOT NULL
           OR @StoredProcName IS NOT NULL
           OR @CompareToXML   IS NOT NULL
         THEN 1 ELSE 0 END;

IF @HasIdentifier = 0
BEGIN
    RAISERROR('You must supply at least one of @QueryPlanHash, @QueryHash, @StoredProcName, or @CompareToXML. Run with @Help = 1 for usage.', 16, 1);
    RETURN;
END;

/* @DatabaseName is a narrower, not an identifier. It needs one of the three
   identifier parameters to scope. In mode 2 it is irrelevant because the plan
   identity comes from the XML. */
IF @DatabaseName IS NOT NULL
   AND @QueryPlanHash  IS NULL
   AND @QueryHash      IS NULL
   AND @StoredProcName IS NULL
   AND @CompareToXML   IS NULL
BEGIN
    RAISERROR('@DatabaseName cannot stand alone. Supply one of @QueryPlanHash, @QueryHash, or @StoredProcName alongside it.', 16, 1);
    RETURN;
END;

IF @CompareToXML IS NOT NULL AND @LinkedServer IS NOT NULL
BEGIN
    RAISERROR('@CompareToXML and @LinkedServer are mutually exclusive. Pick one.', 16, 1);
    RETURN;
END;

IF @LinkedServer IS NOT NULL
   AND @QueryPlanHash  IS NULL
   AND @QueryHash      IS NULL
   AND @StoredProcName IS NULL
BEGIN
    RAISERROR('@LinkedServer requires one of @QueryPlanHash, @QueryHash, or @StoredProcName so we know which plan to look up on both sides.', 16, 1);
    RETURN;
END;

IF @LinkedServer IS NOT NULL
BEGIN
    IF @EngineEdition IN (5, 8)
    BEGIN
        RAISERROR('Linked servers are not supported on Azure SQL DB or Synapse. Use @CompareToXML mode instead.', 16, 1);
        RETURN;
    END;

    IF NOT EXISTS (SELECT 1 FROM sys.servers WHERE QUOTENAME([name]) = @LinkedServer OR [name] = @LinkedServer)
    BEGIN
        RAISERROR('Linked server %s was not found in sys.servers. Configure it (with RPC OUT) and try again.', 16, 1, @LinkedServer);
        RETURN;
    END;
END;

IF @CompareToXML IS NOT NULL
    SET @Mode = 2;
ELSE IF @LinkedServer IS NOT NULL
    SET @Mode = 3;
ELSE
    SET @Mode = 1;

IF @Debug = 1
    RAISERROR('Operating in mode %d (1=emit XML, 2=compare XML, 3=linked server)', 0, 1, @Mode) WITH NOWAIT;

/* Mode 2: extract the QUERY hash from the XML (not the plan hash). The whole point of
   comparing is that the plan may be DIFFERENT on this server — that's the symptom we're
   investigating. So we look up THIS server's plan by query_hash and let the optimizer's
   chosen plan_hash come out of the cache however it got there.

   The existing "look up by query_plan_hash, fall back to query_hash" flow handles this
   cleanly: we stuff the query_hash into @QueryPlanHash, the first lookup (by plan_hash)
   returns 0 rows, and the fallback resolves by query_hash.

   If the user ALSO passes @QueryPlanHash explicitly (e.g. to disambiguate when this
   server has multiple plans for the same query_hash), we honor that and skip the XML
   extraction. */
IF @Mode = 2
BEGIN
    SET @QueryHashBin = CONVERT(BINARY(8),
        @CompareToXML.value('(/BlitzPlanCompareSnapshot/@QueryHash)[1]', 'NVARCHAR(50)'), 1);

    IF @QueryHashBin IS NULL
    BEGIN
        RAISERROR('@CompareToXML is missing the @QueryHash attribute on its root element. Was it produced by sp_BlitzPlanCompare?', 16, 1);
        RETURN;
    END;

    IF @QueryPlanHash IS NULL
        SET @QueryPlanHash = @QueryHashBin;
END;

/* =====================================================================
   Resolve @StoredProcName -> object_id, optionally scoped by @DatabaseName.

   Accepts bare ("usp_Foo"), schema-qualified ("dbo.usp_Foo"), and three-part
   ("[db].[schema].[usp_Foo]") names. OBJECT_ID() handles all three forms,
   including bracketed identifiers with reserved names.

   @DatabaseName scoping uses a USE + OBJECT_ID in dynamic SQL so we can
   resolve a proc in a non-current database on box SQL / MI. On Azure SQL DB
   sessions are single-DB so we reject mismatches up front.

   If resolution fails we RAISERROR with a clear message; don't silently
   fall through with @ResolvedProcObjectId = NULL because the plan lookup
   would then return every plan in cache. A missing proc is an error.
   ===================================================================== */
DECLARE @ResolvedProcObjectId INT = NULL;

IF @StoredProcName IS NOT NULL
BEGIN
    /* RAISERROR can't take function calls as args; capture DB_NAME() into a variable. */
    DECLARE @CurrentDbName SYSNAME = DB_NAME();

    /* Azure SQL DB can't do cross-DB OBJECT_ID lookups. Reject mismatches early. */
    IF @EngineEdition IN (5, 8)
       AND @DatabaseName IS NOT NULL
       AND @DatabaseName <> @CurrentDbName
    BEGIN
        RAISERROR('On Azure SQL DB / Synapse, @DatabaseName (%s) must match the current database (%s) - cross-database OBJECT_ID lookups are not supported. Reconnect to the target database and re-run.',
                  16, 1, @DatabaseName, @CurrentDbName);
        RETURN;
    END;

    IF @DatabaseName IS NULL OR @DatabaseName = @CurrentDbName
    BEGIN
        /* Current-DB resolution. OBJECT_ID parses bare / 2-part / 3-part. */
        SET @ResolvedProcObjectId = OBJECT_ID(@StoredProcName);
    END
    ELSE
    BEGIN
        /* Cross-DB resolution (box SQL, MI). USE [db]; SELECT OBJECT_ID(name);
           using sp_executesql so we can get the int back cleanly. */
        DECLARE @objidSql NVARCHAR(MAX) = N'USE ' + QUOTENAME(@DatabaseName) + N';
            SET @id = OBJECT_ID(@name);';
        BEGIN TRY
            EXEC sp_executesql @objidSql,
                N'@name NVARCHAR(400), @id INT OUTPUT',
                @name = @StoredProcName,
                @id   = @ResolvedProcObjectId OUTPUT;
        END TRY
        BEGIN CATCH
            SET @ErrMessage = ERROR_MESSAGE();
            RAISERROR('Failed to USE database %s for @StoredProcName resolution: %s',
                      16, 1, @DatabaseName, @ErrMessage);
            RETURN;
        END CATCH;
    END;

    IF @ResolvedProcObjectId IS NULL
    BEGIN
        DECLARE @ResolvedInDb SYSNAME = ISNULL(@DatabaseName, @CurrentDbName);
        /* Common case: user called master.dbo.sp_BlitzPlanCompare from a user
           database, but OBJECT_ID() ran in master's context and didn't find
           the proc there. Give them two ways out: pass @DatabaseName, or use
           a 3-part name like [db].[schema].[proc]. */
        RAISERROR('Could not resolve @StoredProcName ''%s'' to an object_id in database %s. Either (a) add @DatabaseName = ''YourDb'', or (b) pass a 3-part name like [YourDb].[dbo].[%s]. Also verify spelling and VIEW DEFINITION permission.',
                  16, 1, @StoredProcName, @ResolvedInDb, @StoredProcName);
        RETURN;
    END;

    IF @Debug = 1
    BEGIN
        DECLARE @DebugDbName SYSNAME = ISNULL(@DatabaseName, @CurrentDbName);
        RAISERROR('@StoredProcName ''%s'' resolved to object_id %d in database %s.',
                  0, 1, @StoredProcName, @ResolvedProcObjectId, @DebugDbName) WITH NOWAIT;
    END;
END;

/* =====================================================================
   Resolve the plan from the local plan cache.

   We prefer the actual execution plan from sys.dm_exec_query_plan_stats when it's available
   (SQL 2019+ / Azure SQL DB and LAST_QUERY_PLAN_STATS enabled on the database), because the
   actual plan carries runtime info: memory grant actually used, spills that actually fired,
   wait stats per operator, and accurate row counts. We fall back to the cached estimated plan
   from sys.dm_exec_query_plan otherwise.

   Detection happens via sys.all_objects so we don't reference the function directly on older
   versions where it doesn't exist (which would fail at parse time). When detected, the plan
   resolution is built as dynamic SQL with a conditional OUTER APPLY. When not, the static
   query path is used.
   ===================================================================== */
IF OBJECT_ID('tempdb..#PlanMatches') IS NOT NULL DROP TABLE #PlanMatches;
CREATE TABLE #PlanMatches (
    plan_handle           VARBINARY(64) NOT NULL,
    sql_handle            VARBINARY(64) NOT NULL,
    statement_start_offset INT          NOT NULL,
    statement_end_offset  INT           NOT NULL,
    creation_time         DATETIME      NULL,
    last_execution_time   DATETIME      NULL,
    execution_count       BIGINT        NULL,
    database_name         SYSNAME       NULL,
    set_options           INT           NULL,
    query_text_snippet    NVARCHAR(500) NULL,
    query_plan_full       XML           NULL, /* always from sys.dm_exec_query_plan - FULL plan with all object refs. Used for #PlanObjects extraction. */
    query_plan_actual     XML           NULL, /* from sys.dm_exec_query_plan_stats if LAST_QUERY_PLAN_STATS is on - may be statement-scoped fragment with runtime info. Used for snapshot embed if present. */
    plan_source           NVARCHAR(50)  NULL, /* 'Actual (LAST_QUERY_PLAN_STATS)' or 'Cached estimated' - tells us which got used for the embed */
    query_plan_hash       BINARY(8)     NULL, /* captured so we can count DISTINCT plans after a query_hash fallback */
    query_hash            BINARY(8)     NULL
);

DECLARE @HasPlanStatsDmv BIT =
    CASE WHEN EXISTS (SELECT 1 FROM sys.all_objects
                       WHERE [name] = 'dm_exec_query_plan_stats'
                         AND [schema_id] = SCHEMA_ID('sys'))
         THEN 1 ELSE 0 END;

/* Build the WHERE clause from whichever identifiers the user supplied. AND-combined.
   Collation applied to @DatabaseName match because DB_NAME() returns the catalog
   collation which may not match the caller's default. */
DECLARE @Predicate NVARCHAR(MAX) = N'qp.query_plan IS NOT NULL';

IF @QueryPlanHash IS NOT NULL
    SET @Predicate = @Predicate + N' AND qs.query_plan_hash = @qph';
IF @QueryHash IS NOT NULL
    SET @Predicate = @Predicate + N' AND qs.query_hash = @qh';
IF @ResolvedProcObjectId IS NOT NULL
    SET @Predicate = @Predicate + N' AND st.objectid = @objid';
IF @DatabaseName IS NOT NULL
    SET @Predicate = @Predicate + N' AND DB_NAME(CONVERT(INT, pa_db.value)) COLLATE DATABASE_DEFAULT = @dbname COLLATE DATABASE_DEFAULT';

DECLARE @PlanLookupSql NVARCHAR(MAX) = N'
INSERT INTO #PlanMatches (plan_handle, sql_handle, statement_start_offset, statement_end_offset,
                          creation_time, last_execution_time, execution_count,
                          database_name, set_options, query_text_snippet,
                          query_plan_full, query_plan_actual, plan_source,
                          query_plan_hash, query_hash)
SELECT  qs.plan_handle,
        qs.sql_handle,
        qs.statement_start_offset,
        qs.statement_end_offset,
        qs.creation_time,
        qs.last_execution_time,
        qs.execution_count,
        DB_NAME(CONVERT(INT, pa_db.value)),
        CONVERT(INT, pa_set.value),
        SUBSTRING(st.text,
                  qs.statement_start_offset / 2 + 1,
                  ((CASE qs.statement_end_offset WHEN -1 THEN DATALENGTH(st.text)
                                                  ELSE qs.statement_end_offset END
                    - qs.statement_start_offset) / 2) + 1),
        qp.query_plan,
        '
      + CASE WHEN @HasPlanStatsDmv = 1
             THEN N'qps.query_plan'
             ELSE N'CAST(NULL AS XML)' END
      + N',
        '
      + CASE WHEN @HasPlanStatsDmv = 1
             THEN N'CASE WHEN qps.query_plan IS NOT NULL THEN N''Actual (LAST_QUERY_PLAN_STATS)''
                         WHEN qp.query_plan  IS NOT NULL THEN N''Cached estimated''
                         ELSE N''(none)'' END'
             ELSE N'CASE WHEN qp.query_plan IS NOT NULL THEN N''Cached estimated''
                         ELSE N''(none)'' END' END
      + N',
        qs.query_plan_hash,
        qs.query_hash
FROM    sys.dm_exec_query_stats AS qs
CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) AS st
OUTER APPLY sys.dm_exec_query_plan(qs.plan_handle) AS qp
'     + CASE WHEN @HasPlanStatsDmv = 1
             THEN N'OUTER APPLY sys.dm_exec_query_plan_stats(qs.plan_handle) AS qps'
             ELSE N'' END
      + N'
OUTER APPLY (SELECT TOP 1 value FROM sys.dm_exec_plan_attributes(qs.plan_handle) WHERE attribute = ''dbid'')        AS pa_db
OUTER APPLY (SELECT TOP 1 value FROM sys.dm_exec_plan_attributes(qs.plan_handle) WHERE attribute = ''set_options'') AS pa_set
WHERE   ' + @Predicate + N'
OPTION (RECOMPILE);';

IF @Debug = 1
BEGIN
    /* RAISERROR's %d substitution parameter rejects BIT (Msg 2748). CAST to INT first. */
    DECLARE @HasPlanStatsDmvInt INT = CAST(@HasPlanStatsDmv AS INT);
    RAISERROR('Plan-lookup dynamic SQL (HasPlanStatsDmv=%d):', 0, 1, @HasPlanStatsDmvInt) WITH NOWAIT;
END;
IF @Debug = 1
    PRINT @PlanLookupSql;

EXEC sp_executesql @PlanLookupSql,
    N'@qph BINARY(8), @qh BINARY(8), @objid INT, @dbname SYSNAME',
    @qph    = @QueryPlanHash,
    @qh     = @QueryHash,
    @objid  = @ResolvedProcObjectId,
    @dbname = @DatabaseName;

SELECT @MatchCount = COUNT(*) FROM #PlanMatches;

/* RAISERROR accepts only variables/literals for parameter markers, not function-call expressions,
   so pre-compute the hex representation. */
DECLARE @QueryPlanHashText NVARCHAR(50) = CONVERT(NVARCHAR(50), @QueryPlanHash, 1);
DECLARE @InputHashText      NVARCHAR(50) = @QueryPlanHashText;  /* preserve original input for messages */
DECLARE @MatchedByQueryHash BIT          = 0;

/* Fallback: if the ONLY identifier supplied was @QueryPlanHash and it matched
   nothing, retry as if the user had passed @QueryHash. This preserves the
   historical forgiving behavior for people who copy/paste hashes without
   knowing which one they grabbed. We skip the fallback if any other identifier
   was supplied because AND-ing the rest would still narrow correctly. */
IF @MatchCount = 0
   AND @QueryPlanHash      IS NOT NULL
   AND @QueryHash          IS NULL
   AND @ResolvedProcObjectId IS NULL
BEGIN
    DECLARE @PlanLookupSqlByQueryHash NVARCHAR(MAX) =
        REPLACE(@PlanLookupSql,
                N'qs.query_plan_hash = @qph',
                N'qs.query_hash = @qph');

    IF @Debug = 1
        RAISERROR('No match on query_plan_hash. Retrying the passed hash as query_hash...', 0, 1) WITH NOWAIT;

    EXEC sp_executesql @PlanLookupSqlByQueryHash,
        N'@qph BINARY(8), @qh BINARY(8), @objid INT, @dbname SYSNAME',
        @qph    = @QueryPlanHash,
        @qh     = @QueryHash,
        @objid  = @ResolvedProcObjectId,
        @dbname = @DatabaseName;
    SELECT @MatchCount = COUNT(*) FROM #PlanMatches;

    IF @MatchCount > 0
    BEGIN
        DECLARE @DistinctPlanHashes INT;
        SELECT @DistinctPlanHashes = COUNT(DISTINCT query_plan_hash) FROM #PlanMatches;

        IF @DistinctPlanHashes > 1
        BEGIN
            RAISERROR('We didn''t find %s in cache as a QueryPlanHash, but we did find it as a QueryHash. However, it has multiple plans in cache. Pass in the QueryPlanHash you want to investigate. The distinct QueryPlanHash values are returned below.',
                      16, 1, @InputHashText);

            SELECT DISTINCT
                   query_plan_hash                                     AS QueryPlanHash,
                   CONVERT(NVARCHAR(50), query_plan_hash, 1)           AS QueryPlanHashText,
                   COUNT(*) OVER (PARTITION BY query_plan_hash)        AS CachedCopies,
                   MIN(query_text_snippet) OVER (PARTITION BY query_plan_hash) AS QueryTextSnippet
            FROM   #PlanMatches
            ORDER BY query_plan_hash;
            RETURN;
        END;

        /* Exactly 1 distinct plan under that query_hash. Adopt its query_plan_hash and
           let the normal single-plan / multi-copy handling take over below. */
        SELECT TOP 1 @QueryPlanHash = query_plan_hash FROM #PlanMatches;
        SET @QueryPlanHashText  = CONVERT(NVARCHAR(50), @QueryPlanHash, 1);
        SET @MatchedByQueryHash = 1;

        RAISERROR('Note: %s was not found as a QueryPlanHash, but matched as a QueryHash. Using its single cached plan (QueryPlanHash = %s).',
                  0, 1, @InputHashText, @QueryPlanHashText) WITH NOWAIT;
    END;
END;

/* Final "nothing found" check. Build a descriptive message that lists every
   identifier the user passed so they can see what was tried. */
IF @MatchCount = 0
BEGIN
    DECLARE @TriedList NVARCHAR(MAX) = N'';
    IF @QueryPlanHash IS NOT NULL
        SET @TriedList = @TriedList + N' @QueryPlanHash=' + CONVERT(NVARCHAR(50), @QueryPlanHash, 1);
    IF @QueryHash IS NOT NULL
        SET @TriedList = @TriedList + N' @QueryHash=' + CONVERT(NVARCHAR(50), @QueryHash, 1);
    IF @StoredProcName IS NOT NULL
        SET @TriedList = @TriedList + N' @StoredProcName=' + @StoredProcName;
    IF @DatabaseName IS NOT NULL
        SET @TriedList = @TriedList + N' @DatabaseName=' + @DatabaseName;

    RAISERROR('No cached plan matched the identifiers you passed:%s. The plan may have aged out of cache, or one of these may be wrong. If you supplied @QueryPlanHash and the values might actually be a query_hash, try @QueryHash instead.',
              16, 1, @TriedList);
    RETURN;
END;

IF @MatchCount > 1
BEGIN
    RAISERROR('Multiple cached plans match the identifiers you passed. The matches are returned below; inspect set_options + query_text_snippet, then re-run with a more specific @QueryPlanHash (or supply additional narrowers like @DatabaseName).',
              11, 1);

    SELECT  plan_handle, sql_handle, statement_start_offset, statement_end_offset,
            creation_time, last_execution_time, execution_count,
            database_name, set_options, query_text_snippet, plan_source,
            CONVERT(NVARCHAR(50), query_plan_hash, 1) AS query_plan_hash_text,
            CONVERT(NVARCHAR(50), query_hash, 1)      AS query_hash_text,
            query_plan_full AS query_plan
    FROM    #PlanMatches
    ORDER BY execution_count DESC;
    RETURN;
END;

DECLARE @PlanSource      NVARCHAR(50);
DECLARE @PlanXmlForEmit  XML;  /* actual plan if available, else full estimated; what we embed in the snapshot */

SELECT  @PlanHandle      = plan_handle,
        @SqlHandle       = sql_handle,
        @StmtStartOffset = statement_start_offset,
        @StmtEndOffset   = statement_end_offset,
        @PlanXml         = query_plan_full,               /* FULL plan - used for #PlanObjects extraction */
        @PlanXmlForEmit  = COALESCE(query_plan_actual, query_plan_full),
        @PlanSource      = plan_source,
        @QueryPlanHash   = query_plan_hash, /* may have been NULL if user passed @QueryHash / @StoredProcName only */
        @QueryHashBin    = query_hash    /* overwrite any mode-2 XML-extracted value with the real resolved plan's query_hash */
FROM    #PlanMatches;

IF @Debug = 1
    RAISERROR('Plan resolved. plan_source = %s', 0, 1, @PlanSource) WITH NOWAIT;

/* =====================================================================
   Parse the plan XML into temp tables describing what the plan touches
   ===================================================================== */
IF OBJECT_ID('tempdb..#PlanObjects')    IS NOT NULL DROP TABLE #PlanObjects;
IF OBJECT_ID('tempdb..#PlanStatsUsed')  IS NOT NULL DROP TABLE #PlanStatsUsed;
IF OBJECT_ID('tempdb..#PlanIndexesRef') IS NOT NULL DROP TABLE #PlanIndexesRef;
IF OBJECT_ID('tempdb..#PlanAttributes') IS NOT NULL DROP TABLE #PlanAttributes;
IF OBJECT_ID('tempdb..#PlanWarnings')   IS NOT NULL DROP TABLE #PlanWarnings;
IF OBJECT_ID('tempdb..#PlanParameters') IS NOT NULL DROP TABLE #PlanParameters;

CREATE TABLE #PlanObjects (
    DatabaseName SYSNAME      NULL,
    SchemaName   SYSNAME      NULL,
    TableName    SYSNAME      NULL,
    Source       VARCHAR(20)  NULL  /* 'Object', 'ColumnReference', 'StatsUsed' */
);

CREATE TABLE #PlanIndexesRef (
    DatabaseName SYSNAME      NULL,
    SchemaName   SYSNAME      NULL,
    TableName    SYSNAME      NULL,
    IndexName    SYSNAME      NULL,
    IndexKind    NVARCHAR(50) NULL
);

CREATE TABLE #PlanStatsUsed (
    DatabaseName     SYSNAME      NULL,
    SchemaName       SYSNAME      NULL,
    TableName        SYSNAME      NULL,
    StatisticsName   SYSNAME      NULL,
    ModificationCount BIGINT      NULL,
    SamplingPercent  DECIMAL(9,4) NULL
);

CREATE TABLE #PlanAttributes (
    AttributeName  VARCHAR(100)   NOT NULL,
    AttributeValue NVARCHAR(4000) NULL
);

CREATE TABLE #PlanWarnings (
    WarningName    VARCHAR(100)   NOT NULL,
    WarningDetails NVARCHAR(4000) NULL
);

CREATE TABLE #PlanParameters (
    ParameterName        SYSNAME        NOT NULL,
    DataType             NVARCHAR(128)  NULL,
    CompiledValue        NVARCHAR(4000) NULL,
    RuntimeValue         NVARCHAR(4000) NULL
);

/* Objects directly named by RelOps */
;WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
INSERT INTO #PlanObjects (DatabaseName, SchemaName, TableName, Source)
SELECT DISTINCT
       PARSENAME(o.n.value('@Database', 'NVARCHAR(258)'), 1),
       PARSENAME(o.n.value('@Schema',   'NVARCHAR(258)'), 1),
       PARSENAME(o.n.value('@Table',    'NVARCHAR(258)'), 1),
       'Object'
FROM   @PlanXml.nodes('//p:RelOp//p:Object') AS o(n)
WHERE  o.n.value('@Database', 'NVARCHAR(258)') LIKE '[[]%'
  AND  o.n.value('@Table',    'NVARCHAR(258)') IS NOT NULL;

/* Indexes named on those objects (used to seed #PlanIndexesRef) */
;WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
INSERT INTO #PlanIndexesRef (DatabaseName, SchemaName, TableName, IndexName, IndexKind)
SELECT DISTINCT
       PARSENAME(o.n.value('@Database', 'NVARCHAR(258)'), 1),
       PARSENAME(o.n.value('@Schema',   'NVARCHAR(258)'), 1),
       PARSENAME(o.n.value('@Table',    'NVARCHAR(258)'), 1),
       PARSENAME(o.n.value('@Index',    'NVARCHAR(258)'), 1),
       o.n.value('@IndexKind', 'NVARCHAR(50)')
FROM   @PlanXml.nodes('//p:RelOp//p:Object') AS o(n)
WHERE  o.n.value('@Database', 'NVARCHAR(258)') LIKE '[[]%'
  AND  o.n.value('@Index',    'NVARCHAR(258)') IS NOT NULL;

/* Tables referenced only via columns (e.g. as join partners on the read side) */
;WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
INSERT INTO #PlanObjects (DatabaseName, SchemaName, TableName, Source)
SELECT DISTINCT
       PARSENAME(c.n.value('@Database', 'NVARCHAR(258)'), 1),
       PARSENAME(c.n.value('@Schema',   'NVARCHAR(258)'), 1),
       PARSENAME(c.n.value('@Table',    'NVARCHAR(258)'), 1),
       'ColumnReference'
FROM   @PlanXml.nodes('//p:ColumnReference[@Database]') AS c(n)
WHERE  c.n.value('@Database', 'NVARCHAR(258)') LIKE '[[]%'
  AND  c.n.value('@Table',    'NVARCHAR(258)') IS NOT NULL
  AND  NOT EXISTS (
        SELECT 1 FROM #PlanObjects po
        WHERE po.DatabaseName = PARSENAME(c.n.value('@Database', 'NVARCHAR(258)'), 1)
          AND po.SchemaName   = PARSENAME(c.n.value('@Schema',   'NVARCHAR(258)'), 1)
          AND po.TableName    = PARSENAME(c.n.value('@Table',    'NVARCHAR(258)'), 1));

/* Statistics used by the optimizer for this plan (high-signal) */
;WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
INSERT INTO #PlanStatsUsed (DatabaseName, SchemaName, TableName, StatisticsName, ModificationCount, SamplingPercent)
SELECT DISTINCT
       PARSENAME(s.n.value('@Database',   'NVARCHAR(258)'), 1),
       PARSENAME(s.n.value('@Schema',     'NVARCHAR(258)'), 1),
       PARSENAME(s.n.value('@Table',      'NVARCHAR(258)'), 1),
       PARSENAME(s.n.value('@Statistics', 'NVARCHAR(258)'), 1),
       s.n.value('@ModificationCount', 'BIGINT'),
       s.n.value('@SamplingPercent',   'DECIMAL(9,4)')
FROM   @PlanXml.nodes('//p:StatsUsed/p:Stats') AS s(n)
WHERE  s.n.value('@Database', 'NVARCHAR(258)') LIKE '[[]%';

/* Make sure the StatsUsed objects are in #PlanObjects too */
INSERT INTO #PlanObjects (DatabaseName, SchemaName, TableName, Source)
SELECT DISTINCT su.DatabaseName, su.SchemaName, su.TableName, 'StatsUsed'
FROM   #PlanStatsUsed su
WHERE  NOT EXISTS (
        SELECT 1 FROM #PlanObjects po
        WHERE po.DatabaseName = su.DatabaseName
          AND po.SchemaName   = su.SchemaName
          AND po.TableName    = su.TableName);

/* Plan-level attributes worth diffing */
;WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
INSERT INTO #PlanAttributes (AttributeName, AttributeValue)
SELECT 'StatementOptmLevel',
       @PlanXml.value('(//p:StmtSimple/@StatementOptmLevel)[1]', 'NVARCHAR(50)')
UNION ALL SELECT 'CardinalityEstimationModelVersion',
       @PlanXml.value('(//p:StmtSimple/@CardinalityEstimationModelVersion)[1]', 'NVARCHAR(50)')
UNION ALL SELECT 'RetrievedFromCache',
       @PlanXml.value('(//p:StmtSimple/@RetrievedFromCache)[1]', 'NVARCHAR(50)')
UNION ALL SELECT 'StatementSubTreeCost',
       @PlanXml.value('(//p:StmtSimple/@StatementSubTreeCost)[1]', 'NVARCHAR(50)')
UNION ALL SELECT 'DegreeOfParallelism',
       @PlanXml.value('(//p:QueryPlan/@DegreeOfParallelism)[1]', 'NVARCHAR(50)')
UNION ALL SELECT 'MemoryGrant',
       @PlanXml.value('(//p:QueryPlan/@MemoryGrant)[1]', 'NVARCHAR(50)')
UNION ALL SELECT 'CompileTime',
       @PlanXml.value('(//p:QueryPlan/@CompileTime)[1]', 'NVARCHAR(50)')
/* CompileCPU, CompileMemory, and CachedPlanSize intentionally omitted - low signal,
   clutters the diff. CompileTime stays because a slow compile is a real symptom. */
UNION ALL SELECT 'EstimatedAvailableMemoryGrant',
       @PlanXml.value('(//p:OptimizerHardwareDependentProperties/@EstimatedAvailableMemoryGrant)[1]', 'NVARCHAR(50)')
UNION ALL SELECT 'EstimatedPagesCached',
       @PlanXml.value('(//p:OptimizerHardwareDependentProperties/@EstimatedPagesCached)[1]', 'NVARCHAR(50)')
UNION ALL SELECT 'EstimatedAvailableDegreeOfParallelism',
       @PlanXml.value('(//p:OptimizerHardwareDependentProperties/@EstimatedAvailableDegreeOfParallelism)[1]', 'NVARCHAR(50)')
UNION ALL SELECT 'MaxCompileMemory',
       @PlanXml.value('(//p:OptimizerHardwareDependentProperties/@MaxCompileMemory)[1]', 'NVARCHAR(50)');

/* Record which plan source we resolved (actual vs cached) so the diff surfaces it. */
INSERT INTO #PlanAttributes (AttributeName, AttributeValue) VALUES ('PlanSource', @PlanSource);

/* =====================================================================
   Runtime totals from the actual plan (LAST_QUERY_PLAN_STATS).
   We compare TOTALS only (not per-operator stats), since operator layouts differ
   between plans. These come from @PlanXmlForEmit (the actual plan if present, else
   the full estimated - in which case these XPaths return NULL and get filtered). */
IF OBJECT_ID('tempdb..#PlanRuntime') IS NOT NULL DROP TABLE #PlanRuntime;
CREATE TABLE #PlanRuntime (
    [Setting]    VARCHAR(100)   NOT NULL,
    ValueText    NVARCHAR(100)  NULL,
    ValueNumeric DECIMAL(38,4)  NULL
);

/* Microsoft quirk: <QueryTimeStats> attribute VALUES are swapped relative to
   the attribute names in the plan XML that sys.dm_exec_query_plan_stats
   produces. Empirically verified against sys.dm_exec_query_stats
   (total_worker_time / total_elapsed_time) on single-execution plans:

       @CpuTime attribute     actually holds the elapsed-time value
       @ElapsedTime attribute actually holds the cpu-time value

   So we SWAP at read time: our ElapsedTimeMs output reads from @CpuTime,
   and our CpuTimeMs output reads from @ElapsedTime. The user-facing labels
   stay correct; only the XPath sources are flipped. The same swap applies
   to the Udf variants. */
;WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
INSERT INTO #PlanRuntime ([Setting], ValueText, ValueNumeric)
SELECT 'ElapsedTimeMs', CAST(v AS NVARCHAR(40)), v
FROM (SELECT @PlanXmlForEmit.value('(//p:QueryTimeStats/@CpuTime)[1]', 'BIGINT') AS v) x
WHERE v IS NOT NULL;

;WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
INSERT INTO #PlanRuntime ([Setting], ValueText, ValueNumeric)
SELECT 'CpuTimeMs', CAST(v AS NVARCHAR(40)), v
FROM (SELECT @PlanXmlForEmit.value('(//p:QueryTimeStats/@ElapsedTime)[1]', 'BIGINT') AS v) x
WHERE v IS NOT NULL;

;WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
INSERT INTO #PlanRuntime ([Setting], ValueText, ValueNumeric)
SELECT 'UdfElapsedTimeMs', CAST(v AS NVARCHAR(40)), v
FROM (SELECT @PlanXmlForEmit.value('(//p:QueryTimeStats/@UdfCpuTime)[1]', 'BIGINT') AS v) x
WHERE v IS NOT NULL;

;WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
INSERT INTO #PlanRuntime ([Setting], ValueText, ValueNumeric)
SELECT 'UdfCpuTimeMs', CAST(v AS NVARCHAR(40)), v
FROM (SELECT @PlanXmlForEmit.value('(//p:QueryTimeStats/@UdfElapsedTime)[1]', 'BIGINT') AS v) x
WHERE v IS NOT NULL;

;WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
INSERT INTO #PlanRuntime ([Setting], ValueText, ValueNumeric)
SELECT 'GrantedMemoryKB', CAST(v AS NVARCHAR(40)), v
FROM (SELECT @PlanXmlForEmit.value('(//p:MemoryGrantInfo/@GrantedMemory)[1]', 'BIGINT') AS v) x
WHERE v IS NOT NULL;

;WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
INSERT INTO #PlanRuntime ([Setting], ValueText, ValueNumeric)
SELECT 'MaxUsedMemoryKB', CAST(v AS NVARCHAR(40)), v
FROM (SELECT @PlanXmlForEmit.value('(//p:MemoryGrantInfo/@MaxUsedMemory)[1]', 'BIGINT') AS v) x
WHERE v IS NOT NULL;

/* Spills: count of <SpillOccurred> elements in the actual plan (runtime-only warning).
   Reported regardless - 0 vs 0 matches as "Same" and suppressed; any difference surfaces. */
;WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
INSERT INTO #PlanRuntime ([Setting], ValueText, ValueNumeric)
SELECT 'SpillCount', CAST(v AS NVARCHAR(20)), v
FROM (SELECT @PlanXmlForEmit.value('count(//p:Warnings/p:SpillOccurred)', 'INT') AS v) x
WHERE @PlanXmlForEmit IS NOT NULL;

/* Per-wait-type runtime waits. One row per wait type encountered by the query.
   SubjectKey = wait type so the diff engine matches PAGEIOLATCH_SH vs PAGEIOLATCH_SH
   across servers. */
IF OBJECT_ID('tempdb..#PlanWait') IS NOT NULL DROP TABLE #PlanWait;
CREATE TABLE #PlanWait (
    WaitType   VARCHAR(80)   NOT NULL,
    WaitTimeMs BIGINT        NULL,
    WaitCount  BIGINT        NULL
);

/* XML-based wait-stats extraction from the plan cache. sys.dm_exec_query_plan_stats
   does NOT populate <WaitStats> - only SET STATISTICS XML ON output does. So this
   INSERT is a fallback for users who pass STATISTICS XML into @CompareToXML. The
   primary source is Query Store (populated inside the per-DB cursor below). */
;WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
INSERT INTO #PlanWait (WaitType, WaitTimeMs, WaitCount)
SELECT w.n.value('@WaitType',   'VARCHAR(80)'),
       w.n.value('@WaitTimeMs', 'BIGINT'),
       w.n.value('@WaitCount',  'BIGINT')
FROM   @PlanXmlForEmit.nodes('//p:WaitStats/p:Wait') AS w(n);

/* Plan warnings (one row per warning element under //p:Warnings) */
;WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
INSERT INTO #PlanWarnings (WarningName, WarningDetails)
SELECT w.n.value('local-name(.)', 'VARCHAR(100)'),
       LEFT(CAST(w.n.query('.') AS NVARCHAR(MAX)), 4000)
FROM   @PlanXml.nodes('//p:Warnings/*') AS w(n);

/* Parameters: compiled vs runtime values (parameter sniffing).
   DISTINCT because a stored proc's plan has one <ParameterList> per statement, and each
   statement that references a parameter lists it again - so a 2-statement proc using both
   @a and @b produces 4 rows from the XPath. They're identical per parameter, so dedup. */
;WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
INSERT INTO #PlanParameters (ParameterName, DataType, CompiledValue, RuntimeValue)
SELECT DISTINCT
       pr.n.value('@Column',                  'SYSNAME'),
       pr.n.value('@ParameterDataType',       'NVARCHAR(128)'),
       pr.n.value('@ParameterCompiledValue',  'NVARCHAR(4000)'),
       pr.n.value('@ParameterRuntimeValue',   'NVARCHAR(4000)')
FROM   @PlanXml.nodes('//p:ParameterList/p:ColumnReference') AS pr(n);

/* =====================================================================
   Build the local snapshot
   ===================================================================== */
IF OBJECT_ID('tempdb..#LocalSnapshot') IS NOT NULL DROP TABLE #LocalSnapshot;
CREATE TABLE #LocalSnapshot (
    Category     VARCHAR(50)    NOT NULL,
    Setting      VARCHAR(200)   NOT NULL,
    SubjectKey   NVARCHAR(776)  NULL,
    ValueText    NVARCHAR(MAX)  NULL,
    ValueNumeric DECIMAL(38,4)  NULL
);

/* ----- Category: Server ----- */
INSERT INTO #LocalSnapshot (Category, Setting, SubjectKey, ValueText)
SELECT 'Server', 'MachineName',     NULL, CAST(SERVERPROPERTY('MachineName')     AS NVARCHAR(256)) UNION ALL
SELECT 'Server', 'ServerName',      NULL, CAST(SERVERPROPERTY('ServerName')      AS NVARCHAR(256)) UNION ALL
SELECT 'Server', 'EngineEdition',   NULL, CAST(@EngineEdition                    AS NVARCHAR(10))  UNION ALL
SELECT 'Server', 'ProductVersion',  NULL, CAST(SERVERPROPERTY('ProductVersion')  AS NVARCHAR(128)) UNION ALL
SELECT 'Server', 'ProductLevel',    NULL, CAST(SERVERPROPERTY('ProductLevel')    AS NVARCHAR(128)) UNION ALL
SELECT 'Server', 'EditionText',     NULL, CAST(SERVERPROPERTY('Edition')         AS NVARCHAR(128)) UNION ALL
SELECT 'Server', 'IsHadrEnabled',   NULL, CAST(ISNULL(SERVERPROPERTY('IsHadrEnabled'), 0) AS NVARCHAR(10)) UNION ALL
SELECT 'Server', 'Collation',       NULL, CAST(SERVERPROPERTY('Collation')       AS NVARCHAR(128)) UNION ALL
SELECT 'Server', 'IsClustered',     NULL, CAST(ISNULL(SERVERPROPERTY('IsClustered'), 0)  AS NVARCHAR(10));

/* ----- Category: Hardware ----- */
INSERT INTO #LocalSnapshot (Category, Setting, SubjectKey, ValueText, ValueNumeric)
SELECT 'Hardware', 'CPUCount',           NULL, CAST(cpu_count            AS NVARCHAR(20)), CAST(cpu_count            AS DECIMAL(38,4)) FROM sys.dm_os_sys_info UNION ALL
SELECT 'Hardware', 'SchedulerCount',     NULL, CAST(scheduler_count      AS NVARCHAR(20)), CAST(scheduler_count      AS DECIMAL(38,4)) FROM sys.dm_os_sys_info UNION ALL
SELECT 'Hardware', 'HyperthreadRatio',   NULL, CAST(hyperthread_ratio    AS NVARCHAR(20)), CAST(hyperthread_ratio    AS DECIMAL(38,4)) FROM sys.dm_os_sys_info;

/* physical_memory_kb is unreliable on Azure SQL DB. Skip it on EngineEdition 5/8. */
IF @EngineEdition NOT IN (5, 8)
BEGIN
    INSERT INTO #LocalSnapshot (Category, Setting, SubjectKey, ValueText, ValueNumeric)
    SELECT 'Hardware', 'PhysicalMemoryKB', NULL, CAST(physical_memory_kb AS NVARCHAR(40)), CAST(physical_memory_kb AS DECIMAL(38,4))
    FROM sys.dm_os_sys_info;

    INSERT INTO #LocalSnapshot (Category, Setting, SubjectKey, ValueText, ValueNumeric)
    SELECT 'Hardware', 'CommittedTargetKB', NULL, CAST(committed_target_kb AS NVARCHAR(40)), CAST(committed_target_kb AS DECIMAL(38,4))
    FROM sys.dm_os_sys_info;
END;

/* ----- Category: SpConfigure (curated allowlist) ----- */
IF OBJECT_ID('tempdb..#ConfigAllowlist') IS NOT NULL DROP TABLE #ConfigAllowlist;
CREATE TABLE #ConfigAllowlist ([name] NVARCHAR(70) PRIMARY KEY);
INSERT INTO #ConfigAllowlist VALUES
    ('max degree of parallelism'),
    ('cost threshold for parallelism'),
    ('max server memory (MB)'),
    ('min server memory (MB)'),
    ('optimize for ad hoc workloads'),
    ('priority boost'),
    ('lightweight pooling'),
    ('query governor cost limit'),
    ('max worker threads');

/* sys.configurations exists on Azure SQL DB but is mostly empty. Try anyway. */
BEGIN TRY
    INSERT INTO #LocalSnapshot (Category, Setting, SubjectKey, ValueText, ValueNumeric)
    SELECT 'SpConfigure', c.[name], NULL, CAST(c.value_in_use AS NVARCHAR(40)), CAST(c.value_in_use AS DECIMAL(38,4))
    FROM   sys.configurations c
    JOIN   #ConfigAllowlist a ON a.[name] = c.[name];
END TRY
BEGIN CATCH
    INSERT INTO #LocalSnapshot (Category, Setting, SubjectKey, ValueText)
    VALUES ('SpConfigure', '_unsupported_', NULL, 'sys.configurations is not readable on this engine edition.');
END CATCH;

/* ----- Category: TraceFlag ----- */
IF @EngineEdition IN (2, 3, 4, 6)  /* Box product or Managed Instance */
BEGIN
    IF OBJECT_ID('tempdb..#TraceFlags') IS NOT NULL DROP TABLE #TraceFlags;
    CREATE TABLE #TraceFlags (
        TraceFlag INT, [Status] BIT, [Global] BIT, [Session] BIT
    );
    BEGIN TRY
        INSERT INTO #TraceFlags EXEC ('DBCC TRACESTATUS(-1) WITH NO_INFOMSGS;');

        INSERT INTO #LocalSnapshot (Category, Setting, SubjectKey, ValueText, ValueNumeric)
        SELECT 'TraceFlag', CAST(TraceFlag AS VARCHAR(20)), NULL,
               CASE WHEN [Global] = 1 THEN 'Global' ELSE 'Session' END
               + CASE WHEN [Status] = 1 THEN ':ON' ELSE ':OFF' END,
               TraceFlag
        FROM #TraceFlags;
    END TRY
    BEGIN CATCH
        INSERT INTO #LocalSnapshot (Category, Setting, SubjectKey, ValueText)
        VALUES ('TraceFlag', '_unsupported_', NULL, 'DBCC TRACESTATUS failed: ' + ERROR_MESSAGE());
    END CATCH;
END
ELSE
BEGIN
    INSERT INTO #LocalSnapshot (Category, Setting, SubjectKey, ValueText)
    VALUES ('TraceFlag', '_unsupported_', NULL, 'Trace flags are not exposed on Azure SQL DB / Synapse.');
END;

/* ----- Categories: Database, DatabaseScopedConfig, Object, Index, Statistics, RowCount, ForcedPlan -----
   Per plan-referenced database, drive a dynamic-SQL block. Wrap in TRY/CATCH so an offline
   or inaccessible DB emits a Priority-1 row instead of killing the run.

   Azure SQL DB note: sessions are bound to one user database, and USE [otherDb] raises an error.
   For any plan-referenced DB that isn't the current DB on Azure SQL DB / Synapse, emit a warning
   row and remove it from the iteration so we don't blow up trying to USE it. */
IF @EngineEdition IN (5, 8)
BEGIN
    INSERT INTO #LocalSnapshot (Category, Setting, SubjectKey, ValueText)
    SELECT DISTINCT 'Database', '_unreadable_', po.DatabaseName,
           'Azure SQL DB / Synapse cannot inspect databases other than the connection''s current database. Re-run sp_BlitzPlanCompare from inside [' + po.DatabaseName + '] if you need to compare it.'
    FROM   #PlanObjects po
    WHERE  po.DatabaseName IS NOT NULL
      AND  po.DatabaseName <> DB_NAME();

    DELETE FROM #PlanObjects
    WHERE  DatabaseName IS NOT NULL
      AND  DatabaseName <> DB_NAME();
END;

/* Exclude tempdb from the comparison. Temp tables are session-scoped with suffixes
   like #TopLocations_____________________0000000000E7 baked into the name, so two
   snapshots of the "same" plan will always see different tempdb object names and
   produce dozens of spurious "Missing on X" rows. Emit one informational row so the
   user knows we skipped tempdb, then drop its objects from the iteration. */
IF EXISTS (SELECT 1 FROM #PlanObjects WHERE DatabaseName = 'tempdb')
BEGIN
    INSERT INTO #LocalSnapshot (Category, Setting, SubjectKey, ValueText)
    VALUES ('Database', '_skipped_', 'tempdb',
            'The plan references tempdb objects (e.g. temp tables). Temp object names include a session-specific suffix, so cross-server comparison of tempdb contents is not meaningful. Skipped.');

    DELETE FROM #PlanObjects WHERE DatabaseName = 'tempdb';
END;

DECLARE @CurrentDb SYSNAME;
DECLARE db_cur CURSOR LOCAL FAST_FORWARD READ_ONLY FOR
    SELECT DISTINCT DatabaseName FROM #PlanObjects WHERE DatabaseName IS NOT NULL;

OPEN db_cur;
FETCH NEXT FROM db_cur INTO @CurrentDb;
WHILE @@FETCH_STATUS = 0
BEGIN
    BEGIN TRY
        IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE [name] = @CurrentDb AND state_desc = 'ONLINE')
        BEGIN
            INSERT INTO #LocalSnapshot (Category, Setting, SubjectKey, ValueText)
            VALUES ('Database', '_unreadable_', @CurrentDb,
                    'Database is not ONLINE or not present on this server.');
        END
        ELSE
        BEGIN
            /* Database-level metadata */
            SET @sql = N'
            SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
            INSERT INTO #LocalSnapshot (Category, Setting, SubjectKey, ValueText, ValueNumeric)
            SELECT ''Database'', ''CompatLevel'',           @db, CAST(d.compatibility_level    AS NVARCHAR(20)), d.compatibility_level FROM sys.databases d WHERE d.[name] = @db UNION ALL
            SELECT ''Database'', ''Collation'',             @db, CAST(d.collation_name         AS NVARCHAR(128)), NULL                  FROM sys.databases d WHERE d.[name] = @db UNION ALL
            SELECT ''Database'', ''IsRCSI'',                @db, CAST(d.is_read_committed_snapshot_on AS NVARCHAR(10)), d.is_read_committed_snapshot_on FROM sys.databases d WHERE d.[name] = @db UNION ALL
            SELECT ''Database'', ''IsAutoCreateStats'',     @db, CAST(d.is_auto_create_stats_on AS NVARCHAR(10)), d.is_auto_create_stats_on FROM sys.databases d WHERE d.[name] = @db UNION ALL
            SELECT ''Database'', ''IsAutoUpdateStats'',     @db, CAST(d.is_auto_update_stats_on AS NVARCHAR(10)), d.is_auto_update_stats_on FROM sys.databases d WHERE d.[name] = @db UNION ALL
            SELECT ''Database'', ''IsAutoUpdateStatsAsync'',@db, CAST(d.is_auto_update_stats_async_on AS NVARCHAR(10)), d.is_auto_update_stats_async_on FROM sys.databases d WHERE d.[name] = @db UNION ALL
            SELECT ''Database'', ''PageVerifyOption'',      @db, CAST(d.page_verify_option_desc AS NVARCHAR(60)), NULL FROM sys.databases d WHERE d.[name] = @db UNION ALL
            SELECT ''Database'', ''RecoveryModel'',         @db, CAST(d.recovery_model_desc     AS NVARCHAR(60)), NULL FROM sys.databases d WHERE d.[name] = @db UNION ALL
            SELECT ''Database'', ''IsQueryStoreOn'',        @db, CAST(d.is_query_store_on       AS NVARCHAR(10)), d.is_query_store_on FROM sys.databases d WHERE d.[name] = @db UNION ALL
            SELECT ''Database'', ''ParameterizationOption'',@db, CASE d.is_parameterization_forced WHEN 1 THEN ''FORCED'' ELSE ''SIMPLE'' END, d.is_parameterization_forced FROM sys.databases d WHERE d.[name] = @db UNION ALL
            SELECT ''Database'', ''ContainmentType'',       @db, CAST(d.containment_desc        AS NVARCHAR(60)), NULL FROM sys.databases d WHERE d.[name] = @db;';
            EXEC sp_executesql @sql, N'@db SYSNAME', @db = @CurrentDb;

            /* Database-scoped configurations */
            SET @sql = N'
            USE ' + QUOTENAME(@CurrentDb) + N';
            SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
            INSERT INTO #LocalSnapshot (Category, Setting, SubjectKey, ValueText)
            SELECT ''DatabaseScopedConfig'',
                   dsc.[name],
                   @db,
                   ISNULL(CAST(dsc.value AS NVARCHAR(200)), ''(null)'')
                   + CASE WHEN dsc.value_for_secondary IS NOT NULL
                          THEN '' / secondary='' + CAST(dsc.value_for_secondary AS NVARCHAR(200))
                          ELSE '''' END
            FROM sys.database_scoped_configurations dsc;';
            EXEC sp_executesql @sql, N'@db SYSNAME', @db = @CurrentDb;

            /* Objects: presence row */
            SET @sql = N'
            USE ' + QUOTENAME(@CurrentDb) + N';
            SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
            INSERT INTO #LocalSnapshot (Category, Setting, SubjectKey, ValueText)
            SELECT ''Object'', ''Present'',
                   @db + ''.'' + po.SchemaName + ''.'' + po.TableName,
                   CASE WHEN OBJECT_ID(QUOTENAME(po.SchemaName) + ''.'' + QUOTENAME(po.TableName)) IS NULL
                        THEN ''missing'' ELSE ''present'' END
            FROM #PlanObjects po
            WHERE po.DatabaseName = @db;';
            EXEC sp_executesql @sql, N'@db SYSNAME', @db = @CurrentDb;

            /* Indexes on plan-referenced objects. Emit BOTH a presence row per index AND
               a definition row. The presence row is the headline "missing on X" signal;
               the definition/key/include rows are collapsed in the diff engine when an
               index is missing entirely, so we don't spam 3 redundant diffs per gap. */
            SET @sql = N'
            USE ' + QUOTENAME(@CurrentDb) + N';
            SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
            INSERT INTO #LocalSnapshot (Category, Setting, SubjectKey, ValueText)
            SELECT ''Index'', ''Present'',
                   @db + ''.'' + s.[name] + ''.'' + t.[name] + ''.'' + ISNULL(i.[name], ''HEAP''),
                   ''present''
            FROM sys.tables t
            JOIN sys.schemas s ON s.schema_id = t.schema_id
            JOIN sys.indexes i ON i.object_id = t.object_id
            JOIN #PlanObjects po ON po.DatabaseName = @db
                                AND po.SchemaName  = s.[name]
                                AND po.TableName   = t.[name];

            INSERT INTO #LocalSnapshot (Category, Setting, SubjectKey, ValueText, ValueNumeric)
            SELECT ''Index'',
                   ''Definition'',
                   @db + ''.'' + s.[name] + ''.'' + t.[name] + ''.'' + ISNULL(i.[name], ''HEAP''),
                   ISNULL(i.type_desc, ''HEAP'')
                     + ''|unique='' + CASE WHEN i.is_unique = 1 THEN ''Y'' ELSE ''N'' END
                     + ''|disabled='' + CASE WHEN i.is_disabled = 1 THEN ''Y'' ELSE ''N'' END
                     + ''|filter='' + ISNULL(i.filter_definition, ''(none)''),
                   i.index_id
            FROM sys.tables t
            JOIN sys.schemas s ON s.schema_id = t.schema_id
            LEFT JOIN sys.indexes i ON i.object_id = t.object_id
            JOIN #PlanObjects po ON po.DatabaseName = @db
                                AND po.SchemaName  = s.[name]
                                AND po.TableName   = t.[name];';
            EXEC sp_executesql @sql, N'@db SYSNAME', @db = @CurrentDb;

            /* Index key + included columns (one row per index, columns concatenated) */
            SET @sql = N'
            USE ' + QUOTENAME(@CurrentDb) + N';
            SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
            ;WITH key_cols AS (
                SELECT i.object_id, i.index_id,
                       STUFF((SELECT '','' + c.[name] + CASE WHEN ic.is_descending_key = 1 THEN '' DESC'' ELSE '''' END
                              FROM sys.index_columns ic
                              JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
                              WHERE ic.object_id = i.object_id AND ic.index_id = i.index_id AND ic.is_included_column = 0
                              ORDER BY ic.key_ordinal
                              FOR XML PATH('''')), 1, 1, '''') AS KeyColumns,
                       STUFF((SELECT '','' + c.[name]
                              FROM sys.index_columns ic
                              JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
                              WHERE ic.object_id = i.object_id AND ic.index_id = i.index_id AND ic.is_included_column = 1
                              FOR XML PATH('''')), 1, 1, '''') AS InclColumns
                FROM sys.indexes i
            )
            INSERT INTO #LocalSnapshot (Category, Setting, SubjectKey, ValueText)
            SELECT ''Index'', ''KeyColumns'',
                   @db + ''.'' + s.[name] + ''.'' + t.[name] + ''.'' + ISNULL(i.[name], ''HEAP''),
                   ISNULL(kc.KeyColumns, ''(none)'')
            FROM sys.tables t
            JOIN sys.schemas s ON s.schema_id = t.schema_id
            JOIN sys.indexes i ON i.object_id = t.object_id
            LEFT JOIN key_cols kc ON kc.object_id = i.object_id AND kc.index_id = i.index_id
            JOIN #PlanObjects po ON po.DatabaseName = @db
                                AND po.SchemaName  = s.[name]
                                AND po.TableName   = t.[name]
            UNION ALL
            SELECT ''Index'', ''InclColumns'',
                   @db + ''.'' + s.[name] + ''.'' + t.[name] + ''.'' + ISNULL(i.[name], ''HEAP''),
                   ISNULL(kc.InclColumns, ''(none)'')
            FROM sys.tables t
            JOIN sys.schemas s ON s.schema_id = t.schema_id
            JOIN sys.indexes i ON i.object_id = t.object_id
            LEFT JOIN key_cols kc ON kc.object_id = i.object_id AND kc.index_id = i.index_id
            JOIN #PlanObjects po ON po.DatabaseName = @db
                                AND po.SchemaName  = s.[name]
                                AND po.TableName   = t.[name];';
            EXEC sp_executesql @sql, N'@db SYSNAME', @db = @CurrentDb;

            /* Statistics: header data only */
            SET @sql = N'
            USE ' + QUOTENAME(@CurrentDb) + N';
            SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
            INSERT INTO #LocalSnapshot (Category, Setting, SubjectKey, ValueText, ValueNumeric)
            SELECT ''Statistics'', ''Header'',
                   @db + ''.'' + s.[name] + ''.'' + t.[name] + ''.'' + st.[name],
                   ''lastUpdated='' + ISNULL(CONVERT(VARCHAR(33), sp.last_updated, 126), ''(null)'')
                     + ''|rows='' + ISNULL(CAST(sp.[rows] AS VARCHAR(20)), ''(null)'')
                     + ''|sampled='' + ISNULL(CAST(sp.rows_sampled AS VARCHAR(20)), ''(null)'')
                     + ''|modCounter='' + ISNULL(CAST(sp.modification_counter AS VARCHAR(20)), ''(null)'')
                     + ''|autoCreated='' + CASE WHEN st.auto_created = 1 THEN ''Y'' ELSE ''N'' END
                     + ''|hasFilter='' + CASE WHEN st.has_filter = 1 THEN ''Y'' ELSE ''N'' END,
                   sp.modification_counter
            FROM sys.stats st
            JOIN sys.tables t  ON t.object_id  = st.object_id
            JOIN sys.schemas s ON s.schema_id  = t.schema_id
            CROSS APPLY sys.dm_db_stats_properties(st.object_id, st.stats_id) sp
            JOIN #PlanObjects po ON po.DatabaseName = @db
                                AND po.SchemaName  = s.[name]
                                AND po.TableName   = t.[name];';
            EXEC sp_executesql @sql, N'@db SYSNAME', @db = @CurrentDb;

            /* Row counts and page counts */
            SET @sql = N'
            USE ' + QUOTENAME(@CurrentDb) + N';
            SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
            INSERT INTO #LocalSnapshot (Category, Setting, SubjectKey, ValueText, ValueNumeric)
            SELECT ''RowCount'', ''Rows'',
                   @db + ''.'' + s.[name] + ''.'' + t.[name],
                   CAST(SUM(ps.row_count) AS NVARCHAR(40)),
                   SUM(ps.row_count)
            FROM sys.tables t
            JOIN sys.schemas s ON s.schema_id = t.schema_id
            JOIN sys.dm_db_partition_stats ps
                 ON ps.object_id = t.object_id AND ps.index_id IN (0, 1)
            JOIN #PlanObjects po ON po.DatabaseName = @db
                                AND po.SchemaName  = s.[name]
                                AND po.TableName   = t.[name]
            GROUP BY s.[name], t.[name]
            UNION ALL
            SELECT ''RowCount'', ''Pages'',
                   @db + ''.'' + s.[name] + ''.'' + t.[name],
                   CAST(SUM(ps.in_row_used_page_count) AS NVARCHAR(40)),
                   SUM(ps.in_row_used_page_count)
            FROM sys.tables t
            JOIN sys.schemas s ON s.schema_id = t.schema_id
            JOIN sys.dm_db_partition_stats ps
                 ON ps.object_id = t.object_id AND ps.index_id IN (0, 1)
            JOIN #PlanObjects po ON po.DatabaseName = @db
                                AND po.SchemaName  = s.[name]
                                AND po.TableName   = t.[name]
            GROUP BY s.[name], t.[name];';
            EXEC sp_executesql @sql, N'@db SYSNAME', @db = @CurrentDb;

            /* Forced plans: Query Store. TRY/CATCH because sys.query_store_plan
               is only present when Query Store is enabled on the database. */
            BEGIN TRY
                SET @sql = N'
                USE ' + QUOTENAME(@CurrentDb) + N';
                SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
                INSERT INTO #LocalSnapshot (Category, Setting, SubjectKey, ValueText)
                SELECT ''ForcedPlan'', ''QueryStore'', @db,
                       ''plan_id='' + CAST(plan_id AS NVARCHAR(40))
                       + '' hash='' + CONVERT(NVARCHAR(50), query_plan_hash, 1)
                FROM sys.query_store_plan
                WHERE is_forced_plan = 1;';
                EXEC sp_executesql @sql, N'@db SYSNAME', @db = @CurrentDb;
            END TRY
            BEGIN CATCH
                /* Query Store off, view missing on this version, or no permission. Skip. */
                IF @Debug = 1
                BEGIN
                    SET @ErrMessage = ERROR_MESSAGE();
                    RAISERROR('Query Store probe skipped for %s: %s', 0, 1, @CurrentDb, @ErrMessage) WITH NOWAIT;
                END;
            END CATCH;

            /* Per-query wait stats from Query Store. sys.dm_exec_query_plan_stats does NOT
               populate <WaitStats> in the plan XML - only SET STATISTICS XML ON does.
               Query Store's sys.query_store_wait_stats is the reliable cached source.
               Wait data is bucketed by wait_category_desc (not raw wait type).
               Guarded by NOT EXISTS so we only pull from the first DB with a QS match -
               prevents duplicates when the plan references multiple DBs that all have QS on.
               TRY/CATCH because QS may be off, disabled, or the view may not exist on older versions. */
            IF NOT EXISTS (SELECT 1 FROM #PlanWait)
            BEGIN
                BEGIN TRY
                    SET @sql = N'
                    USE ' + QUOTENAME(@CurrentDb) + N';
                    SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
                    INSERT INTO #PlanWait (WaitType, WaitTimeMs, WaitCount)
                    SELECT qsws.wait_category_desc,
                           SUM(qsws.total_query_wait_time_ms),
                           NULL  /* sys.query_store_wait_stats has no wait-count column - only timing */
                    FROM   sys.query_store_plan qsp
                    JOIN   sys.query_store_wait_stats qsws ON qsws.plan_id = qsp.plan_id
                    WHERE  qsp.query_plan_hash = @qph
                    GROUP BY qsws.wait_category_desc;';
                    EXEC sp_executesql @sql, N'@qph BINARY(8)', @qph = @QueryPlanHash;
                END TRY
                BEGIN CATCH
                    IF @Debug = 1
                    BEGIN
                        SET @ErrMessage = ERROR_MESSAGE();
                        RAISERROR('Query Store wait-stats probe skipped for %s: %s', 0, 1, @CurrentDb, @ErrMessage) WITH NOWAIT;
                    END;
                END CATCH;
            END;

            BEGIN TRY
                SET @sql = N'
                USE ' + QUOTENAME(@CurrentDb) + N';
                SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
                INSERT INTO #LocalSnapshot (Category, Setting, SubjectKey, ValueText)
                SELECT ''ForcedPlan'', ''PlanGuide'',
                       @db + ''.'' + ISNULL(scope_object_id_name, ''(server)''),
                       [name] + '':'' + scope_type_desc + '':'' + ISNULL(hints, ''(no hints)'')
                FROM (
                    SELECT pg.[name], pg.scope_type_desc, pg.hints,
                           OBJECT_NAME(pg.scope_object_id) AS scope_object_id_name
                    FROM sys.plan_guides pg
                ) x;';
                EXEC sp_executesql @sql, N'@db SYSNAME', @db = @CurrentDb;
            END TRY
            BEGIN CATCH
                IF @Debug = 1
                BEGIN
                    SET @ErrMessage = ERROR_MESSAGE();
                    RAISERROR('Plan guide probe failed for %s: %s', 0, 1, @CurrentDb, @ErrMessage) WITH NOWAIT;
                END;
            END CATCH;
        END;
    END TRY
    BEGIN CATCH
        SET @ErrMessage = ERROR_MESSAGE();
        INSERT INTO #LocalSnapshot (Category, Setting, SubjectKey, ValueText)
        VALUES ('Database', '_unreadable_', @CurrentDb, 'Could not snapshot database: ' + @ErrMessage);
    END CATCH;

    FETCH NEXT FROM db_cur INTO @CurrentDb;
END;
CLOSE db_cur;
DEALLOCATE db_cur;

/* ----- Category: PlanAttribute and PlanWarning (from already-shredded temp tables) -----
   TRY_CAST populates ValueNumeric for numeric attributes (MaxCompileMemory,
   MemoryGrant, EstimatedAvailableMemoryGrant, EstimatedPagesCached, CompileTime,
   CompileCPU, CompileMemory, CachedPlanSize, DegreeOfParallelism,
   CardinalityEstimationModelVersion, StatementSubTreeCost, etc.). That lets the
   25% noise-suppression filter apply, so tiny hardware-dependent jitter (e.g. a
   ~1% MaxCompileMemory delta) doesn't fire a finding. Text attributes
   (PlanSource, RetrievedFromCache, StatementOptmLevel) yield NULL from TRY_CAST
   and therefore bypass the threshold - any change still surfaces. */
INSERT INTO #LocalSnapshot (Category, Setting, SubjectKey, ValueText, ValueNumeric)
SELECT 'PlanAttribute', AttributeName, NULL, AttributeValue,
       TRY_CAST(AttributeValue AS DECIMAL(38, 4))
FROM   #PlanAttributes
WHERE  AttributeValue IS NOT NULL;

INSERT INTO #LocalSnapshot (Category, Setting, SubjectKey, ValueText)
SELECT 'PlanWarning', WarningName, NULL, WarningDetails
FROM   #PlanWarnings;

/* ----- Category: PlanRuntime (plan-level runtime totals - only populated when the
   actual plan was captured via LAST_QUERY_PLAN_STATS). If LAST_QUERY_PLAN_STATS is
   off on one server and on on the other, these rows will appear as "Missing on X"
   - which itself is useful signal (tells the user to enable it for fair comparison). */
INSERT INTO #LocalSnapshot (Category, Setting, SubjectKey, ValueText, ValueNumeric)
SELECT 'PlanRuntime', [Setting], NULL, ValueText, ValueNumeric
FROM   #PlanRuntime;

/* ----- Category: PlanWait (per-wait-type runtime waits incurred by this query's
   last execution). SubjectKey = wait type so PAGEIOLATCH_SH diffs against PAGEIOLATCH_SH
   and not some other wait. Only populated when actual plan has wait stats. */
INSERT INTO #LocalSnapshot (Category, Setting, SubjectKey, ValueText, ValueNumeric)
SELECT 'PlanWait', 'WaitTimeMs', WaitType, CAST(WaitTimeMs AS NVARCHAR(40)), WaitTimeMs
FROM   #PlanWait
WHERE  WaitTimeMs IS NOT NULL

UNION ALL
SELECT 'PlanWait', 'WaitCount', WaitType, CAST(WaitCount AS NVARCHAR(40)), WaitCount
FROM   #PlanWait
WHERE  WaitCount IS NOT NULL;

/* ----- Category: Parameter (compiled values, sniffing visibility) ----- */
INSERT INTO #LocalSnapshot (Category, Setting, SubjectKey, ValueText)
SELECT 'Parameter', 'CompiledValue', ParameterName,
       ISNULL(DataType, '(unknown type)') + ' = ' + ISNULL(CompiledValue, '(null)')
FROM   #PlanParameters;

/* ----- Category: LiveState (point-in-time snapshot for sessions running this hash) ----- */
DECLARE @RunnableTasks INT, @WorkerCount INT, @BlockedSessions INT;

SELECT @RunnableTasks = COUNT(*)
FROM   sys.dm_os_schedulers
WHERE  status = 'VISIBLE ONLINE';

SELECT @WorkerCount = COUNT(*) FROM sys.dm_os_workers;

SELECT @BlockedSessions = COUNT(*) FROM sys.dm_exec_requests WHERE blocking_session_id <> 0;

INSERT INTO #LocalSnapshot (Category, Setting, SubjectKey, ValueText, ValueNumeric)
VALUES ('LiveState', 'RunnableSchedulers', NULL, CAST(@RunnableTasks AS NVARCHAR(20)), @RunnableTasks),
       ('LiveState', 'WorkerCount',        NULL, CAST(@WorkerCount   AS NVARCHAR(20)), @WorkerCount),
       ('LiveState', 'BlockedSessions',    NULL, CAST(@BlockedSessions AS NVARCHAR(20)), @BlockedSessions);

/* Top 5 wait types sessions running this query are blocked on RIGHT NOW.
   Matched by query_hash (stable across servers), not query_plan_hash (which
   usually differs when the plans diverge - the whole point of comparing). */
INSERT INTO #LocalSnapshot (Category, Setting, SubjectKey, ValueText, ValueNumeric)
SELECT TOP 5 'LiveState', 'WaitType', wt.wait_type,
       CAST(SUM(wt.wait_duration_ms) AS NVARCHAR(40)),
       SUM(wt.wait_duration_ms)
FROM   sys.dm_exec_requests req
JOIN   sys.dm_os_waiting_tasks wt ON wt.session_id = req.session_id
WHERE  req.query_hash IS NOT NULL
  AND  req.query_hash = @QueryHashBin
GROUP BY wt.wait_type
ORDER BY SUM(wt.wait_duration_ms) DESC;

/* LiveWait: cumulative session-level wait totals for sessions CURRENTLY running
   this query. Sources sys.dm_exec_session_wait_stats (cumulative since session
   open), filtered to sessions whose active request matches @QueryHashBin.

   Why this matters: when a query is in-flight on one server and racking up
   waits (IO, locks, memory grant, parallelism), Query Store hasn't flushed
   those to sys.query_store_wait_stats yet - they only land there after the
   query finishes. The PlanWait category reads Query Store and therefore
   misses in-flight waits entirely. LiveWait captures the in-flight picture.

   Caveat: session_wait_stats is per-SESSION, not per-REQUEST. If the same
   session ran other queries earlier, their waits will also be included.
   Still the best signal available for a running query. Sum across sessions
   in case multiple copies of the same query are executing concurrently. */
;WITH live_sessions AS (
    SELECT req.session_id
    FROM   sys.dm_exec_requests req
    WHERE  req.query_hash = @QueryHashBin
      AND  req.query_hash IS NOT NULL
),
live_waits AS (
    SELECT sws.wait_type,
           SUM(sws.wait_time_ms)         AS wait_time_ms,
           SUM(sws.waiting_tasks_count)  AS waiting_tasks_count
    FROM   sys.dm_exec_session_wait_stats sws
    WHERE  sws.session_id IN (SELECT session_id FROM live_sessions)
    GROUP BY sws.wait_type
)
/* 1-second / 1000-count floor keeps the signal (PAGEIOLATCH_SH 10M ms, CXPACKET,
   LOCK waits, etc.) and drops the inevitable trickle of sub-1s housekeeping
   waits (SLEEP_TASK 11 ms, LATCH_EX 3 ms, HADR_CLUSAPI_CALL 12 ms) that would
   otherwise bury the real bottleneck in 30+ noise rows. */
INSERT INTO #LocalSnapshot (Category, Setting, SubjectKey, ValueText, ValueNumeric)
SELECT 'LiveWait', 'WaitTimeMs', lw.wait_type,
       CAST(lw.wait_time_ms AS NVARCHAR(40)),
       lw.wait_time_ms
FROM   live_waits lw
WHERE  lw.wait_time_ms >= 1000
UNION ALL
SELECT 'LiveWait', 'WaitCount', lw.wait_type,
       CAST(lw.waiting_tasks_count AS NVARCHAR(40)),
       lw.waiting_tasks_count
FROM   live_waits lw
WHERE  lw.waiting_tasks_count >= 1000;

/* Active memory grants for this query hash (if currently executing). */
INSERT INTO #LocalSnapshot (Category, Setting, SubjectKey, ValueText, ValueNumeric)
SELECT 'LiveState', 'ActiveMemoryGrantKB', CAST(session_id AS NVARCHAR(20)),
       CAST(granted_memory_kb AS NVARCHAR(40)) + '/' + CAST(ideal_memory_kb AS NVARCHAR(40))
       + ' (used ' + CAST(ISNULL(used_memory_kb, 0) AS NVARCHAR(40)) + ')',
       granted_memory_kb
FROM   sys.dm_exec_query_memory_grants
WHERE  plan_handle = @PlanHandle;

/* =====================================================================
   Mode 1: emit XML and return
   ===================================================================== */
IF @Mode = 1
BEGIN
    /* Build the body element for #LocalSnapshot rows */
    DECLARE @SnapshotBody XML;
    SET @SnapshotBody = (
        SELECT Category   AS [@Category],
               Setting    AS [@Setting],
               SubjectKey AS [@SubjectKey],
               ValueText  AS [@ValueText],
               ValueNumeric AS [@ValueNumeric]
        FROM   #LocalSnapshot
        FOR XML PATH('Row'), TYPE);

    /* Build a separate Plan element so the typed XML round-trips cleanly */
    DECLARE @PlanContainer XML;
    /* Embed the actual-plan-if-available variant (richer runtime info), not the full
       plan used for object extraction. For stored procs with multiple statements,
       the actual plan is a statement-scoped fragment with runtime stats, while the
       full plan is the whole batch's plan used for table/index diffing. */
    SET @PlanContainer = (SELECT @PlanXmlForEmit AS [*] FOR XML PATH('Plan'), TYPE);

    SET @SnapshotXml = (
        SELECT '0.01'                                              AS [@Version],
               CONVERT(NVARCHAR(33), SYSUTCDATETIME(), 126)        AS [@GeneratedUTC],
               CONVERT(NVARCHAR(50), @QueryHashBin, 1)             AS [@QueryHash],
               CONVERT(NVARCHAR(50), @QueryPlanHash, 1)            AS [@QueryPlanHash],
               @LocalServerName                                    AS [@SourceServer],
               @SnapshotBody                                       AS [Snapshot],
               @PlanContainer                                      AS [*]
        FOR XML PATH('BlitzPlanCompareSnapshot'), TYPE);

    /* CallStack: an exact, copy/paste-ready EXEC statement the user can run on the
       other server. The snapshot XML is serialized to NVARCHAR(MAX) and single quotes
       are doubled so it's a valid SQL string literal. Click the CallStack cell in SSMS,
       copy the whole thing, paste into a query window on the other server, hit F5. */
    DECLARE @SnapshotText NVARCHAR(MAX) = CAST(@SnapshotXml AS NVARCHAR(MAX));
    DECLARE @CallStack    NVARCHAR(MAX) =
        N'EXEC dbo.sp_BlitzPlanCompare @CompareToXML = N'''
      + REPLACE(@SnapshotText, N'''', N'''''')
      + N''';';

    /* Single CallStack column - NVARCHAR, so it crosses linked-server INSERT EXEC
       cleanly (XML columns hit Msg 9514 "Xml data type is not supported in
       distributed queries"). The snapshot XML is embedded inside the EXEC text
       with single quotes doubled per T-SQL string-literal rules; mode 3 strips
       the prefix/suffix and undoubles the quotes to recover the XML. */
    SELECT @CallStack AS [CallStack];
    RETURN;
END;

/* =====================================================================
   Build the remote snapshot
   ===================================================================== */
IF OBJECT_ID('tempdb..#RemoteSnapshot') IS NOT NULL DROP TABLE #RemoteSnapshot;
CREATE TABLE #RemoteSnapshot (
    Category     VARCHAR(50)    NOT NULL,
    Setting      VARCHAR(200)   NOT NULL,
    SubjectKey   NVARCHAR(776)  NULL,
    ValueText    NVARCHAR(MAX)  NULL,
    ValueNumeric DECIMAL(38,4)  NULL
);

IF OBJECT_ID('tempdb..#Diff') IS NOT NULL DROP TABLE #Diff;
CREATE TABLE #Diff (
    Priority    TINYINT        NULL,
    Category    VARCHAR(50)    NULL,
    Setting     VARCHAR(200)   NULL,
    [Object]    NVARCHAR(776)  NULL,
    LocalValue  NVARCHAR(MAX)  NULL,
    RemoteValue NVARCHAR(MAX)  NULL,
    Finding     NVARCHAR(500)  NULL,
    [URL]       NVARCHAR(500)  NULL,
    Details     NVARCHAR(1000) NULL,
    CallStack   XML            NULL  /* typed XML so SSMS renders it as a clickable cell - populated only for the parameter-sniffing reproducer row today, reserved for future row types */
);

DECLARE @RemoteXml XML;

IF @Mode = 2
BEGIN
    SET @RemoteXml = @CompareToXML;
END
ELSE IF @Mode = 3
BEGIN
    IF OBJECT_ID('tempdb..#RemoteEmit') IS NOT NULL DROP TABLE #RemoteEmit;
    /* Staging table for the remote proc's first output column. INSERT...EXEC matches
       columns by position, so the remote's column name (PlanMetadataXml, or CallStack
       if the remote version returns CallStack first) doesn't have to match here - we
       only consume the plan XML. Name it to match the remote proc's output for clarity. */
    /* Must match the column list of mode-1 output EXACTLY (count + types).
       Mode 1 returns a single NVARCHAR(MAX) CallStack column - the ready-to-run
       EXEC sp_BlitzPlanCompare @CompareToXML = N'<snapshot>'; statement. We do
       NOT need the snapshot XML or plan XML as separate columns; both are
       embedded inside the CallStack text and we extract them below.

       NVARCHAR (not XML) because XML columns can't travel across a linked-server
       INSERT EXEC - SQL Server raises Msg 9514 "Xml data type is not supported
       in distributed queries. Remote object 'IROWSET' has xml column(s)."
       Column-count mismatches between remote and local surface as the cryptic
       "Msg 0, Level 11, State 0, A severe error occurred on the current command". */
    CREATE TABLE #RemoteEmit ([CallStack] NVARCHAR(MAX));

    /* Isolation note: the remote sp_BlitzPlanCompare runs in its own session on the remote server
       and sets READ UNCOMMITTED itself at the top of the proc, so the remote work does not block.

       Pass @QueryHashBin (the local plan's query_hash) rather than @QueryPlanHash.
       The query_plan_hash is specific to the compiled plan shape and is almost always
       DIFFERENT on each server (different stats, indexes, hardware, compat level all
       shift the optimizer toward different operators). The query_hash is stable: it's
       derived from the normalized query text and is what the remote should be looking
       up. Mode 2 (paste-in XML) already relies on this exact fallback - we extract the
       remote QueryHash from the snapshot and the local resolution logic tries it as a
       plan_hash first, falls back to query_hash. The remote proc's plan-resolution
       block does the same thing when we hand it a query_hash disguised as a plan_hash. */
    SET @sql = N'EXEC ' + @LinkedServer + N'.master.dbo.sp_BlitzPlanCompare @QueryPlanHash = @hash;';

    IF @Debug = 1
        RAISERROR('Linked-server invocation: %s (passing local QueryHash so remote can resolve its own plan with a matching query_hash)', 0, 1, @sql) WITH NOWAIT;

    BEGIN TRY
        INSERT INTO #RemoteEmit ([CallStack])
        EXEC sp_executesql @sql, N'@hash BINARY(8)', @hash = @QueryHashBin;

        /* The remote returned a single string of the form:
               EXEC dbo.sp_BlitzPlanCompare @CompareToXML = N'<BlitzPlanCompareSnapshot ...>';
           Strip the prefix and the trailing ';' suffix, then undouble the
           single quotes (T-SQL string-literal escaping) to recover the XML. */
        DECLARE @RemoteCallStack NVARCHAR(MAX),
                @XmlPrefix       NVARCHAR(100) = N'EXEC dbo.sp_BlitzPlanCompare @CompareToXML = N''',
                @PrefixPos       INT,
                @XmlBody         NVARCHAR(MAX);

        SELECT TOP 1 @RemoteCallStack = [CallStack] FROM #RemoteEmit;

        SET @PrefixPos = CHARINDEX(@XmlPrefix, @RemoteCallStack);
        IF @PrefixPos > 0
        BEGIN
            /* Trim prefix, then trim the closing N''';' suffix (3 chars: ' + ; + optional WS). */
            SET @XmlBody = SUBSTRING(@RemoteCallStack,
                                     @PrefixPos + LEN(@XmlPrefix),
                                     LEN(@RemoteCallStack));
            /* Strip trailing ';' and the closing single quote. The CallStack
               builder always emits ...''';' at the end. */
            SET @XmlBody = LEFT(@XmlBody,
                                LEN(@XmlBody) - 2);  -- drop ;'
            /* Undouble the single quotes (T-SQL string literal escaping). */
            SET @XmlBody = REPLACE(@XmlBody, N'''''', N'''');

            SET @RemoteXml = TRY_CAST(@XmlBody AS XML);
        END;
    END TRY
    BEGIN CATCH
        SET @ErrNumber = ERROR_NUMBER();
        SET @ErrMessage = ERROR_MESSAGE();

        /* Build a copy/paste-ready sp_serveroption command for the rpc/rpc out fixes.
           Strip outer brackets so sp_serveroption gets the raw server name. */
        DECLARE @LinkedServerBare SYSNAME = REPLACE(REPLACE(@LinkedServer, '[', ''), ']', '');
        DECLARE @RpcFixCommand NVARCHAR(1000) =
            'EXEC sp_serveroption @server = ''' + @LinkedServerBare + ''', @optname = ''rpc'',     @optvalue = ''true''; '
          + 'EXEC sp_serveroption @server = ''' + @LinkedServerBare + ''', @optname = ''rpc out'', @optvalue = ''true'';';

        IF @ErrNumber = 2812
        BEGIN
            INSERT INTO #Diff (Priority, Category, Setting, [Object], Finding, [URL], Details)
            VALUES (1, 'Setup', 'RemoteInstall', @LinkedServer,
                    'sp_BlitzPlanCompare is not installed on the linked server. Install it there or use @CompareToXML mode.',
                    'http://FirstResponderKit.org',
                    'Linked server returned err 2812: ' + @ErrMessage);

            /* Skip the diff; just return the setup row. */
            SELECT Priority, Category, Setting, [Object], LocalValue, RemoteValue, Finding, [URL], Details
            FROM   #Diff
            ORDER BY Priority, Category, Setting;
            RETURN;
        END
        ELSE IF @ErrNumber IN (7411, 7415)
        BEGIN
            /* 7411: "Server '...' is not configured for RPC."
               7415: "Ad hoc access to OLE DB provider ... has been denied."
               Both surface when RPC / RPC OUT isn't set on the linked server. */
            INSERT INTO #Diff (Priority, Category, Setting, [Object], Finding, [URL], Details)
            VALUES (1, 'Setup', 'RpcOutDisabled', @LinkedServer,
                    'RPC OUT is not enabled on the linked server. Run this on the local server to fix it: ' + @RpcFixCommand,
                    'http://FirstResponderKit.org',
                    'Error ' + CAST(@ErrNumber AS NVARCHAR(20)) + ': ' + @ErrMessage);

            SELECT Priority, Category, Setting, [Object], LocalValue, RemoteValue, Finding, [URL], Details
            FROM   #Diff
            ORDER BY Priority, Category, Setting;
            RETURN;
        END
        ELSE IF @ErrNumber = 8509
        BEGIN
            /* 8509: "Import of Microsoft Distributed Transaction Coordinator (MS DTC)
               transaction failed: 0x8004d00e(XACT_E_NOTRANSACTION)."
               Raised when the linked server tries to promote the remote call to a
               distributed transaction but MSDTC isn't reachable / isn't running.
               The fix is to turn off remote proc transaction promotion on the
               linked server so the RPC runs without MSDTC enlistment. */
            DECLARE @DtcFixCommand NVARCHAR(1000) =
                'EXEC master.dbo.sp_serveroption ' + CHAR(13) + CHAR(10)
              + '    @server   = N''' + @LinkedServerBare + ''', ' + CHAR(13) + CHAR(10)
              + '    @optname  = N''remote proc transaction promotion'', ' + CHAR(13) + CHAR(10)
              + '    @optvalue = N''false'';';

            INSERT INTO #Diff (Priority, Category, Setting, [Object], Finding, [URL], Details)
            VALUES (1, 'Setup', 'DtcPromotionFailed', @LinkedServer,
                    'The linked-server call tried to promote to a distributed transaction and MSDTC is not available. '
                  + 'Turn off remote proc transaction promotion on the linked server and try again. Run this on the local server: '
                  + @DtcFixCommand,
                    'http://FirstResponderKit.org',
                    'Error 8509: ' + @ErrMessage);

            SELECT Priority, Category, Setting, [Object], LocalValue, RemoteValue, Finding, [URL], Details
            FROM   #Diff
            ORDER BY Priority, Category, Setting;
            RETURN;
        END
        ELSE
        BEGIN
            INSERT INTO #Diff (Priority, Category, Setting, [Object], Finding, [URL], Details)
            VALUES (1, 'Setup', 'RemoteCallFailed', @LinkedServer,
                    'Linked-server call failed.', 'http://FirstResponderKit.org',
                    'Error ' + CAST(@ErrNumber AS NVARCHAR(20)) + ': ' + @ErrMessage);

            SELECT Priority, Category, Setting, [Object], LocalValue, RemoteValue, Finding, [URL], Details
            FROM   #Diff
            ORDER BY Priority, Category, Setting;
            RETURN;
        END;
    END CATCH;

    IF @RemoteXml IS NULL
    BEGIN
        /* Same RPC fix command as in the CATCH block above, in case the user lands here. */
        DECLARE @LinkedServerBareNull SYSNAME = REPLACE(REPLACE(@LinkedServer, '[', ''), ']', '');
        DECLARE @RpcFixCommandNull NVARCHAR(1000) =
            'EXEC sp_serveroption @server = ''' + @LinkedServerBareNull + ''', @optname = ''rpc'',     @optvalue = ''true''; '
          + 'EXEC sp_serveroption @server = ''' + @LinkedServerBareNull + ''', @optname = ''rpc out'', @optvalue = ''true'';';

        INSERT INTO #Diff (Priority, Category, Setting, [Object], Finding, [URL], Details)
        VALUES (1, 'Setup', 'RemoteEmptyResult', @LinkedServer,
                'Linked server returned no XML. The remote may not have the matching plan in cache, or RPC OUT may be disabled. To enable RPC OUT, run this on the local server: ' + @RpcFixCommandNull,
                'http://FirstResponderKit.org', NULL);

        SELECT Priority, Category, Setting, [Object], LocalValue, RemoteValue, Finding, [URL], Details
        FROM   #Diff
        ORDER BY Priority, Category, Setting;
        RETURN;
    END;
END;

/* Shred the remote snapshot XML into #RemoteSnapshot */
INSERT INTO #RemoteSnapshot (Category, Setting, SubjectKey, ValueText, ValueNumeric)
SELECT r.n.value('@Category',     'VARCHAR(50)'),
       r.n.value('@Setting',      'VARCHAR(200)'),
       r.n.value('@SubjectKey',   'NVARCHAR(776)'),
       r.n.value('@ValueText',    'NVARCHAR(MAX)'),
       r.n.value('@ValueNumeric', 'DECIMAL(38,4)')
FROM   @RemoteXml.nodes('/BlitzPlanCompareSnapshot/Snapshot/Row') AS r(n);

IF NOT EXISTS (SELECT 1 FROM #RemoteSnapshot)
BEGIN
    INSERT INTO #Diff (Priority, Category, Setting, Finding, [URL], Details)
    VALUES (1, 'Setup', 'RemoteSnapshotEmpty',
            'Remote snapshot XML had no rows. The XML may be malformed or from a different version.',
            'http://FirstResponderKit.org', NULL);

    SELECT Priority, Category, Setting, [Object], LocalValue, RemoteValue, Finding, [URL], Details
    FROM   #Diff
    ORDER BY Priority, Category, Setting;
    RETURN;
END;

/* =====================================================================
   Rubric: priority + finding text + URL per (Category, Setting)
   ===================================================================== */
IF OBJECT_ID('tempdb..#Rubric') IS NOT NULL DROP TABLE #Rubric;
CREATE TABLE #Rubric (
    Category VARCHAR(50)   NOT NULL,
    Setting  VARCHAR(200)  NOT NULL,  /* '*' = wildcard / category default */
    Priority TINYINT       NOT NULL,
    Finding  NVARCHAR(500) NOT NULL,
    [URL]    NVARCHAR(500) NULL
);

INSERT INTO #Rubric (Category, Setting, Priority, Finding, [URL]) VALUES
    /* Setup / catastrophic */
    ('Database',             '_unreadable_',         1,  'A plan-referenced database is not readable on one of the servers. The diff will be incomplete.', 'http://FirstResponderKit.org'),
    ('Database',             '_skipped_',            100, 'One plan references tempdb (temp tables / table variables) and the other does not. Temp contents are not compared directly because names include session-specific suffixes.', 'http://FirstResponderKit.org'),
    ('Object',               'Present',              1,  'A table referenced by the plan is missing on one server. This will break the query entirely.', 'http://FirstResponderKit.org'),
    ('ForcedPlan',           '*',                    1,  'A forced plan exists on one side. Forced plans override the optimizer and almost always cause cross-server divergence.', 'http://FirstResponderKit.org'),
    ('DatabaseScopedConfig', 'LEGACY_CARDINALITY_ESTIMATION', 1, 'LEGACY_CARDINALITY_ESTIMATION setting differs. This is one of the most common reasons for plan divergence.', 'http://FirstResponderKit.org'),

    /* Parameter sniffing */
    ('Parameter',            'CompiledValue',        10, 'The plan was compiled with a different parameter value on each server. Classic parameter sniffing scenario.', 'http://FirstResponderKit.org'),
    ('Parameter',            '*',                    10, 'A plan parameter differs.', 'http://FirstResponderKit.org'),

    /* Cardinality estimator drift */
    ('Statistics',           'Header',               15, 'A statistic the optimizer relied on differs between servers (last update, row count, sampling, or modification counter).', 'http://FirstResponderKit.org'),
    ('Database',             'CompatLevel',          15, 'Database compatibility level differs. This changes the cardinality estimator and can change every plan.', 'http://FirstResponderKit.org'),
    ('PlanAttribute',        'CardinalityEstimationModelVersion', 15, 'The cardinality estimator model version baked into this plan differs across servers.', 'http://FirstResponderKit.org'),

    /* Major perf drivers */
    ('Hardware',             '*',                    20, 'Hardware capacity differs. The optimizer uses available CPU/memory when costing plans.', 'http://FirstResponderKit.org'),
    ('SpConfigure',          'max degree of parallelism', 20, 'MAXDOP differs. This changes whether the plan can go parallel and how many threads it gets.', 'http://FirstResponderKit.org'),
    ('SpConfigure',          'cost threshold for parallelism', 20, 'Cost threshold for parallelism differs. This decides whether a plan goes parallel at all.', 'http://FirstResponderKit.org'),
    ('SpConfigure',          'max server memory (MB)', 20, 'Max server memory differs. This caps memory grants and buffer pool size.', 'http://FirstResponderKit.org'),
    ('SpConfigure',          '*',                    55, 'A configured option differs.', 'http://FirstResponderKit.org'),
    ('PlanAttribute',        'DegreeOfParallelism',  20, 'The cached plan was compiled with a different DOP on each server.', 'http://FirstResponderKit.org'),
    ('PlanAttribute',        'MemoryGrant',          20, 'The cached plan asked for a different memory grant on each server.', 'http://FirstResponderKit.org'),
    ('PlanAttribute',        'EstimatedAvailableMemoryGrant',  20, 'The optimizer saw a different available memory grant on each server when compiling.', 'http://FirstResponderKit.org'),
    ('PlanAttribute',        'EstimatedPagesCached', 20, 'The optimizer saw a different cached-pages estimate on each server when compiling.', 'http://FirstResponderKit.org'),
    ('PlanAttribute',        'EstimatedAvailableDegreeOfParallelism', 20, 'The optimizer saw different available DOP on each server when compiling.', 'http://FirstResponderKit.org'),
    ('PlanAttribute',        'MaxCompileMemory',     20, 'Max compile memory differs across servers.', 'http://FirstResponderKit.org'),
    ('PlanAttribute',        'PlanSource',           30, 'One server resolved the actual execution plan via LAST_QUERY_PLAN_STATS while the other used the cached estimated plan. The actual plan typically carries richer runtime info.', 'http://FirstResponderKit.org'),
    ('PlanAttribute',        '*',                    30, 'A plan-level attribute differs across servers.', 'http://FirstResponderKit.org'),

    /* Index presence - priority 1 because a plan-referenced index being missing
       on one side means the optimizer literally couldn't build the same plan. */
    ('Index',                'Present',              1,  'An index exists on one server but not the other. The optimizer on the side without it couldn''t pick the same plan. Create the missing index there to compare apples to apples.', 'http://FirstResponderKit.org'),

    /* Index quality */
    ('Index',                'Definition',           25, 'An index definition differs (type, uniqueness, disabled state, or filter).', 'http://FirstResponderKit.org'),
    ('Index',                'KeyColumns',           25, 'An index has different key columns across servers.', 'http://FirstResponderKit.org'),
    ('Index',                'InclColumns',          25, 'An index has different included columns across servers.', 'http://FirstResponderKit.org'),
    ('RowCount',             'Rows',                 25, 'Table row counts differ. Statistics estimates and join orders depend on this.', 'http://FirstResponderKit.org'),
    ('RowCount',             'Pages',                35, 'Table page counts differ. IO costing depends on this.', 'http://FirstResponderKit.org'),

    /* Plan warnings */
    ('PlanWarning',          '*',                    30, 'The plan reports a warning on one server but not the other.', 'http://FirstResponderKit.org'),

    /* Plan runtime totals (from LAST_QUERY_PLAN_STATS actual plan). These are the
       headline "did the query run fast/slow" signals. */
    ('PlanRuntime',          'ElapsedTimeMs',        20, 'Query elapsed time (wall clock) differs between servers by more than 25%.', 'http://FirstResponderKit.org'),
    ('PlanRuntime',          'CpuTimeMs',            20, 'Query CPU time differs between servers by more than 25%.', 'http://FirstResponderKit.org'),
    ('PlanRuntime',          'GrantedMemoryKB',      20, 'Actual memory grant differs between servers by more than 25%.', 'http://FirstResponderKit.org'),
    ('PlanRuntime',          'MaxUsedMemoryKB',      30, 'Actual memory used differs between servers by more than 25%. If one side uses much less than granted, that is oversizing.', 'http://FirstResponderKit.org'),
    ('PlanRuntime',          'SpillCount',           15, 'The query spilled to tempdb on one server but not the other (or more on one side). Usually a memory-grant or stats problem.', 'http://FirstResponderKit.org'),
    ('PlanRuntime',          'UdfElapsedTimeMs',     30, 'Time spent in UDFs differs between servers by more than 25%.', 'http://FirstResponderKit.org'),
    ('PlanRuntime',          'UdfCpuTimeMs',         30, 'CPU spent in UDFs differs between servers by more than 25%.', 'http://FirstResponderKit.org'),
    ('PlanRuntime',          '*',                    35, 'A plan runtime total differs between servers.', 'http://FirstResponderKit.org'),

    /* Per-wait-type runtime waits. Any wait type incurred on one server but not the other
       (or waited significantly longer on one side) points at the bottleneck. */
    ('PlanWait',             'WaitTimeMs',           20, 'The query incurred significantly more time on this wait type on one server than the other. Classic bottleneck signal.', 'http://FirstResponderKit.org'),
    ('PlanWait',             'WaitCount',            35, 'The query incurred a different number of this wait type on each server.', 'http://FirstResponderKit.org'),
    ('PlanWait',             '*',                    35, 'A wait-stat metric differs between servers.', 'http://FirstResponderKit.org'),

    /* LiveWait: cumulative waits on currently-running sessions for this query hash.
       Only populated when the query is in-flight on at least one server. Higher
       priority than PlanWait because an in-flight wait is happening right now,
       not something you're reconstructing from history. */
    ('LiveWait',             'WaitTimeMs',           15, 'A query matching this plan is running RIGHT NOW on at least one server and has accumulated significantly different wait time on this wait type. In-flight waits beat historical ones - this is what the query is blocked on as you investigate.', 'http://FirstResponderKit.org'),
    ('LiveWait',             'WaitCount',            30, 'A currently-running session has incurred a different number of this wait type.', 'http://FirstResponderKit.org'),
    ('LiveWait',             '*',                    30, 'A live-wait metric differs between servers.', 'http://FirstResponderKit.org'),

    /* Live state */
    ('LiveState',            'BlockedSessions',      35, 'Different number of blocked sessions right now. The slow server may be losing time to blocking, not the plan.', 'http://FirstResponderKit.org'),
    ('LiveState',            'WaitType',             35, 'Different wait types currently being incurred by this query. The slow server may be IO- or memory-bound.', 'http://FirstResponderKit.org'),
    ('LiveState',            'ActiveMemoryGrantKB',  35, 'A query is currently executing with a memory grant that differs from the other server.', 'http://FirstResponderKit.org'),
    ('LiveState',            '*',                    55, 'A live-state metric differs.', 'http://FirstResponderKit.org'),

    /* Config drift */
    ('TraceFlag',            '*',                    55, 'A trace flag is enabled on one server but not the other.', 'http://FirstResponderKit.org'),
    ('Database',             'IsAutoUpdateStatsAsync', 55, 'Async stats update differs. This changes whether queries wait for stats updates.', 'http://FirstResponderKit.org'),
    ('Database',             'PageVerifyOption',     55, 'Page verify option differs.', 'http://FirstResponderKit.org'),
    ('Database',             'IsRCSI',               55, 'Read Committed Snapshot Isolation differs. Plans and locking change accordingly.', 'http://FirstResponderKit.org'),
    ('Database',             'ParameterizationOption', 55, 'Forced parameterization setting differs.', 'http://FirstResponderKit.org'),
    ('Database',             'IsQueryStoreOn',       55, 'Query Store enabled on one side only. This affects forced plans.', 'http://FirstResponderKit.org'),
    ('Database',             '*',                    100, 'A database-level setting differs.', 'http://FirstResponderKit.org'),
    ('DatabaseScopedConfig', '*',                    55, 'A database-scoped configuration differs.', 'http://FirstResponderKit.org'),

    /* Cosmetic */
    ('Server',               'Collation',            100, 'Server collation differs.', 'http://FirstResponderKit.org'),
    ('Server',               'MachineName',          200, 'Different machines (informational).', 'http://FirstResponderKit.org'),
    ('Server',               'ServerName',           200, 'Different server names (informational).', 'http://FirstResponderKit.org'),
    ('Server',               '*',                    100, 'A server property differs.', 'http://FirstResponderKit.org'),

    /* Catch-all */
    ('Setup',                '*',                    1,  'Setup error.', 'http://FirstResponderKit.org');

/* =====================================================================
   Diff engine: FULL OUTER JOIN local vs remote, classify, prioritize
   ===================================================================== */
;WITH paired AS (
    SELECT COALESCE(l.Category,   r.Category)   AS Category,
           COALESCE(l.Setting,    r.Setting)    AS Setting,
           COALESCE(l.SubjectKey, r.SubjectKey) AS SubjectKey,
           l.ValueText                          AS LocalValue,
           r.ValueText                          AS RemoteValue,
           l.ValueNumeric                       AS LocalValueNumeric,
           r.ValueNumeric                       AS RemoteValueNumeric,
           /* Category is a join key (NOT NULL on both sides), so an unmatched
              left/right row leaves l.Category/r.Category as NULL respectively. */
           CASE WHEN l.Category IS NULL THEN 'Missing on Local'
                WHEN r.Category IS NULL THEN 'Missing on Remote'
                WHEN ISNULL(l.ValueText, N'') <> ISNULL(r.ValueText, N'') THEN 'Mismatch'
                ELSE 'Same' END                 AS DiffKind
    FROM   #LocalSnapshot l
    FULL OUTER JOIN #RemoteSnapshot r
      ON   l.Category = r.Category
      AND  l.Setting  = r.Setting
      AND  ISNULL(l.SubjectKey, N'') = ISNULL(r.SubjectKey, N'')
),
/* When an Index.Present row says "Missing on X" for some db.schema.table.indexname,
   the Definition/KeyColumns/InclColumns diffs for the same SubjectKey are redundant
   noise - they just restate "the thing isn't there". Suppress them so the user gets
   one clear "index is missing" signal instead of three. */
index_gaps AS (
    SELECT SubjectKey
    FROM   paired
    WHERE  Category = 'Index' AND Setting = 'Present' AND DiffKind LIKE 'Missing%'
)
INSERT INTO #Diff (Priority, Category, Setting, [Object], LocalValue, RemoteValue, Finding, [URL], Details)
SELECT  COALESCE(rb.Priority, rbDefault.Priority, 200)                                           AS Priority,
        p.Category,
        p.Setting,
        p.SubjectKey                                                                             AS [Object],
        p.LocalValue,
        p.RemoteValue,
        COALESCE(rb.Finding, rbDefault.Finding, 'Difference detected.')                          AS Finding,
        COALESCE(rb.[URL],   rbDefault.[URL],   'http://FirstResponderKit.org')                  AS [URL],
        p.DiffKind + ': ' + p.Category + '.' + p.Setting
            + ISNULL(' (' + p.SubjectKey + ')', '')                                              AS Details
FROM    paired p
LEFT JOIN #Rubric rb         ON rb.Category = p.Category AND rb.Setting  = p.Setting
LEFT JOIN #Rubric rbDefault  ON rbDefault.Category = p.Category AND rbDefault.Setting = '*'
WHERE   p.DiffKind <> 'Same'
  AND   NOT (p.Category = 'Index'
             AND p.Setting IN ('Definition', 'KeyColumns', 'InclColumns')
             AND p.SubjectKey IN (SELECT SubjectKey FROM index_gaps))
  /* 25% numeric-delta filter: suppress noise from small variations across runs.
       - LiveState:     server-level point-in-time metrics (blocking, workers, etc.)
       - PlanRuntime:   query runtime totals (elapsed, CPU, memory grant) - but NOT
                        PlanRuntime.SpillCount, where 0 vs 1 is a real signal.
       - PlanWait:      per-wait-type runtime waits from Query Store.
       - LiveWait:      per-wait-type waits from sys.dm_exec_session_wait_stats
                        on sessions currently running this query.
       - PlanAttribute: plan-compile-time numerics that jitter with hardware
                        (MaxCompileMemory, EstimatedAvailableMemoryGrant,
                        EstimatedPagesCached, CompileTime, CompileMemory, etc.).
                        Text attributes (PlanSource, StatementOptmLevel, etc.)
                        have ValueNumeric = NULL and bypass this filter.
                        CardinalityEstimationModelVersion is exact-match: any
                        delta matters (70 vs 160 vs 170 all change optimizer
                        behavior), even though it happens to be numeric.
     Rows present on only one side bypass this check since LocalValueNumeric or
     RemoteValueNumeric is NULL - a missing wait type / attribute is still signal. */
  AND   NOT (p.Category IN ('LiveState', 'PlanRuntime', 'PlanWait', 'LiveWait', 'PlanAttribute')
             AND NOT (p.Category = 'PlanRuntime'   AND p.Setting = 'SpillCount')
             AND NOT (p.Category = 'PlanAttribute' AND p.Setting = 'CardinalityEstimationModelVersion')
             AND p.LocalValueNumeric  IS NOT NULL
             AND p.RemoteValueNumeric IS NOT NULL
             AND ABS(p.LocalValueNumeric - p.RemoteValueNumeric)
                 <= 0.25 * NULLIF(
                     CASE WHEN ABS(p.LocalValueNumeric) >= ABS(p.RemoteValueNumeric)
                          THEN ABS(p.LocalValueNumeric)
                          ELSE ABS(p.RemoteValueNumeric) END,
                     0));

/* Echo header info as Priority-200 metadata so it's always present.
   The QueryHash should be the same on both sides (it's the join key that found the plans);
   the QueryPlanHash is the headline diff - if it's different, the two servers literally
   compiled different plans, which is the whole mystery we're explaining. */
INSERT INTO #Diff (Priority, Category, Setting, [Object], LocalValue, RemoteValue, Finding, [URL])
SELECT 200, 'Meta', 'QueryHash', NULL,
       CONVERT(NVARCHAR(50), @QueryHashBin, 1),
       @RemoteXml.value('(/BlitzPlanCompareSnapshot/@QueryHash)[1]', 'NVARCHAR(50)'),
       'Query hash (the logical query). Should be the same on both sides.', 'http://FirstResponderKit.org';

INSERT INTO #Diff (Priority, Category, Setting, [Object], LocalValue, RemoteValue, Finding, [URL])
SELECT 200, 'Meta', 'QueryPlanHash', NULL,
       CONVERT(NVARCHAR(50), @QueryPlanHash, 1),
       @RemoteXml.value('(/BlitzPlanCompareSnapshot/@QueryPlanHash)[1]', 'NVARCHAR(50)'),
       'Plan hash on each server. Different values mean the optimizer picked different plans - the whole reason you''re comparing.', 'http://FirstResponderKit.org';

INSERT INTO #Diff (Priority, Category, Setting, [Object], LocalValue, RemoteValue, Finding, [URL])
SELECT 200, 'Meta', 'GeneratedUTC', NULL,
       CONVERT(NVARCHAR(33), SYSUTCDATETIME(), 126),
       @RemoteXml.value('(/BlitzPlanCompareSnapshot/@GeneratedUTC)[1]', 'NVARCHAR(33)'),
       'Snapshot timestamps (local now vs remote when emitted).', 'http://FirstResponderKit.org';

INSERT INTO #Diff (Priority, Category, Setting, [Object], LocalValue, RemoteValue, Finding, [URL])
SELECT 0, 'Meta', 'SourceServer', NULL,
       @LocalServerName,
       @RemoteXml.value('(/BlitzPlanCompareSnapshot/@SourceServer)[1]', 'NVARCHAR(256)'),
       'Server names (local vs remote).', 'http://FirstResponderKit.org';

/* ============================================================================
   Parameter-sniffing reproducer
   ---------------------------------------------------------------------------
   When any parameter compiled to different values on the two servers, give the
   user a copy/paste-ready EXEC (or sp_executesql) for each side that reproduces
   the scenario. The reproducer is emitted as a typed XML cell so SSMS renders
   it clickable - opening the cell shows two <Local>/<Remote> blocks with the
   full call to run on each server.

   We only emit the reproducer when the call is easy to reproduce:
     - Stored procedure / function  -> EXEC [db].[schema].[name] @p1 = v, ...
     - Ad-hoc parameterized SQL     -> EXEC sp_executesql N'...', N'@p1 int', @p1 = v
   Anything where we can't build a deterministic reproducer (no sql_handle,
   missing proc metadata, etc.) falls through silently.
   ============================================================================ */

DECLARE @RemotePlanXml XML = @RemoteXml.query('/BlitzPlanCompareSnapshot/Plan/*');
DECLARE @ProcObjectId INT, @ProcDbId INT, @ProcName NVARCHAR(600), @SqlText NVARCHAR(MAX);

BEGIN TRY
    SELECT @ProcObjectId = st.objectid,
           @ProcDbId     = st.dbid,
           @SqlText      = st.text
    FROM   sys.dm_exec_sql_text(@SqlHandle) AS st;

    IF @ProcObjectId IS NOT NULL
        SET @ProcName = QUOTENAME(ISNULL(DB_NAME(@ProcDbId), DB_NAME())) + N'.'
                      + QUOTENAME(ISNULL(OBJECT_SCHEMA_NAME(@ProcObjectId, @ProcDbId), N'dbo')) + N'.'
                      + QUOTENAME(OBJECT_NAME(@ProcObjectId, @ProcDbId));
END TRY
BEGIN CATCH
    /* permission or expired handle - we'll fall back to the sp_executesql form */
    SET @ProcName = NULL;
END CATCH;

/* Shred remote plan's <ParameterList> into its own temp table. We mirror the
   local #PlanParameters schema so we can join by ParameterName. DISTINCT
   because multi-statement procs list the same parameters per statement. */
IF OBJECT_ID('tempdb..#RemotePlanParameters') IS NOT NULL DROP TABLE #RemotePlanParameters;
CREATE TABLE #RemotePlanParameters (
    ParameterName SYSNAME        NOT NULL,
    DataType      NVARCHAR(128)  NULL,
    CompiledValue NVARCHAR(4000) NULL,
    RuntimeValue  NVARCHAR(4000) NULL
);

BEGIN TRY
    IF @RemotePlanXml IS NOT NULL
    BEGIN
        ;WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
        INSERT INTO #RemotePlanParameters (ParameterName, DataType, CompiledValue, RuntimeValue)
        SELECT DISTINCT
               pr.n.value('@Column',                 'SYSNAME'),
               pr.n.value('@ParameterDataType',      'NVARCHAR(128)'),
               pr.n.value('@ParameterCompiledValue', 'NVARCHAR(4000)'),
               pr.n.value('@ParameterRuntimeValue',  'NVARCHAR(4000)')
        FROM   @RemotePlanXml.nodes('//p:ParameterList/p:ColumnReference') AS pr(n);
    END;
END TRY
BEGIN CATCH
    /* malformed remote plan - reproducer will be skipped below */
END CATCH;

/* Only proceed if BOTH sides have parameters AND at least one compiled value
   actually differs. No difference = no sniffing signal = no reproducer. */
IF EXISTS (
    SELECT 1
    FROM       #PlanParameters      l
    INNER JOIN #RemotePlanParameters r ON r.ParameterName = l.ParameterName
    WHERE ISNULL(l.CompiledValue, N'') <> ISNULL(r.CompiledValue, N'')
)
BEGIN
    DECLARE @LocalExec  NVARCHAR(MAX),
            @RemoteExec NVARCHAR(MAX),
            @ParamDecls NVARCHAR(MAX),
            @UseDb      NVARCHAR(400);

    SET @UseDb = N'USE ' + QUOTENAME(ISNULL(DB_NAME(@ProcDbId), DB_NAME())) + N';' + CHAR(13) + CHAR(10);

    IF @ProcName IS NOT NULL
    BEGIN
        /* Proc / function form: EXEC [db].[schema].[name] @p = v, ... */
        SELECT @LocalExec = @UseDb + N'EXEC ' + @ProcName
            + ISNULL(STUFF((
                SELECT N',' + CHAR(13) + CHAR(10)
                     + N'    '
                     + CASE WHEN LEFT(ParameterName, 1) = N'@' THEN ParameterName ELSE N'@' + ParameterName END
                     + N' = '
                     + ISNULL(CompiledValue, N'NULL')
                FROM   #PlanParameters
                ORDER BY ParameterName
                FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 1, N''), N'')
            + N';';

        SELECT @RemoteExec = @UseDb + N'EXEC ' + @ProcName
            + ISNULL(STUFF((
                SELECT N',' + CHAR(13) + CHAR(10)
                     + N'    '
                     + CASE WHEN LEFT(ParameterName, 1) = N'@' THEN ParameterName ELSE N'@' + ParameterName END
                     + N' = '
                     + ISNULL(CompiledValue, N'NULL')
                FROM   #RemotePlanParameters
                ORDER BY ParameterName
                FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 1, N''), N'')
            + N';';
    END
    ELSE IF @SqlText IS NOT NULL
    BEGIN
        /* Ad-hoc parameterized SQL form: EXEC sp_executesql N'...', N'@p int', @p = v */
        SELECT @ParamDecls = STUFF((
            SELECT N', '
                 + CASE WHEN LEFT(ParameterName, 1) = N'@' THEN ParameterName ELSE N'@' + ParameterName END
                 + N' ' + ISNULL(DataType, N'sql_variant')
            FROM   #PlanParameters
            ORDER BY ParameterName
            FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 2, N'');

        SET @LocalExec = @UseDb + N'EXEC sp_executesql N'''
            + REPLACE(@SqlText, N'''', N'''''')
            + N''','
            + CHAR(13) + CHAR(10) + N'    N''' + ISNULL(@ParamDecls, N'') + N''''
            + ISNULL(STUFF((
                SELECT N',' + CHAR(13) + CHAR(10)
                     + N'    '
                     + CASE WHEN LEFT(ParameterName, 1) = N'@' THEN ParameterName ELSE N'@' + ParameterName END
                     + N' = '
                     + ISNULL(CompiledValue, N'NULL')
                FROM   #PlanParameters
                ORDER BY ParameterName
                FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 1, N''), N'')
            + N';';

        SET @RemoteExec = @UseDb + N'EXEC sp_executesql N'''
            + REPLACE(@SqlText, N'''', N'''''')
            + N''','
            + CHAR(13) + CHAR(10) + N'    N''' + ISNULL(@ParamDecls, N'') + N''''
            + ISNULL(STUFF((
                SELECT N',' + CHAR(13) + CHAR(10)
                     + N'    '
                     + CASE WHEN LEFT(ParameterName, 1) = N'@' THEN ParameterName ELSE N'@' + ParameterName END
                     + N' = '
                     + ISNULL(CompiledValue, N'NULL')
                FROM   #RemotePlanParameters
                ORDER BY ParameterName
                FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 1, N''), N'')
            + N';';
    END;

    /* Build the clickable XML. The column type is XML so SSMS renders it as a
       hyperlink that opens in a new tab, but the CONTENT is plain T-SQL (with
       comments and blank lines). We use a single root element whose text body
       is the full reproducer script - no nested XML structure - so the SSMS
       XML tab shows readable SQL with proper line breaks. FOR XML entity-encodes
       any '<', '>', '&' that appear in literal parameter values. */
    IF @LocalExec IS NOT NULL AND @RemoteExec IS NOT NULL
    BEGIN
        DECLARE @RemoteServerName NVARCHAR(256) =
            @RemoteXml.value('(/BlitzPlanCompareSnapshot/@SourceServer)[1]', 'NVARCHAR(256)');
        DECLARE @CRLF NCHAR(2) = CHAR(13) + CHAR(10);
        /* Leading + trailing @CRLF + @CRLF puts blank lines between the XML
           open/close tags and the T-SQL body so the user can triple-click-drag
           just the code rows without grabbing the wrapper tags. */
        DECLARE @ReproText NVARCHAR(MAX) =
              @CRLF + @CRLF
            + N'/* Local server ' + ISNULL(@LocalServerName, N'(unknown)') + N' */' + @CRLF
            + @LocalExec + @CRLF
            + @CRLF
            + N'/* Remote server ' + ISNULL(@RemoteServerName, N'(unknown)') + N' */' + @CRLF
            + @RemoteExec
            + @CRLF + @CRLF;

        DECLARE @Repro XML =
            (SELECT @ReproText AS [*] FOR XML PATH('ParameterSniffingReproducer'), TYPE);

        INSERT INTO #Diff (Priority, Category, Setting, [Object], LocalValue, RemoteValue, Finding, [URL], Details, CallStack)
        VALUES (
            10, 'Parameter', 'Reproducer', NULL,
            N'(see CallStack)',
            N'(see CallStack)',
            N'Parameters compiled to different values on each server - textbook parameter sniffing. Click the CallStack cell to open copy/paste-ready EXECs for both servers.',
            N'http://FirstResponderKit.org',
            CASE WHEN @ProcName IS NOT NULL
                 THEN N'Proc: ' + @ProcName
                 ELSE N'Ad-hoc parameterized SQL (sp_executesql form)' END,
            @Repro);
    END;
END;

/* ============================================================================
   Operator-by-operator row variance analysis.

   Strategy:
   1. Shred both plans into (NodeId, ParentNodeId, PhysicalOp, LogicalOp,
      ObjectSig, ActualRows, EstRows, HasRuntime).
   2. "Same shape" = identical operator count + matching (PhysicalOp, LogicalOp,
      ObjectSig) per NodeId. This is a stronger definition than FRK's usual
      plan-signature check because it also requires object references to match.
   3. If BOTH plans have <RunTimeInformation> on any operator, we use ActualRows
      (the "last actual plan" case). Otherwise we fall back to EstimateRows.
      Mixing "actual on one side, estimated on the other" would be misleading;
      we treat that as estimated-only.
   4. Traversal order for same-shape plans is post-order with outer-child-first
      siblings - i.e. start at the top-right leaf in SSMS's diagram, then walk
      right-to-left along that branch, then drop down top-to-bottom to the
      next branch, finally ending at the root (top-left SELECT/INSERT/...).
      In pre-order-numbered NodeIds (which ShowPlanXML uses), post-order =
      ORDER BY MaxNodeIdInSubtree ASC, NodeId DESC.
   5. If shapes DIFFER, we only compare the root operator (NodeId = 0, the
      final SELECT/INSERT/UPDATE/DELETE). Per-operator comparison is meaningless
      when operators don't line up.
   6. "Variance" threshold = 25% of the larger magnitude, matching the existing
      LiveState / PlanRuntime noise suppression.
   ============================================================================ */

IF OBJECT_ID('tempdb..#OpsLocal')  IS NOT NULL DROP TABLE #OpsLocal;
IF OBJECT_ID('tempdb..#OpsRemote') IS NOT NULL DROP TABLE #OpsRemote;
CREATE TABLE #OpsLocal (
    NodeId       INT             NOT NULL,
    ParentNodeId INT             NULL,
    PhysicalOp   VARCHAR(200)    NULL,
    LogicalOp    VARCHAR(200)    NULL,
    ObjectSig    NVARCHAR(4000)  NULL,
    ActualRows   BIGINT          NULL,
    EstRows      FLOAT           NULL,
    HasRuntime   BIT             NOT NULL DEFAULT 0,
    MaxInSubtree INT             NULL
);
CREATE TABLE #OpsRemote (
    NodeId       INT             NOT NULL,
    ParentNodeId INT             NULL,
    PhysicalOp   VARCHAR(200)    NULL,
    LogicalOp    VARCHAR(200)    NULL,
    ObjectSig    NVARCHAR(4000)  NULL,
    ActualRows   BIGINT          NULL,
    EstRows      FLOAT           NULL,
    HasRuntime   BIT             NOT NULL DEFAULT 0,
    MaxInSubtree INT             NULL
);

/* @RemotePlanXml was declared above during the parameter-sniffing reproducer build. */

/* Shred local plan. TRY/CATCH because a malformed plan shouldn't tank the whole run.
   SQL Server's XQuery doesn't support the ancestor axis, so we use parent-driven
   traversal: enumerate RelOps, then CROSS APPLY to each RelOp's direct-child
   operator wrapper to find its child RelOps and Objects. The XPath step
   (star)/p:RelOp walks RelOp -> any-element -> RelOp, which is how ShowPlanXML nests. */
BEGIN TRY
    IF @PlanXmlForEmit IS NOT NULL AND @PlanXmlForEmit.exist('//*[local-name(.)="RelOp"]') = 1
    BEGIN
        ;WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
        INSERT INTO #OpsLocal (NodeId, PhysicalOp, LogicalOp, ActualRows, EstRows, HasRuntime)
        SELECT
            rop.n.value('@NodeId',     'INT'),
            rop.n.value('@PhysicalOp', 'VARCHAR(200)'),
            rop.n.value('@LogicalOp',  'VARCHAR(200)'),
            rop.n.value('sum(p:RunTimeInformation/p:RunTimeCountersPerThread/@ActualRows)', 'BIGINT'),
            rop.n.value('@EstimateRows', 'FLOAT'),
            CASE WHEN rop.n.exist('p:RunTimeInformation') = 1 THEN 1 ELSE 0 END
        FROM @PlanXmlForEmit.nodes('//p:RelOp') AS rop(n);

        -- Parent->child linkage via RelOp/<operator>/RelOp
        ;WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p),
        pc AS (
            SELECT par.n.value('@NodeId', 'INT') AS ParentId,
                   ch.n.value('@NodeId', 'INT')  AS ChildId
            FROM       @PlanXmlForEmit.nodes('//p:RelOp') AS par(n)
            CROSS APPLY par.n.nodes('*/p:RelOp')          AS ch(n)
        )
        UPDATE child SET ParentNodeId = pc.ParentId
        FROM #OpsLocal child JOIN pc ON pc.ChildId = child.NodeId;

        -- Object signature per NodeId via RelOp/<operator>/Object - catches
        -- Scan/Seek/Update objects which are direct grandchildren of their owning RelOp.
        ;WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p),
        obj AS (
            SELECT
                par.n.value('@NodeId', 'INT') AS NodeId,
                ISNULL(o.n.value('@Database', 'NVARCHAR(258)'), '') + '.'
              + ISNULL(o.n.value('@Schema',   'NVARCHAR(258)'), '') + '.'
              + ISNULL(o.n.value('@Table',    'NVARCHAR(258)'), '') + '.'
              + ISNULL(o.n.value('@Index',    'NVARCHAR(258)'), '') AS Sig
            FROM       @PlanXmlForEmit.nodes('//p:RelOp') AS par(n)
            CROSS APPLY par.n.nodes('*/p:Object')         AS o(n)
        )
        UPDATE l SET ObjectSig = x.Agg
        FROM #OpsLocal l
        CROSS APPLY (
            SELECT STUFF((SELECT '|' + o.Sig FROM obj o WHERE o.NodeId = l.NodeId
                          ORDER BY o.Sig FOR XML PATH('')), 1, 1, '') AS Agg
        ) x;
    END;
END TRY
BEGIN CATCH
    IF @Debug = 1
    BEGIN
        SET @ErrMessage = ERROR_MESSAGE();
        RAISERROR('Local plan shredding for operator variance failed: %s', 0, 1, @ErrMessage) WITH NOWAIT;
    END;
END CATCH;

/* Shred remote plan - same pattern. */
BEGIN TRY
    IF @RemotePlanXml IS NOT NULL AND @RemotePlanXml.exist('//*[local-name(.)="RelOp"]') = 1
    BEGIN
        ;WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
        INSERT INTO #OpsRemote (NodeId, PhysicalOp, LogicalOp, ActualRows, EstRows, HasRuntime)
        SELECT
            rop.n.value('@NodeId',     'INT'),
            rop.n.value('@PhysicalOp', 'VARCHAR(200)'),
            rop.n.value('@LogicalOp',  'VARCHAR(200)'),
            rop.n.value('sum(p:RunTimeInformation/p:RunTimeCountersPerThread/@ActualRows)', 'BIGINT'),
            rop.n.value('@EstimateRows', 'FLOAT'),
            CASE WHEN rop.n.exist('p:RunTimeInformation') = 1 THEN 1 ELSE 0 END
        FROM @RemotePlanXml.nodes('//p:RelOp') AS rop(n);

        ;WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p),
        pc AS (
            SELECT par.n.value('@NodeId', 'INT') AS ParentId,
                   ch.n.value('@NodeId', 'INT')  AS ChildId
            FROM       @RemotePlanXml.nodes('//p:RelOp') AS par(n)
            CROSS APPLY par.n.nodes('*/p:RelOp')         AS ch(n)
        )
        UPDATE child SET ParentNodeId = pc.ParentId
        FROM #OpsRemote child JOIN pc ON pc.ChildId = child.NodeId;

        ;WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p),
        obj AS (
            SELECT
                par.n.value('@NodeId', 'INT') AS NodeId,
                ISNULL(o.n.value('@Database', 'NVARCHAR(258)'), '') + '.'
              + ISNULL(o.n.value('@Schema',   'NVARCHAR(258)'), '') + '.'
              + ISNULL(o.n.value('@Table',    'NVARCHAR(258)'), '') + '.'
              + ISNULL(o.n.value('@Index',    'NVARCHAR(258)'), '') AS Sig
            FROM       @RemotePlanXml.nodes('//p:RelOp') AS par(n)
            CROSS APPLY par.n.nodes('*/p:Object')        AS o(n)
        )
        UPDATE r SET ObjectSig = x.Agg
        FROM #OpsRemote r
        CROSS APPLY (
            SELECT STUFF((SELECT '|' + o.Sig FROM obj o WHERE o.NodeId = r.NodeId
                          ORDER BY o.Sig FOR XML PATH('')), 1, 1, '') AS Agg
        ) x;
    END;
END TRY
BEGIN CATCH
    IF @Debug = 1
    BEGIN
        SET @ErrMessage = ERROR_MESSAGE();
        RAISERROR('Remote plan shredding for operator variance failed: %s', 0, 1, @ErrMessage) WITH NOWAIT;
    END;
END CATCH;

/* Compute MaxInSubtree (rightmost descendant NodeId) for post-order traversal. */
IF EXISTS (SELECT 1 FROM #OpsLocal)
BEGIN
    ;WITH subtree AS (
        SELECT NodeId AS RootId, NodeId AS DescId FROM #OpsLocal
        UNION ALL
        SELECT s.RootId, c.NodeId
        FROM subtree s
        JOIN #OpsLocal c ON c.ParentNodeId = s.DescId
    )
    UPDATE l SET MaxInSubtree = x.MaxDesc
    FROM #OpsLocal l
    CROSS APPLY (SELECT MAX(DescId) AS MaxDesc FROM subtree WHERE RootId = l.NodeId) x
    OPTION (MAXRECURSION 0);
END;

IF EXISTS (SELECT 1 FROM #OpsRemote)
BEGIN
    ;WITH subtree AS (
        SELECT NodeId AS RootId, NodeId AS DescId FROM #OpsRemote
        UNION ALL
        SELECT s.RootId, c.NodeId
        FROM subtree s
        JOIN #OpsRemote c ON c.ParentNodeId = s.DescId
    )
    UPDATE r SET MaxInSubtree = x.MaxDesc
    FROM #OpsRemote r
    CROSS APPLY (SELECT MAX(DescId) AS MaxDesc FROM subtree WHERE RootId = r.NodeId) x
    OPTION (MAXRECURSION 0);
END;

/* Shape-match + actual-vs-estimated mode. */
DECLARE @ShapeMatch BIT = 0, @BothActual BIT = 0;
DECLARE @Metric VARCHAR(20) = 'EstimateRows';
DECLARE @LocalOpCount  INT = (SELECT COUNT(*) FROM #OpsLocal);
DECLARE @RemoteOpCount INT = (SELECT COUNT(*) FROM #OpsRemote);

IF @LocalOpCount > 0 AND @RemoteOpCount > 0 AND @LocalOpCount = @RemoteOpCount
AND NOT EXISTS (
    SELECT 1
    FROM       #OpsLocal  l
    FULL OUTER JOIN #OpsRemote r ON r.NodeId = l.NodeId
    WHERE l.NodeId IS NULL OR r.NodeId IS NULL
       OR ISNULL(l.PhysicalOp, '') <> ISNULL(r.PhysicalOp, '')
       OR ISNULL(l.LogicalOp,  '') <> ISNULL(r.LogicalOp,  '')
       OR ISNULL(l.ObjectSig,  '') <> ISNULL(r.ObjectSig,  '')
)
    SET @ShapeMatch = 1;

IF EXISTS (SELECT 1 FROM #OpsLocal  WHERE HasRuntime = 1)
AND EXISTS (SELECT 1 FROM #OpsRemote WHERE HasRuntime = 1)
BEGIN
    SET @BothActual = 1;
    SET @Metric = 'ActualRows';
END;

/* Informational row: shape match result + metric used. */
IF @LocalOpCount > 0 AND @RemoteOpCount > 0
BEGIN
    INSERT INTO #Diff (Priority, Category, Setting, [Object], LocalValue, RemoteValue, Finding, [URL], Details)
    VALUES (
        100, 'PlanShape', 'Shape', NULL,
        CAST(@LocalOpCount  AS NVARCHAR(10)) + ' operators',
        CAST(@RemoteOpCount AS NVARCHAR(10)) + ' operators',
        CASE WHEN @ShapeMatch = 1
             THEN 'The two plans have the same shape - same operators on the same objects in the same order. Row counts are compared per operator.'
             ELSE 'The two plans have different shapes. Only the final (top-left) operator is compared, since per-operator diffs are not meaningful when operators do not line up.' END,
        'http://FirstResponderKit.org',
        'ShapeMatch=' + CASE WHEN @ShapeMatch = 1 THEN 'yes' ELSE 'no' END
        + ' / Metric=' + @Metric
        + ' / BothActual=' + CASE WHEN @BothActual = 1 THEN 'yes' ELSE 'no' END);
END;

/* First operator exceeding the 25% variance threshold. */
IF @LocalOpCount > 0 AND @RemoteOpCount > 0
BEGIN
    ;WITH pairs AS (
        SELECT l.NodeId, l.PhysicalOp, l.LogicalOp, l.ObjectSig, l.MaxInSubtree,
               CASE WHEN @BothActual = 1 THEN l.ActualRows ELSE CAST(l.EstRows AS BIGINT) END AS LocalMetric,
               CASE WHEN @BothActual = 1 THEN r.ActualRows ELSE CAST(r.EstRows AS BIGINT) END AS RemoteMetric
        FROM       #OpsLocal  l
        INNER JOIN #OpsRemote r ON r.NodeId = l.NodeId
    ),
    flagged AS (
        SELECT *,
               CASE
                   WHEN LocalMetric IS NULL OR RemoteMetric IS NULL THEN 0
                   WHEN ABS(LocalMetric - RemoteMetric)
                        > 0.25 * (CASE WHEN ABS(LocalMetric) >= ABS(RemoteMetric)
                                       THEN ABS(LocalMetric) ELSE ABS(RemoteMetric) END)
                        THEN 1
                   ELSE 0
               END AS IsVariance
        FROM pairs
    )
    INSERT INTO #Diff (Priority, Category, Setting, [Object], LocalValue, RemoteValue, Finding, [URL], Details)
    SELECT TOP 1
           CASE WHEN @ShapeMatch = 1 THEN 5 ELSE 15 END,
           'OperatorVariance',
           @Metric,
           'NodeId ' + CAST(NodeId AS VARCHAR(10)) + ': ' + ISNULL(PhysicalOp, '(unknown)')
            + CASE WHEN NULLIF(ObjectSig, '') IS NOT NULL THEN ' on ' + ObjectSig ELSE '' END,
           CAST(LocalMetric  AS NVARCHAR(40)),
           CAST(RemoteMetric AS NVARCHAR(40)),
           CASE WHEN @ShapeMatch = 1
                THEN 'Walking the plan right-to-left, top-to-bottom, this is the first operator whose ' + @Metric
                     + ' differs by 25% or more. Upstream operators match; divergence starts here - this is where to focus.'
                ELSE 'Plans have different shapes, so only the root (final SELECT/INSERT/UPDATE/DELETE) operator was compared. Its '
                     + @Metric + ' differs by 25% or more across servers.' END,
           'http://FirstResponderKit.org',
           'Physical=' + ISNULL(PhysicalOp, '(null)')
           + ' / Logical=' + ISNULL(LogicalOp, '(null)')
           + ' / ObjectSig=' + ISNULL(NULLIF(ObjectSig, ''), '(none)')
    FROM flagged
    WHERE IsVariance = 1
      AND (@ShapeMatch = 1 OR NodeId = 0)
    ORDER BY
        CASE WHEN @ShapeMatch = 1 THEN MaxInSubtree ELSE NodeId END ASC,
        NodeId DESC;
END;

/* Final result set. CallStack is typed XML so SSMS renders it as a clickable
   cell; only the Parameter.Reproducer row populates it today.

   Sort: Priority, Category, Setting first (as before). For wait-stat rows
   (LiveWait + PlanWait), the tiebreaker is the larger of LocalValueNumeric /
   RemoteValueNumeric descending so the heaviest wait floats to the top of its
   group. For everything else the tiebreaker is [Object] alphabetical. That
   keeps PAGEIOLATCH_SH (10M ms) above CXPACKET (47 ms) instead of burying it
   under alphabetically-earlier but numerically-smaller waits. */
SELECT  Priority,
        Category,
        Setting,
        [Object],
        LocalValue,
        RemoteValue,
        Finding,
        [URL],
        Details,
        CallStack
FROM    #Diff
ORDER BY Priority, Category, Setting,
         /* Wait categories: sort by the bigger numeric DESC so top waits surface first.
            Other categories: NULL sort key (falls through to [Object] below). */
         CASE WHEN Category IN ('LiveWait', 'PlanWait')
              THEN - (CASE WHEN ISNULL(TRY_CAST(LocalValue  AS DECIMAL(38, 4)), 0)
                                >= ISNULL(TRY_CAST(RemoteValue AS DECIMAL(38, 4)), 0)
                           THEN ISNULL(TRY_CAST(LocalValue  AS DECIMAL(38, 4)), 0)
                           ELSE ISNULL(TRY_CAST(RemoteValue AS DECIMAL(38, 4)), 0) END)
              ELSE NULL END,
         [Object];

/* ---------------------------------------------------------------------------
   Second result set: the actual query plans we analyzed, one row per server.
   QueryPlan is a typed XML column so SSMS renders it as a clickable plan
   diagram when you click the cell. This lets the user eyeball both plans
   side-by-side after reading the diff above. Works for every mode (mode 2
   compare-from-XML and mode 3 linked-server included) because the remote
   plan is always embedded in the snapshot XML under /BlitzPlanCompareSnapshot/Plan.
   --------------------------------------------------------------------------- */
SELECT  CAST('Local' AS VARCHAR(10))       AS [Server],
        CAST(@LocalServerName AS NVARCHAR(256)) AS ServerName,
        CAST(@PlanSource AS NVARCHAR(50))  AS PlanSource,
        @PlanXmlForEmit                    AS QueryPlan
UNION ALL
SELECT  'Remote',
        @RemoteXml.value('(/BlitzPlanCompareSnapshot/@SourceServer)[1]', 'NVARCHAR(256)'),
        ISNULL(
            @RemoteXml.value(
                '(/BlitzPlanCompareSnapshot/Snapshot/Row[@Category="PlanAttribute" and @Setting="PlanSource"]/@ValueText)[1]',
                'NVARCHAR(50)'),
            '(unknown)'),
        @RemoteXml.query('/BlitzPlanCompareSnapshot/Plan/*');

END;
GO
