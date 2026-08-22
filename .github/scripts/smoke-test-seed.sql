/*
Seeds the throwaway CI SQL Server so the First Responder Kit scripts have
something real to look at.

A stock container has no user databases, no backup history, no plan cache and
no index usage, so most checks bind against empty tables and report nothing.
That still catches runtime errors, but it does not exercise much. This builds
the smallest state that makes the scripts do real work.

Runs once per CI job, before the kit is installed. Nothing in here should depend
on the version of the kit being tested.
*/
SET NOCOUNT ON;
GO

/* ---------------------------------------------------------------------------
   Test database
   --------------------------------------------------------------------------- */
IF DB_ID('FRKSmokeTest') IS NOT NULL
BEGIN
    ALTER DATABASE FRKSmokeTest SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE FRKSmokeTest;
END;
GO

CREATE DATABASE FRKSmokeTest;
GO

/* FULL recovery so log backups are legal and sp_BlitzBackups has an RPO to report */
ALTER DATABASE FRKSmokeTest SET RECOVERY FULL;
GO

USE FRKSmokeTest;
GO

CREATE TABLE dbo.Users
(
    Id           INT IDENTITY(1,1) NOT NULL CONSTRAINT PK_Users PRIMARY KEY CLUSTERED,
    DisplayName  NVARCHAR(40)  NULL,
    Reputation   INT           NOT NULL,
    CreationDate DATETIME      NOT NULL
);

CREATE TABLE dbo.Posts
(
    Id           INT IDENTITY(1,1) NOT NULL CONSTRAINT PK_Posts PRIMARY KEY CLUSTERED,
    OwnerUserId  INT           NULL,
    Score        INT           NOT NULL,
    CreationDate DATETIME      NOT NULL,
    Title        NVARCHAR(250) NULL
);

/* A heap, so sp_BlitzIndex has a heap to complain about */
CREATE TABLE dbo.CommentsHeap
(
    Id     INT         NOT NULL,
    Body   NVARCHAR(200) NULL,
    Filler CHAR(100)   NOT NULL
);
GO

/* CHECKSUM(NEWID()) can return -2147483648, and ABS() of INT_MIN overflows --
   which would abort the seed, and with it the whole job, at random. Widening to
   BIGINT before ABS keeps that from turning CI flaky. */
INSERT dbo.Users (DisplayName, Reputation, CreationDate)
SELECT TOP (2000)
       LEFT(CONVERT(NVARCHAR(40), NEWID()), 20),
       ABS(CAST(CHECKSUM(NEWID()) AS BIGINT)) % 50000,
       DATEADD(DAY, -(ABS(CAST(CHECKSUM(NEWID()) AS BIGINT)) % 3000), GETDATE())
FROM sys.all_objects a
CROSS JOIN sys.all_objects b;

INSERT dbo.Posts (OwnerUserId, Score, CreationDate, Title)
SELECT TOP (5000)
       ABS(CAST(CHECKSUM(NEWID()) AS BIGINT)) % 2000 + 1,
       ABS(CAST(CHECKSUM(NEWID()) AS BIGINT)) % 100,
       DATEADD(DAY, -(ABS(CAST(CHECKSUM(NEWID()) AS BIGINT)) % 1000), GETDATE()),
       LEFT(CONVERT(NVARCHAR(250), NEWID()), 50)
FROM sys.all_objects a
CROSS JOIN sys.all_objects b;

INSERT dbo.CommentsHeap (Id, Body, Filler)
SELECT TOP (1000)
       ROW_NUMBER() OVER (ORDER BY (SELECT NULL)),
       LEFT(CONVERT(NVARCHAR(200), NEWID()), 40),
       'x'
FROM sys.all_objects a
CROSS JOIN sys.all_objects b;
GO

/* Deliberately duplicate + unused indexes so sp_BlitzIndex has findings */
CREATE INDEX IX_Users_Reputation      ON dbo.Users (Reputation);
CREATE INDEX IX_Users_Reputation_Dupe ON dbo.Users (Reputation);
CREATE INDEX IX_Posts_OwnerUserId     ON dbo.Posts (OwnerUserId) INCLUDE (Score);
CREATE INDEX IX_Posts_Score           ON dbo.Posts (Score);
GO

/* ---------------------------------------------------------------------------
   Plan cache activity, so sp_BlitzCache and sp_BlitzWho have something to read
   --------------------------------------------------------------------------- */
DECLARE @i INT = 0;
WHILE @i < 20
BEGIN
    SELECT TOP (100) u.DisplayName, SUM(p.Score) AS TotalScore
    FROM dbo.Users AS u
    JOIN dbo.Posts AS p ON p.OwnerUserId = u.Id
    WHERE u.Reputation > @i * 100
    GROUP BY u.DisplayName
    ORDER BY TotalScore DESC;

    SELECT COUNT_BIG(*) FROM dbo.CommentsHeap WHERE Body LIKE N'A%';

    SET @i += 1;
END;
GO

USE master;
GO

/* ---------------------------------------------------------------------------
   Backup history, so sp_Blitz backup checks and sp_BlitzBackups have data,
   and sp_DatabaseRestore has real files to enumerate.

   /var/opt/mssql/data always exists in the mssql Linux image; the container's
   own backup directory does not necessarily.
   --------------------------------------------------------------------------- */
BACKUP DATABASE FRKSmokeTest
    TO DISK = '/var/opt/mssql/data/FRKSmokeTest_Full.bak'
    WITH INIT, FORMAT, NAME = 'FRKSmokeTest full';
GO

BACKUP LOG FRKSmokeTest
    TO DISK = '/var/opt/mssql/data/FRKSmokeTest_Log.trn'
    WITH INIT, FORMAT, NAME = 'FRKSmokeTest log';
GO

/* A second full backup so backup-history checks see more than one row */
BACKUP DATABASE FRKSmokeTest
    TO DISK = '/var/opt/mssql/data/FRKSmokeTest_Full2.bak'
    WITH INIT, FORMAT, NAME = 'FRKSmokeTest full 2';
GO

/* ---------------------------------------------------------------------------
   Checks CI skips, and why.

   CheckID 106 reads the live default trace with fn_trace_gettable. That file is
   being written continuously -- more so here, because the smoke test itself does
   constant DDL -- and a read that lands on a torn or rolling file raises Msg 568,
   which aborts the whole sp_Blitz run rather than just that check. It fired on
   one of six sp_Blitz calls in a single CI run, which would mean red builds on
   pull requests that changed nothing. Tracked in issue #4050; remove this row
   once that is fixed.

   Feeding it through @SkipChecksTable rather than hard-coding an exclusion has a
   side benefit: the skip-checks code path is itself exercised on every run.
   --------------------------------------------------------------------------- */
IF OBJECT_ID('FRKSmokeTest.dbo.BlitzChecksToSkip') IS NOT NULL
    DROP TABLE FRKSmokeTest.dbo.BlitzChecksToSkip;
GO

CREATE TABLE FRKSmokeTest.dbo.BlitzChecksToSkip
(
    DatabaseName NVARCHAR(128) NULL,
    CheckID      INT           NULL,
    ServerName   NVARCHAR(128) NULL
);
GO

INSERT FRKSmokeTest.dbo.BlitzChecksToSkip (DatabaseName, CheckID, ServerName)
VALUES (NULL, 106, NULL);  /* issue #4050 */
GO

PRINT 'Seed complete.';
GO
