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

/* Keep an empty skip table to exercise the @SkipChecksTable input path. */
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


/* A disabled login can be impersonated by the runner, but cannot log in over the network. */
USE master;
IF SUSER_ID(N'FRKSmokeLimited') IS NULL
BEGIN
    DECLARE @CreateLimitedLogin nvarchar(max) =
        N'CREATE LOGIN FRKSmokeLimited WITH PASSWORD = ''' + CONVERT(nvarchar(36), NEWID()) + N'aA1!'';';
    EXEC sys.sp_executesql @CreateLimitedLogin;
END;
ALTER LOGIN FRKSmokeLimited DISABLE;
GRANT VIEW SERVER STATE TO FRKSmokeLimited;
IF CONVERT(int, SERVERPROPERTY('ProductMajorVersion')) >= 16
    EXEC(N'GRANT VIEW SERVER PERFORMANCE STATE TO FRKSmokeLimited;');
IF USER_ID(N'FRKSmokeLimited') IS NULL
    CREATE USER FRKSmokeLimited FOR LOGIN FRKSmokeLimited;
DENY SELECT ON sys.extended_procedures TO FRKSmokeLimited;
DENY SELECT ON sys.master_files TO FRKSmokeLimited;
GO
USE msdb;
IF USER_ID(N'FRKSmokeLimited') IS NULL
    CREATE USER FRKSmokeLimited FOR LOGIN FRKSmokeLimited;
/* An absent user alone is insufficient: msdb normally allows guest access. */
DENY CONNECT TO FRKSmokeLimited;
GO
USE FRKSmokeTest;
IF USER_ID(N'FRKSmokeLimited') IS NULL
    CREATE USER FRKSmokeLimited FOR LOGIN FRKSmokeLimited;
IF OBJECT_ID(N'dbo.LimitedLoginChecksToSkip') IS NOT NULL
    DROP TABLE dbo.LimitedLoginChecksToSkip;
CREATE TABLE dbo.LimitedLoginChecksToSkip
(
    DatabaseName nvarchar(128) NULL,
    CheckID int NOT NULL PRIMARY KEY,
    ServerName nvarchar(128) NULL
);
/* The five hoisted probes whose skip guards this test protects. */
INSERT dbo.LimitedLoginChecksToSkip(CheckID) VALUES (202),(178),(105),(116),(191);
/* Other checks requiring the deliberately denied msdb/master metadata. */
INSERT dbo.LimitedLoginChecksToSkip(CheckID)
VALUES (1),(2),(3),(8),(90),(92),(93),(111),(119),(186),(232),(234),(236),(256);
GRANT SELECT ON dbo.LimitedLoginChecksToSkip TO FRKSmokeLimited;
GO
USE master;
/* Per-run session identity for the dedicated sp_kill victim. */
IF OBJECT_ID('FRKSmokeTest.dbo.KillVictim') IS NOT NULL
    DROP TABLE FRKSmokeTest.dbo.KillVictim;
CREATE TABLE FRKSmokeTest.dbo.KillVictim
(
    Token uniqueidentifier NOT NULL PRIMARY KEY,
    SessionId smallint NOT NULL,
    LoginTime datetime NOT NULL
);
GO

PRINT 'Seed complete.';
GO
