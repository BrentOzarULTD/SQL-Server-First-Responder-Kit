# SQL Server First Responder Kit
[![licence badge]][licence]
[![stars badge]][stars]
[![forks badge]][forks]
[![issues badge]][issues]

You're a DBA, sysadmin, or developer who manages Microsoft SQL Servers. It's your fault if they're down or slow. These tools help you understand what's going on in your server.

* When you want an overall health check, run [sp_Blitz](#sp_blitz-overall-health-check).
* To learn which queries have been using the most resources, run [sp_BlitzCache](#sp_blitzcache-find-the-most-resource-intensive-queries).
* To analyze which indexes are missing or slowing you down, run [sp_BlitzIndex](#sp_blitzindex-tune-your-indexes).
* To find out why the server is slow right now, run [sp_BlitzFirst](#sp_blitzfirst-real-time-performance-advice).

To install, [download the latest release ZIP](https://github.com/BrentOzarULTD/SQL-Server-First-Responder-Kit/releases), then run the SQL files in the master database. (You can use other databases if you prefer.)

Only Microsoft-supported versions of SQL Server are supported here - sorry, 2005 and 2000. Some of these may work some of the time on 2005, but no promises, and don't file a support issue when they fail.

## How to Get Support

Everyone here is expected to abide by the [Contributor Covenant Code of Conduct](CONTRIBUTING.md#the-contributor-covenant-code-of-conduct).

Want to talk to the developers? [Join SQLServer.slack.com](https://sql-server-slack.herokuapp.com/), and we're in the [#FirstResponderKit channel](https://sqlserver.slack.com/messages/firstresponderkit/).

Got a question? Ask it on [DBA.StackExchange.com](http://dba.stackexchange.com). Tag your question with the script name, like sp_Blitz, sp_BlitzCache, sp_BlitzIndex, etc, and we’ll be alerted of it right away.

Want to contribute by writing, testing, or documenting code, or suggesting a new check? [Read the contributing.md file](CONTRIBUTING.md).

## sp_Blitz: Overall Health Check

Run sp_Blitz daily or weekly for an overall health check. Just run it from SQL Server Management Studio, and you'll get a prioritized list of issues on your server right now:

![sp_Blitz](http://u.brentozar.com/github-images/sp_Blitz.png)

Output columns include:

* Priority - 1 is the most urgent, stuff that could get you fired. The warnings get progressively less urgent.
* FindingsGroup, Findings - describe the problem sp_Blitz found on the server.
* DatabaseName - the database having the problem. If it's null, it's a server-wide problem.
* URL - copy/paste this into a browser for more information.
* Details - not just bland text, but dynamically generated stuff with more info.

Commonly used parameters:

* @CheckUserDatabaseObjects = 0 - by default, we check inside user databases for things like triggers or heaps. Turn this off (0) to make checks go faster, or ignore stuff you can't fix if you're managing third party databases. If a server has 50+ databases, @CheckUserDatabaseObjects is automatically turned off unless...
* @BringThePain = 1 - required if you want to run @CheckUserDatabaseObjects = 1 with over 50 databases. It's gonna be slow.
* @CheckServerInfo = 1 - includes additional rows at priority 250 with server configuration details like service accounts. 
* @IgnorePrioritiesAbove = 50 - if you want a daily bulletin of the most important warnings, set @IgnorePrioritiesAbove = 50 to only get the urgent stuff.

### Advanced sp_Blitz Parameters

In addition to the [parameters common to many of the stored procedures](#parameters-common-to-many-of-the-stored-procedures), here are the ones specific to sp_Blitz:

#### Writing sp_Blitz Output to a Table

```SQL
sp_Blitz @OutputDatabaseName = 'DBAtools', @OutputSchemaName = 'dbo', @OutputDatabaseName = 'BlitzResults';
```

Checks for the existence of a table DBAtools.dbo.BlitzResults, creates it if necessary, then adds the output of sp_Blitz into this table. This table is designed to support multiple outputs from multiple servers, so you can track your server's configuration history over time.

#### Skipping Checks or Databases

```SQL
CREATE TABLE dbo.BlitzChecksToSkip (
ServerName NVACHAR(128),
DatabaseName NVARCHAR(128),
CheckID INT
);
GO
INSERT INTO dbo.BlitzChecksToSkip (ServerName, DatabaseName, CheckID)
VALUES (NULL, 'SalesDB', 50)
sp_Blitz @SkipChecksDatabase = 'DBAtools', @SkipChecksSchema = 'dbo', @SkipChecksTable = 'BlitzChecksToSkip'
```

Checks for the existence of a table named Fred - just kidding, named DBAtools.dbo.BlitzChecksToSkip. The table needs at least the columns shown above (ServerName, DatabaseName, and CheckID). For each row:

* If the DatabaseName is populated but CheckID is null, then all checks will be skipped for that database
* If both DatabaseName and CheckID are populated, then that check will be skipped for that database
* If CheckID is populated but DatabaseName is null, then that check will be skipped for all databases


## sp_BlitzCache: Find the Most Resource-Intensive Queries

(stub - describe the big picture here)

### Advanced sp_BlitzCache Parameters

In addition to the [parameters common to many of the stored procedures](#parameters-common-to-many-of-the-stored-procedures), here are the ones specific to sp_BlitzCache:

(stub - describe the lesser-used stuff)

## sp_BlitzIndex: Tune Your Indexes

(stub - describe the big picture here)

### Advanced sp_BlitzIndex Parameters

In addition to the [parameters common to many of the stored procedures](#parameters-common-to-many-of-the-stored-procedures), here are the ones specific to sp_BlitzIndex:

(stub - describe the lesser-used stuff)

## sp_BlitzFirst: Real-Time Performance Advice

(stub - describe the big picture here)

### Advanced sp_BlitzFirst Parameters

In addition to the [parameters common to many of the stored procedures](#parameters-common-to-many-of-the-stored-procedures), here are the ones specific to sp_BlitzFirst:

(stub - describe the lesser-used stuff)

## Parameters Common to Many of the Stored Procedures

* @Help = 1 - returns a result set or prints messages explaining the stored procedure's input and output. Make sure to check the Messages tab in SSMS to read it.
* @ExpertMode = 1 - turns on more details useful for digging deeper into results.
* @OutputDatabaseName, @OutputSchemaName, @OutputTableName - pass all three of these in, and the stored proc's output will be written to a table. We'll create the table if it doesn't already exist.
* @OutputServerName - not functional yet. To track (or help!) implementation status: https://github.com/BrentOzarULTD/SQL-Server-First-Responder-Kit/issues/293

## License

[The SQL Server First Responder Kit uses the MIT License.](LICENSE.md)


[licence badge]:https://img.shields.io/badge/license-MIT-blue.svg
[stars badge]:https://img.shields.io/github/stars/BrentOzarULTD/SQL-Server-First-Responder-Kit.svg
[forks badge]:https://img.shields.io/github/forks/BrentOzarULTD/SQL-Server-First-Responder-Kit.svg
[issues badge]:https://img.shields.io/github/issues/BrentOzarULTD/SQL-Server-First-Responder-Kit.svg

[licence]:https://github.com/BrentOzarULTD/SQL-Server-First-Responder-Kit/blob/master/LICENSE.md
[stars]:https://github.com/BrentOzarULTD/SQL-Server-First-Responder-Kit/stargazers
[forks]:https://github.com/BrentOzarULTD/SQL-Server-First-Responder-Kit/network
[issues]:https://github.com/BrentOzarULTD/SQL-Server-First-Responder-Kit/issues
