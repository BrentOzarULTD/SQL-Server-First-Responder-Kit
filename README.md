# SQL Server First Responder Kit

You're a DBA, sysadmin, or developer who manages Microsoft SQL Servers. It's your fault if they're down or slow. These tools help you understand what's going on in your server.

* When you want an overall health check, run [sp_Blitz](#sp_Blitz:-Overall-Health-Check).
* To learn which queries have been using the most resources, run sp_BlitzCache.
* To analyze which indexes are missing or slowing you down, run sp_BlitzIndex.
* To find out why the server is slow right now, run sp_AskBrent.

To install, [download the latest release ZIP](https://github.com/BrentOzarULTD/SQL-Server-First-Responder-Kit/zipball/master), then run the SQL files in the master database. (You can use other databases if you prefer.)

Only Microsoft-supported versions of SQL Server are supported here - sorry, 2005 and 2000. Some of these may work some of the time on 2005, but no promises, and don't file a support issue when they fail.

## How to Get Support

Want to talk to the developers? [Join SQLServer.slack.com](https://sql-server-slack.herokuapp.com/), and we're in the [#FirstResponderKit channel](https://sqlserver.slack.com/messages/firstresponderkit/).

Got a question? Ask it on [DBA.StackExchange.com](http://dba.stackexchange.com). Tag your question with the script name, like sp_Blitz, sp_BlitzCache, sp_BlitzIndex, etc, and we’ll be alerted of it right away.

Got a feature request? [Open a Github issue.](https://github.com/BrentOzarULTD/SQL-Server-First-Responder-Kit/issues) Github is the source control tool where we work with the public on improving the First Responder Kit.

## sp_Blitz: Overall Health Check

(stub - describe the big picture here)

### Advanced sp_Blitz Parameters

(stub - describe the lesser-used stuff)

## sp_BlitzCache: Find the Most Resource-Intensive Queries

(stub - describe the big picture here)

### Advanced sp_BlitzCache Parameters

(stub - describe the lesser-used stuff)

## sp_BlitzIndex: Tune Your Indexes

(stub - describe the big picture here)

### Advanced sp_BlitzIndex Parameters

(stub - describe the lesser-used stuff)

## sp_AskBrent: Real-Time Performance Advice

(stub - describe the big picture here)

### Advanced sp_AskBrent Parameters

(stub - describe the lesser-used stuff)

## Parameters Common to Many of the Stored Procedures

* @Help = 1 - returns a result set or prints messages explaining the stored procedure's input and output. Make sure to check the Messages tab in SSMS to read it.
* @ExpertMode = 1 - turns on more details useful for digging deeper into results.
* @OutputDatabaseName, @OutputSchemaName, @OutputTableName - pass all three of these in, and the stored proc's output will be written to a table. We'll create the table if it doesn't already exist.
* @OutputServerName - not functional yet. To track (or help!) implementation status: https://github.com/BrentOzarULTD/SQL-Server-First-Responder-Kit/issues/293


## How to Contribute Code

Before you start, check the [Github issues list.](https://github.com/BrentOzarULTD/SQL-Server-First-Responder-Kit/issues) Search for what you're trying to do - there may already be an issue for it. Make sure to search through closed issues, too, because we often decline things that aren't a good fit for these tools.

If you've got a new idea that isn't covered in an existing issue, open a Github issue for it. Outline what you'd like to do, and how you'd like to code it. This just helps make sure other users agree that it's a good idea to add to these tools.

To start coding, [open a new Github branch.](https://www.brentozar.com/archive/2015/07/pull-request-101-for-dbas-using-github/) This lets you code in your own area without impacting anyone else. When your code is ready, test it on a case-sensitive instance of the oldest supported version of SQL Server (2008), and the newest version (2016).

When it's ready for review, make a pull request, and one of the core contributors can check your work.

Feel free to hop into Slack and ask us questions before you get started. [Join SQLServer.slack.com](https://sql-server-slack.herokuapp.com/), and we're in the [#FirstResponderKit channel](https://sqlserver.slack.com/messages/firstresponderkit/). We welcome newcomers, and there's always a way you can help.

## License

The SQL Server First Responder Kit uses the MIT License.
