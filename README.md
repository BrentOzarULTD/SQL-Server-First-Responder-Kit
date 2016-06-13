# SQL Server First Responder Kit

The kit includes a few handy T-SQL scripts:

* sp_Blitz - prioritized SQL Server health check to make sure your databases are backed up, you don't have corruption, and there's no ugly surprises in your server.
* sp_BlitzCache - lists the most resource-intensive queries in your plan cache.
* sp_BlitzIndex - checks a database's indexes for duplicates, heaps, missing indexes, and more.
* sp_AskBrent - performance health check of what's broken on the server right now.

## How to Install the First Responder Kit

(Stub - note that they can install the scripts in any database, as long as they know to fully qualify the names)

## How to Use the Scripts

### sp_Blitz Instructions

(stub)

### sp_BlitzCache Instructions

(stub)

### sp_BlitzIndex Instructions

(stub)

### sp_AskBrent Instructions

(stub)

## How to Get Support

Got a feature request? [Open a Github issue.](https://github.com/BrentOzarULTD/SQL-Server-First-Responder-Kit/issues) – Github is the source control tool where we work with the public on improving the First Responder Kit.

Got a question? [Email us](https://www.brentozar.com/contact/), or ask it on [DBA.StackExchange.com](http://dba.stackexchange.com). – Tag your question with the script name, like sp_Blitz, sp_BlitzCache, sp_BlitzIndex, etc, and we’ll be alerted of it right away.

Want to talk to the developers? [Join SQLServer.slack.com](https://sql-server-slack.herokuapp.com/), and we're in the [#FirstResponderKit channel](https://sqlserver.slack.com/messages/firstresponderkit/).

## How to Contribute Code

Before you start, check the [Github issues list.](https://github.com/BrentOzarULTD/SQL-Server-First-Responder-Kit/issues) Search for what you're trying to do - there may already be an issue for it. Make sure to search through closed issues, too, because we often decline things that aren't a good fit for these tools.

If you've got a new idea that isn't covered in an existing issue, open a Github issue for it. Outline what you'd like to do, and how you'd like to code it. This just helps make sure other users agree that it's a good idea to add to these tools.

To start coding, [open a new Github branch.](https://www.brentozar.com/archive/2015/07/pull-request-101-for-dbas-using-github/) This lets you code in your own area without impacting anyone else. When your code is ready, test it on a case-sensitive instance of the oldest supported version of SQL Server (2008), and the newest version (2016).

When it's ready for review, make a pull request, and one of the core contributors can check your work.

Feel free to hop into Slack and ask us questions before you get started. [Join SQLServer.slack.com](https://sql-server-slack.herokuapp.com/), and we're in the [#FirstResponderKit channel](https://sqlserver.slack.com/messages/firstresponderkit/). We welcome newcomers, and there's always a way you can help.
