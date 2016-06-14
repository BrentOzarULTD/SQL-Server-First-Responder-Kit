# Contributing to the SQL Server First Responder Kit

First of all, welcome! We're excited that you'd like to contribute. How would you like to help?

* [I'd like to report a bug or request an enhancement](#how-to-report-bugs-or-request-enhancements)
* [I'd like to write new T-SQL checks](#how-to-write-new-t-sql-checks)
* [I'd like to fix bugs in T-SQL checks](#how-to-fix-bugs-in-existing-t-sql-checks)
* [I'd like to test checks written by someone else](#how-to-test-checks-written-by-someone-else)
* [I'd like to write or update documentation](#how-to-write-or-update-documentation)

Wanna do something else, or have a question not answered here? Hop into Slack and ask us questions before you get started. [Join SQLServer.slack.com](https://sql-server-slack.herokuapp.com/), and we're in the [#FirstResponderKit channel](https://sqlserver.slack.com/messages/firstresponderkit/). We welcome newcomers, and there's always a way you can help.

## How to Report Bugs or Request Enhancements

[Check out the Github issues list.](https://github.com/BrentOzarULTD/SQL-Server-First-Responder-Kit/issues) Search for what you're interested in - there may already be an issue for it. Make sure to search through closed issues, too, because we often decline things that aren't a good fit for these tools.

If you can't find a similar issue, go ahead and open your own. Include as much detail as you can - what you're seeing now, and what you'd like to see.

When requesting new checks, keep in mind that we want to focus on:

* Actionable warnings - SQL Server folks are usually overwhelmed with data, and we only want to report on things they can actually do something about
* Performance issues or reliability risks - if it's just a setting we don't agree with, let's set that aside
* Things that end users or managers will notice - if we're going to have someone change a setting on their system, we want it to be worth their time

Now [head on over to the Github issues list](https://github.com/BrentOzarULTD/SQL-Server-First-Responder-Kit/issues) and get started.

## How to Write New T-SQL Checks

Before you code, check the [Github issues list](https://github.com/BrentOzarULTD/SQL-Server-First-Responder-Kit/issues) for what you're trying to do - there may already be an issue for it. Make sure to search through closed issues, too, because we often decline things that aren't a good fit for these tools.

If you've got a new idea that isn't covered in an existing issue, open a Github issue for it. Outline what you'd like to do, and how you'd like to code it. This just helps make sure other users agree that it's a good idea to add to these tools.

After a discussion, to start coding, [open a new Github branch.](https://www.brentozar.com/archive/2015/07/pull-request-101-for-dbas-using-github/) This lets you code in your own area without impacting anyone else. When your code is ready, test it on a case-sensitive instance of the oldest supported version of SQL Server (2008), and the newest version (2016).

When it's ready for review, make a pull request, and one of the core contributors can check your work.

## How to Fix Bugs in Existing T-SQL Checks

(stub)

## How to Test Checks Written by Someone Else

(stub)

* Test only on case-sensitive instances. A surprising number of folks out there run on these.
* Test on as many currently-supported versions of SQL Server as possible. At minimum, test on the oldest version (currently 2008), and the newest version (currently 2016).

## How to Write or Update Documentation

(stub)
