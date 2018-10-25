# First Responder Kit Release Process

From http://FirstResponderKit.org

This doc explains how to do a release of the First Responder Kit scripts.

It's for internal use at BrentOzar.com, but other folks might find it useful.

Well, useful is probably the wrong word. More like entertaining. Here we go.

## Preparations

* Create a Milestone to tag issues/PRs as you work on them. In Github, go into Issues, Milestones (button at the top), and add a new milestone named YYYY-MM, like 2017-01.
* As you merge pull requests into the dev branch (or before), tag them with the milestone. This makes it easy to find the related issues when you go to write release notes.


## Finalize and Test the Code

* Make sure all issues in the milestone are closed - click Issues, Milestones, and it'll show the percent complete. If there's any issues you want to bump to the next round, add the next round's milestone and tag the issues with it.
* When enough PRs are in dev, do a round of code testing in 2008-2016 in the cloud lab.
    * Merge scripts into three files:
        * Runs command in Merge Blitz.ps1
        * These get moved to AWS for testing
    * Run _TestBed.sql: this has stored proc calls with common parameters. May have to add in new scenarios if new features are added. 
* If it passes, bump all the version numbers inside the scripts and re-run the PowerShell commands so combined scripts reflect correct version and date.
    * sp_foreachdb @Version and @VersionDate
    * sp_BlitzWho @Version and @VersionDate
    * sp_BlitzIndex @Version and @VersionDate
    * sp_BlitzFirst @Version and @VersionDate 
    * sp_BlitzCache @Version and @VersionDate 
    * sp_Blitz @Version and @VersionDate
    * sp_DatabaseRestore @Version and @VersionDate
    * sp_BlitzBackups @Version and @VersionDate
    * sp_BlitzQueryStore @Version and @VersionDate
    * sp_AllNightLog @Version and @VersionDate
    * sp_AllNightLog_Setup @Version and @VersionDate
    * sp_BlitzInMemoryOLTP @Version and @VersionDate
* Update the SQL Server versions script from here: https://github.com/jadarnel27/SqlServerVersionScript
    * Right now, you need to run it in Visual Studio, and paste the command window output into the script file

## Push to Master

* Push to the master branch from dev. (Make sure you're pushing FROM dev, TO master.)
* Draft a new release. Click Code, Releases, and edit one of the recent releases to get the Markdown syntax. Copy/paste that into a new release, and put the issue numbers for the relevant changes.
* Publish the release pointing at the current master branch code (not dev).

## Announce It

* Copy the FRK scripts into BrentOzar.com's First Responder Kit zip file (Employees/Products/First Responder Kit/FirstResponderKit.zip)
* Copy the FirstResponderKit.zip into Employees/Public.BrentOzar
* For PowerBi, only keep the .pbit file
* Copy the Github release text into a WordPress blog post with the First Responder Kit Updates category. The nice thing about doing the Github release first is that you should be able to copy/paste the Github release page and the HTML should paste smoothly into the WordPress draft window, complete with links to the Github issues. At the end of the post, put a download now link that points to: https://www.brentozar.com/first-aid/
* Blog post support wording:

This goes at the top and bottom of each post:
<strong>You can <a href="https://www.brentozar.com/first-aid/">download the updated FirstResponderKit.zip here</a>.</strong>

This goes at the end of each post:
<h3>For Support</h3>

When you have questions about how the tools work, talk with the community in the <a href="https://sqlcommunity.slack.com/messages/firstresponderkit/" target="_blank" rel="noopener">#FirstResponderKit Slack channel</a>. If you need a free invite, hit <a href="http://SQLslack.com/" target="_blank" rel="noopener">SQLslack.com</a>. Be patient - it's staffed with volunteers who have day jobs, heh.

When you find a bug or want something changed, read the <a href="https://github.com/BrentOzarULTD/SQL-Server-First-Responder-Kit/blob/master/CONTRIBUTING.md" target="_blank" rel="noopener">contributing.md</a> file.

When you have a question about what the scripts found, first make sure you read the "More Details" URL for any warning you find. We put a lot of work into documentation, and we wouldn't want someone to yell at you to go read the fine manual. After that, when you've still got questions about how something works in SQL Server, post a question at <a href="http://dba.stackexchange.com" target="_blank" rel="noopener">DBA.StackExchange.com</a> and the community (that includes us!) will help. Include exact errors and any applicable screenshots, your SQL Server version number (including the build #), and the version of the tool you're working with.

<strong>You can <a href="https://www.brentozar.com/first-aid/">download the updated FirstResponderKit.zip here</a>.</strong>

## Update the Collector
Because you're a spaz and you always forget to do it.