# sp_AskBrent Presentation Notes

## Presentation Flow

* Talk about something (not sure if it's typical DBA challenges or what)
* Email toast pops up at the bottom right saying the server is slow, and they told me to go ask Brent
* Skype chat pops up saying the server is slow, and they told me to go ask Brent

* You know what we need to do - let's ask Brent.
* Open SSMS and run sp_askbrent

* Explain how the code works
* Show a couple of sample checks

* Last 5 minutes: you can also ask Brent a question: sp_askbrent 'sample question'
* Final slide: BrentOzar.com/askbrent


## Project Plan

### For Chicago UG July 11th:

Building the query:

* Build shell stored proc with comments
* Define the result set
* Check it into Github

Designing the checks:

* Come up with a core set of checks we'll ship with
* Code those checks

Building the web site plumbing:

* Upload the stored proc to a login-protected area for now
* In login-protected area, write instructions on running it with a certificate

Deliver the presentation:

* June 30 - download the Summit speaker template
* Order pizza
* Record the session audio for questions

### For PASS Summit slide decks August 23:

* Finalize slide deck in their format
* Build shell landing page, but have it as "Coming Soon" with a countdown. Don't show the actual content on the site because people can get in early October 7th.

### For PASS Summit October Oct 16-18:

Improving the query:

* Build a unit test with AdventureWorks2012 for each core check
* Test at clients during typical loads (to eliminate false positives), ugly loads

Building the web site plumbing:

* Create a shell landing page for each check
* Create a shell landing page for the top 20-30 most common wait types
* Record a howto video
* Finalize landing page, link to it from tools/scripts pages
* Put download version behind EULA
* Write content for each check page

### Nice-To-Have

* Set up contributions web page with instructions

## Check Notes

### Phase 1: Gather initial snapshots

* Take wait sample at start and set a time stamp. Get snapshots of waits, Perfmon counters, and dm_io_virtual_file_stats.
* Get query stats for top 50 queries by execution count and by logical reads. 

### Phase 2: Background checks

Done:

* Queries blocking others
* Backup, restore, or DBCC running
* Data or log file growing
* Check plan cache creation dates to warn if the plan cache was recently dumped

To do:

* Queries that are rolling back
* High CPU use outside of SQL Server
* Check estimated subtree cost on long running queries
* Long running query wants an index

Perfmon counters check:

* Very low PLE
* Memory grants pending
* Compiles per second relative to batches per second

Additional data sources:

* File growths in default trace
* Take IO file stats delta, warn on slow IO


### Phase 3: Gather second snapshots

* Check time stamp. If less than 10 seconds, twiddle thumbs to pass the time.
* Take another sample
* Do differential
* Explain waits if possible
* Link to page on our site per wait
* For plan cache, after 10 seconds, can get an idea of the death by a thousand cuts scenario if one is skyrocketing. Also get top 50 by logical reads, and if one is having huge growths, we could blow it out of the plan cache.

### Phase 3: Log Findings to Table

* Optionally, log the findings to a table along with the server name and datestamp.
* Add a parameter for @AsOf DATETIME, and hit the logging table looking for the nearest sample before and after to show why the server was slow then.
* Document how to set up a SQL Agent job to run sp_AskBrent and log to table.


