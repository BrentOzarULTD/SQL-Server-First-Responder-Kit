# sp_AskBrent Presentation Notes


## Prep work

On /askbrent:

* Add anchor links to jump to parts of the page
* Add instructions on running it via a SQL Agent job and fetching the history. Need to delete it yourself, like a weekly truncate job.

Demo data:

* Set up fake past data for @AsOf demo

## Presentation Flow

* Talk about something (not sure if it's typical DBA challenges or what)
* Email toast pops up at the bottom right saying the server is slow, and they told me to go ask Brent
* Skype chat pops up saying the server is slow, and they told me to go ask Brent
* You know what we need to do - let's ask Brent.
* Open SSMS and run sp_askbrent
* Explain how the code works
* Show a couple of sample checks
* Run @AsOf with the output table names
* Set it up via a SQL Agent job
* Run it with certificates
* Run it via the DAC
* Last 5 minutes: you can also ask Brent a question: sp_askbrent 'sample question'
* Final slide: BrentOzar.com/askbrent


## Nice-to-have web page work

Building the web site plumbing:

* Create a shell landing page for each check
* Create a shell landing page for the top 20-30 most common wait types


## Go-Live morning:

* Remove the "coming soon" from http://www.brentozar.com/first-aid/downloads/end-user-license-agreement/downloads/
* Upload sp_AskBrent.txt to the protected file directory - /wp-content/uploads/scripts
* Add it to First Aid pages

