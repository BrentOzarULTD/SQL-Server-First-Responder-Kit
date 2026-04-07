# SQL Server Setup Checklist

Get the latest version free from https://github.com/BrentOzarULTD/SQL-Server-First-Responder-Kit/tree/main/Documentation/Setup_Checklist_SQL_Server.md.

Last Updated: March 2026

If you'd like to contribute to this setup guide, check out [the contribution guide](https://github.com/BrentOzarULTD/SQL-Server-First-Responder-Kit/blob/dev/CONTRIBUTING.md), which explains how to report bugs, how to add features, and how contributors are expected to behave in the community.

## What's in this Setup Guide?

This is a summary of basic best practices for configuring SQL Servers.

Meeting the basic minimum configuration helps get you started on your way to a healthy SQL Server. However, configuring SQL Server is complex. This configuration doesn't yet provide:

- Configuration for virtualization
- Steps to set up high availability
- Performance testing / storage tuning
- Protection or testing of all potential hardware failures


## Provisioning the Server

### Team multiple physical network cards

If you're using a virtual machine, we're talking about the host layer.

If you're using shared storage (either on a VM or physical), storage network drops are a classic recipe for database corruption. When SQL Server's storage drops out from underneath it, breaking in-progress writes, SQL Server is **usually** good at recovering, and recovers **most** of the time. Most of the time isn't good enough for production databases.

If you're using AlwaysOn Availability Groups (either physical or virtual), even a temporary network drop can [take the databases down](http://www.brentozar.com/archive/2012/06/why-your-sql-servers-network-connection-matters/). 

To prevent these problems, physical servers come with multiple physical network cards (or in the case of blades, multiple physical network interfaces.)

For each network that the server needs to be on (OS network communications, storage network communications, management network, etc), work with your network team to set up teaming. Server and storage communication needs to continue if any one network connection fails. Ideally, your network team uses pairs of switch networks too, so that network connectivity continues even if a switch goes down.

Teaming multiple ports on a single network card does not mitigate this risk.

For each NIC, you'll need to update firmware/drivers. You may need to install upgraded teaming software, depending on what type of cards and teaming you are using. The teaming may be done by the network card's drivers & software, or by Windows.

You'll configure and test the network teaming after Windows has been installed, but the preparation for this should be done before the server is installed so that the network team has the wiring already done and ready to go. Otherwise, you'll be facing lengthy delays during server configuration and testing, because this stuff is usually political and requires coordination from multiple teams.

### Physical servers only

The following steps are only valid for bare metal boxes.

#### Configure and test a remote management card

These cards provide remote access to the server via a separate network connection. These can be extremely useful when a server is non-responsive and can't be managed through accessing the operating system remotely.

Test the ILO/RIB card by logging into it and performing system related tasks (like rebooting). Make sure the ILO/RIB card properly identifies the server it's connected to. Many a server has been rebooted accidentally due to this being skipped.

Document where the authentication info is stored for the remote management card so you can access it in the event of an emergency.

#### Update all BIOS and firmware versions

You don't have to wait until Windows is installed to do this - in fact, it's a great use of your hardware manufacturer's remote management tools. You should be able to update the server's BIOS and firmware via its remote management tools even when the OS isn't installed yet.

This really does matter: in 2025, Brent dealt with a case where CPU-intensive workloads caused a cluster node to fail over, and the solution was to update the server's firmware. You want to do this proactively, not after you're facing mysterious outages.

Update the firmware on local storage devices, too: SSDs and PCI Express storage devices have firmware to help prevent and troubleshoot corruption issues.

#### Configure local storage with RAID

Your boot drive (C) should be a mirrored pair (RAID 1) of drives. You don't want a single drive outage to take the entire server down. RAID 5 or RAID 10 is also fine if your server needs that much space for the boot drive, but generally speaking 200GB or so should be fine for the boot drive. You won't be using that space for user or system databases. 

You can also install SQL Server on the boot volume (C). The binaries don't take up much space. When you do that SQL Server installation later (much later) in this document, we'll talk about where to put system and user databases, but those definitely don't go on the C drive.

RAID 0 is a little misnamed because it doesn't include redundancy. (That's the R in RAID.) Without redundancy, if any one drive fails, the entire array will go offline. Don't use RAID 0 here.

If you're using local SSDs for TempDB, even those should be protected with RAID 1 or higher. If the TempDB volume is a RAID 0 array, then even a single drive failure will take down TempDB, and at that point, your SQL Server instance will be nearly unusable. RAID 0 is not a good fit here either.

Even PCI-Express SSD devices should use RAID 1 (with two devices) so that you can tolerate the failure of any single device. For these devices, this may require the use of Software RAID at the Windows level.

The need for RAID is not mitigated or eliminated by high availability features or manufacturer's claim for redundancy within a single device. If a vendor claims their storage (or storage-interfering software, like software-based clustering) is good enough for SQL Server, review the Microsoft whitepaper [SQL Server 2000 I/O Basics](http://technet.microsoft.com/en-us/library/cc966500.aspx). It's old but good, including gems like write ordering, which has to be preserved by any underlying storage subsystem.


## Provision storage

Your server needs storage for:

* The operating system, page file, and memory dumps
* The SQL Server application files
* System databases (master, model, msdb)
* TempDB
* User databases
* Backups

There's no one right answer for all servers, but let's discuss each of those individually so you can make the right decision for your own server.

### The operating system, page file, and memory dumps

The operating system volume speed isn't a big concern. SQL Server only swaps to disk when it's under extreme memory pressure, and once that happens, it doesn't really matter of fast the volume is - you're screwed.

Space is a concern, though: the boot volume needs enough space for a [32GB page file and at least one 32GB memory dump](https://learn.microsoft.com/en-us/troubleshoot/windows-client/performance/how-to-determine-the-appropriate-page-file-size-for-64-bit-versions-of-windows#support-for-system-crash-dumps). In most cases, the 32GB automatic memory dumps will be enough to troubleshoot most server issues. 

If you find yourself in a situation where Microsoft Support needs a full memory dump, and you have a server with a lot of memory (say, 256GB or higher), then you would need a really big OS volume to handle a full dump. Then, you'd find yourself with a new problem: a giant 256GB memory dump that you'd need to copy to a USB drive, then ship over to Microsoft. Let's hope you never find yourself in that scenario.

### The SQL Server application files

The SQL Server app itself doesn't take much space (only a few gigabytes), and performance on those files isn't really important since they're only heavily accessed during initial startup. Once the SQL Server service is up and running, those files could be on a slow USB thumb drive for that matter.

If you're building a failover cluster or Always On Availability Group, these app files are still installed once per server, so they're still fine on local storage like the operating system volume.

Because of that, these days, it's fine to install the SQL Server application itself on the operating system volume. A dedicated volume isn't necessary for the binary files.

### System databases (master, model, msdb)

If you're building a failover cluster instance (FCI), then these databases have to be on a shared storage volume. They don't necessarily need their own isolated volume, because these databases tend to be fairly small and low-load. You can either put them on the same volume as the user databases, or on their own dedicated volume.

### TempDB

Okay, now we're at the first unusual volume.

Whenever SQL Server restarts, TempDB's contents start fresh.

TempDB's volume has to be reliable when the SQL Server is on, but the data inside that volume doesn't really need to be persisted across restarts, or replicated to another region for disaster recovery. TempDB just gets recreated each time, empty.

That gives us flexibility to put TempDB on the fastest, cheapest, least-protected (but still online) storage.

**If you're using VM replication, storage replication, or VM-level backups** to protect your servers, you probably want a different protection policy where the TempDB files live. They don't need to be backed up or replicated, and this is especially important because these files have a high level of churn. Their contents change all the time, making backups and replication expensive and bandwidth-consuming. If you're using these replication or backup technologies, you want to provision a separate volume for TempDB, and turn off replication and backups for that volume.

**If you're provisioning a cloud VM**, check to see whether your chosen VM type offers local (ephemeral) solid state storage. If it does, dedicate that to TempDB. Writes to local solid state will be faster (less latency) than shared storage, plus this noisy write traffic will stay off your storage area network, keeping storage network latency and throughput higher for your valuable user databases.

**If you're using a failover clustered instance**, you probably want to use a mirrored pair of local solid state (either PCIe, SAS, or SATA) for the same reasons cited above for cloud VMs. This is useful in on-premises clusters as well.

If none of the above apply, you can still choose whether to put TempDB on its own dedicated volume, or put it on the same volume(s) as the user databases. Before making that decision, read [this cheat sheet on how to configure TempDB](https://www.brentozar.com/archive/2016/01/cheat-sheet-how-to-configure-tempdb-for-microsoft-sql-server/).

### User databases

Provision enough capacity and speed for the user databases.

The speed is the hard part. Whether you're in the cloud, in VMs, or on bare metal, you may need to provision multiple volumes for your user databases, and add a separate data file on each volume in order to get the aggregate throughput that you need.

That's beyond what we can cover quickly in this document, but if you'd like to help flesh it out, check out the contribution info on the first page.

### Backups

It sounds odd to talk about backups before we've installed Windows, but your backups are critically important. We've seen many a server go live only to find out the hardware couldn't possibly back up the databases quickly enough to match the business requirements for downtime and data loss.

Get your business's Recovery Point Objective (RPO) and Recovery Time Objective (RTO) for high availability and disaster recovery in writing. Learn more about these key concepts here: [https://www.brentozar.com/archive/2011/12/letters-that-get-dbas-fired/](https://www.brentozar.com/archive/2011/12/letters-that-get-dbas-fired/)

Then, a backup and recovery strategy that can meet these goals. A highly available backup design can mean you:

- Keep multiple copies of backup files (full and log) -- not on the same storage, and never all on the production server
- Perform frequent transaction log backups, [like every minute](https://www.brentozar.com/archive/2014/02/back-transaction-logs-every-minute-yes-really/). If it's not OK to lose data, you want to be doing log backups every minute (and also investing in a secondary high availability solution because you could still lose data with log backups every minute!)
- Copy off backup files quickly to a secondary location like tape or your DR site

For most SQL Servers, the right answer is to [back up to a network share, not local storage.](https://www.brentozar.com/archive/2008/09/back-up-your-database-to-the-network-not-local-disk/) You want your backups to be available even when the server won't start up. Provision the network share with the backup space to keep, at minimum:

- Two copies of multiple days of full backups
- Two copies of multiple days of transaction log backups

To determine how many days of backups you need to keep online, ask business owners, "If data was accidentally deleted or corrupted two weeks ago (or whatever time period), would we need to restore to the night before? Or right before it happened?"

Design your backup strategy to match your RPO and RTO, and that'll determine how much backup space you need.



## Choose, install, and patch Windows

### Use the most recent Windows version possible

You want the longest service life possible because you don't wanna go through this process often. Using the most recent version gives you the longest support life.

Then get the server up to date with all of the most recent patches. If you're using multiple servers in a cluster or a high availability scenario, apply the same patches to all of 'em.

### Configure the Windows page file

If you have enough drive space on the boot volume, create a [32GB page file and at least one 32GB memory dump](https://learn.microsoft.com/en-us/troubleshoot/windows-client/performance/how-to-determine-the-appropriate-page-file-size-for-64-bit-versions-of-windows#support-for-system-crash-dumps) per Microsoft's guidelines.

Beware removing the page file altogether. While a properly-configured, high-performance SQL Server shouldn't ever need to swap its memory out to the page file, Windows still needs it in order to write diagnostic information in the event of a crash or extreme memory pressure. You don't want to be facing a production outage and not be able to call Microsoft Support afterwards in order to understand what happened or why.

### Set anti-virus exclusions

It's OK to run anti-virus software on a SQL Server - after all, we know the kinds of web sites you visit. You need to configure exclusions for all SQL Server files per Microsoft's guidelines. It's not just mdf and ldf files - there's a lot more to exclude:

[https://learn.microsoft.com/en-us/troubleshoot/sql/database-engine/security/antivirus-and-sql-server](https://learn.microsoft.com/en-us/troubleshoot/sql/database-engine/security/antivirus-and-sql-server)

If you have additional tools that restrict creating/modification of files, they should also have exclusions set.

## Configure and test storage

A lot of storage configuration can only be done after the operating system is installed, so here we go!

### Configure host bus adapters (HBAs) and multipathing

Update the HBA firmware (on the VM host or the physical SQL Server.) Downlevel HBA firmware can cause all kinds of nasty problems, especially in clustered servers. Generally, these updates can't be done online while the server accesses data, so it's better to get the code up to date before the box goes into production. For HP servers, this firmware isn't shown in the System Homepage: install Emulex HBAnyware on the server instead, and it will flash the HBAs inside of Windows without a reboot. HBAnyware is available in the HP Support site by searching for downloads for the HBA's part number instead of the server's part number. This is the only driver/firmware at HP that works this way.

Set up multipathing drivers. Sometimes this is done by the storage team, but the DBA should get involved enough to understand whether the multipathing is active/active or just failover. Ideally, it's active/active, so that you can use multiple paths at all times for maximum throughput.

Test the multipathing & failover. Start a huge file copy to each array, and do them all simultaneously. Have your storage admin disable one of the storage cables. Watch to make sure the file copy continues. Some SAN drivers will take 10-15 seconds to fail over, but the file copies should not be disrupted, period. If they're disrupted, the multipathing didn't work. Light the port back up, and then down another port. Again, the file copy should continue. Finally, while a file copy is running, ask the SAN admin to disable one of the SAN zones for the server - that way, the fiber cable will still be lit up, but the path to the storage will be gone. (This is a tougher failover method than just downing the fiber connection.)

### Configure iSCSI pathing

Set up multipathing. Database servers can't rely on one single network connection for iSCSI any more than a fiber-connected SAN can rely on one single host bus adapter. Ideally, two (or more) network cards will be connected to two different switches for redundancy, but at the very least, we need two network cards dedicated to iSCSI storage. The multipathing method can be active/active (meaning 20 gigs of throughput for two 10 gig NICs) or active/passive.

Test the multipathing. We usually see active/passive on a per-array basis - meaning, if you have two different iSCSI drive letters, then the multipathing drivers will put each drive on its own network card. Start multiple simultaneous drive copies and go into Task Manager, in the Network tab. Look at the bandwidth used by each network card. If a network card is sitting idle, then you're leaving performance on the table. Now is the time to tweak the multipathing software and ask questions of the vendor - it's easier to troubleshoot file copy performance than it is to troubleshoot SQL Server performance.

Test the failover. As with the fiber cable testing, start multiple simultaneous file copies to/from the network drives and down one of the server's storage network ports. If the file copy fails (if Windows throws an error) then SQL would have crashed. Tweak the teaming software until it can fail over seamlessly, and ideally it should fail over back and forth and go back to higher bandwidth levels as the networks come back online.

### Format the drives with 64K allocation blocks

SQL Server likes to have a block size of 64K on most storage platforms. If your storage platform gives specific advice to use a different block size for SQL Server, you may want to test which is better. Otherwise, use a 64K block size.

Note: this only applies to drives holding SQL Server database and log files (including tempdb). Your C drive / system drive should be separate and 4K block size is appropriate for that logical drive. Format your database drive through the GUI with a 64K allocation unit.

You can't do any of this with the C drive - Windows doesn't give you these options during install - but C drive performance really isn't an issue as long as we're not storing database files there. (And you're not going to do that. Please, seriously, tell me you're not going to do that. Even if it's a VM on shared storage, you don't want to put the database files where they might grow, force the OS to run out of space, and cause a very ugly crash. Thanks, I appreciate you telling me that you knew that already and would never do that.)

### Benchmark the storage

Now that you're sure the storage hardware is set up correctly, start by running the portable edition of Crystal Disk Mark to benchmark your drive performance before you go live. Instructions are here: [http://www.brentozar.com/archive/2012/03/how-fast-your-san-or-how-slow/](http://www.brentozar.com/archive/2012/03/how-fast-your-san-or-how-slow/)

Running these tests will not prevent problems, but it's a start. These tools will give you benchmark information for storage performance before you start - and if you run into performance problems later, having this information available and being able to compare is extremely valuable.

You're looking for at least 200MB/sec of sequential read throughput per CPU core. If you can't achieve that throughput, odds are your SQL Server will never use large amounts of CPU power because it'll be sitting around bored waiting for the storage to deliver data faster. Low CPU usage isn't necessarily a good thing - remember, we paid for SQL Server licensing by the core, and we don't want to have a lot of cores just hanging out, smoking cigarettes outside of the break room. Or whatever it is they do when they're not working hard.

### Test network teaming with file copies

Remote desktop into the server and copy a large file (say, last night's production backup) from one network file share to another.

As you're copying the file, change the VLAN for one of your switch ports at the network level. This isn't something you can do in the server - if these words sound like gibberish to you, get a member of your network team involved. We don't want to outright disable the network port - we want Windows to think the network is still up, but we don't want the packets to actually go anywhere.

When this happens, your remote desktop connection needs to stay up, and the file copy needs to continue. Pauses are okay, but errors are not. Watch NIC utilization and make sure that teams are working as configured, and that you get the transfer rate you expect.

## Create service accounts and grant permissions

### Choosing your service accounts

These days, choosing the right service accounts is tricky, and changes based on whether you're using failover clusters, Availability Groups, Kerberos, Linux, and more. Your best bet is to read [Microsoft's documentation on the various scenarios](https://learn.microsoft.com/en-us/sql/database-engine/configure-windows/configure-windows-service-accounts-and-permissions), and use the version dropdown at the top left of the page to pick which SQL Server version you're using so you get the right guidance for your version.

If you're not using AlwaysOn AGs, consider a different service account per production server. If you use the same service account for every server, then when somebody repeatedly mistypes the service account password during an installation, you can lock out the service account, causing widespread havoc.

### Granting the right permissions

Do not make the service account a local administrator. SQL Server's installer will automatically grant the least privileges required during setup.

Enable Instant File Initialization: Grant the 'Perform Volume Maintenance Tasks' right to the account that will be used for the SQL Server service (the engine, not the agent). This can be done during setup, or you can do it ahead of time.

## Install and configure SQL Server

### Select the components you will install

While lots of things come "free" in the SQL Server boxed product, you don't want to install them unless you genuinely need them, and you don't have any other choice as to where to put them. The SQL Server database engine wants as much CPU power, memory, and storage throughput as it can get.

While SQL Server does have settings to limit how much power it uses overall, or for groups of users or databases, those controls only pertain to the database engine itself - not to other services like SSAS or SSIS. And when those services are active, guess what they're doing: hammering SQL Server at the same time. You end up with a situation where all of these services want to use the same horsepower, at the same time, leading to slowdowns.

If you don't need those other services on the same server, don't install them. Don't even think you should install them just in case someone wants to use them someday - because someone will take advantage of the services just because they're there, and next thing you know, they're a performance problem.

During setup, SQL Server will ask how you want to configure TempDB, max server memory, and max degrees of parallelism based on your particular hardware. You'll want to accept those recommendations. (They're not the defaults - you actually have to click to accept them.)

SQL Server will also ask what paths you want to use for the user databases and TempDB. Point those to the volumes you configured earlier.

### Install the most recent updates

Get the latest cumulative updates: [https://SQLServerUpdates.com](https://SQLServerUpdates.com)

We recommend upgrading all Development / QA / PreProduction machines to the version and CU that you intend to use and testing it prior to going live.

Cumulative updates are truly cumulative. To install SQL Server 2025 Cumulative Update 3, you only need to install SQL Server 2025, then CU3. You don't have to install CU1 or CU2 - CU3 is cumulative, and includes all cumulative updates.

### Enable TCP/IP

Configure this in the SQL Server Configuration Manager under "SQL Server Network Configuration." Enabling the TCP/IP protocol will only take effect after the SQL Server instance is restarted.

### Test Instant File Initialization (IFI)

Create an empty database. Grow the data file by 5GB. If it doesn't complete immediately, IFI isn't working. (Revisit the previous step where it was granted.) If you've verified IFI is working, go ahead and drop the empty database.

### Configure SQL Server Cost Threshold for Parallelism

The optimizer uses that cost threshold to figure out when it should start evaluating plans that can use multiple threads. Although there's no right or wrong number, 5 is a really low setting. It's appropriate for purely OLTP applications, but as soon as you add a modicum of complexity - ka-boom!

Set Cost Threshold for Parallelism to 50. If you're passionate about a particular value, we're fine with values from 40 to 125, but beyond that, you probably want to double-check your default to make sure it makes sense for your workload. To learn more, [read this blog post about parallelism waits](https://www.brentozar.com/archive/2013/08/what-is-the-cxpacket-wait-type-and-how-do-you-reduce-it/).

### Set the default backup options

This isn't offered during the setup wizard. In SSMS, right-click on the server name, click Properties, Database Settings, and check the boxes for "Compress backup" and "Backup checksum". In the default locations section, under backups, choose the network path you provisioned for your backups.

### Tweak settings on the model database

Set the recovery model and filegrowth settings to what you would like to be the default for new databases. We recommend moderately small fixed growth units (not percentage units), such as:

- 256MB for data files
- 128MB for log files

### Set up maintenance

Configure and schedule regular maintenance for all of the following:

- Full (and possibly differential) backups
- Log backups (for databases in Full recovery model)
- CheckDB
- Index and statistics maintenance

Don't forget that your system databases need backup and CheckDB also!

Options for setting up maintenance:

1. Use free scripts from Ola Hallengren to create customized SQL Server Agent Jobs: [http://ola.hallengren.com/](http://ola.hallengren.com/)
2. Use SQL Server Maintenance Plans if you'd like a graphical user interface, but just keep in mind that they're not as flexible and powerful as Ola's scripts. For example, they take a shotgun approach to index maintenance - they'll rebuild all of the indexes, every time, whether there's any fragmentation or not.

Learn more about configuring maintenance:

- Transaction Log Backup Frequency: [http://www.brentozar.com/archive/2014/02/back-transaction-logs-every-minute-yes-really](http://www.brentozar.com/archive/2014/02/back-transaction-logs-every-minute-yes-really)
- Why to avoid the Update Statistics Task in Maintenance Plans: [http://www.brentozar.com/archive/2014/01/update-statistics-the-secret-io-explosion/](http://www.brentozar.com/archive/2014/01/update-statistics-the-secret-io-explosion/)
- Rebuild or Reorganize - Index Maintenance: [http://www.brentozar.com/archive/2013/09/index-maintenance-sql-server-rebuild-reorganize/](http://www.brentozar.com/archive/2013/09/index-maintenance-sql-server-rebuild-reorganize/)
- Why Index Fragmentation Doesn't Matter: [http://www.brentozar.com/archive/2013/09/why-index-fragmentation-doesnt-matter-video/](http://www.brentozar.com/archive/2013/09/why-index-fragmentation-doesnt-matter-video/)

### Performance test the maintenance jobs

Restore a full sized copy of your production database. Time how long this takes - it's your one chance to do it before going live - and track the length of time it takes. That way, when the CIO wants to know how long it'll take before the production server is back up and running, you won't have to guess - you'll know exactly how long a restore took when the server went live.

Run all of your maintenance jobs (backups, CheckDB, reindexing) and make sure that the jobs complete successfully and you get the backup files you expect.

## Configure security and authentication

### Test SQL Authentication (if you're using it)

If you've created/migrated any accounts using SQL authentication, make sure they work. The instance might be in Windows authentication mode only -- you can still create accounts but they won't work.

Change SQL Server to mixed mode authentication if needed, restart it, and test to make sure the access works.

### Configure linked servers and security if needed

Linked servers can be a security and performance nightmare, so be careful out there.

### Configure DTC Security for Distributed Queries

Only do this step if you use distributed queries.

We don't recommend distributed queries for performance reasons, but if you use them you may have to configure DTC Security at the Windows level.

For more information: [https://learn.microsoft.com/en-us/previous-versions/windows/it-pro/windows-server-2008-R2-and-2008/cc731495(v=ws.11)?redirectedfrom=MSDN](https://learn.microsoft.com/en-us/previous-versions/windows/it-pro/windows-server-2008-R2-and-2008/cc731495(v=ws.11)?redirectedfrom=MSDN)

### Enable remote access to the Dedicated Admin Connection

The DAC is a separate CPU scheduler that you can use to run diagnostic queries when SQL Server seems locked up or unresponsive. By default, it's enabled - but only for local connections, meaning you have to remote desktop into the SQL Server to use it. Instead, enable it for remote troubleshooting: [https://www.brentozar.com/archive/2011/08/dedicated-admin-connection-why-want-when-need-how-tell-whos-using/](https://www.brentozar.com/archive/2011/08/dedicated-admin-connection-why-want-when-need-how-tell-whos-using/)

## Configure SQL Server alerting and monitoring

### Configure Database Mail

Configure database mail using the wizard - sadly, there's not a good T-SQL coverage area for this just yet, and the wizard is the easiest way to do it: [https://learn.microsoft.com/en-us/sql/relational-databases/database-mail/configure-database-mail](https://learn.microsoft.com/en-us/sql/relational-databases/database-mail/configure-database-mail)

Test it using the "send test email" functionality, and send an email to the distribution list you use for SQL Server alert notifications. Never send emails directly to individuals - sooner or later, as hard as this is to conceive, they are going to take a vacation. (More likely, they're going to quit their jobs, but we're trying to be optimistic here.)

Be aware that developers can use Database Mail for things that SQL Server shouldn't be doing. For example, they may decide to use Database Mail to send out mass emails to your end users or customers. There's nothing technically wrong with that, but it increases the load on the database server and it sends all outgoing email with the SQL Server's Database Mail account.

If you choose to allow this, consider setting up separate private and public email profiles. The public email profile used by the developers should be sent from the developer management team's group email address - that way, they can address any replies themselves.

### Create Operators in the SQL Server Agent

You may need more than one.

### Configure Database Mail in the SQL Server Agent properties

Go into the SQL Server Agent properties, enable database mail, and select the right profile.

Configure a failsafe operator while you're in there. During the setup process, this has been known to restart Agent, so you want to do this before you go live.

### Create alerts for high severity and data corruption

Use our handy script to create alerts. Just change the operator name to your operator - the one that points to your distribution list. [http://www.brentozar.com/blitz/configure-sql-server-alerts/](http://www.brentozar.com/blitz/configure-sql-server-alerts/)

### Restart the Agent Service and Test

Restart the SQL Server Agent Service after to make the change you made to enable the mail profile take effect. (This isn't the whole SQL Server, just the Agent service.)

After creating the alerts, test them and make sure the operator you configured receives the notification. You can do this by running a command like:

```sql
RAISERROR('Alert Test', 18, 1) WITH LOG;
```

## Install tools and run a health check

Here are our favorite open source tools that we like installing by default on new SQL Servers:

* [First Responder Kit](https://www.brentozar.com/first-aid/) - sp_Blitz, sp_BlitzCache, sp_BlitzIndex, etc.
* [DarlingData](https://github.com/erikdarlingdata/DarlingData) - sp_PressureDetector, sp_HumanEvents, etc.
* [sp_WhoIsActive](https://github.com/amachanic/sp_whoisactive) - replaces Activity Monitor
* [Ola Hallengren's maintenance scripts](https://ola.hallengren.com) - for backups, corruption checks, index maintenance

These utilities go on your machine, not the server, but we recommend them:

* [PerformanceMonitor](https://github.com/erikdarlingdata/PerformanceMonitor) - open source performance monitoring that's production-grade.
* [PerformanceStudio](https://github.com/erikdarlingdata/PerformanceStudio) - cross-platform execution plan tuning tool.


## Deploy custom code & settings for your app

### Migrate App Specific Logins, Agent Jobs, and Custom Alerts

You also need to configure all of the following for your application, typically copied from your current production server:

- Application logins. If migrating from another server, you can migrate the logins: [https://learn.microsoft.com/en-us/troubleshoot/sql/database-engine/security/transfer-logins-passwords-between-instances](https://learn.microsoft.com/en-us/troubleshoot/sql/database-engine/security/transfer-logins-passwords-between-instances)
- Application specific SQL Agent jobs (can be scripted from an existing instance)
- Custom alerts (can be scripted from an existing instance)
- Identify and clean up Orphan Users: [https://learn.microsoft.com/en-us/sql/sql-server/failover-clusters/troubleshoot-orphaned-users-sql-server](https://learn.microsoft.com/en-us/sql/sql-server/failover-clusters/troubleshoot-orphaned-users-sql-server)

### CLR Components

Make a plan if you're migrating databases that use CLR components to configure and smoketest those components after migration.

Enable CLR components in SQL Server: [http://technet.microsoft.com/en-us/library/ms131048.aspx](http://technet.microsoft.com/en-us/library/ms131048.aspx)

Depending on whether the assembly is signed by a certificate or is unsafe/external access, you may need to use additional steps including creating certificates, setting the database trustworthy property, and changing the owner of the database.

Test CLR assembly functionality on a restored copy of a database prior to migration.
