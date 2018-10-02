SET ANSI_NULLS ON;
SET ANSI_PADDING ON;
SET ANSI_WARNINGS ON;
SET ARITHABORT ON;
SET CONCAT_NULL_YIELDS_NULL ON;
SET QUOTED_IDENTIFIER ON;
SET STATISTICS IO OFF;
SET STATISTICS TIME OFF;
GO

IF OBJECT_ID('dbo.sp_BlitzJobinfo') IS NULL
  EXEC ('CREATE PROCEDURE dbo.sp_BlitzJobinfo AS RETURN 0;');
GO

ALTER PROCEDURE sp_BlitzJobinfo 
                            @WhatToGet varchar(30) = 'ALL',
                            @Job varchar(256) = 'ALL',
                            @Help bit = 0,
                            @VersionDate datetime = NULL OUTPUT
WITH RECOMPILE
AS
BEGIN

SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

DECLARE @HelpText NVARCHAR(4000)
DECLARE @Version VARCHAR(30);
SET @Version = '1.0';
SET @VersionDate = '20180901';

--List of the available information to get
IF OBJECT_ID('tempdb..#InfoList') IS NOT NULL
DROP TABLE #InfoList

CREATE TABLE #InfoList (Entry varchar(30) NOT NULL, Description varchar(512) NOT NULL)
INSERT INTO #InfoList
SELECT 'schedules', 'Gets the list of the defined schedules' UNION
SELECT 'jobs', 'Gets the list of the Jobs' UNION
SELECT 'job history', 'Gets the Job Execution History' UNION
SELECT 'job schedule', 'Gets the association between schedules and Jobs that are running on it' UNION
SELECT 'job overview', 'Gets the list of the Jobs with an overview of their status' UNION
SELECT 'job top 20 by frequency', 'Gets the list of the top 20 jobs by execution frequency' UNION
SELECT 'job top 20 by duration', 'Gets the list of the top 20 jobs by duration' UNION
SELECT 'job top 20 by failures', 'Gets the list of the top 20 jobs by number of failures' UNION
SELECT 'maintenance plans', 'Gets the list of the Maintenance plans of the Instance' UNION
SELECT 'maintenance plans tasks', 'Gets the list of the Tasks for each maintenance plan' UNION
SELECT 'maintenance plans jobs', 'Gets the list of jobs associated with each maintenance plan'

SET @HelpText = '
/*
sp_BlitzJobInfo from http://FirstResponderKit.org
	
This script returns information about the Jobs in your instance.
Example usage:
    
    sp_BlitzJobinfo @WhatToGet = ''job top 20 by duration''

List of what you can get (@WhatToGet):
'

SELECT @HelpText = @HelpText + CHAR(9) + '''' + Entry + '''' + ': ' + Description + CHAR(10)
FROM #InfoList

SET @HelpText += '
Known limitations of this version:
 - Only Microsoft-supported versions of SQL Server. Sorry, 2005 and 2000.
Unknown limitations of this version:
 - We knew them once, but we forgot.
Changes - for the full list of improvements and fixes in this version, see:
https://github.com/BrentOzarULTD/SQL-Server-First-Responder-Kit/milestone/4?closed=1

MIT License
Copyright (c) 2018 Brent Ozar Unlimited
Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:
The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
';

IF @WhatToGet = 'ALL'
    SET @Help = 1

IF @Help = 1
    PRINT @HelpText

DECLARE @Santiy bit = 0

SELECT @Santiy = COUNT(*)
FROM #InfoList
WHERE LOWER(@WhatToGet) = Entry

IF @Santiy = 0
BEGIN
    RAISERROR('Invalid value for parameter @@WhatToGet. See Help for list of accepted paramenters (Launch with @Help=1)',12,1);
    RETURN;
END

IF OBJECT_ID('tempdb..#jobstats') IS NOT NULL
DROP TABLE #jobstats

IF OBJECT_ID('tempdb..#jobinfo') IS NOT NULL
DROP TABLE #jobinfo

IF OBJECT_ID('tempdb..#jobsched') IS NOT NULL
DROP TABLE #jobsched

IF OBJECT_ID('tempdb..#jobsteps') IS NOT NULL
DROP TABLE #jobsteps

--Schedules
IF (LOWER(@WhatToGet) LIKE 'schedules%')
BEGIN

    SELECT
           [schedule_id] AS [ScheduleID],
           [name] AS [ScheduleName],
           CASE [enabled]
               WHEN 1
               THEN 'Yes'
               WHEN 0
               THEN 'No'
           END AS [IsEnabled],
           CASE
               WHEN [freq_type] = 64
               THEN 'Start automatically when SQL Server Agent starts'
               WHEN [freq_type] = 128
               THEN 'Start whenever the CPUs become idle'
               WHEN [freq_type] IN(4, 8, 16, 32)
               THEN 'Recurring'
               WHEN [freq_type] = 1
               THEN 'One Time'
           END [ScheduleType],
           CASE [freq_type]
               WHEN 1
               THEN 'One Time'
               WHEN 4
               THEN 'Daily'
               WHEN 8
               THEN 'Weekly'
               WHEN 16
               THEN 'Monthly'
               WHEN 32
               THEN 'Monthly - Relative to Frequency Interval'
               WHEN 64
               THEN 'Start automatically when SQL Server Agent starts'
               WHEN 128
               THEN 'Start whenever the CPUs become idle'
           END [Occurrence],
           CASE [freq_type]
               WHEN 4
               THEN 'Occurs every '+CAST([freq_interval] AS VARCHAR(3))+' day(s)'
               WHEN 8
               THEN 'Occurs every '+CAST([freq_recurrence_factor] AS VARCHAR(3))+' week(s) on '+CASE
                                                                                                    WHEN [freq_interval]&1 = 1
                                                                                                    THEN 'Sunday'
                                                                                                    ELSE ''
                                                                                                END+CASE
                                                                                                        WHEN [freq_interval]&2 = 2
                                                                                                        THEN ', Monday'
                                                                                                        ELSE ''
                                                                                                    END+CASE
                                                                                                            WHEN [freq_interval]&4 = 4
                                                                                                            THEN ', Tuesday'
                                                                                                            ELSE ''
                                                                                                        END+CASE
                                                                                                                WHEN [freq_interval]&8 = 8
                                                                                                                THEN ', Wednesday'
                                                                                                                ELSE ''
                                                                                                            END+CASE
                                                                                                                    WHEN [freq_interval]&16 = 16
                                                                                                                    THEN ', Thursday'
                                                                                                                    ELSE ''
                                                                                                                END+CASE
                                                                                                                        WHEN [freq_interval]&32 = 32
                                                                                                                        THEN ', Friday'
                                                                                                                        ELSE ''
                                                                                                                    END+CASE
                                                                                                                            WHEN [freq_interval]&64 = 64
                                                                                                                            THEN ', Saturday'
                                                                                                                            ELSE ''
                                                                                                                        END
               WHEN 16
               THEN 'Occurs on Day '+CAST([freq_interval] AS VARCHAR(3))+' of every '+CAST([freq_recurrence_factor] AS VARCHAR(3))+' month(s)'
               WHEN 32
               THEN 'Occurs on '+CASE [freq_relative_interval]
                                     WHEN 1
                                     THEN 'First'
                                     WHEN 2
                                     THEN 'Second'
                                     WHEN 4
                                     THEN 'Third'
                                     WHEN 8
                                     THEN 'Fourth'
                                     WHEN 16
                                     THEN 'Last'
                                 END+' '+CASE [freq_interval]
                                             WHEN 1
                                             THEN 'Sunday'
                                             WHEN 2
                                             THEN 'Monday'
                                             WHEN 3
                                             THEN 'Tuesday'
                                             WHEN 4
                                             THEN 'Wednesday'
                                             WHEN 5
                                             THEN 'Thursday'
                                             WHEN 6
                                             THEN 'Friday'
                                             WHEN 7
                                             THEN 'Saturday'
                                             WHEN 8
                                             THEN 'Day'
                                             WHEN 9
                                             THEN 'Weekday'
                                             WHEN 10
                                             THEN 'Weekend day'
                                         END+' of every '+CAST([freq_recurrence_factor] AS VARCHAR(3))+' month(s)'
           END AS [Recurrence],
           CASE [freq_subday_type]
               WHEN 1
               THEN 'Occurs once at '+STUFF(STUFF(RIGHT('000000'+CAST([active_start_time] AS VARCHAR(6)), 6), 3, 0, ':'), 6, 0, ':')
               WHEN 2
               THEN 'Occurs every '+CAST([freq_subday_interval] AS VARCHAR(3))+' Second(s) between '+STUFF(STUFF(RIGHT('000000'+CAST([active_start_time] AS VARCHAR(6)), 6), 3, 0, ':'), 6, 0, ':')+' & '+STUFF(STUFF(RIGHT('000000'+CAST([active_end_time] AS VARCHAR(6)), 6), 3, 0, ':'), 6, 0, ':')
               WHEN 4
               THEN 'Occurs every '+CAST([freq_subday_interval] AS VARCHAR(3))+' Minute(s) between '+STUFF(STUFF(RIGHT('000000'+CAST([active_start_time] AS VARCHAR(6)), 6), 3, 0, ':'), 6, 0, ':')+' & '+STUFF(STUFF(RIGHT('000000'+CAST([active_end_time] AS VARCHAR(6)), 6), 3, 0, ':'), 6, 0, ':')
               WHEN 8
               THEN 'Occurs every '+CAST([freq_subday_interval] AS VARCHAR(3))+' Hour(s) between '+STUFF(STUFF(RIGHT('000000'+CAST([active_start_time] AS VARCHAR(6)), 6), 3, 0, ':'), 6, 0, ':')+' & '+STUFF(STUFF(RIGHT('000000'+CAST([active_end_time] AS VARCHAR(6)), 6), 3, 0, ':'), 6, 0, ':')
           END [Frequency],
           STUFF(STUFF(CAST([active_start_date] AS VARCHAR(8)), 5, 0, '-'), 8, 0, '-') AS [ScheduleUsageStartDate],
           STUFF(STUFF(CAST([active_end_date] AS VARCHAR(8)), 5, 0, '-'), 8, 0, '-') AS [ScheduleUsageEndDate],
           [date_created] AS [ScheduleCreatedOn],
           [date_modified] AS [ScheduleLastModifiedOn]
    FROM   [msdb].[dbo].[sysschedules]
    ORDER BY
             [ScheduleName];

END

--Maintenance Plans
IF (LOWER(@WhatToGet) LIKE 'maintenance%')
BEGIN

    IF LOWER(@WhatToGet) = 'maintenance plans jobs'
    BEGIN

        SELECT [smj].[plan_id],
               [plan_name] AS [PlanName],
               [smj].[job_id],
               [sj].[name] AS [JobName]
        FROM   msdb.dbo.sysdbmaintplan_jobs smj
        JOIN msdb.dbo.sysjobs sj
        ON sj.job_id = smj.job_id
        JOIN msdb.dbo.sysdbmaintplans smp
        ON smp.plan_id = smj.plan_id;

    END


    /* Get maintenance plan steps if we're on 2008 or newer */
    CREATE TABLE #jobsteps (name NVARCHAR(128), maintenance_plan_xml XML);

    IF CONVERT(VARCHAR(128), SERVERPROPERTY ('productversion')) LIKE '1%'
      INSERT INTO #jobsteps (name, maintenance_plan_xml)
      SELECT name,
                    CAST(CAST(packagedata AS VARBINARY(MAX)) AS XML) AS maintenance_plan_xml
             FROM msdb.dbo.sysssispackages
             WHERE packagetype = 6;


    IF LOWER(@WhatToGet) = 'maintenance plans'
    BEGIN
            WITH XMLNAMESPACES ('www.microsoft.com/SqlServer/Dts' AS DTS),
        maintenance_plan_steps AS (
             SELECT name, maintenance_plan_xml
             FROM #jobsteps
        ), 
        maintenance_plan_table AS (
            SELECT mps.name,
                   c.value('(@DTS:ObjectName)', 'NVARCHAR(128)') AS step_name
            FROM maintenance_plan_steps mps
                CROSS APPLY maintenance_plan_xml.nodes('//DTS:Executables/DTS:Executable') t(c)
            WHERE c.value('(@DTS:ObjectName)', 'NVARCHAR(128)') NOT LIKE '%{%}%'
        ),
        mp_steps_pretty AS (
            SELECT DISTINCT m1.name ,
                STUFF((
                      SELECT N', ' + m2.step_name  
                      FROM  maintenance_plan_table AS m2 
                      WHERE m1.name = m2.name 
                    FOR XML PATH(N'')), 1, 2, N''
                  ) AS maintenance_plan_steps
            FROM maintenance_plan_table AS m1
        ),
        mp_info AS (
            SELECT plan_id ,
                    AVG(DATEDIFF(SECOND, start_time, end_time)) AS avg_runtime_seconds ,
                    SUM(CASE succeeded
                          WHEN 1 THEN 0
                          ELSE 1
                        END) AS failures
            FROM msdb.dbo.sysmaintplan_log
            GROUP BY
                plan_id ,
                succeeded
         )
         SELECT
            p.name ,
            p.id ,
            p.owner ,
            cac.Subplan_Count ,
            tf_info.avg_runtime_seconds ,
            tf_info.failures,
            msp.maintenance_plan_steps
         FROM
            msdb.dbo.sysmaintplan_plans p
            JOIN mp_steps_pretty msp
              ON msp.name = p.name
            CROSS APPLY ( 
              SELECT
                  COUNT(*) AS Subplan_Count
              FROM
                  msdb.dbo.sysmaintplan_subplans sp
              WHERE
                  p.id = sp.plan_id ) cac
            OUTER APPLY (
                SELECT AVG(avg_runtime_seconds) as avg_runtime_seconds, SUM(COALESCE(failures,0)) as failures
                FROM mp_info mp
                WHERE mp.plan_id = p.id

            ) tf_info;

    END
    ELSE IF LOWER(@WhatToGet) = 'maintenance plans tasks'
    BEGIN
         WITH XMLNAMESPACES('www.microsoft.com/SqlServer/Dts' AS p1, 'www.microsoft.com/SqlServer/Dts' AS DTS, 'www.microsoft.com/sqlserver/dts/tasks/sqltask' AS SQLTask),
         X
         AS (
         SELECT
                [Name] AS [PlanName],
                [d].[b].value('./@DTS:refId[1]', 'varchar(200)') AS [TaskName],
                [C].[a].value('./@SQLTask:DatabaseName[1]', 'varchar(20)') AS [DatabaseName]
         FROM   [#jobsteps]
                CROSS APPLY [maintenance_plan_xml].[nodes]('/DTS:Executable/DTS:Executables/DTS:Executable/DTS:Executables/DTS:Executable/DTS:ObjectData/SQLTask:SqlTaskData/SQLTask:SelectedDatabases') AS [c]([a])
                CROSS APPLY [maintenance_plan_xml].[nodes]('/DTS:Executable/DTS:Executables/DTS:Executable/DTS:Executables/DTS:Executable') [d]([b]))
         SELECT
                [PlanName],
                SUBSTRING([TaskName], 9, CHARINDEX('\', [TaskName], 9)-9) AS [SubPlan],
                SUBSTRING([TaskName], CHARINDEX('\', [TaskName], 9)+1, 256) AS [Task],
                [DatabaseName]
         FROM   [X];

    END
END

--Job Info
IF (LOWER(@WhatToGet) LIKE 'job%')
BEGIN
    
    IF (LOWER(@WhatToGet) = 'jobs')
    BEGIN
        SELECT job_id, name, description
        FROM msdb.dbo.sysjobs
    END
    ELSE 
    IF (LOWER(@WhatToGet) = 'job history')
    BEGIN

        SELECT
               [h].[instance_id],
               [j].[name] AS 'JobName',
               [s].[step_id] AS 'Step',
               [JobRunID] as [JobRunID],
               [s].[step_name] AS 'StepName',
               [msdb].[dbo].[agent_datetime]([run_date], [run_time]) AS 'RunDateTime',
               (([run_duration] / 10000 * 3600 + ([run_duration] / 100) % 100 * 60 + [run_duration] % 100)) AS 'RunDurationSeconds',
               (([run_duration] / 10000 * 3600 + ([run_duration] / 100) % 100 * 60 + [run_duration] % 100 + 31) / 60) AS 'RunDurationMinutes',
               [h].[retries_attempted],
               [j].[enabled],
               [j].[job_id],
               [h].[message],
               [c].[name] AS [category_name]
        FROM      [msdb].[dbo].[sysjobs] [j]
                  INNER JOIN [msdb].[dbo].[sysjobsteps] [s] ON [j].[job_id] = [s].[job_id]
                  INNER JOIN [msdb].[dbo].[sysjobhistory] [h] ON [s].[job_id] = [h].[job_id]
                                                                 AND [s].[step_id] = [h].[step_id]
                  LEFT JOIN [msdb].[dbo].[syscategories] [c] ON [j].[category_id] = [c].[category_id]
                  OUTER APPLY
        (
            SELECT TOP 1
                   CAST([job_id] AS VARCHAR(50)) + CAST([run_date] AS VARCHAR) + CAST([run_time] AS VARCHAR) AS [JobRunID]
            FROM   [msdb].[dbo].[sysjobhistory] [K]
            WHERE  [K].[job_id] = [h].[job_id]
                   AND [K].[instance_id] =
        (
            SELECT
                   MIN([instance_id])
            FROM   [msdb].[dbo].[sysjobhistory] [JM]
            WHERE  [JM].[instance_id] > [h].[instance_id]
                   AND [JM].[job_id] = [K].[job_id]
                   AND [JM].[step_id] = 0
        )
        ) AS [JobRunID]

    END
    ELSE IF (LOWER(@WhatToGet) = 'job schedule')
    BEGIN
        
        SELECT
               [SC].[schedule_id],
               [SC].[job_id],
               [S].[name] AS [JobName],
               [ss].[name] AS [ScheduleName],
               IIF([next_run_date] = 0, NULL, [msdb].[dbo].[agent_datetime]([next_run_date], [next_run_time])) AS 'Next_RunDateTime'
        FROM   [msdb].[dbo].[sysjobschedules] [SC]
               JOIN [msdb].[dbo].[sysjobs] [S] ON [S].[job_id] = [SC].[job_id]
               JOIN [msdb].[dbo].[sysschedules] [ss] ON [ss].[schedule_id] = [SC].[schedule_id]
        WHERE  [SC].[schedule_id] <> 1;

    END
    ELSE IF (LOWER(@WhatToGet) = 'job overview')
    BEGIN
            SELECT
            sjh.job_id ,
            SUM(CASE WHEN sjh.run_status = 1
                          AND sjh.step_id = 0 THEN 1
                     ELSE 0
                END) AS job_success ,
            SUM(CASE WHEN sjh.run_status = 3
                          AND sjh.step_id = 0 THEN 1
                     ELSE 0
                END) AS job_cancel ,
            SUM(CASE WHEN sjh.run_status = 2 THEN 1
                     ELSE 0
                END) AS job_retry ,
            SUM(CASE WHEN sjh.run_status = 0
                          AND sjh.step_id = 0 THEN 1
                     ELSE 0
                END) AS job_fail ,
            MAX(cas.last_failure_date) AS last_failure_date ,
            MAX(sjh.operator_id_emailed) AS operator_id_emailed ,
            MIN(CASE WHEN sjh.run_duration IS NOT NULL
                     THEN STUFF(STUFF(STUFF(RIGHT('00000000'
                                                  + CAST(sjh.run_duration AS VARCHAR(8)),
                                                  8), 3, 0, ':'), 6, 0, ':'), 9, 0,
                                ':')
                     ELSE NULL
                END) AS shortest_duration ,
            MAX(CASE WHEN sjh.run_duration IS NOT NULL
                     THEN STUFF(STUFF(STUFF(RIGHT('00000000'
                                                  + CAST(sjh.run_duration AS VARCHAR(8)),
                                                  8), 3, 0, ':'), 6, 0, ':'), 9, 0,
                                ':')
                     ELSE NULL
                END) AS longest_duration ,
            MAX(can.stop_execution_date) AS last_run_date ,
            MAX(can.next_scheduled_run_date) next_run_date
        INTO #jobstats
        FROM
            msdb.dbo.sysjobhistory AS sjh
            OUTER APPLY ( SELECT TOP 1
                            sja.stop_execution_date ,
                            sja.next_scheduled_run_date
                          FROM
                            msdb.dbo.sysjobactivity sja
                          WHERE
                            sja.job_id = sjh.job_id
                          ORDER BY
                            sja.run_requested_date DESC ) can
            OUTER APPLY ( SELECT TOP 1
                            CAST(CAST(s.run_date AS VARCHAR(10)) AS DATETIME) AS last_failure_date
                          FROM
                            msdb.dbo.sysjobhistory AS s
                          WHERE
                            s.job_id = sjh.job_id
                            AND s.run_status = 0
                          ORDER BY
                            sjh.run_date DESC ) AS cas
        GROUP BY
            sjh.job_id;

        SELECT
            sj.job_id ,
            sj.name ,
            CASE sj.enabled
              WHEN 1 THEN 'Enabled'
              ELSE 'Disabled -- Should I be?'
            END AS is_enabled ,
            SUSER_SNAME(sj.owner_sid) AS owner_name ,
            CASE sj.notify_level_eventlog
              WHEN 0 THEN 'Never'
              WHEN 1 THEN 'When the job succeeds'
              WHEN 2 THEN 'When the job fails'
              WHEN 3 THEN 'When the job completes'
            END AS log_when ,
            CASE sj.notify_level_email
              WHEN 0 THEN 'Never'
              WHEN 1 THEN 'When the job succeeds'
              WHEN 2 THEN 'When the job fails'
              WHEN 3 THEN 'When the job completes'
            END AS email_when ,
            ISNULL(so.name, 'No operator') AS operator_name ,
            ISNULL(so.email_address, 'No email address') AS operator_email_address ,
            CASE so.enabled
              WHEN 1 THEN 'Enabled'
              ELSE 'Not Enabled'
            END AS is_operator_enabled,
            ca.name as category_name
        INTO #jobinfo
        FROM
            msdb.dbo.sysjobs sj
            LEFT JOIN msdb.dbo.sysoperators so
            ON so.id = sj.notify_email_operator_id
            LEFT JOIN msdb.dbo.syscategories ca
            ON sj.category_id = ca.category_id;    


        ;WITH jobsched AS (
        SELECT
            sjs.job_id ,
            CASE ss.freq_type
              WHEN 1 THEN 'Once'
              WHEN 4 THEN 'Daily'
              WHEN 8 THEN 'Weekly'
              WHEN 16 THEN 'Monthly'
              WHEN 32 THEN 'Monthly relative'
              WHEN 64 THEN 'When Agent starts'
              WHEN 128 THEN 'When CPUs are idle'
              ELSE NULL
            END AS frequency ,
            CASE ss.freq_type
              WHEN 1 THEN 'O'
              WHEN 4
              THEN 'Every ' + CONVERT(VARCHAR(100), ss.freq_interval) + ' day(s)'
              WHEN 8
              THEN 'Every ' + CONVERT(VARCHAR(100), ss.freq_recurrence_factor)
                   + ' weeks(s) on '
                   + CASE WHEN ss.freq_interval & 1 = 1 THEN 'Sunday, '
                          ELSE ''
                     END + CASE WHEN ss.freq_interval & 2 = 2 THEN 'Monday, '
                                ELSE ''
                           END
                   + CASE WHEN ss.freq_interval & 4 = 4 THEN 'Tuesday, '
                          ELSE ''
                     END + CASE WHEN ss.freq_interval & 8 = 8 THEN 'Wednesday, '
                                ELSE ''
                           END
                   + CASE WHEN ss.freq_interval & 16 = 16 THEN 'Thursday, '
                          ELSE ''
                     END + CASE WHEN ss.freq_interval & 32 = 32 THEN 'Friday, '
                                ELSE ''
                           END
                   + CASE WHEN ss.freq_interval & 64 = 64 THEN 'Saturday, '
                          ELSE ''
                     END
              WHEN 16
              THEN 'Day ' + CONVERT(VARCHAR(100), ss.freq_interval) + ' of every '
                   + CONVERT(VARCHAR(100), ss.freq_recurrence_factor)
                   + ' month(s)'
              WHEN 32
              THEN 'The ' + CASE ss.freq_relative_interval
                              WHEN 1 THEN 'First'
                              WHEN 2 THEN 'Second'
                              WHEN 4 THEN 'Third'
                              WHEN 8 THEN 'Fourth'
                              WHEN 16 THEN 'Last'
                            END + CASE ss.freq_interval
                                    WHEN 1 THEN ' Sunday'
                                    WHEN 2 THEN ' Monday'
                                    WHEN 3 THEN ' Tuesday'
                                    WHEN 4 THEN ' Wednesday'
                                    WHEN 5 THEN ' Thursday'
                                    WHEN 6 THEN ' Friday'
                                    WHEN 7 THEN ' Saturday'
                                    WHEN 8 THEN ' Day'
                                    WHEN 9 THEN ' Weekday'
                                    WHEN 10 THEN ' Weekend Day'
                                  END + ' of every '
                   + CONVERT(VARCHAR(100), ss.freq_recurrence_factor)
                   + ' month(s)'
              ELSE NULL
            END AS schedule,
            CASE ss.freq_subday_type
              WHEN 1
              THEN 'Occurs once at ' + STUFF(STUFF(RIGHT('000000'
                                                         + CONVERT(VARCHAR(8), ss.active_start_time),
                                                         6), 5, 0, ':'), 3, 0, ':')
              WHEN 2
              THEN 'Occurs every ' + CONVERT(VARCHAR(100), ss.freq_subday_interval)
                   + ' Seconds(s) between ' + STUFF(STUFF(RIGHT('000000'
                                                                + CONVERT(VARCHAR(8), ss.active_start_time),
                                                                6), 5, 0, ':'), 3, 0,
                                                    ':') + ' and '
                   + STUFF(STUFF(RIGHT('000000'
                                       + CONVERT(VARCHAR(8), ss.active_end_time),
                                       6), 5, 0, ':'), 3, 0, ':')
              WHEN 4
              THEN 'Occurs every ' + CONVERT(VARCHAR(100), ss.freq_subday_interval)
                   + ' Minute(s) between ' + STUFF(STUFF(RIGHT('000000'
                                                               + CONVERT(VARCHAR(8), ss.active_start_time),
                                                               6), 5, 0, ':'), 3, 0,
                                                   ':') + ' and '
                   + STUFF(STUFF(RIGHT('000000'
                                       + CONVERT(VARCHAR(8), ss.active_end_time),
                                       6), 5, 0, ':'), 3, 0, ':')
              WHEN 8
              THEN 'Occurs every ' + CONVERT(VARCHAR(100), ss.freq_subday_interval)
                   + ' Hour(s) between ' + STUFF(STUFF(RIGHT('000000'
                                                             + CONVERT(VARCHAR(8), ss.active_start_time),
                                                             6), 5, 0, ':'), 3, 0, ':')
                   + ' and ' + STUFF(STUFF(RIGHT('000000'
                                                 + CONVERT(VARCHAR(8), ss.active_end_time),
                                                 6), 5, 0, ':'), 3, 0, ':')
              ELSE NULL
            END AS schedule_detail,
            ss.Name as schedule_name,
            IIF(ss.enabled =1,'Yes','No') as schedule_enabled,
            sjs.schedule_id
        FROM
            msdb.dbo.sysjobschedules AS sjs
            INNER JOIN msdb.dbo.sysschedules AS ss
            ON sjs.schedule_id = ss.schedule_id
        )
        SELECT jobsched.job_id ,
               jobsched.frequency ,
               LEFT(jobsched.schedule, LEN(jobsched.schedule) -1) AS schedule,
               jobsched.schedule_detail,
               jobsched.schedule_name,
               jobsched.schedule_enabled,
               jobsched.schedule_id
        INTO #jobsched
        FROM jobsched
 
        SELECT
            j.name ,
            j.job_id,
            j.is_enabled ,
            j.owner_name ,
            COALESCE(j2.shortest_duration, 'N/A') AS shortest_duration ,
            COALESCE(j2.longest_duration, 'N/A') AS longest_duration ,
            COALESCE(CAST(j2.last_run_date AS VARCHAR(20)), 'N/A') AS last_run_date ,
            COALESCE(CAST(j2.next_run_date AS VARCHAR(20)), 'N/A') AS next_run_date ,
            COALESCE(j3.frequency, 'N/A') AS frequency ,
            COALESCE(j3.schedule, 'N/A') AS schedule ,
            COALESCE(j3.schedule_name, 'N/A') AS schedule_name ,
            COALESCE(j3.schedule_enabled, 'N/A') AS schedule_enabled ,
            COALESCE(CAST(j3.schedule_id as varchar(10)), 'N/A') AS schedule_id ,
            COALESCE(j3.schedule_detail, 'N/A') AS schedule_detail ,
            COALESCE(CAST(j2.job_success AS VARCHAR(10)), 'N/A') AS job_success ,
            COALESCE(CAST(j2.job_cancel AS VARCHAR(10)), 'N/A') AS job_cancel ,
            COALESCE(CAST(j2.job_retry AS VARCHAR(10)), 'N/A') AS job_retry ,
            COALESCE(CAST(j2.job_fail AS VARCHAR(10)), 'N/A') AS job_fail ,
            COALESCE(CAST(j2.last_failure_date AS VARCHAR(20)), 'N/A') AS last_failure_date ,
            COALESCE(CASE WHEN j2.job_fail > 0
                               AND j2.operator_id_emailed = 0
                          THEN 'No operator emailed'
                          ELSE CAST(j2.operator_id_emailed AS VARCHAR(100))
                     END, 'N/A') AS operator_id_emailed ,
            j.log_when ,
            j.email_when ,
            j.operator_name ,
            j.operator_email_address ,
            j.is_operator_enabled,
            j.category_name
        FROM
            #jobinfo AS j
            LEFT JOIN #jobstats AS j2
            ON j2.job_id = j.job_id
            LEFT JOIN #jobsched AS j3
            ON j3.job_id = j.job_id
        ORDER BY CASE WHEN j.is_enabled = 'Enabled' THEN 9999999 ELSE 0 END
    END
    ELSE IF (LOWER(@WhatToGet) = 'job top 20 by frequency')
    BEGIN

        --Top 20 Most Frequent
        BEGIN TRY
        
            SELECT
                   ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) as ID,
                   [sj].[job_id] AS [Job ID],
                   [sj].[name] AS [Job Name],
                   [sl].[name] AS [Owner Name],
        (
            SELECT
                   COUNT(*)
            FROM    [msdb].[dbo].[sysjobschedules] [js1]
            WHERE  [js1].[job_id] = [sj].[job_id]
        ) AS [Schedules Count],
        (
            SELECT
                   COUNT(*)
            FROM   [msdb].[dbo].[sysjobsteps] [js2]
            WHERE  [js2].[job_id] = [sj].[job_id]
        ) AS [Steps Count],
        (
            SELECT
                   COUNT(*)
            FROM   [msdb].[dbo].[sysjobhistory] [jh1]
            WHERE  [jh1].[job_id] = [sj].[job_id]
                   AND [jh1].[step_id] = 0
                   AND [jh1].[run_status] = 0
                   AND DATEDIFF(day, CONVERT(DATETIME, CONVERT(VARCHAR, [jh1].[run_date])), GETDATE()) < 7
        ) AS [Failure Count],
        (
            SELECT
                   AVG((([run_duration] / 10000 * 3600) + (([run_duration] % 10000) / 100 * 60) + ([run_duration] % 100)) + 0.0)
            FROM   [msdb].[dbo].[sysjobhistory] [jh2]
            WHERE  [jh2].[job_id] = [sj].[job_id]
                   AND [jh2].[step_id] = 0
                   AND DATEDIFF(day, CONVERT(DATETIME, CONVERT(VARCHAR, [jh2].[run_date])), GETDATE()) < 7
        ) AS [Average Run Duration],
        (
            SELECT
                   AVG([retries_attempted] + 0.0)
            FROM   [msdb].[dbo].[sysjobhistory] [jh2]
            WHERE  [jh2].[job_id] = [sj].[job_id]
                   AND [jh2].[step_id] = 0
                   AND DATEDIFF(day, CONVERT(DATETIME, CONVERT(VARCHAR, [jh2].[run_date])), GETDATE()) < 7
        ) AS [Average  Retries Attempted],
                   COUNT(*) AS [Execution Count],
                   1 AS [l1]
            FROM [msdb].[dbo].[sysjobhistory] [jh]
                 INNER JOIN [msdb].[dbo].[sysjobs] [sj] ON([jh].[job_id] = [sj].[job_id])
                 INNER JOIN [msdb].[sys].[syslogins] [sl] ON([sl].[sid] = [sj].[owner_sid])
            WHERE [jh].[step_id] = 0
                  AND DATEDIFF(day, CONVERT(DATETIME, CONVERT(VARCHAR, [jh].[run_date])), GETDATE()) < 7
            GROUP BY
                     [sj].[job_id],
                     [sj].[name],
                     [sl].[name]
            ORDER BY
                     [Execution Count] DESC;
        END TRY
        BEGIN CATCH
            SELECT
                   1 AS [Job ID],
                   1 AS [Job Name],
                   1 AS [Owner Name],
                   1 AS [run_status],
                   1 AS [Schedules Count],
                   1 AS [Steps Count],
                   ERROR_NUMBER() AS [Execution Count],
                   ERROR_SEVERITY() AS [Average Run Duration],
                   ERROR_STATE() AS [Average  Retries Attempted],
                   ERROR_MESSAGE() AS [Failure Count],
                   -100 AS [l1];
        END CATCH;

    END
    ELSE IF (LOWER(@WhatToGet) = 'job top 20 by duration')
    BEGIN

        --Top 20 Slowest
        BEGIN TRY
           
            WITH MainQuery
                 AS (
                 SELECT
                        [sj].[job_id] AS [Job ID],
                        [sj].[name] AS [Job Name],
                        [sl].[name] AS [Owner Name],
            (
                SELECT
                       COUNT(*)
                FROM    [msdb].[dbo].[sysjobschedules] [js1]
                WHERE  [js1].[job_id] = [sj].[job_id]
            ) AS [Schedules Count],
            (
                SELECT
                       COUNT(*)
                FROM   [msdb].[dbo].[sysjobsteps] [js2]
                WHERE  [js2].[job_id] = [sj].[job_id]
            ) AS [Steps Count],
            (
                SELECT
                       COUNT(*)
                FROM   [msdb].[dbo].[sysjobhistory] [jh1]
                WHERE  [jh1].[job_id] = [sj].[job_id]
                       AND [jh1].[step_id] = 0
                       AND [jh1].[run_status] = 0
                       AND DATEDIFF(day, CONVERT(DATETIME, CONVERT(VARCHAR, [jh1].[run_date])), GETDATE()) < 7
            ) AS [Failure Count],
            (
                SELECT
                       AVG((([run_duration] / 10000 * 3600) + (([run_duration] % 10000) / 100 * 60) + ([run_duration] % 100)) + 0.0)
                FROM   [msdb].[dbo].[sysjobhistory] [jh2]
                WHERE  [jh2].[job_id] = [sj].[job_id]
                       AND [jh2].[step_id] = 0
                       AND DATEDIFF(day, CONVERT(DATETIME, CONVERT(VARCHAR, [jh2].[run_date])), GETDATE()) < 7
            ) AS [Average Run Duration],
            (
                SELECT
                       AVG([retries_attempted] + 0.0)
                FROM   [msdb].[dbo].[sysjobhistory] [jh2]
                WHERE  [jh2].[job_id] = [sj].[job_id]
                       AND [jh2].[step_id] = 0
                       AND DATEDIFF(day, CONVERT(DATETIME, CONVERT(VARCHAR, [jh2].[run_date])), GETDATE()) < 7
            ) AS [Average  Retries Attempted],
                        COUNT(*) AS [Execution Count],
                        1 AS [l1]
                 FROM [msdb].[dbo].[sysjobhistory] [jh]
                      INNER JOIN [msdb].[dbo].[sysjobs] [sj] ON([jh].[job_id] = [sj].[job_id])
                      INNER JOIN [msdb].[sys].[syslogins] [sl] ON([sl].[sid] = [sj].[owner_sid])
                 WHERE [jh].[step_id] = 0
                       AND DATEDIFF(day, CONVERT(DATETIME, CONVERT(VARCHAR, [jh].[run_date])), GETDATE()) < 7
                 GROUP BY
                          [sj].[job_id],
                          [sj].[name],
                          [sl].[name])
                 SELECT
                        ROW_NUMBER() OVER(ORDER BY [Average Run Duration] DESC) AS [ID],
                        *
                 FROM   [MainQuery]
                 ORDER BY
                          [Average Run Duration] DESC;

        END TRY
        BEGIN CATCH
            SELECT
                   1 AS [Job ID],
                   1 AS [Job Name],
                   1 AS [Owner Name],
                   1 AS [run_status],
                   1 AS [Schedules Count],
                   1 AS [Steps Count],
                   ERROR_NUMBER() AS [Execution Count],
                   ERROR_SEVERITY() AS [Average Run Duration],
                   ERROR_STATE() AS [Average  Retries Attempted],
                   ERROR_MESSAGE() AS [Failure Count],
                   -100 AS [l1];
        END CATCH;

    END
    ELSE IF (LOWER(@WhatToGet) = 'job top 20 by failures')
    BEGIN
       
       --Most Failures
        BEGIN TRY
           
            SELECT TOP 20
                   ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) as ID,
                   [sj].[job_id] AS [Job ID],
                   [sj].[name] AS [Job Name],
                   [sl].[name] AS [Owner Name],
                   [jh].[run_status],
        (
            SELECT
                   COUNT(*)
            FROM    [msdb].[dbo].[sysjobschedules] [js1]
            WHERE  [js1].[job_id] = [sj].[job_id]
        ) AS [Schedules Count],
        (
            SELECT
                   COUNT(*)
            FROM   [msdb].[dbo].[sysjobsteps] [js2]
            WHERE  [js2].[job_id] = [sj].[job_id]
        ) AS [Steps Count],
        (
            SELECT
                   COUNT(*)
            FROM   [msdb].[dbo].[sysjobhistory] [jh1]
            WHERE  [jh1].[job_id] = [sj].[job_id]
                   AND [jh1].[step_id] = 0
                   AND DATEDIFF(day, CONVERT(DATETIME, CONVERT(VARCHAR, [jh1].[run_date])), GETDATE()) < 7
        ) AS [Execution Count],
        (
            SELECT
                   AVG((([run_duration] / 10000 * 3600) + (([run_duration] % 10000) / 100 * 60) + ([run_duration] % 100)) + 0.0)
            FROM   [msdb].[dbo].[sysjobhistory] [jh2]
            WHERE  [jh2].[job_id] = [sj].[job_id]
                   AND [jh2].[step_id] = 0
                   AND DATEDIFF(day, CONVERT(DATETIME, CONVERT(VARCHAR, [jh2].[run_date])), GETDATE()) < 7
        ) AS [Average Run Duration],
        (
            SELECT
                   AVG([retries_attempted] + 0.0)
            FROM   [msdb].[dbo].[sysjobhistory] [jh2]
            WHERE  [jh2].[job_id] = [sj].[job_id]
                   AND [jh2].[step_id] = 0
                   AND DATEDIFF(day, CONVERT(DATETIME, CONVERT(VARCHAR, [jh2].[run_date])), GETDATE()) < 7
        ) AS [Average  Retries Attempted],
                   COUNT(*) AS [Failure Count],
                   1 AS [l1]
            FROM [msdb].[dbo].[sysjobhistory] [jh]
                 INNER JOIN [msdb].[dbo].[sysjobs] [sj] ON([jh].[job_id] = [sj].[job_id])
                 INNER JOIN [msdb].[sys].[syslogins] [sl] ON([sl].[sid] = [sj].[owner_sid])
            WHERE [jh].[step_id] = 0
                  AND DATEDIFF(day, CONVERT(DATETIME, CONVERT(VARCHAR, [jh].[run_date])), GETDATE()) < 7
                  AND [jh].[run_status] = 0
            GROUP BY
                     [sj].[job_id],
                     [sj].[name],
                     [sl].[name],
                     [jh].[run_status]
            ORDER BY
                     [Failure Count] DESC;
        END TRY
        BEGIN CATCH
            SELECT
                   1 AS [Job ID],
                   1 AS [Job Name],
                   1 AS [Owner Name],
                   1 AS [run_status],
                   1 AS [Schedules Count],
                   1 AS [Steps Count],
                   ERROR_NUMBER() AS [Execution Count],
                   ERROR_SEVERITY() AS [Average Run Duration],
                   ERROR_STATE() AS [Average  Retries Attempted],
                   ERROR_MESSAGE() AS [Failure Count],
                   -100 AS [l1];
        END CATCH;
           
    END


END


END