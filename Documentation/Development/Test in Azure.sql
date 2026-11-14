DROP TABLE IF EXISTS dbo.BlitzWho_Results;
DROP TABLE IF EXISTS dbo.BlitzFirst;
DROP TABLE IF EXISTS dbo.BlitzFirst_FileStats;
DROP TABLE IF EXISTS dbo.BlitzFirst_PerfmonStats;
DROP TABLE IF EXISTS dbo.BlitzFirst_WaitStats;
DROP TABLE IF EXISTS dbo.BlitzCache;
DROP TABLE IF EXISTS dbo.BlitzWho;
DROP TABLE IF EXISTS dbo.BlitzLock;
GO
EXEC dbo.sp_BlitzCache @SortOrder = 'all';
GO
EXEC dbo.sp_BlitzCache @OutputSchemaName = 'dbo', @OutputTableName = 'BlitzCache';
GO
EXEC dbo.sp_BlitzFirst @Seconds = 5, @ExpertMode = 1;
GO
EXEC dbo.sp_BlitzFirst @SinceStartup = 1;
GO
EXEC dbo.sp_BlitzFirst @OutputSchemaName = 'dbo',
                       @OutputTableName = 'BlitzFirst',
                       @OutputTableNameFileStats = 'BlitzFirst_FileStats',
                       @OutputTableNamePerfmonStats = 'BlitzFirst_PerfmonStats',
                       @OutputTableNameWaitStats = 'BlitzFirst_WaitStats',
                       @OutputTableNameBlitzCache = 'BlitzCache',
                       @OutputTableNameBlitzWho = 'BlitzWho';
GO
EXEC dbo.sp_BlitzIndex @Mode = 0;
GO
EXEC dbo.sp_BlitzIndex @Mode = 1;
GO
EXEC dbo.sp_BlitzIndex @Mode = 2;
GO
EXEC dbo.sp_BlitzIndex @Mode = 3;
GO
EXEC dbo.sp_BlitzIndex @Mode = 4;
GO
EXEC dbo.sp_BlitzIndex @TableName = 'Users'
GO
/* A deterministic deadlock graph exercises Azure's supported table-source path.
   It is a parser fixture, not a live XE capture test. Its date range is explicit
   so the test remains valid after the fixture's event timestamp. */
IF OBJECT_ID(N'dbo.FRKReleaseDeadlockInput') IS NOT NULL
    THROW 51000, 'FRKReleaseDeadlockInput already exists; do not overwrite it.', 1;
CREATE TABLE dbo.FRKReleaseDeadlockInput(EventData xml NOT NULL);
BEGIN TRY
    INSERT dbo.FRKReleaseDeadlockInput VALUES(N'<event name="xml_deadlock_report" package="sqlserver" timestamp="2026-09-12T00:45:02.719Z">
  <data name="xml_report">
    <type name="xml" package="package0" />
    <value>
      <deadlock>
        <victim-list>
          <victimProcess id="processc959084e8" />
        </victim-list>
        <process-list>
          <process id="processc959084e8" taskpriority="0" logused="240" waitresource="KEY: 5:72057594047496192 (8194443284a0)" waittime="4236" ownerId="557637" transactionname="user_transaction" lasttranstarted="2026-09-12T00:44:56.473" XDES="0xca8500470" lockMode="X" schedulerid="7" kpid="920" status="suspended" spid="68" sbid="0" ecid="0" priority="0" trancount="2" lastbatchstarted="2026-09-12T00:44:56.470" lastbatchcompleted="2026-09-12T00:44:56.473" lastattention="1900-01-01T00:00:00.473" clientapp="sqlcmd" hostname="FRKSmokeHost" hostpid="1" loginname="FRKSmokeLogin" isolationlevel="read committed (2)" xactid="557637" currentdb="5" currentdbname="FRKSmokeTest" lockTimeout="4294967295" clientoption1="671088672" clientoption2="128056">
            <executionStack>
              <frame>UPDATE dbo.FRKFixture SET Value=1;</frame>
              <frame>UPDATE dbo.FRKFixture SET Value=1;</frame>
            </executionStack>
            <inputbuf>UPDATE dbo.FRKFixture SET Value=1;</inputbuf>
          </process>
          <process id="processc80078ca8" taskpriority="0" logused="240" waitresource="KEY: 5:72057594047496192 (61a06abd401c)" waittime="4240" ownerId="557640" transactionname="user_transaction" lasttranstarted="2026-09-12T00:44:56.473" XDES="0xca2604470" lockMode="X" schedulerid="1" kpid="620" status="suspended" spid="69" sbid="0" ecid="0" priority="0" trancount="2" lastbatchstarted="2026-09-12T00:44:56.470" lastbatchcompleted="2026-09-12T00:44:56.473" lastattention="1900-01-01T00:00:00.473" clientapp="sqlcmd" hostname="FRKSmokeHost" hostpid="1" loginname="FRKSmokeLogin" isolationlevel="read committed (2)" xactid="557640" currentdb="5" currentdbname="FRKSmokeTest" lockTimeout="4294967295" clientoption1="671088672" clientoption2="128056">
            <executionStack>
              <frame>UPDATE dbo.FRKFixture SET Value=1;</frame>
              <frame>UPDATE dbo.FRKFixture SET Value=1;</frame>
            </executionStack>
            <inputbuf>UPDATE dbo.FRKFixture SET Value=1;</inputbuf>
          </process>
        </process-list>
        <resource-list>
          <keylock hobtid="72057594047496192" dbid="5" objectname="FRKSmokeTest.dbo.CodexLockFixture" indexname="PK__CodexLoc__3214EC0793D77C34" id="lockc88628c00" mode="X" associatedObjectId="72057594047496192">
            <owner-list>
              <owner id="processc80078ca8" mode="X" />
            </owner-list>
            <waiter-list>
              <waiter id="processc959084e8" mode="X" requestType="wait" />
            </waiter-list>
          </keylock>
          <keylock hobtid="72057594047496192" dbid="5" objectname="FRKSmokeTest.dbo.CodexLockFixture" indexname="PK__CodexLoc__3214EC0793D77C34" id="lockc9f6c9f00" mode="X" associatedObjectId="72057594047496192">
            <owner-list>
              <owner id="processc959084e8" mode="X" />
            </owner-list>
            <waiter-list>
              <waiter id="processc80078ca8" mode="X" requestType="wait" />
            </waiter-list>
          </keylock>
        </resource-list>
      </deadlock>
    </value>
  </data>
</event>');
    DECLARE @FixtureDatabase sysname = DB_NAME();
    EXEC dbo.sp_BlitzLock @TargetDatabaseName = @FixtureDatabase,
        @TargetSchemaName = N'dbo', @TargetTableName = N'FRKReleaseDeadlockInput',
        @TargetColumnName = N'EventData', @StartDate = '20260911', @EndDate = '20260913',
        @SkipExecutionPlans = 1, @OutputDatabaseName = @FixtureDatabase,
        @OutputSchemaName = N'dbo', @OutputTableName = N'BlitzLock';
    IF OBJECT_ID(N'dbo.BlitzLock', N'U') IS NULL
        THROW 51000, 'sp_BlitzLock did not create its output table.', 1;
    EXEC(N'IF (SELECT COUNT(DISTINCT spid) FROM dbo.BlitzLock
              WHERE spid IN (68, 69) AND client_app = N''sqlcmd''
              AND CONVERT(nvarchar(max), deadlock_graph) LIKE N''%CodexLockFixture%'') <> 2
        THROW 51000, ''sp_BlitzLock did not parse both fixture participants.'', 1;');
    DROP TABLE dbo.FRKReleaseDeadlockInput;
END TRY
BEGIN CATCH
    DROP TABLE IF EXISTS dbo.FRKReleaseDeadlockInput;
    THROW;
END CATCH;
GO
EXEC dbo.sp_BlitzWho @ExpertMode = 1;
GO
EXEC dbo.sp_BlitzWho @ExpertMode = 0;
GO
EXEC dbo.sp_BlitzWho @OutputSchemaName = 'dbo', @OutputTableName = 'BlitzWho_Results';
GO
EXEC dbo.sp_kill @ExecuteKills = 'N';
GO
EXEC dbo.sp_kill @ExecuteKills = 'Y';
GO
