Describe "sp_BlitzAnalysis Tests" {
   
    It "sp_BlitzAnalysis Check" { 
        
        # Run sp_BlitzFirst to populate the tables used by sp_BlitzAnalysis
        Invoke-SqlCmd -Query "EXEC dbo.sp_BlitzFirst @OutputDatabaseName = 'tempdb', @OutputSchemaName = N'dbo', @OutputTableName = N'BlitzFirst', @OutputTableNameFileStats = N'BlitzFirst_FileStats',@OutputTableNamePerfmonStats  = N'BlitzFirst_PerfmonStats',
        @OutputTableNameWaitStats = N'BlitzFirst_WaitStats',
        @OutputTableNameBlitzCache = N'BlitzCache',
        @OutputTableNameBlitzWho= N'BlitzWho'" 

        $results = Invoke-SqlCmd -Query "EXEC dbo.sp_BlitzAnalysis @OutputDatabaseName = 'tempdb'" -OutputAs DataSet
        $results.Tables.Count | Should -BeGreaterThan 6
    }
    
}
