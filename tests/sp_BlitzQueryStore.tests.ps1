Describe "sp_BlitzQueryStore Tests" {
    
    It "sp_BlitzQueryStore Check" {     
        # Note: Added 'SELECT 1 AS A' as an empty first resultset causes issues returning the full DataSet
        $results = Invoke-SqlCmd -Query "SELECT 1 AS A;EXEC dbo.sp_BlitzQueryStore" -OutputAs DataSet
        # Adjust table count to get the actual tables returned from sp_BlitzQueryStore (So reporting isn't confusing)
        $tableCount = $results.Tables.Count - 1
        ## We haven't specified @DatabaseName and don't have DBs with Query Store enabled so table count is 0
        $tableCount | Should -Be 0
    }
    
}