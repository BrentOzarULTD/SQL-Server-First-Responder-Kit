Describe "sp_BlitzLock Tests" {
    
    It "sp_BlitzLock Check" {     
        # Note: Added 'SELECT 1 AS A' as an empty first resultset causes issues returning the full DataSet
        $results = Invoke-SqlCmd -Query "SELECT 1 AS A;EXEC dbo.sp_BlitzLock" -OutputAs DataSet
        # Adjust table count to get the actual tables returned from sp_BlitzLock (So reporting isn't confusing)
        $tableCount = $results.Tables.Count - 1
        $tableCount | Should -Be 3
    }
    
}