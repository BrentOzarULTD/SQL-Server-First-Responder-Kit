Describe "sp_BlitzCache Tests" {

    It "sp_BlitzCache Check" {     
        # Note: Added 'SELECT 1 AS A' as an empty first resultset causes issues returning the full DataSet
        $results = Invoke-SqlCmd -Query "SELECT 1 AS A;EXEC dbo.sp_BlitzCache" -OutputAs DataSet
        # Adjust table count to get the actual tables returned from sp_BlitzCache (So reporting isn't confusing)
        $tableCount = $results.Tables.Count -1
        $tableCount | Should -Be 2
        $results.Tables[1].Columns.Count | Should -Be 43
        $results.Tables[2].Columns.Count | Should -Be 6
        $results.Tables[2].Rows.Count | Should -BeGreaterThan 0
    }
}