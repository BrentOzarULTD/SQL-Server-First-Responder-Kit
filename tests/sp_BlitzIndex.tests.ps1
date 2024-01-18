Describe "sp_BlitzIndex Tests" {
    
    It "sp_BlitzIndex Check" {     
        $results = Invoke-SqlCmd -Query "EXEC dbo.sp_BlitzIndex" -OutputAs DataSet
        $results.Tables.Count | Should -Be 1
        $results.Tables[0].Columns.Count | Should -Be 12
        $results.Tables[0].Rows.Count | Should -BeGreaterThan 0
    }
    
}