Describe "sp_Blitz Tests"  {
   
    It "sp_Blitz Check" {     
        $results = Invoke-SqlCmd -Query "EXEC dbo.sp_Blitz" -OutputAs DataSet
        $results.Tables.Count | Should -Be 1
        $results.Tables[0].Columns.Count | Should -Be 9
        $results.Tables[0].Rows.Count | Should -BeGreaterThan 0
    }

}
