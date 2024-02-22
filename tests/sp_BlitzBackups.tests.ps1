Describe "sp_BlitzBackups Tests" {
   
    It "sp_BlitzBackups Check" {     
        # Give sp_BlitzBackups something to capture by performing a dummy backup of model DB
        # Test to be run in GitHub action but backing up model to NUL should be safe on most systems
        Invoke-SqlCmd -Query "BACKUP DATABASE model TO DISK='NUL'"
        $results = Invoke-SqlCmd -Query "EXEC dbo.sp_BlitzBackups" -OutputAs DataSet
        $results.Tables.Count | Should -Be 3

        $results.Tables[0].Columns.Count | Should -Be 39
        $results.Tables[0].Rows.Count | Should -BeGreaterThan 0

        $results.Tables[1].Columns.Count | Should -Be 32
        $results.Tables[1].Rows.Count | Should -BeGreaterThan 0

        $results.Tables[2].Columns.Count | Should -Be 5
        $results.Tables[2].Rows.Count | Should -BeGreaterThan 0
    }

}
