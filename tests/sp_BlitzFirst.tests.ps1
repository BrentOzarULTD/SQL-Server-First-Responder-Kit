Describe "sp_BlitzFirst Tests" {

    It "sp_BlitzFirst Check" {     
        # Give sp_BlitzFirst something to capture
        Start-Job -ScriptBlock {
            Invoke-SqlCmd -Query "WAITFOR DELAY '00:00:15'" -ServerInstance $using:ServerInstance -Username $using:UserName -Password $using:Password -TrustServerCertificate:$using:TrustServerCertificate
        } 
        Start-Sleep -Milliseconds 1000
        $results = Invoke-SqlCmd -Query "EXEC dbo.sp_BlitzFirst" -OutputAs DataSet
        $results.Tables.Count | Should -Be 1
        $results.Tables[0].Columns.Count | Should -Be 8
        $results.Tables[0].Rows.Count | Should -BeGreaterThan 0

        $results = Invoke-SqlCmd -Query "EXEC dbo.sp_BlitzFirst @ExpertMode=1" -OutputAs DataSet
        $results.Tables.Count | Should -Be 7

        $results.Tables[0].Columns.Count | Should -Be 21
        $results.Tables[0].Rows.Count | Should -BeGreaterThan 0

        $results.Tables[1].Columns.Count | Should -Be 40
        $results.Tables[1].Rows.Count | Should -BeGreaterThan 0

        $results.Tables[2].Columns.Count | Should -Be 13
        $results.Tables[2].Rows.Count | Should -BeGreaterThan 0

        $results.Tables[3].Columns.Count | Should -Be 11
        $results.Tables[3].Rows.Count | Should -BeGreaterThan 0

        $results.Tables[4].Columns.Count | Should -Be 10
        $results.Tables[4].Rows.Count | Should -BeGreaterThan 0

        $results.Tables[5].Columns.Count | Should -Be 4
        $results.Tables[5].Rows.Count | Should -BeGreaterThan 0

        $results.Tables[6].Columns.Count | Should -Be 21
        $results.Tables[6].Rows.Count | Should -BeGreaterThan 0

    }
    
}