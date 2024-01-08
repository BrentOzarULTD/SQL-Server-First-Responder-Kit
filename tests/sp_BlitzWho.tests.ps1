Describe "sp_BlitzWho Tests" {

    It "sp_BlitzWho Check" {     
        # Give sp_BlitzWho something to capture
        Start-Job -ScriptBlock {
            Invoke-SqlCmd -Query "WAITFOR DELAY '00:00:15'" -ServerInstance $using:ServerInstance -Username $using:UserName -Password $using:Password -TrustServerCertificate:$using:TrustServerCertificate
        } 
        Start-Sleep -Milliseconds 1000
        $results = Invoke-SqlCmd -Query "EXEC dbo.sp_BlitzWho" -OutputAs DataSet
        $results.Tables.Count | Should -Be 1
        $results.Tables[0].Columns.Count | Should -Be 21
        $results.Tables[0].Rows.Count | Should -BeGreaterThan 0
    }

}