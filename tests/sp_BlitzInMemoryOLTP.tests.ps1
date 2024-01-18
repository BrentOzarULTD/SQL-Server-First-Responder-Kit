Describe "sp_BlitzInMemoryOLTP Tests" {
    
    It "sp_BlitzInMemoryOLTP Check" {     
        # Create InMemory OLTP Database
        Invoke-SqlCmd -Query "CREATE DATABASE sp_BlitzInMemoryOLTPTest;ALTER DATABASE sp_BlitzInMemoryOLTPTest SET MEMORY_OPTIMIZED_ELEVATE_TO_SNAPSHOT = ON;ALTER DATABASE sp_BlitzInMemoryOLTPTest ADD FILEGROUP sp_BlitzInMemoryOLTPTest CONTAINS MEMORY_OPTIMIZED_DATA;"
        # Note: Added 'SELECT 1 AS A' as an empty first resultset causes issues returning the full DataSet
        $results = Invoke-SqlCmd -Query "SELECT 1 AS A;EXEC dbo.sp_BlitzInMemoryOLTP" -OutputAs DataSet
        # Adjust table count to get the actual tables returned from sp_BlitzInMemoryOLTP (So reporting isn't confusing)
        $tableCount = $results.Tables.Count -1
        $tableCount | Should -BeGreaterThan 0
    }
    
}