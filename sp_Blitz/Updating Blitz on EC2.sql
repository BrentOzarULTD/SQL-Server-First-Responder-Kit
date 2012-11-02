/* 
1. Remote desktop into the Blitz VM
2. Save the stored proc in c:\temp\temp.txt, BUT ONLY THE CREATE STATEMENT.
	If there's anything in the file other than the CREATE PROC....GO then the client updates will fail.
	Make sure to not include the update of the sp_BlitzUpdate stored proc.  Only sp_Blitz should be in there.
3. Run this T-SQL in SSMS:
*/

DECLARE @LotsOfText VARCHAR(MAX)
SELECT @LotsOfText = BulkColumn FROM OPENROWSET(BULK 'c:\temp\temp.txt', SINGLE_BLOB) AS x
UPDATE dbo.Scripts SET ScriptDDL = @LotsOfText WHERE ScriptName = 'sp_Blitz'

/*
Then try to update the Blitz script from a client machine and make sure it bumps the version number.
*/