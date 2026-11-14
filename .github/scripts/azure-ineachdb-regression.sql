/* Uses only local temporary state; the Azure runner already verifies edition 5. */
SET NOCOUNT ON;
DECLARE @Current sysname = DB_NAME(), @Quoted nvarchar(258) = QUOTENAME(DB_NAME()),
        @Other sysname = N'FRKMissing_' + CONVERT(nvarchar(36), NEWID());
CREATE TABLE #Visited (DatabaseName sysname);
DECLARE @Cases TABLE (CaseID int IDENTITY, Included nvarchar(max), Excluded nvarchar(max), Expected int);
INSERT @Cases VALUES
    (NULL, NULL, 1),
    (@Quoted, NULL, 1),
    (NULL, @Quoted, 0),
    (@Quoted, @Quoted, 0),
    (QUOTENAME(@Other), NULL, 0),
    (NULL, QUOTENAME(@Other), 1),
    (QUOTENAME(@Other) + N',' + @Quoted, NULL, 1),
    (N'  ' + @Current + N'  ', NULL, 1);
DECLARE @ID int, @In nvarchar(max), @Out nvarchar(max), @Expected int;
WHILE EXISTS (SELECT 1 FROM @Cases)
BEGIN
    SELECT TOP (1) @ID = CaseID, @In = Included, @Out = Excluded, @Expected = Expected
    FROM @Cases ORDER BY CaseID;
    TRUNCATE TABLE #Visited;
    EXEC dbo.sp_ineachdb @command = N'INSERT #Visited VALUES(DB_NAME());',
         @database_list = @In, @exclude_list = @Out, @name_pattern = @Current;
    IF (SELECT COUNT(*) FROM #Visited) <> @Expected
       OR EXISTS (SELECT 1 FROM #Visited WHERE DatabaseName <> @Current)
    BEGIN
        DECLARE @Error nvarchar(2048) = CONCAT(N'Azure include/exclude regression failed, case ', @ID);
        THROW 51000, @Error, 1;
    END;
    DELETE @Cases WHERE CaseID = @ID;
END;
PRINT 'All eight Azure include/exclude assertions passed.';
