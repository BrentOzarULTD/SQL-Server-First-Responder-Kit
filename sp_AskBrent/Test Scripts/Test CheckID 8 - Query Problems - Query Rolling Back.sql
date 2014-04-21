/* Testing CheckID 9, sleeping queries holding locks. Run this and wait 10 seconds. */

DECLARE @Counter BIGINT = 1
BEGIN TRAN
	UPDATE AdventureWorks2008R2.Sales.Customer
	  SET FirstName = 'x', LastName = 'x', MiddleName = 'x';
	WHILE @Counter > 1000000
	BEGIN
		UPDATE AdventureWorks2008R2.Sales.Customer
		  SET ModifiedDate = DATEADD(ss, -1, ModifiedDate);
		UPDATE AdventureWorks2008R2.Sales.Customer
		  SET ModifiedDate = DATEADD(ss, 1, ModifiedDate);
		SET @Counter = @Counter + 1
	END
/*
To finish the test:
ROLLBACK
*/