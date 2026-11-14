param(
    [Parameter(Mandatory)][string]$ServerInstance,
    [string]$BuildScriptPath = (Join-Path $PSScriptRoot '../../Documentation/Development/Build Installation Scripts.ps1')
)
$ErrorActionPreference = 'Stop'
$source = Get-Content $BuildScriptPath -Raw -Encoding UTF8
$sql = [regex]::Match($source, '(?s)\$outputPrerequisite.*?@"\r?\n(.*?)\r?\n"@').Groups[1].Value
if (-not $sql) { throw 'Could not extract the real DBAtools prerequisite.' }
$sql = $sql.Replace("'", "''")
# A user without a login cannot connect independently. All changes are rolled back.
$query = @"
IF USER_ID('frk4203') IS NOT NULL THROW 51001, 'Test user already exists.', 1;
BEGIN TRAN;
CREATE USER frk4203 WITHOUT LOGIN;
GRANT CREATE TABLE, CREATE VIEW TO frk4203;
GRANT ALTER, SELECT, INSERT, DELETE ON SCHEMA::dbo TO frk4203;
DECLARE @q nvarchar(max) = N'$sql';
EXECUTE AS USER = 'frk4203';
EXEC(@q);
REVERT;
PRINT 'PASS: all required permissions accepted.';
DENY CREATE VIEW TO frk4203;
EXECUTE AS USER = 'frk4203';
BEGIN TRY
    EXEC(@q);
    THROW 51001, 'Expected CREATE VIEW denial to fail the prerequisite.', 1;
END TRY
BEGIN CATCH
    IF ERROR_NUMBER() <> 51000 OR ERROR_MESSAGE() NOT LIKE 'Release tests require%' THROW;
    PRINT 'PASS: missing CREATE VIEW permission rejected.';
END CATCH;
REVERT;
ROLLBACK;
"@
# Any unexpected error terminates this connection and rolls back its transaction.
$output = & sqlcmd -S $ServerInstance -E -C -b -I -d DBAtools -Q $query 2>&1
$exitCode = $LASTEXITCODE
$output | Write-Output
if ($exitCode) { throw 'Live output-permission regression failed.' }
