"""Generate a destructive uninstall regression for the disposable CI server only.

Run after the smoke matrix: all-database mode intentionally removes the kit.
The production script is embedded verbatim except for its allDatabases setting.
"""
from pathlib import Path

source = (Path(__file__).resolve().parents[2] / 'Uninstall.sql').read_text()
mode_declaration = "DECLARE @allDatabases bit = 0;"
if source.count(mode_declaration) != 1:
    raise RuntimeError("Expected exactly one allDatabases declaration; cannot generate both uninstall modes.")
all_source = source.replace(mode_declaration, "DECLARE @allDatabases bit = 1;")

def literal(value):
    return "N'" + value.replace("'", "''") + "'"
def identifier(value):
    return '[' + value.replace(']', ']]') + ']'

names = ["FRKUninstall'雪]", 'FRKUninstall' + 'x' * 115 + ']', 'FRKUninstallCaseInsensitive']
print('USE master; SET NOCOUNT ON;')
print("DECLARE @DataPath nvarchar(4000) = CONVERT(nvarchar(4000), SERVERPROPERTY('InstanceDefaultDataPath'));")
print("DECLARE @CreateDatabase nvarchar(max);")
print("DECLARE @MasterBlitzID int = (SELECT object_id FROM master.sys.procedures WHERE schema_id=1 AND name COLLATE Latin1_General_100_BIN2=N'sp_Blitz');")
print("IF @MasterBlitzID IS NULL THROW 51000, 'Install the kit before testing uninstall.', 1;")
for name in names:
    collation = 'Latin1_General_100_CI_AS' if name == names[2] else 'Latin1_General_100_CS_AS'
    print(f"IF DB_ID({literal(name)}) IS NOT NULL THROW 51000, 'Uninstall fixture already exists.', 1;")
    if len(name) < 100:
        print(f'EXEC({literal("CREATE DATABASE " + identifier(name) + " COLLATE " + collation)});')
    else:
        prefix = 'CREATE DATABASE ' + identifier(name) + " ON PRIMARY (NAME=N'FRKUninstallLong', FILENAME=N'"
        middle = "FRKUninstallLong.mdf') LOG ON (NAME=N'FRKUninstallLong_log', FILENAME=N'"
        suffix = "FRKUninstallLong_log.ldf') COLLATE Latin1_General_100_CS_AS;"
        print(f"SET @CreateDatabase = {literal(prefix)} + REPLACE(@DataPath, '''', '''''') + {literal(middle)} + REPLACE(@DataPath, '''', '''''') + {literal(suffix)};")
        print('EXEC(@CreateDatabase);')
    setup = f'''USE {identifier(name)};
EXEC(N'CREATE SCHEMA custom');
EXEC(N'CREATE PROCEDURE dbo.sp_Blitz AS RETURN;');
EXEC(N'CREATE PROCEDURE custom.sp_Blitz AS RETURN;');
EXEC(N'CREATE PROCEDURE dbo.SP_BLITZ AS RETURN;');
EXEC(N'CREATE PROCEDURE dbo.KeepMe AS RETURN;');
CREATE TABLE dbo.SqlServerVersions (n int);
CREATE TABLE custom.SqlServerVersions (n int);'''
    if name == names[2]:
        setup = setup.replace("EXEC(N'CREATE PROCEDURE dbo.sp_Blitz AS RETURN;');", '')
    print(f'EXEC({literal(setup)});')

def assertions(name):
    sql = f'''USE {identifier(name)};
IF EXISTS (SELECT 1 FROM sys.procedures WHERE schema_id = 1 AND name = N'sp_Blitz')
    THROW 51000, 'dbo kit procedure remains.', 1;
IF NOT EXISTS (SELECT 1 FROM sys.procedures WHERE schema_id = SCHEMA_ID(N'custom') AND name = N'sp_Blitz')
    THROW 51000, 'Custom procedure was removed.', 1;
IF NOT EXISTS (SELECT 1 FROM sys.procedures WHERE schema_id=1 AND name COLLATE Latin1_General_100_BIN2=N'SP_BLITZ')
    THROW 51000, 'Differently cased procedure was removed.', 1;
IF OBJECT_ID(N'dbo.KeepMe') IS NULL OR OBJECT_ID(N'custom.SqlServerVersions') IS NULL
    THROW 51000, 'Unrelated object was removed.', 1;
IF OBJECT_ID(N'dbo.SqlServerVersions') IS NOT NULL
    THROW 51000, 'Kit versions table remains.', 1;'''
    if name == names[2]:
        sql = sql.replace("IF NOT EXISTS (SELECT 1 FROM sys.procedures WHERE schema_id=1 AND name COLLATE Latin1_General_100_BIN2=N'SP_BLITZ')\n    THROW 51000, 'Differently cased procedure was removed.', 1;", '')
    print(f'EXEC({literal(sql)});')
# Current mode covers both CS and CI identifier semantics, and must preserve master.
for name in (names[0], names[2]):
    current_source = "USE " + identifier(name) + ";" + chr(10) + source
    print(f'EXEC({literal(current_source)});')
    print("IF NOT EXISTS (SELECT 1 FROM master.sys.procedures WHERE object_id=@MasterBlitzID AND schema_id=1 AND name COLLATE Latin1_General_100_BIN2=N'sp_Blitz') THROW 51000, 'Current uninstall removed the master procedure.', 1;")
    assertions(name)
# Recreate the CI kit name so all-databases mode must remove it as well.
reseed = "USE " + identifier(names[2]) + "; EXEC(N'CREATE PROCEDURE dbo.SP_BLITZ AS RETURN;'); CREATE TABLE dbo.SqlServerVersions(n int);"
print(f'EXEC({literal(reseed)});')
# Exercise the real all-databases enumeration, including both unusual names.
print(f'EXEC({literal(all_source)});')
for name in names:
    assertions(name)
    print(f'EXEC({literal("DROP DATABASE " + identifier(name))});')
print("PRINT 'Uninstall current/all-database regression passed.';")
