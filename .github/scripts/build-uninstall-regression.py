"""Generate a destructive uninstall regression for the disposable CI server only.

Run after the smoke matrix: all-database mode intentionally removes the kit.
The production script is embedded verbatim except for its allDatabases setting.
"""
from pathlib import Path

source = (Path(__file__).resolve().parents[2] / 'Uninstall.sql').read_text()
def literal(value):
    return "N'" + value.replace("'", "''") + "'"
def identifier(value):
    return '[' + value.replace(']', ']]') + ']'

names = ["FRKUninstall'雪]", 'FRKUninstall' + 'x' * 115 + ']']
print('USE master; SET NOCOUNT ON;')
print("DECLARE @DataPath nvarchar(4000) = CONVERT(nvarchar(4000), SERVERPROPERTY('InstanceDefaultDataPath'));")
print("DECLARE @CreateDatabase nvarchar(max);")
print("IF OBJECT_ID(N'dbo.sp_Blitz') IS NULL THROW 51000, 'Install the kit before testing uninstall.', 1;")
for name in names:
    print(f"IF DB_ID({literal(name)}) IS NOT NULL THROW 51000, 'Uninstall fixture already exists.', 1;")
    if len(name) < 100:
        print(f'EXEC({literal("CREATE DATABASE " + identifier(name))});')
    else:
        prefix = 'CREATE DATABASE ' + identifier(name) + " ON PRIMARY (NAME=N'FRKUninstallLong', FILENAME=N'"
        middle = "FRKUninstallLong.mdf') LOG ON (NAME=N'FRKUninstallLong_log', FILENAME=N'"
        suffix = "FRKUninstallLong_log.ldf');"
        print(f"SET @CreateDatabase = {literal(prefix)} + REPLACE(@DataPath, '''', '''''') + {literal(middle)} + REPLACE(@DataPath, '''', '''''') + {literal(suffix)};")
        print('EXEC(@CreateDatabase);')
    setup = f'''USE {identifier(name)};
EXEC(N'CREATE SCHEMA custom');
EXEC(N'CREATE PROCEDURE dbo.sp_Blitz AS RETURN;');
EXEC(N'CREATE PROCEDURE custom.sp_Blitz AS RETURN;');
EXEC(N'CREATE PROCEDURE dbo.KeepMe AS RETURN;');
CREATE TABLE dbo.SqlServerVersions (n int);
CREATE TABLE custom.SqlServerVersions (n int);'''
    print(f'EXEC({literal(setup)});')

# Current-database path must not fall through to the system procedure in master.
current_source = "USE " + identifier(names[0]) + ";" + chr(10) + source
print(f'EXEC({literal(current_source)});')
print("IF OBJECT_ID(N'master.dbo.sp_Blitz') IS NULL THROW 51000, 'Current uninstall removed the master procedure.', 1;")

def assertions(name):
    sql = f'''USE {identifier(name)};
IF EXISTS (SELECT 1 FROM sys.procedures WHERE schema_id = 1 AND name = N'sp_Blitz')
    THROW 51000, 'dbo kit procedure remains.', 1;
IF NOT EXISTS (SELECT 1 FROM sys.procedures WHERE schema_id = SCHEMA_ID(N'custom') AND name = N'sp_Blitz')
    THROW 51000, 'Custom procedure was removed.', 1;
IF OBJECT_ID(N'dbo.KeepMe') IS NULL OR OBJECT_ID(N'custom.SqlServerVersions') IS NULL
    THROW 51000, 'Unrelated object was removed.', 1;
IF OBJECT_ID(N'dbo.SqlServerVersions') IS NOT NULL
    THROW 51000, 'Kit versions table remains.', 1;'''
    print(f'EXEC({literal(sql)});')
assertions(names[0])
# Exercise the real all-databases enumeration, including both unusual names.
print(f'EXEC({literal(source.replace("DECLARE @allDatabases bit = 0;", "DECLARE @allDatabases bit = 1;"))});')
for name in names:
    assertions(name)
    print(f'EXEC({literal("DROP DATABASE " + identifier(name))});')
print("PRINT 'Uninstall current/all-database regression passed.';")
