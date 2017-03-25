[CmdletBinding()]
[string]$defaultServerInstance = '(local)'
[string]$defaultDatabase = 'master'

$ServerInstance = Read-Host "Enter Server Instance, default is [$($defaultServerInstance)]"
if ($ServerInstance -eq "") {
    $ServerInstance = $defaultServerInstance
}

$Database = Read-Host "Enter Database, default is [$($defaultDatabase)]"
if ($Database -eq "") {
    $Database = $defaultDatabase
}

Write-Output "Installing SQL Server First Responder Kit to [$Database] on $($ServerInstance.ToUpper())"

$url = 'https://github.com/BrentOzarULTD/SQL-Server-First-Responder-Kit/archive/master.zip'

$temp = ([System.IO.Path]::GetTempPath()).TrimEnd("\")
$zipfile = "$temp\SQL-Server-First-Responder-Kit.zip"

# Check we can connect to $Database
try {
    $checkdb = Get-SqlDatabase -ServerInstance $ServerInstance -Name $Database -ErrorAction Stop
}
catch {
    Write-Host "Cannot connect to $Database on $ServerInstance"
    Exit
}

# Check if ReportServer datasbase exists
try {
    $ReportServer = Get-SqlDatabase -ServerInstance $ServerInstance -Name 'ReportServer' -ErrorAction Stop
    $ReportServer = $true
}
catch {
    $ReportServer = $false
}

Write-Output "Downloading archive from github"
try {
    Invoke-WebRequest $url -OutFile $zipfile
} catch {
    #try with default proxy and usersettings
    Write-Output "Probably using a proxy for internet access, trying default proxy settings"
    (New-Object System.Net.WebClient).Proxy.Credentials = [System.Net.CredentialCache]::DefaultNetworkCredentials
    Invoke-WebRequest $url -OutFile $zipfile
}

# Unblock if there's a block
Unblock-File $zipfile -ErrorAction SilentlyContinue

Write-Output "Unzipping"

# Keep it backwards compatible
$shell = New-Object -COM Shell.Application
$zipPackage = $shell.NameSpace($zipfile)
$destinationFolder = $shell.NameSpace($temp)
$destinationFolder.CopyHere($zipPackage.Items())

$scripts = Get-ChildItem "$temp\SQL-Server-First-Responder-Kit-master\*" -Include *.sql

foreach ($script in $scripts) {
    If (($script.Name -ne 'sp_BlitzRS.sql') -or ($script.Name -eq 'sp_BlitzRS.sql' -and $ReportServer -eq $true)) {
        try {
            Invoke-Sqlcmd -ServerInstance $ServerInstance -Database $Database -InputFile $script -SuppressProviderContextWarning -ErrorAction Stop
            Write-Host "Executing $($script.name)"
        } catch {
            Write-Host "Error executing $($script.name)" -ForegroundColor Red
            Write-Host $_ -ForegroundColor Red
            Continue
        }
    } else {
        Write-Host "ReportServer not found skipping $($script.name)"
    }
}

Write-Output "Cleaning up"
Remove-Item -Path "$temp\SQL-Server-First-Responder-Kit-master" -Recurse
Remove-Item -Path $zipfile

Write-Output "Done!"