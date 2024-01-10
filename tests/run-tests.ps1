# Assign default values if script-scoped variables are not set
$ServerInstance = if ($null -ne $script:ServerInstance) { $script:ServerInstance } else { "localhost" }
$UserName = if ($null -ne $script:UserName) { $script:UserName } else { "sa" }
$Password = if ($null -ne $script:Password) { $script:Password } else { "dbatools.I0" }
$TrustServerCertificate = if ($null -ne $script:TrustServerCertificate) { $script:TrustServerCertificate } else { $true }

$PSDefaultParameterValues = @{
    "*:ServerInstance" = $ServerInstance
    "*:UserName" = $UserName
    "*:Password" = $Password
    "*:TrustServerCertificate" = $TrustServerCertificate
}

Invoke-Pester -PassThru