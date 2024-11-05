#This template is taken from this page: https://learn.microsoft.com/en-us/rest/api/fabric/articles/get-started/deploy-project?toc=%2Fpower-bi%2Fdeveloper%2Fprojects%2FTOC.json

# Parameters 
$workspaceName = ""
if (${env:ENVIRONMENT} -eq "dev") {
    $workspaceName = "Cheffelo [dev]"
}
elseif(${env:ENVIRONMENT} -eq "test"){
    $workspaceName = "Cheffelo [test]"
}
elseif(${env:ENVIRONMENT} -eq "prod"){
    $workspaceName = "Cheffelo"
}
else{
    Write-Host "Workspace name is not set correctly"
}

$modelName = "Main Data Model"
$pbipPath = "D:\a\sous-chef\sous-chef\projects\powerbi\workspace-content\"
$pbipSemanticModelPath = "$($pbipPath)$($modelName).SemanticModel"
$currentPath = (Split-Path $MyInvocation.MyCommand.Definition -Parent)

# Download modules and install
New-Item -ItemType Directory -Path ".\modules" -ErrorAction SilentlyContinue | Out-Null
@("https://raw.githubusercontent.com/microsoft/Analysis-Services/master/pbidevmode/fabricps-pbip/FabricPS-PBIP.psm1"
, "https://raw.githubusercontent.com/microsoft/Analysis-Services/master/pbidevmode/fabricps-pbip/FabricPS-PBIP.psd1") |% {
    Invoke-WebRequest -Uri $_ -OutFile ".\modules\$(Split-Path $_ -Leaf)"
}

if(-not (Get-Module Az.Accounts -ListAvailable)) { 
    Install-Module Az.Accounts -Scope CurrentUser -Force
}

Import-Module ".\modules\FabricPS-PBIP" -Force

#Service principal credential
$secret = ${env:CLIENT_SECRET} | ConvertTo-SecureString -AsPlainText -Force
$credential = [System.Management.Automation.PSCredential]::new(${env:CLIENT_ID},$secret)  
$secureStringPtr = [System.Runtime.InteropServices.Marshal]::SecureStringToBSTR($credential.Password)
$plainTextPwd = [System.Runtime.InteropServices.Marshal]::PtrToStringAuto($secureStringPtr)
Set-FabricAuthToken -servicePrincipalId $credential.UserName `
                    -servicePrincipalSecret $plainTextPwd `
                    -tenantId ${env:TENANT_ID} -reset

Set-Location $currentPath

# Ensure workspace exists
$workspaceId = New-FabricWorkspace  -name $workspaceName -skipErrorIfExists

# Import the semantic model and save the item id
Set-SemanticModelParameters -path $pbipSemanticModelPath -parameters @{
    "Catalog"       = ${env:ENVIRONMENT}; 
    "Schema"        = "gold"; 
    "Host"          = ${env:DATABRICKS_HOST}; 
    "HttpPath"      = ${env:DATABRICKS_HTTP_PATH}
}

$semanticModelImport = Import-FabricItem -workspaceId $workspaceId -path $pbipSemanticModelPath

# Get all paths ending with .Report
$reportDirs = Get-ChildItem -Path $pbipPath -Directory -Recurse | Where-Object { $_.Name -like '*.Report' }

# Itererer over hver mappe og utfører handlinger
foreach ($dir in $reportDirs) {
    if ($dir.Name -eq "$($modelName).Report") {
        continue
    }
    $pbipReportPath = $dir.FullName
    $definitionFilePath = "$($pbipReportPath)/definition.pbir"

    $jsonContent = Get-Content -Path $definitionFilePath -Raw | ConvertFrom-Json
    $jsonContent.datasetReference.byConnection.connectionString = "Data Source=powerbi://api.powerbi.com/v1.0/myorg/$workspaceName;Initial Catalog='$modelName';Access Mode=readonly;Integrated Security=ClaimsToken"
    $jsonContent | ConvertTo-Json -Depth 10 | Set-Content -Path $definitionFilePath

    $reportImport = Import-FabricItem -workspaceId $workspaceId -path $pbipReportPath -itemProperties @{"semanticModelId" = $semanticModelImport.Id}
}

# Write variables to GITHUB_ENV for exposing them to preceding step
Add-Content -Path $env:GITHUB_ENV -Value "SEMANTIC_MODEL_ID=$($semanticModelImport.Id)"
Add-Content -Path $env:GITHUB_ENV -Value "WORKSPACE_ID=$workspaceId"