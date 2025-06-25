#This template is taken from this page: https://learn.microsoft.com/en-us/rest/api/fabric/articles/get-started/deploy-project?toc=%2Fpower-bi%2Fdeveloper%2Fprojects%2FTOC.json

. "$PSScriptRoot/powerbi_add_refresh_policy.ps1"

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
    Install-Module Az.Accounts -Scope CurrentUser -Force -AllowClobber
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

# Update parameters to correct catalog and schema
Set-SemanticModelParameters -path $pbipSemanticModelPath -parameters @{
    "Catalog"       = ${env:ENVIRONMENT}; 
    "Schema"        = "gold"; 
    "Host"          = ${env:DATABRICKS_HOST}; 
    "HttpPath"      = ${env:DATABRICKS_HTTP_PATH}
}

# Change from directQuery to import mode
$tablesPath = "$($pbipSemanticModelPath)/definition/tables"
$tableFiles = Get-ChildItem -Path $tablesPath -Filter "*.tmdl"
foreach ($file in $tableFiles) {
    $tmdlContent = Get-Content -Path $file.FullName -Raw
    $updatedContent = $tmdlContent -replace "directQuery", "import"
    Set-Content -Path $file.FullName -Value $updatedContent
    Write-Host "Updated $($file.Name) to import mode."
}

# Add incremental load
$ordersPath = "$($tablesPath)/Order Measures.tmdl"
Add-RefreshPolicy -tmdlFilePath $ordersPath -sourceTableName "fact_orders" -dateFieldName "source_created_at"
$factBillingAgreementsPath = "$($tablesPath)/Fact Billing Agreements.tmdl"
Add-RefreshPolicy -tmdlFilePath $factBillingAgreementsPath -sourceTableName "fact_billing_agreements_daily" -dateFieldName "date"

# Import the semantic model and save the item id
$semanticModelImport = Import-FabricItem -workspaceId $workspaceId -path $pbipSemanticModelPath

# Get all paths ending with .Report
$reportDirs = Get-ChildItem -Path $pbipPath -Directory -Recurse | Where-Object { $_.Name -like '*.Report' }

# Iterating over each .Report folder, connecting to correct semantic model and deploys
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