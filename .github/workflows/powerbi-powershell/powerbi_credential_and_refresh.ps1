
# Download Power BI modules for updating credential on dataset
Install-Module -Name MicrosoftPowerBIMgmt -Scope CurrentUser -Force
Import-Module MicrosoftPowerBIMgmt -Force

#Log in as service principal
$secureClientSecret = ConvertTo-SecureString ${env:CLIENT_SECRET} -AsPlainText -Force
$credential = New-Object -TypeName System.Management.Automation.PSCredential -ArgumentList ${env:CLIENT_ID}, $secureClientSecret
Connect-PowerBIServiceAccount -ServicePrincipal -TenantId ${env:TENANT_ID} -Credential $credential

# Get token access token
$body = @{
    grant_type    = "client_credentials"
    scope         = "https://analysis.windows.net/powerbi/api/.default"
    client_id     = ${env:CLIENT_ID}
    client_secret = ${env:CLIENT_SECRET} 
} 

$tokenResponse = Invoke-RestMethod -Uri "https://login.microsoftonline.com/${env:TENANT_ID}/oauth2/v2.0/token" -Method Post -ContentType "application/x-www-form-urlencoded" -Body $body
$accessToken = $tokenResponse.access_token

$headers = @{
    "Content-Type"  = "application/json"
    "Authorization" = "Bearer $accessToken"
}

# Prepare body for credential update

$credentialsJson = @{
    credentialData = @(
        @{
            name  = "key"
            value = ${env:PAT}
        }
    )
} | ConvertTo-Json -Compress

$body = @{
    credentialDetails = @{
        credentialType = "Key"
        credentials = $credentialsJson
        privacyLevel = "None"
        encryptedConnection = "Encrypted"
        encryptionAlgorithm = "None"
    }
} | ConvertTo-Json -Depth 5

#Get gatewayId and datasourceId needed for credential update
$datasources = Invoke-RestMethod -Uri "https://api.powerbi.com/v1.0/myorg/groups/${env:WORKSPACE_ID} /datasets/${env:SEMANTIC_MODEL_ID}  /datasources" -Method Get -Headers $headers
$gatewayId = $datasources.value[0].gatewayId
$datasourceId = $datasources.value[0].datasourceId

#Take over semantic model
Invoke-RestMethod -Uri "https://api.powerbi.com/v1.0/myorg/groups/${env:WORKSPACE_ID} /datasets/${env:SEMANTIC_MODEL_ID}  /Default.TakeOver" -Method Post -Headers $headers

#Update data source credential on dataset
Invoke-RestMethod -Uri "https://api.powerbi.com/v1.0/myorg/gateways/$gatewayId/datasources/$datasourceId" -Method Patch -Body $body -Headers $headers

#Refresh semantic model
Invoke-RestMethod -Uri "https://api.powerbi.com/v1.0/myorg/groups/${env:WORKSPACE_ID} /datasets/${env:SEMANTIC_MODEL_ID}  /refreshes" -Method Post -Headers $headers

