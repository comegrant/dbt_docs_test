# Getting started

## Set environmental variables
This is only needed when running locally. 
The client secret used below as environmental variable can be found in key vault: 
https://portal.azure.com/#@godtlevertno.onmicrosoft.com/asset/Microsoft_Azure_KeyVault/Secret/https://kv-chefdp-common.vault.azure.net/secrets/azure-sp-azureResources-clientSecret

export TF_VAR_client_secret="your-client-secret"

## Go to directory
cd deployment

## Terraforming
terraform init -backend-config=backend.conf

terraform workspace select dev
terraform plan -out=dev
terraform apply "dev"

# Build and Test
TODO: Describe and show how to build your code and run the tests. 

# Manual steps
For serverless sql NCC must be added. This might be possible to do with terraform, but is manual process for now. 
https://learn.microsoft.com/en-us/azure/databricks/security/network/serverless-network-security/serverless-firewall