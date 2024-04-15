# Getting started

cd deployment

terraform init -backend-config=backend.conf

terraform workspace new dev
terraform workspace new test
terraform workspace new prod

terraform workspace select dev
terraform plan -var-file=dev.tfvars -out=dev
terraform apply "dev"

# Build and Test
TODO: Describe and show how to build your code and run the tests. 

# Manual steps
For serverless sql NCC must be added. This might be possible to do with terraform, but is manual process for now. 
https://learn.microsoft.com/en-us/azure/databricks/security/network/serverless-network-security/serverless-firewall