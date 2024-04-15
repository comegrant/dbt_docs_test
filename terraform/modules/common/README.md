# Getting started

terraform init

terraform workspace new common
terraform workspace select common
terraform plan -var-file=common.tfvars -out=common
terraform apply "common"

# Build and Test
TODO: Describe and show how to build your code and run the tests. 