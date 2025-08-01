<h1 align="center">
<img src="../assets/terraform/terraform.png" alt="terraform" width="100"/>
<br>
Terraform
</h1>

## What is Terraform?
Terraform is what we use to manage our infrastructure as code. This means that we can define all the resources we need here in code, and then use Terraform to create, update and destroy our infrastructure, rather than doing it all manually within the UI of Databricks and Azure Portal.

It means that we can have better control over our infrastructure, and can spin up and down resources quickly as needed.

## Getting setup
We will walk you through the steps to get started with Terraform.

### Install Terraform
First you need to install Terraform on your machine.

<details>
<summary>Windows</summary>

1. Download the AMD64 program [here](https://developer.hashicorp.com/terraform/install#windows) and follow the instructions to install it.
2. Verify the installation by running the following command in your terminal:
```bash
terraform -help
```

</details>

<details>
<summary>MacOS</summary>

If you have homebrew installed, you can install Terraform with the following command:
```bash
brew tap hashicorp/tap
brew install hashicorp/tap/terraform
```

Or, you can download the binary ARM64 file [here](https://developer.hashicorp.com/terraform/install#macos) and follow the instructions to install it.

Once installed, verify the installation by running the following command in your terminal:
```bash
terraform -help
```

</details>

### Install Azure CLI
Now we will install the Azure CLI so that we can authenticate with Azure.

<details>
<summary>Windows</summary>

1. Open your PowerShell prompt as an administrator and run the following command:
```powershell
Invoke-WebRequest -Uri https://aka.ms/installazurecliwindows -OutFile .\AzureCLI.msi; Start-Process msiexec.exe -Wait -ArgumentList '/I AzureCLI.msi /quiet'; rm .\AzureCLI.msi
```

2. Verify the installation by running the following command in your terminal:
```bash
az --version
```
</details>

<details>
<summary>MacOS</summary>

1. Download the Azure CLI through Homebrew:
```bash
brew update && brew install azure-cli
```

2. Verify the installation by running the following command in your terminal:
```bash
az --version
```
</details>

🔗 [Terraform documentation](https://developer.hashicorp.com/terraform/tutorials/azure-get-started/install-cli#install-terraform) if you have any issues.

### Authenticate with Azure
Now we will authenticate with Azure so that we can use the Azure CLI to create resources.

Run the following command in your terminal and follow the instructions to authenticate:

```bash
az login
```

A browser window will open, where you can login with your Microsoft account.

You will then be taken back to the terminal, where you will see a list of subscriptions that you have access to. Please select the subscription called "Data Platform" by typing the relevant number and pressing enter.

Now we're ready to start terraforming!

## Common Terraform Commands
There are a few basic commands that are used a lot in Terraform, these are:

- `terraform init` - This initialises the Terraform project and installs the necessary tools.
- `terraform fmt` - This formats the Terraform code to a consistent style.
- `terraform validate` - This checks that the Terraform code is valid and internally consistent.
- `terraform plan` - This figures out what changes need to be made to the infrastructure and creates a plan for what to create, update or destroy.
- `terraform apply` - This applies the plan and makes the changes to the infrastructure.
- `terraform destroy` - This destroys all resources managed by Terraform (use with caution).
- `terraform workspace` - This manages workspaces for different environments (dev, staging, prod).

## Typical Terraform Workflow
Let's walk through a typical workflow with Terraform.

1. In order to work with Terraform, you need to navigate to the directory you want to work in.

```bash
cd terraform/deployment
```

2. Initialize the Terraform project to install the necessary tools.

```bash
terraform init
```

3. Make a change to some Terraform code
4. Format the Terraform code to a consistent style

```bash
terraform fmt
```

5. Validate the Terraform code to check for errors

```bash
terraform validate
```

6. Plan the changes Terraform will make to the infrastructure

```bash
# Create a plan just for the dev workspace
terraform plan -out=dev
```

7. The plan will be output to the terminal, and you can see what changes will be made. If this looks good, you can move to the next step.

8. Apply the plan and make the changes to the infrastructure

```bash
# Apply the plan for the dev workspace
terraform apply "dev"
```

9. Stage and commit your change, and create a pull request

## Project Structure
```
terraform/
├── deployment/          # Main deployment configuration
│   ├── main.tf         # Main Terraform configuration
│   ├── variables.tf    # Variable declarations
│   ├── backend.conf    # Backend configuration
│   └── *.tfvars        # Variable values for different environments
└── modules/            # Reusable Terraform modules
    ├── databricks/     # Databricks-specific modules
    ├── networking/     # Networking modules
    └── storage/        # Storage modules
```

## Terraform Concepts

### Files
Here is a quick overview of the common files you will find in the Terraform project.
- `main.tf`
  - This is where you define the resources you want to create.

- `variables.tf`
  - Declares input variables used in the `main.tf` file
  - You can compare these variables to the inputs in a Python function, you define the name and maybe a default value, but the real value comes from when it's called.
  - These variables are called through the `main.tf` file, and the values that get sent through these variables are set in either:
    1. Environment variables
    2. Default values in the `variables.tf` file
    3. Values in the `.tfvars` file
    4. Local variables (`locals`) set in `main.tf`
  - Example of a variable at work:
    ```hcl
    # variables.tf

    variable "region" {
      type = string
    }

    # terraform.tfvars

    region = "northeurope"

    # main.tf

    resource "azurerm_resource_group" "this" {
      name = var.region
    }
    ```

- `.tfvars`
  - Contains actual values for variables declared in `variables.tf`

- `backend.conf`
  - Configures the storage location for the state file
  - Contains backend-specific variables like storage account details

- `terraform.tfstate`
  - Tracks the current state of your infrastructure
  - Contains sensitive information and should be excluded from source control
  - Best practice is to store remotely in a backend like Azure Storage

### Block Types
Terraform is a declarative language, which means that you tell it what you want, and it will figure out how to do it. Because of this, you will see a lot of different blocks types, the common ones are:

- `provider`
  - A plugin that enables Terraform to interact with cloud providers, APIs, or services

- `resource`
  - Defines an infrastructure component to be created and managed by Terraform
  - Examples include virtual machines, storage accounts, databases, and networking components

- `module`
  - A container that groups related resources and logic together
  - Requires a `source` argument specifying where the module code is located
  - Variables are passed through to the module in this block which allows for variable reuse across multiple modules

- `variable`
  - Defines input parameters that can be used in a configuration
  - Similar to function arguments in programming languages
  - Referenced using `var.<name>` syntax
  - Defined in `variables.tf` files
  - Can have type constraints, descriptions, and default values

- `output`
  - Defines an output, these can be used as input for other blocks
  - Can be compared with function return values in Python

- `backend`
  - Configures where and how Terraform stores its state data

- `locals`
  - Assigns names to expressions for reuse within a module
  - Can combine multiple variables into a single value
  - Referenced using `local.<name>` syntax
  - Useful for constructing complex names or standardizing common values
  - Example:
    ```hcl
    variable "project_name" {
      type = string
    }

    locals {
      data_lake_name = "dls${var.project_name}${terraform.workspace}"
    }
    ```

## Best Practices
- Always run `terraform plan` before `terraform apply` to review changes
- Only use the dev workspace when working locally, test and prod workspaces should only be modified by CI/CD
- Keep your state files secure and use remote backends
- Use meaningful names for resources and variables
- Document your variables with descriptions
- Use modules to promote code reuse
- Use consistent formatting with `terraform fmt`

## Troubleshooting
Common issues and solutions:

**Authentication Errors**
```bash
# If you get authentication errors, try re-authenticating:
az login
az account set --subscription "Data Platform"
```

**State Lock Issues**
```bash
# If you get state lock errors, check if another process is running:
terraform force-unlock <lock-id>
```

**Module Not Found**
```bash
# If modules aren't found, reinitialize:
terraform init -upgrade
```

## Service Principal Management
Our service principals are stored in the key vault, but we need to refresh them regularly. In order to do this, do the following:

1. Run the [Deploy Terraform](https://github.com/cheffelo/sous-chef/actions/workflows/cd_terraform.yml) workflow in GitHub Actions. This will create a plan and apply it in test.
2. Once this is successful, check the output in the workflow and see the proposed Plan it output. If this looks good and there's nothing unexpected that it will change, then it can be run in production
3. Either get someone with the necessary permissions to approve the deploy to production, or approve it yourself
4. Everything should work automatically with the new service principals as they are stored in the key vault, but you do need to update PowerBI manually
5. To update PowerBI, run the [PowerBI Deploy workflow](https://github.com/cheffelo/sous-chef/actions/workflows/powerbi_deploy.yml) in GitHub Actions, and follow the same deploy to production process as above.

## Manual Steps
For serverless SQL, NCC must be added. This might be possible to do with Terraform, but is a manual process for now.
https://learn.microsoft.com/en-us/azure/databricks/security/network/serverless-network-security/serverless-firewall
