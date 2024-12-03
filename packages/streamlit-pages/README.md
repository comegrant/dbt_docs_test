# Streamlit Pages

A package to manage streamlit pages state

## Usage
Run the following command to install this package:

```bash
chef add streamlit-pages
```

## Develop

### Installation
To develop on this package, install all the dependencies with `poetry install`.

Then make your changes with your favorite editor of choice.


### Testing
Run the following command to test that the changes do not impact critical functionality:

```bash
chef test
```
This will spin up a Docker image and run your code in a similar environment as our production code.

### Linting
We have added linting which automatically enforces coding style guides.
You may as a result get some failing pipelines from this.

Run the following command to fix and reproduce the linting errors:

```bash
chef lint
```

You can also install `pre-commit` if you want linting to be applied on commit.


## Deploying :rocket:

### Prerequisites

Before starting, make sure you have the following:

1. Access To Azure Portal
    - The Azure Portal is required to manage cloud resources like the container registry and environment variables.
    - This access is necessary to configure and monitor the resources needed by your pipeline. 
2. A Docker image
    - A Docker image is a package that contains the application code, runtime, libraries, and dependencies needed to run your code.
---
#### Setting up Docker Image Build for PRs
To automatically build a Docker image on every pull request, you need the following files. These should be automatically added by **Chef** when creating the project.

**File Paths:**
- sous-chef/.github/workflows/YOUR_PROJECT_pr.yml
- sous-chef/projects/YOUR_PROJECT/**

**File content for `.github/workflows/YOUR_PROJECT_pr.yml`:**
```yaml
name: <YOUR PROJECT> PR Pipeline
on:
  pull_request:
    paths:
      - '.github/workflows/template_test_in_docker.yml'
      - '.github/workflows/YOUR_PROJECT_pr.yml'
      - 'projects/YOUR_PROJECT/**'
  # Allows to trigger the workflow manually in GitHub UI
  workflow_dispatch:  

jobs:
  build-YOUR-PROJECT-image:
    uses: ./.github/workflows/cd_docker_image_template.yml
    with:
      working-directory: projects/YOUR_PROJECT
      dockerfile: Dockerfile
      registry: ${{ vars.CONTAINER_REGISTRY }}
      image-name: your-project
      tag: dev-latest
    secrets: inherit
```
---
3. Necessary environment variables
    - These variables ensure that your application or pipeline can connect to the required resources securely, avoiding the need to hardcode this information.
    - E.g.`DATABRICKS_HOST`, `DATABRICKS_TOKEN`: These allow access to Databricks resources  if the application interacts with Databricks.


### Step 1: Configure a Web App in Azure Portal

#### 1. Access Azure Portal and Create Resource
- Navigate to [Azure Portal](https://portal.azure.com/#home) -> **App Services**.
- Click on **Create** -> **Web App**.


#### 2. Set Configurations for Resource

- **Basics:**
    - **Subscription**: `godtlevertgruppenas`
    - **Resource Group**: `gg-analytics-backend-prod`
    - **Web App Name**: Desired name of app 
    - **Publish**: `Container`
    - **Operating System**: `Linux`
    - **Region**: `North Europe`
    - **Linux Plan**: `analytics-qa`

3. **Database:**
    - Leave the default settings unchanged.

4. **Container:**
    - **Image Source**: `Azure Container Registry`
    - **Registry**: `bhregistry`
    - **Image**: Select your Docker image
    - **Tag**: `dev-latest` (i.e. the latest image built in your PR)
    - **Startup command**: `bash streamlit run <YOUR_APP>.py --server.fileWatcherType none --server.port 80`

5. **Networking:**
    - **Enable public access**: `On`
    - **Enable network injection**: `Off`
    - **NB!** This will be further configured in **Step 2**.

6. **Monitor + secure:**
    - Leave the default settings unchanged.

7. **Tags:**
    - Leave the default settings unchanged (for now).

8. **Review and Create:**
    - If validation did not fail in Container Tab -> `Create`
    - Remember to save!


### Step 2: Configure Resource

1. **Access Resource**
- Navigate to the Resource created in **Step 1**

2. **Configure environment variables**
- Navigate to **Settings** -> **Environment Variables**
- Set environment variables and their respective values, e.g. DATABRICKS_HOST
---
#### **Example Configurations**

1. **For Connecting to Databricks**  
   Use these variables to set up access to Databricks:  
   - `DATABRICKS_HOST` = `<your-databricks-host>`  
   - `DATABRICKS_TOKEN` = `<your-databricks-access-token>`  

2. **For Accessing Azure Data Lake**  
   Use these variables to integrate with Data Lake:  
   - `DATALAKE_SERVICE_ACCOUNT_NAME` = `<your-service-account-name>`  
   - `DATALAKE_STORAGE_ACCOUNT_KEY` = `<your-storage-account-key>`  

3. **For Authentification**  
   *(An example of this can be seen in Attribute Scoring)* 
   - `AZURE_TENANT_ID` = `<azure_tenant_id>`  
   - `CLIENT_ID_STREAMLIT` = `<client_id_streamlit>`  
   - `CLIENT_SECRET_STREAMLIT` = `<client_secret_streamlit>`
---
- Remember to apply your changes.

2. **Configure Network Settings**
- Navigate to **Settings** -> **Inbound Traffic Configurations** -> **Public network access**
- Public network access: `Enabled from select virtual networks and IP addresses`
- Main site/Unmatched rule action: `Deny`
- Add manual IPs:
    - Click `+ Add`
    - Action: `Allow`
    - Name: `Norwegian VPN`
    - Priority: `300`
    - IP Address Block: `<IP of Norwegian VPN>`
    - Click `Add rule`

### Step 3: Enjoy your deployed app! :sparkles: