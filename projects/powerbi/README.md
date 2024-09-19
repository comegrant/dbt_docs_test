# About the Project
This is the official documentation of the Power BI project that is managed by the data team.
This will be what we call the official content, and consist mainly of:
- The central data model(s) that is used by all official reports and end user reports
- The offical reports (e.i. the reports created by the data team for the business)
Please read [the outline document](https://chef.getoutline.com/doc/power-bi-z4Hd5VHnbH) with an overview of Power BI in Cheffelo.

## Terminology
- **Semantic model/data model/dataset**: Different ways of describing the same thing, namely the central semantic model(s) containing the dimension and fact tables with relations and defined measures. Currently there is only _one_ sementaic model which will be used for everything. In the future we might create more, but preferably only 2-3.
- **Report**: This is where visuals are created. The reports will be connected to the semantic model. Sometime people will call this dashboards, but be aware that there is actully something else that is a dashboard in Power Bi. See below.
- **Dashboard**: Dashboards can contain tiles/visuals from one or multiple reports. Dashboards are meant for having a first overview of what is important for the user or the team. People can create their own dashboards or we can create official dashboards for certain groups or purposes.
- **Workspace**: We will have one offical workspace where the data team create all official stuff. The workspace is where the PowerBI content is placed. In addition to the official workspace, we will have end user workspaces where end users can create their own reports.
- **App**: An app is connected to one workspace. The app is meant for distributing reports to users who should only read the reports. Please view this [video showing](https://www.youtube.com/watch?v=nchNa44o2D4) the difference between a workspace and an app and also how to work with apps.
- **Power BI Service**: This is what we call the online version of Power BI. This means where you have your workspaces etc.
- **Power BI Desktop**: This is the desktop app you can install on your computor in order to do advanced work which can not be done in Power BI Service.

## Folder structure
- **powerbi/workspace-content**: This is the folder we will connect the workspaces to through git. The code for all official content (semantic models, reports and dahsboards) will be placed in this folder.
- **powerbi/Rules-Dataset.json**: This file contains the rules we want to test on the semantic models when doing a pull request.

### The structure within `powerbi/workspace-content`
#### *.Report
This contain report definisions ..
More to come here

#### *.SemanticModel
For the semantic model we use the TMDL format which is in Preview in Power BI.
See Microsoft Documentation:
- https://powerbi.microsoft.com/en-us/blog/tmdl-in-power-bi-desktop-developer-mode-preview/
- https://learn.microsoft.com/en-us/power-bi/developer/projects/projects-dataset#tmdl-format

The files we use when developing the semantic model will be found in the sub-folder `definition`.
All table definitions included columns and measures can be found in the `tables` folder below `definition`.

#### *.pbip
The *.pbip file can be opened in Power BI Desktop. Modifications done in Power BI that are saved as a `.pbip` file will update the corresponding `.SemanticModel` and `.Report` files.
This means that changes can be done both in Power BI Desktop by saving to `.pbip` or in a code editor like Visual Studio Code by changing the `.SemanticModel` files or `.Report` files directly.

# Get started

## Prerequisites
- Visual Studio Code is installed on your computer
- Git is installed on your computer
- The `sous-chef` repository is cloned to you computer

## Install required tools (One time only)
1.  Install the extension `TMDL` Visual Studio Code
2. (Optional) Install Power BI Desktop.
    - In Windows you find this in _Microsoft Store_.
    - For Mac this needs a VM. We need to figure out how to do this.
    - After installation update the following options. (You find these in `File`-->`Options and settings`-->`Options`):
        - GLOBAL/Data Load/Time intelligence: Uncheck `Auto date/time for new files`
        - CURRENT FILE/Data Load/Time intelligence: Uncheck `Auto date/time for new files` if its not unchecked already
        - GLOBAL/Preview features: Check all of the following:
            - `Power BI Project (.pbip) save option`
            - `Store semantic model using TMDL option`
            - `Store reports using enhanced metadata format (PBIR)`

## Connect a Power BI workspace to a feature branch (One time only)

### Create personal access token in GitHub
See this [guide](https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/managing-your-personal-access-tokens#creating-a-fine-grained-personal-access-token) and/or follow these steps.
1. Log into GitHub
2. In the upper-right corner of any page on GitHub, click your profile photo, then click _Settings_.
3. In the left sidebar, click _<> Developer settings_.
4. In the left sidebar, under Personal access tokens, click Fine-grained tokens.
5. Click _Generate new token_
6. Enter token name with naming convention `powerbi-<firstname>`, e.g. `powerbi-anna`
7. Select expiration. You will need to update the token before it expires.
8. Add description, e.g. `Token used so that PowerBI Workspaces can be connected to sous-chef in GitHub`
9. Choose `cheffelo` as Resource owner
10. Add request for generating the access token. This will be approved by Stephen.
11. Check _Only select repositories_ and search for sous-chef. Select `cheffelo/sous-chef`
12. Set _Repository permissions_ by adding `Read and write` access on _Contents_
13. Click _Generate token_
14. Make sure to copy the generated token and paste it in e.g. a notepad (you will use this later)
15. Make sure Stephen approves your token request.


### Create a branch in `sous-chef`
This should be based on `main` and be placed in a folder with the developers name.
There is a bug/limitation in Power BI that limits how many branches that show in PowerBI, so that and only those in the beginning of the alphabet will show. The GitHub connection is in preview so hopefully this will be fixed later.
Suggested branch name is therefore `0_<developer_name>/<feature_name>`. E.g. `0_anna/testing_git_for_powerbi`.

### Create a workspace in Power BI
This will be used as a personal area to checkout feature branches. It should not be the "My Workspace" but a new workspace. Make sure to have the following settings:
- _Workspace Name_: "[<Users first name>] Features", e.g. `[Anna] Features`
- _License Configuration_: Premium per-user
- _Semantic model storage format_: Large semantic model storage format

### Connect the workspace to GitHub
1. Below "Workspace Settings", navigate to "Git integration".
2. Add a GitHub account
    - Display name: `sous-chef-<developer_name>`, e.g. `sous-chef-anna`
    - Personal access token: Paste the access token you generated earlier
    - Repository URL: `https://github.com/cheffelo/sous-chef`
3. Select the branch you created above
4. Select Git folder `/projects/powerbi/workspace-content`
5. You might need to refresh the semantic model to see data.
    - The first time you do this you might be asked to provide credential. You may do as follows:
        1. Set Authentication method to `Key`.
        2. Add the key value. This can be found in [key vault](https://portal.azure.com/#@godtlevertno.onmicrosoft.com/resource/subscriptions/5a07602a-a1a5-43ee-9770-2cf18d1fdaf1/resourceGroups/rg-chefdp-common/providers/Microsoft.KeyVault/vaults/kv-chefdp-common/secrets) and is called `databricks_sp_bundle_pat_<env>` where `<env>` is the environment you are connecting to (usually `dev`).
        3. Set Privacy level to `Organizational`.
    - The first time you refresh, this might take a while (about 30 min), but the next times this will go faster due to incremental refresh (explaination of this will come later).

Going forward, when working with new feature branches you need to update the branch here in the "git integration settings".

## Editing the semantic model and reports
There are two ways of updating the _semantic model_
1. Within **Power BI Desktop** and save the file as a pbip file. This will automatically update the TMDL files (see option 2).
2. Modifying the TMDL files in e.g. **Visual Studio Code**. This will automatically update the pbip file (see option 1).

There are three ways of updating the _reports_
1. In the workspace in **Power BI Service**. When you do this in your feature workspace connected to git you can write commit messages and push the changes to github (recommended).
2. In **Power BI Desktop**. Be sure to connect to the existing data model in the service and to save the report within the `projects/powerbi/workspace-content` folder in the `sous-chef` repo.
3. Through code in e.f. **Visual Studio Code**. The *.Report files will be modifyable using code. Usually it is not easy to know how to change visualizations using code, but if one need to update a measure name used in many visualizations this is a nice way of doing it.

Note: If you create a new report, allways make sure to connect to the Main Data Model in your feature workspace. This will then be deployed to dev, test and production on merge to main.

In the following we walk through the different ways of working with the semantic models and reports.

### Edit or view the code for semantic model or reports in Visual Studio Code
1. Open a "Git Bash" terminal in visual studio code (or do it your preferred way)
2. Make sure to be connected to sous-chef
3. Run `git pull` in the command line to pull down changes and let your local git understand that a new branch has been added
4. Run `git checkout [branch-name]` in the command line to checkout your new branch
5. Look around in the files in `projects/powerbi/Main Data Model.SemanticModel/definition`
6. Do modifications if you like
7. Run `git status` to view your changes
8. Run `git add --all` to stage your changes
9. Run `git commit -m "A commit message"` to commit your changes with a commit message explaining the change
10. Run `git push` to push your changes to the remote github

### Edit or view your models and reports in Power BI Desktop
1. Check out and pull any changes from git like described above
2. Open the `.pbip` file that exist in `projects/powerbi/workspace-content` in Power BI Desktop
3. If you did changes in visual studio you should see these changes in Power BI Desktop
4. Do changes in Power BI Desktop and make sure to save.
5. Open Visual Studio Code again and run `git status`
6. Notice that your changes are reflected in the `.SemanticModel` files or `.Report` files.
7. Add, commit and push your changes to the remote GitHub
Note: You should not use the _publish_ functionality in Power BI Desktop. In order to get the changes in your workspace you should pull the changes from git. See "Work with Git in the Power BI Workspace" section below.

### Work with reports and git in the Power BI Workspace
1. Notice that the Git integration in your Power BI workspace has picked up that there are changes in the remote branch (given that you did changes in the above steps).
2. Update your workspace with the incomming changes
3. View the changes in the reports in your workspace.
4. Do a change in the reports in your workspace
5. Add a commit message in your workspace
6. Push the changes to git remote.
Note: The Main Data Model report is only used for debugging in feature branch and will not be deployed to dev, test and prod workspaces.

### Control and change where the data is connected
You can select "Lineage view" in the upper right corner when you are looking at your feature workspace. This will show you how the data is flowing. The lineage view will show what host in Databricks the data is coming from, but not necessarily the schema. To do this you should do the following:
1. Click on the databricks connection and then on "Go to settings"
2. Open Parameters and see the current parameters that are used.
3. Change the schema if you would like. When working with features you might want to connect to your personal dbt schema in databricks (e.g. `anna_broyn_gold`).
4. If you changed the schema you might need to do a refresh the data.
Note: If you change the parameters in your feature branch, this will be overwritten when the code is deployed to dev, test and prod (see the section "Create a PR and merge to main" below)

## Create PR and merge to main
When you are finished with your development work you should create a PR and merge to main
1. Open sous-chef in GitHub in your browser
2. Navigate to your branch and view the file changes and commits
3. If you are happy, create a Pull Request
4. View the checks that is running:
    - A "Build and test semantic model" job is running. View the step called Run Dataset Rules and see any warnings on the semantic model.
    - Make sure all tests are green
5. View the content in the workspace in Power BI called "Cheffelo [dev]". The data will now come from the `gold` schema in the `dev` catalog in databricks.
6. Add a reviewer. It is often nice to have a couple of extra eyes looking at your PR before you complete it.
7. If you are happy with the output of all of the above, you may complete the PR. This will merge the code into main and start a deployment pipeline for deploying the code to test and prod.
8. The content will automatically be deployed to the workspace called "Cheffelo [test]", you may follow the deployment in Actions in GitHub.
9. View the result in "Cheffelo [test]". The data will now come from the `gold` schema in the `test` catalog in databricks. This will be "prod-like" data and thus a last test before we deploy everything to prod.
10. If you are happy you may go into Actions in Github again and complete the deployment to production. The content is deployed to the workspace called "Cheffelo" with data connected to the `gold` schema in the `prod` catalog in Databricks
