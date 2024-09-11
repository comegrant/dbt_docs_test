This README is work in progress

# About the Power BI setup
In this folder we keep the code for our official semantic models and reports. 

# Folder content
- *.Report
- *.SemanticModel
- *.pbip

## *.Report
This contain report definisions ..
More to come here

## *.SemanticModel
For the semantic model we use the TMDL format which is in Preview in Power BI. 
See Microsoft Documentation: 
- https://powerbi.microsoft.com/en-us/blog/tmdl-in-power-bi-desktop-developer-mode-preview/
- https://learn.microsoft.com/en-us/power-bi/developer/projects/projects-dataset#tmdl-format

The interessting code of the semantic model will be found in the sub-folder `definition`. 
All table definitions included columns and measures can be found in the `tables` folder below `definition`.

## *.pbip
The *.pbip file can be opened in Power BI Desktop. Modifications done in Power BI that are saved as a `.pbip` file will update the corresponding `.SemanticModel` and `.Report` files. 
This means that changes can be done both in Power BI Desktop by saving to `.pbip` or in a code editor like Visual Studio Code by changing the `.SemanticModel` files or `.Report` files directly. 

# Get started

## Prerequisites
- Visual Studio Code is installed on your computer
- Git is installed on your computer
- The `sous-chef` repository is cloned to you computer

## Install required tools
1.  Install the extension `TMDL` Visual Studio Code
2. (Optional) Install Power BI Desktop. 
    - In Windows you find this in _Microsoft Store_. 
    - For Mac this needs a VM. We need to figure out how to do this. 

## Connect a Power BI workspace to a feature branch

### Create a branch in `sous-chef` 
This should be based on `main` and be placed in a folder with the developers name. E.g. `anna/testing_git_for_powerbi`

### Create a workspace in Power BI 
This will be used as a personal area to checkout featre branches. It should not be the "My Workspace" but a new workspace. Make sure to have the following settings: 
- _Workspace Name_: "[Users first name] Features", e.g. `Anna Features`
- _License Configuration_: Premium per-user
- _Semantic model storage format_: Large semantic model storage format

### Connect the workspace to GitHub
1. Below "Workspace Settings", navigate to "Git integration". 
2. Select the existing account called "sous-chef"
3. Select the branch you created above
4. Select Git folder `/projects/data-model/powerbi`
5. You might need to refresh the semantic model to see data

Going forward, when working with new feature branches you need to update the branch here. 

## Edit or view the code in Visual Studio Code
1. Open a "Git Bash" terminal in visual studio code (or do it your preferred way)
2. Make sure to be connected to sous-chef
3. Run `git pull` in the command line to pull down changes and let your local git understand that a new branch has been added
4. Run `git checkout [branch-name]` in the command line to checkout your new branch
5. Look around in the files in `projects/data-model/powerbi/Main Data Model.SemanticModel/definition`
6. Do modifications if you like
7. Run `git add --all` to stage your changes
8. Run `git commit -m "A commit message"` to commit your changes with a commit message explaining the change
9. Run `git push` to push your changes to the remote github 

## Edit or view your models and reports in Power BI Desktop
1. Open the `.pbip` file that exist in `projects/data-model/powerbi` in Power BI Desktop
2. If you did changes in visual studio you should see these changes in Power BI Desktop
3. Do changes in Power BI Desktop
4. Open Visual Studio Code again and run `git status`
5. Notice that your changes are reflected in the `.SemanticModel` files or `.Report` files. 
6. Add, commit and push your changes to the remote GitHub

Note: You should not use the publish branch for this, it is better to get the changes to your workspace through git. 

## Work with Git in the Power BI Workspace
1. Notice that the Git integration in your Power BI workspace has picked up that there are changes in the remote branch (given that you did changes in the above steps). 
2. Update your workspace with the incomming changes
3. View the changes in the reports in your workspace. 
4. Do a change in the reports in your workspace
5. Add a commit message in your workspace 
6. Push the changes to git remote. 

## Create PR and merge to main

1. Open sous-chef in GitHub in your browser
2. Navigate to your branch and view the file changes and commits
3. If you are happy, create a Pull Request

-- WIP
When a Pull Request is created the following should happen: 
- A test is run to see that we follow best practices etc. 
- The content is deployed to the workspace called "Cheffelo [dev]" with data connected to the `gold` schema in the `dev` catalog in Databricks
- The result can then be viewed in "Cheffelo [dev]"
- Another person should review the PR as well
- If everything is good the PR should be completed

When a Pull Request is completed the following should happen: 
- The content is deployed to the workspace called "Cheffelo [test]" with data connected to the `gold` schema in the `test` catalog in Databricks
- If evertthing is good we can complete the workflow and deploy to production: content is deployed to the workspace called "Cheffelo" with data connected to the `gold` schema in the `prod` catalog in Databricks


