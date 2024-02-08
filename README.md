<h1 align="center">
Sous-chef 🧑‍🍳
</h1>

<p align="center">
<img src="sous-chef.png" alt="sous-chef" width="200"/>
</p>

<p align="center">
Sous chef is the Python monorepo for the data team.
</p>


---

## Getting Started
Simply clone the repository and start by installing the `chef` cli tool.

The following script assumes that you have `poetry`.

### Clone Repo
```bash
git clone https://github.com/cheffelo/sous-chef sous-chef
cd sous-chef
```

### Install Python and `chef` cli
```
bash setup_tooling.sh
poetry shell
```

This will spin up a new Python virtualenv, and activate the venv in a new shell.
It will also install the core utils (`chef`) to manage the monorepo.

### Create a project
The video bellow shows how to create a new project with `chef` and get started.

### Creating New Project and Packages
To create a new project, run the new-service target from the command line and provide a name for your service:

```shell
chef create project
```

To create a new package:
```shell
chef create package
```

For more information about the `chef` cli, view [packages/`chef`/README.md](packages/chef/README.md)

#### What is the difference between \<library name\>, \<package name\> and \<module name\>?

**Library** name is a human readable name. *E.g: Analytics API*

**Package** name is a name without spaces and upper letters for workflows and folders. *E.g: analytics-api*

**Module** name is the python package name which needs underscores. *E.g: analytics_api*
