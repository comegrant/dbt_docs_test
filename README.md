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


### File Structure
```
├── doc                    # Project-level documentation folder
├── packages               # Packages root folder
├── scripts                # Project-level scripts folder
├── services               # Services root folder
├── templates              # Template folder for new packages and services
│   ├── package            # Package template folder
│   │   ├── doc            # Package documentation folder
│   │   ├── scripts        # Package scripts folder
│   │   ├── src            # Package source code folder
│   │   ├── tests          # Package test folder
│   │   ├── Makefile       # Package Makefile for running common commands
│   │   ├── README.md      # Package README file
│   │   ├── poetry.lock    # Poetry lock file for package dependencies
│   │   └── pyproject.toml # Poetry configuration file for the package
│   └── service            # Service template folder
│       ├── doc            # Service documentation folder
│       ├── scripts        # Service scripts folder
│       ├── src            # Service source code folder
│       ├── tests          # Service test folder
│       ├── Dockerfile     # Dockerfile for building the service
│       ├── Makefile       # Service Makefile for running common commands
│       ├── README.md      # Service README file
│       ├── poetry.lock    # Poetry lock file for service dependencies
│       └── pyproject.toml # Poetry configuration file for the service
├── LICENSE                # License file
├── Makefile               # Project-level Makefile for running common commands for the entire repo
├── README.md              # README file for the entire repo
└── pyproject.toml         # Poetry configuration file for the entire repo
```

### Creating New Project and Packages
To create a new project, run the new-service target from the command line and provide a name for your service:

```shell
chef new project
```

To create a new package:
```shell
chef new package
```

#### What is the difference between \<library name\>, \<package name\> and \<module name\>?

**Library** name is a human readable name. *E.g: Analytics API*

**Package** name is a name without spaces and upper letters for workflows and folders. *E.g: analytics-api*

**Module** name is the python package name which needs underscores. *E.g: analytics_api*
