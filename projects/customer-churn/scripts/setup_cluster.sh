#! /bin/bash   
set -m

# Change to the parent directory of the script
cd "$(dirname "$0")"/..

# Check if Poetry is installed
if ! command -v poetry &> /dev/null
then
    echo "Poetry could not be found, installing now..."
    # Install Poetry - ensure the command is up-to-date by checking the official documentation
    curl -sSL https://install.python-poetry.org | python - --version  1.8.2
else
    echo "Poetry is already installed."
fi

# Update the PATH just for this script's execution context
export PATH="$HOME/.local/bin:$PATH"

# Install the desired package using Poetry
poetry config virtualenvs.create false
poetry install

echo "Installation completed."