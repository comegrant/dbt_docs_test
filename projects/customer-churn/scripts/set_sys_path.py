from pip._internal.operations import freeze
import sys

packages = list(freeze.freeze())
editable_packages = [
  package for package in packages if package.startswith("# Editable")
]
local_paths = [
  "/Workspace" + package.split("/Workspace")[-1]
  for package in editable_packages
]
sys.path.extend(local_paths)