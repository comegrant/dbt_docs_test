from chef.cli import is_existing_package

if is_existing_package("{{ cookiecutter.package_name }}"):
    raise ValueError("Package already exists. This can lead to odd errors. Please choose a different package name.")
