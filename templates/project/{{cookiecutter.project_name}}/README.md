# {{ cookiecutter.library_name }}

{{cookiecutter.project_description}}

## Usage

You can run the project in two different ways.

- CLI
- GUI (Streamlit)

### CLI
To run the project from the CLI, use the following command from the root dir.

```bash
python -m {{cookiecutter.module_name}}.main --name "{{cookiecutter.owner_full_name}}"
```

### GUI (Streamlit)
If you want a graphical interface, then start the streamlit app at `app.py`.

This can be done by running the following command and go to the url shown in the terminal. Most likely [127.0.0.1:8501](127.0.0.1:8501).

```bash
chef up
```
