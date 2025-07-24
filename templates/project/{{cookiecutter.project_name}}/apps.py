from chef.deploy import Apps, StreamlitApp

apps = Apps(
    debug=StreamlitApp(main_app="streamlit_apps/main.py"),
)
