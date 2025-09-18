from chef.deploy import Apps, StreamlitApp

apps = Apps(
    docker_image="bhregistry.azurecr.io/menu-optimiser:dev-latest",
    app=StreamlitApp(
        main_app="streamlit_app/Menu_Optimiser.py",
        env_vars={
            "auth_redirect_uri": "https://menu-optimiser-app-test.grayrock-1e8d6fab.northeurope.azurecontainerapps.io",
        },
    ),
)
