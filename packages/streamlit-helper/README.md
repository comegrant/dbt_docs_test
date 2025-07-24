# Streamlit Helper

A package to make it easier to setup streamlit apps

## Usage
Run the following command to install this package:

```bash
chef add streamlit-helper
```

### Setup a streamlit app
One part of this package is to help with setting up common functionalities like logging, and monitoring.

Therefore, to setup the datadog loggers and metrics monitoring, wrap your application in the `@setup_streamlit`.

```python
from streamlit_helper import setup_streamlit

@setup_streamlit(
    datadog_config=None,
    datadog_metrics_config=None
)
def main() -> None:
    st.write("Something...")
    ...
```

### Authentication
We often want to limit our apps to a few people.
Therefore, we also have some shared authentication logic.

```python
from streamlit_helper import authenticated_user, AuthenticatedUser

def main() -> None:
    # The code above is not authenticated

    user: AuthenticatedUser = autenticated_user()

    # All code below is authenticated
    st.write(f"Welcome {user.display_name}!")
```

By default will this assume that you run our application at `http://localhost:8501`. However, you may need to update the [`chef-spn-dp-streamlit`](https://portal.azure.com/#view/Microsoft_AAD_RegisteredApps/ApplicationMenuBlade/~/Authentication/appId/28e5eb90-f1d6-4a1f-8f6d-4d461801deb7/isMSAApp~/false) application to allow other urls if it fails.
