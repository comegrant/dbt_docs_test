import pandas as pd
import plotly.express as px
import plotly.graph_objects as go


def plot_histogram(
    df_ab_test: pd.DataFrame,
    group_colname: str,
    response_colname: str,
) -> go.Figure:
    # Create the histogram
    group_names = df_ab_test[group_colname].unique()

    color_map = {
        group_names[0]: 'blue',
        group_names[1]: 'red',
    }
    # Binary groups, plot differently
    fig = px.histogram(
        df_ab_test,
        x=response_colname,
        color=group_colname,
        barmode="group",
        marginal="box",
        color_discrete_map=color_map
    )

    # Update layout for better visualization
    fig = fig.update_layout(
        title=f"Histogram of '{response_colname}' by group",
        xaxis_title=response_colname,
        yaxis_title="count",
        bargap=0.2,
        template='plotly_white',
    )

    return fig
