# Author/credits: https://github.com/DTrimarchi10/confusion_matrix
"""
Usage (binary classification:

from sklearn.metrics import confusion_matrix

cf_matrix = confusion_matrix(y_test, y_pred)
labels = ['True Neg', 'False Pos', 'False Neg', 'True Pos']
categories = ['active', 'churned']
make_confusion_matrix(cf_matrix,
                      group_names=labels,
                      categories=categories,
                      cmap='binary',
                      figsize=(7, 5))

count:         If True, show the raw number in the confusion matrix. Default is True.

normalize:     If True, show the proportions for each category. Default is True.

cbar:          If True, show the color bar. The cbar values are based off the values in the confusion matrix.
                Default is True.

xyticks:       If True, show x and y ticks. Default is True.

xyplotlabels:  If True, show 'True Label' and 'Predicted Label' on the figure. Default is True.

sum_stats:     If True, display summary statistics below the figure. Default is True.
"""


import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns

COUNT = True
PERCENT = True
CBAR = True
XYTICKS = True
XYPLOTLABELS = True
SUM_STATS = True

BINARY_CONFUSION_MATRIX_LEN = 2


def make_confusion_matrix(
    cf: np.ndarray,
    group_names: list[str] | None = None,
    categories: str = "auto",
    figsize: tuple[int, int] | None = None,
    cmap: str = "Blues",
    title: str | None = None,
) -> None:
    """
    This function will make a pretty plot of an sklearn Confusion Matrix cm using a Seaborn heatmap visualization.

    Arguments
    ---------
    cf:            confusion matrix to be passed in

    group_names:   List of strings that represent the labels row by row to be shown in each square.

    categories:    List of strings containing the categories to be displayed on the x,y axis. Default is 'auto'

    figsize:       Tuple representing the figure size. Default will be the matplotlib rcParams value.

    cmap:          Colormap of the values displayed from matplotlib.pyplot.cm. Default is 'Blues'
                   See http://matplotlib.org/examples/color/colormaps_reference.html

    title:         Title for the heatmap. Default is None.

    """

    # CODE TO GENERATE TEXT INSIDE EACH SQUARE
    blanks = ["" for i in range(cf.size)]

    group_labels = (
        [f"{value}\n" for value in group_names]
        if group_names and len(group_names) == cf.size
        else blanks
    )
    group_counts = [f"{value:0.0f}\n" for value in cf.flatten()] if COUNT else blanks
    group_percentages = (
        [f"{value:.2%}" for value in cf.flatten() / np.sum(cf)] if PERCENT else blanks
    )

    box_labels = [
        f"{v1}{v2}{v3}".strip()
        for v1, v2, v3 in zip(
            group_labels,
            group_counts,
            group_percentages,
            strict=False,
        )
    ]
    box_labels = np.asarray(box_labels).reshape(cf.shape[0], cf.shape[1])

    # CODE TO GENERATE SUMMARY STATISTICS & TEXT FOR SUMMARY STATS
    if SUM_STATS:
        # Accuracy is sum of diagonal divided by total observations
        accuracy = np.trace(cf) / float(np.sum(cf))

        # if it is a binary confusion matrix, show some more stats
        if len(cf) == BINARY_CONFUSION_MATRIX_LEN:
            # Metrics for Binary Confusion Matrices
            precision = cf[1, 1] / sum(cf[:, 1])
            recall = cf[1, 1] / sum(cf[1, :])
            f1_score = 2 * precision * recall / (precision + recall)
            stats_text = f"""
                          \n\nAccuracy={accuracy:0.3f}
                          \nPrecision={precision:0.3f}
                          \nRecall={recall:0.3f}
                          \nF1 Score={f1_score:0.3f}"""
        else:
            stats_text = f"\n\nAccuracy={accuracy:0.3f}"
    else:
        stats_text = ""

    # SET FIGURE PARAMETERS ACCORDING TO OTHER ARGUMENTS
    if figsize is None:
        # Get default figure size if not set
        figsize = plt.rcParams.get("figure.figsize")

    if XYTICKS is False:
        # Do not show categories if xyticks is False
        categories = False

    # MAKE THE HEATMAP VISUALIZATION
    plt.figure(figsize=figsize)
    sns.heatmap(
        cf,
        annot=box_labels,
        fmt="",
        cmap=cmap,
        cbar=CBAR,
        xticklabels=categories,
        yticklabels=categories,
    )

    if XYPLOTLABELS:
        plt.ylabel("True label")
        plt.xlabel("Predicted label" + stats_text)
    else:
        plt.xlabel(stats_text)

    if title:
        plt.title(title)
