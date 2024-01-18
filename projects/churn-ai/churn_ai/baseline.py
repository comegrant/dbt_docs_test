import pandas as pd
import numpy as np
from sklearn.metrics import classification_report
from sklearn.metrics import confusion_matrix
import sys
import glob
import matplotlib.pyplot as plt
import seaborn as sns
import sys

# sys.path.insert(1, '../src')
from utils.cf_matrix import make_confusion_matrix


def baseline_pred(df_d, numberOfWeeksWithoutDelivery, threshold):
    # Remove deleted customers
    df_d = df_d[df_d["snapshot_status"] != "deleted"]
    # Add pred column
    df_d["pred_churned"] = 0

    # ## Rule 1
    # If the customer has not received a delivery the last x weeks, mark as churned
    df_d["pred_churned"] = np.where(
        (df_d["weeks_since_last_delivery"] >= numberOfWeeksWithoutDelivery), 1, 0
    )

    # # Rule 2
    # If the customer changes the agreement_status to freeze
    df_d["pred_churned"] = df_d["pred_churned"] + np.where(
        df_d["snapshot_status"] == "freezed", 1, 0
    )

    # Rule 3 If the customer have been using the services for over a year with more than 20 deliveries(a delivery 40
    # percent of the weeks as a customer), then a different ruleset applies
    # Rule 1 x+1 weeks without delivery is churn
    # Rule 2 x-1 weeks without delivery and freeze is churn
    # Create mask
    a = df_d[df_d["customer_since_weeks"] > 52].index
    b = df_d[
        ((df_d["number_of_total_orders"] / df_d["customer_since_weeks"]) > threshold)
    ].index
    c = a.intersection(b)
    # Clear earlier predictions of loyal customers
    df_d.loc[c, "pred_churned"] = 0
    # Loyal rule 1
    Rule1Mask = df_d[
        df_d["weeks_since_last_delivery"] >= numberOfWeeksWithoutDelivery + 1
    ].index
    Rule1Mask = Rule1Mask.intersection(c)
    df_d.loc[Rule1Mask, "pred_churned"] += 1
    # Loyal rule 2
    Rule2Mask = df_d[
        df_d["weeks_since_last_delivery"] >= numberOfWeeksWithoutDelivery - 1
    ].index
    Rule2Mask2 = df_d[df_d["snapshot_status"] == "freezed"].index
    Rule2Mask = Rule2Mask.intersection(Rule2Mask2)
    Rule2Mask = Rule2Mask.intersection(c)
    df_d.loc[Rule2Mask, "pred_churned"] += 1

    # Get y_true and y_pred
    y_true = list(np.where(df_d["forecast_status"] == "churned", 1, 0).copy())
    y_pred = list(df_d["pred_churned"].copy())

    # Replace all non zero values in pred_churn column with ones
    for index, x in enumerate(y_pred):
        if x != 0:
            y_pred[index] = 1

    cf_matrix = confusion_matrix(y_true, y_pred)
    dict_report = classification_report(
        y_true, y_pred, target_names=["Not Churned", "Churned"], output_dict=True
    )

    return dict_report, cf_matrix


if __name__ == "__main__":

    # Setup
    numberOfWeeksWithoutDelivery = 2
    threshold = 0.40

    # Read all files in snapshot folder
    files = glob.glob("../data/processed/snapshots/*.csv")
    print(files)
    df_list = []
    for f in files:
        csv = pd.read_csv(f)
        df_list.append(csv)

    # Get reports on all snapshots
    report_list = []
    cf_list = []
    for df in df_list:
        report, cf = baseline_pred(df, numberOfWeeksWithoutDelivery, threshold)
        report_list.append(report)
        cf_list.append(cf)

    # Sort values y values
    precision_churned = []
    recall_churned = []
    f1_score_churned = []
    precision_not_churned = []
    recall_not_churned = []
    f1_score_not_churned = []
    accuracy = []

    for report_dict in report_list:
        precision_churned.append(report_dict["Churned"]["precision"])
        recall_churned.append(report_dict["Churned"]["recall"])
        f1_score_churned.append(report_dict["Churned"]["f1-score"])
        precision_not_churned.append(report_dict["Not Churned"]["precision"])
        recall_not_churned.append(report_dict["Not Churned"]["recall"])
        f1_score_not_churned.append(report_dict["Not Churned"]["f1-score"])
        accuracy.append(report_dict["accuracy"])

    # Generate x values
    x = []
    for f in files:
        tmp = f[37:]
        x.append(tmp[:-4])

    # Avg values
    print("Precision")
    print("Churned", np.average(precision_churned))
    print("Not Churned", np.average(precision_not_churned))
    print("Recall")
    print("Churned", np.average(recall_churned))
    print("Not Churned", np.average(recall_not_churned))
    print("F1-score")
    print("Churned", np.average(f1_score_churned))
    print("Not Churned", np.average(f1_score_not_churned))

    # Plot precision for churned and not churned
    plt.figure(1)
    plt.plot(x, precision_churned, label="Churned")
    plt.plot(x, precision_not_churned, label="Not Churned")
    plt.legend()
    plt.xticks(rotation=45, ha="right")
    plt.title("Precision")

    plt.figure(2)
    plt.plot(x, recall_churned, label="Churned")
    plt.plot(x, recall_not_churned, label="Not Churned")
    plt.legend()
    plt.xticks(rotation=45, ha="right")
    plt.title("Recall")

    plt.figure(3)
    plt.plot(x, f1_score_churned, label="Churned")
    plt.plot(x, f1_score_not_churned, label="Not Churned")
    plt.legend()
    plt.xticks(rotation=45, ha="right")
    plt.title("F1-score")

    plt.figure(4)
    plt.plot(x, accuracy, label="Accuracy")
    plt.legend()
    plt.xticks(rotation=45, ha="right")
    plt.title("Accuracy")

    make_confusion_matrix(cf_list[1], categories=["Not Churned", "Churned"])

    """
    for i in range(len(cf_list)):
        plt.figure(i + 5)
        ax = sns.heatmap(cf_list[i], annot=True, cmap='Blues', fmt='g')
        ax.set_title(x[i]);
        ax.set_xlabel('Predicted Values')
        ax.set_ylabel('Actual Values ')
        ax.xaxis.set_ticklabels(['False', 'True'])
        ax.yaxis.set_ticklabels(['False', 'True'])
    """
    plt.show()
