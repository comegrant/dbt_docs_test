# Introduction 
Repository hosting customer churn prediction ML.

High level flow diagram below describes main modules involved in data prep, model training and model predictions.

![High Level Churn-AI module flow](churn-ai.jpeg)



# Getting Started
1. Create repo for new project in Azure Devops and clone it
2. Download this repository without version control (Download as zip)
3. Unzip in the newly cloned repo and make intial commit. 


# Development
To initialize development environment, create a virtual environment
```bash
# Initialize project
python -m venv venv
source venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt

# Run project
python -m src.main

# Run tests
python -m pytest
```

# Data Preparation
Data preprocessing for model training and predictions are handled in Gen module (/src/gen.py). 
All inputs needed for the module should be configured through config.py 

```
PREP_CONFIG = {
    "snapshot": {
        'start_date': '2021-06-01',  # Data snapshot start date
        'end_date': '2022-01-01',  # Data snapshot end date
        'forecast_weeks': 4,  # Prediction n-weeks ahead
        'buy_history_churn_weeks': 4  # Number of customer weeks to include in data model history
    },
    "input_files": {
        "customers": "../data/raw/customers.csv",
        "events": "../data/raw/events.csv",
        "orders": "../data/raw/orders.csv",
        "crm_segments": "../data/raw/crm_segments_cleared.csv",
        "complaints": "../data/raw/complaints.csv",
        "bisnode": "../data/raw/bisnode_enrichments.csv"
    },
    "output_dir": "../data/processed/"  # Output directory
}
```

Example of using Gen() module for generating and preparing data for ML training:

```
categ_columns=['snapshot_status', 'planned_delivery']
columns_to_keep = ['number_of_total_orders', 'weeks_since_last_delivery', 'customer_since_weeks', 'impulsiveness', 
                   'consumption', 'total_complaints', 'forecast_status']  + categ_columns

validation_split_date = '2021-09-01'

# Train / Test set
df_x, df_y = Prep().prep_training(df[df.snapshot_date < validation_split_date].copy(),
                                  columns_to_keep, 
                                  categ_columns=categ_columns,
                                  drop_nan=True, 
                                  label_column='forecast_status')

# Validation set
df_x_val, df_y_val = Prep().prep_training(df[df.snapshot_date >= validation_split_date].copy(),
                                          columns_to_keep,
                                          categ_columns=categ_columns,
                                          drop_nan=True,
                                          label_column='forecast_status')
```

# Model Training & Predictions

Example notebook with minimal code for model training and prediction: /notebooks/model_test.ipynb


## Model Training
```
from model_logreg import ModelLogReg

model = ModelLogReg(data=df, config=PREP_CONFIG)
df_ml = model.prep_training()
model.fit(save_model_filename='log_reg.pkl')
```

Example from output from model training:
```
Starting to fit model..
Training features columns:Index(['number_of_total_orders', 'weeks_since_last_delivery',
       'customer_since_weeks', 'weeks_since_last_complaint',
       'snapshot_status_active', 'snapshot_status_freezed',
       'planned_delivery_False', 'planned_delivery_True'],
      dtype='object')
precision: 0.761074283832912
recall: 0.7553308100386896
f1-score: 0.7572013576701323
Saving model to: log_reg.pkl
Model saved!
```

## Model predictions
Currently implemented ML models for predictions are:
 - Linear Regression (src/model_logreg.py)

```
from model_logreg import ModelLogReg

model = ModelLogReg(config=PREP_CONFIG)
pred = model.predict(df, model_filename='log_reg.pkl')
```

# Build and Test
Tests should be added to the `src/tests` folder. We test with `pytest`, and all test scripts are to be named `test_*.py`.


# Contribute
The contents of this structure is the result of trial and error. We know that it is not perfect. Let us know if you have any suggestions.


## Documentation

# What should the model predict
The baseline model will predict a 0 for low probability and 1 for high probability for that the customer will churn in 4-7 weeks.
The final model will predict a probability.
The definition for a churned customer is 4 weeks without delivery and no future delivery planned.
The data does not include customers that are in the onboarding phase (first 12 weeks).

# Usage
The model runs once per week.
x number of the hits will be chosen to be given anti-churn actions.

# Evaluation of model
Business value is connected to the effectiveness of rescue actions. 
However, this requires A/B testing and can only be measured once the model is deployed. 
Therefore, the primary performance metric is the ability to discover churn. 
Recall, precision and f-score will be used as metrics for the model's ability to discover churn.

The data for the baseline model will be made with a sliding window.
For each week we will look back four weeks and create the features we need for our baseline predictions.
Then we will look forward four weeks to label each customer with a churned label, that we can measure our predictions against.
The data consist of customers that are buyers, which exclude people in the onboarding phase (First 12 weeks) and deleted customers.

# Baseline rules
Number of weeks since last delivery
Customer will churn if the has been x > weeks_since_last_delivery

Change of Agreement status
If the customer changes from active to freeze we will predict a churn

Number of weeks as a customer and number of deliveries
Customer will be marked loyal if the have been customer for over a year with a delivery z% of the weeks.
Then for the loyal customers, Rule1 and Rule2 of the baseline rules change to
Rule 1  
x+1 weeks without delivery is churn  
Rule 2  
x-1 weeks without delivery and freeze is churn
