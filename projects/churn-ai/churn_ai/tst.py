from gen import Gen

PREP_CONFIG = {
    "snapshot": {
        "start_date": "2021-06-01",  # Data snapshot start date
        "end_date": "2022-01-01",  # Data snapshot end date
        "forecast_weeks": 4,  # Prediction n-weeks ahead
        "buy_history_churn_weeks": 4,  # Number of customer weeks to include in data model history
    },
    "input_files": {
        "customers": "C:\\Users\\Filipe Monteiro\\Downloads\\raw\\raw\\customers.csv",
        "events": "C:\\Users\\Filipe Monteiro\\Downloads\\raw\\raw\\events.csv",
        "orders": "C:\\Users\\Filipe Monteiro\\Downloads\\raw\\raw\\orders.csv",
        "crm_segments": "C:\\Users\\Filipe Monteiro\\Downloads\\raw\\raw\\crm_segments_cleared.csv",
        "complaints": "C:\\Users\\Filipe Monteiro\\Downloads\\raw\\raw\\complaints.csv",
        "bisnode": "C:\\Users\\Filipe Monteiro\\Downloads\\raw\\raw\\bisnode_enrichments.csv",
    },
    "output_dir": "./processed/",  # Output directory
}


gen = Gen(PREP_CONFIG)
gen.main(model_training=True)
