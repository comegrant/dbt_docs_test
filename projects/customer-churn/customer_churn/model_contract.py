from aligned import (
    EventTimestamp,
    FileSource,
    Float,
    Int32,
    String,
    Timestamp,
    feature_view,
    model_contract,
)
from data_contracts.sources import azure_dl_creds

from customer_churn.owner import contact

model_data_dir = azure_dl_creds.directory("models/customer_churn")


@feature_view(
    name="bisnode_features",
    source=FileSource.parquet_at("bisnode_features.parquet"),
    contacts=[contact.markdown()],
)
class BisnodeFeatures:
    agreement_id = String().as_entity()
    impulsiveness = Float()
    created_at = Timestamp()
    cultural_class = String()
    perceived_purchasing_power = Float()
    consumption = Float()
    children_probability = Float()
    financial_class = String()
    life_stage = String()
    type_of_housing = String()
    confidence_level = Float()


@feature_view(
    name="complaints_features",
    source=FileSource.parquet_at("complaints_features.parquet"),
    contacts=[contact.markdown()],
)
class ComplaintsFeatures:
    agreement_id = String().as_entity()
    category = String()
    weeks_since_last_complaint = Int32()
    total_complaints = Int32()
    number_of_complaints_last_n_weeks = Int32()


@feature_view(
    name="crm_segment_features",
    source=FileSource.parquet_at("crm_segment_features.parquet"),
    contacts=[contact.markdown()],
)
class CRMSegmentFeatures:
    agreement_id = String().as_entity()
    planned_delivery = Int32()


@feature_view(
    name="customer_features",
    source=FileSource.parquet_at("customer_features.parquet"),
    contacts=[contact.markdown()],
)
class CustomerFeatures:
    agreement_id = String().as_entity()
    agreement_start_year = Int32()
    agreement_start_week = Int32()
    agreement_first_delivery_year = Int32()
    agreement_first_delivery_week = Int32()
    agreement_status = String()
    agreement_start_date = Timestamp()


@feature_view(
    name="events_features",
    source=FileSource.parquet_at("events_features.parquet"),
    contacts=[contact.markdown()],
)
class EventsFeatures:
    agreement_id = String().as_entity()
    total_normal_activities = Int32()
    total_account_activities = Int32()
    number_of_normal_activities_last_n_weeks = Int32()
    number_of_account_activities_last_n_weeks = Int32()
    average_normal_activity_difference = Int32()
    average_account_activity_difference = Int32()
    snapshot_status = String()
    forecast_status = String()


@feature_view(
    name="orders_features",
    source=FileSource.parquet_at("orders_features.parquet"),
    contacts=[contact.markdown()],
)
class OrdersFeatures:
    agreement_id = String().as_entity()
    snapshot_date = EventTimestamp()
    year = Int32()
    month = Int32()
    week = Int32()
    number_of_total_orders = Int32()
    number_of_forecast_orders = Int32()
    weeks_since_last_delivery = Int32()


@model_contract(
    name="customer_churn",
    features=[
        BisnodeFeatures().transformation,
        ComplaintsFeatures().transformation,
        CRMSegmentFeatures().transformation,
        CustomerFeatures().transformation,
        EventsFeatures().transformation,
        OrdersFeatures().transformation,
    ],
    contacts=[contact.markdown()],
    description="Prediction wether a customer will churn or not",
    prediction_source=model_data_dir.directory("predictions").parquet_at(
        "churn_predictions.parquet",
    ),
    exposed_at_url="https://customer_churn:8501",
    dataset_store=model_data_dir.json_at("churn_dataset.json"),
)
class ChurnModelContract:
    some_id = Int32().as_entity()

    predicted_at = EventTimestamp()

    # churn_prediction =
    # model_type =
