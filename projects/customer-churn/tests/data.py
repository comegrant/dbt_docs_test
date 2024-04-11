import logging
import random
from datetime import UTC, datetime, timedelta

import pandas as pd
from customer_churn.data.models.bisnode import Bisnode
from customer_churn.data.models.complaints import Complaints
from customer_churn.data.models.crm import CRMSegment
from customer_churn.data.models.customer import Customer
from customer_churn.data.models.events import EventRecord
from customer_churn.data.models.orders import OrderRecord

logger = logging.getLogger(__name__)


def generate_random_date(start_date: datetime, end_date: datetime) -> datetime:
    time_between_dates = end_date - start_date
    days_between_dates = time_between_dates.days
    random_number_of_days = random.randrange(days_between_dates)
    return start_date + timedelta(days=random_number_of_days)


def generate_random_timestamp(start_year: int, end_year: int) -> datetime:
    start_date = datetime(start_year, 1, 1, tzinfo=UTC)
    end_date = datetime(end_year, 12, 31, 23, 59, 59, tzinfo=UTC)
    delta = end_date - start_date
    int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
    random_second = random.randrange(int_delta)
    return start_date + timedelta(seconds=random_second)


def generate_bisnode_data(rows: int = 10) -> pd.DataFrame:
    records = []
    for _ in range(rows):
        record = Bisnode(
            agreement_id=112211,
            gedi="A2GF2XH3G42R",
            impulsiveness=random.uniform(0, 50),
            installment_affinity=random.uniform(0, 30),
            profile_online_media=random.choice([4.0, 5.0, 6.0]),
            cultural_class=random.choice(["02. Medium", "03. Høy"]),
            perceived_purchasing_power=random.choice(["02. Medium", "03. Høy"]),
            consumption=random.uniform(90, 100),
            profile_newspaper=random.choice([5.0, 6.0]),
            financial_class=random.choice(["02. Medium", "03. Høy"]),
            probability_children_0_to_6=random.uniform(0, 30),
            probability_children_7_to_17=random.uniform(0, 30),
            education_years=random.uniform(12, 20),
            purchase_power=random.uniform(20000, 30000),
            level_of_education="04. Universitet, høyskole - lavere nivå",
            life_stage="05. Etablerte barnefamilier",
            type_of_housing="01. Enebolig",
            confidence_level=random.choice(["green", "yellow", "red"]),
            number_of_hits=random.randint(0, 5),
            created_at=generate_random_date(
                datetime(2020, 1, 1, tzinfo=UTC),
                datetime(2021, 12, 31, tzinfo=UTC),
            ),
            updated_at=generate_random_date(
                datetime(2020, 1, 1, tzinfo=UTC),
                datetime(2021, 12, 31, tzinfo=UTC),
            ),
            persons_in_household=random.choice([1, 2, 3, 4]),
            household_id=random.uniform(10000000, 99999999),
        )
        records.append(record.model_dump())

    df = pd.DataFrame(records)
    return df


def generate_complaints_data(rows: int = 10) -> pd.DataFrame:
    case_line_types = ["Type1", "Type2", "Type3"]
    categories = ["Category1", "Category2", "Category3"]
    responsibles = ["Responsible1", "Responsible2", "Responsible3"]
    causes = ["Cause1", "Cause2", "Cause3"]
    comments = ["Comment1", "Comment2", "Comment3", None]
    records = []

    for _ in range(rows):
        year = random.choice([2020, 2021])
        registration_date = generate_random_date(
            start_date=datetime(year, 1, 1, tzinfo=UTC),
            end_date=datetime(year, 12, 31, tzinfo=UTC),
        )
        record = Complaints(
            agreement_id=random.randint(100000, 999999),
            order_id=random.randint(1000, 9999),
            company_id=random.randint(100, 999),
            delivery_year=year,
            delivery_week=random.randint(1, 52),
            case_line_type=random.choice(case_line_types),
            case_line_amount=round(random.uniform(100, 10000), 2),
            category=random.choice(categories),
            responsible=random.choice(responsibles),
            cause=random.choice(causes),
            comment=random.choice(comments),
            registration_date=registration_date,
        )
        records.append(record.model_dump())

    df = pd.DataFrame(records)
    return df


def generate_crm_data(rows: int = 10) -> pd.DataFrame:
    main_segment_names = ["Buyer"]
    sub_segment_names = [
        "Super loyal",
        "Sporadic",
        "Onboarding",
        "Loyal",
        "Pending onboarding",
    ]
    records = []

    for _ in range(rows):
        record = CRMSegment(
            agreement_id=112211,
            current_delivery_year=random.choice([2020, 2021]),
            current_delivery_week=random.randint(1, 52),
            main_segment_name=random.choice(main_segment_names),
            sub_segment_name=random.choice(sub_segment_names),
            planned_delivery=random.choice([True, False]),
            number_of_orders=random.randint(0, 32),  # Adjust the range as needed
            bargain_hunter=random.choice([True, False]),
            revenue=round(
                random.uniform(0, 30000),
                2,
            ),  # Adjust the range and precision as needed
        )
        records.append(record.model_dump())

    df = pd.DataFrame(records)
    return df


def generate_customer_data(rows: int = 10) -> pd.DataFrame:
    records = []
    for _ in range(rows):
        agreement_creation_date = generate_random_date(
            datetime(2020, 1, 1, tzinfo=UTC),
            datetime(2020, 12, 31, tzinfo=UTC),
        )
        agreement_start_date = agreement_creation_date + timedelta(
            days=random.randint(1, 30),
        )
        agreement_first_delivery_date = agreement_start_date
        last_delivery_date = generate_random_date(
            datetime(2021, 1, 1, tzinfo=UTC),
            datetime(2021, 12, 31, tzinfo=UTC),
        )
        next_estimated_delivery_date = last_delivery_date + timedelta(
            days=random.randint(7, 30),
        )

        record = Customer(
            agreement_id=112211,
            agreement_status=random.choice(["active", "inactive"]),
            agreement_creation_date=agreement_creation_date,
            agreement_start_date=agreement_start_date,
            agreement_first_delivery_date=agreement_first_delivery_date,
            source="Sales",
            source_sales_company="Decision",
            source_sales_department="Oslo",
            source_sales_person_id=random.randint(10000, 99999),
            source_sales_type=random.choice(["CC", "DD"]),
            sign_up_code="D-gl-55x1-roedepaske",
            onboarding_week_number=None,  # Assuming optional
            number_of_deliveries=random.uniform(0, 100),
            number_of_deliveries_group=random.choice(["0-20", "20-40", "40-60"]),
            delivery_address_city="RANDABERG",
            delivery_address_municipality="RANDABERG",
            delivery_address_county="ROGALAND",
            weeks_since_last_delivery=random.uniform(-1, 52),
            weeks_since_last_delivery_group=random.choice(["0-4", "5-8", "9-12"]),
            last_delivery_date=last_delivery_date,
            last_delivery_year=float(last_delivery_date.year),
            last_delivery_week=float(last_delivery_date.isocalendar()[1]),
            next_estimated_delivery_date=next_estimated_delivery_date,
            payment_method=random.choice(["cc", "invoice"]),
            timeblock_day=random.choice(
                ["monday", "tuesday", "wednesday", "thursday", "friday"],
            ),
            timeblock_interval=random.choice(["16:00 - 22:00", "08:00 - 12:00"]),
            registration_process=random.choice(["recurring_normal", "one_time"]),
            email_reservation=random.choice([True, False]),
            phone_reservation=random.choice([True, False]),
            door_reservation=random.choice([True, False]),
            sms_reservation=random.choice([True, False]),
            agreement_regret_weeks=random.uniform(0, 52),
            agreement_start_year=2020,
            agreement_start_week=random.randint(1, 52),
            agreement_first_delivery_year=int(agreement_first_delivery_date.year),
            agreement_first_delivery_week=int(
                agreement_first_delivery_date.isocalendar()[1],
            ),
            sign_up_payment_method="cc",
            subscribed_delivery_week_interval=random.choice(["0-4", "5-8", "9-12"]),
        )
        records.append(record.model_dump())

    df = pd.DataFrame(records)
    return df


def generate_events_data(rows: int = 10) -> pd.DataFrame:
    event_texts = [
        "ceDeviationOrdered",
        "changePayment",
        "changeStatus_Activated",
        "changeStatus_Freezed",
        "changeWeekEditorFilter",
        "Coupon Applied",
        "Experiment Viewed",
        "jsError",
        "Login success",
        "postDeviation",
    ]
    records = []

    for _ in range(rows):
        record = EventRecord(
            agreement_id=112211,  # Assuming a constant agreement_id for simplicity
            event_text=random.choice(event_texts),
            timestamp=generate_random_timestamp(2020, 2021),
        )
        records.append(record.model_dump())

    df = pd.DataFrame(records)
    return df


def generate_orders_data(rows: int = 10) -> pd.DataFrame:
    company_name = "Godtlevert"
    records = []

    for _ in range(rows):
        year = random.choice([2020, 2021])
        delivery_date = generate_random_date(
            start_date=datetime(year, 1, 1, tzinfo=UTC),
            end_date=datetime(year, 12, 31, tzinfo=UTC),
        )
        record = OrderRecord(
            agreement_id=112211,
            company_name=company_name,
            order_id=random.uniform(1000000, 9999999),
            delivery_date=delivery_date,
            delivery_year=year,
            delivery_week=delivery_date.isocalendar()[1],
            net_revenue_ex_vat=round(random.uniform(300, 900), 4),
            gross_revenue_ex_vat=round(random.uniform(300, 900), 4),
        )
        records.append(record.model_dump())

    df = pd.DataFrame(records)
    return df
