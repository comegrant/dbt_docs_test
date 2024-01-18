from datetime import datetime, timedelta
import numpy as np


class Queries:
    """Holds all the queries used in this model. Used exclusively on Databricks"""

    def __init__(self, company_id, year, schema, model_training=False):
        self.company_id = company_id
        self.year = year
        self.schema = schema
        self.model_training = model_training

    def get_tables(self):
        return [
            "analytics.crm_segment_agreement_main_log",
            "analytics.crm_segment_agreement_main_latest",
            "bisnode.bisnode_enrichments",
            "bisnode.bisnode_enrichments_history",
            "cms.billing_agreement",
            f"{self.schema}.tracks_churn_view",
            "mb.complaints",
            "mb.customers",
            "mb.orders",
        ]

    def get_customers(self, customer_id=None):
        """
        One row for each customer_id with respective details.
        """
        base_query = """
            SELECT 
                agreement_id
            ,	agreement_status
            ,	agreement_start_date
            ,	agreement_start_year
            ,	agreement_start_week
            ,	agreement_first_delivery_year
            ,	agreement_first_delivery_week
            ,   agreement_creation_date
            ,   agreement_first_delivery_date
            ,   last_delivery_date
            ,   next_estimated_delivery_date

            FROM mb.customers
            WHERE company_id = '{company}'
            AND agreement_status_id != 40
        """.format(
            company=self.company_id
        )

        if customer_id is not None:
            return (
                base_query
                + """
                AND agreement_id = {customer_id}
            """.format(
                    customer_id=customer_id
                )
            )
        else:
            return base_query

    def get_bisnode_enrichments(self, customer_id=None):
        """
        Bisnode Enrichments details.
        """
        base_query1 = """
            SELECT
            be.agreement_id, 
            be.impulsiveness, 
            be.created_at, 
            be.cultural_class,
            be.perceived_purchasing_power, 
            be.consumption, 
            be.financial_class, 
            be.life_stage, 
            be.type_of_housing, 
            be.confidence_level,
            be.probability_children_0_to_6,
            be.probability_children_7_to_17
            FROM bisnode.bisnode_enrichments be
            INNER JOIN cms.billing_agreement ba ON ba.agreement_id = be.agreement_id AND ba.company_id = '{company}'
        """.format(
            company=self.company_id
        )
        base_query2 = """
            UNION ALL

            SELECT
            beh.agreement_id, 
            beh.impulsiveness, 
            beh.created_at, 
            beh.cultural_class,
            beh.perceived_purchasing_power, 
            beh.consumption, 
            beh.financial_class, 
            beh.life_stage, 
            beh.type_of_housing, 
            beh.confidence_level,
            beh.probability_children_0_to_6,
            beh.probability_children_7_to_17
            FROM bisnode.bisnode_enrichments_history beh
            INNER JOIN cms.billing_agreement ba ON ba.agreement_id = beh.agreement_id AND ba.company_id = '{company}'
        """.format(
            company=self.company_id
        )

        if customer_id is not None:
            return (
                base_query1
                + """
                AND ba.agreement_id = {customer_id}
            """.format(
                    customer_id=customer_id
                )
                + base_query2
                + """
                AND ba.agreement_id = {customer_id}
            """.format(
                    customer_id=customer_id
                )
            )
        else:
            return base_query1 + base_query2

    def get_complaints(self, customer_id=None):
        """
        Complaints details.
        """
        base_query = """
            SELECT 
                agreement_id
            ,	delivery_year
            ,	delivery_week
            ,	category
            ,	registration_date
            FROM mb.complaints
            WHERE delivery_year >= {year}
            AND company_id = '{company}'
        """.format(
            company=self.company_id, year=self.year
        )

        if customer_id is not None:
            return (
                base_query
                + """
                AND agreement_id = {customer_id}
            """.format(
                    customer_id=customer_id
                )
            )
        else:
            return base_query

    def get_crm_segments(self, customer_id=None, model_training=False):
        """
        CRM Segments details.
        """
        if model_training == True:
            base_query = """
             SELECT 
                agreement_id
            ,	current_delivery_year
            ,	current_delivery_week
            ,	planned_delivery
            FROM analytics.crm_segment_agreement_main_log
            WHERE current_delivery_year >= {year}
            AND company_id = '{company}'
            AND sub_segment_name != 'Deleted'   -- temporary as there are no events for deleted users
             """.format(
                company=self.company_id, year=self.year
            )

            if customer_id is not None:
                return (
                    base_query
                    + """
                AND agreement_id = {customer_id}
                """.format(
                        customer_id=customer_id
                    )
                )
            else:
                return base_query

        elif model_training == False:
            base_query = """
             SELECT 
                agreement_id
            ,	current_delivery_year
            ,	current_delivery_week
            ,	planned_delivery
            FROM analytics.crm_segment_agreement_main_latest
            WHERE current_delivery_year >= {year}
            AND company_id = '{company}'
            AND main_segment_name == 'Buyer'
             """.format(
                company=self.company_id, year=self.year
            )

            if customer_id is not None:
                return (
                    base_query
                    + """
                AND agreement_id = {customer_id}
                """.format(
                        customer_id=customer_id
                    )
                )
            else:
                return base_query

    def get_events(self):
        """
        Events details.
        """
        return f"""
            SELECT 
                agreement_id
                ,   event_text
                ,   timestamp
            FROM {self.schema}.tracks_churn_view                
            """

    def get_orders(self, customer_id=None):
        """
        Orders details.
        """
        base_query = """
            SELECT
                agreement_id
            ,	company_name
            ,	order_id
            ,	delivery_date
            ,	delivery_year
            ,	delivery_week
            ,	net_revenue_ex_vat
            ,	gross_revenue_ex_vat
            FROM mb.orders
            WHERE company_id = '{company}'
            AND delivery_year >= {year}
        """.format(
            company=self.company_id, year=self.year
        )

        if customer_id is not None:
            return (
                base_query
                + """
                AND agreement_id = {customer_id}
            """.format(
                    customer_id=customer_id
                )
            )
        else:
            return base_query

    def insert_into_query(self, data, table, colnames):
        """Generates an insert into query for your needs"""

        # Takes in a pandas DataFrame
        # later: has a batch split option (if the table is big (over 1000 rows))
        # makes it into a large batch insert into statement
        # commits to the db. (can do this on code)

        if data.empty:
            print("WARNING: No rows in the data")
            return None
        else:
            queries = []

            if data.shape[0] > 1000:
                # Split into chunks (because sql server cant handle bigger batches)
                number_of_chunks = np.ceil(data.shape[0] / 1000)
                data_splits = np.array_split(data, number_of_chunks)
            else:
                data_splits = [data]

            for partition in data_splits:
                str_data = partition.to_csv(
                    header=False,
                    index=False,
                    quotechar="'",
                    quoting=2,
                    line_terminator="),\n(",
                )
                str_data_good = "(" + str_data[:-3] + ";"

                query = """
                INSERT INTO {0} ({1}) VALUES
                {2}
                """.format(
                    table, ", ".join(colnames), str_data_good
                )

                queries.append(query)

            return queries
