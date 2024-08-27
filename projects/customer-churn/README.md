*## IMPORTANT NOTE: The project is not fully usable for production until the new data platform is up running. Especially events data needs to be aggregated to avoid computing the entire table each time and changes in customer needs to be saved in a historical table to escape the snapshot process.*


# customer_churn


A Machine Learning algorithm to predict wether a customer will churn or not in the coming weeks.


<h2>What does it do</h2>

This project trains a machine learning model to predict if a customer will churn or not in the coming weeks. The project contains three workflows that does the following tasks:

1. Generates features from raw tables and stores results in a customer churn snapshot feature table
    - The raw data are snapshots of the customer data at a point in time.
2. Train machine learning model and store model on databricks
3. Make prediction on incoming data and store to databricks

<h2>Model details</h2>
<h3>Raw input data</h3>

1. Customer data: Information regarding the customer, such as current status at, start date, first delivery date. Used to generate features and also to understand the status or future status (which is used to set the label) or the customer.
2. Complaints: Get complaints data to check if the customer have many any complaints in the past and if any of those are in the last week.
3. CRM Segment data: From CRM we gather information about the customer that the might tell us more of what type of customer we are dealing with. There are features such as the customer have planned delivery, sub segment category and if the customer is a bargain hunter.
4. Events: Events from frontend might tell us more about the behavior of the customer. The churn model mostly look at three categories of events; status change, normal account activities (viewing or changing product or menu) and account activities (updating personal data or payments). From the status change we can also determine if the customer set themself to active or inactive which sets the label.
5. Orders: Features on the orders, how many orders, revenue, weeks since last orders etc.


<h2>🏃‍♀️How to run it locally?</h2>

This project is built for databricks and thus should be connected to a databricks workspace. This can be done in the VS Code extension databricks. The files in the folder `notebook` are databricks notebooks that can easily be run as databricks workflows.
