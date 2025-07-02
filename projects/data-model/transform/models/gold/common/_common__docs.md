# General

{% docs column__fact_updated_at %}

The source updated at timestamp from the source that was updated most recently. I.e., in Fact Orders we have recipe ratings which can be added after the initial order was updated.

{% enddocs %}

# Dim Dates

{% docs column__pk_dim_dates %}

Primary key of the dim_dates table.
This is an integer on the format YYYYMMDD (e.g. 20250117).

{% enddocs %}

# Dim Companies
{% docs column__pk_dim_companies %}

...

{% enddocs %}

# Dim Time
{% docs column__pk_dim_time %}
Primary key of the dim_time table
{% enddocs %}

{% docs column__hour %}
Hour of the day (0-23)
{% enddocs %}

{% docs column__minute %}
Minute of the hour (0-59)
{% enddocs %}

# Dim Periods Since First Order

{% docs column__pk_dim_periods_since %}

The unique key of each row in Dim Periods Since. Same as "Days Since".

{% enddocs %}

{% docs column__days_since %}

Number of days since the event in question (could be since the first menu week of an agreement).

{% enddocs %}

{% docs column__weeks_since %}

Number of weeks since the event in question (could be since the first menu week of an agreement). Calculated as "days since" devided by 7.

{% enddocs %}

{% docs column__months_since %}

Number of months since the event in question (could be since the first menu week of an agreement). Calculated as "days since" devided by 30.

{% enddocs %}

{% docs column__quarters_since %}

Number of quarters since the event in question (could be since the first menu week of an agreement). Calculated as "days since" devided by 365 and multiplied with 4.

{% enddocs %}

{% docs column__years_since %}

Number of years since the event in question (could be since the first menu week of an agreement). Calculated as "days since" first order devided by 365.

{% enddocs %}
