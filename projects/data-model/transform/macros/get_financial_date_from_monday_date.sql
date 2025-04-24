{#
    Get the financial date of the week based on the monday date in the week.
    The financial date is the first date of the week, except for the week of 2024-09-30,
    where the financial date is the second date of the week, making the week belong to the next month and quarter.
#}

{% macro get_financial_date_from_monday_date(monday_date) %}

    case
        when {{ monday_date}} = '2024-09-30'
        then date_add({{monday_date}}, 1)
        else {{ monday_date }}
    end

{% endmacro %}
