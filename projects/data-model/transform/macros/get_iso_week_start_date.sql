{# 
    Get the first date of the iso week based on year and week. 
#}

{% macro get_iso_week_start_date(column_year, column_week) %}

{# 
    In the ISO calendar the year starts in the week
    where January 4th is present. 

    Hence we find the first monday of the iso week by using 
    January 4th as a starting point
#}

{# 
    Get the first Monday of the given ISO week by adding
    number of days until the week to the first Monday of the year.
#}
    dateadd(
        {# 
            Get the first Monday of the year, 
            by finding weekday number of January 4th 
            and extract it from January 4th.

            If Jan 4 is a Monday, then weekday number is 0,
            If Jan 4 is a Tuesday, then weekday number is 1 and so on.
        #}
        dateadd(
            to_date(concat({{ column_year }}, '-01-04'), 'y-M-d'),
            -weekday(to_date(concat({{ column_year }}, '-01-04'), 'y-M-d'))
        ), 

        {# 
            Number of days to the given week from the beginning of the year 
            is calculated by taking the weeknumber-1 and multiply with seven days.

            Weeknumber is substracted by one since we start in the first week 
            of the year.
        #}
        ({{ column_week }}-1) * 7
    )

{% endmacro %}
