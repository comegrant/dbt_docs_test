{# 
    Get the Monday date of a date in a week where Monday is the first day of the week, and Sunday is the seventh.
#}

{% macro get_monday_date_of_date(column_date) %}

{# 
    Get the Monday date of the given date in a week by subtracting the
    given date by its dayofweek number where Monday is 0 and Sunday is 6.
    
#}
    date_sub(
	    cast({{ column_date }} as date), 
        {# 
            Get an adjusted day of week number where Monday is 0 and Sunday is 6.
        #}
	    case when dayofweek({{ column_date }}) = 1 then 6 else dayofweek({{ column_date }}) - 2 
	end
	)

{% endmacro %}
