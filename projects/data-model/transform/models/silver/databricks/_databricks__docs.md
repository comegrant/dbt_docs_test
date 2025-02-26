# Dates
{% docs column__date %}

Date of year (e.g. 2025-01-25)

{% enddocs %}

{% docs column__calendar_year %}

The normal calendar year of the date.

{% enddocs %}

{% docs column__year_of_calendar_week %}

The year corresponding to the ISO week of the date. 
For example, December 30th, 2024, falls within ISO week 1 of 2025. Therefore, this column will display 2025 for that date, as it reflects the year associated with the ISO week, even though the calendar year of the date is 2024.

{% enddocs %}

{% docs column__calendar_quarter %}

Quarter of the year (1-4), holding three iso months each. 

{% enddocs %}

{% docs column__calendar_month_number %}

Month of the date (1-12). This follows the normal iso calendar which is the normal calendar in Scandinavia. 

{% enddocs %}

{% docs column__calendar_month_name %}

Full name of the month. This follows the normal iso calendar which is the normal calendar in Scandinavia. 

{% enddocs %}

{% docs column__calendar_week %}

The iso week of the year. This follows the normal iso calendar which is the normal calendar in Scandinavia. 

{% enddocs %}

{% docs column__financial_year %}

The year corresponding to the Monday of the same week. 
For example, January 1st, 2025, falls on a Wednesday, and the Monday of that week is December 30th, 2024. Therefore, the financial year for January 1st, 2025, is 2024, as it follows the year of the corresponding Monday.

{% enddocs %}

{% docs column__financial_quarter %}

The quarter corresponding to the Monday of the same week. 
For example, January 1st, 2025, falls on a Wednesday, and the Monday of that week is December 30th, 2024. Therefore, the financial quarter for January 1st, 2025, is 4, as it follows the quarter of the corresponding Monday.

{% enddocs %}

{% docs column__financial_month_number %}

The number of the month corresponding to the Monday of the same week. 
For example, January 1st, 2025, falls on a Wednesday, and the Monday of that week is December 30th, 2024. Therefore, the financial month number for January 1st, 2025, is December, as it follows the quarter of the corresponding Monday.

{% enddocs %}

{% docs column__financial_month_name %}

The full name of the month corresponding to the Monday of the same week. 
For example, January 1st, 2025, falls on a Wednesday, and the Monday of that week is December 30th, 2024. Therefore, the financial month name for January 1st, 2025, is December, as it follows the quarter of the corresponding Monday.

{% enddocs %}

{% docs column__financial_week %}

The week number of the date, adjusted to align with the Monday of the corresponding week. A financial week 53 is introduced when necessary to replace ISO week 1 of the following year.

Example: Monday, December 30th, 2024, falls in ISO week 1 of 2025. Since this Monday is in 2024, we create financial week 53, and all days in ISO week 1 are assigned to it. This makes week 2 the first financial week of 2025.

{% enddocs %}

{% docs column__day_of_week %}

Day number of the week. Monday = 1, Sunday = 7.

{% enddocs %}

{% docs column__weekday_name %}

Name of the weekday.

{% enddocs %}

{% docs column__monday_date %}

The date of the Monday in the week. 

{% enddocs %}

{% docs column__monday_date_previous_week %}

The date of the Monday in the previous week. This is useful when applying relative filters in Power BI, as it allows you to reference the menu week from the preceding week with the cutoff.

{% enddocs %}

{% docs column__day_of_financial_year_number  %}

Counts the number of days since the first day of the financial year. Some years the financial year starts on week two instead of week one, when this occurs the counter will start counting on day 8 instead of 1.

{% enddocs %}

{% docs column__day_of_financial_quarter_number  %}

Counts the number of days since the beginning of the financial quarter

{% enddocs %}

{% docs column__day_of_financial_month_number  %}

Counts the number of days since the beginning of the financial month.

{% enddocs %}