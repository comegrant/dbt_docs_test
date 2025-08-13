# Business wide fields

{% docs column__menu_week_monday_date %}

The first date of the week when the menu is delivered to the customer (starts on a Monday).

{% enddocs %}

{% docs column__menu_week_financial_date %}

The financial date of the week when the menu is delivered to the customer. This is usually the Monday of the week, but with some exceptions: 
- In week 40 2024, the financial date is 2024-10-01, this is so that the week will belong to october and the quarter 3

{% enddocs %}

{% docs column__menu_year %}

The year the menu is delivered to customers.

{% enddocs %}

{% docs column__menu_week %}

The week of the year the menu is delivered to customers.

{% enddocs %}

{% docs column__menu_year_week %}

The menu year and menu week together as a number in the format YYYYWW.

{% enddocs %}

# Geographical Fields
{% docs column__latitude %}

Latitude are a geographic coordinate that specify a location on Earth's surface. Used together with longitude.

{% enddocs %}

{% docs column__longitude %}

Longitude are a geographic coordinate that specify a location on Earth's surface. Used together with latitude.

{% enddocs %}


# SCD2 fields
{% docs column__valid_from %}

...

{% enddocs %}

{% docs column__valid_to %}

...

{% enddocs %}

{% docs column__is_current %}

...

{% enddocs %}

# System Fields

{% docs column__source_created_at %}

The timestamp of when the row was created in the source system.

{% enddocs %}

{% docs column__source_created_by %}

Who created the row in the source system.

{% enddocs %}

{% docs column__source_updated_at %}

The timestamp of when the row was updated in the source system.

{% enddocs %}

{% docs column__source_updated_by %}

Who updated the row in the source system.

{% enddocs %}

# Base Table Fields
{% docs column__is_history_base %}

Used to indicate if the rows comes from a history table in the source, which is added as a base table and concatenated to the silver table of the most recent data.

{% enddocs %}

# Segment Fields

{% docs column__client_device_called_at %}

System field from Segment originially called `originalTimestamp`. Time on the client device when call was invoked or the timestamp value manually passed in through server-side libraries. Used by Segment to calculate timestamp.

Note: originalTimestamp is not useful for analysis since it’s not always trustworthy as it can be easily adjusted and affected by clock skew.

{% enddocs %}

{% docs column__client_device_sent_at %}

System field from Segment originially called `sentAt`. Time on client device when call was sent or sentAt value manually passed in. Used by Segment to calculate timestamp.

Note: sentAt is not useful for analysis since it’s not always trustworthy as it can be easily adjusted and affected by clock skew.

{% enddocs %}

{% docs column__source_recieved_at %}

System field from Segment originially called `recievedAt`. Time on Segment server clock when call was received. Used by Segment to calculate timestamp, and used as sort key in Warehouses.

Note: For max query speed, receivedAt is the recommended timestamp for analysis when chronology does not matter as chronology is not ensured.

{% enddocs %}

{% docs column__source_created_at_segment %}

System field from Segment originially called `timestamp`. alculated by Segment to correct client-device clock skew using the following formula:
receivedAt - (sentAt - originalTimestamp)

Used by Segment to send to downstream destinations, and used for historical replays.

Note: Recommended timestamp for analysis when chronology does matter.

{% enddocs %}

{% docs column__event_id_segment %}

Unique id created by Segment when an event is sent.

{% enddocs %}
