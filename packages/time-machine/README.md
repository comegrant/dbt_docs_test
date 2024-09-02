# Time machine

A Python pacakge relating to dates, holidays, and calendars

## Usage
Run the following command to install this package:

```bash
chef add time_machine
```

## Modules
The package includes the following Python functions:
### holidays
1.	`get_holidays_dictionary(start_date, end_date, country=["Sweden", "Norway", "Denmark"]):` This function returns a dictionary containing holiday objects for the specified countries within the specified date range.
2.	`get_holidays_dataframe(start_date, end_date, country=["Sweden", "Norway", "Denmark"]):` This function provides a DataFrame with holidays for the specified date range.
3.	`get_calendar_dataframe_with_holiday_features(start_date, end_date, country=["Sweden", "Norway", "Denmark"]):` This function returns a DataFrame with all dates in the given range and related calendar dates and holiday features.

### forecasting
As forecasting often requires manipulation of calendars, the following functions have been added
1. Generate forecasting calendar
The function below gives you a dataframe with the integer columns `year` and `week`, starting at an inferred start_date based on your forecast_date, and contains num_weeks consecutive weeks.
```
from time_machine.forecasting import get_forecast_calendar
from datetime import date
df_forecast_calendar = get_forecast_calendar(
    num_weeks=11,
    cut_off_day=2,
    forecast_date = date(year=2024, month=8, day=25),
)
```
2. Infer forecast start
```
from time_machine.forecasting import get_forecast_calendar
from datetime import date
start_year, start_week = get_forecast_start(
    cut_off_day=2,
    forecast_date=date(year=2024, month=8, day=1),
)
```

### weekdateutils
This modules contains other miscellaneous functions that manipulates date, week and year.
1. create a date from a specified year and week number
```
from time_machine.weekdateutils import get_date_from_year_week
# the date is Tuesday, 29 Sep. 2024
a_date = get_date_from_year_week(
    year=2024,
    week=39,
    weekday=2
)
```
2. Check if a year has iso_week 53
```
from time_machine.weekdateutils import has_year_week_53

has_year_week_53(2024)  # False
has_year_week_53(2020)  # True
```


### Example Usage
`import time_machine`

#### Get holidays dictionary for a given date range in Sweden
```
holiday_dict = time_machine.get_holidays_dictionary('2022-01-01', '2022-12-31', 'Sweden')
```

#### Get holidays dataframe for a given date range in Norway
```
holiday_df = time_machine.get_holidays_dataframe('2022-01-01', '2022-12-31', 'Norway')
```

#### Get calendar dataframe with holiday features for a given date range in Denmark

```
calendar_df = time_machine.get_calendar_dataframe_with_holiday_features('2022-01-01', '2022-12-31', 'Denmark')
```

## Develop

### Installation
To develop on this package, install all the dependencies with `poetry install`.

Then make your changes with your favorite editor of choice.


### Testing
Run the following command to test that the changes do not impact critical functionality:

```bash
chef test
```
This will spin up a Docker image and run your code in a similar environment as our production code.

### Linting
We have added linting which automatically enforces coding style guides.
You may as a result get some failing pipelines from this.

Run the following command to fix and reproduce the linting errors:

```bash
chef lint
```

You can also install `pre-commit` if you want linting to be applied on commit.
