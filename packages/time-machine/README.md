# Time machine

A Python pacakge relating to dates, holidays, and calendars

## Usage
Run the following command to install this package:

```bash
chef add time_machine
```

### Functions
The package includes the following Python functions:
1.	`get_holidays_dictionary(start_date, end_date, country=["Sweden", "Norway", "Denmark"]):` This function returns a dictionary containing holiday objects for the specified countries within the specified date range.
2.	`get_holidays_dataframe(start_date, end_date, country=["Sweden", "Norway", "Denmark"]):` This function provides a DataFrame with holidays for the specified date range.
3.	`get_calendar_dataframe_with_holiday_features(start_date, end_date, country=["Sweden", "Norway", "Denmark"]):` This function returns a DataFrame with all dates in the given range and related calendar dates and holiday features.

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
