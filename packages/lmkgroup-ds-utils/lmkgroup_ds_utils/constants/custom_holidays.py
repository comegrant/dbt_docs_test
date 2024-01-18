# Place to put custom holidays (special dates that are not in the holidays.py module).
# We store stuff in a dictionary with the following params
#   -   company_id: if it is specific to a certain company. If not, set null (need to develop)
#   -   holiday: short name for the thingy
#   -   week: the week in question (is handled in the code as the date of the monday that year)
#   -   lower_window: the lower bound of number of days for the effect (default = 0)
#   -   upper_window: the upper bound of number of days for the effect (default = 0)


# I would like to instead filter by country maybe
import pandas as pd

from . import Company


def custom_holidays(company_id):
    """
    Function that defines custom holidays for the different brands
    """
    data = [
        [Company.GODTLEVERT, "fall_break_1", 40, 0, 6],
        [Company.GODTLEVERT, "fall_break_2", 41, 0, 6],
        [Company.GODTLEVERT, "last_week_before_christmas", 51, 0, 6],
        [Company.GODTLEVERT, "winter_break", 8, 0, 6],
        [Company.ADAMS_MATKASSE, "fall_break_1", 40, 0, 6],
        [Company.ADAMS_MATKASSE, "fall_break_2", 41, 0, 6],
        [Company.ADAMS_MATKASSE, "last_week_before_christmas", 51, 0, 6],
        [Company.ADAMS_MATKASSE, "winter_break", 8, 0, 6],
    ]

    holidays = pd.DataFrame.from_records(
        data, columns=["company_id", "holiday", "week", "lower_window", "upper_window"]
    )

    return holidays[holidays["company_id"] == company_id]
