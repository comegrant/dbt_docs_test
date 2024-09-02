from datetime import date

import pandas as pd
import pytest
from time_machine.forecasting import get_forecast_calendar, get_forecast_start


@pytest.fixture
def cut_off_day() -> int:
    return 2

@pytest.fixture
def num_weeks() -> int:
    return 3


@pytest.fixture
def date_before_cut_off() -> date:
    return date(2024, 12, 30)


@pytest.fixture
def date_past_cut_off() -> date:
    return date(2024, 8, 28)


def test_get_forecast_start(
    cut_off_day: int,
    date_before_cut_off: date,
    date_past_cut_off: date
) -> None:
    before_year_week = get_forecast_start(
        cut_off_day=cut_off_day,
        forecast_date=date_before_cut_off
    )
    after_year_week = get_forecast_start(cut_off_day=2, forecast_date=date_past_cut_off)
    before_expected = (2025, 2)
    after_expected = (2024, 37)

    assert before_year_week == before_expected
    assert after_year_week == after_expected


def test_get_forecast_calendar(
    date_before_cut_off: date,
    date_past_cut_off: date,
    cut_off_day: int,
    num_weeks: int
) -> None:
    expected_past_cut_off = pd.DataFrame(
        {
            "year": [2024, 2024, 2024],
            "week": [37, 38, 39],
        }
    )

    expected_before_cut_off = pd.DataFrame(
        {
            "year": [2025, 2025, 2025],
            "week": [2, 3, 4],
        }
    )

    result_past_cut_off = get_forecast_calendar(
        forecast_date=date_past_cut_off,
        cut_off_day=cut_off_day,
        num_weeks=num_weeks
    )

    result_before_cut_off = get_forecast_calendar(
        forecast_date=date_before_cut_off,
        cut_off_day=cut_off_day,
        num_weeks=num_weeks
    )

    assert (result_before_cut_off.values == expected_before_cut_off.values).all()
    assert (result_past_cut_off.values == expected_past_cut_off.values).all()
