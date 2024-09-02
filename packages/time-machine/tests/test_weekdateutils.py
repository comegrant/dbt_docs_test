from datetime import date

from time_machine.weekdateutils import get_date_from_year_week, has_year_week_53


def test_get_date_from_year_week() -> None:
    result = get_date_from_year_week(
        year=2024,
        week_number=35,
        weekday=1
    )
    expected = date(year=2024, month=8, day=26)
    assert result == expected


def test_has_year_week_53() -> None:
    assert not has_year_week_53(2024)
    assert has_year_week_53(2009)
    assert has_year_week_53(2020)
    assert has_year_week_53(2026)
