from project_owners.owner import Owner, owner_for_email


def test_owner_for_email() -> None:
    expected = Owner.matsmoll()
    assert owner_for_email(expected.email) == expected
