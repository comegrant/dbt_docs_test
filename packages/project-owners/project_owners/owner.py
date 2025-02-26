from __future__ import annotations

from dataclasses import dataclass


@dataclass
class Owner:
    name: str
    email: str
    slack_member_id: str

    def markdown(self) -> str:
        full_markdown = f" - **{self.name}**"
        if self.email:
            full_markdown += f"\n    - Email: *{self.email}*"
        if self.slack_member_id:
            full_markdown += f"\n    - Slack Member ID: *{self.slack_member_id}*"
        return full_markdown

    @staticmethod
    def matsmoll() -> Owner:
        return Owner(
            name="Mats Eikeland Mollestad",
            slack_member_id="U05Q1RUDC20",
            email="mats.mollestad@cheffelo.com",
        )

    @staticmethod
    def jose() -> Owner:
        return Owner(
            name="José Cruz",
            slack_member_id="U046CCXA5GW",
            email="jose.cruz@syone.com",
        )

    @staticmethod
    def niladri() -> Owner:
        return Owner(
            name="Niladri Banerjee",
            slack_member_id="U018KSDQY14",
            email="niladri.banerjee@cheffelo.com",
        )

    @staticmethod
    def thomassve() -> Owner:
        return Owner(
            name="Thomas Sve",
            slack_member_id="U03EC1FF10T",
            email="thomas.sve@cheffelo.com",
        )

    @staticmethod
    def stephen() -> Owner:
        return Owner(
            name="Stephen Allwright",
            slack_member_id="U05PZG6ED27",
            email="stephen.allwright@cheffelo.com",
        )

    @staticmethod
    def sylvia() -> Owner:
        return Owner(
            name="Sylvia Liu",
            slack_member_id="U04TRPCUKGD",
            email="sylvia.liu@cheffelo.com",
        )

    @staticmethod
    def grant() -> Owner:
        return Owner(
            name="Grant Levang",
            slack_member_id="U058KUJC1PV",
            email="grant.levang@cheffelo.com",
        )

    @staticmethod
    def marie() -> Owner:
        return Owner(
            name="Marie Borg",
            slack_member_id="U06CHPA1CLU",
            email="marie.borg@cheffelo.com",
        )

    @staticmethod
    def anna() -> Owner:
        return Owner(
            name="Anna Brøyn",
            slack_member_id="U06B9U6R8D6",
            email="anna.broyn@cheffelo.com",
        )

    @staticmethod
    def agathe() -> Owner:
        return Owner(
            name="Agathe Raaum",
            slack_member_id="U07AVT7SG3D",
            email="agathe.raaum@cheffelo.com",
        )

    @staticmethod
    def all_owners() -> list[Owner]:
        return [
            Owner.matsmoll(),
            Owner.jose(),
            Owner.niladri(),
            Owner.thomassve(),
            Owner.stephen(),
            Owner.sylvia(),
            Owner.grant(),
            Owner.marie(),
            Owner.anna(),
            Owner.agathe(),
        ]


def owner_for_email(email: str) -> Owner | None:
    for owner in Owner.all_owners():
        if owner.email == email:
            return owner
    return None
