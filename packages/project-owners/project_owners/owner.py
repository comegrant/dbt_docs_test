from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class Owner:
    name: str
    email: str
    slack_member_id: str
    github_handle: str | None = field(default=None)

    def markdown(self) -> str:
        full_markdown = f" - **{self.name}**"
        if self.email:
            full_markdown += f"\n    - Email: *{self.email}*"
        if self.slack_member_id:
            full_markdown += f"\n    - Slack Member ID: *{self.slack_member_id}*"
        if self.github_handle:
            full_markdown += f"\n    - Github Handle: *@{self.github_handle}*"

        return full_markdown

    @staticmethod
    def matsmoll() -> Owner:
        return Owner(
            name="Mats Eikeland Mollestad",
            slack_member_id="U05Q1RUDC20",
            email="mats.mollestad@cheffelo.com",
            github_handle="MatsMoll",
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
            email="sylvia.liu@cheffelo.com",
            slack_member_id="U04TRPCUKGD",
            github_handle="sylvia-liu-qinghua",
        )

    @staticmethod
    def grant() -> Owner:
        return Owner(
            name="Grant Levang",
            slack_member_id="U058KUJC1PV",
            email="grant.levang@cheffelo.com",
            github_handle="grantlevang",
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
    def daniel() -> Owner:
        return Owner(
            name="Daniel Gebäck",
            slack_member_id="UCJ8E7KE3",
            email="daniel.geback@cheffelo.com",
        )

    @staticmethod
    def synne() -> Owner:
        return Owner(
            name="Synne Andersen",
            slack_member_id="U04E4HBEEHZ",
            email="synne.andersen@cheffelo.com",
        )

    @staticmethod
    def lina() -> Owner:
        return Owner(
            name="Lina Sjölin",
            slack_member_id="UF7UCANTC",
            email="lina.sjolin@cheffelo.com",
        )

    @staticmethod
    def all_owners() -> list[Owner]:
        return [
            Owner.matsmoll(),
            Owner.jose(),
            Owner.niladri(),
            Owner.stephen(),
            Owner.sylvia(),
            Owner.grant(),
            Owner.marie(),
            Owner.anna(),
            Owner.agathe(),
            Owner.daniel(),
            Owner.synne(),
            Owner.lina(),
        ]


def owner_for_email(email: str) -> Owner | None:
    for owner in Owner.all_owners():
        if owner.email == email:
            return owner
    return None
