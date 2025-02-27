from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class Contacts:
    name: str
    slack_handel: str | None = field(default=None)
    email: str | None = field(default=None)

    def markdown(self) -> str:
        full_markdown = f" - **{self.name}**"
        if self.slack_handel:
            full_markdown += f"\n    - Slack Handel: *{self.slack_handel}*"
        if self.email:
            full_markdown += f"\n    - Email: *{self.email}*"
        return full_markdown

    @staticmethod
    def jose() -> Contacts:
        return Contacts(
            name="José Cruz",
            slack_handel="@José Cruz",
            email="jose.cruz@syone.com",
        )

    @staticmethod
    def matsmoll() -> Contacts:
        return Contacts(
            name="Mats Eikeland Mollestad",
            slack_handel="@Mats Eikeland Mollestad",
            email="mats.mollestad@cheffelo.com",
        )

    @staticmethod
    def niladri() -> Contacts:
        return Contacts(
            name="Niladri Banerjee",
            slack_handel="@Dr. N",
            email="niladri.banerjee@cheffelo.com",
        )
