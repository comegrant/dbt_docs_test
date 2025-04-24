from pydantic import BaseModel
from typing import Literal


class Args(BaseModel):
    language: Literal["danish", "norwegian", "swedish"]
    env: Literal["dev", "test", "prod"]

    @property
    def language_id(self) -> int:
        if self.language == "norwegian":
            return 1
        elif self.language == "swedish":
            return 5
        elif self.language == "danish":
            return 6
        else:
            raise ValueError("Invalid language")

    @property
    def language_short(self) -> str:
        if self.language == "norwegian":
            return "no"
        elif self.language == "swedish":
            return "se"
        elif self.language == "danish":
            return "dk"
        else:
            raise ValueError("Invalid language")
