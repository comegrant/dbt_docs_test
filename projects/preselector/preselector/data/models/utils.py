from pydantic import BaseModel


class Yearweek(BaseModel):
    year: int
    week: int

    @property
    def yearweek(self) -> int:
        return self.year * 100 + self.week

    class PydanticMeta:
        computed = ["yearweek"]
