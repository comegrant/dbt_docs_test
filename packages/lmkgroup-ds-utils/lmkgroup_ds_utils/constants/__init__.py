""" Stores constants commonly used in the code """
import logging
from typing import ClassVar

from pydantic import BaseModel

logger = logging.getLogger(__name__)


class Company(BaseModel):
    id: str
    name: str
    code: str


class Companies:
    LMK = Company(id="6A2D0B60-84D6-4830-9945-58D518D27AC2", name="LINAS_MATKASSE", code="LMK")
    AMK = Company(id="8A613C15-35E4-471F-91CC-972F933331D7", name="ADAMS_MATKASSE", code="AMK")
    GL = Company(id="09ECD4F0-AE58-4539-8E8F-9275B1859A19", name="GODTLEVERT", code="GL")
    RN = Company(id="5E65A955-7B1A-446C-B24F-CFE576BF52D7", name="RETNEMNT", code="RN")

    ALL: ClassVar[list[Company]] = [LMK, AMK, GL, RN]

    @classmethod
    def get_id_from_name(cls: "Companies", name: str) -> str:
        for company in cls.ALL:
            if company.name == name:
                return company.id

    @classmethod
    def get_id_from_code(cls: "Companies", code: str) -> str:
        for company in cls.ALL:
            if company.code == code:
                return company.id

    @classmethod
    def get_name_from_id(cls: "Companies", company_id: str) -> str:
        for company in cls.ALL:
            if company.id == company_id:
                return company.name

    @classmethod
    def get_code_from_id(cls: "Companies", company_id: str) -> str:
        for company in cls.ALL:
            if company.id == company_id:
                return company.code


class ProductType:
    MEALBOX = "2F163D69-8AC1-6E0C-8793-FF0000804EB3"
    VELGVRAK = "CAC333EA-EC15-4EEA-9D8D-2B9EF60EC0C1"
