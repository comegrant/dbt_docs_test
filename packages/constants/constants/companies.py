import dataclasses


@dataclasses.dataclass
class Company:
    company_code: str
    company_name: str
    company_id: str
    cut_off_week_day: int
    country: str
    language: str


company_amk = Company(
    company_code="AMK",
    company_name="Adams Matkasse",
    company_id="8A613C15-35E4-471F-91CC-972F933331D7",
    cut_off_week_day=2,
    country="Norway",
    language="Norwegian",
)


company_gl = Company(
    company_code="GL",
    company_name="Godtlevert",
    company_id="09ECD4F0-AE58-4539-8E8F-9275B1859A19",
    cut_off_week_day=2,
    country="Norway",
    language="Norwegian",
)


company_lmk = Company(
    company_code="LMK",
    company_name="Linas Matkasse",
    company_id="6A2D0B60-84D6-4830-9945-58D518D27AC2",
    cut_off_week_day=2,
    country="Sweden",
    language="Swedish",
)


company_rt = Company(
    company_code="RT",
    company_name="Retnemt",
    company_id="5E65A955-7B1A-446C-B24F-CFE576BF52D7",
    cut_off_week_day=4,
    country="Denmark",
    language="Danish",
)


def get_all_companies() -> list[object]:
    return [company_amk, company_gl, company_lmk, company_rt]


def get_company_by_id(company_id: str) -> object:
    all_companies = get_all_companies()
    for cls in all_companies:
        if hasattr(cls, "company_id") and cls.company_id == company_id:
            return cls
    raise ValueError("No company with the given id found.")


def get_company_by_name(company_name: str) -> object:
    all_companies = get_all_companies()
    for cls in all_companies:
        company_name_processed = company_name.lower().replace(" ", "")
        if hasattr(cls, "company_name"):
            class_company_name = cls.company_name.lower().replace(" ", "")
            if company_name_processed in class_company_name:
                return cls
    raise ValueError("No company with the given name found.")


def get_company_by_code(company_code: str) -> object:
    all_companies = get_all_companies()
    for cls in all_companies:
        if hasattr(cls, "company_code") and cls.company_code == company_code:
            return cls
    raise ValueError("No company with the given company code found.")
