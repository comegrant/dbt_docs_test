from constants.companies import get_company_by_code
from pydantic import BaseModel

AMK = get_company_by_code("AMK").company_id
LMK = get_company_by_code("LMK").company_id
GL = get_company_by_code("GL").company_id
RT = get_company_by_code("RT").company_id


class TaxonomyOneSubMapping(BaseModel):
    chefs_favorite: dict[str, list[str]]
    family_friendly: dict[str, list[str]]
    quick_and_easy: dict[str, list[str]]
    vegetarian: dict[str, list[str]]
    low_calorie: dict[str, list[str]]


TAXONOMY_ONESUB_MAPPING = TaxonomyOneSubMapping(
    chefs_favorite={
        AMK: ["inspirasjon", "chefs choice", "inspirerende"],
        GL: ["favoritter", "chefs choice", "inspirerende"],
        LMK: ["kockens val", "klassiskt och inspirerande", "chefs choice", "inspirerande"],
        RT: ["chefs choice", "inspirerende"],
    },
    family_friendly={
        AMK: ["anbefalt", "family", "barnevennlig", "familiefavoritter"],
        GL: ["familie", "favorittkassen", "barnevennlig", "family", "familiefavoritter"],
        LMK: ["familjefavoriter", "barnfamilj", "familjefavorit", "family", "barnvänligt", "barnvänlig"],
        RT: ["familievenlige", "family", "børnevenlig", "familiefavoritter"],
    },
    quick_and_easy={
        AMK: ["ekspress", "rask", "express", "laget på 1-2-3"],
        GL: ["raske retter", "ekspress", "express", "rask", "fort gjort"],
        LMK: ["snabbt & lättlagat", "enkelt och lättlagat", "express", "linas express", "snabb"],
        RT: ["hurtige", "hurtig", "nem på 5", "express"],
    },
    vegetarian={
        AMK: ["vegetar", "vegan", "vegetarisk (lakto-ovo)", "vegetarian"],
        GL: ["vegetar", "vegan", "vegetarisk (lakto-ovo)", "vegetarian"],
        LMK: ["vegetarisk", "vegetariskt", "vegan", "vegetarisk (lakto-ovo)", "vegetarian", "vegetar"],
        RT: [
            "vegetariske",
            "vegan",
            "vegetar-opskrifter",
            "vegetarretter",
            "vegetarian",
            "vegetar",
            "vegetar (lacto-ovo)",
        ],
    },
    low_calorie={
        AMK: ["sunn og lett", "low calorie", "roede", "sunt valg", "sunn"],
        GL: ["sunt valg", "roedekassen", "roede", "low calorie", "sunt valg", "sunn"],
        LMK: ["kalorismart", "viktväktarna", "ww viktväktarna", "low calorie"],
        RT: ["kalorilet", "low calorie", "sund"],
    },
)
