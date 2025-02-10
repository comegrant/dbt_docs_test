import logging
from typing import Literal, Optional

from pydantic import BaseModel

logger = logging.getLogger(__name__)


class Args(BaseModel):
    company: Literal["LMK", "AMK", "GL", "RT"]
    env: Literal["dev", "test", "prod"]
    target: Optional[Literal["has_chefs_favorite_taxonomy", "has_family_friendly_taxonomy"]] = None
