import logging
from pathlib import Path

from aligned import ContractStore

logger = logging.getLogger(__name__)


async def all_contracts() -> ContractStore:
    """
    Reads all the contracts that we have stored.

    Note! This will load all modules in the data contracts module. Thereby, it can be slow.
    """
    current_dir = Path.cwd().absolute()
    package_path = "packages/data-contracts"
    max_parent = 10
    moves = 0

    while not (current_dir / package_path).is_dir():
        if moves >= max_parent:
            raise ValueError(f"Was unable to find the contracts dir from '{Path.cwd()}'")

        moves += 1
        current_dir = current_dir.parent

    package_dir = current_dir / package_path
    logger.info(f"Found contracts at '{package_dir}'")

    return await ContractStore.from_dir(package_dir.as_posix())
