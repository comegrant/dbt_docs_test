# Necessary for importing the source code modules in test
import sys
from pathlib import Path

root_dir = Path(Path.parents(Path(__file__))).parents(2)
sys.path.append(root_dir)
