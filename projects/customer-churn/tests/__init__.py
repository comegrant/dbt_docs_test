# Necessary for importing the source code modules in test
import sys
from pathlib import Path

root_dir = Path(__file__).parent.parent
sys.path.append(root_dir)
