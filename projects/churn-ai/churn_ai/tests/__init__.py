# Necessary for importing the source code modules in test
import sys, os

root_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
sys.path.append(root_dir)
