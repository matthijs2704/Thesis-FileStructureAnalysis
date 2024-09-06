from project_selection import filter_projects
from pathlib import Path
import sys

input_dataset = Path(sys.argv[1])
output_dataset = Path(sys.argv[2])

filter_projects(input_dataset, output_dataset)
print("Done")
