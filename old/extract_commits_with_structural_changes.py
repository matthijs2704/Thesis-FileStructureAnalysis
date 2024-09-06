# BEGIN: 8j3d9f4g5h6j
import pandas as pd
import sys

# read the JSONL file into a pandas DataFrame
import pandas as pd
import sys

input_file = sys.argv[1]
df = pd.read_json(input_file, lines=True)

# extract commit_id column as list to stdout
commit_ids = df['commit_id'].unique().tolist()
for commit_id in commit_ids:
    sys.stdout.write(commit_id + '\n')

