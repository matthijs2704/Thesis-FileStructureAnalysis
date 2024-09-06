#!/usr/bin/env /bin/bash
REPO_URL=$1
REPO_NAME=$(basename $REPO_URL)

export PYTHONPATH=/Users/matthijs/Development/TUe/Thesis/ThesisCode

if [ ! -d "/Volumes/RAMDisk/$REPO_NAME.git" ]; then
    pyrepositoryminer clone $REPO_URL /Volumes/RAMDisk/$REPO_NAME.git
fi

mkdir -p output/
if [ -d "output/" ]; then
    rm -rf output/*
fi

# pyrepositoryminer branch /Volumes/RAMDisk/$REPO_NAME.git | \
# pyrepositoryminer commits --simplify-first-parent /Volumes/RAMDisk/$REPO_NAME.git | \
#     pyrepositoryminer analyze --workers 14 --custom-metrics metrics:ExtractMutationsMetric \
#         /Volumes/RAMDisk/$REPO_NAME.git > output/output_debug.jsonl


# pyrepositoryminer branch /Volumes/RAMDisk/$REPO_NAME.git | \
# pyrepositoryminer commits --simplify-first-parent /Volumes/RAMDisk/$REPO_NAME.git | \
#     pyrepositoryminer analyze --workers 15 --custom-metrics metrics:ExtractMutationsMetric \

python main.py /Volumes/RAMDisk/$REPO_NAME.git > output/raw_data.jsonl

python convert_dataset.py output/raw_data.jsonl

# python extract_commits_with_structural_changes.py output/data.jsonl > output/commits_struc_changes.txt

# cat output/commits_struc_changes.txt | pyrepositoryminer analyze --workers 15 --custom-metrics metrics:Linguist /Volumes/RAMDisk/$REPO_NAME.git | while read -r line ; do
#     echo "$line" | python convert_linguist_metric_output.py >> output/linguist_data.jsonl
# done

# cat output/commits_struc_changes.txt | pyrepositoryminer analyze --workers 14 --custom-metrics analyse_linguist:Linguist \
#         /Volumes/RAMDisk/$REPO_NAME.git > output/raw_lang_data.jsonl
# python test_tree.py