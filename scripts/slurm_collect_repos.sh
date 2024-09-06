#!/bin/bash
#SBATCH --partition=mcs.default.q
#SBATCH --job-name=RepoCollect
#SBATCH --output=../.logs/repo-collect.out
#SBATCH --error=../.logs/repo-collect.err
#SBATCH --time=02:00:00
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1

module load Python/3.10.13-GCCcore-11.3.0

export PATH=/home/mcs001/20172091/.local/bin:$PATH
poetry install -n --no-root
poetry run python3 -u main_collect_repos.py datasets/top_10_languages.json datasets/github_data_filtered.csv