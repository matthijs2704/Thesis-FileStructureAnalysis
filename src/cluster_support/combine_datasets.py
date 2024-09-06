import os
from pathlib import Path
import sys
import pandas as pd
from typing import List, Tuple
import json
import multiprocessing as mp
import numpy as np

import dask

dask.config.set({"dataframe.query-planning": True})
import dask.dataframe as dd


def get_dataset_files(input_dir: Path, file_name: Path) -> List[Tuple[str, str]]:
    file_paths = []
    for dir in input_dir.iterdir():
        if not dir.is_dir():
            continue

        file = dir / file_name
        if file.exists():
            file_paths.append((dir.name, file))
        else:
            print(f"Error in {dir.name}: {file_name} not found")
    return file_paths


def combine_repo_info(input_dir: Path, output_dir: Path):
    # Combine the repo_info.json files into a single dataset
    print("REPO_INFO Combining repo_info.json files into a single dataset")
    filelist = get_dataset_files(input_dir, "repo_info.json")
    df_list = []
    for repo_id, file in filelist:
        with open(file, "r") as f:
            repo_info = json.load(f)
            repo_info["repo_id"] = repo_id
            df_list.append(pd.DataFrame([repo_info]))
    print("REPO_INFO Loaded all repo info!")
    repo_info_big_df = pd.concat(df_list, ignore_index=True)
    repo_info_big_df = repo_info_big_df.set_index("repo_id")
    print("REPO_INFO Dataset shape: ", repo_info_big_df.shape)
    repo_info_big_df.to_parquet(
        f"{output_dir}/repo_infos.parquet", index=True, compression="brotli"
    )
    print("REPO_INFO Done")


def combine_all_commits(input_dir: Path, output_dir: Path):
    # Combine the all_commits.csv files into a single dataset
    print("ALL_COMMITS Combining all_commits.csv files into a single dataset")
    filelist = get_dataset_files(input_dir, "all_commits.parquet")
    df_list = []
    for repo_id, file in filelist:
        df = pd.read_parquet(file)
        df["repo_id"] = repo_id
        df_list.append(df)
    print("ALL_COMMITS Loaded all commits!")

    bucket = 0
    cumulative_size = 0
    threshold = 100 * 1024 * 1024  # 100MB
    for df in df_list:
        size = df.memory_usage(deep=True).sum()
        cumulative_size += size
        if cumulative_size > threshold:
            bucket += 1
            cumulative_size = size
        df["bucket"] = bucket

    all_commits_big_df = pd.concat(df_list)
    print("ALL_COMMITS Dataset shape: ", all_commits_big_df.shape)
    all_commits_big_df.to_parquet(
        f"{output_dir}/all_commits.parquet",
        index=True,
        compression="brotli",
        partition_cols="bucket",
    )
    print("ALL_COMMITS Done")


def process_commits_file(repo_id: str, file: str) -> pd.DataFrame:
    try:
        df = pd.read_parquet(file)
        df["repo_id"] = repo_id
        df["message"] = df["message"].fillna("")

        # DEPRECATED: This is now done in the preprocess function, as the total_files_per_level column is now a comma separated string,
        #               we can convert it to a list of integers when needed in the analysis
        # Fix the total_files_per_level column
        # if "total_files_per_level" in df.columns:
        #     max_file_depth = int(df["total_files_per_level"].str.len().max())
        #     df[[f"no_files_level_{i}" for i in range(max_file_depth)]] = df[
        #         "total_files_per_level"
        #     ].apply(pd.Series)
        #     df = df.drop(columns=["total_files_per_level"])
    except Exception as e:
        print(f"Error processing {file}: {e}")
        df = pd.DataFrame()
    return df


def combine_commits(input_dir: Path, output_dir: Path):
    # Combine commits.parquet files into a single dataset
    print("COMMITS Combining commits.parquet files into a single dataset")
    filelist = get_dataset_files(input_dir, "commits.parquet")
    df_list = []

    pool = mp.Pool(max(1, mp.cpu_count() - 16))
    results = [
        pool.apply_async(process_commits_file, args=(repo_id, file))
        for repo_id, file in filelist
    ]
    df_list = [result.get() for result in results]
    pool.close()
    pool.join()
    print("COMMITS Loaded commits!")

    # Bucket the commits into 100MB partitions
    # Keeping in mind repo_id
    bucket = 0
    cumulative_size = 0
    threshold = 100 * 1024 * 1024  # 100MB
    for df in df_list:
        size = df.memory_usage(deep=True).sum()
        cumulative_size += size
        if cumulative_size > threshold:
            bucket += 1
            cumulative_size = size
        df["bucket"] = bucket

    commits_big_df = pd.concat(df_list)
    print("COMMITS Dataset shape: ", commits_big_df.shape)

    # Save parquet, using the bucket column to partition the data
    commits_big_df.to_parquet(
        f"{output_dir}/commits.parquet",
        index=True,
        compression="brotli",
        partition_cols="bucket",
    )
    print("COMMITS Done")


def process_data_file(data: Tuple[str, str]) -> pd.DataFrame:
    df = pd.read_parquet(data[1])
    df["repo_id"] = data[0]
    return df


def combine_data(input_dir: Path, output_dir: Path):
    # Combine the mutations.parquet files into a single dataset
    print("DATA Combining mutations.parquet files into a single dataset")

    filelist = get_dataset_files(input_dir, "mutations.parquet")

    pool = mp.Pool(mp.cpu_count())
    dfs = []
    for project_df in pool.imap_unordered(process_data_file, filelist):
        dfs.append(project_df)
        print(f"Loaded {len(dfs)} datasets")
    pool.close()
    pool.join()

    print("MUTATIONS Datasets loaded")
    bucket = 0
    cumulative_size = 0
    threshold = 100 * 1024 * 1024  # 100MB
    for df in dfs:
        size = df.memory_usage(deep=True).sum()
        cumulative_size += size
        if cumulative_size > threshold:
            bucket += 1
            cumulative_size = size
        df["bucket"] = bucket

    data_big_df = pd.concat(dfs, ignore_index=True)
    print("MUTATIONS Dataset shape: ", data_big_df.shape)

    # Ensure that file_depth is a list of length 2, and does not contain null as the first or second element
    # apply(
    #     lambda x: x if x else [-1, -1]
    # )
    # data_big_df["file_depth"] = data_big_df["file_depth"].apply(
    #     lambda x: x if len(x) == 2 else [-1, -1]
    # )

    print("MUTATIONS Writing to parquet")
    data_big_df.to_parquet(
        f"{output_dir}/mutations.parquet",
        compression="brotli",
        partition_cols="bucket",
    )
    # with pq.ParquetWriter(
    #     f"{output_dir}/data.parquet", schema, compression="brotli"
    # ) as writer:
    #     table = pa.Table.from_pandas(data_big_df, schema=schema)
    #     writer.write_table(table)

    print("MUTATIONS Done")


def combine_hpc_output_datasets(input_dir: Path, output_dir: Path):
    combine_repo_info_process = mp.Process(
        target=combine_repo_info,
        args=(
            input_dir,
            output_dir,
        ),
    )
    combine_all_commits_process = mp.Process(
        target=combine_all_commits,
        args=(
            input_dir,
            output_dir,
        ),
    )
    combine_commits_process = mp.Process(
        target=combine_commits,
        args=(
            input_dir,
            output_dir,
        ),
    )

    combine_repo_info_process.start()
    combine_all_commits_process.start()
    combine_commits_process.start()

    combine_repo_info_process.join()
    combine_all_commits_process.join()
    combine_commits_process.join()

    print("Starting combine_data")
    combine_data(input_dir, output_dir)


if __name__ == "__main__":
    args = sys.argv
    if len(args) < 3:
        print("Usage: python combine_datasets.py <input_dir> <output_dir>")
        sys.exit(1)

    input_dir = Path(args[1])
    output_dir = Path(args[2])
    combine_hpc_output_datasets(input_dir, output_dir)
    print("Done!")
    sys.stdout.flush()
    sys.exit(0)
