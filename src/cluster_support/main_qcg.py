import datetime
from pathlib import Path
from typing import Optional
from simple_slurm import Slurm
import sys
import os
import pandas as pd

from qcg.pilotjob.api.manager import Manager, LocalManager
from qcg.pilotjob.api.job import Jobs
import shutil

from cluster_support.jobs import Job, Project, CloneJob, ExtractJob
from os import environ
import math
import functools
import argparse
from cluster_support.combine_datasets import combine_hpc_output_datasets

parser = argparse.ArgumentParser()
parser.add_argument(
    "n_largest_projects",
    type=int,
    help="Number of most popular projects per programming language",
    nargs="?",
)
args = parser.parse_args()

n_largest_projects = args.n_largest_projects

MAX_BUFFERED_CLONE_JOBS = 144
RESERVED_CLONE_CORES = 24

running_clone_jobs: list[CloneJob] = []
running_extract_jobs: list[ExtractJob] = []
buffered_clone_jobs: list[CloneJob] = []

# If SLURM, use cluster paths, otherwise use local (as QCP allows running local)
if "SLURM_JOB_ID" in environ:
    base_tmp_dir = Path("/local") / "20172091"
    base_extract_out_dir = (
        Path("/home") / "mcs001" / "20172091" / "thesis" / "raw-output"
    )
    base_out_dir = Path("/home") / "mcs001" / "20172091" / "thesis" / "output"
    base_repo_cache_dir = (
        Path("/home") / "mcs001" / "20172091" / "thesis" / "repo_cache"
    )
    base_repo_tmp_dir = base_tmp_dir / "repo_tmp"
    # base_repo_cache_dir = base_tmp_dir / "repo_cache"
    base_out_tmp_dir = base_tmp_dir / "output"
    base_log_dir = Path("/home") / "mcs001" / "20172091" / "thesis" / "logs"
else:
    base_tmp_dir = Path(".tmp/repo_tmp")
    base_extract_out_dir = Path(".tmp/extract-output")
    base_out_dir = Path(".tmp/output")
    base_repo_tmp_dir = Path(".tmp/repo_tmp")
    base_repo_cache_dir = Path(".tmp/repos")
    base_out_tmp_dir = Path(".tmp/out_tmp")
    base_log_dir = Path(".tmp/logs")


# Load project dataset
def load_project_df() -> pd.DataFrame:
    dataset_filepath = "datasets/github_data_07_03_2024.csv"

    repo_core_map_dataset_filepath = "datasets/repo_cores.csv"
    df_cores = pd.read_csv(repo_core_map_dataset_filepath)

    df = pd.read_csv(dataset_filepath, keep_default_na=False)

    # Remove ignored projects
    df = df[~df["ignored"]]

    # Add repo_id
    df["repo_id"] = df["owner.name"].str.lower() + "_" + df["name"].str.lower()

    # Merge with cores
    df = df.merge(df_cores, on="repo_id", how="left")
    df["no_cores"] = (
        df["no_cores"].fillna(df["size"].apply(compute_num_cores)).astype(int)
    )

    # Sort by number of cores (ascending)
    # df = df.sort_values("no_cores", ascending=True)
    # df = df.sort_values("size", ascending=True)
    df = df.sort_values("stargazers_count", ascending=False)

    return df


# Load data


def df_to_projects(df: pd.DataFrame, n: Optional[int] = None) -> list[Project]:
    # Select top projects
    # if n is not None:
    #     top_projects = df.groupby("lang")["stargazers_count"].nlargest(n).reset_index()
    #     df = df[df.index.isin(top_projects["level_1"].unique())]

    # Get n projects for every core count as single df
    # if n is not None:
    #     df = df.groupby("no_cores").head(n)
    #     df = df.sort_values("no_cores", ascending=True)
    if n is not None:
        df = df.head(n)

    def dict_to_project(project) -> Project:
        project_name = project["owner.name"] + "/" + project["name"]
        project_id = project["owner.name"].lower() + "_" + project["name"].lower()
        project_cores = project["no_cores"] or compute_num_cores(project["size"])
        return Project(
            project_id,
            project_name,
            project["clone_url"],
            project_cores,
        )

    projects = list(map(lambda x: dict_to_project(x[1]), df.iterrows()))
    return projects


def prepare_directories():
    # Prepare directories
    base_tmp_dir.mkdir(parents=True, exist_ok=True)
    base_extract_out_dir.mkdir(parents=True, exist_ok=True)
    base_out_dir.mkdir(parents=True, exist_ok=True)
    base_repo_tmp_dir.mkdir(parents=True, exist_ok=True)
    base_repo_cache_dir.mkdir(parents=True, exist_ok=True)
    base_out_tmp_dir.mkdir(parents=True, exist_ok=True)
    base_log_dir.mkdir(parents=True, exist_ok=True)


# If the core count is not based on the number of commits from an earlier run
# Then we can use the following function to calculate the number of cores
# This function is based on the size of the project, which is not the best way to calculate the number of cores
def compute_num_cores(project_size: int) -> int:
    # Define scaling and shift factors
    min_cores = 1
    max_cores = 16

    # Define the base for the logarithmic function
    base = 3.25

    # Calculate the scaling factor for the logarithmic transformation
    scale_factor = (max_cores - min_cores) / (math.log(4910332, base))

    # Transform project size to logarithmic scale
    log_project_size = math.log(project_size + 1, base) * scale_factor

    # Calculate the number of cores based on the logarithmic scale
    num_cores = math.ceil(log_project_size + min_cores)

    # Ensure the number of cores is within the allowed range
    num_cores = max(min_cores, min(num_cores, max_cores))

    return int(num_cores)


def wait_for_any_job_finish() -> Job:
    job_name, status = manager.wait4_any_job_finish()
    filt_extract_jobs = list(
        filter(lambda x: x.getJobName() == job_name, running_extract_jobs)
    )
    filt_clone_jobs = list(
        filter(lambda x: x.getJobName() == job_name, running_clone_jobs)
    )
    if len(filt_extract_jobs) > 0:
        return filt_extract_jobs[0]
    elif len(filt_clone_jobs) > 0:
        return filt_clone_jobs[0]


def submit_project_extract(
    project: Project,
    after: str,
):
    extract_jobs = Jobs()
    extract_job = ExtractJob(
        base_repo_tmp_dir,
        base_repo_cache_dir,
        base_out_tmp_dir,
        base_extract_out_dir,
        project,
    )
    extract_job.schedule(
        extract_jobs,
        base_log_dir,
        {
            "numNodes": 1,
            "numCores": project.cores,
            "after": after,
        },
    )
    running_extract_jobs.append(extract_job)
    manager.submit(extract_jobs)


def submit_buffered_extract_jobs(force: bool = False):
    # Wait for resources to be available
    current_running_use = sum(map(lambda x: x.project.cores, running_extract_jobs))
    while (
        (
            current_running_use
            < total_cores - max(RESERVED_CLONE_CORES, len(running_clone_jobs))
        )
        or force
    ) and len(buffered_clone_jobs) > 0:
        buf_clone_job = buffered_clone_jobs.pop(0)
        buf_project = buf_clone_job.project
        print(
            f"Submit extract of {buf_project.id} with {buf_project.cores} cores (current resource use: {manager.resources()['used_cores']}/{total_resource_use()}/{manager.resources()['total_cores']})"
        )
        submit_project_extract(buf_project, after=buf_clone_job.getJobName())
        current_running_use += buf_project.cores


def total_resource_use():
    return (
        sum(map(lambda x: x.project.cores, running_extract_jobs))
        + sum(map(lambda x: x.project.cores, buffered_clone_jobs))
        + len(running_clone_jobs)
    )


def check_job_status():
    running_clone_job_names = list(map(lambda x: x.getJobName(), running_clone_jobs))
    if len(running_clone_job_names) > 0:
        clone_job_status = manager.status(running_clone_job_names)
        for job_name, data in clone_job_status["jobs"].items():
            if manager.is_status_finished(data["data"]["status"]):
                clone_job = list(
                    filter(lambda x: x.getJobName() == job_name, running_clone_jobs)
                )[0]
                buffered_clone_jobs.append(clone_job)
                running_clone_jobs.remove(clone_job)

    running_extract_job_names = list(
        map(lambda x: x.getJobName(), running_extract_jobs)
    )
    if len(running_extract_job_names) > 0:
        extract_job_status = manager.status(running_extract_job_names)
        for job_name, data in extract_job_status["jobs"].items():
            if manager.is_status_finished(data["data"]["status"]):
                extract_job = list(
                    filter(lambda x: x.getJobName() == job_name, running_extract_jobs)
                )[0]
                running_extract_jobs.remove(extract_job)


def runQCG():
    for project in projects:
        # Wait until there is space for a new clone job
        # And schedule extract jobs
        while True:
            check_job_status()

            # First, try to schedule extract jobs
            submit_buffered_extract_jobs()

            # Ensure that there are enough resources to schedule a new clone job
            # current_total_resource_use = total_resource_use()
            current_pending_use = (
                # sum(map(lambda x: x.project.cores, running_clone_jobs))
                sum(map(lambda x: x.project.cores, buffered_clone_jobs))
                + sum(map(lambda x: x.project.cores, running_extract_jobs))
                + len(running_clone_jobs)
            )

            if (
                (len(running_clone_jobs) + len(buffered_clone_jobs))
                < MAX_BUFFERED_CLONE_JOBS
            ) or (current_pending_use <= total_cores):
                break  # Enough resources and buffer space to schedule a new clone job

            finished_job = wait_for_any_job_finish()
            if type(finished_job) == CloneJob:
                buffered_clone_jobs.append(finished_job)
                running_clone_jobs.remove(finished_job)
            elif type(finished_job) == ExtractJob:
                running_extract_jobs.remove(finished_job)

        # Schedule clone job
        print(
            f"Submit clone of {project.id} (current resource use: {manager.resources()['used_cores']}/{total_resource_use()}/{manager.resources()['total_cores']})"
        )
        clone_jobs = Jobs()
        clone_job = CloneJob(base_repo_tmp_dir, base_repo_cache_dir, project)
        clone_job.schedule(
            clone_jobs,
            base_log_dir,
            {
                "numNodes": 1,
                "numCores": 1,
            },
        )
        running_clone_jobs.append(clone_job)
        manager.submit(clone_jobs)

    # Schedule remaining buffered extract jobs
    submit_buffered_extract_jobs(True)

    # Schedule remaining extract jobs, for projects that are being cloned
    while len(running_clone_jobs) > 0:
        running_clone_job = running_clone_jobs.pop(0)
        clone_job_name = running_clone_job.getJobName()
        print(
            f"Submit remaining extract of {running_clone_job.project.id} with {running_clone_job.project.cores} cores (current resource use: {manager.resources()['used_cores']}/{manager.resources()['total_cores']})"
        )
        submit_project_extract(running_clone_job.project, after=clone_job_name)

    # Wait for all remaining obs to finish
    manager.wait4all()
    manager.finish()


if __name__ == "__main__":
    manager = LocalManager()
    resources = manager.resources()
    print("Available resources: ", resources)
    total_cores = resources["total_cores"]

    prepare_directories()

    projects_df = load_project_df()
    all_projects = df_to_projects(projects_df, n_largest_projects)

    print("Total number of projects: ", len(all_projects))

    # Filter projects that are done
    done_projects = os.listdir(base_extract_out_dir)
    projects = list(filter(lambda x: not x.id in done_projects, all_projects))
    print(f"Total number of projects to process: {len(projects)}/{len(all_projects)}")

    if len(projects) > 0:
        runQCG()

    print("Start copying data to tmp")
    # Copy data to tmp
    try:
        shutil.copytree(
            base_extract_out_dir, base_tmp_dir / "extract-output", dirs_exist_ok=False
        )
    except FileExistsError:
        pass

    print("Start generating final datasets")
    (base_tmp_dir / "output").mkdir(parents=True, exist_ok=True)
    # Generate final datasets
    combine_hpc_output_datasets(
        base_tmp_dir / "extract-output", base_tmp_dir / "output"
    )

    print("Copying final datasets to output")
    # Copy final datasets to output
    shutil.rmtree(base_out_dir, ignore_errors=True)
    shutil.copytree(base_tmp_dir / "output", base_out_dir)

    print("Cleaning up tmp directories")
    # Clean up
    shutil.rmtree(base_tmp_dir)

    print("Finished!")
