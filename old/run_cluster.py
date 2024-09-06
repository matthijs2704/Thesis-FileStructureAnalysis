import datetime
from pathlib import Path
from simple_slurm import Slurm
import sys

partition = sys.argv[1] if len(sys.argv) > 1 else "mcs.default.q"

projects = [
    "https://github.com/torvalds/linux",  # C/C++
    "https://github.com/tensorflow/tensorflow",  # Python
    "https://github.com/facebook/react-native",
    "https://github.com/facebook/react",
    "https://github.com/laravel/laravel",
    "https://github.com/ytdl-org/youtube-dl",
    "https://github.com/Alamofire/Alamofire",
    "https://github.com/vuejs/vue",
]

logs_dir = Path("logs")
logs_dir.mkdir(exist_ok=True)


def submit_clone_job(proj_name, git_url, local_dir_temp, local_dir):
    slurm = Slurm(
        partition=partition,
        nodes=1,
        cpus_per_task=1,
        job_name=f"repo_clone_{proj_name}",
        output=f"logs/repo_clone_{proj_name}_{Slurm.JOB_ARRAY_MASTER_ID}.out",
        time=datetime.timedelta(hours=1, minutes=0, seconds=0),
    )

    slurm_clone_script = (
        "module load Python/3.10.13-GCCcore-11.3.0\n"
        "module load PyTorch/1.12.1-foss-2022a-CUDA-11.7.0\n"
        'export PATH="/home/mcs001/20172091/.local/bin:$PATH"\n'
        "poetry install -n\n"
        f"mkdir -p {local_dir_temp}\n"
        f"echo Starting clone job {proj_name}\n"
        f"poetry run python -u main_clone.py {git_url} {local_dir_temp}\n"
        f"cp -r {local_dir_temp} {local_dir}\n"
        f"rm -rf {local_dir_temp}"
    )

    return slurm.sbatch(slurm_clone_script)


def submit_extract_data_job(
    proj_name,
    clone_job_id,
    repo_cache_path,
    base_tmp_dir,
    project_tmp_dir,
    project_tmp_output,
    project_output,
):
    slurm = Slurm(
        partition=partition,
        nodes=1,
        # array=range(3, 12),
        cpus_per_task=16,
        dependency=dict(afterok=clone_job_id),
        # gres=["gpu:kepler:2", "gpu:tesla:2", "mps:400"],
        job_name=f"repo_mine_{proj_name}",
        output=f"logs/repo_analyze_{proj_name}_{Slurm.JOB_ARRAY_MASTER_ID}.out",
        time=datetime.timedelta(hours=1, minutes=0, seconds=0),
    )

    slurm_script = (
        "module load Python/3.10.13-GCCcore-11.3.0\n"
        "module load PyTorch/1.12.1-foss-2022a-CUDA-11.7.0\n"
        'export PATH="/home/mcs001/20172091/.local/bin:$PATH"\n'
        "poetry install -n\n"
        f"mkdir -p {base_tmp_dir}\n"
        f"echo Copying data from cache from {repo_cache_path}/{proj_name} to {base_tmp_dir}\n"
        f"cp -R {repo_cache_path}/{proj_name} {base_tmp_dir}\n"
        f"echo Starting extract job: {proj_name}\n"
        f"poetry run python -u main_analyze.py {project_tmp_dir} --workers {Slurm.SLURM_CPUS_PER_TASK}\n"
        f"cp -r {project_tmp_output}/ {project_output}\n"
        f"rm -rf {project_tmp_dir}"
    )

    slurm.sbatch(slurm_script)


for project_url in projects:
    proj_name = "_".join(project_url.split("/")[-2:])

    base_tmp_dir = Path("/local") / "20172091"

    # Temporary project output directory (on RAM disk)
    # project_tmp = Path("/dev/shm") / "20172091" / proj_name
    project_tmp_dir = base_tmp_dir / proj_name
    project_tmp_output = project_tmp_dir / "output"

    # Final project output directory (on shared storage)
    project_output = (
        Path("/home") / "mcs001" / "20172091" / "thesis" / "output" / proj_name
    )

    # Temporary directory where the repo will be cloned (on RAM disk)
    repo_tmp = Path("/dev/shm") / "20172091" / proj_name
    # Repo cache directory (on shared storage)
    repo_cache_path = Path("/home") / "mcs001" / "20172091" / "thesis" / ".repos"

    # Add job for cloning the repo
    clone_job_id = submit_clone_job(proj_name, project_url, repo_tmp, repo_cache_path)
    # clone_job_id = None
    submit_extract_data_job(
        proj_name,
        clone_job_id,
        repo_cache_path,
        base_tmp_dir,
        project_tmp_dir,
        project_tmp_output,
        project_output,
    )
