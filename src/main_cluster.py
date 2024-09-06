import datetime
from pathlib import Path
from simple_slurm import Slurm
import sys
import argparse

parser = argparse.ArgumentParser(description="Run cluster script")
parser.add_argument(
    "num_projects_per_pc",
    type=int,
    help="Number of projects per Programming Language",
    nargs="?",
)
parser.add_argument(
    "-p", "--partition", default="tue.default.q", help="Slurm partition name", nargs="?"
)
parser.add_argument(
    "-n", "--num_nodes", type=int, default=1, help="Number of nodes", nargs="?"
)
parser.add_argument(
    "-c", "--num_cores", type=int, default=64, help="Number of cores", nargs="?"
)
args = parser.parse_args()

num_projects_per_pc = args.num_projects_per_pc
partition = args.partition
num_nodes = args.num_nodes
num_cores = args.num_cores

logs_dir = Path("../logs")
logs_dir.mkdir(exist_ok=True)

slurm = Slurm(
    partition=partition,
    nodes=num_nodes,
    ntasks_per_node=num_cores,
    mem="128G",
    job_name=f"RepoAnalysis-QCG",
    output=str(logs_dir / "qcg.out"),
    time=datetime.timedelta(days=7, hours=0, minutes=0, seconds=0),
    mail_type="ALL",
    mail_user="m.logemann@student.tue.nl",
)
slurm_clone_script = (
    "module load Python/3.10.13-GCCcore-11.3.0\n"
    # "module load QCG-PilotJob/0.13.1-foss-2022a\n"
    'export PATH="/home/mcs001/20172091/.local/bin:$PATH"\n'
    "poetry install -n --no-root\n"
    f"echo Starting QCG\n"
    f"poetry run python -u -m cluster_support.main_qcg {num_projects_per_pc or ''}\n"
)

slurm.sbatch(slurm_clone_script)
