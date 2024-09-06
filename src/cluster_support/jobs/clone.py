from cluster_support.jobs import Job, Project
from pathlib import Path


class CloneJob(Job):
    name = "clone"
    modules = ["Python/3.10.13-GCCcore-11.3.0"]

    def __init__(
        self, base_tmp_dir: Path, base_cache_dir: Path, project: Project
    ) -> None:
        super().__init__()
        self.repo_tmp_dir = base_tmp_dir / f"{project.id}.git"
        self.repo_cache_dir = base_cache_dir / f"{project.id}.git"
        self.project = project

    def createScript(self) -> str:
        # Create the script for cloning the repository
        slurm_clone_script = (
            "export PATH=/home/mcs001/20172091/.local/bin:$PATH\n"  # Add the local bin directory to the PATH for poetry
            f"mkdir -p {self.repo_tmp_dir}\n"  # Create the temporary directory for the repository
            f"echo Starting clone job {self.project.name}\n"  # Print a message indicating the start of the clone job
            f"poetry run python -u -m project_collection.main {self.project.git_url} {self.repo_tmp_dir} {self.repo_cache_dir}\n"  # Run the script to clone the repository
            f"rm -rf {self.repo_tmp_dir}"  # Remove the temporary directory after cloning
        )

        return slurm_clone_script
