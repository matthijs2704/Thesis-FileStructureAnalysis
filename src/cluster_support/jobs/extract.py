from cluster_support.jobs import Job, Project
from pathlib import Path


class ExtractJob(Job):
    name = "extract"
    modules = ["Python/3.10.13-GCCcore-11.3.0"]

    def __init__(
        self,
        base_repo_tmp_dir: Path,
        base_repo_cache_dir: Path,
        base_out_tmp_dir: Path,
        base_out_dir: Path,
        project: Project,
    ) -> None:
        super().__init__()
        self.project = project
        self.repo_dir = base_repo_cache_dir / f"{project.id}.git"
        self.repo_tmp_dir = base_repo_tmp_dir / f"{project.id}.git"

        self.project_tmp_dir = base_out_tmp_dir / f"{project.id}"
        self.project_output = base_out_dir / f"{project.id}"

    def createScript(self) -> str:
        check_if_done = f"if [ -d {self.project_output} ]; then\n"  # Check if the project output directory already exists
        check_if_done += f"    echo Project {self.project.name} already extracted, skipping\n"  # Print a message indicating that the project has already been extracted
        check_if_done += f"    exit 0\n"  # Exit the script with a success code
        check_if_done += f"fi\n"  # Close the if statement

        # Create the tmp project dir if it doesn't exist
        create_tmp_dir = f"mkdir -p {self.project_tmp_dir}\n"

        # Copy data from cache directory to temporary directory
        copy_data = (
            f"echo Moving data from cache from {self.repo_dir} to {self.repo_tmp_dir}\n"
            f"mv {self.repo_dir} {self.repo_tmp_dir}\n"
        )

        # Start extract job with specified number of workers
        start_job = (
            f"echo Starting extract job {self.project.name} using $QCG_PM_NPROCS workers \n"
            f"poetry run python -u -m repository_analysis.main {self.repo_tmp_dir} {self.project_tmp_dir} --workers $QCG_PM_NPROCS\n"
        )

        # Finish extraction
        finish_extraction = f"echo Extraction finished\n"

        # Remove project output directory
        remove_output = f"rm -rf {self.project_output}\n"

        # Copy temporary directory to project output directory
        copy_tmp_dir = f"cp -r {self.project_tmp_dir} {self.project_output}\n"

        # Remove temporary directories
        remove_tmp_dirs = f"rm -rf {self.project_tmp_dir} {self.repo_tmp_dir}\n"

        # Combine all the commands into a single script
        slurm_script = (
            "export PATH=/home/mcs001/20172091/.local/bin:$PATH\n"  # Add the local bin directory to the PATH for poetry
            + check_if_done
            + create_tmp_dir
            + copy_data
            + start_job
            + finish_extraction
            + remove_output
            + copy_tmp_dir
            + remove_tmp_dirs
        )

        return slurm_script
