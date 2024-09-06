from typing import Any, Optional
from qcg.pilotjob.api.job import Jobs
from pathlib import Path


class Project:
    def __init__(self, id: str, name: str, git_url: str, cores: Optional[int]) -> None:
        self.id = id
        self.name = name
        self.git_url = git_url
        self.cores = cores


class Job:
    name: str
    modules: list[str] = []
    project: Project

    def createScript(kwargs) -> str:
        pass

    def getJobName(self) -> str:
        return f"{self.name}_{self.project.id}"

    def schedule(self, jobs: Jobs, logs_dir: Path, attributes: dict[str, Any]):
        jobs.add(
            name=self.getJobName(),
            stdout=f"{logs_dir}/{self.getJobName()}.out",
            stderr=f"{logs_dir}/{self.getJobName()}.err",
            script=self.createScript(),
            modules=self.modules,
            **attributes,
        )
