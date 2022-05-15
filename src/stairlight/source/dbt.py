import glob
import pathlib
import re
import shlex
import subprocess
from typing import Iterator

import yaml

from .. import config_key
from ..config import get_config_value
from .base import Template, TemplateSource, TemplateSourceType


class DbtTemplate(Template):
    def __init__(
        self,
        mapping_config: dict,
        key: str,
        source_type: TemplateSourceType,
        project_name: str,
    ):
        super().__init__(
            mapping_config=mapping_config,
            key=key,
            source_type=source_type,
        )
        self.uri = self.get_uri()
        self.project_name = project_name

    def get_uri(self) -> str:
        """Get uri from file path

        Returns:
            str: uri
        """
        return str(pathlib.Path(self.key).resolve())

    def get_template_str(self) -> str:
        """Get template string that read from a file

        Returns:
            str: Template string
        """
        with open(self.key) as f:
            return f.read()

    def render(self, params: dict = None) -> str:
        return self.get_template_str()


class DbtTemplateSource(TemplateSource):
    def __init__(
        self,
        stairlight_config: dict,
        mapping_config: dict,
        source_attributes: dict,
    ) -> None:
        super().__init__(
            stairlight_config=stairlight_config,
            mapping_config=mapping_config,
        )
        self.source_attributes = source_attributes
        self.source_type = TemplateSourceType.DBT

    def search_templates_iter(self) -> Iterator[Template]:
        dbt_config = self.get_dbt_config()
        self.execute_dbt_compile(
            project_dir=dbt_config["project_dir"],
            profiles_dir=dbt_config["profiles_dir"],
            profile=dbt_config["profile"],
            target=dbt_config["target"],
            vars=dbt_config["vars"],
        )

        dbt_project_config = self.get_dbt_project_config(dbt_config=dbt_config)
        for model_path in dbt_project_config[config_key.DBT_MODEL_PATHS]:
            path = (
                f"{dbt_project_config[config_key.DBT_PROJECT_NAME]}/"
                f"{dbt_project_config[config_key.DBT_TARGET_PATH]}/"
                "compiled/"
                f"{model_path}/"
            )
            path_obj = pathlib.Path(path)
            for p in path_obj.glob("**/*.sql"):
                if (
                    not re.fullmatch(r"schema.yml/.*\.sql$", str(p))
                ) or self.is_excluded(source_type=self.source_type, key=str(p)):
                    self.logger.debug(f"{str(p)} is skipped.")
                    continue

                yield DbtTemplate(
                    mapping_config=self._mapping_config,
                    key=str(p),
                    source_type=self.source_type,
                    project_name=dbt_project_config[config_key.DBT_PROJECT_NAME],
                )

    @staticmethod
    def execute_dbt_compile(
        project_dir: str,
        profiles_dir: str,
        profile: str = None,
        target: str = None,
        vars: dict = None,
    ) -> None:
        if not target:
            target = "default"
        command = (
            "dbt compile "
            f"--project-dir {project_dir} "
            f"--profiles-dir {profiles_dir} "
        )
        if profile:
            command += f"--profile {profile} "
        if target:
            command += f"--target {target} "
        if vars:
            command += f"--vars '{vars}' "
        proc = subprocess.run(
            args=shlex.split(command),
            shell=False,
        )
        if proc.returncode != 0:
            raise Exception(proc.stderr)

    def get_dbt_config(self) -> dict:
        dbt_config = {}
        dbt_attributes = [
            {"key": config_key.DBT_PROJECT_DIR, "fail_if_not_found": True},
            {"key": config_key.DBT_PROFILES_DIR, "fail_if_not_found": True},
            {"key": config_key.DBT_PROFILE, "fail_if_not_found": True},
            {"key": config_key.DBT_TARGET, "fail_if_not_found": True},
            {"key": config_key.DBT_VARS, "fail_if_not_found": False},
        ]
        for dbt_attribute in dbt_attributes:
            dbt_config[dbt_attribute['key']] = get_config_value(
                key=dbt_attribute['key'],
                target=self.source_attributes,
                fail_if_not_found=dbt_attribute['fail_if_not_found'],
                enable_logging=False,
            )
        return dbt_config

    def get_dbt_project_config(self, dbt_config: dict) -> dict:
        dbt_project_config = {}
        project: dict = self.read_dbt_project_yml(
            project_dir=dbt_config["project_dir"]
        )
        project_attributes = [
            {"key": config_key.DBT_PROJECT_NAME},
            {"key": config_key.DBT_TARGET_PATH},
            {"key": config_key.DBT_MODEL_PATHS},
        ]
        for project_attribute in project_attributes:
            dbt_project_config[project_attribute['key']] = get_config_value(
                key=project_attribute['key'],
                target=project,
                fail_if_not_found=True,
                enable_logging=False,
            )
        return dbt_project_config

    def read_dbt_project_yml(self, project_dir: str) -> dict:
        """Read dbt_project.yml

        Args:
            project_dir (str): dbt project directory

        Returns:
            dict: dbt project settings
        """
        project: dict = None
        pattern = f"^{project_dir}/dbt_project.yml$"
        project_file = [
            p
            for p in glob.glob(f"{self.dir}/**", recursive=False)
            if re.fullmatch(pattern, p)
        ]
        if project_file:
            with open(project_file[0]) as file:
                project = yaml.safe_load(file)
        return project
