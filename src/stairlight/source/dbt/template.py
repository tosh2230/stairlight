import glob
import os
import pathlib
import re
import shlex
import subprocess
from typing import Any, Dict, Iterator, List

import yaml

from ..config import MappingConfig, StairlightConfig
from ..config_key import DbtProjectKey
from ..template import Template, TemplateSource, TemplateSourceType
from .config import StairlightConfigIncludeDbt


class DbtTemplate(Template):
    def __init__(
        self,
        mapping_config: MappingConfig,
        key: str,
        project_name: str,
    ):
        super().__init__(
            mapping_config=mapping_config,
            key=key,
            source_type=TemplateSourceType.DBT,
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

    def render(
        self, params: Dict[str, Any] = None, ignore_params: List[str] = None
    ) -> str:
        return self.get_template_str()


class DbtTemplateSource(TemplateSource):
    DBT_PROJECT_YAML = "dbt_project.yml"
    REGEX_SCHEMA_TEST_FILE = re.compile(r".*/schema.yml/.*\.sql$")

    def __init__(
        self,
        stairlight_config: StairlightConfig,
        mapping_config: MappingConfig,
        include: StairlightConfigIncludeDbt,
    ) -> None:
        super().__init__(
            stairlight_config=stairlight_config,
            mapping_config=mapping_config,
        )
        self._include = include

    def search_templates(self) -> Iterator[Template]:
        project_dir: str = self._include.ProjectDir
        dbt_project_config: Dict[str, Any] = self.read_dbt_project_yml(
            project_dir=project_dir
        )

        _ = self.execute_dbt_compile(
            project_dir=project_dir,
            profiles_dir=self._include.ProfilesDir,
            profile=dbt_project_config.get(DbtProjectKey.PROFILE),
            target=self._include.Target,
            vars=self._include.Vars,
        )

        target_path = dbt_project_config[DbtProjectKey.TARGET_PATH]
        project_name = dbt_project_config[DbtProjectKey.PROJECT_NAME]
        model_paths = dbt_project_config[DbtProjectKey.MODEL_PATHS]

        for model_path in model_paths:
            dbt_model_path_str = self.concat_dbt_model_path_str(
                project_dir=project_dir,
                target_path=target_path,
                project_name=project_name,
                model_path=model_path,
            )
            dbt_model_path = pathlib.Path(dbt_model_path_str)
            for p in dbt_model_path.glob("**/*"):
                if self.is_skipped(p=p):
                    self.logger.debug(f"{str(p)} is skipped.")
                    continue

                yield DbtTemplate(
                    mapping_config=self._mapping_config,
                    key=str(p),
                    project_name=project_name,
                )

    def is_skipped(self, p: pathlib.Path):
        return (
            p.is_dir()
            or self.REGEX_SCHEMA_TEST_FILE.fullmatch(str(p))
            or self.is_excluded(
                source_type=TemplateSourceType(self._include.TemplateSourceType),
                key=str(p),
            )
        )

    def read_dbt_project_yml(self, project_dir: str) -> dict:
        """Read dbt_project.yml

        Args:
            project_dir (str): dbt project directory

        Returns:
            dict: dbt project settings
        """
        dbt_project_pattern = re.compile(f"^{project_dir}/{self.DBT_PROJECT_YAML}$")
        return self.read_yml(dir=project_dir, re_pattern=dbt_project_pattern)

    def read_yml(self, dir: str, re_pattern: re.Pattern) -> dict:
        files = [
            obj
            for obj in glob.glob(f"{dir}/**", recursive=False)
            if re_pattern.fullmatch(obj)
        ]
        if not files:
            self.logger.error(f"Not found: {re_pattern} in {os.getcwd()}")
            exit()
        with open(files[0]) as file:
            return yaml.safe_load(file)

    @staticmethod
    def concat_dbt_model_path_str(
        project_dir: str,
        target_path: str,
        project_name: str,
        model_path: pathlib.Path,
    ) -> str:
        return (
            f"{project_dir}/"
            f"{target_path}/"
            "compiled/"
            f"{project_name}/"
            f"{model_path}/"
        )

    @staticmethod
    def build_dbt_compile_command(
        project_dir: str,
        profiles_dir: str,
        profile: str = None,
        target: str = None,
        vars: Dict[str, Any] = None,
    ):
        command = (
            "dbt compile"
            f" --project-dir {project_dir}"
            f" --profiles-dir {profiles_dir}"
        )
        if profile:
            command += f" --profile {profile}"
        if target:
            command += f" --target {target}"
        if vars:
            command += f" --vars '{vars}'"
        return command

    def execute_dbt_compile(
        self,
        project_dir: str,
        profiles_dir: str,
        profile: str = None,
        target: str = None,
        vars: Dict[str, Any] = None,
    ) -> int:
        command = self.build_dbt_compile_command(
            project_dir=project_dir,
            profiles_dir=profiles_dir,
            profile=profile,
            target=target,
            vars=vars,
        )
        proc = subprocess.run(
            args=shlex.split(command),
            shell=False,
            check=True,
            stdout=subprocess.DEVNULL,
        )
        return proc.returncode
