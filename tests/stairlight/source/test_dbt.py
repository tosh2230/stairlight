import pytest

from src.stairlight.key import StairlightConfigKey
from src.stairlight.source.base import Template
from src.stairlight.source.dbt import DbtTemplate, DbtTemplateSource, TemplateSourceType


@pytest.mark.parametrize(
    "key",
    [
        "tests/dbt/project_01/target/compiled/project_01/a/example_a.sql",
    ],
)
class TestDbtTemplate:
    @pytest.fixture(scope="function")
    def dbt_template(
        self,
        mapping_config: dict,
        key: str,
    ):
        return DbtTemplate(
            mapping_config=mapping_config,
            key=key,
            project_name="",
        )

    def test_get_uri(self, dbt_template: DbtTemplate, key: str):
        assert dbt_template.uri.endswith(key)


@pytest.mark.parametrize(
    "project_dir, profiles_dir, target, vars, profile, project_name",
    [
        (
            "tests/dbt/project_01",
            "tests/dbt",
            "prod",
            {"key_a": "value_a", "key_b": "value_b"},
            "profile_01",
            "project_01",
        ),
        (
            "tests/dbt/project_02",
            "tests/dbt",
            "dev",
            {},
            "profile_02",
            "project_02",
        ),
    ],
)
class TestDbtTemplateSource:
    @pytest.fixture(scope="function")
    def dbt_template_source(
        self,
        stairlight_config: dict,
        mapping_config: dict,
        project_dir: str,
        profiles_dir: str,
        target: str,
        vars: dict,
        profile: str,
        project_name: str,
    ) -> DbtTemplateSource:
        source_attributes = {
            StairlightConfigKey.TEMPLATE_SOURCE_TYPE: TemplateSourceType.DBT.value,
            StairlightConfigKey.Dbt.PROJECT_DIR: project_dir,
            StairlightConfigKey.Dbt.PROFILES_DIR: profiles_dir,
            StairlightConfigKey.Dbt.TARGET: target,
            StairlightConfigKey.Dbt.VARS: vars,
        }
        return DbtTemplateSource(
            stairlight_config=stairlight_config,
            mapping_config=mapping_config,
            source_attributes=source_attributes,
        )

    def test_execute_dbt_compile(
        self,
        dbt_template_source: DbtTemplateSource,
        project_dir: str,
        profiles_dir: str,
        profile: str,
        target: str,
        vars: dict,
    ):
        actual = dbt_template_source.execute_dbt_compile(
            project_dir=project_dir,
            profiles_dir=profiles_dir,
            profile=profile,
            target=target,
            vars=vars,
        )
        assert actual == 0

    def test_read_dbt_project_yml(
        self,
        dbt_template_source: DbtTemplateSource,
        project_dir: str,
        project_name: str,
    ):
        actual = dbt_template_source.read_dbt_project_yml(project_dir=project_dir)
        assert actual["name"] == project_name

    @pytest.fixture(scope="function")
    def dbt_templates(
        self,
        dbt_template_source: DbtTemplateSource,
    ) -> "list[Template]":
        dbt_templates: list[Template] = []
        for dbt_template in dbt_template_source.search_templates():
            dbt_templates.append(dbt_template)
        return dbt_templates

    def test_search_templates(
        self,
        dbt_template_source: DbtTemplateSource,
        dbt_templates: "list[DbtTemplate]",
    ):
        assert len(dbt_templates) > 0

    def test_not_exists_schema(
        self,
        dbt_template_source: DbtTemplateSource,
        dbt_templates: "list[DbtTemplate]",
    ):
        re_matched = [
            dbt_template.key
            for dbt_template in dbt_templates
            if dbt_template_source.REGEX_SCHEMA_TEST_FILE.fullmatch(dbt_template.key)
        ]
        assert not re_matched
