from __future__ import annotations

from typing import Any

import pytest

from src.stairlight.configurator import Configurator
from src.stairlight.source.config import (
    ConfigAttributeNotFoundException,
    MappingConfig,
    StairlightConfig,
)
from src.stairlight.source.config_key import StairlightConfigKey
from src.stairlight.source.file.config import StairlightConfigIncludeFile
from src.stairlight.source.file.template import (
    FileTemplate,
    FileTemplateSource,
    TemplateSourceType,
)


@pytest.mark.parametrize(
    ("key", "expected_mapped"),
    [
        ("tests/sql/cte_multi_line_params.sql", True),
        ("tests/sql/cte_multi_line_params_copy.sql", True),
        ("tests/sql/undefined.sql", False),
        ("tests/sql/gcs/cte/cte_multi_line.sql", False),
    ],
    ids=[
        "tests/sql/cte_multi_line_params.sql",
        "tests/sql/cte_multi_line_params_copy.sql",
        "tests/sql/undefined.sql",
        "tests/sql/gcs/cte/cte_multi_line.sql",
    ],
)
class TestFileTemplate:
    @pytest.fixture(scope="function")
    def file_template(
        self,
        mapping_config: MappingConfig,
        key: str,
        expected_mapped: bool,
    ):
        return FileTemplate(
            mapping_config=mapping_config,
            key=key,
        )

    def test_mapped(self, file_template: FileTemplate, expected_mapped: bool):
        assert file_template.mapped == expected_mapped

    def test_get_jinja_params(self, file_template: FileTemplate):
        template_str = file_template.get_template_str()
        assert len(file_template.get_jinja_params(template_str)) > 0


class TestFileTemplateRender:
    @pytest.mark.parametrize(
        (
            "key",
            "params",
            "ignore_params",
            "expected_table",
        ),
        [
            (
                "tests/sql/cte_multi_line_params.sql",
                {
                    "params": {
                        "main_table": "PROJECT_P.DATASET_Q.TABLE_R",
                        "sub_table_01": "PROJECT_S.DATASET_T.TABLE_U",
                        "sub_table_02": "PROJECT_V.DATASET_W.TABLE_X",
                    }
                },
                [],
                "PROJECT_P.DATASET_Q.TABLE_R",
            ),
            (
                "tests/sql/cte_multi_line.sql",
                {
                    "params": {
                        "PROJECT": "PROJECT_g",
                        "DATASET": "DATASET_h",
                        "TABLE": "TABLE_i",
                    }
                },
                [
                    "execution_date.add(days=1).isoformat()",
                    "execution_date.add(days=2).isoformat()",
                ],
                "PROJECT_g.DATASET_h.TABLE_i",
            ),
            (
                "tests/sql/params_with_default_value.sql",
                {
                    "params": {
                        "main_table": "PROJECT_P.DATASET_Q.TABLE_R",
                        "sub_table_01": "PROJECT_S.DATASET_T.TABLE_U",
                        "sub_table_02": "PROJECT_V.DATASET_W.TABLE_X",
                    }
                },
                [
                    "params.target_column | default('\"top\"')",
                    'params.target_column or "top"',
                ],
                "PROJECT_P.DATASET_Q.TABLE_R",
            ),
            (
                "tests/sql/nested_join.sql",
                None,
                [],
                "PROJECT_B.DATASET_B.TABLE_B",
            ),
            (
                "tests/sql/cte_multi_line_identifiers.sql",
                {
                    "main_table": "PROJECT_P.DATASET_Q.TABLE_R",
                    "sub_table_01": "PROJECT_S.DATASET_T.TABLE_U",
                    "sub_table_02": "PROJECT_V.DATASET_W.TABLE_X",
                },
                [],
                "PROJECT_P.DATASET_Q.TABLE_R",
            ),
        ],
        ids=[
            "tests/sql/cte_multi_line_params.sql",
            "tests/sql/cte_multi_line.sql",
            "tests/sql/params_with_default_value.sql",
            "tests/sql/nested_join.sql",
            "tests/sql/cte_multi_line_identifiers.sql",
        ],
    )
    def test_render(
        self,
        mapping_config: MappingConfig,
        key,
        params,
        ignore_params,
        expected_table,
    ):
        file_template = FileTemplate(
            mapping_config=mapping_config,
            key=key,
        )
        actual = file_template.render(
            params=params,
            ignore_params=ignore_params,
        )
        assert expected_table in actual

    @pytest.mark.parametrize(
        ("key", "detected_params"),
        [
            (
                "tests/sql/cte_multi_line_params.sql",
                [
                    "params.sub_table_01",
                    "params.sub_table_02",
                    "params.main_table",
                ],
            ),
            (
                "tests/sql/cte_multi_line.sql",
                [
                    "execution_date.add(days=1).isoformat()",
                    "execution_date.add(days=2).isoformat()",
                    "params.PROJECT",
                    "params.DATASET",
                    "params.TABLE",
                ],
            ),
            (
                "tests/sql/params_with_default_value.sql",
                [
                    "params.sub_table_01",
                    "params.sub_table_02",
                    "params.main_table",
                    "params.target_column | default('\"top\"')",
                    'params.target_column_2 or "top"',
                    'params.target_column_2 or "latest"',
                ],
            ),
            (
                "tests/sql/nested_join.sql",
                [],
            ),
            (
                "tests/sql/cte_multi_line_identifiers.sql",
                [],
            ),
        ],
        ids=[
            "tests/sql/cte_multi_line_params.sql",
            "tests/sql/cte_multi_line.sql",
            "tests/sql/params_with_default_value.sql",
            "tests/sql/nested_join.sql",
            "tests/sql/cte_multi_line_identifiers.sql",
        ],
    )
    def test_get_jinja_params(
        self,
        mapping_config,
        key,
        detected_params,
    ):
        file_template = FileTemplate(
            mapping_config=mapping_config,
            key=key,
        )
        template_str = file_template.get_template_str()
        actual = file_template.get_jinja_params(template_str=template_str)
        assert actual == detected_params

    @pytest.mark.parametrize(
        ("key", "ignore_params"),
        [
            (
                "tests/sql/cte_multi_line.sql",
                [
                    "execution_date.add(days=1).isoformat()",
                    "execution_date.add(days=2).isoformat()",
                ],
            ),
            (
                "tests/sql/params_with_default_value.sql",
                [
                    "params.target_column | default('\"top\"')",
                    'params.target_column or "top"',
                ],
            ),
            (
                "tests/sql/nested_join.sql",
                [],
            ),
        ],
        ids=[
            "tests/sql/cte_multi_line.sql",
            "tests/sql/params_with_default_value.sql",
            "tests/sql/nested_join.sql",
        ],
    )
    def test_ignore_jinja_params(
        self,
        mapping_config,
        key,
        ignore_params,
    ):
        file_template = FileTemplate(
            mapping_config=mapping_config,
            key=key,
        )
        template_str = file_template.get_template_str()
        actual = file_template.ignore_jinja_params(
            template_str=template_str,
            ignore_params=ignore_params,
        )
        assert all(ignore_param not in actual for ignore_param in ignore_params)

    @pytest.mark.parametrize(
        ("key", "ignore_params"),
        [
            (
                "tests/sql/cte_multi_line_identifiers.sql",
                ["main_table", "sub_table_02"],
            ),
        ],
        ids=["tests/sql/cte_multi_line_identifiers.sql"],
    )
    def test_ignore_string_template_params(
        self,
        mapping_config,
        key,
        ignore_params,
    ):
        file_template = FileTemplate(
            mapping_config=mapping_config,
            key=key,
        )
        template_str = file_template.get_template_str()
        actual = file_template.ignore_string_template_params(
            template_str=template_str,
            ignore_params=ignore_params,
        )
        assert all(ignore_param not in actual for ignore_param in ignore_params)


@pytest.mark.parametrize(
    ("key", "params"),
    [
        (
            "tests/sql/cte_multi_line.sql",
            {
                "params": {
                    "PROJECT": "RENDERED_PROJECT",
                    "DATASET": "RENDERED_DATASET",
                    "TABLE": "RENDERED_TABLE",
                }
            },
        ),
    ],
    ids=["tests/sql/cte_multi_line.sql"],
)
class TestFileTemplateUndefinedError:
    @pytest.fixture(scope="function")
    def file_template(
        self,
        mapping_config: MappingConfig,
        key: str,
    ):
        return FileTemplate(
            mapping_config=mapping_config,
            key=key,
        )

    def test_render(
        self,
        file_template: FileTemplate,
        key: str,
        params: dict[str, Any],
        caplog,
    ):
        _ = file_template.render(params=params)
        assert "undefined" in caplog.text


@pytest.mark.parametrize(
    ("key", "expected_is_excluded"),
    [
        ("tests/sql/one_line_no_project.sql", False),
        ("tests/sql/exclude.sql", True),
    ],
    ids=[
        "tests/sql/one_line_no_project.sql",
        "tests/sql/exclude.sql",
    ],
)
class TestFileTemplateSource:
    @pytest.fixture(scope="function")
    def file_template_source(
        self,
        stairlight_config: StairlightConfig,
        mapping_config: MappingConfig,
        key: str,
        expected_is_excluded: bool,
    ) -> FileTemplateSource:
        _include = StairlightConfigIncludeFile(
            **{
                StairlightConfigKey.TEMPLATE_SOURCE_TYPE: TemplateSourceType.FILE.value,
                StairlightConfigKey.File.FILE_SYSTEM_PATH: "./tests/sql",
                StairlightConfigKey.REGEX: ".*/*.sql",
            }
        )
        return FileTemplateSource(
            stairlight_config=stairlight_config,
            mapping_config=mapping_config,
            include=_include,
        )

    def test_search_templates(
        self,
        file_template_source: FileTemplateSource,
    ):
        result = []
        for file in file_template_source.search_templates():
            result.append(file)
        assert len(result) > 0

    def test_is_excluded(
        self,
        file_template_source: FileTemplateSource,
        key: str,
        expected_is_excluded: bool,
    ):
        actual = file_template_source.is_excluded(
            source_type=TemplateSourceType.FILE,
            key=key,
        )
        assert actual == expected_is_excluded


@pytest.mark.parametrize(
    ("key", "expected_is_excluded"),
    [
        ("tests/sql/one_line_no_project.sql", False),
        ("tests/sql/exclude.sql", False),
    ],
    ids=[
        "tests/sql/one_line_no_project.sql",
        "tests/sql/exclude.sql",
    ],
)
class TestFileTemplateSourceNoExclude:
    @pytest.fixture(scope="class")
    def file_template_source(
        self,
        configurator: Configurator,
        mapping_config: MappingConfig,
    ) -> FileTemplateSource:
        stairlight_config = configurator.read_stairlight(prefix="stairlight_no_exclude")
        _include = StairlightConfigIncludeFile(
            **{
                StairlightConfigKey.TEMPLATE_SOURCE_TYPE: TemplateSourceType.FILE.value,
                StairlightConfigKey.File.FILE_SYSTEM_PATH: "./tests/sql",
                StairlightConfigKey.REGEX: ".*/*.sql",
            }
        )
        return FileTemplateSource(
            stairlight_config=stairlight_config,
            mapping_config=mapping_config,
            include=_include,
        )

    def test_is_excluded(
        self,
        file_template_source: FileTemplateSource,
        key: str,
        expected_is_excluded: bool,
    ):
        actual = file_template_source.is_excluded(
            source_type=TemplateSourceType.FILE,
            key=key,
        )
        assert actual == expected_is_excluded


class TestFileTemplateSourceConfigKeyNotFound:
    @pytest.fixture(scope="class")
    def file_template_source(
        self,
        configurator: Configurator,
        mapping_config: MappingConfig,
    ) -> FileTemplateSource:
        stairlight_config = configurator.read_stairlight(
            prefix="stairlight_key_not_found"
        )
        _include = StairlightConfigIncludeFile(
            **{
                StairlightConfigKey.TEMPLATE_SOURCE_TYPE: TemplateSourceType.FILE.value,
                StairlightConfigKey.REGEX: ".*/*.sql",
            }
        )
        return FileTemplateSource(
            stairlight_config=stairlight_config,
            mapping_config=mapping_config,
            include=_include,
        )

    def test_search_templates(
        self,
        file_template_source: FileTemplateSource,
    ):
        iter = file_template_source.search_templates()
        with pytest.raises(ConfigAttributeNotFoundException) as exception:
            next(iter)
        assert exception.value.args[0] == (
            f"FileSystemPath is not found. {file_template_source._include}"
        )
