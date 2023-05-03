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
from src.stairlight.source.template import RenderingTemplateException


@pytest.mark.parametrize(
    ("key", "expected_is_mapped"),
    [
        ("tests/sql/main/cte_multi_line_params.sql", True),
        ("tests/sql/main/cte_multi_line_params_copy.sql", True),
        ("tests/sql/main/undefined.sql", False),
        ("tests/sql/gcs/cte/cte_multi_line.sql", False),
    ],
    ids=[
        "tests/sql/main/cte_multi_line_params.sql",
        "tests/sql/main/cte_multi_line_params_copy.sql",
        "tests/sql/main/undefined.sql",
        "tests/sql/gcs/cte/cte_multi_line.sql",
    ],
)
class TestFileTemplate:
    @pytest.fixture(scope="function")
    def file_template(
        self,
        mapping_config: MappingConfig,
        key: str,
        expected_is_mapped: bool,
    ):
        return FileTemplate(
            mapping_config=mapping_config,
            key=key,
        )

    def test_is_mapped(self, file_template: FileTemplate, expected_is_mapped: bool):
        assert file_template.is_mapped() == expected_is_mapped

    def test_detect_jinja_params(self, file_template: FileTemplate):
        template_str = file_template.get_template_str()
        assert len(file_template.detect_jinja_params(template_str)) > 0


@pytest.mark.parametrize(
    ("key", "params", "ignore_params", "expected_table", "detected_params"),
    [
        (
            "tests/sql/main/cte_multi_line_params.sql",
            {
                "params": {
                    "main_table": "PROJECT_P.DATASET_Q.TABLE_R",
                    "sub_table_01": "PROJECT_S.DATASET_T.TABLE_U",
                    "sub_table_02": "PROJECT_V.DATASET_W.TABLE_X",
                }
            },
            [],
            "PROJECT_P.DATASET_Q.TABLE_R",
            [
                "params.sub_table_01",
                "params.sub_table_02",
                "params.main_table",
            ],
        ),
        (
            "tests/sql/main/cte_multi_line.sql",
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
            [
                "execution_date.add(days=1).isoformat()",
                "execution_date.add(days=2).isoformat()",
                "params.PROJECT",
                "params.DATASET",
                "params.TABLE",
            ],
        ),
        (
            "tests/sql/main/params_with_default_value.sql",
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
            "tests/sql/query/nested_join.sql",
            None,
            [],
            "PROJECT_B.DATASET_B.TABLE_B",
            [],
        ),
    ],
    ids=[
        "tests/sql/main/cte_multi_line_params.sql",
        "tests/sql/main/cte_multi_line.sql",
        "tests/sql/main/params_with_default_value.sql",
        "tests/sql/query/nested_join.sql",
    ],
)
class TestFileTemplateRender:
    @pytest.fixture(scope="function")
    def file_template(
        self,
        mapping_config: MappingConfig,
        key: str,
    ) -> FileTemplate:
        return FileTemplate(
            mapping_config=mapping_config,
            key=key,
        )

    def test_render(
        self,
        file_template: FileTemplate,
        params,
        ignore_params,
        expected_table,
        detected_params,
    ):
        actual = file_template.render(
            params=params,
            ignore_params=ignore_params,
        )
        assert expected_table in actual

    def test_detect_jinja_params(
        self,
        file_template: FileTemplate,
        params,
        ignore_params,
        expected_table,
        detected_params,
    ):
        template_str = file_template.get_template_str()
        actual = file_template.detect_jinja_params(template_str=template_str)
        assert actual == detected_params

    def test_ignore_params_from_template_str(
        self,
        file_template: FileTemplate,
        params,
        ignore_params,
        expected_table,
        detected_params,
    ):
        template_str = file_template.get_template_str()
        actual = file_template.ignore_params_from_template_str(
            template_str=template_str,
            ignore_params=ignore_params,
        )
        assert all(ignore_param not in actual for ignore_param in ignore_params)


@pytest.mark.parametrize(
    ("key", "params"),
    [
        (
            "tests/sql/main/cte_multi_line.sql",
            {
                "params": {
                    "PROJECT": "RENDERED_PROJECT",
                    "DATASET": "RENDERED_DATASET",
                    "TABLE": "RENDERED_TABLE",
                }
            },
        ),
    ],
    ids=["tests/sql/main/cte_multi_line.sql"],
)
class TestFileTemplateRenderException:
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
    ):
        with pytest.raises(RenderingTemplateException) as exception:
            _ = file_template.render(params=params)
        assert exception.value.args[0] == (
            f"'execution_date' is undefined, "
            f"source_type: {file_template.source_type}, "
            f"key: {key}"
        )


@pytest.mark.parametrize(
    ("key", "expected_is_excluded"),
    [
        ("tests/sql/main/one_line_no_project.sql", False),
        ("tests/sql/main/exclude.sql", True),
    ],
    ids=[
        "tests/sql/main/one_line_no_project.sql",
        "tests/sql/main/exclude.sql",
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
        ("tests/sql/main/one_line_no_project.sql", False),
        ("tests/sql/main/exclude.sql", False),
    ],
    ids=[
        "tests/sql/main/one_line_no_project.sql",
        "tests/sql/main/exclude.sql",
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
