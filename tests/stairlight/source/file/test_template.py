from typing import Any, Dict

import pytest

from src.stairlight.configurator import Configurator
from src.stairlight.source.config_key import (
    ConfigKeyNotFoundException,
    StairlightConfigKey,
)
from src.stairlight.source.file.template import (
    FileTemplate,
    FileTemplateSource,
    TemplateSourceType,
)
from src.stairlight.source.template import RenderingTemplateException


@pytest.mark.parametrize(
    "key, expected_is_excluded",
    [
        ("tests/sql/main/one_line_no_project.sql", False),
        ("tests/sql/main/exclude.sql", True),
    ],
)
class TestFileTemplateSource:
    @pytest.fixture(scope="function")
    def file_template_source(
        self,
        stairlight_config: Dict[str, Any],
        mapping_config: Dict[str, Any],
        key: str,
        expected_is_excluded: bool,
    ) -> FileTemplateSource:
        source_attributes = {
            StairlightConfigKey.TEMPLATE_SOURCE_TYPE: TemplateSourceType.FILE.value,
            StairlightConfigKey.File.FILE_SYSTEM_PATH: "./tests/sql",
            StairlightConfigKey.REGEX: ".*/*.sql",
        }
        return FileTemplateSource(
            stairlight_config=stairlight_config,
            mapping_config=mapping_config,
            source_attributes=source_attributes,
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
    "key, expected_is_excluded",
    [
        ("tests/sql/main/one_line_no_project.sql", False),
        ("tests/sql/main/exclude.sql", False),
    ],
)
class TestFileTemplateSourceNoExclude:
    @pytest.fixture(scope="class")
    def file_template_source(
        self,
        configurator: Configurator,
        mapping_config: Dict[str, Any],
    ) -> FileTemplateSource:
        stairlight_config = configurator.read(prefix="stairlight_no_exclude")
        source_attributes = {
            StairlightConfigKey.TEMPLATE_SOURCE_TYPE: TemplateSourceType.FILE.value,
            StairlightConfigKey.File.FILE_SYSTEM_PATH: "./tests/sql",
            StairlightConfigKey.REGEX: ".*/*.sql",
        }
        return FileTemplateSource(
            stairlight_config=stairlight_config,
            mapping_config=mapping_config,
            source_attributes=source_attributes,
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


class TestFileTemplateKeyNotFound:
    @pytest.fixture(scope="class")
    def file_template_source(
        self,
        configurator: Configurator,
        mapping_config: Dict[str, Any],
    ) -> FileTemplateSource:
        stairlight_config = configurator.read(prefix="stairlight_key_not_found")
        source_attributes = {
            StairlightConfigKey.TEMPLATE_SOURCE_TYPE: TemplateSourceType.FILE.value,
            StairlightConfigKey.REGEX: ".*/*.sql",
        }
        return FileTemplateSource(
            stairlight_config=stairlight_config,
            mapping_config=mapping_config,
            source_attributes=source_attributes,
        )

    def test_search_templates(
        self,
        file_template_source: FileTemplateSource,
    ):
        iter = file_template_source.search_templates()
        with pytest.raises(ConfigKeyNotFoundException):
            next(iter)


@pytest.mark.parametrize(
    "key",
    [
        "tests/sql/main/cte_multi_line_params.sql",
    ],
)
class TestFileTemplateMapped:
    @pytest.fixture(scope="function")
    def file_template(
        self,
        mapping_config: Dict[str, Any],
        key: str,
    ):
        return FileTemplate(
            mapping_config=mapping_config,
            key=key,
        )

    def test_is_mapped(self, file_template: FileTemplate):
        assert file_template.is_mapped()

    def test_detect_jinja_params(self, file_template: FileTemplate):
        template_str = file_template.get_template_str()
        assert len(file_template.detect_jinja_params(template_str)) > 0


@pytest.mark.parametrize(
    "key",
    [
        "tests/sql/main/undefined.sql",
        "tests/sql/gcs/cte/cte_multi_line.sql",
    ],
)
class TestFileTemplateNotMapped:
    @pytest.fixture(scope="function")
    def file_template(self, mapping_config, key):
        return FileTemplate(
            mapping_config=mapping_config,
            key=key,
        )

    def test_is_mapped(self, file_template):
        assert not file_template.is_mapped()

    def test_detect_jinja_params(self, file_template):
        template_str = file_template.get_template_str()
        assert len(file_template.detect_jinja_params(template_str)) > 0


@pytest.mark.parametrize(
    "key, params, expected_table, expected_params",
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
            "PROJECT_P.DATASET_Q.TABLE_R",
            [
                "params.sub_table_01",
                "params.sub_table_02",
                "params.main_table",
            ],
        ),
        ("tests/sql/query/nested_join.sql", None, "PROJECT_B.DATASET_B.TABLE_B", []),
    ],
)
class TestFileTemplateRender:
    @pytest.fixture(scope="function")
    def file_template(self, mapping_config, key) -> FileTemplate:
        return FileTemplate(
            mapping_config=mapping_config,
            key=key,
        )

    def test_render(
        self, file_template: FileTemplate, params, expected_table, expected_params
    ):
        actual = file_template.render(params=params)
        assert expected_table in actual

    def test_detect_jinja_params(
        self,
        file_template: FileTemplate,
        params,
        expected_table,
        expected_params,
    ):
        template_str = file_template.get_template_str()
        actual = file_template.detect_jinja_params(template_str=template_str)
        assert actual == expected_params


@pytest.mark.parametrize(
    "key, params",
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
)
class TestFileTemplateRenderException:
    @pytest.fixture(scope="function")
    def file_template(self, mapping_config, key):
        return FileTemplate(
            mapping_config=mapping_config,
            key=key,
        )

    def test_render(
        self,
        file_template: FileTemplate,
        key: str,
        params: Dict[str, Any],
    ):
        with pytest.raises(RenderingTemplateException) as exception:
            _ = file_template.render(params=params)
        assert exception.value.args[0] == (
            f"'execution_date' is undefined, "
            f"source_type: {file_template.source_type}, "
            f"key: {key}"
        )
