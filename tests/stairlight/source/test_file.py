import pytest

from src.stairlight import config_key
from src.stairlight.config import Configurator, ConfigKeyNotFoundException
from src.stairlight.source.file import (
    FileTemplate,
    FileTemplateSource,
    TemplateSourceType,
)


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
        stairlight_config: dict,
        mapping_config: dict,
        key: str,
        expected_is_excluded: bool,
    ) -> FileTemplateSource:
        source_attributes = {
            config_key.TEMPLATE_SOURCE_TYPE: TemplateSourceType.FILE.value,
            config_key.FILE_SYSTEM_PATH: "./tests/sql",
            config_key.REGEX: ".*/*.sql",
        }
        return FileTemplateSource(
            stairlight_config=stairlight_config,
            mapping_config=mapping_config,
            source_attributes=source_attributes,
        )

    def test_search_templates_iter(
        self,
        file_template_source: FileTemplateSource,
    ):
        result = []
        for file in file_template_source.search_templates_iter():
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
    def template_source(
        self,
        configurator: Configurator,
        mapping_config: dict,
    ) -> FileTemplateSource:
        stairlight_config = configurator.read(prefix="stairlight_no_exclude")
        source_attributes = {
            config_key.TEMPLATE_SOURCE_TYPE: TemplateSourceType.FILE.value,
            config_key.FILE_SYSTEM_PATH: "./tests/sql",
            config_key.REGEX: ".*/*.sql",
        }
        return FileTemplateSource(
            stairlight_config=stairlight_config,
            mapping_config=mapping_config,
            source_attributes=source_attributes,
        )

    def test_is_excluded(
        self,
        template_source: FileTemplateSource,
        key: str,
        expected_is_excluded: bool,
    ):
        actual = template_source.is_excluded(
            source_type=TemplateSourceType.FILE,
            key=key,
        )
        assert actual == expected_is_excluded


class TestFileTemplateKeyNotFound:
    @pytest.fixture(scope="class")
    def template_source(
        self,
        configurator: Configurator,
        mapping_config: dict,
    ) -> FileTemplateSource:
        stairlight_config = configurator.read(prefix="stairlight_key_not_found")
        source_attributes = {
            config_key.TEMPLATE_SOURCE_TYPE: TemplateSourceType.FILE.value,
            config_key.REGEX: ".*/*.sql",
        }
        return FileTemplateSource(
            stairlight_config=stairlight_config,
            mapping_config=mapping_config,
            source_attributes=source_attributes,
        )

    def test_search_templates_iter(
        self,
        template_source: FileTemplateSource,
    ):
        iter = template_source.search_templates_iter()
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
        mapping_config: dict,
        key: str,
    ):
        return FileTemplate(
            mapping_config=mapping_config,
            source_type=TemplateSourceType.FILE,
            key=key,
            bucket=None,
        )

    def test_is_mapped(self, file_template: FileTemplate):
        assert file_template.is_mapped()

    def test_get_jinja_params(self, file_template: FileTemplate):
        template_str = file_template.get_template_str()
        assert len(file_template.get_jinja_params(template_str)) > 0


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
            source_type=TemplateSourceType.FILE,
            key=key,
            bucket=None,
        )

    def test_is_mapped(self, file_template):
        assert not file_template.is_mapped()

    def test_get_jinja_params(self, file_template):
        template_str = file_template.get_template_str()
        assert len(file_template.get_jinja_params(template_str)) > 0


@pytest.mark.parametrize(
    "key, params, expected",
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
        )
    ],
)
class TestFileTemplateRender:
    @pytest.fixture(scope="function")
    def file_template(self, mapping_config, key):
        return FileTemplate(
            mapping_config=mapping_config,
            source_type=TemplateSourceType.FILE,
            key=key,
            bucket=None,
        )

    def test_render(self, file_template, params, expected):
        actual = file_template.render(params=params)
        assert expected in actual
