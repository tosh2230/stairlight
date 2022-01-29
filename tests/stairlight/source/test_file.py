import pytest

import src.stairlight.config as config
from src.stairlight.source.file import (
    FileTemplate,
    FileTemplateSource,
    TemplateSourceType,
)


class TestFileTemplateSource:
    configurator = config.Configurator(dir="./config")
    stairlight_config = configurator.read(prefix=config.STAIRLIGHT_CONFIG_PREFIX)
    mapping_config = configurator.read(prefix=config.MAPPING_CONFIG_PREFIX)

    source_attributes = {
        "type": TemplateSourceType.FILE.value,
        "path": "./tests/sql",
        "regex": ".*/*.sql",
    }
    template_source = FileTemplateSource(
        stairlight_config=stairlight_config,
        mapping_config=mapping_config,
        source_attributes=source_attributes,
    )

    def test_search_templates_iter(self):
        result = []
        for file in self.template_source.search_templates_iter():
            result.append(file)
        assert len(result) > 0

    def test_is_excluded_test_a(self):
        assert not self.template_source.is_excluded(
            source_type=TemplateSourceType.FILE,
            file_path="tests/sql/main/test_a.sql",
        )

    def test_is_excluded_test_exclude(self):
        assert self.template_source.is_excluded(
            source_type=TemplateSourceType.FILE,
            file_path="tests/sql/main/test_exclude.sql",
        )


@pytest.mark.parametrize(
    "file_path",
    [
        "tests/sql/main/test_a.sql",
        "tests/sql/main/test_exclude.sql",
    ],
)
class TestFileTemplateSourceNoExclude:
    configurator = config.Configurator(dir="./config")
    stairlight_config = configurator.read(prefix="stairlight_no_exclude")
    mapping_config = configurator.read(prefix=config.MAPPING_CONFIG_PREFIX)

    source_attributes = {
        "type": TemplateSourceType.FILE.value,
        "path": "./tests/sql",
        "regex": ".*/*.sql",
    }
    template_source = FileTemplateSource(
        stairlight_config=stairlight_config,
        mapping_config=mapping_config,
        source_attributes=source_attributes,
    )

    def test_is_excluded(self, file_path):
        assert not self.template_source.is_excluded(
            source_type=TemplateSourceType.FILE,
            file_path=file_path,
        )


@pytest.mark.parametrize(
    "file_path, bucket",
    [
        ("tests/sql/main/test_c.sql", None),
    ],
)
class TestFileTemplateMapped:
    configurator = config.Configurator(dir="./config")
    mapping_config = configurator.read(prefix=config.MAPPING_CONFIG_PREFIX)

    def test_is_mapped(self, file_path, bucket):
        sql_template = FileTemplate(
            mapping_config=self.mapping_config,
            source_type=TemplateSourceType.FILE,
            file_path=file_path,
            bucket=bucket,
        )
        assert sql_template.is_mapped()

    def test_get_jinja_params(self, file_path, bucket):
        sql_template = FileTemplate(
            mapping_config=self.mapping_config,
            source_type=TemplateSourceType.FILE,
            file_path=file_path,
            bucket=bucket,
        )
        template_str = sql_template.get_template_str()
        assert len(sql_template.get_jinja_params(template_str)) > 0


@pytest.mark.parametrize(
    "file_path, bucket",
    [
        ("tests/sql/main/test_undefined.sql", None),
        ("tests/sql/gcs/test_b/test_b.sql", None),
    ],
)
class TestFileTemplateNotMapped:
    configurator = config.Configurator(dir="./config")
    mapping_config = configurator.read(prefix=config.MAPPING_CONFIG_PREFIX)

    def test_is_mapped(self, file_path, bucket):
        sql_template = FileTemplate(
            mapping_config=self.mapping_config,
            source_type=TemplateSourceType.FILE,
            file_path=file_path,
            bucket=bucket,
        )
        assert not sql_template.is_mapped()

    def test_get_jinja_params(self, file_path, bucket):
        sql_template = FileTemplate(
            mapping_config=self.mapping_config,
            source_type=TemplateSourceType.FILE,
            file_path=file_path,
            bucket=bucket,
        )
        template_str = sql_template.get_template_str()
        assert len(sql_template.get_jinja_params(template_str)) > 0


class TestFileTemplateRender:
    configurator = config.Configurator(dir="./config")
    mapping_config = configurator.read(prefix=config.MAPPING_CONFIG_PREFIX)

    def test_render(self):
        params = {
            "main_table": "PROJECT_P.DATASET_Q.TABLE_R",
            "sub_table_01": "PROJECT_S.DATASET_T.TABLE_U",
            "sub_table_02": "PROJECT_V.DATASET_W.TABLE_X",
        }
        sql_template = FileTemplate(
            mapping_config=self.mapping_config,
            source_type=TemplateSourceType.FILE,
            file_path="tests/sql/main/test_c.sql",
        )
        query_str = sql_template.render(params=params)
        expected = """WITH c AS (
    SELECT
        test_id,
        col_c
    FROM
        PROJECT_S.DATASET_T.TABLE_U
    WHERE
        0 = 0
),
d AS (
    SELECT
        test_id,
        col_d
    FROM
        PROJECT_V.DATASET_W.TABLE_X
    WHERE
        0 = 0
)

SELECT
    *
FROM
    PROJECT_P.DATASET_Q.TABLE_R AS b
    INNER JOIN c
        ON b.test_id = c.test_id
    INNER JOIN d
        ON b.test_id = d.test_id
WHERE
    1 = 1"""
        assert query_str == expected
