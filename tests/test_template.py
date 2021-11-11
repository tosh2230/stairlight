import pytest

import src.stairlight.config as config
import src.stairlight.template as template


class TestTemplateSourceSuccess:
    configurator = config.Configurator("./config/")
    strl_config = configurator.read(prefix=config.STRL_CONFIG_PREFIX)
    map_config = configurator.read(prefix=config.MAP_CONFIG_PREFIX)
    template_source = template.TemplateSource(
        strl_config=strl_config, map_config=map_config
    )

    def test_search_fs(self):
        source = {
            "type": template.SourceType.FS.value,
            "path": "./tests/sql",
            "regex": ".*/*.sql",
        }
        result = []
        for file in self.template_source.search_fs(source=source):
            result.append(file)
        assert len(result) > 0

    def test_search_gcs(self):
        source = {
            "type": template.SourceType.GCS.value,
            "project": None,
            "bucket": "stairlight",
            "regex": "sql/.*/*.sql",
        }
        result = []
        for file in self.template_source.search_gcs(source=source):
            result.append(file)
        assert len(result) > 0

    def test_is_excluded(self):
        assert self.template_source.is_excluded(
            source_type=template.SourceType.FS,
            file_path="tests/sql/main/test_exclude.sql",
        )


@pytest.mark.parametrize(
    "source_type, file_path, params, bucket, mapped_table",
    [
        (
            template.SourceType.FS,
            "tests/sql/main/test_c.sql",
            {
                "main_table": "PROJECT_P.DATASET_Q.TABLE_R",
                "sub_table_01": "PROJECT_S.DATASET_T.TABLE_U",
                "sub_table_02": "PROJECT_V.DATASET_W.TABLE_X",
            },
            None,
            "PROJECT_J.DATASET_K.TABLE_L",
        ),
        (
            template.SourceType.GCS,
            "sql/test_b/test_b.sql",
            {
                "PROJECT": "PROJECT_g",
                "DATASET": "DATASET_h",
                "TABLE": "TABLE_i",
            },
            "stairlight",
            "PROJECT_d.DATASET_e.TABLE_f",
        ),
    ],
)
class TestSQLTemplateSuccess:
    configurator = config.Configurator("./config/")
    map_config = configurator.read(prefix=config.MAP_CONFIG_PREFIX)

    def test_get_param_list(self, source_type, file_path, params, bucket, mapped_table):
        sql_template = template.SQLTemplate(
            map_config=self.map_config,
            source_type=source_type,
            file_path=file_path,
            bucket=bucket,
        )
        assert sql_template.get_param_list() == [params]

    def test_search_mapped_table(
        self, source_type, file_path, params, bucket, mapped_table
    ):
        sql_template = template.SQLTemplate(
            map_config=self.map_config,
            source_type=source_type,
            file_path=file_path,
            bucket=bucket,
        )
        assert sql_template.search_mapped_table(params=params) == mapped_table

    def test_get_jinja_params(
        self, source_type, file_path, params, bucket, mapped_table
    ):
        sql_template = template.SQLTemplate(
            map_config=self.map_config,
            source_type=source_type,
            file_path=file_path,
            bucket=bucket,
        )
        assert len(sql_template.get_jinja_params()) > 0


class TestSQLTemplateRenderSuccess:
    configurator = config.Configurator("./config/")
    map_config = configurator.read(prefix=config.MAP_CONFIG_PREFIX)

    def test_render_fs(self):
        params = {
            "main_table": "PROJECT_P.DATASET_Q.TABLE_R",
            "sub_table_01": "PROJECT_S.DATASET_T.TABLE_U",
            "sub_table_02": "PROJECT_V.DATASET_W.TABLE_X",
        }
        sql_template = template.SQLTemplate(
            map_config=self.map_config,
            source_type=template.SourceType.FS,
            file_path="tests/sql/main/test_c.sql",
        )
        query_str = sql_template.render_fs(params=params)
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
