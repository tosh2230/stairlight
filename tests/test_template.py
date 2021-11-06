import pytest

import stairlight.config as config
import stairlight.template as template


class TestTemplateSourceSuccess:
    configurator = config.Configurator("./config/")
    strl_config = configurator.read(config.STRL_CONFIG)
    map_config = configurator.read(config.MAP_CONFIG)
    template_source = template.TemplateSource(
        strl_config=strl_config, map_config=map_config
    )

    def test_search_fs(self):
        source = {
            "type": "fs",
            "path": "./tests/sql/main",
            "pattern": "*.sql",
        }
        result = []
        for file in self.template_source.search_fs(source=source):
            result.append(file)
        assert len(result) > 0

    def test_search_gcs(self):
        source = {
            "type": "gcs",
            "project": None,
            "bucket": "stairlight",
            "prefix": "sql/",
        }
        result = []
        for file in self.template_source.search_gcs(source=source):
            result.append(file)
        assert len(result) > 0

    def test_is_excluded(self):
        assert self.template_source.is_excluded("tests/sql/main/test_exclude.sql")


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
            "sql/test_b.sql",
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
    map_config = configurator.read(config.MAP_CONFIG)

    def test_get_param_list(self, source_type, file_path, params, bucket, mapped_table):
        sql_template = template.SQLTemplate(
            map_config=self.map_config, source_type=source_type, file_path=file_path
        )
        assert sql_template.get_param_list() == [params]

    def test_get_mapped_table(
        self, source_type, file_path, params, bucket, mapped_table
    ):
        sql_template = template.SQLTemplate(
            map_config=self.map_config, source_type=source_type, file_path=file_path
        )
        assert sql_template.get_mapped_table(params=params) == mapped_table

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
