import src.stairlight.config as config
from src.stairlight import config_key
from src.stairlight.source.gcs import GcsTemplate, GcsTemplateSource, TemplateSourceType


class TestSQLTemplate:
    configurator = config.Configurator(dir="tests/config")
    mapping_config = configurator.read(prefix=config_key.MAPPING_CONFIG_FILE_PREFIX)
    sql_template = GcsTemplate(
        mapping_config=mapping_config,
        source_type=TemplateSourceType.GCS,
        key="sql/cte/cte_multi_line.sql",
        bucket="stairlight",
    )

    def test_is_mapped(self):
        assert self.sql_template.is_mapped()

    def test_get_jinja_params(self):
        template_str = self.sql_template.get_template_str()
        assert len(self.sql_template.get_jinja_params(template_str)) > 0

    def test_get_uri(self):
        assert self.sql_template.uri == "gs://stairlight/sql/cte/cte_multi_line.sql"

    def test_render(self):
        params = {
            "params": {
                "PROJECT": "PROJECT_g",
                "DATASET": "DATASET_h",
                "TABLE": "TABLE_i",
            }
        }
        expected = """WITH c AS (
    SELECT
        test_id,
        col_c
    FROM
        PROJECT_C.DATASET_C.TABLE_C
    WHERE
        0 = 0
),
d AS (
    SELECT
        test_id,
        col_d
    FROM
        PROJECT_d.DATASET_d.TABLE_d
    WHERE
        0 = 0
)

SELECT
    *
FROM
    PROJECT_g.DATASET_h.TABLE_i AS b
    INNER JOIN c
        ON b.test_id = c.test_id
    INNER JOIN d
        ON b.test_id = d.test_id
WHERE
    1 = 1"""
        actual = self.sql_template.render(params=params)
        assert actual == expected


class TestGcsTemplateSource:
    configurator = config.Configurator(dir="tests/config")
    stairlight_config = configurator.read(
        prefix=config_key.STAIRLIGHT_CONFIG_FILE_PREFIX
    )
    mapping_config = configurator.read(prefix=config_key.MAPPING_CONFIG_FILE_PREFIX)

    source_attributes = {
        config_key.TEMPLATE_SOURCE_TYPE: TemplateSourceType.GCS.value,
        config_key.PROJECT_ID: None,
        config_key.BUCKET_NAME: "stairlight",
        config_key.REGEX: "sql/.*/*.sql",
    }
    template_source = GcsTemplateSource(
        stairlight_config=stairlight_config,
        mapping_config=mapping_config,
        source_attributes=source_attributes,
    )

    def test_search_templates_iter(self):
        result = []
        for file in self.template_source.search_templates_iter():
            result.append(file)
        assert len(result) > 0
