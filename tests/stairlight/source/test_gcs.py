import pytest

from src.stairlight import config_key
from src.stairlight.source.gcs import GcsTemplate, GcsTemplateSource, TemplateSourceType


class TestGcsTemplateSource:
    @pytest.fixture(scope="class")
    def gcs_template_source(self, stairlight_config, mapping_config):
        source_attributes = {
            config_key.TEMPLATE_SOURCE_TYPE: TemplateSourceType.GCS.value,
            config_key.PROJECT_ID: None,
            config_key.BUCKET_NAME: "stairlight",
            config_key.REGEX: "sql/.*/*.sql",
        }
        return GcsTemplateSource(
            stairlight_config=stairlight_config,
            mapping_config=mapping_config,
            source_attributes=source_attributes,
        )

    def test_search_templates_iter(self, gcs_template_source):
        result = []
        for file in gcs_template_source.search_templates_iter():
            result.append(file)
        assert len(result) > 0


@pytest.mark.parametrize(
    "bucket, key",
    [
        ("stairlight", "sql/cte/cte_multi_line.sql"),
    ],
)
class TestGcsTemplate:
    @pytest.fixture(scope="function")
    def gcs_template(self, mapping_config, bucket, key):
        return GcsTemplate(
            mapping_config=mapping_config,
            source_type=TemplateSourceType.GCS,
            bucket=bucket,
            key=key,
        )

    def test_is_mapped(self, gcs_template):
        assert gcs_template.is_mapped()

    def test_get_jinja_params(self, gcs_template):
        template_str = gcs_template.get_template_str()
        assert len(gcs_template.get_jinja_params(template_str)) > 0

    def test_get_uri(self, gcs_template):
        assert gcs_template.uri == "gs://stairlight/sql/cte/cte_multi_line.sql"

    def test_render(self, gcs_template):
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
        actual = gcs_template.render(params=params)
        assert actual == expected
