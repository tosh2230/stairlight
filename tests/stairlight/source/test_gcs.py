import pytest

from src.stairlight.key import StairlightConfigKey
from src.stairlight.source.gcs import (
    GCS_URI_SCHEME,
    GcsTemplate,
    GcsTemplateSource,
    TemplateSourceType,
)


class TestGcsTemplateSource:
    @pytest.fixture(scope="class")
    def gcs_template_source(
        self,
        stairlight_config: dict,
        mapping_config: dict,
    ) -> GcsTemplateSource:
        source_attributes = {
            StairlightConfigKey.TEMPLATE_SOURCE_TYPE: TemplateSourceType.GCS.value,
            StairlightConfigKey.Gcs.PROJECT_ID: None,
            StairlightConfigKey.Gcs.BUCKET_NAME: "stairlight",
            StairlightConfigKey.REGEX: "sql/.*/*.sql",
        }
        return GcsTemplateSource(
            stairlight_config=stairlight_config,
            mapping_config=mapping_config,
            source_attributes=source_attributes,
        )

    def test_search_templates(self, gcs_template_source: GcsTemplateSource):
        result = []
        for file in gcs_template_source.search_templates():
            result.append(file)
        assert len(result) > 0


@pytest.mark.parametrize(
    "bucket, key, params, expected, ignore_params",
    [
        (
            "stairlight",
            "sql/cte/cte_multi_line.sql",
            {
                "params": {
                    "PROJECT": "PROJECT_g",
                    "DATASET": "DATASET_h",
                    "TABLE": "TABLE_i",
                }
            },
            "PROJECT_g.DATASET_h.TABLE_i",
            ["execution_date.add(days=1).isoformat()"],
        ),
    ],
)
class TestGcsTemplate:
    @pytest.fixture(scope="function")
    def gcs_template(
        self,
        mapping_config: dict,
        bucket: str,
        key: str,
        params: dict,
        expected: str,
        ignore_params: "list[str]",
    ) -> GcsTemplate:
        return GcsTemplate(
            mapping_config=mapping_config,
            bucket=bucket,
            key=key,
        )

    def test_is_mapped(
        self,
        gcs_template: GcsTemplate,
    ):
        assert gcs_template.is_mapped()

    def test_detect_jinja_params(
        self,
        gcs_template: GcsTemplate,
    ):
        template_str = gcs_template.get_template_str()
        assert len(gcs_template.detect_jinja_params(template_str)) > 0

    def test_get_uri(
        self,
        gcs_template: GcsTemplate,
        bucket: str,
        key: str,
    ):
        assert gcs_template.uri == f"{GCS_URI_SCHEME}{bucket}/{key}"

    def test_render(
        self,
        gcs_template: GcsTemplate,
        params: dict,
        expected: str,
        ignore_params: "list[str]",
    ):
        actual = gcs_template.render(params=params, ignore_params=ignore_params)
        assert expected in actual
