from typing import Any, Dict, List

import pytest

from src.stairlight.configurator import Configurator
from src.stairlight.source.config import (
    ConfigAttributeNotFoundException,
    MappingConfig,
    StairlightConfig,
)
from src.stairlight.source.config_key import StairlightConfigKey
from src.stairlight.source.gcs.config import StairlightConfigIncludeGcs
from src.stairlight.source.gcs.template import (
    GCS_URI_SCHEME,
    GcsTemplate,
    GcsTemplateSource,
    TemplateSourceType,
)


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
        mapping_config: MappingConfig,
        bucket: str,
        key: str,
        params: Dict[str, Any],
        expected: str,
        ignore_params: List[str],
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
        params: Dict[str, Any],
        expected: str,
        ignore_params: List[str],
    ):
        actual = gcs_template.render(params=params, ignore_params=ignore_params)
        assert expected in actual


class TestGcsTemplateSource:
    @pytest.fixture(scope="class")
    def gcs_template_source(
        self,
        stairlight_config: StairlightConfig,
        mapping_config: MappingConfig,
    ) -> GcsTemplateSource:
        _include = StairlightConfigIncludeGcs(
            **{
                StairlightConfigKey.TEMPLATE_SOURCE_TYPE: TemplateSourceType.GCS.value,
                StairlightConfigKey.Gcs.PROJECT_ID: None,
                StairlightConfigKey.Gcs.BUCKET_NAME: "stairlight",
                StairlightConfigKey.REGEX: "sql/.*/*.sql",
            }
        )
        return GcsTemplateSource(
            stairlight_config=stairlight_config,
            mapping_config=mapping_config,
            include=_include,
        )

    def test_search_templates(self, gcs_template_source: GcsTemplateSource):
        result = []
        for file in gcs_template_source.search_templates():
            result.append(file)
        assert len(result) > 0


class TestGcsTemplateSourceConfigKeyNotFound:
    @pytest.fixture(scope="class")
    def gcs_template_source(
        self,
        configurator: Configurator,
        mapping_config: MappingConfig,
    ) -> GcsTemplateSource:
        stairlight_config = configurator.read_stairlight(
            prefix="stairlight_key_not_found"
        )
        _include = StairlightConfigIncludeGcs(
            **{
                StairlightConfigKey.TEMPLATE_SOURCE_TYPE: TemplateSourceType.GCS.value,
                StairlightConfigKey.REGEX: ".*/*.sql",
            }
        )
        return GcsTemplateSource(
            stairlight_config=stairlight_config,
            mapping_config=mapping_config,
            include=_include,
        )

    def test_search_templates(
        self,
        gcs_template_source: GcsTemplateSource,
    ):
        iter = gcs_template_source.search_templates()
        with pytest.raises(ConfigAttributeNotFoundException) as exception:
            next(iter)
        assert exception.value.args[0] == (
            f"BucketName is not found. {gcs_template_source._include}"
        )
