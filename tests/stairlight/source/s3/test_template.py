from typing import Any, Dict, List

import pytest

from src.stairlight.configurator import Configurator
from src.stairlight.source.config import (
    ConfigAttributeNotFoundException,
    MappingConfig,
    StairlightConfig,
)
from src.stairlight.source.config_key import StairlightConfigKey
from src.stairlight.source.s3.config import StairlightConfigIncludeS3
from src.stairlight.source.s3.template import (
    S3_URI_SCHEME,
    S3Template,
    S3TemplateSource,
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
class TestS3Template:
    @pytest.fixture(scope="function")
    def s3_template(
        self,
        mapping_config: MappingConfig,
        bucket: str,
        key: str,
        params: Dict[str, Any],
        expected: str,
        ignore_params: List[str],
    ) -> S3Template:
        return S3Template(
            mapping_config=mapping_config,
            bucket=bucket,
            key=key,
        )

    def test_is_mapped(
        self,
        s3_template: S3Template,
    ):
        assert s3_template.is_mapped()

    def test_detect_jinja_params(
        self,
        s3_template: S3Template,
    ):
        template_str = s3_template.get_template_str()
        assert len(s3_template.detect_jinja_params(template_str)) > 0

    def test_get_uri(
        self,
        s3_template: S3Template,
        bucket: str,
        key: str,
    ):
        assert s3_template.uri == f"{S3_URI_SCHEME}{bucket}/{key}"

    def test_render(
        self,
        s3_template: S3Template,
        params: Dict[str, Any],
        expected: str,
        ignore_params: List[str],
    ):
        actual = s3_template.render(params=params, ignore_params=ignore_params)
        assert expected in actual


class TestS3TemplateSource:
    @pytest.fixture(scope="class")
    def s3_template_source(
        self,
        stairlight_config: StairlightConfig,
        mapping_config: MappingConfig,
    ) -> S3TemplateSource:
        _include = StairlightConfigIncludeS3(
            **{
                StairlightConfigKey.TEMPLATE_SOURCE_TYPE: TemplateSourceType.S3.value,
                StairlightConfigKey.S3.BUCKET_NAME: "stairlight",
                StairlightConfigKey.REGEX: "sql/.*/*.sql",
            }
        )
        return S3TemplateSource(
            stairlight_config=stairlight_config,
            mapping_config=mapping_config,
            include=_include,
        )

    def test_search_templates(self, s3_template_source: S3TemplateSource):
        result = []
        for file in s3_template_source.search_templates():
            result.append(file)
        assert len(result) > 0


class TestS3TemplateSourceConfigKeyNotFound:
    @pytest.fixture(scope="class")
    def s3_template_source(
        self,
        configurator: Configurator,
        mapping_config: MappingConfig,
    ) -> S3TemplateSource:
        stairlight_config = configurator.read_stairlight(
            prefix="stairlight_key_not_found"
        )
        _include = StairlightConfigIncludeS3(
            **{
                StairlightConfigKey.TEMPLATE_SOURCE_TYPE: TemplateSourceType.S3.value,
                StairlightConfigKey.REGEX: ".*/*.sql",
            }
        )
        return S3TemplateSource(
            stairlight_config=stairlight_config,
            mapping_config=mapping_config,
            include=_include,
        )

    def test_search_templates(
        self,
        s3_template_source: S3TemplateSource,
    ):
        iter = s3_template_source.search_templates()
        with pytest.raises(ConfigAttributeNotFoundException) as exception:
            next(iter)
        assert exception.value.args[0] == (
            f"BucketName is not found. {s3_template_source._include}"
        )
