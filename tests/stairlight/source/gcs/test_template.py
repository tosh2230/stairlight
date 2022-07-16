from collections import namedtuple
from typing import Any, Callable, Dict, List

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

MockGcsBlob = namedtuple("MockGcsBlob", ["name"])


@pytest.fixture(scope="session")
def gcs_blob_factory() -> Callable:
    def factory(name: str) -> MockGcsBlob:
        return MockGcsBlob(name=name)

    return factory


@pytest.fixture(scope="function")
def patch_list_blob(mocker, gcs_blob_factory) -> None:
    mocker.patch(
        "src.stairlight.source.gcs.template.storage.Client.list_blobs",
        return_value=[gcs_blob_factory(name="sql/cte/cte_multi_line.sql")],
    )


@pytest.mark.parametrize(
    "bucket, key, local_file_path, params, ignore_params, expected",
    [
        (
            "stairlight",
            "sql/cte/cte_multi_line.sql",
            "tests/sql/gcs/cte/cte_multi_line.sql",
            {
                "params": {
                    "PROJECT": "PROJECT_g",
                    "DATASET": "DATASET_h",
                    "TABLE": "TABLE_i",
                }
            },
            ["execution_date.add(days=1).isoformat()"],
            "PROJECT_g.DATASET_h.TABLE_i",
        ),
    ],
    ids=["stairlight/sql/cte/cte_multi_line.sql"],
)
class TestGcsTemplate:
    @pytest.fixture(scope="function")
    def gcs_template(
        self,
        mapping_config: MappingConfig,
        bucket: str,
        key: str,
        local_file_path: str,
        params: Dict[str, Any],
        ignore_params: List[str],
        expected: str,
    ) -> GcsTemplate:
        return GcsTemplate(mapping_config=mapping_config, bucket=bucket, key=key)

    def test_is_mapped(self, gcs_template: GcsTemplate):
        assert gcs_template.is_mapped()

    def test_get_uri(
        self,
        gcs_template: GcsTemplate,
        bucket: str,
        key: str,
    ):
        assert gcs_template.uri == f"{GCS_URI_SCHEME}{bucket}/{key}"

    def test_detect_jinja_params(
        self, mocker, gcs_template: GcsTemplate, local_file_path: str
    ):
        with open(local_file_path, "r") as test_file:
            test_file_str = test_file.read()
        mocker.patch(
            "src.stairlight.source.gcs.template.storage.Blob.download_as_bytes",
            return_value=test_file_str.encode(),
        )
        template_str = gcs_template.get_template_str()
        assert len(gcs_template.detect_jinja_params(template_str)) > 0

    @pytest.mark.integration
    def test_detect_jinja_params_integration(self, gcs_template: GcsTemplate):
        template_str = gcs_template.get_template_str()
        assert len(gcs_template.detect_jinja_params(template_str)) > 0

    def test_render(
        self,
        mocker,
        gcs_template: GcsTemplate,
        params: Dict[str, Any],
        ignore_params: List[str],
        expected: str,
    ):
        mocker.patch(
            "src.stairlight.source.gcs.template.storage.Blob.download_as_bytes",
            return_value=expected.encode(),
        )
        actual = gcs_template.render(params=params, ignore_params=ignore_params)
        assert expected in actual

    @pytest.mark.integration
    def test_render_integration(
        self,
        gcs_template: GcsTemplate,
        params: Dict[str, Any],
        ignore_params: List[str],
        expected: str,
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

    def test_search_templates(
        self, gcs_template_source: GcsTemplateSource, patch_list_blob: Callable
    ):
        result = []
        for file in gcs_template_source.search_templates():
            result.append(file)
        assert len(result) > 0

    @pytest.mark.integration
    def test_search_templates_integration(self, gcs_template_source: GcsTemplateSource):
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
