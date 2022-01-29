import pytest

from src.stairlight import config_key
import src.stairlight.config as config
from src.stairlight.source.gcs import (
    GcsTemplate,
    GcsTemplateSource,
    TemplateSourceType,
)


class TestGcsTemplateSource:
    configurator = config.Configurator(dir="./config")
    stairlight_config = configurator.read(
        prefix=config_key.STAIRLIGHT_CONFIG_FILE_PREFIX
    )
    mapping_config = configurator.read(prefix=config_key.MAPPING_CONFIG_FILE_PREFIX)

    source_attributes = {
        config_key.CONFIG_KEY_TEMPLATE_SOURCE_TYPE: TemplateSourceType.GCS.value,
        config_key.CONFIG_KEY_PROJECT_ID: None,
        config_key.CONFIG_KEY_BUCKET_NAME: "stairlight",
        config_key.CONFIG_KEY_REGEX: "sql/.*/*.sql",
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


@pytest.mark.parametrize(
    "key, bucket",
    [
        ("sql/test_b/test_b.sql", "stairlight"),
    ],
)
class TestSQLTemplateMapped:
    configurator = config.Configurator(dir="./config")
    mapping_config = configurator.read(prefix=config_key.MAPPING_CONFIG_FILE_PREFIX)

    def test_is_mapped(self, key, bucket):
        sql_template = GcsTemplate(
            mapping_config=self.mapping_config,
            source_type=TemplateSourceType.GCS,
            key=key,
            bucket=bucket,
        )
        assert sql_template.is_mapped()

    def test_get_jinja_params(self, key, bucket):
        sql_template = GcsTemplate(
            mapping_config=self.mapping_config,
            source_type=TemplateSourceType.GCS,
            key=key,
            bucket=bucket,
        )
        template_str = sql_template.get_template_str()
        assert len(sql_template.get_jinja_params(template_str)) > 0
