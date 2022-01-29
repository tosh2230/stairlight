import pytest

import src.stairlight.config as config
from src.stairlight.source.gcs import (
    GcsTemplate,
    GcsTemplateSource,
    TemplateSourceType,
)


class TestGcsTemplateSourceSuccess:
    configurator = config.Configurator(dir="./config")
    stairlight_config = configurator.read(prefix=config.STAIRLIGHT_CONFIG_PREFIX)
    mapping_config = configurator.read(prefix=config.MAPPING_CONFIG_PREFIX)

    source_attributes = {
        "type": TemplateSourceType.GCS.value,
        "project": None,
        "bucket": "stairlight",
        "regex": "sql/.*/*.sql",
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
    "file_path, bucket",
    [
        ("sql/test_b/test_b.sql", "stairlight"),
    ],
)
class TestSQLTemplateMapped:
    configurator = config.Configurator(dir="./config")
    mapping_config = configurator.read(prefix=config.MAPPING_CONFIG_PREFIX)

    def test_is_mapped(self, file_path, bucket):
        sql_template = GcsTemplate(
            mapping_config=self.mapping_config,
            source_type=TemplateSourceType.GCS,
            file_path=file_path,
            bucket=bucket,
        )
        assert sql_template.is_mapped()

    def test_get_jinja_params(self, file_path, bucket):
        sql_template = GcsTemplate(
            mapping_config=self.mapping_config,
            source_type=TemplateSourceType.GCS,
            file_path=file_path,
            bucket=bucket,
        )
        template_str = sql_template.get_template_str()
        assert len(sql_template.get_jinja_params(template_str)) > 0
