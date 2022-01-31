import os
from re import M

from src.stairlight import config_key
import src.stairlight.config as config
from src.stairlight.source.redash import (
    RedashTemplate,
    RedashTemplateSource,
    TemplateSourceType,
)


class TestRedashTemplate:
    configurator = config.Configurator(dir="./config")
    mapping_config = configurator.read(prefix="mapping_redash")
    sql_template = RedashTemplate(
        mapping_config=mapping_config,
        query_id=5,
        query_name="Copy of (#4) New Query",
        query_str="SELECT * FROM {{ table }}",
        data_source_name="metadata",
    )

    def test_get_mapped_table_attributes_iter(self):
        expected = {
            "Labels": {"Category": "Redash test"},
            "Parameters": {"table": "dashboards"},
            "TableName": "Copy of (#4) New Query",
        }
        actual = {}
        for attribute in self.sql_template.get_mapped_table_attributes_iter():
            if attribute:
                actual = attribute
        assert actual == expected

    def test_get_template_str(self):
        assert self.sql_template.get_template_str() == "SELECT * FROM {{ table }}"

    def test_render(self):
        params = {"table": "dashboards"}
        assert self.sql_template.render(params=params) == "SELECT * FROM dashboards"


class TestRedashTemplateSource:
    configurator = config.Configurator(dir="./config")
    stairlight_config = configurator.read(prefix="stairlight_redash")
    mapping_config = configurator.read(prefix="mapping_redash")
    env_key = "REDASH_DATABASE_URL"
    source_attributes = {
        config_key.TEMPLATE_SOURCE_TYPE: TemplateSourceType.REDASH.value,
        config_key.DATABASE_URL_ENVIRONMENT_VARIABLE: env_key,
        config_key.DATA_SOURCE_NAME: "metadata",
        config_key.QUERY_IDS: [1, 3, 5],
    }
    template_source = RedashTemplateSource(
        stairlight_config=stairlight_config,
        mapping_config=mapping_config,
        source_attributes=source_attributes,
    )
    sql_file_path = "./src/stairlight/source/sql/redash_queries.sql"

    def test_build_query_string(self):
        expected = """SELECT
    queries.id,
    queries.name,
    queries.query,
    data_sources.name
FROM
    queries
    INNER JOIN data_sources
        ON queries.data_source_id = data_sources.id
WHERE data_sources.name = :data_source AND queries.id IN :query_ids"""

        actual = self.template_source.build_query_string(self.sql_file_path)
        assert actual == expected

    def test_read_query_from_file(self):
        expected = """SELECT
    queries.id,
    queries.name,
    queries.query,
    data_sources.name
FROM
    queries
    INNER JOIN data_sources
        ON queries.data_source_id = data_sources.id
"""

        actual = self.template_source.read_query_from_file(self.sql_file_path)
        assert actual == expected

    def test_get_connection_str(self, monkeypatch):
        expected = "postgresql://postgres:testpassword@testhost/postgres"
        envs = {self.env_key: expected}
        monkeypatch.setattr(os, "environ", envs)
        actual = self.template_source.get_connection_str()
        assert actual == expected
