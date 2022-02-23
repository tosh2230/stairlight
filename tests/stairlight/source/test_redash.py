import os

import pytest

from src.stairlight import config_key
from src.stairlight.config import Configurator
from src.stairlight.source.redash import (
    RedashTemplate,
    RedashTemplateSource,
    TemplateSourceType,
)


@pytest.mark.parametrize(
    "env_key, path",
    [
        ("REDASH_DATABASE_URL", "src/stairlight/source/sql/redash_queries.sql"),
    ],
)
class TestRedashTemplateSource:
    @pytest.fixture(scope="function")
    def redash_template_source(
        self,
        configurator: Configurator,
        mapping_config: dict,
        env_key: str,
        path: str,
    ) -> RedashTemplateSource:
        stairlight_config = configurator.read(prefix="stairlight_redash")
        source_attributes = {
            config_key.TEMPLATE_SOURCE_TYPE: TemplateSourceType.REDASH.value,
            config_key.DATABASE_URL_ENVIRONMENT_VARIABLE: env_key,
            config_key.DATA_SOURCE_NAME: "metadata",
            config_key.QUERY_IDS: [1, 3, 5],
        }
        return RedashTemplateSource(
            stairlight_config=stairlight_config,
            mapping_config=mapping_config,
            source_attributes=source_attributes,
        )

    def test_build_query_string(
        self,
        redash_template_source: RedashTemplateSource,
        path: str,
    ):
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

        actual = redash_template_source.build_query_string(path=path)
        assert actual == expected

    def test_read_query_from_file(
        self,
        redash_template_source: RedashTemplateSource,
        path: str,
    ):
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

        actual = redash_template_source.read_query_from_file(path=path)
        assert actual == expected

    def test_get_connection_str(
        self,
        monkeypatch,
        redash_template_source: RedashTemplateSource,
        env_key: str,
    ):
        expected = "postgresql://postgres:testpassword@testhost/postgres"
        envs = {env_key: expected}
        monkeypatch.setattr(os, "environ", envs)
        actual = redash_template_source.get_connection_str()
        assert actual == expected


@pytest.mark.parametrize(
    (
        "query_id, query_name, query_str, "
        "data_source_name, params, mapped_table_attributes"
    ),
    [
        (
            5,
            "Copy of (#4) New Query",
            "SELECT * FROM {{ table }}",
            "metadata",
            {"table": "dashboards"},
            {
                "Labels": {"Category": "Redash test"},
                "Parameters": {"table": "dashboards"},
                "TableName": "Copy of (#4) New Query",
            },
        ),
    ],
)
class TestRedashTemplate:
    @pytest.fixture(scope="function")
    def redash_template(
        self,
        configurator: Configurator,
        query_id: int,
        query_name: str,
        query_str: str,
        data_source_name: str,
        params: dict,
        mapped_table_attributes: dict,
    ) -> RedashTemplate:
        mapping_config = configurator.read(prefix="mapping_redash")
        return RedashTemplate(
            mapping_config=mapping_config,
            query_id=query_id,
            query_name=query_name,
            query_str=query_str,
            data_source_name=data_source_name,
        )

    def test_get_mapped_table_attributes_iter(
        self,
        redash_template: RedashTemplate,
        mapped_table_attributes: dict,
    ):
        expected = mapped_table_attributes
        actual = {}
        for attribute in redash_template.get_mapped_table_attributes_iter():
            if attribute:
                actual = attribute
        assert actual == expected

    def test_get_template_str(self, redash_template: RedashTemplate):
        assert redash_template.get_template_str() == "SELECT * FROM {{ table }}"

    def test_render(self, redash_template, params: RedashTemplate):
        assert redash_template.render(params=params) == "SELECT * FROM dashboards"
