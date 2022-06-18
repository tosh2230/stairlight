import os
from typing import Any, Dict

import pytest
from sqlalchemy.exc import ArgumentError

from src.stairlight.configurator import Configurator
from src.stairlight.source.config import MappingConfig, MappingConfigMappingTable
from src.stairlight.source.config_key import StairlightConfigKey as SlKey
from src.stairlight.source.redash.config import StairlightConfigIncludeRedash
from src.stairlight.source.redash.template import (
    RedashTemplate,
    RedashTemplateSource,
    TemplateSourceType,
)


@pytest.mark.parametrize(
    "env_key, path",
    [
        ("REDASH_DATABASE_URL", "src/stairlight/source/redash/sql/redash_queries.sql"),
    ],
)
class TestRedashTemplateSource:
    @pytest.fixture(scope="function")
    def redash_template_source(
        self,
        configurator: Configurator,
        mapping_config: MappingConfig,
        env_key: str,
        path: str,
    ) -> RedashTemplateSource:
        stairlight_config = configurator.read_stairlight(prefix="stairlight_redash")
        _include = StairlightConfigIncludeRedash(
            **{
                SlKey.TEMPLATE_SOURCE_TYPE: TemplateSourceType.REDASH.value,
                SlKey.Redash.DATABASE_URL_ENV_VAR: env_key,
                SlKey.Redash.DATA_SOURCE_NAME: "metadata",
                SlKey.Redash.QUERY_IDS: [1, 3, 5],
            }
        )
        return RedashTemplateSource(
            stairlight_config=stairlight_config,
            mapping_config=mapping_config,
            include=_include,
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
            MappingConfigMappingTable(
                **{
                    "TableName": "Copy of (#4) New Query",
                    "Parameters": {"table": "dashboards"},
                    "Labels": {"Category": "Redash test"},
                }
            ),
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
        params: Dict[str, Any],
        mapped_table_attributes: Dict[str, Any],
    ) -> RedashTemplate:
        mapping_config = configurator.read_mapping(prefix="mapping_redash")
        return RedashTemplate(
            mapping_config=mapping_config,
            query_id=query_id,
            query_name=query_name,
            query_str=query_str,
            data_source_name=data_source_name,
        )

    def test_find_mapped_table_attributes(
        self,
        redash_template: RedashTemplate,
        mapped_table_attributes: MappingConfigMappingTable,
    ):
        expected = mapped_table_attributes
        actual: MappingConfigMappingTable
        for attribute in redash_template.find_mapped_table_attributes():
            actual = attribute
            break
        assert actual == expected

    def test_get_template_str(self, redash_template: RedashTemplate):
        assert redash_template.get_template_str() == "SELECT * FROM {{ table }}"

    def test_render(self, redash_template, params: RedashTemplate):
        assert redash_template.render(params=params) == "SELECT * FROM dashboards"


class TestRedashConfigKeyNotFound:
    @pytest.fixture(scope="class")
    def redash_template_source(
        self,
        configurator: Configurator,
        mapping_config: MappingConfig,
    ) -> RedashTemplateSource:
        stairlight_config = configurator.read_stairlight(
            prefix="stairlight_key_not_found"
        )
        _include = StairlightConfigIncludeRedash(
            **{
                SlKey.TEMPLATE_SOURCE_TYPE: TemplateSourceType.REDASH.value,
            }
        )
        return RedashTemplateSource(
            stairlight_config=stairlight_config,
            mapping_config=mapping_config,
            include=_include,
        )

    def test_search_templates(
        self,
        redash_template_source: RedashTemplateSource,
    ):
        iter = redash_template_source.search_templates()
        with pytest.raises(ArgumentError) as exception:
            next(iter)
        assert exception
