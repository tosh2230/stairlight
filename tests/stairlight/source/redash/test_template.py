from __future__ import annotations

import os
from collections import OrderedDict
from typing import Any

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
from src.stairlight.source.template import Template


@pytest.mark.parametrize(
    (
        "query_id",
        "query_name",
        "query_str",
        "data_source_name",
        "params",
        "mapped_table_attributes",
    ),
    [
        (
            5,
            "Copy of (#4) New Query",
            "SELECT * FROM {{ table }}",
            "metadata",
            {"table": "dashboards"},
            MappingConfigMappingTable(
                TableName="Copy of (#4) New Query",
                Parameters=OrderedDict({"table": "dashboards"}),
                Labels={"Category": "Redash test"},
            ),
        ),
    ],
    ids=[5],
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
        params: dict[str, Any],
        mapped_table_attributes: dict[str, Any],
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


@pytest.mark.parametrize(
    ("env_key", "path", "expected_conn_str"),
    [
        (
            "REDASH_DATABASE_URL",
            "src/stairlight/source/redash/sql/redash_queries.sql",
            "postgresql://postgres:testpassword@testhost/postgres",
        ),
    ],
    ids=["queries"],
)
class TestRedashTemplateSource:
    @pytest.fixture(scope="function")
    def redash_template_source(
        self,
        configurator: Configurator,
        mapping_config: MappingConfig,
        env_key: str,
        path: str,
        expected_conn_str: str,
    ) -> RedashTemplateSource:
        stairlight_config = configurator.read_stairlight(prefix="stairlight_redash")
        _include = StairlightConfigIncludeRedash(
            TemplateSourceType=TemplateSourceType.REDASH.value,
            DatabaseUrlEnvironmentVariable=env_key,
            DataSourceName="metadata",
            QueryIds=[1, 3, 5],
        )
        return RedashTemplateSource(
            stairlight_config=stairlight_config,
            mapping_config=mapping_config,
            include=_include,
        )

    def test_search_templates(
        self,
        mocker,
        redash_template_source: RedashTemplateSource,
    ):
        mocker.patch(
            (
                "src.stairlight.source.redash.template"
                ".RedashTemplateSource.get_redash_queries"
            ),
            return_value=[
                ["test_id", "test_name", "test_str", "test_data_source_name"]
            ],
        )
        templates: list[Template] = []
        for template in redash_template_source.search_templates():
            templates.append(template)
        assert len(templates) > 0

    def test_get_redash_queries(
        self,
        mocker,
        redash_template_source: RedashTemplateSource,
    ):
        mocker.patch("src.stairlight.source.redash.template.create_engine")
        _ = redash_template_source.get_redash_queries()

    def test_build_query_string_data_source(
        self,
        redash_template_source: RedashTemplateSource,
        path: str,
    ):
        expected = redash_template_source.WHERE_CLAUSE_TEMPLATES[
            SlKey.Redash.DATA_SOURCE_NAME
        ]
        actual = redash_template_source.build_query_string(path=path)
        assert expected in actual

    def test_build_query_string_query_ids(
        self,
        redash_template_source: RedashTemplateSource,
        path: str,
    ):
        expected = redash_template_source.WHERE_CLAUSE_TEMPLATES[SlKey.Redash.QUERY_IDS]
        actual = redash_template_source.build_query_string(path=path)
        assert expected in actual

    def test_get_connection_str(
        self,
        monkeypatch,
        redash_template_source: RedashTemplateSource,
        env_key: str,
        expected_conn_str: str,
    ):
        monkeypatch.setattr(os, "environ", {env_key: expected_conn_str})
        actual = redash_template_source.get_connection_str()
        assert actual == expected_conn_str


class TestRedashTemplateSourceConfigKeyNotFound:
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
            TemplateSourceType=TemplateSourceType.REDASH.value,
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
