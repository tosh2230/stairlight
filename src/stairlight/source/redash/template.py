from __future__ import annotations

import os
from dataclasses import asdict
from logging import getLogger
from typing import Any, Iterator

from sqlalchemy import create_engine, text
from sqlalchemy.engine.row import Row

from ..config import MappingConfig, MappingConfigMappingTable, StairlightConfig
from ..config_key import StairlightConfigKey
from ..template import Template, TemplateSource, TemplateSourceType
from .config import StairlightConfigIncludeRedash

logger = getLogger(__name__)


class RedashTemplate(Template):
    def __init__(
        self,
        mapping_config: MappingConfig,
        query_id: int,
        query_name: str,
        query_str: str = "",
        data_source_name: str = "",
    ):
        super().__init__(
            mapping_config=mapping_config,
            key=str(query_id),
            source_type=TemplateSourceType.REDASH,
        )
        self.query_id = query_id
        self.query_str = query_str
        self.uri = query_name
        self.data_source_name = data_source_name

    def find_mapped_table_attributes(self) -> Iterator[MappingConfigMappingTable]:
        """Get mapped tables as iterator

        Yields:
            Iterator[dict]: Mapped table attributes
        """
        mapping: Any
        for mapping in self._mapping_config.get_mapping():
            if mapping.TemplateSourceType != self.source_type.value:
                continue

            if (
                mapping.DataSourceName == self.data_source_name
                and mapping.QueryId == self.query_id
            ):
                for table_attributes in mapping.get_table():
                    yield table_attributes
                break

    def get_template_str(self) -> str:
        """Get template string that read from Redash
        Returns:
            str: Template string
        """
        return self.query_str

    def get_uri(self) -> str:
        """Get uri"""
        return super().get_uri()


class RedashTemplateSource(TemplateSource):
    REDASH_QUERIES = "sql/redash_queries.sql"
    WHERE_CLAUSE_TEMPLATES = {
        StairlightConfigKey.Redash.DATA_SOURCE_NAME: "data_sources.name = :data_source",
        StairlightConfigKey.Redash.QUERY_IDS: "queries.id IN :query_ids",
    }

    def __init__(
        self,
        stairlight_config: StairlightConfig,
        mapping_config: MappingConfig,
        include: StairlightConfigIncludeRedash,
    ) -> None:
        super().__init__(
            stairlight_config=stairlight_config,
            mapping_config=mapping_config,
        )
        self._include = include
        self.where_clause: list[str] = []
        self.conditions: dict[str, Any] = {}

    def search_templates(self) -> Iterator[Template]:
        """Search query template files

        Yields:
            Iterator[Template]: Attributes of query template files
        """
        results = self.get_redash_queries()
        for result in results:
            # see columns "src/stairlight/source/redash/sql/redash_queries.sql"
            yield RedashTemplate(
                mapping_config=self._mapping_config,
                query_id=result[0],
                query_name=result[1],
                query_str=result[2],
                data_source_name=result[3],
            )

    def get_redash_queries(self) -> list[Row]:
        """Get Redash queries

        Returns:
            list[Row]: Queries
        """
        current_dir = os.path.dirname(os.path.abspath(__file__))
        query_text = self.build_query_string(
            path=f"{current_dir}/{self.REDASH_QUERIES}"
        )
        data_source = self._include.DataSourceName
        query_ids = tuple(self._include.QueryIds) if self._include.QueryIds else None

        connection_str = self.get_connection_str()
        engine = create_engine(connection_str)
        queries = engine.execute(
            text(query_text),
            data_source=data_source,
            query_ids=query_ids,
        )
        return queries.fetchall()

    def build_query_string(self, path: str) -> str:
        """Build a query string

        Args:
            path (str): Path

        Returns:
            str: Query string
        """
        where_clauses: list[str] = []
        for key, value in self.WHERE_CLAUSE_TEMPLATES.items():
            if key in asdict(self._include).keys():
                where_clauses.append(value)

        base_query_string = self.read_query_string(path=path)
        return base_query_string + "WHERE " + " AND ".join(where_clauses)

    def read_query_string(self, path: str) -> str:
        """Read a query string

        Args:
            path (str): Path

        Returns:
            str: Query string
        """
        with open(path, "r", encoding="utf-8") as f:
            return f.read()

    def get_connection_str(self) -> str:
        """Get a database connection string

        Returns:
            str: Connection string
        """
        environment_variable_name = self._include.DatabaseUrlEnvironmentVariable
        connection_str = os.environ.get(environment_variable_name, "")
        if not connection_str:
            logger.error(f"{environment_variable_name} is not found.")
        return connection_str
