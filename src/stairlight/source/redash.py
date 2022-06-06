import os
from logging import getLogger
from typing import Iterator

from sqlalchemy import create_engine, text

from ..config import get_config_value
from ..key import MappingConfigKey, StairlightConfigKey
from .base import Template, TemplateSource, TemplateSourceType

logger = getLogger(__name__)


class RedashTemplate(Template):
    def __init__(
        self,
        mapping_config: dict,
        query_id: int,
        query_name: str,
        query_str: str = None,
        data_source_name: str = None,
    ):
        super().__init__(
            mapping_config=mapping_config,
            key=query_id,
            source_type=TemplateSourceType.REDASH,
        )
        self.query_id = query_id
        self.query_str = query_str
        self.uri = query_name
        self.data_source_name = data_source_name

    def find_mapped_table_attributes(self) -> Iterator[dict]:
        """Get mapped tables as iterator

        Yields:
            Iterator[dict]: Mapped table attributes
        """
        for mapping in self._mapping_config.get(MappingConfigKey.MAPPING_SECTION):
            if (
                mapping.get(MappingConfigKey.Redash.QUERY_ID) == self.query_id
                and mapping.get(MappingConfigKey.Redash.DATA_SOURCE_NAME)
                == self.data_source_name
            ):
                for table_attributes in mapping.get(MappingConfigKey.TABLES):
                    yield table_attributes
                break

    def get_template_str(self) -> str:
        """Get template string that read from Redash
        Returns:
            str: Template string
        """
        return self.query_str


class RedashTemplateSource(TemplateSource):
    def __init__(
        self, stairlight_config: dict, mapping_config: dict, source_attributes: dict
    ) -> None:
        super().__init__(
            stairlight_config=stairlight_config, mapping_config=mapping_config
        )
        self.source_type = TemplateSourceType.REDASH
        self.source_attributes = source_attributes
        self.where_clause = []
        self.conditions = self.make_conditions()

    def make_conditions(self) -> dict:
        data_source_name = get_config_value(
            key=StairlightConfigKey.Redash.DATA_SOURCE_NAME,
            target=self.source_attributes,
            fail_if_not_found=True,
            enable_logging=False,
        )
        query_ids = get_config_value(
            key=StairlightConfigKey.Redash.QUERY_IDS,
            target=self.source_attributes,
            fail_if_not_found=True,
            enable_logging=False,
        )
        return {
            StairlightConfigKey.Redash.DATA_SOURCE_NAME: {
                "key": StairlightConfigKey.Redash.DATA_SOURCE_NAME,
                "query": "data_sources.name = :data_source",
                "parameters": data_source_name,
            },
            StairlightConfigKey.Redash.QUERY_IDS: {
                "key": StairlightConfigKey.Redash.QUERY_IDS,
                "query": "queries.id IN :query_ids",
                "parameters": (tuple(query_ids) if query_ids else None),
            },
        }

    def search_templates(self) -> Iterator[Template]:
        results = self.get_queries_from_redash()
        for result in results:
            yield RedashTemplate(
                mapping_config=self._mapping_config,
                query_id=result[0],
                query_name=result[1],
                query_str=result[2],
                data_source_name=result[3],
            )

    def get_queries_from_redash(self) -> list:
        sql_file_name = "sql/redash_queries.sql"
        current_dir = os.path.dirname(os.path.abspath(__file__))
        query_text = text(
            self.build_query_string(path=f"{current_dir}/{sql_file_name}")
        )

        data_source_condition = self.conditions.get(
            StairlightConfigKey.Redash.DATA_SOURCE_NAME
        )
        query_ids_condition = self.conditions.get(StairlightConfigKey.Redash.QUERY_IDS)
        connection_str = self.get_connection_str()
        engine = create_engine(connection_str)
        queries = engine.execute(
            query_text,
            data_source=data_source_condition.get("parameters"),
            query_ids=query_ids_condition.get("parameters"),
        ).fetchall()

        return queries

    def build_query_string(self, path: str) -> str:
        base_query_string = self.read_query_from_file(path=path)
        for condition in self.conditions.values():
            if self.source_attributes.get(condition["key"]):
                self.where_clause.append(condition["query"])
        return base_query_string + "WHERE " + " AND ".join(self.where_clause)

    def read_query_from_file(self, path: str) -> str:
        with open(path, "r", encoding="utf-8") as f:
            return f.read()

    def get_connection_str(self) -> str:
        environment_variable_name = get_config_value(
            key=StairlightConfigKey.Redash.DATABASE_URL_ENV_VAR,
            target=self.source_attributes,
            fail_if_not_found=True,
            enable_logging=False,
        )
        connection_str = os.environ.get(environment_variable_name)
        if not connection_str:
            logger.error(f"{environment_variable_name} is not found.")
        return connection_str
