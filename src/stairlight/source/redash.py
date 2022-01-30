from io import open_code
import os
from typing import Iterator, Optional

from sqlalchemy import create_engine, text

from .base import Template, TemplateSource, TemplateSourceType
from .. import config_key


class RedashTemplate(Template):
    def __init__(
        self,
        mapping_config: dict,
        query_id: str,
        query_name: str,
        query_str: str = None,
        data_source_name: str = None,
        source_type: Optional[TemplateSourceType] = TemplateSourceType.REDASH,
        key: Optional[str] = None,
        bucket: Optional[str] = None,
        project: Optional[str] = None,
        default_table_prefix: Optional[str] = None,
    ):
        super().__init__(
            mapping_config=mapping_config,
            key=key,
            source_type=source_type,
            bucket=bucket,
            project=project,
            default_table_prefix=default_table_prefix,
        )
        self.query_id = query_id
        self.query_str = query_str
        self.uri = query_name
        self.data_source_name = data_source_name

    def get_template_str(self) -> str:
        """Get template string that read from Redash
        Returns:
            str: Template string
        """
        return self.query_str

    def render(self, params: dict) -> str:
        """Render SQL query string from a jinja template on Redash queries
        Args:
            params (dict): Jinja paramters
        Returns:
            str: SQL query string
        """
        template_str = self.get_template_str()
        return self.render_by_base_loader(template_str=template_str, params=params)


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
        self.conditions = {
            "data_source": {
                "key": config_key.DATA_SOURCE_NAME,
                "query": "data_sources.name = :data_source",
                "parameters": self.source_attributes.get(config_key.DATA_SOURCE_NAME),
            },
            "query_ids": {
                "key": config_key.QUERY_IDS,
                "query": "queries.id IN :query_ids",
                "parameters": (
                    tuple(self.source_attributes.get(config_key.QUERY_IDS))
                    if self.source_attributes.get(config_key.QUERY_IDS)
                    else None
                ),
            },
        }

    def search_templates_iter(self) -> Iterator[Template]:
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

        connection_str = os.environ.get(
            self.source_attributes.get(config_key.DATABASE_URL_ENVIRONMENT_VARIABLE)
        )
        engine = create_engine(connection_str)
        queries = engine.execute(
            query_text,
            data_source=self.conditions.get("data_source").get("parameters"),
            query_ids=self.conditions.get("query_ids").get("parameters"),
        ).fetchall()

        print(queries)

        return queries

    def build_query_string(self, path) -> str:
        base_query_string = self.read_query_from_file(path=path)
        for condition in self.conditions.values():
            self.update_where_clause(key=condition["key"], condition=condition["query"])
        return base_query_string + "WHERE " + " AND ".join(self.where_clause)

    def read_query_from_file(self, path) -> str:
        with open(path, "r", encoding="utf-8") as f:
            return f.read()

    def update_where_clause(self, key, condition) -> None:
        if self.source_attributes.get(key):
            self.where_clause.append(condition)
