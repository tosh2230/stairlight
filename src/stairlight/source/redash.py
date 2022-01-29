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

    def search_templates_iter(self) -> Iterator[Template]:
        results = self.get_queries_from_redash()
        for result in results:
            yield RedashTemplate(
                mapping_config=self._mapping_config,
                query_id=result[0],
                query_name=result[1],
                query_str=result[2],
            )

    def get_queries_from_redash(self) -> list:
        connection_str = os.environ.get(
            self.source_attributes.get(config_key.DATABASE_URL_ENVIRONMENT_VARIABLE)
        )
        current_dir = os.path.dirname(os.path.abspath(__file__))
        query_text = text(
            self.read_query_from_file(path=f"{current_dir}/sql/redash_queries.sql")
        )
        engine = create_engine(connection_str)
        queries = engine.execute(
            query_text,
            data_source=self.source_attributes.get(config_key.DATA_SOURCE_NAME),
            query_id_list=tuple(self.source_attributes.get(config_key.QUERY_IDS)),
        ).fetchall()

        return queries

    def read_query_from_file(self, path) -> str:
        with open(path, "r", encoding="utf-8") as f:
            return f.read()
