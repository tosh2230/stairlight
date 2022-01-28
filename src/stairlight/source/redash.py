import os
from typing import Iterator, Optional

from sqlalchemy import create_engine, text

from .base import TemplateSourceType, Template, TemplateSource


class RedashTemplate(Template):
    def __init__(
        self,
        mapping_config: dict,
        source_type: TemplateSourceType,
        file_path: str,
        bucket: Optional[str] = None,
        project: Optional[str] = None,
        default_table_prefix: Optional[str] = None,
        labels: Optional[dict] = None,
        template_str: Optional[str] = None,
    ):
        super().__init__(
            mapping_config,
            source_type,
            file_path,
            bucket=bucket,
            project=project,
            default_table_prefix=default_table_prefix,
            labels=labels,
            template_str=template_str,
        )
        self.uri = self.get_uri()

    def get_template_str(self) -> str:
        """Get template string that read from Redash

        Returns:
            str: Template string
        """
        return self.template_str

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
        super().__init__(stairlight_config, mapping_config)
        self.source_type = TemplateSourceType.REDASH
        self.source_attributes = source_attributes

    def search_templates_iter(self) -> Iterator[Template]:
        connection_str = os.environ.get(self.source_attributes.get("conn_str_env_var"))
        engine = create_engine(connection_str)
        query_text = text(
            """\
            SELECT
                queries.query
            FROM
                queries
                INNER JOIN data_sources
                    ON queries.data_source_id = data_sources.id
            WHERE
                data_sources.name = %(data_source)s
                queries.id IN %(query_id_list)s
            """
        )
        template_str_list = engine.execute(
            object=query_text,
            data_source=self.source_attributes.get("data_source"),
            query_id_list=tuple(self.source_attributes.get("query_id_list")),
        ).fetchall()

        for template_str in template_str_list:
            yield RedashTemplate(
                mapping_config=self._mapping_config,
                source_type=self.source_type,
                file_path=None,
                template_str=template_str,
            )
