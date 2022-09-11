from __future__ import annotations

import re
from typing import Iterator

import boto3
from botocore.response import StreamingBody
from mypy_boto3_s3.service_resource import (
    Bucket,
    BucketObjectsCollection,
    ObjectSummary,
    S3ServiceResource,
)
from mypy_boto3_s3.type_defs import GetObjectOutputTypeDef

from ..config import ConfigAttributeNotFoundException, MappingConfig, StairlightConfig
from ..controller import S3_URI_SCHEME
from ..template import Template, TemplateSource, TemplateSourceType
from .config import StairlightConfigIncludeS3


class S3Template(Template):
    def __init__(
        self,
        mapping_config: MappingConfig,
        key: str,
        bucket: str | None = None,
        project: str | None = None,
        default_table_prefix: str | None = None,
    ):
        super().__init__(
            mapping_config=mapping_config,
            key=key,
            source_type=TemplateSourceType.S3,
            bucket=bucket,
            project=project,
            default_table_prefix=default_table_prefix,
        )
        self.uri: str = self.get_uri()
        self.s3: S3ServiceResource = boto3.resource("s3")

    def get_uri(self) -> str:
        """Get uri from file path

        Returns:
            str: uri
        """
        return f"{S3_URI_SCHEME}{self.bucket}/{self.key}"

    def get_template_str(self) -> str:
        """Get template string that read from a file in S3

        Returns:
            str: Template string
        """
        template_str: str = ""
        if not self.bucket:
            return template_str

        s3_bucket: Bucket = self.s3.Bucket(self.bucket)
        object_output: GetObjectOutputTypeDef = s3_bucket.Object(self.key).get()
        body: StreamingBody = object_output["Body"]
        if body:
            template_str = body.read().decode("utf-8")
        return template_str


class S3TemplateSource(TemplateSource):
    def __init__(
        self,
        stairlight_config: StairlightConfig,
        mapping_config: MappingConfig,
        include: StairlightConfigIncludeS3,
    ) -> None:
        super().__init__(
            stairlight_config=stairlight_config,
            mapping_config=mapping_config,
        )
        self._include = include
        self.s3: S3ServiceResource = boto3.resource("s3")

    def search_templates(self) -> Iterator[Template]:
        """Search SQL template files from S3

        Yields:
            Iterator[SQLTemplate]: SQL template file attributes
        """
        project = self._include.ProjectId
        bucket_name = self._include.BucketName

        if not bucket_name:
            raise ConfigAttributeNotFoundException(
                f"BucketName is not found. {self._include}"
            )

        objects: BucketObjectsCollection = self.s3.Bucket(bucket_name).objects.all()
        for obj in objects:
            if self.is_skipped(obj=obj):
                self.logger.debug(f"{obj.key} is skipped.")
                continue

            yield S3Template(
                mapping_config=self._mapping_config,
                key=obj.key,
                project=project,
                bucket=bucket_name,
                default_table_prefix=self._include.DefaultTablePrefix,
            )

    def is_skipped(self, obj: ObjectSummary) -> bool:
        """Check the target object is skipped or not

        Args:
            obj (ObjectSummary): Object

        Returns:
            bool: Is skipped or not
        """
        return not re.fullmatch(
            rf"{self._include.Regex}",
            obj.key,
        ) or self.is_excluded(
            source_type=TemplateSourceType(self._include.TemplateSourceType),
            key=obj.key,
        )
