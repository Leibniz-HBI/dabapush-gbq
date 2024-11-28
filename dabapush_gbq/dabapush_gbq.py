"""Google BigQuery writer for Dabapush.

This module provides the Google BigQuery writer for Dabapush. Configuration must contain
the following fields:
- project_name: The name of the project.
- dataset_name: The name of the dataset.
- table_name: The name of the table.
- auth_file: The path to the authentication file.
- schema_file: The path to the schema file.
"""

import io
from typing import Optional, override

import ujson
from dabapush.Configuration.WriterConfiguration import WriterConfiguration
from dabapush.Writer.Writer import Writer
from google.cloud import bigquery
from google.cloud.exceptions import BadRequest
from loguru import logger

# pylint: disable=I1101,R


class GBQWriterConfiguration(WriterConfiguration):
    """Configuration for the Google BigQuery writer.

    Attributes:
        project_name (str): The name of the project.
        dataset_name (str): The name of the dataset.
        table_name (str): The name of the table.
        auth_file (str): The path to the authentication file. Defaults to None. If None, the
            environment variable GOOGLE_APPLICATION_CREDENTIALS must be set.
        schema_file (str): The path to the schema file. Defaults to None.
    """

    yaml_tag = "!dabapush_gbq:GBQWriterConfiguration"

    def __init__(
        self,
        name: Optional[str],
        id: Optional[str] = None,  # pylint: disable=W0622
        chunk_size: Optional[int] = 2000,
        project_name: Optional[str] = None,
        dataset_name: Optional[str] = None,
        table_name: Optional[str] = None,
        auth_file: Optional[str] = None,
        schema_file: Optional[str] = None,
    ):
        super().__init__(name=name, id=id, chunk_size=chunk_size)
        self.project_name = project_name
        self.dataset_name = dataset_name
        self.table_name = table_name
        self.auth_file = auth_file
        self.schema_file = schema_file

    @override
    def get_instance(self):  # pylint: disable=W0221
        return GBQWriter(self)


class GBQWriter(Writer):
    """Writes data to Google BigQuery."""

    def __init__(self, config: GBQWriterConfiguration):
        super().__init__(config)
        self.config = config
        self.bigquery_client = (
            bigquery.Client.from_service_account_json(self.config.auth_file)
            if self.config.auth_file
            else bigquery.Client()  # This supposes that the environment variable, from which GBQ
            # client magics its auth credentials, is already set up.
        )
        self.job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            autodetect=False,
            ignore_unknown_values=True,
            schema=self.bigquery_client.schema_from_json(self.config.schema_file),
        )
        self.table = self.bigquery_client.get_table(
            f"{self.config.project_name}.{self.config.dataset_name}.{self.config.table_name}"
        )

    def _get_table(self, allow_create: bool = True):
        """Checks if the table exists in the GBQ project."""
        try:
            return self.bigquery_client.get_table(
                f"{self.config.project_name}.{self.config.dataset_name}.{self.config.table_name}"
            )
        except Exception as exception:  # pylint: disable=W0703
            if allow_create:
                return self.bigquery_client.create_table(
                    f"{self.config.project_name}."
                    f"{self.config.dataset_name}."
                    f"{self.config.table_name}"
                )
            else:
                raise exception

    def persist(self):
        """Persist the records to the destination."""
        table = self._get_table()
        write_buffer = io.BytesIO(initial_bytes=b"")
        for record in self.buffer:
            if len(record.payload) == 0:
                continue
            write_buffer.write(str.encode(ujson.dumps(record.payload) + "\n"))

        result = self.bigquery_client.load_table_from_file(
            write_buffer,
            job_config=self.job_config,
            destination=table,
            rewind=True,
        )
        try:
            result.result()
        except BadRequest as bad_request_error:
            logger.warning(
                f"Omitting {self.config.chunk_size} rows because {bad_request_error}."
            )
            return
        write_buffer.close()
