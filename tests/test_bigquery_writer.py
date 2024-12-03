"""Test suite for the BigQuery writer."""

import io
import os

import pytest
import ujson
from dabapush.Record import Record
from google.api_core.exceptions import BadRequest

from dabapush_gbq import GBQWriterConfiguration

# pylint: disable=W0613, W0621, C0116


@pytest.fixture()
def mock_client():
    """Fixture for the Google BigQuery client."""

    def _mock_client_(
        load_table_from_file=None,
        from_service_account_json=None,
        create_table=None,
        get_table=None,
        schema_from_json=None,
    ):
        def return_self(*args, **kwargs):
            return args[0]

        def return_empty(*args, **kwargs):
            return {}

        return type(
            "MockClient",
            (object,),
            {
                "load_table_from_file": load_table_from_file or return_empty,
                "create_table": create_table or return_empty,
                "get_table": get_table or return_empty,
                "schema_from_json": schema_from_json or return_empty,
                "from_service_account_json": classmethod(
                    from_service_account_json or return_self
                ),
            },
        )

    yield _mock_client_


@pytest.fixture()
def configuration(monkeypatch):
    """Fixture for the BigQuery writer configuration."""

    yield GBQWriterConfiguration(
        name="test",
        project_name="test",
        dataset_name="test",
        table_name="test",
        auth_file="test",
        schema_file="test",
        chunk_size=1,
    )


@pytest.fixture()
def data():
    yield [Record(uuid=str(_), payload={"key": "😄😄😄😄"}) for _ in range(10)]


def test_big_query_writer(monkeypatch, configuration):
    """Should write data to BigQuery."""
    pytest.skip()


def test_big_query_writer_error_handling(monkeypatch, configuration, data, mock_client):
    """Should catch BadRequest exceptions."""

    def test_response(*args, **kwargs):
        raise BadRequest(
            "Bad request",
            errors=[
                "JSON parsing error in row starting at"
                "position 1503169: An internal error has occurred."
            ],
        )

    MockClient = mock_client(load_table_from_file=test_response)
    with monkeypatch.context() as m:
        m.setattr("google.cloud.bigquery.Client", MockClient)

        writer = configuration.get_instance()
        with pytest.raises(BadRequest) as exec_info:
            writer.write(data)

        assert "Bad request" in str(exec_info.value)


def test_big_query_writer_emits_utf8(monkeypatch, configuration, data, mock_client):
    """Should emit UTF-8 encoded data."""
    result = []

    def handle_write(
        buffer: io.BytesIO,
        *args,
        **kwargs,
    ):
        nonlocal result
        buffer.seek(0, os.SEEK_SET)  # rewind
        for line in buffer.readlines():
            result.append(line.decode("utf8"))

        return type("Result", (object,), {"result": lambda *args, **kwargs: None})

    MockClient = mock_client(load_table_from_file=handle_write)

    with monkeypatch.context() as m:
        m.setattr("google.cloud.bigquery.Client", MockClient)

        writer = configuration.get_instance()
        writer.write(data)
        for expected, actual in zip(data, result):
            assert ujson.loads(actual) == expected.payload
