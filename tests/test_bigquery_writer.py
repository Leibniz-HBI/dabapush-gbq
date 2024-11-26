"""Test suite for the BigQuery writer."""

import pytest
from dabapush.Record import Record
from google.api_core.exceptions import BadRequest

from dabapush_gbq import GBQWriterConfiguration

# pylint: disable=W0613, W0621, C0116


@pytest.fixture()
def configuration(tmp_path, monkeypatch):
    """Fixture for the BigQuery writer configuration."""

    class MockClient:
        """Mock Google BigQuery client."""

        def load_table_from_file(self, *args, **kwargs):
            raise BadRequest(
                "Bad request",
                errors=[
                    "JSON parsing error in row starting at"
                    "position 1503169: An internal error has occurred."
                ],
            )

        @classmethod
        def from_service_account_json(cls, *args, **kwargs):
            return cls()

        def create_table(self, *args, **kwargs):
            return {}

        def get_table(self, *args, **kwargs):
            return {}

        def schema_from_json(self, *args, **kwargs):
            return {}

    monkeypatch.setattr("google.cloud.bigquery.Client", MockClient)
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
    yield [Record(uuid=str(_), payload={"key": "value"}) for _ in range(10)]


def test_big_query_writer(monkeypatch, configuration):
    """Should write data to BigQuery."""
    pytest.skip()


def test_big_query_writer_error_handling(configuration, data):
    """Should catch BadRequest exceptions."""

    writer = configuration.get_instance()
    with pytest.raises(BadRequest) as exec_info:
        writer.write(data)

    # JSON table encountered too many errors, giving up. Rows: 375; errors: 1. Please look into the
    # errors[] collection for more details.; reason: invalid, message: Error while reading data,
    # error message: JSON table encountered too many errors, giving up. Rows: 375; errors: 1.
    # Please look into the errors[] collection for more details.;
    # reason: invalid, message: Error while reading data, error message:
    # JSON processing encountered too many errors, giving up. Rows: 375; errors: 1; max bad: 0;
    # error percent: 0; reason: invalid, message:
    # Error while reading data, error message: JSON parsing error in row starting at
    # position 1503169: An internal error has occurred..

    assert "Bad request" in str(exec_info.value)
