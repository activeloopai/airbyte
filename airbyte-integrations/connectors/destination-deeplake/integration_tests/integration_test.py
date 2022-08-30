#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#

import json
import random
import string
import tempfile
from datetime import datetime
from typing import Dict
from unittest.mock import MagicMock

import pytest
from airbyte_cdk.models import (
    AirbyteMessage,
    AirbyteRecordMessage,
    AirbyteStream,
    ConfiguredAirbyteCatalog,
    ConfiguredAirbyteStream,
    DestinationSyncMode,
    Status,
    SyncMode,
    Type,
)
from destination_deeplake import DestinationDeeplake

import hub

@pytest.fixture(scope="module")
def local_file_config() -> Dict[str, str]:
    path = tempfile.TemporaryDirectory()
    yield {"path": f"{path.name}"}
    path.cleanup()


@pytest.fixture(scope="module")
def test_table_name() -> str:
    letters = string.ascii_lowercase
    rand_string = "".join(random.choice(letters) for _ in range(10))
    return f"airbyte_integration_{rand_string}"


@pytest.fixture
def table_schema() -> str:
    schema = {"type": "object", "properties": {"column1": {"type": ["null", "string"]}}}
    return schema


@pytest.fixture
def configured_catalogue(test_table_name: str, table_schema: str) -> ConfiguredAirbyteCatalog:
    append_stream = ConfiguredAirbyteStream(
        stream=AirbyteStream(name=test_table_name, json_schema=table_schema),
        sync_mode=SyncMode.incremental,
        destination_sync_mode=DestinationSyncMode.append,
    )
    return ConfiguredAirbyteCatalog(streams=[append_stream])


@pytest.fixture
def invalid_config() -> Dict[str, str]:
    return {"path": "/sqlite.db"}


@pytest.fixture
def airbyte_message1(test_table_name: str):
    return AirbyteMessage(
        type=Type.RECORD,
        record=AirbyteRecordMessage(stream=test_table_name, data={"column1": 1}, emitted_at=int(datetime.now().timestamp()) * 1000),
    )


@pytest.fixture
def airbyte_message2(test_table_name: str):
    return AirbyteMessage(
        type=Type.RECORD,
        record=AirbyteRecordMessage(stream=test_table_name, data={"column1": 2}, emitted_at=int(datetime.now().timestamp()) * 1000),
    )


@pytest.mark.parametrize("config", ["invalid_config"])
@pytest.mark.disable_autouse
def test_check_fails(config, request):
    config = request.getfixturevalue(config)
    destination = DestinationDeeplake()
    status = destination.check(logger=MagicMock(), config=config)
    assert status.status == Status.FAILED


@pytest.mark.parametrize("config", ["local_file_config"])
def test_check_succeeds(config, request):
    config = request.getfixturevalue(config)
    destination = DestinationDeeplake()
    status = destination.check(logger=MagicMock(), config=config)
    assert status.status == Status.SUCCEEDED


@pytest.mark.parametrize("config", ["local_file_config"])
def test_write(
    config: Dict[str, str],
    request,
    configured_catalogue: ConfiguredAirbyteCatalog,
    airbyte_message1: AirbyteMessage,
    airbyte_message2: AirbyteMessage,
    test_table_name: str,
):
    config = request.getfixturevalue(config)
    destination = DestinationDeeplake()
    generator = destination.write(
        config=config, configured_catalog=configured_catalogue, input_messages=[airbyte_message1, airbyte_message2]
    )

    result = list(generator)
    assert len(result) == 0
    ds = hub.load(f'{config.get("path")}/{test_table_name}')

    column1 = ds["column1"].numpy()
    assert len(ds) == 2
    assert column1[0] == airbyte_message1.record.data["column1"]
    assert column1[1] == airbyte_message2.record.data["column1"]
