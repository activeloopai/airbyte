#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#


from sre_constants import ANY
from typing import Any, Dict, Iterable, List, Mapping, Union

from asyncio.log import logger
from airbyte_cdk import AirbyteLogger
from airbyte_cdk.destinations import Destination
from airbyte_cdk.models import AirbyteConnectionStatus, AirbyteMessage, ConfiguredAirbyteCatalog, Status, DestinationSyncMode, Status, Type

import numpy as np
import hub
import json


class DestinationDeeplake(Destination):

    type_map = {
        "integer": hub.htype.DEFAULT,
        "number": hub.htype.DEFAULT,
        "string": hub.htype.TEXT,
        "array": hub.htype.DEFAULT,
        "json": hub.htype.JSON,
        "null": hub.htype.TEXT,
        "object": hub.htype.JSON,
        "boolean": hub.htype.DEFAULT,
    }

    def map_types(self, dtype: Union[str, List]):
        """
        Translate type into hub type, if it is complex then just output as a json

        Args:
            type (Union[str, List]): the type specified by schemma

        Returns:
            str: hub type
        """
        if str(dtype) in hub.HTYPE_CONFIGURATIONS.keys():
            return str(dtype)

        if isinstance(dtype, list) or isinstance(dtype, dict) or dtype not in self.type_map:
            dtype = "json"
        return self.type_map[dtype]

    def denulify(self, element: Union[int, list, str], dtype: dict):
        """
        Replace Nones on higher level with empty arrays or drop nones from arrays recursively

        Args:
            element (Union[int, list, str]): element or cell
            dtype (dict): specifies the type of the element

        Returns:
            Union[int, list, str]: return the element
        """
        if (dtype["type"] == "number") and element is None:
            return np.array([])
        elif dtype["type"] == "string" and element is None:
            return ""
        elif dtype["type"] == "array":
            return [self.denulify(x, dtype["items"]) for x in element if x is not None]
        return element

    def process_row(self, data: dict, schema: Mapping[str, Any], transform=None) -> Iterable[Dict]:
        """
        Fill in missing columns, apply transformations upon definition and clean up nulls

        Args:
            data (dict): record.data
            schema (Mapping[str, Any]): structure of the data

        Returns:
            dict: cleaned sample to append into hub dataset
        """
        tran_sample = eval(transform, {"row": data, "hub": hub, "np": np}) if not transform is None else {}

        sample = {}
        for column, definition in schema:
            if transform is not None and column in tran_sample.keys():
                sample[column] = self.denulify(tran_sample[column], definition)
            elif column in data:
                sample[column] = self.denulify(data[column], definition)
            else:
                sample[column] = np.array([])

        return sample

    def construct_schema(self, schema: Mapping[str, Any], config: Mapping[str, Any]) -> Iterable[Dict]:
        new_schema = {}
        tensor_defs = {}
        if "overwrite_tensor_definitions" in config:
            tensor_defs = str(config["overwrite_tensor_definitions"]).replace("'", '"')
            tensor_defs = json.loads(tensor_defs)

        for column_name, items in schema:
            if "neglected_tensors" in config and column_name in config["neglected_tensors"]:
                continue
            elif column_name in tensor_defs:
                new_schema[column_name] = {"type": tensor_defs[column_name]["htype"], "kwargs": tensor_defs[column_name]}
            else: 
                new_schema[column_name] = items
        return new_schema.items()

    def load_datasets(self, config: Mapping[str, Any], configured_catalog: ConfiguredAirbyteCatalog) -> Iterable[Dict]:
        """
        Create or load datasets

        Args:
            config (Mapping[str, Any]): configurations of datasets
            configured_catalog (ConfiguredAirbyteCatalog): configurations of streams

        Returns:
            Iterable[Dict]: stream dictionary that contain schema, sync_mode, dataset and cache
        """
        streams = {
            s.stream.name: {"schema": s.stream.json_schema["properties"].items(), "sync_mode": s.destination_sync_mode}
            for s in configured_catalog.streams
        }

        for name, schema in streams.items():
            print(f"Creating dataset at {config['path']}/{name} in sync={schema['sync_mode']}")
            overwrite = schema["sync_mode"] == DestinationSyncMode.overwrite
            token = config["token"] if "token" in config else None

            if hub.exists(f"{config['path']}/{name}", token=token):
                ds = hub.load(f"{config['path']}/{name}", token=token, read_only=False)
            else:
                ds = hub.empty(f"{config['path']}/{name}", overwrite=overwrite, token=token)

            self.setup_activeloop_creds(ds, config)

            # construct schema
            schema["schema"] = self.construct_schema(schema["schema"], config)

            with ds:
                for column_name, definition in schema["schema"]:

                    htype = self.map_types(definition["type"])
                    if overwrite and column_name in ds.tensors:
                        ds.delete_tensor(column_name, large_ok=True) 
                    
                    if column_name not in ds.tensors:
                        if "kwargs" in definition:
                            kwargs = definition["kwargs"]
                            ds.create_tensor(column_name, **kwargs)
                            logger.info(f"Created tensor {column_name} with overwritten arguments {kwargs}")
                        else:
                            ds.create_tensor(
                                column_name,
                                htype=htype,
                                exist_ok=True,
                                create_shape_tensor=False,
                                create_sample_info_tensor=False,
                                create_id_tensor=False,
                            )
                            logger.info(f"Created tensor {column_name} with htype {htype}")

            print(f"Loaded {name} dataset")
            streams[name]["ds"] = ds
            streams[name]["cache"] = []

        return streams

    def flush(self, streams: Iterable[Dict]):
        """
        Flushes the cache into datasets

        Args:
            streams (Iterable[Dict]): _description_
        """
        for name, stream in streams.items():
            length = len(stream["cache"])

            if length == 0:
                continue
            cache = {column_name: [row[column_name] for row in stream["cache"]] for column_name, _ in stream["schema"]}

            with stream["ds"] as ds:
                for column_name, column in cache.items():
                    ds[column_name].extend(column)
                ds.commit(f"appended {length} rows", allow_empty=True)

            print(f"Appended into {name} {length} rows")
            stream["cache"] = []

    def write(
        self, config: Mapping[str, Any], configured_catalog: ConfiguredAirbyteCatalog, input_messages: Iterable[AirbyteMessage]
    ) -> Iterable[AirbyteMessage]:

        """
        Reads the input stream of messages, config, and catalog to write data to the destination.

        This method returns an iterable (typically a generator of AirbyteMessages via yield) containing state messages received
        in the input message stream. Outputting a state message means that every AirbyteRecordMessage which came before it has been
        successfully persisted to the destination. This is used to ensure fault tolerance in the case that a sync fails before fully completing,
        then the source is given the last state message output from this method as the starting point of the next sync.

        :param config: dict of JSON configuration matching the configuration declared in spec.json
        :param configured_catalog: The Configured Catalog describing the schema of the data being received and how it should be persisted in the
                                    destination
        :param input_messages: The stream of input messages received from the source
        :return: Iterable of AirbyteStateMessages wrapped in AirbyteMessage structs
        """
        streams = self.load_datasets(config, configured_catalog)

        transform = compile(config["custom_transform"], "", "eval") if "custom_transform" in config else None

        for message in input_messages:
            if message.type == Type.STATE:
                self.flush(streams)
                yield message

            elif message.type == Type.RECORD:
                record = message.record
                sample = self.process_row(record.data, streams[record.stream]["schema"], transform)
                streams[record.stream]["cache"].append(sample)
            else:
                # ignore other message types for now
                continue

        self.flush(streams)

    def setup_activeloop_creds(self, ds: hub.Dataset, config: Mapping[str, Any], managed=False):
        if "managed_creds" in config:
            existing_keys = ds.get_creds_keys()
            for creds_name in config["managed_creds"]:
                if creds_name not in existing_keys:
                    ds.add_creds_key(creds_name, managed=managed)

    def check(self, logger: AirbyteLogger, config: Mapping[str, Any]) -> AirbyteConnectionStatus:
        """
        Tests if the input configuration can be used to successfully connect to the destination with the needed permissions
            e.g: if a provided API token or password can be used to connect and write to the destination.

        :param logger: Logging object to display debug/info/error to the logs
            (logs will not be accessible via airbyte UI if they are not passed to this logger)
        :param config: Json object containing the configuration of this destination, content of this json is as specified in
        the properties of the spec.json file

        :return: AirbyteConnectionStatus indicating a Success or Failure
        """
        try:
            token = config["token"] if "token" in config else None
            path = f"{config['path']}/_airbyte_test"
            ds = hub.empty(path, token=token, overwrite=True)

            self.setup_activeloop_creds(ds, config)

            if "overwrite_tensor_definitions" in config:
                tensor_defs = str(config["overwrite_tensor_definitions"]).replace("'", '"')
                tensor_defs = json.loads(tensor_defs)
                for tensor_name, kwargs in tensor_defs.items():
                    ds.create_tensor(tensor_name, **kwargs)
                    logger.info(msg=f"Tensor named '{tensor_name}' with args {kwargs} will be updated")

            if "neglected_tensors" in config:
                for tensor_name in config["neglected_tensors"]:
                    logger.info(msg=f"Tensor named '{tensor_name}' will be neglected")

            if "custom_transform" in config:
                # parse python code here
                compile(config["custom_transform"], "", "eval")
                logger.info("successfully compiled the transformation")
            ds.delete()

            # TODO add more exhaustive checks for parameters
            return AirbyteConnectionStatus(status=Status.SUCCEEDED)
        except Exception as e:
            return AirbyteConnectionStatus(status=Status.FAILED, message=f"An exception occurred: {repr(e)}")
