# Activeloop Lakehouse

## Overview
This destination syncs data to Deep Lake on Activeloop. Each stream is written to its own dep lake dataset.

## Sync Mode

| Feature | Support | Notes |
| :--- | :---: | :--- |
| Full Refresh Sync | ✅ | Warning: this mode deletes all previously synced data in the path. |
| Incremental - Append Sync | ✅ |  |
| Incremental - Deduped History | ❌ |  |
| Namespaces | ❌|  |

## Configuration

| Category         | Parameter             |  Type   | Notes                                                                                                                                                                                                                                                                                                                                                       |
|:-----------------|:----------------------|:-------:|:------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Path       | destination       | string  | DeepLake path dataset including hub://..., s3://...
| Token       | API token       | string  | Activeloop api token generated

## Output Schema

Each dataset will have the following columns as tensors:

| Tensor | Type | Notes |
| :--- | :---: | :--- |
| Data fields from the source stream | various | |

Under the hood, the data that is empty is replaced and nulls transformed into hub compatible Nones such as empty arrays or dropped if they are in nested structures. 

## ChangeLog