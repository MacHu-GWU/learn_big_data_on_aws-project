# -*- coding: utf-8 -*-

"""
This is a solution can split large JSON file into smaller chunks (if there is a
big array node) without using much memory.

Use case 1:

Input::

    {
        "id": 1,
        "records": [
            {"key": 1},
            {"key": 2},
            {"key": 3},
            ...
        ],
        "name: "alice"
    }

Output::

    # data.json
    {
        "id": 1,
        "name: "alice"
    }

    # arrays/1.json
    [
        {"key": 1},
        {"key": 2},
        ...
        {"key": 10}
    ]

    # arrays/2.json
    [
        {"key": 11},
        {"key": 12},
        ...
        {"key": 20}
    ]

    # arrays/10.json
    [
        {"key": 91},
        {"key": 92},
        ...
        {"key": 100}
    ]

Use case 2:

Input::

    {
        "id": 1,
        "data": {
            "date": "2000-01-01",
            "records": [
                {"key": 1},
                {"key": 2},
                {"key": 3},
                ...
            ],
        },
        "name: "alice"
    }

Output::

    # data.json
    {
        "id": 1,
        "data": {
            "date": "2000-01-01",
        },
        "name: "alice"
    }

    # arrays/1.json
    [
        {"key": 1},
        {"key": 2},
        ...
        {"key": 10}
    ]

    # arrays/2.json
    [
        {"key": 11},
        {"key": 12},
        ...
        {"key": 20}
    ]

    # arrays/10.json
    [
        {"key": 91},
        {"key": 92},
        ...
        {"key": 100}
    ]

Benchmark result:

- Input file: 1G, split into 10 files
- Macbook pro 1: 32G memory + SSD
- Time: 12s
"""

import typing as T
import json
import ijson
import shutil
import itertools
from pathlib import Path
from datetime import datetime

dir_here = Path(__file__).parent


def make_data(path: Path):
    # n_records = 10
    # str_length = 10

    n_records = 1000
    str_length = 1000000

    data = {
        "id": 1,
        "data": {
            "date": "2000-01-01",
            "records": [
                {"k": i, "v": "a" * str_length} for i in range(1, 1 + n_records)
            ],
        },
        "name": "alice",
    }

    # data = {
    #     "id": 1,
    #     "records": [
    #         {"k": i, "v": "a" * str_length}
    #         for i in range(1, 1 + n_file)
    #     ],
    #     "name": "alice",
    # }

    with path.open("w") as f:
        json.dump(data, f)


def delete_node(
    p_in: Path,
    json_path: str,
) -> dict:
    """
    Example::

        # example 1
        >>> input_data = {
        ...     "id": 1,
        ...     "delete": [],
        ... },
        >>> json_path = "delete"
        >>> print(output_data)
        {
            "id": 1
        },

        # example 2
        >>> input_data ={
        ...     "id": 1,
        ...     "a": {
        ...         "delete": []
        ...     },
        ... }
        >>> json_path = "a.delete"
        >>> print(output_data)
        {
            "id": 1,
            "a": {}
        }

        # example 3
        >>> input_data ={
        ...     "id": 1,
        ...     "a": {
        ...         "a_value": 2,
        ...         "b": {
        ...             "delete": [],
        ...             "b_value": 3,
        ...         },
        ...     },
        ... }
        >>> json_path = "a.b.delete"
        >>> print(output_data)
        {
            "id": 1,
            "a": {
                "a_value": 2,
                "b": {
                    "b_value": 3
                }
            }
        }
    """
    parts = json_path.split(".")
    prefix_and_key_pairs = []
    lst = list()
    for part in parts:
        prefix = ".".join(lst)
        key = part
        prefix_and_key_pairs.append((prefix, key))
        lst.append(part)

    new_data = dict()
    parent_data = new_data
    for prefix, key in prefix_and_key_pairs:
        # print(f"------ prefix = {prefix}, key = {key} ------")
        data = dict()
        with p_in.open("r") as f_in:
            for k, v in ijson.kvitems(f_in, prefix):
                if k != key:
                    data[k] = v
        if prefix == "":
            new_data = data
        else:
            parent_data[prefix.split(".")[-1]] = data
        parent_data = data
        # print("new_data:", new_data)

    return new_data


def take(n: int, iterable: T.Iterable):
    """
    Return first n items of the iterable as a list
    """
    return list(itertools.islice(iterable, n))


def split_json(
    p_in: Path,
    dir_out: Path,
    json_path: str,
    chunk_size: int,
):
    """
    :param p_in: input data path
    :param dir_out: output data directory, it should not exist
    :param json_path: the json path in dot notation to the array you want to split
    :param chunk_size: group items in the array into chunks of this size
    """
    if dir_out.exists():
        raise FileExistsError(f"{dir_out} already exists")

    path_data = dir_out.joinpath("data.json")
    dir_arrays = dir_out.joinpath("arrays")
    dir_arrays.mkdir(parents=True)

    # split the big json array into many small json arrays
    with p_in.open("r") as f_in:
        iterator = ijson.items(f_in, f"{json_path}.item")
        for ith in range(1, 1 + 999):
            items = take(chunk_size, iterator)
            path_out = dir_arrays.joinpath(f"{ith}.json")
            if len(items) == 0:
                break
            else:
                with path_out.open("w") as f_out:
                    json.dump(items, f_out)

    # delete the big json array node from the original json
    data = delete_node(p_in=p_in, json_path=json_path)
    with path_data.open("w") as f_out:
        json.dump(data, f_out)


if __name__ == "__main__":
    dir_output = dir_here / "output"
    path_data = dir_here / "data.json"

    def test_delete_node():
        path = dir_here.joinpath("test_delete_node.json")
        input_output_jsonpath = [
            (
                {
                    "id": 1,
                    "delete": [],
                },
                {
                    "id": 1,
                },
                "delete",
            ),
            (
                {
                    "id": 1,
                    "a": {"delete": []},
                },
                {
                    "id": 1,
                    "a": {},
                },
                "a.delete",
            ),
            (
                {
                    "id": 1,
                    "a": {
                        "a_value": 2,
                        "b": {
                            "delete": [],
                            "b_value": 3,
                        },
                    },
                },
                {
                    "id": 1,
                    "a": {
                        "a_value": 2,
                        "b": {
                            "b_value": 3,
                        },
                    },
                },
                "a.b.delete",
            ),
        ]
        for input_data, output_data, jsonpath in input_output_jsonpath:
            path.write_text(json.dumps(input_data))
            result = delete_node(path, json_path=jsonpath)
            assert result == output_data

    def test_split_json():
        shutil.rmtree(dir_output, ignore_errors=True)
        st = datetime.utcnow()
        split_json(
            p_in=path_data,
            dir_out=dir_output,
            json_path="data.records",
            chunk_size=120,
        )
        et = datetime.utcnow()
        elapse = (et - st).total_seconds()
        print(f"elapsed time: {elapse:.2f} seconds")

    # test_delete_node()
    # make_data(path_data)
    # test_split_json()
