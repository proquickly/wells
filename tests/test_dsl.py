import os
import pandas as pd
import pytest
import json
from wells.dsl import DataMapper


@pytest.fixture
def sample_data():
    data = {
        "old_column1": [1, 2, 3, 4],
        "old_column2": [5, 6, 7, 8],
        "old_columnA": ["a", "b", "c", "d"],
        "old_columnB": [10, 20, 30, 40],
    }
    df = pd.DataFrame(data)
    df.to_csv("sample.csv", index=False)
    yield "sample.csv"
    os.remove("sample.csv")


def test_rename_columns(sample_data):
    mapper = DataMapper(sample_data)
    mapper.rename_columns(
        {"old_column1": "new_column1", "old_column2": "new_column2"}
    )
    assert "new_column1" in mapper.df.columns
    assert "new_column2" in mapper.df.columns


def test_filter_columns(sample_data):
    mapper = DataMapper(sample_data)
    mapper.filter_columns(["old_column1", "old_column2"])
    assert list(mapper.df.columns) == ["old_column1", "old_column2"]


def test_filter_rows(sample_data):
    mapper = DataMapper(sample_data)
    mapper.filter_rows("old_column1 > 2")
    assert len(mapper.df) == 2


def test_add_column(sample_data):
    mapper = DataMapper(sample_data)
    mapper.add_column(
        "new_column", lambda row: row["old_column1"] + row["old_column2"]
    )
    assert "new_column" in mapper.df.columns
    assert mapper.df["new_column"].tolist() == [6, 8, 10, 12]


def test_lookup_value(sample_data):
    mapper = DataMapper(sample_data)
    lookup_dict = {"a": "alpha", "b": "beta", "c": "gamma", "d": "delta"}
    mapper.lookup_value("old_columnA", lookup_dict)
    assert mapper.df["old_columnA"].tolist() == [
        "alpha",
        "beta",
        "gamma",
        "delta",
    ]


def test_drop_columns(sample_data):
    mapper = DataMapper(sample_data)
    mapper.drop_columns(["old_column1", "old_column2"])
    assert "old_column1" not in mapper.df.columns
    assert "old_column2" not in mapper.df.columns


def test_sort_rows(sample_data):
    mapper = DataMapper(sample_data)
    mapper.sort_rows(by="old_column1", ascending=False)
    assert mapper.df["old_column1"].tolist() == [4, 3, 2, 1]


def test_group_by(sample_data):
    mapper = DataMapper(sample_data)
    mapper.group_by("old_columnA", {"old_columnB": "sum"})
    assert "old_columnB" in mapper.df.columns
    assert mapper.df["old_columnB"].tolist() == [10, 20, 30, 40]


def test_fill_missing(sample_data):
    mapper = DataMapper(sample_data)
    mapper.df.loc[0, "old_column1"] = None
    mapper.fill_missing("old_column1", 0)
    assert mapper.df["old_column1"].tolist() == [0, 2, 3, 4]


def test_change_values(sample_data):
    mapper = DataMapper(sample_data)
    mapper.change_values("old_column1", lambda x: x * 2)
    assert mapper.df["old_column1"].tolist() == [2, 4, 6, 8]


def test_change_values_conditionally(sample_data):
    mapper = DataMapper(sample_data)
    mapper.change_values_conditionally(
        "old_column1", "old_column1 > 2", lambda x: x * 2
    )
    assert mapper.df["old_column1"].tolist() == [1, 2, 6, 8]


def test_pivot(sample_data):
    data = {
        "index": [1, 1, 2, 2],
        "columns": ["A", "B", "A", "B"],
        "values": [10, 20, 30, 40],
    }
    df = pd.DataFrame(data)
    df.to_csv("pivot_sample.csv", index=False)
    mapper = DataMapper("pivot_sample.csv")
    mapper.pivot(index="index", columns="columns", values="values")
    assert "A" in mapper.df.columns
    assert "B" in mapper.df.columns
    assert mapper.df["A"].tolist() == [10, 30]
    assert mapper.df["B"].tolist() == [20, 40]
    os.remove("pivot_sample.csv")


def test_to_json(sample_data):
    mapper = DataMapper(sample_data)
    json_file_path = "output.json"
    mapper.to_json(json_file_path)
    with open(json_file_path, "r") as json_file:
        data = json.load(json_file)
    assert len(data) == 4
    os.remove(json_file_path)


def test_example(sample_data):
    mapper = DataMapper(sample_data)
    mapper.rename_columns(
        {"old_column1": "new_column1", "old_column2": "new_column2"}
    ).filter_columns(["new_column1", "new_column2"]).filter_rows(
        "new_column2 > 5"
    ).add_column(
        "new_column3", lambda row: row["new_column1"] + row["new_column2"]
    ).to_json("example.json")
