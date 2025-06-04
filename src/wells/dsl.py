import pandas as pd
import json
import uuid
from sqlalchemy import create_engine

DATA = "data"


class DataMapper:
    def __init__(self, dataframe):
        self.df = dataframe

    @classmethod
    def from_csv(cls, csv_file_path):
        dataframe = pd.read_csv(csv_file_path)
        return cls(dataframe)

    @classmethod
    def from_xlsx(cls, xlsx_file_path, sheet_name="Sheet1"):
        dataframe = pd.read_excel(
            xlsx_file_path, engine="openpyxl", sheet_name=sheet_name
        )
        return cls(dataframe)

    def rename_columns(self, columns_mapping):
        self.df.rename(columns=columns_mapping, inplace=True)
        return self

    def filter_columns(self, columns):
        self.df = self.df[columns]
        return self

    def filter_rows(self, condition):
        self.df = self.df.query(condition)
        return self

    def add_column(self, column_name, function):
        self.df[column_name] = self.df.apply(function, axis=1)
        return self

    def drop_columns(self, columns):
        self.df.drop(columns=columns, inplace=True)
        return self

    def sort_rows(self, by, ascending=True):
        self.df.sort_values(by=by, ascending=ascending, inplace=True)
        return self

    def group_by(self, by, agg_func):
        self.df = self.df.groupby(by).agg(agg_func).reset_index()
        return self

    def fill_missing(self, column, value):
        self.df[column] = self.df[column].fillna(value)
        return self

    def lookup_value(self, column_name, lookup_dict):
        self.df[column_name] = self.df[column_name].map(lookup_dict)
        return self

    def change_values(self, column_name, function):
        self.df[column_name] = self.df[column_name].apply(function)
        return self

    def change_values_conditionally(self, column_name, condition, function):
        condition_result = self.df.eval(condition)
        self.df.loc[condition_result, column_name] = self.df.loc[
            condition_result, column_name
        ].apply(function)
        return self

    def pivot(self, index, columns, values):
        self.df = self.df.pivot(
            index=index, columns=columns, values=values
        ).reset_index()
        return self

    def to_json(self, json_file_path):
        json_data = self.df.to_json(orient="records")
        with open(json_file_path, "w") as json_file:
            json.dump(json.loads(json_data), json_file, indent=4)

    def to_sql(self, table_name, connection_string):
        engine = create_engine(connection_string)
        self.df.to_sql(
            name=table_name, con=engine, if_exists="replace", index=False
        )
