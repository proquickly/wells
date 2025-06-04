import wells.dsl
import pandas as pd
import json
import uuid
from sqlalchemy import create_engine

DATA = "xxxxxxxx"

# EXAMPLE

d = wells.dsl.DataMapper.from_xlsx(f"{DATA}/sample.xlsx")
d.rename_columns(
    {"old_column1": "new_column1", "old_column2": "new_column2"}
).filter_columns(["new_column1", "new_column2"]).filter_rows("new_column1 > 2")
d.add_column("new_column", lambda row: row["new_column1"] + row["new_column2"])
d.drop_columns(["new_column1", "new_column2"]).sort_rows(
    by="new_column", ascending=False
)
d.group_by(by="new_column", agg_func="sum").fill_missing("new_column", 0)
d.to_sql("wells", "wells.db")
