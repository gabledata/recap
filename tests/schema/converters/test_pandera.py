import pandas as pd
import pandera as pa
from pandera.typing import Index, DataFrame, Series
from recap.schema.converters.pandera import from_pandera
from recap.schema import model


pandera_schema = pa.DataFrameSchema(
    {
        "column1": pa.Column(int),
        "column2": pa.Column(float)
    }
)

print(type(pandera_schema.columns['column1'].dtype))

# recap_schema = from_pandera(pandera_schema)
# print(recap_schema)

#int64
#float64
#bool
#str