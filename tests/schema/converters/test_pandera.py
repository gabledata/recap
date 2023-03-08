import pandas as pd
import pandera as pa
from pandera.typing import Index, DataFrame, Series


class InputSchema(pa.SchemaModel):
    year: Series[int] = pa.Field(gt=2000, coerce=True)
    month: Series[int] = pa.Field(ge=1, le=12, coerce=True)
    day: Series[int] = pa.Field(ge=0, le=365, coerce=True)

class OutputSchema(InputSchema):
    revenue: Series[float]

@pa.check_types
def transform(df: DataFrame[InputSchema]) -> DataFrame[OutputSchema]:
    return df.assign(revenue=100.0)


df = pd.DataFrame({
    "year": ["2001", "2002", "2003"],
    "month": ["3", "6", "12"],
    "day": ["200", "156", "365"],
})



# print(transform(df))

# print(type(transform(df)))

print('data type',InputSchema.to_schema())

print(InputSchema.to_schema().columns)


# understand the -> notations

def parse(inputSchema):
        for cols in inputSchema.columns:
             print(cols, '->', inputSchema.columns[cols].dtype)

parse(InputSchema.to_schema())

#TODO: Discuss the best way to go about extraction
#TODO: Discuss the approach to handle non included data types