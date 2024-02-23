import contextlib
import os
import time

import dapla as dp

from dapla_pseudo import Pseudonymize

JSON_FILE = "data/personer.json"
CSV_FILE = "data/personer.csv"


dtype = {
    "fnr": "string",
    "fornavn": "string",
    "etternavn": "string",
    "kjonn": "category",
    "fodselsdato": "string",
}

os.environ[
    "PSEUDO_SERVICE_AUTH_TOKEN"
] = "eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICI3WGVjbDNLUjE2YmhzNzJ5NzR0NXFvbEIxQ19qclh2ME85cWprMVRlblJzIn0.eyJleHAiOjE3MDg3MTU3ODQsImlhdCI6MTcwODY3OTc4NCwiYXV0aF90aW1lIjoxNzA4Njc5Nzg0LCJqdGkiOiJiYTZlNjI2NS0zNmM2LTQ5ZmItYjlkMC0wZWIwNGFjYjQ0ZWMiLCJpc3MiOiJodHRwczovL2tleWNsb2FrLnN0YWdpbmctYmlwLWFwcC5zc2Iubm8vYXV0aC9yZWFsbXMvc3NiIiwiYXVkIjoiaHR0cGJpbi1mZSIsInN1YiI6IjgzYzdjZDY2LWZkZDAtNDUyYi1hM2JkLTdmMjVmOTEwOTFlMiIsInR5cCI6IklEIiwiYXpwIjoiaHR0cGJpbi1mZSIsInNlc3Npb25fc3RhdGUiOiIwOTg3YmQxZS05MDZjLTRkNWYtYmJhYS1jZjI4MGZhYmE5YzYiLCJhdF9oYXNoIjoiYkd0bV8zdWxxOHE5OE1yZ19fRGxjZyIsImFjciI6IjEiLCJlbWFpbF92ZXJpZmllZCI6dHJ1ZSwibmFtZSI6Ik1pY2hhZWwgQWxscG9ydCIsIm9pZCI6ImQyMTBkMjQwLWQ2MTktNGMxMC1iNmY0LTIwMGYzMmI4NTQ1NSIsInByZWZlcnJlZF91c2VybmFtZSI6Im1pY0Bzc2Iubm8iLCJnaXZlbl9uYW1lIjoiTWljaGFlbCIsImZhbWlseV9uYW1lIjoiQWxscG9ydCIsImVtYWlsIjoibWljQHNzYi5ubyJ9.IiqxsgsxDVz8QoHEc_AVhcS9RmtvB56dHj4L23wDvuJxWa-CNrro_YzgKeLM04fm7p5_rf_YBLX2G2C_V0wzL8Fgww-DFelhLbJ5NXFtkqHbdAbhcmW7rNBj_yxsInjgIlTZNbJGGAmekzltEhnNnHHIwMiPBXnpKHrkHo1CdnHGFsxVW3Uy_GHx0rJTQ4Q2smBHMFkbvBN53hSObx1nVBJiiwxiIz-IlzxfYoYXltpftRyOLcwpp5h5U0wLmviKYrm13KKtqrGrzbmQ0L3XXwceDiBtsoGm8x9eUJrqe6s-9uvsrEKoeliMHymzxl0RJiDJ2uHPccBBR_VM7xZ0Vw"
os.environ["PSEUDO_SERVICE_URL"] = "http://localhost:8000"
# result = (
#    Pseudonymize.from_pandas(df)  ### GENERAL WORKFLOW ###  # Select dataset
#    .on_fields("fnr")  # Select fields in dataset
#    .with_default_encryption()  # Select encryption method on fields
#   .run()  # Apply pseudonymization
# )

### PSEUDO JAVA SERVICE TIME: 64.8 seconds
### PSEUDO PYTHON SERVICE TIME: 1.7 seconds


@contextlib.contextmanager
def time_block(label):
    start_time = time.time()
    yield
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"{label}: {elapsed_time:.3f} seconds")


synthetic_bucket = "gs://ssb-prod-demo-stat-b-data-delt/komp_test_source_parquet/part-00000-534e3ece-b60d-4155-8680-e92fbf030b5c-c000.snappy.parquet"

df = dp.pandas.read_pandas(synthetic_bucket)
print(f"Length of df: {len(df.index)}")
print(df.head())

with time_block("Cold cache start"):
    result = (
        Pseudonymize.from_pandas(df).on_fields("fnr").with_default_encryption().run()
    )

with time_block("Hot cache start"):
    result = (
        Pseudonymize.from_pandas(df).on_fields("fnr").with_default_encryption().run()
    )

df = result.to_pandas()
print(result.datadoc)
result.to_polars().head()

print(result.to_pandas().head())  # Convert result to Polars DataFrame
