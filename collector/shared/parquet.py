import io
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

def records_to_parquet_bytes(records: list) -> bytes:
    if not records:
        return b""

    df = pd.json_normalize(records)
    table = pa.Table.from_pandas(df)

    buffer = io.BytesIO()
    pq.write_table(
        table,
        buffer,
        compression="snappy"
    )

    return buffer.getvalue()
