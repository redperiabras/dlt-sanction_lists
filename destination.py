import dlt

from dlt.common.typing import TDataItems
from dlt.common.schema import TTableSchema


@dlt.destination(batch_size=5)
def print_sink(items: TDataItems, table: TTableSchema):
    print(f"\nTable: {table['name']}")
    for item in items:
        print(item)
