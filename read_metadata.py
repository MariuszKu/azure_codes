import pyarrow as pa
import pyarrow.parquet as pq
from os import listdir
from convert.helper import *

for file in listdir("export"):
    pfile = pq.read_table(f"export/{file}")
    # print("Column names: {}".format(pfile.column_names))
    print(f"-----------------{file}")
    columns = []
    for x in pfile.schema:
        columns.append(f"{x.name}  {convert_to_sql(x.type)}")
    tab_name = file[:file.find(".")]
    print(f"CREATE TABLE {tab_name} (")
    print(",\n".join(columns))
    print(")")
