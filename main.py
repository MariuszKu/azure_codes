from adlfs import AzureBlobFileSystem
import json
import pyarrow as pa
import pyarrow.parquet as pq
from convert.helper import *
import sys

def read_metdata(file, abfs):
    print(f"-----------------{file}")
    pfile = pq.read_table(file, filesystem=abfs)
    columns = []
    for x in pfile.schema:
        columns.append(f"{x.name}  {convert_to_sql(x.type)}")
    tab_name = file[:file.find(".")].split("/")[-2]
    location = "/".join(file.split("/")[1:-1])
    sql = ""
    sql = f"CREATE EXTERNAL TABLE {tab_name} ("
    sql += ",\n".join(columns)
    sql += f""")
    WITH (
        LOCATION = '{location}',
        DATA_SOURCE = silver,
        FILE_FORMAT = ParquetFormat
    );"""
    return sql

def traverse_node(path, abfs):
    scan=abfs.ls(path[0]["name"], detail = True)
    for item in scan:
        if item["type"] == "file" and "_SUCCESS" not in item["name"]:
            return item["name"]

    traverse_node(scan)


def main(path_to_scan):
    print(f"scan path: {path_to_scan}")
    settings = {}
    with open('settings.json') as file:
        settings = json.load(file)
        
    abfs = AzureBlobFileSystem(account_name = settings["account_name"],
                            account_key = settings["key"], container_name = settings["container"])

    file = abfs.ls(path_to_scan, detail = True)
    sql_statment = ""
    for item in file:
        sql_statment += read_metdata(traverse_node([item], abfs), abfs)
        print(sql_statment)

    with open("data_lake.sql", "w") as file:
        file.write(sql_statment)


if __name__ == "__main__":
    # arv path to scan for eg: "silver/movies"
    main(sys.argv[1])