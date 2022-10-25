
type_to_sql = {
    "string": "VARCHAR(256)",
    "int64": "BIGINT",
    "double": "FLOAT",
}

def convert_to_sql(type):
    return type_to_sql[type]
