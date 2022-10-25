import pandas as pd
from os import listdir


def convert_to_parquet(file_name):
    pos = file_name.find(".")
    first = file_name.rfind("/")
    if ".tsv" in file_name:
        df = pd.read_csv(file_name, sep="\t", header=0, low_memory=False)
    elif ".csv" in file_name:   
        df = pd.read_csv(file_name, sep=",", header=0, low_memory=False)

    file_name = file_name[first:pos]
    print(df.dtypes)
    df.to_parquet(f"export/{file_name}.parquet", 
            engine="pyarrow",
            compression="snappy")


def main():
    for x in listdir("data"):
        convert_to_parquet(f"data/{x}")
        print(x)


if __name__ == "__main__":
    main()
