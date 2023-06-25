import json
import os

import mysql.connector
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()
USER = os.getenv("MYSQL_USER")
PASSWORD = os.getenv("MYSQL_PASSWORD")
DB_NAME = os.getenv("MYSQL_DB")
TABLE_NAME = os.getenv("MYSQL_TABLE")
HOST = os.getenv("MYSQL_HOST")
PORT = os.getenv("MYSQL_PORT")


def connect_to_db():
    mydb = mysql.connector.connect(
        host=HOST,
        user=USER,
        password=PASSWORD,
        PORT=PORT,
    )
    return mydb


def merge_jsons():
    merged_json = {}
    files = os.listdir("scraped_data")
    for file in files:
        file_json = json.load(open(f"scraped_data/{file}", "r"))
        merged_json.update(file_json)

    with open("merged_data.json", "w") as f:
        json.dump(merged_json, f)

    return merged_json


def generate_csv():
    merged_json = merge_jsons()
    flattened_data = []
    # add name of locality to each row
    for locality, data in merged_json.items():
        for row in data:
            row["locality"] = locality
            flattened_data.append(row)
    with open("flattened_data.json", "w") as f:
        json.dump(flattened_data, f)

    df = pd.DataFrame(flattened_data)
    df["registration_date"] = pd.to_datetime(df["registration_date"], format="%d-%m-%Y")
    # registration_number should be int
    df["registration_number"] = df["registration_number"].astype(int)
    df["area_sq_ft"] = df["area"].apply(fix_area_unit)
    with open("flattened_data.csv", "w") as f:
        df.to_csv(f, index=False)
    # show datatype for each column
    for col in df.columns:
        print(col, df[col].dtype)
    return df


def fix_area_unit(area):
    if "Sq. Yard" in area:
        return float(area.split("Sq.")[0].strip()) * 9
    elif "Sq. Meter" in area:
        return float(area.split("Sq.")[0].strip()) * 10.7639
    else:
        return float(area.split("Sq.")[0].strip())


def check_db_exists():
    mydb = connect_to_db()
    cursor = mydb.cursor()
    cursor.execute("set GLOBAL max_allowed_packet=67108864")
    cursor.execute("SHOW DATABASES")
    for db in cursor:
        if DB_NAME in db:
            return True
    cursor.close()
    return False


def check_table():
    mydb = connect_to_db()
    cursor = mydb.cursor()
    cursor.execute("USE property_data")
    cursor.execute("SHOW TABLES")
    for table in cursor:
        if TABLE_NAME == table:
            cursor.close()
            return True
    cursor.close()
    return False


def create_db():
    mydb = connect_to_db()
    mydb.cursor().execute(f"CREATE DATABASE {DB_NAME}")


def store_to_mysql(df):
    engine = create_engine(
        f"mysql+mysqlconnector://{USER}:{PASSWORD}@localhost:{PORT}/{DB_NAME}"
    )
    # set registration_number as index
    df.set_index("registration_number", inplace=True)
    df.to_sql(TABLE_NAME, con=engine, if_exists="replace")


def upload_to_mysql():
    # if csv exists, skip json processing
    if not os.path.exists("flattened_data.csv"):
        merge_jsons()
        df = generate_csv()
    else:
        df = pd.read_csv("flattened_data.csv")
        df["registration_date"] = pd.to_datetime()
        
    if not check_db_exists():
        create_db()
    if check_table():
        # first drop table
        cursor = mydb.cursor()
        cursor.execute(f"USE {DB_NAME}")
        cursor.execute(f"DROP TABLE {TABLE_NAME}")
        cursor.close()
    store_to_mysql(df)


if __name__ == "__main__":
    upload_to_mysql()
