import json
import os

import mysql.connector
from dotenv import load_dotenv

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
        port=PORT,
    )
    return mydb


def get_record_by(param: str, value: str):
    print(param, value)
    try:
        mydb = connect_to_db()
        mycursor = mydb.cursor()
        mycursor.execute(f"USE {DB_NAME}")
    except Exception as e:
        return {"status": "error", "message": str(e)}
    # show columns from table like param
    mycursor.execute(f"USE {DB_NAME}")
    print(f"SHOW COLUMNS FROM {TABLE_NAME} LIKE '{param}'")

    mycursor.execute(f"SHOW COLUMNS FROM {TABLE_NAME} LIKE '{param}'")
    if mycursor.fetchone():
        print(f"SELECT * FROM {TABLE_NAME} WHERE {param} = {value}")
        # if value is string, add quotes
        # else no quotes
        # if value is numeric, no quotes
        if value.isnumeric():
            mycursor.execute(f"SELECT * FROM {TABLE_NAME} WHERE {param} = {value}")
        else:
            mycursor.execute(f"SELECT * FROM {TABLE_NAME} WHERE {param} = '{value}'")
        results = mycursor.fetchall()
        COLUMN_NAMES = [desc[0] for desc in mycursor.description]
        final_results = []
        for result in results:
            final_results.append(dict(zip(COLUMN_NAMES, result)))
        return final_results


if __name__ == "__main__":
    print(get_record_by("registration_number", "1069"))
