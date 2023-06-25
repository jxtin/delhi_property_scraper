import difflib
import json
import os
from typing import Union

import uvicorn
from fastapi import FastAPI

import scraper
import scraping_utils
from mysql_fetch import get_record_by
from process_data import upload_to_mysql

app = FastAPI()


@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.get("/localities")
def read_localities(
    force_scrape: bool = False,
):
    """function to read localities from localities.json or scrape them if not present

    Args:
        force_scrape (bool, optional): If set True, the list of localities will be scraped again. Defaults to False.

    Returns:
        _type_: List[str]]
    """
    if force_scrape:
        print("force scraping")
        if os.path.exists("localities.json"):
            os.remove("localities.json")
        return scraping_utils.scrape_localities()
    else:
        if os.path.exists("localities.json"):
            with open("localities.json", "r") as f:
                return json.load(f)
        else:
            return scraping_utils.scrape_localities()


def get_closest_locality(locality: str):
    with open("localities.json", "r") as f:
        localities = json.load(f)
    return difflib.get_close_matches(locality, localities, n=1)[0]


@app.get("/scrape")
def scrape(
    locality: str,
    force_scrape: bool = False,
):
    """Function to run scraper on a specific locality, if the data is already present, it will be returned from the file, unless force_scrape is set to True

    Args:
        locality (str): Name of the locality
        force_scrape (bool, optional): If True, data from file will be ignored and scraped fresh. Defaults to False.

    Returns:
        _type_: list[dict]
    """
    print(locality)
    if locality not in read_localities():
        locality = get_closest_locality(locality)
    if force_scrape:
        print("force scraping")
        if os.path.exists(
            f"scraped_data/{locality.replace('/', '-').replace(' ', '_').replace('*', '')}.json"
        ):
            os.remove(
                f"scraped_data/{locality.replace('/', '-').replace(' ', '_').replace('*', '')}.json"
            )
        return scraper.main_scraper(locality)
    else:
        if os.path.exists(
            f"scraped_data/{locality.replace('/', '-').replace(' ', '_').replace('*', '')}.json"
        ):
            with open(
                f"scraped_data/{locality.replace('/', '-').replace(' ', '_').replace('*', '')}.json",
                "r",
            ) as f:
                return json.load(f)
        else:
            return scraper.main_scraper(locality)


@app.get("/upload_to_mysql")
def populate_mysql() -> dict:
    try:
        upload_to_mysql()
        return {"status": "success"}
    except Exception as e:
        print(e)
        return {"status": "failed"}


@app.get("/get_records_by")
def get_records_by_endpoint(
    param: str,
    value: str,
):
    """Send a request to this endpoint to get records from the database (mysql)

    Args:
        param (str): Column name, any of the following : registration_number,registration_date,first_party,second_party,property_address,area,deed_type,property_type,locality,area_sq_ft
        value (str): Value of the column

    Returns:
        _type_: list[dict]
    """
    print(param, value)
    param = param.replace('"', "")
    value = value.replace('"', "")
    return get_record_by(param, value)


if __name__ == "__main__":
    uvicorn.run("app:app", port=8000, reload=True)
