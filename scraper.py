import json
import os
import time
from concurrent.futures import ThreadPoolExecutor

import bs4
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from dotenv import load_dotenv

from ocr_utils import solve_captcha
from scraping_utils import (
    fill_captcha,
    fill_data,
    has_server_error,
    scrape_localities,
    scrape_locality,
)


URL = "https://esearch.delhigovt.nic.in/Complete_search.aspx"
options = webdriver.ChromeOptions()
options.add_experimental_option("excludeSwitches", ["enable-logging"])
load_dotenv()
if os.getenv("DRIVER_MODE") == "headless":
    options.add_argument("--headless")


def main_scraper(locality: str):
    start_time = time.time()
    print(f"========== scraping {locality} ==========")
    driver = webdriver.Chrome(options=options)
    driver.get(URL)
    time.sleep(3)
    driver = fill_data(driver, locality)
    driver = fill_captcha(driver, locality)
    time.sleep(3)
    data = scrape_locality(driver, locality)

    driver.close()
    if not os.path.exists("scraped_data"):
        os.mkdir("scraped_data")
    filename = f"{locality}.json".replace("/", "-").replace(" ", "_").replace("*", "")
    with open("scraped_data/" + filename, "w") as f:
        json.dump(data, f)
    print(f"========== saved {locality} ==========")
    print(
        f"========== time taken for {locality} : {time.time() - start_time} =========="
    )
    return data


def main_parallel(force_scrape=False):
    if not os.path.exists("localities.json"):
        list_of_localities = scrape_localities()
    else:
        with open("localities.json", "r") as f:
            list_of_localities = json.load(f)
    if not os.path.exists("scraped_data"):
        os.mkdir("scraped_data")
    if force_scrape:
        # ignore saved files and scrape all
        data_scraped = []
        futures = []
        with ThreadPoolExecutor(max_workers=4) as executor:
            for locality in list_of_localities:
                futures.append(executor.submit(main_scraper, locality))
        for future in futures:
            data_scraped.append(future.result())
    else:
        # scrape only the files that are not saved
        localities_left = []
        for locality in list_of_localities:
            if locality.replace("/", "-").replace(" ", "_").replace("*", "") not in [
                x.split(".")[0] for x in os.listdir("scraped_data")
            ]:
                localities_left.append(locality)

        print(len(localities_left))
        data_scraped = []
        futures = []
        print(localities_left)
        with ThreadPoolExecutor(max_workers=4) as executor:
            for locality in localities_left:
                futures.append(executor.submit(main_scraper, locality))
        for future in futures:
            data_scraped.append(future.result())
    with open("all_data.json", "w") as f:
        json.dump(data_scraped, f)


def main_sequential(force_scrape=True):
    if not os.path.exists("localities.json"):
        list_of_localities = scrape_localities()
    else:
        with open("localities.json", "r") as f:
            list_of_localities = json.load(f)
    if not os.path.exists("scraped_data"):
        os.mkdir("scraped_data")
    if force_scrape:
        # ignore saved files and scrape all
        for locality in list_of_localities:
            main_scraper(locality)
    else:
        # scrape only the files that are not saved
        for locality in list_of_localities:
            if locality.replace("/", "-").replace(" ", "_").replace(
                "*", ""
            ) not in os.listdir("scraped_data"):
                main_scraper(locality)


def handler_main(event, context):
    # i will send http request to this lambda function, with locality_name as query parameter, call main_scraper with that locality_name and return the data
    locality_name = event["queryStringParameters"]["locality_name"]
    data = main_scraper(locality_name)
    return {
        "statusCode": 200,
        "body": json.dumps(data),
    }
