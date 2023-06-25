import json
import os
import time
from concurrent.futures import ThreadPoolExecutor
from typing import List, Union

import bs4
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from dotenv import load_dotenv

from ocr_utils import solve_captcha

URL = "https://esearch.delhigovt.nic.in/Complete_search.aspx"
options = webdriver.ChromeOptions()
options.add_experimental_option("excludeSwitches", ["enable-logging"])
load_dotenv()
if os.getenv("DRIVER_MODE") == "headless":
    options.add_argument("--headless")


def has_server_error(driver: webdriver.chrome.webdriver.WebDriver) -> Union[bool, None]:
    h1_tags = driver.find_elements(By.TAG_NAME, "h1")
    if len(h1_tags) > 0:
        if h1_tags[0].text == "Server Error in '/' Application.":
            return True


def fill_data(
    driver: webdriver.chrome.webdriver.WebDriver, locality: str
) -> webdriver.chrome.webdriver.WebDriver:
    if has_server_error(driver):
        driver.close()
        driver = webdriver.Chrome(options=options)
        driver.get(URL)
    driver.find_element(By.ID, "ctl00_ContentPlaceHolder1_ddl_sro_s").send_keys(
        "Central -Asaf Ali (SR III)"
    )

    time.sleep(2)
    print(locality)
    try:
        driver.find_element(By.ID, "ctl00_ContentPlaceHolder1_ddl_loc_s").send_keys(
            locality
        )
        print("locality selected")
        time.sleep(1)
        driver.find_element(By.ID, "ctl00_ContentPlaceHolder1_ddl_year_s").send_keys(
            "2021-2022"
        )
    except:
        print("page not loading properly")
        driver.close()
        driver = webdriver.Chrome(options=options)
        driver.get(URL)
        time.sleep(5)
        driver = fill_data(driver, locality)
    return driver


def fill_captcha(
    driver: webdriver.chrome.webdriver.WebDriver, locality: str
) -> webdriver.chrome.webdriver.WebDriver:
    success = False
    while not success:
        if has_server_error(driver):
            driver.close()
            driver = webdriver.Chrome(options=options)
            driver.get(URL)
            driver = fill_data(driver, locality)
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")

        captcha_url = (
            driver.find_element(By.ID, "ctl00_ContentPlaceHolder1_UpdatePanel4")
            .find_element(By.TAG_NAME, "img")
            .get_attribute("src")
        )

        solutions = [x for x in solve_captcha(captcha_url) if len(x) == 5]
        print(solutions)
        if len(solutions) == 0:
            print("could not solve captcha")
            # press the refresh button
            success = False
            driver.find_element(By.ID, "ctl00_ContentPlaceHolder1_ibtnRefresh1").click()
            time.sleep(1)
        else:
            driver.find_element(
                By.ID, "ctl00_ContentPlaceHolder1_txtcaptcha_s"
            ).send_keys(solutions[0])
            time.sleep(3)
            driver.find_element(By.ID, "ctl00_ContentPlaceHolder1_btn_search_s").click()
            # if an alert pops up, then call the function again
            try:
                alert = driver.switch_to.alert
                alert.accept()
                success = False
            except:
                if has_server_error(driver):
                    success = False
                    continue
                print("==============success==============")
                success = True
    return driver


def set_rows_20(
    driver: webdriver.chrome.webdriver.WebDriver,
) -> webdriver.chrome.webdriver.WebDriver:
    driver.find_element(
        By.ID, "ctl00_ContentPlaceHolder1_gv_search_ctl13_ddlPageSize"
    ).send_keys("20")
    return driver


def scrape_locality(
    driver: webdriver.chrome.webdriver.WebDriver, locality: str
) -> dict:
    time.sleep(2)
    if has_server_error(driver):
        driver.refresh()
        time.sleep(5)
        driver.refresh()
    try:
        # if page contains "No Record Found according to your search criteria", return empty list
        try:
            if (
                "No Record Found according to your search criteria"
                in driver.find_element(By.ID, "ctl00_ContentPlaceHolder1_lblmsg").text
            ):
                return {locality: []}
        except:
            pass
        try:
            driver = set_rows_20(driver)
        except:
            pass

        next_btn_id = "ctl00_ContentPlaceHolder1_gv_search_ctl23_Button2"
        all_data = []
        while True:
            # scroll down to the bottom of the page
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            table = driver.find_element(By.ID, "ctl00_ContentPlaceHolder1_UpdatePanel1")
            table = bs4.BeautifulSoup(
                table.get_attribute("innerHTML"), "html.parser"
            ).find("table")
            for row in table.find_all("tr"):
                tds = row.find_all("td")
                if len(tds) == 11:
                    all_data.append(
                        {
                            "registration_number": tds[0].text.strip(),
                            "registration_date": tds[1].text.strip(),
                            "first_party": tds[2].text.strip(),
                            "second_party": tds[4].text.strip(),
                            "property_address": tds[6].text.strip(),
                            "area": tds[8].text.strip(),
                            "deed_type": tds[9].text.strip(),
                            "property_type": tds[10].text.strip(),
                        }
                    )
            try:
                driver.find_element(By.ID, next_btn_id).click()
            except:
                break
            time.sleep(1.5)

        return {locality: all_data}
    except Exception as e:
        print(e)
        driver.refresh()
        time.sleep(10)
        return scrape_locality(driver, locality)


def scrape_localities() -> List[str]:
    driver = webdriver.Chrome(options=options)
    driver.get(URL)
    driver.find_element(By.ID, "ctl00_ContentPlaceHolder1_ddl_sro_s").send_keys(
        "Central -Asaf Ali (SR III)"
    )
    time.sleep(2)

    list_of_localities = [
        x.text
        for x in driver.find_element(
            By.ID, "ctl00_ContentPlaceHolder1_ddl_loc_s"
        ).find_elements(By.TAG_NAME, "option")
    ][1:]
    driver.close()
    with open("localities.json", "w") as f:
        json.dump(list_of_localities, f)
    return list_of_localities
