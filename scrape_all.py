import requests
import concurrent.futures
import time


def scrape_locality(locality: str):
    print(f"scraping {locality}")
    start = time.time()
    r = requests.get(f"http://localhost:8000/scrape?locality={locality}")
    while r.status_code != 200:
        r = requests.get(f"http://localhost:8000/scrape?locality={locality}")

    print(f"scraped {locality} in {time.time() - start} seconds")
    return r.json()


if __name__ == "__main__":
    list_of_localities = requests.get("http://localhost:8000/localities").json()
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        future_to_url = {
            executor.submit(scrape_locality, locality): locality
            for locality in list_of_localities
        }
        for future in concurrent.futures.as_completed(future_to_url):
            print(future.result())

    # upload_to_mysql
    r = requests.get("http://localhost:8000/upload_to_mysql")
    if r.status_code == 200:
        print("uploaded to mysql")
