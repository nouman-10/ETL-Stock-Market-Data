from selenium import webdriver
from selenium.webdriver.common.by import By
import time
import csv
import re

# Setup
driver = webdriver.Chrome()
url = '<https://www.investing.com>'
driver.get(url)

# Find Stocks button and click
navbar = driver.find_element(By.XPATH, '//nav[@class="whitespace-nowrap mb-8"]')
nav_links = navbar.find_elements(By.TAG_NAME, 'li')
stocks_link = None
for link in nav_links:
    if link.text == 'Stocks':
        stocks_link = link
        break
if stocks_link is not None:
    stocks_link.click()

time.sleep(5)

# Get stock links
tbody = driver.find_element(By.XPATH, '//tbody[contains(@class, "datatable")]')
rows = tbody.find_elements(By.TAG_NAME, "tr")
stock_links = []
for row in rows:
    link_element = row.find_element(By.TAG_NAME, "a")
    link = f"{url}{link_element.get_attribute('href')}"
    stock_name = link_element.text
    stock_links.append({"stock_name": stock_name, "link": link})

# Scrape data for each stock
total_data = []
headers = []
for stock in stock_links:
    link = stock['link']
    stock_name = stock['stock_name']
    driver.get(link)
    time.sleep(2)

    # Click on Historical Data button
    historical_data_button = driver.find_element(By.XPATH, '//li[@data-test="Historical Data"]')
    historical_data_button.click()
    time.sleep(2)

    # Scrape table data
    table = driver.find_element(By.XPATH, '//table[contains(@class, "freeze-column")]')
    tbody = table.find_element(By.TAG_NAME, "tbody")
    rows = tbody.find_elements(By.TAG_NAME, "tr")
    if len(headers) == 0:
        thead = table.find_element(By.TAG_NAME, "thead")
        header_cols = thead.find_elements(By.TAG_NAME, "th")
        headers = [ele.text.strip() for ele in header_cols]
    for row in rows:
        cols = row.find_elements(By.TAG_NAME, "td")
        cols = [ele.text.strip() for ele in cols]
        data = dict(zip(headers, cols))
        data['stock_name'] = stock_name
        total_data.append(data)

    print(f"Fetched data for {stock_name}")

# Write data to CSV
def write_to_csv(data, filename):
    keys = data[0].keys()
    with open(filename, 'w', newline='') as output_file:
        dict_writer = csv.DictWriter(output_file, keys)
        dict_writer.writeheader()
        dict_writer.writerows(data)

write_to_csv(total_data, "stock_data.csv")
