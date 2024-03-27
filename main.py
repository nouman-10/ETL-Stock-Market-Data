from requests import get
from bs4 import BeautifulSoup as bs

def get_soup(url):
    response = get(url)
    soup = bs(response.text)
    return soup

url = "https://www.investing.com"
soup = get_soup(url)

import re
regex = re.compile('.*datatable.*')
tbody = soup.find("tbody", class_=regex)

rows = tbody.find_all("tr")

def get_stock_link(row):
    link_element = row.find("a")
    link = f"{url}{link_element.get('href')}-historical-data"
    stock_name = link_element.text

    return {
        "stock_name": stock_name,
        "link": link
    }

historical_data_links = [get_stock_link(row) for row in tbody.find_all("tr")]

total_data = []
headers = []
for stock in historical_data_links:
    link = stock['link']
    stock_name = stock['stock_name']
    soup = get_soup(link)

    regex = re.compile('.*freeze-column.*')
    table = soup.find("table", class_=regex)
    
    if not len(headers):
		    thead = table.find("thead")
				header_cols = thead.find_all("th")
				headers = [ele.text.strip() for ele in header_cols]

    tbody = table.find("tbody")
    rows = tbody.find_all("tr")

    for row in rows:
        cols = row.find_all("td")
        cols = [ele.text.strip() for ele in cols]
        data = dict(zip(headers, cols))
        data['stock_name'] = stock_name
        total_data.append(data)

    print(f"Fetched data for {stock_name}")


import csv
def write_to_csv(data, filename):
    keys = data[0].keys()
    with open(filename, 'w', newline='') as output_file:
        dict_writer = csv.DictWriter(output_file, keys)
        dict_writer.writeheader()
        dict_writer.writerows(data)

write_to_csv(total_data, "stock_data.csv")
