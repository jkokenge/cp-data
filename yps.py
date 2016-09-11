import requests
import csv
import os
import dataset
import time
import json
import re
from os import environ
from bs4 import BeautifulSoup
from pprint import pprint
from hashlib import sha1

CACHE_DIR = os.path.join(os.path.dirname(__file__), 'data/yellow_pages')
pg_uname = environ['PGUNAME']
pg_passwd = environ['PGPSSWD']

connection_string = "postgresql://{}:{}@localhost:5432/cp_scrape".format(pg_uname, pg_passwd)

db = dataset.connect(connection_string)

BASEURL = 'http://www.yellowpages.com/search?search_terms='
search_terms = 'dentists'
geo_location_url = "&geo_location_terms="
geo_location_terms = "austin, tx"
page = "&page="

def url_to_filename(url):
    hash_file = sha1(url).hexdigest() + '.html'
    return os.path.join(CACHE_DIR, hash_file)

def store_local(url, content):
    if not os.path.isdir(CACHE_DIR):
        os.makedirs(CACHE_DIR)

    local_path = url_to_filename(url)

    with open(local_path, 'wb') as f:
        f.write(content)

def load_local(url):
    local_path = url_to_filename(url)
    if not os.path.exists(local_path):
        return None

    with open(local_path, 'rb') as f:
        return f.read()

def get_content(url):
    content = load_local(url)

    if content is None:
        r = requests.get(url)
        content = r.content
        store_local(url,content)

    return content

def html_processor(html_divs):
    biz_name = None
    listingID = None
    ypid = None
    streetAddress = None
    addressLocality = geo_location_terms
    addressRegion = None
    postalCode = None
    phone = None
    websiteIsYp = None
    site_url = None
    ypad = None
    key = None
    biz_type = None
    yp_url_regex = re.compile('local.yp.com')

    data = {}

    for div in html_divs:
        site_url = None
        json_data = json.loads(div['data-analytics'])
        name_div = div.find('div', {'class':'info'})
        biz_name = name_div.find('a', { 'class':'business-name'}).text.strip()


        listingID = json_data['listing_id']
        ypid = json_data['ypid']

        if div.find('span', { 'itemprop':'streetAddress' }):
            streetAddress = div.find('span', { 'class':'street-address' }).text.strip()

        if div.find('span', { 'itemprop':'addressRegion' }):
            addressRegion = div.find('span', { 'itemprop':'addressRegion' }).text.strip()

        if div.find('span', { 'itemprop':'postalCode' }):
            postalCode = div.find('span', { 'itemprop':'postalCode' }).text.strip()

        if div.find('div', { 'itemprop':'telephone' }):
            phone = div.find('div', { 'itemprop':'telephone'}).text.strip()

        if div.find('a', { 'class':'track-visit-website'}):
            site_url = div.find('a', { 'class':'track-visit-website'}).get('href')
            if yp_url_regex.search(site_url):
                websiteIsYp = True
            else:
                websiteIsYp = False

        key = ypid + "-" + listingID

        data = {
            'biz_name': biz_name,
            'listing_id': listingID,
            'yellow_page_id': ypid,
            'streetaddress': streetAddress, 
            'address_locality' : addressLocality, 
            'addressregion' :  addressRegion, 
            'postalcode' : postalCode,
            'phone_num' : phone,
            'website_url' : site_url,
            'websiteisyp' :  websiteIsYp,
            'isad': ypad,
            'biz_type': search_terms,
            'key' : key

        }
        db['yellowpages_austintx'].upsert(data, ['key'])

def scrape_content(content):
    soup = BeautifulSoup(content, "html.parser")

    if soup.find('div', { 'class':'search-results organic' }):
        organic_search_results = soup.find('div', {'class':'search-results organic'})
        divs_organic = organic_search_results
        html_processor(divs_organic)

def iterate_page():
    counter = 1

    while (counter < 75):
        url = BASEURL + search_terms + geo_location_url + geo_location_terms + page + "%d" % counter
        print("On url: %s " % url)
        content = get_content(url)
        scrape_content(content)

        counter += 1
        time.sleep(2.5)

if __name__ == '__main__':
    iterate_page() 