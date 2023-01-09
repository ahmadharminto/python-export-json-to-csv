import csv
import os
import requests
import gspread
import pandas as pd
import smtplib
from dotenv import load_dotenv
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from urllib.parse import urlparse, parse_qs, urlencode

load_dotenv()

CSV_HEAD = [
    'Listing ID',
    'Listing Name',
    'Final URL',
    'Image URL',
    'City name',
    'Description',
    'Price',
    'Property type',
    'Listing type',
    'Contextual keywords',
    'Address',
    'Tracking template'
]

_headers = {
    'Accept': 'application/json',
    'Content-Type': 'application/json'
}

CWD = str(Path(__file__).parent.resolve()) + '/'


def extract_json_al(url, data={}, others={}, verify_ssl=True) -> dict:
    formatted = []
    json = None
    try:
        response = requests.request('GET', url=url, headers=_headers, params=data, verify=verify_ssl)
        generated_url = response.history[0].url if response.history else response.url
        print(f"--> get data from : {generated_url}")
        json = response.json()
    except requests.exceptions.HTTPError as e:
        print(e)
        print(f"Unable to get data from {url}")
    except Exception as e:
        print(e)
        print(f"Unable to get data from {url}")
    if not json:
        return {
            'next': None,
            'data': formatted
        }

    next_url = json['next'] if 'next' in json else None
    results = json['results'] if 'results' in json else []

    if not next_url and not results:
        print(json)

    parsed_url = urlparse(url)
    base_url = parsed_url.scheme + '://' + parsed_url.netloc
    url_queries = None
    if parsed_url.query:
        url_queries = parse_qs(parsed_url.query)

    search_type = data['search_type'] if 'search_type' in data else ''
    rent_min__gt = data['rent_min__gt'] if 'rent_min__gt' in data else ''

    if not search_type and url_queries:
        search_type = url_queries['search_type'][0] if 'search_type' in url_queries else ''
    if not rent_min__gt and url_queries:
        rent_min__gt = url_queries['rent_min__gt'][0] if 'rent_min__gt' in url_queries else ''

    if not search_type and others:
        search_type = others['search_type'] if 'search_type' in others else ''
    if not rent_min__gt and url_queries:
        rent_min__gt = others['rent_min__gt'] if 'rent_min__gt' in others else ''

    country = ''
    currency = ''

    if 'realestate.com.kh' in base_url:
        country = 'Cambodia'
        currency = 'USD'
    elif 'hausples.com.pg' in base_url:
        country = 'Papua New Guinea'
        currency = 'PGK'

    for row in results:
        try:
            if not row['images']:
                continue

            listing_type = row['listing_type']
            if search_type in ['sale', 'both'] and listing_type == 'rent':
                continue
            if search_type == 'rent':
                if (listing_type == 'sale' or listing_type == 'sale/rent'):
                    continue
                if (rent_min__gt and int(row['rent_min']) <= int(rent_min__gt)):
                    continue

            features = []
            if row['bedrooms']:
                features.append(f"{row['bedrooms']} bed")
            if row['bathrooms']:
                features.append(f"{row['bathrooms']} bath")
            headline = row['headline_en'].strip()
            if headline.startswith('"') and headline.endswith('"'):
                headline = headline[1:-1]
            description = ', '.join(features)
            image_url = row['images'][0]['url']
            price = row['price_min'] if row['listing_type'] in ['sale', 'sale/rent'] else row['rent_min']
            if price:
                price = f"{price:,} {currency}"

            listing = [
                row['id'],
                headline,
                base_url + row['url'],
                image_url,
                f"{row['address_locality']}, {row['address_subdivision']}",
                description,
                price,
                row['category_name'],
                listing_type,
                f"{headline} for {listing_type} in {row['address_line_2']} ID {row['id']}",
                f"{row['address_locality']}, {row['address_subdivision']}, {country}"
                ''
            ]
            formatted.append(listing)
        except Exception as e:
            print('--------------------- begin : error ---------------------')
            print(e)
            for k,v in row.items():
                if k not in [
                    'id', 
                    'headline_en', 
                    'url', 
                    'listing_type',
                    'bedrooms',
                    'bathrooms',
                    'images',
                    'price_min',
                    'rent_min',
                    'address_locality',
                    'address_subdivision',
                    'category_name',
                    'address_line_2'
                ]:
                    continue
                print(f"{k} : {v}")
            print('--------------------- end : error ---------------------')

    print(f"...{len(formatted):,} data read")

    return {
        'next': next_url,
        'data': formatted
    }


def extract_json_appr(url, data={}, others={}, verify_ssl=True) -> dict:
    formatted = []
    json = None
    try:
        response = requests.request('GET', url=url, headers=_headers, params=data, verify=verify_ssl)
        generated_url = response.history[0].url if response.history else response.url
        print(f"--> get data from : {generated_url}")
        json = response.json()
    except requests.exceptions.HTTPError as e:
        print(e)
        print(f"Unable to get data from {url}")
    except Exception as e:
        print(e)
        print(f"Unable to get data from {url}")
    if not json:
        return {
            'next': None,
            'data': formatted
        }

    parsed_url = urlparse(url)
    base_url = parsed_url.scheme + '://' + parsed_url.netloc    
    url_queries = {}
    if parsed_url.query:
        url_queries = parse_qs(parsed_url.query)
        for k,v in url_queries.items():
            url_queries[k] = v[0]

    next_url = None
    last_page = json['last_page']
    page = url_queries['page'] if 'page' in url_queries else 1
    page = int(page)
    if page < int(last_page):
        page += 1
        url_queries['page'] = page
        url_params = urlencode(url_queries)
        next_url = f"{base_url}{parsed_url.path}?{url_params}"        

    results = json['results'] if 'results' in json else []

    if not next_url and not results:
        print(json)

    country = ''
    currency = ''

    if 'realestate.com.kh' in base_url:
        country = 'Cambodia'
        currency = 'USD'
    elif 'hausples.com.pg' in base_url:
        country = 'Papua New Guinea'
        currency = 'PGK'

    for row in results:
        try:
            if not row['images']:
                continue

            listing_type = row['listing_type']
            headline = row['headline'].strip()
            if headline.startswith('"') and headline.endswith('"'):
                headline = headline[1:-1]
            description = headline
            image_url = row['images'][0]['url']
            price = row['display_price'] if listing_type in ['sale', 'sale/rent'] else row['display_rent']
            price = price.replace('K', '')

            listing = [
                row['id'],
                headline,
                base_url + row['url'],
                image_url,
                f"{row['address']}",
                description,
                f"{price} {currency}",
                row['category_name'],
                listing_type,
                f"{row['title_img_alt']}",
                f"{row['address']}, {country}"
                ''
            ]
            formatted.append(listing)
        except Exception as e:
            print('--------------------- begin : error ---------------------')
            print(e)
            for k,v in row.items():
                if k not in [
                    'id', 
                    'headline_en', 
                    'url', 
                    'listing_type',
                    'bedrooms',
                    'bathrooms',
                    'images',
                    'price_min',
                    'rent_min',
                    'address_locality',
                    'address_subdivision',
                    'category_name',
                    'address_line_2'
                ]:
                    continue
                print(f"{k} : {v}")
            print('--------------------- end : error ---------------------')

    print(f"...{len(formatted):,} data read")

    return {
        'next': next_url,
        'data': formatted
    }


def extract_json(url, data={}, others={}, verify_ssl=True) -> dict:
    if "/api/portal/pages/results" in url:
        return extract_json_appr(url, data, others, verify_ssl)
    return extract_json_al(url, data, others, verify_ssl)


def write_to_csv(filename='exported/export.csv', csv_data=[], mode='w'):
    with open(CWD + filename, mode, encoding='utf-8', newline='') as f:
        writer = csv.writer(f, delimiter=',', lineterminator='\r\n', quoting=csv.QUOTE_NONNUMERIC)
        if mode == 'w':
            writer.writerow(CSV_HEAD)
        writer.writerows(csv_data)
        print(f"...{len(csv_data):,} data writen")


def init_google_spreadsheet(spreadsheet_name=''):
    try:
        google_spread = gspread.service_account(filename=CWD + 'google-service.json')
        return google_spread.open(spreadsheet_name)
    except Exception as e:
        print(f'Error : {e}')
        return None


def write_to_gsheet(spreadsheet, sheet_name='Feed1', csv_data=[], mode='w'):
    if not spreadsheet:
        print("Error : spreadsheet not initialized yet")
        return

    try:
        worksheet = spreadsheet.worksheet(sheet_name)
        if mode == 'w':
            worksheet.clear()
            head_df = pd.DataFrame([CSV_HEAD])
            worksheet.update(head_df.values.tolist())
        csv_df = pd.DataFrame(csv_data)
        worksheet.append_rows(csv_df.values.tolist())
        print(f"...{len(csv_data):,} data writen")
    except Exception as e:
        print(f'Error : {e}')


def send_mail_notif(title='', body=''):
    sender_address = os.getenv('MAIL_USERNAME')
    sender_pass = os.getenv('MAIL_PASSWORD')
    receiver_address = os.getenv('MAIL_TO').split(',')
    mail_host = os.getenv('MAIL_HOST')
    mail_port = os.getenv('MAIL_PORT')
    
    message = MIMEMultipart('alternative')
    message['From'] = sender_address
    message['To'] = ','.join(receiver_address)
    message['Subject'] = title   
    message.attach(MIMEText(format_body_html(body), 'html'))

    session = smtplib.SMTP(mail_host, mail_port)
    session.ehlo()
    session.starttls()
    session.login(sender_address, sender_pass)
    session.sendmail(sender_address, receiver_address, message.as_string())
    session.quit()

    print('~~~~~~~~~Mail Sent~~~~~~~~~')


def format_body_html(body=''):
    return f' \
        <html> \
        <head></head> \
        <body>{body}</body> \
        </html>'