import csv
import requests
from urllib.parse import urlparse

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
    'Tracking template',
    'Custom parameter1'
]

_headers = {
    'Accept': 'application/json',
    'Content-Type': 'application/json'
}

def extract_json(url, data={}, verify_ssl=True) -> dict:
    formatted = []
    json = None
    try:
        response = requests.request('GET', url=url, headers=_headers, params=data, verify=verify_ssl)
        json = response.json()
    except requests.exceptions.HTTPError as err:
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

    next_url = json['next']
    results = json['results']
    parsed_url = urlparse(url)
    base_url = parsed_url.scheme + '://' + parsed_url.netloc
    search_type = data['search_type'] if 'search_type' in data else ''
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
            if search_type == 'rent' and (listing_type == 'sale' or listing_type == 'sale/rent'):
                continue

            features = []
            if row['bedrooms']:
                features.append(f"{row['bedrooms']} bed")
            if row['bathrooms']:
                features.append(f"{row['bathrooms']} bath")
            description = ', '.join(features)
            image_url = row['images'][0]['url']
            price = row['price_min'] if row['listing_type'] in ['sale', 'sale/rent'] else row['rent_min']
            if price:
                price = f"{price:,} {currency}"

            listing = [
                row['id'],
                row['headline_en'],
                base_url + row['url'],
                image_url,
                f"{row['address_locality']}, {row['address_subdivision']}",
                description,
                price,
                row['category_name'],
                listing_type,
                f"{row['headline_en']} for {listing_type} in {row['address_line_2']} ID {row['id']}",
                f"{row['address_locality']}, {row['address_subdivision']}, {country}"
                '',
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

    return {
        'next': next_url,
        'data': formatted
    }


def write_to_csv(filename='exported/export.csv', csv_data=[], mode='w'):
    with open(filename, mode, encoding='UTF8', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(CSV_HEAD)
        writer.writerows(csv_data)
