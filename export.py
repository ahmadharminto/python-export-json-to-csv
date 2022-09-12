import os
import time
from dotenv import load_dotenv
from utils import extract_json, write_to_csv
from urllib.parse import urlencode

load_dotenv()


def get_next_data(url='', filename='') -> int:
    response = extract_json(url)
    data_rows = response['data']
    next = response['next']
    count = len(data_rows)

    write_to_csv(filename, data_rows, 'a')
    if next:
        time.sleep(1)
        count = count + get_next_data(next, filename)

    return count


def export_data():
    page_size = 100
    #url_local = os.getenv('LOCAL_BASE_URL') + '/api/listing/'
    url_reakh = os.getenv('REAKH_BASE_URL') + '/api/listing/'
    url_hp = os.getenv('HAUSPLES_BASE_URL') + '/api/listing/'

    export_list = [
        {
            'filename': 'exported/feed1.csv',
            'url': url_reakh,
            'filters': {
                'status': 'current',
                'get_child_listings': True,
                'search_type': 'sale',
                'categories': ['project', 'House', 'Villa', 'Condo', 'Apartment', 'Studio'],
                'page_size': page_size
            }
        },
        {
            'filename': 'exported/feed2.csv',
            'url': url_reakh,
            'filters': {
                'status': 'current',
                'get_child_listings': True,
                'search_type': 'rent',
                'categories': ['House', 'Villa', 'Condo', 'Apartment', 'Studio'],
                'rent_min__gt': 700,
                'offices': [
                    6889, # IPS Cambodia
                    7206, # GC Realty
                    6020, # Rentex Cambodia
                    6500, # CBRE Cambodia
                    1743, # Knight Frank
                    1453, # Century 21 Advance Property
                    4453, # Daka Kun Realty
                    6869, # Coldwell Banker Cambodia
                    1420, # Century 21 Imperial Realty
                    7144, # Angkor Leasing Homes
                    6927 # MLS Property Cambodia
                ], 
                'page_size': page_size
            }
        },
        {
            'filename': 'exported/feed3.csv',
            'url': url_hp,
            'filters': {
                'status': 'current',
                'get_child_listings': True,
                'search_type': 'rent',
                'categories': ['Apartment', 'Townhouse', 'ServicedApartment'],            
                'page_size': page_size
            }
        }
    ]

    for req in export_list:
        print('<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<')
        print(f"get data from : {req['url']}?{urlencode(req['filters'])}")
        response = extract_json(req['url'], req['filters'])
        data_rows = response['data']
        next = response['next']
        count = len(data_rows)

        write_to_csv(req['filename'], data_rows)
        if next:
            time.sleep(1)
            count = count + get_next_data(next, req['filename'])

        print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
        print(f"{count:,} data exported to {req['filename']}")
        print('>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>')


if __name__ == '__main__':
    export_data()