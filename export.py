import os
import time
from dotenv import load_dotenv
from utils import extract_json, write_to_csv
from urllib.parse import urlencode

load_dotenv()


def get_next_data(url='', filename='', others={}, verify_ssl=True) -> int:
    response = extract_json(url, others=others, verify_ssl=verify_ssl)
    data_rows = response['data']
    next = response['next']
    count = len(data_rows)

    write_to_csv(filename, data_rows, 'a')
    if next:
        time.sleep(1)
        count = count + get_next_data(next, filename, others)

    return count


def get_first_data(req={}, verify_ssl=True, mode='w') -> int:
    response = extract_json(req['url'], req['filters'], req['others'], verify_ssl)
    data_rows = response['data']
    next = response['next']
    count = len(data_rows)

    write_to_csv(req['filename'], data_rows, mode)
    if next:
        time.sleep(1)
        count = count + get_next_data(next, req['filename'], req['others'], verify_ssl)

    return count

def export_data():
    page_size = 200
    url_local = os.getenv('LOCAL_BASE_URL') + '/api/listing/'
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
            },
            'others': {}
        },
        {
            'filename': 'exported/feed2.csv',
            'url': url_reakh,
            'filters': {
                'status': 'current',
                'get_child_listings': True,
                'search_type': 'rent',
                'categories': ['House', 'Villa', 'Condo', 'Apartment', 'Studio'],
                'page_size': page_size  
            },
            'others': {
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
                    7221, # Ellington Property
                    7233, # LIA JC Global
                    7235, # VANGUARD INVESTMENT AND CONSULTANCY SERVICES
                    1451, # Century 21 Fuji
                    6980, # T'S Home
                    7143, # Full Trust
                    7001, # Expert realty (apimo)
                    6104, # Expert realty
                    6927 # MLS Property Cambodia
                ], 
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
            },
            'others': {}
        }
    ]

    for req in export_list:
        print(f"<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<< [process: {req['filename']}]")
        count = 0
        if req['others']:
            search_type = req['filters']['search_type'] if 'search_type' in req['filters'] else ''
            offices = req['others']['offices'] if 'offices' in req['others'] else []
            rent_min__gt = req['others']['rent_min__gt'] if 'rent_min__gt' in req['others'] else ''
            if search_type == 'rent' and rent_min__gt:
                req['filters']['price_min__gte'] = rent_min__gt
            if offices:
                idx_office = 1
                for office_id in offices:
                    req['filters']['offices'] = [office_id]
                    mode = 'w' if idx_office == 1 else 'a'
                    count += get_first_data(req, True, mode)
                    idx_office += 1
            else:
                count += get_first_data(req)
        else:
            count += get_first_data(req)

        print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
        print(f"{count:,} data exported to {req['filename']}")
        print('>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>')


if __name__ == '__main__':
    export_data()