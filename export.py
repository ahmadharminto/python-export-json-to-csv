import getopt
import os
import sys
import time
from dotenv import load_dotenv
from utils import (
    extract_json, 
    init_google_spreadsheet, 
    write_to_csv, 
    write_to_gsheet,
    send_mail_notif
)

load_dotenv()

EXPORT_TO = os.getenv('EXPORT_TO', default='csv')
if EXPORT_TO not in ['csv', 'google_spreadsheet']:
    EXPORT_TO = 'csv'


def get_next_data(url='', filename='', others={}, verify_ssl=True, spreadsheet=None) -> int:
    response = extract_json(url, others=others, verify_ssl=verify_ssl)
    data_rows = response['data']
    next = response['next']
    count = len(data_rows)

    if EXPORT_TO == 'csv':
        write_to_csv(filename, data_rows, 'a')
    elif EXPORT_TO == 'google_spreadsheet':
        write_to_gsheet(spreadsheet, filename, data_rows, 'a')
    if next:
        time.sleep(1)
        count = count + get_next_data(next, filename, others, verify_ssl, spreadsheet)

    return count


def get_first_data(req={}, verify_ssl=True, mode='w') -> int:
    response = extract_json(req['url'], req['filters'], req['others'], verify_ssl)
    data_rows = response['data']
    next = response['next']
    count = len(data_rows)
    spreadsheet = None

    filename = ''
    if EXPORT_TO == 'csv':
        filename = req['filename']
        write_to_csv(filename, data_rows, mode)
    elif EXPORT_TO == 'google_spreadsheet':
        spreadsheet = init_google_spreadsheet(req['spreadsheet_name'])
        filename = req['sheetname']
        write_to_gsheet(spreadsheet, filename, data_rows, mode)
    
    if next:
        time.sleep(1)
        count = count + get_next_data(next, filename, req['others'], verify_ssl, spreadsheet)

    return count


def export_data(argv):
    page_size = 200
    # url_local = os.getenv('LOCAL_BASE_URL') + '/api/listing/'
    url_reakh = os.getenv('REAKH_BASE_URL') + '/api/listing/'
    url_hp = os.getenv('HAUSPLES_BASE_URL') + '/api/listing/'

    export_list = [
        {
            'filename': 'exported/feed1.csv',
            'sheetname': 'KH_ForSale_Condo-House-project-Villa',
            'spreadsheet_name': 'KH_ForSale_Condo-House-project-Villa',
            'spreadsheet_url': 'https://docs.google.com/spreadsheets/d/1TPbzbha-Pce9SI5XV2hYi5Gd7jQ38LZ1arXtiCV53fo/',
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
            'sheetname': 'KH_ForRent_Above-USD700-Referral-Agencies',
            'spreadsheet_name': 'KH_ForRent_Above-USD700-Referral-Agencies',
            'spreadsheet_url': 'https://docs.google.com/spreadsheets/d/1d2fO_Hdt3pZJDCknsLZCzui8gwcAyLGlUCxrbcXhqRg/',
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
            'sheetname': 'HP_Rentalproperties',
            'spreadsheet_name': 'HP_Rentalproperties',
            'spreadsheet_url': 'https://docs.google.com/spreadsheets/d/1dOQPmjl70jtjO_rY7-Mz1iNke9f3W8NSQdGkLuF9wWw/',
            'url': url_hp,
            'filters': {
                'status': 'current',
                'get_child_listings': True,
                'search_type': 'rent',
                'categories': ['Apartment', 'Townhouse', 'ServicedApartment'],            
                'page_size': page_size
            },
            'others': {}
        },
        {
            'filename': 'exported/feed4.csv',
            'sheetname': 'HP_Allprojects',
            'spreadsheet_name': 'HP_Allprojects',
            'spreadsheet_url': 'https://docs.google.com/spreadsheets/d/1st2LqwUyyIZI4trkmtQZs-c3TZZXVK2FW5j7-w7az-o/',
            'url': None,
            'urls': [
                f"https://www.hausples.com.pg/api/portal/pages/results/?active_tab=recent&order_by=relevance&property_type=residential&q=office%3A%20Hausples%20Support&search_type=rent&page_size={page_size}",
                f"https://www.hausples.com.pg/api/portal/pages/results/?active_tab=recent&order_by=relevance&property_type=project&q=office%3A%20Hausples%20Support&search_type=rent&page_size={page_size}",
                f"https://www.hausples.com.pg/api/portal/pages/results/?active_tab=recent&order_by=relevance&property_type=commercial&q=office%3A%20Hausples%20Support&search_type=rent&page_size={page_size}",
                f"https://www.hausples.com.pg/api/portal/pages/results/?active_tab=recent&order_by=relevance&property_type=compounds&q=office%3A%20Hausples%20Support&search_type=rent&page_size={page_size}"
            ],
            'filters': {},
            'others': {}
        },
        {
            'filename': 'exported/feed5.csv',
            'sheetname': 'KH_ForSale_newdev-condo-apartments-houses-boreys',
            'spreadsheet_name': 'KH_ForSale_newdev-condo-apartments-houses-boreys',
            'spreadsheet_url': 'https://docs.google.com/spreadsheets/d/1E0ruQCfUfAFSuNvTF8VLkihS4Ak6B0w-eZTRGZRUqwk/',
            'url': url_reakh,
            'filters': {
                'status': 'current',
                'get_child_listings': True,
                'search_type': 'sale',
                'categories': ['project', 'Condo', 'Apartment', 'House', 'borey'],
                'page_size': page_size
            },
            'others': {}
        },
        {
            'filename': 'exported/feed6.csv',
            'sheetname': 'KH_ForRent_newdev-condo-apartments-houses-boreys',
            'spreadsheet_name': 'KH_ForRent_newdev-condo-apartments-houses-boreys',
            'spreadsheet_url': 'https://docs.google.com/spreadsheets/d/1VS6slDy7I1niTcmqDCzaggEYzpofaBAov9XJGNPmAxM/',
            'url': url_reakh,
            'filters': {
                'status': 'current',
                'get_child_listings': True,
                'search_type': 'rent',
                'categories': ['project', 'Condo', 'Apartment', 'House', 'borey'],
                'page_size': page_size
            },
            'others': {}
        },
    ]

    arg_index = ""
    arg_help = f"{argv[0]} -i <export_list_index:0-{len(export_list)-1}>"

    def print_help():
        print(arg_help)
        print(f"example with index : {argv[0]} -i 0,1,2 | {argv[0]} --index 0,1,2")
        for i in range(len(export_list)):
            sheet_name = export_list[i].get("sheetname")
            print(f"{i} : {sheet_name}")

    try:
        opts, args = getopt.getopt(argv[1:], "hi:", ["help", "index=",])
    except:
        print_help()
        sys.exit(2)

    for opt, arg in opts:
        if opt in ("-h", "--help"):
            print_help()
            sys.exit(2)
        elif opt in ("-i", "--index"):
            arg_index = arg

    arg_index_list = arg_index.split(",") if arg_index else []
    arg_index_list = list(map(int, arg_index_list))

    mail_messages = []
    i = 0
    processed = 0
    for req in export_list:
        if arg_index_list and i not in arg_index_list:
            i += 1
            continue
        filename = ''
        filename_on_mail = ''
        if EXPORT_TO == 'csv':
            filename = req['filename']
            filename_on_mail = req['filename']
        elif EXPORT_TO == 'google_spreadsheet':
            filename = req['spreadsheet_name']
            filename_on_mail = f"<a href=\"{req['spreadsheet_url']}\" target=\"_blank\">{req['spreadsheet_name']}</a>"

        print(f"<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<< [process: {filename}]")
        count = 0
        if req['url']:
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
        else:
            if req['urls']:
                idx_url = 1
                for url in req['urls']:
                    mode = 'w' if idx_url == 1 else 'a'
                    req['url'] = url
                    count += get_first_data(req, True, mode)
                    idx_url += 1

        msg = f"{count:,} data exported to {filename_on_mail}"
        mail_messages.append(msg)
        print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
        print(msg)
        print('>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>')
        i += 1
        processed += 1

    if processed > 0:
        send_mail_notif('Google Ads Feed Update', f"<b>Information : </b><ul><li>{'</li><li>'.join(mail_messages)}</li></ul>")

if __name__ == '__main__':
    export_data(sys.argv)