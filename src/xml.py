import json
from lxml import etree
import uuid
from db import insert_data, connect_to_db 

def convert_to_uuid(text_id):
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, text_id))

def extract_data(elem):
    try:
        brand = int(elem.findtext('vendorCode', 0))
    except ValueError:
        brand = 0

    try:
        seller_id = int(elem.findtext('vendor', 0))
    except ValueError:
        seller_id = 0

    category_path = elem.findtext('categoryPath', '').split('/')
    category_lvl_1 = category_path[0] if len(category_path) > 0 else ''
    category_lvl_2 = category_path[1] if len(category_path) > 1 else ''
    category_lvl_3 = category_path[2] if len(category_path) > 2 else ''
    category_remaining = '/'.join(category_path[3:]) if len(category_path) > 3 else ''

    data = {
        'uuid': convert_to_uuid(elem.get('id')),
        'marketplace_id': 1,
        'product_id': int(elem.get('product_id', 0)),
        'title': elem.findtext('name', ''),
        'description': elem.findtext('description', ''),
        'brand': brand,
        'seller_id': seller_id,
        'seller_name': elem.findtext('vendor', ''),
        'first_image_url': elem.findtext('picture', ''),
        'category_id': int(elem.findtext('categoryId', 0)),
        'category_lvl_1': category_lvl_1,
        'category_lvl_2': category_lvl_2,
        'category_lvl_3': category_lvl_3,
        'category_remaining': category_remaining,
        'features': json.dumps({}),
        'rating_count': 0, 
        'rating_value': 0.0,  
        'price_before_discounts': float(elem.findtext('price', '0.0')),
        'discount': 0.0,  
        'price_after_discounts': float(elem.findtext('price', '0.0')),
        'bonuses': 0, 
        'sales': 0, 
        'currency': elem.findtext('currencyId', 'Unknown'),
        'barcode': int(elem.findtext('barcode', '0'))
    }
    return data

def process_xml(filename, db_connection):
    context = etree.iterparse(filename, events=('end',), tag='offer')
    for event, elem in context:
        if event == 'end': 
            data = extract_data(elem)
            insert_data(db_connection, data)
            elem.clear()