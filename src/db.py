import psycopg2
from psycopg2 import extras
import uuid
import time
import lxml.etree as ET

def connect_to_db():
    retries = 5
    delay = 5
    while retries > 0:
        try:
            conn = psycopg2.connect(
                dbname='testadmin',
                user='testadmin',
                password='testadmin',
                host='db',
                port='5432'
            )
            print("Connected to the database.")
            return conn
        except psycopg2.OperationalError as e:
            retries -= 1
            print(f"Failed to connect to the database: {e}. Retrying in {delay} seconds...")
            time.sleep(delay)
    raise Exception("Could not connect to the database after several retries.")

def create_table(conn):
    cursor = conn.cursor()
    try:
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS public.sku
            (
                uuid                   UUID PRIMARY KEY,
                marketplace_id         INTEGER,
                product_id             BIGINT,
                title                  TEXT,
                description            TEXT,
                brand                  INTEGER,
                seller_id              INTEGER,
                seller_name            TEXT,
                first_image_url        TEXT,
                category_id            INTEGER,
                category_lvl_1         TEXT,
                category_lvl_2         TEXT,
                category_lvl_3         TEXT,
                category_remaining     TEXT,
                features               JSON,
                rating_count           INTEGER,
                rating_value           DOUBLE PRECISION,
                price_before_discounts REAL,
                discount               DOUBLE PRECISION,
                price_after_discounts  REAL,
                bonuses                INTEGER,
                sales                  INTEGER,
                inserted_at            TIMESTAMP DEFAULT NOW(),
                updated_at             TIMESTAMP DEFAULT NOW(),
                currency               TEXT,
                barcode                BIGINT
            );
            comment on column public.sku.uuid is 'id товара в нашей бд';
            comment on column public.sku.marketplace_id is 'id маркетплейса';
            comment on column public.sku.product_id is 'id товара в маркетплейсе';
            comment on column public.sku.title is 'название товара';
            comment on column public.sku.description is 'описание товара';
            comment on column public.sku.category_lvl_1 is 'Первая часть категории товара';
            comment on column public.sku.category_lvl_2 is 'Вторая часть категории товара';
            comment on column public.sku.category_lvl_3 is 'Третья часть категории товара';
            comment on column public.sku.category_remaining is 'Остаток категории товара';
            comment on column public.sku.features is 'Характеристики товара';
            comment on column public.sku.rating_count is 'Кол-во отзывов о товаре';
            comment on column public.sku.rating_value is 'Рейтинг товара (0-5)';
            comment on column public.sku.barcode is 'Штрихкод';
            CREATE INDEX IF NOT EXISTS sku_brand_index ON public.sku (brand);
            CREATE UNIQUE INDEX IF NOT EXISTS sku_uuid_uindex ON public.sku (uuid);
        ''')
        conn.commit()
        print("Table created successfully.")
    except Exception as e:
        conn.rollback()
        print(f"Failed to create table: {e}")
    finally:
        cursor.close()

def parse_xml_file(file_path):
    context = ET.iterparse(file_path, events=('end',), tag='offer')
    for event, elem in context:
        yield elem
        elem.clear()

def extract_data_from_offer(offer):
    data = {
        'uuid': uuid.uuid5(uuid.NAMESPACE_URL, f"{offer.get('marketplace_id')}:{offer.get('id')}"),
        'marketplace_id': offer.get('marketplace_id', None),
        'product_id': offer.get('id', None),
        'title': offer.findtext('name', default=None),
        'description': offer.findtext('description', default=None),
        'brand': offer.findtext('vendor', default=None), 
        'seller_id': None, 
        'seller_name': offer.findtext('vendor', default=None), 
        'first_image_url': offer.findtext('picture', default=None),
        'category_id': offer.findtext('categoryId', default=None),
        'category_lvl_1': None, 
        'category_lvl_2': None,  
        'category_lvl_3': None, 
        'category_remaining': None,  
        'features': '{}',  
        'rating_count': 0,  
        'rating_value': 0.0,  
        'price_before_discounts': offer.findtext('oldprice', default=None),
        'discount': 0.0,  
        'price_after_discounts': offer.findtext('price', default=None),
        'bonuses': 0,  
        'sales': 0,  
        'currency': offer.findtext('currencyId', default=None),
        'barcode': int(offer.findtext('barcode', default=0)) 
    }
    print(data)
    
    return data

def insert_data(conn, data):
    """Insert or update data in the database."""
    cursor = conn.cursor()
    insert_query = """
    INSERT INTO public.sku (
        uuid, marketplace_id, product_id, title, description, brand, seller_id, seller_name,
        first_image_url, category_id, category_lvl_1, category_lvl_2, category_lvl_3,
        category_remaining, features, rating_count, rating_value, price_before_discounts,
        discount, price_after_discounts, bonuses, sales, currency, barcode
    ) VALUES (
        %(uuid)s, %(marketplace_id)s, %(product_id)s, %(title)s, %(description)s, %(brand)s, %(seller_id)s, %(seller_name)s,
        %(first_image_url)s, %(category_id)s, %(category_lvl_1)s, %(category_lvl_2)s, %(category_lvl_3)s,
        %(category_remaining)s, %(features)s, %(rating_count)s, %(rating_value)s,
        %(price_before_discounts)s, %(discount)s, %(price_after_discounts)s,
        %(bonuses)s, %(sales)s, %(currency)s, %(barcode)s
    ) ON CONFLICT (uuid) DO UPDATE SET
        title = EXCLUDED.title,
        description = EXCLUDED.description,
        brand = EXCLUDED.brand,
        seller_id = EXCLUDED.seller_id,
        seller_name = EXCLUDED.seller_name,
        first_image_url = EXCLUDED.first_image_url,
        category_id = EXCLUDED.category_id,
        category_lvl_1 = EXCLUDED.category_lvl_1,
        category_lvl_2 = EXCLUDED.category_lvl_2,
        category_lvl_3 = EXCLUDED.category_lvl_3,
        category_remaining = EXCLUDED.category_remaining,
        features = EXCLUDED.features,
        rating_count = EXCLUDED.rating_count,
        rating_value = EXCLUDED.rating_value,
        price_before_discounts = EXCLUDED.price_before_discounts,
        discount = EXCLUDED.discount,
        price_after_discounts = EXCLUDED.price_after_discounts,
        bonuses = EXCLUDED.bonuses,
        sales = EXCLUDED.sales,
        currency = EXCLUDED.currency,
        barcode = EXCLUDED.barcode
    """
