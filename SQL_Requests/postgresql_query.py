create_query = '''
CREATE TABLE if not exists temp_data (
ID BIGINT,
ITEMCODE VARCHAR,
ITEMNAME VARCHAR,
FICHENO VARCHAR,
DATE_ TIMESTAMP DEFAULT NULL,
AMOUNT FLOAT,
PRICE FLOAT,
LINENETTOTAL FLOAT,
LINENET FLOAT,
BRANCHNR INT,
BRANCH VARCHAR,
SALESMAN VARCHAR,
CITY VARCHAR,
REGION VARCHAR,
LATITUDE FLOAT,
LONGITUDE FLOAT,
CLIENTCODE VARCHAR,
CLIENTNAME VARCHAR,
BRANDCODE VARCHAR,
BRAND VARCHAR,
CATEGORY_NAME1 VARCHAR,
CATEGORY_NAME2 VARCHAR,
CATEGORY_NAME3 VARCHAR,
STARTDATE TIMESTAMP DEFAULT NULL,
ENDDATE TIMESTAMP DEFAULT NULL,
GENDER VARCHAR
)
'''
# Используем позиционные параметры (%s)
insert_query1 = r'''
INSERT INTO temp_data (
    ID, ITEMCODE, ITEMNAME, FICHENO, DATE_, 
    AMOUNT, PRICE, LINENETTOTAL, LINENET,
    BRANCHNR, BRANCH, SALESMAN, CITY, REGION,
    LATITUDE, LONGITUDE, CLIENTCODE, CLIENTNAME,
    BRANDCODE, BRAND, CATEGORY_NAME1, CATEGORY_NAME2, 
    CATEGORY_NAME3, STARTDATE, ENDDATE, GENDER
) VALUES (
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s, %s
)
'''
create_query2 = '''
CREATE TABLE if not exists branches (
  branch_id SERIAL PRIMARY KEY,
  branch_nr INT UNIQUE ,
  branch_name VARCHAR ,
  city VARCHAR ,
  region VARCHAR ,
  latitude FLOAT,
  longitude FLOAT
);


CREATE TABLE if not exists salesmen (
  salesman_id SERIAL PRIMARY KEY,
  salesman_name VARCHAR ,
  branch_id INT REFERENCES branches(branch_id)
);


CREATE TABLE if not exists clients (
  client_id SERIAL PRIMARY KEY,
  client_code VARCHAR UNIQUE ,
  client_name VARCHAR ,
  gender VARCHAR(1)
);

CREATE TABLE if not exists brands (
  brand_id SERIAL PRIMARY KEY,
  brand_code VARCHAR UNIQUE ,
  brand_name VARCHAR 
);


CREATE TABLE if not exists categories (
  category_id SERIAL PRIMARY KEY,
  category_name1 VARCHAR ,
  category_name2 VARCHAR,
  category_name3 VARCHAR
);


CREATE TABLE if not exists items (
  item_id SERIAL PRIMARY KEY,
  item_code VARCHAR UNIQUE ,
  item_name VARCHAR ,
  brand_id INT REFERENCES brands(brand_id),
  category_id INT REFERENCES categories(category_id),
  price FLOAT 
);


CREATE TABLE if not exists sales (
  sale_id SERIAL PRIMARY KEY,
  fisheno VARCHAR ,
  date TIMESTAMP ,
  client_id INT REFERENCES clients(client_id),
  salesman_id INT REFERENCES salesmen(salesman_id),
  start_date TIMESTAMP,
  end_date TIMESTAMP
);


CREATE TABLE if not exists sale_items (
  sale_item_id SERIAL PRIMARY KEY,
  sale_id INT REFERENCES sales(sale_id) ,
  item_id INT REFERENCES items(item_id) ,
  amount FLOAT ,
  line_net_total FLOAT ,
  line_net FLOAT 
);
'''
insert_into_tmp_table = '''
INSERT INTO branches (branch_nr, branch_name, city, region, latitude, longitude)
SELECT DISTINCT 
    BRANCHNR, 
    BRANCH, 
    CITY, 
    REGION, 
    NULLIF(LATITUDE, 0),
    NULLIF(LONGITUDE, 0)
FROM temp_data
WHERE BRANCHNR IS NOT NULL AND BRANCH IS NOT NULL AND CITY IS NOT NULL AND REGION IS NOT NULL;

INSERT INTO salesmen (salesman_name, branch_id)
SELECT DISTINCT 
    t.SALESMAN, 
    b.branch_id
FROM temp_data t
JOIN branches b ON t.BRANCHNR = b.branch_nr
WHERE t.SALESMAN IS NOT NULL AND b.branch_id IS NOT NULL;

INSERT INTO clients (client_code, client_name, gender)
SELECT DISTINCT 
    CLIENTCODE, 
    CLIENTNAME, 
    NULLIF(GENDER, '')
FROM temp_data
WHERE CLIENTCODE IS NOT NULL AND CLIENTNAME IS NOT NULL
ON CONFLICT (client_code) DO NOTHING;

INSERT INTO brands (brand_code, brand_name)
SELECT DISTINCT 
    NULLIF(BRANDCODE, ''), 
    NULLIF(BRAND, '')
FROM temp_data
WHERE BRANDCODE IS NOT NULL AND BRAND IS NOT NULL
ON CONFLICT (brand_code) DO NOTHING;

INSERT INTO categories (category_name1, category_name2, category_name3)
SELECT DISTINCT 
    NULLIF(CATEGORY_NAME1, ''), 
    NULLIF(NULLIF(CATEGORY_NAME2, ''), 'N/A'),
    NULLIF(NULLIF(CATEGORY_NAME3, ''), 'N/A')
FROM temp_data
WHERE CATEGORY_NAME1 IS NOT NULL;

INSERT INTO items (item_code, item_name, brand_id, category_id, price)
SELECT DISTINCT 
    t.ITEMCODE, 
    t.ITEMNAME, 
    b.brand_id, 
    c.category_id,
    t.PRICE
FROM temp_data t
JOIN brands b ON t.BRANDCODE = b.brand_code
JOIN categories c ON t.CATEGORY_NAME1 = c.category_name1 
    AND (t.CATEGORY_NAME2 = c.category_name2 OR (t.CATEGORY_NAME2 IS NULL AND c.category_name2 IS NULL))
    AND (t.CATEGORY_NAME3 = c.category_name3 OR (t.CATEGORY_NAME3 IS NULL AND c.category_name3 IS NULL))
WHERE t.ITEMCODE IS NOT NULL AND t.ITEMNAME IS NOT NULL AND t.PRICE IS NOT NULL
AND b.brand_id IS NOT NULL AND c.category_id IS NOT NULL
ON CONFLICT (item_code) DO NOTHING;


ALTER TABLE sales ADD CONSTRAINT sales_fisheno_date_unique UNIQUE (fisheno, date);

INSERT INTO sales (fisheno, date, client_id, salesman_id, start_date, end_date)
SELECT DISTINCT 
    t.FICHENO, 
    t.DATE_, 
    cl.client_id, 
    s.salesman_id,
    NULLIF(t.STARTDATE, '1970-01-01'::timestamp),
    NULLIF(t.ENDDATE, '1970-01-01'::timestamp)
FROM temp_data t
JOIN clients cl ON t.CLIENTCODE = cl.client_code
JOIN salesmen s ON t.SALESMAN = s.salesman_name
WHERE t.FICHENO IS NOT NULL AND t.DATE_ IS NOT NULL
AND cl.client_id IS NOT NULL AND s.salesman_id IS NOT NULL
ON CONFLICT (fisheno, date) DO NOTHING;

INSERT INTO sale_items (sale_id, item_id, amount, line_net_total, line_net)
SELECT 
    s.sale_id, 
    i.item_id, 
    NULLIF(t.AMOUNT, 0),
    NULLIF(t.LINENETTOTAL, 0),
    NULLIF(t.LINENET, 0)
FROM temp_data t
JOIN sales s ON t.FICHENO = s.fisheno AND t.DATE_ = s.date
JOIN items i ON t.ITEMCODE = i.item_code
WHERE s.sale_id IS NOT NULL AND i.item_id IS NOT NULL
AND t.AMOUNT IS NOT NULL AND t.LINENETTOTAL IS NOT NULL AND t.LINENET IS NOT NULL;
'''

select_query = """
SELECT AMOUNT, PRICE, GENDER
FROM temp_data
"""