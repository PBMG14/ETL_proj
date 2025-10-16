create_purchases = """
CREATE TABLE purchases (
    id UUID DEFAULT generateUUIDv4(),
    clientcode Int64,
    gender String,
    price Float64,
    amount Float64,
    timestamp DateTime 

) ENGINE = MergeTree()
ORDER BY (id, timestamp);
"""

create_date_purchases = """
CREATE TABLE date_purchases (
    id UUID DEFAULT generateUUIDv4(),
    day DateTime,
    date_amount Float64,
    date_price Float64,
    average_price Float64
) ENGINE = MergeTree()
ORDER BY (day);   
"""

create_date_purchases_by_gender = """
CREATE TABLE date_purchases_by_gender (
    id UUID DEFAULT generateUUIDv4(),
    gender String,
    day DateTime,
    date_amount Float64,
    date_price Float64,
    average_price Float64
) ENGINE = MergeTree()
ORDER BY (day, gender);   
"""

insert_data_to_purchases = """
INSERT INTO purchases (clientcode, gender, price, amount, timestamp)
VALUES (
    %(clientcode)s,
    %(gender)s,
    %(price)s,
    %(amount)s,
    %(timestamp)s
)
"""
insert_data_to_date_purchases = """
INSERT INTO date_purchases (day, date_amount, date_price, average_price)
SELECT toStartOfDay(timestamp) AS day, SUM(amount) as date_amount, SUM(price) AS date_price, date_price / date_amount 
FROM purchases GROUP BY day;
"""

insert_data_to_date_purchases_by_gender = """
INSERT INTO date_purchases_by_gender (day, gender, date_amount, date_price, average_price)
SELECT toStartOfDay(timestamp) AS day, gender, SUM(amount) as date_amount, SUM(price) AS date_price, 
date_price / date_amount 
FROM purchases GROUP BY (day, gender);
"""


get_sum_all_time = """
SELECT SUM(amount * price) FROM purchases
"""

get_sum_all_time_by_gender = """
SELECT SUM(amount * price) FROM purchases WHERE gender = %(gender)s
"""

drop_purchases = """
DROP TABLE IF EXISTS purchases;
"""

drop_date_purchases = """
DROP TABLE IF EXISTS date_purchases;
"""

drop_date_purchases_by_gender = """
DROP TABLE IF EXISTS date_purchases_by_gender;
"""
