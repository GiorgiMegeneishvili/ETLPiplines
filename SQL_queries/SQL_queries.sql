-- One Big Table data mart: 1 row = 1 order line

CREATE SCHEMA IF NOT EXISTS silver;

DROP TABLE IF EXISTS silver.mart_sales_obt CASCADE;

CREATE TABLE silver.mart_sales_obt (
    -- order
    order_id            INTEGER         NOT NULL,
    order_date          DATE,
    required_date       DATE,
    shipped_date        DATE,
    order_year          INTEGER,
    order_month         INTEGER,
    days_to_ship        INTEGER,
    is_late             BOOLEAN,
    is_shipped          BOOLEAN,

    -- customer
    customer_id         VARCHAR(5),
    customer_name       VARCHAR(40),
    customer_city       VARCHAR(15),
    customer_country    VARCHAR(15),

    -- employee
    employee_id         INTEGER,
    employee_name       VARCHAR(31),
    employee_title      VARCHAR(30),
    manager_name        VARCHAR(31),

    -- product
    product_id          INTEGER         NOT NULL,
    product_name        VARCHAR(40),
    category_name       VARCHAR(15),
    supplier_name       VARCHAR(40),
    supplier_country    VARCHAR(15),

    -- shipping
    shipper_name        VARCHAR(40),
    ship_city           VARCHAR(15),
    ship_country        VARCHAR(15),

    -- measures
    quantity            INTEGER,
    unit_price          NUMERIC(19,4),
    discount            NUMERIC(5,4),
    gross_amount        NUMERIC(19,4),
    discount_amount     NUMERIC(19,4),
    net_amount          NUMERIC(19,4),
    freight_allocated   NUMERIC(19,4),

    load_ts             TIMESTAMP,

    CONSTRAINT pk_mart_sales_obt
        PRIMARY KEY (order_id, product_id)
);


CREATE TABLE IF NOT EXISTS silver.crypto_prices (
    id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    crypto        VARCHAR(50)    NOT NULL,
    price         NUMERIC(38,12),
    market_cap    NUMERIC(38,2),
    volume_24h    NUMERIC(38,2),
    change_24h    NUMERIC(18,8),
    last_updated  TIMESTAMP,                              -- UTC
    load_ts       TIMESTAMPTZ    NOT NULL DEFAULT now()   -- როდის ჩაიტვირთა
);


CREATE TABLE IF NOT EXISTS silver.weather_data1 (
    id          BIGSERIAL PRIMARY KEY,
    city        VARCHAR(100)        NOT NULL,
    temperature NUMERIC(6,2)        NOT NULL,     -- FloatType → 6 ციფრი სულ, 2 ათწილადი
    humidity    INTEGER             NOT NULL,
    weather     VARCHAR(150),                     -- nullable = True
    event_time  TIMESTAMP           NOT NULL,     -- StringType → to_timestamp() გარდაქმნის
    pressure    INTEGER,                          -- nullable = True
    wind_speed  NUMERIC(5,2),                     -- nullable = True
    source      VARCHAR(50),                       -- nullable = True
    
    ingested_at TIMESTAMP           DEFAULT CURRENT_TIMESTAMP
    
);
select * from silver.mart_sales_obt;
select * from silver.crypto_prices;
select * from silver.weather_data1;
