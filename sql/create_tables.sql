-- Products table
CREATE TABLE IF NOT EXISTS products (
    id BIGINT PRIMARY KEY,
    productcode VARCHAR(255),
    productname VARCHAR(255),
    energy VARCHAR(255),
    consumptiontype VARCHAR(255),
    deleted SMALLINT,
    modificationdate TIMESTAMP
);

-- Contracts table
CREATE TABLE IF NOT EXISTS contracts (
    id BIGINT PRIMARY KEY,
    type VARCHAR(255),
    energy VARCHAR(255),
    usage INT,
    usagenet INT,
    createdat TIMESTAMP,
    startdate TIMESTAMP,
    enddate TIMESTAMP,
    fillingdatecancellation TIMESTAMP,
    cancellationreason VARCHAR(255),
    city VARCHAR(255),
    status VARCHAR(255),
    productid INT,
    modificationdate TIMESTAMP
);

-- Contracts table temp
CREATE TABLE IF NOT EXISTS contracts_temp (
    id BIGINT PRIMARY KEY,
    type VARCHAR(255),
    energy VARCHAR(255),
    usage INT,
    usagenet INT,
    createdat TIMESTAMP,
    startdate TIMESTAMP,
    enddate TIMESTAMP,
    fillingdatecancellation TIMESTAMP,
    cancellationreason VARCHAR(255),
    city VARCHAR(255),
    status VARCHAR(255),
    productid INT,
    modificationdate TIMESTAMP
);

-- Prices table
CREATE TABLE IF NOT EXISTS prices (
    id BIGINT PRIMARY KEY,
    productid INT,
    pricecomponentid INT,
    productcomponent VARCHAR(255),
    price DECIMAL(38,10),
    unit VARCHAR(255),
    valid_from TIMESTAMP,
    valid_until TIMESTAMP,
    modificationdate TIMESTAMP
);

-- Prices table temp
CREATE TABLE IF NOT EXISTS prices_temp (
    id BIGINT PRIMARY KEY,
    productid INT,
    pricecomponentid INT,
    productcomponent VARCHAR(255),
    price DECIMAL(38,10),
    unit VARCHAR(255),
    valid_from TIMESTAMP,
    valid_until TIMESTAMP,
    modificationdate TIMESTAMP
);
