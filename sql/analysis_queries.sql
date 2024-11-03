-- Average revenue calculation between 01.10.2020 and 01.01.2021
WITH price_data AS (
    SELECT 
        p.productid,
        p.productcomponent,
        p.price,
        p.valid_from,
        p.valid_until
    FROM 
        Prices p
    WHERE 
        p.productcomponent IN ('baseprice', 'energyprice', 'workingprice')
),
revenue_data AS (
    SELECT
        c.id AS contract_id,
        c.productid,
        c.usage,
        COALESCE(bp.price, 0) AS base_price,
        COALESCE(ep.price, 0) AS energy_price,
        COALESCE(wp.price, 0) AS working_price,
        c.createdat
    FROM 
        Contracts c
    LEFT JOIN price_data bp ON c.productid = bp.productid 
                            AND bp.productcomponent = 'baseprice'
                            AND c.startdate BETWEEN bp.valid_from AND bp.valid_until
    LEFT JOIN price_data ep ON c.productid = ep.productid 
                            AND ep.productcomponent = 'energyprice'
                            AND c.startdate BETWEEN ep.valid_from AND ep.valid_until
    LEFT JOIN price_data wp ON c.productid = wp.productid 
                            AND wp.productcomponent = 'workingprice'
                            AND c.startdate BETWEEN wp.valid_from AND wp.valid_until
    WHERE 
        c.createdat BETWEEN '2020-10-01' AND '2021-01-01'
)
SELECT
    AVG(base_price + (usage * energy_price)) AS avg_revenue_formula1,
    AVG(base_price + (usage * working_price)) AS avg_revenue_formula2
FROM
    revenue_data;

-- Count contracts on delivery as of 01.01.2021
SELECT COUNT(*) AS contracts_on_delivery
FROM contracts
WHERE startdate <= '2021-01-01'
  AND (enddate IS NULL OR enddate >= '2021-01-01');

-- Count new contracts loaded on 01.12.2020
SELECT COUNT(*) AS new_contracts_loaded
FROM contracts
WHERE createdat = '2020-12-01';
