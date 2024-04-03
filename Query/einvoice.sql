--1
SELECT DISTINCT invoiceno, AVG(unitprice), SUM(quantity)
FROM einvoice
GROUP BY invoiceno;

--2
SELECT DISTINCT invoiceno, AVG(unitprice), SUM(quantity), COUNT(DISTINCT stockcode), MAX(unitprice), MIN(unitprice)
FROM einvoice
GROUP BY invoiceno;

--3
SELECT stockcode, unitprice,
       DENSE_RANK() OVER (PARTITION BY stockcode ORDER BY unitprice DESC) AS rank
FROM einvoice;

--4
WITH rank_items AS (
    SELECT invoiceno, stockcode, unitprice,
           DENSE_RANK() OVER(PARTITION BY invoiceno ORDER BY unitprice DESC) AS rank
    FROM einvoice
)
SELECT stockcode, rank
FROM rank_items
WHERE rank <= 5;

--5
SELECT customerid, revenue AS current_month,
LAG(revenue, 1) OVER (PARTITION BY customerid ORDER BY invoicedate) AS last_month,
LEAD(revenue, 1) OVER (PARTITION BY customerid ORDER BY invoicedate) AS next_month
FROM einvoice;

--6
SELECT customerid, invoicemonth, revenue
FROM einvoice
GROUP BY customerid, invoicemonth
ORDER BY customerid, invoicemonth;