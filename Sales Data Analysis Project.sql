select * from Orders

--1- write a sql to get all the orders where customers name has "a" as second character and "d" as fourth character (58 rows)

select * from Orders
where Customer_Name like '_a_d%'

--2- write a sql to get all the orders placed in the month of dec 2020 (352 rows) 

select * from Orders
where MONTH(Order_Date)=12 and YEAR(Order_Date)=2020

--3- write a query to get all the orders where ship_mode is neither in 'Standard Class' nor in 'First Class' 
--and ship_date is after nov 2020 (944 rows)
select* from Orders
where Ship_Mode not in ('standard class','first class') and Ship_Date>'2020-11-30'

--4- write a query to get all the orders where customer name neither start with "A" and nor ends with "n" (9815 rows)

select * from Orders
where Customer_Name not like 'a%n'

--5- write a query to get all the orders where profit is negative (1871 rows)

select * from Orders
where Profit<0

--6- write a query to get all the orders where either quantity is less than 3 or profit is 0 (3348)

select * from Orders where Quantity<3 or Profit=0

--7- your manager handles the sales for South region and he wants you to create a report of all the orders in his region 
--where some discount is provided to the customers (815 rows)

select * from Orders
where region='south' and discount>0

--write a query to find top 5 orders with highest sales in furniture category 

select top 5 * FROM Orders
where Category='furniture'
order by Sales desc

--9- write a query to find all the records in technology and furniture category for the orders placed in the year 2020 only (1021 rows)

select * from Orders
where Category in ('technology','furniture') and YEAR(Order_Date)=2020

--10-write a query to find all the orders where order date is in year 2020 but ship date is in 2021 (33 rows)
select * from Orders
where YEAR(Order_Date)=2020 and YEAR(Ship_Date)=2021


--11- write a update statement to update city as null for order ids :  CA-2020-161389 , US-2021-156909

update orders set city=null where order_id in ('CA-2020-161389','US-2021-156909')

--12- write a query to find orders where city is null (2 rows)
select * from orders where city is null

--13- write a query to get total profit, first order date and latest order date for each category

select Category
	, sum(Profit) as TOTAL_PROFIT
	, MIN(Order_Date) as FIRST_ORDER_DATE
	, MAX(Order_Date) as LATEST_ORDER_DATE
from Orders
group by Category

--14- write a query to find sub-categories where average profit is more than the half of the max profit in that sub-category

select Sub_Category
from Orders
group by Sub_Category
having AVG(Profit)>MAX(Profit)/2

--15- write a query to find total number of products in each category.
select Category, COUNT(distinct Product_ID) as TOTAL_PRODUCT
from Orders
group by Category


--16- write a query to find top 5 sub categories in west region by total quantity sold

select top 5 Sub_Category, SUM(Quantity) as TOTAL_QUANTITY_SOLD
from Orders
where Region='west'
group by Sub_Category
order by TOTAL_QUANTITY_SOLD desc

--17- write a query to find total sales for each region and ship mode combination for orders in year 2020

select Region,Ship_Mode, SUM(Sales) as TOTAL_SALES
from Orders
where YEAR(Order_Date)=2020
group by Region, Ship_Mode

--18- write a query to get region wise count of return orders
select * from Orders
select * from returns

select Region, COUNT(distinct o.Order_ID) as CNT_RETURN_ORDERS 
from Orders o
inner join returns r
	on o.Order_ID=r.Order_ID
	group by Region

--19- write a query to get category wise sales of orders that were not returned
select Category, SUM(Sales) as SALES
from Orders o
left join returns r
	on o.Order_ID=r.Order_ID
	where r.Order_ID is null
group by Category

--20- write a query to print sub categories where we have all 3 kinds of returns (others,bad quality,wrong items) (12 rows)
select Sub_Category
from Orders o
inner join returns r
	on o.Order_ID=r.Order_ID
	group by Sub_Category
	having COUNT(distinct Return_reason)=3

--21- write a query to find cities where not even a single order was returned.(413 rows)

select City
from Orders o
left join returns r
	on o.Order_ID=r.Order_ID
	group by City
	having COUNT(r.Return_reason)=0

--22- write a query to find top 3 subcategories by sales of returned orders in east region

select top 3 Sub_Category, SUM(Sales) as EAST_SALES
from Orders o
inner join returns r
	on o.Order_ID=r.Order_ID
	where Region='east'
	group by Sub_Category
	order by EAST_SALES desc

--23- write a query to find subcategories who never had any return orders in the month of november (irrespective of years)

select sub_category
from orders o
left join returns r on o.order_id=r.order_id
	where DATEPART(month,order_date)=11
	group by sub_category
	having count(r.order_id)=0;

--24- orders table can have multiple rows for a particular order_id when customers buys more than 1 product in an order.
--write a query to find order ids where there is only 1 product bought by the customer.(2538 rows)

select order_id
from orders 
group by order_id
having count(1)=1

--25- write a query to get number of business days between order_date and ship_date (exclude weekends). 
--Assume that all order date and ship date are on weekdays only

select order_id,order_date,ship_date 
,datediff(day,order_date,ship_date)-2*datediff(week,order_date,ship_date) as no_of_business_days
from 
orders

--26- write a query to print 3 columns : category, total_sales and (total sales of returned orders)
select o.category,sum(o.sales) as total_sales
,sum(case when r.order_id is not null then sales end) as return_orders_sales
from orders o
left join returns r on o.order_id=r.order_id
	group by category


--27- write a query to print below 3 columns
--category, total_sales_2019(sales in year 2019), total_sales_2020(sales in year 2020)

select category
,sum(case when datepart(year,order_date)=2019 then sales end) as total_sales_2019
,sum(case when datepart(year,order_date)=2020 then sales end) as total_sales_2020
from orders 
group by category

--28- write a query print top 5 cities in west region by average no of days between order date and ship date.

select top 5 City, AVG(DATEDIFF(DAY,Order_Date,Ship_Date)) as AVG_NO_DAYS
from Orders
where Region='west'
group by City
order by AVG_NO_DAYS desc



--29- write a query to print first name and last name of a customer using orders table
--(everything after first space can be considered as last name)
--customer_name, first_name,last_name

select * from Orders

select Customer_Name
	,LEFT(Customer_Name, CHARINDEX(' ',Customer_Name ))
	,RIGHT(customer_name, LEN(customer_name)-CHARINDEX(' ',Customer_Name ))
from Orders

--30-write a query to print below output from orders data. example output
--hierarchy type,hierarchy name ,total_sales_in_west_region,total_sales_in_east_region
--category , Technology, ,
--category, Furniture, ,
--category, Office Supplies, ,
--sub_category, Art , ,
--sub_category, Furnishings, ,
--and so on all the category ,subcategory and ship_mode hierarchies 

select 'Category' as 'Hierarchy type'
,Category,sum(case when Region='west' then sales end) as total_sales_in_west_region
,sum(case when Region='east' then sales end) as total_sales_in_east_region
from Orders
group by Category
union 
select 'Sub_Category' as 'Hierarchy type', Sub_Category,sum(case when Region='west' then sales end) as total_sales_in_west_region
,sum(case when Region='east' then sales end) as total_sales_in_east_region
from Orders
group by Sub_Category
union
select 'Ship_Mode' as 'Hierarchy type', Ship_Mode,
SUM(case when Region='west' then Sales end) as total_sales_in_west_region,
SUM(case when Region='west' then Sales end) as total_sales_in_west_region
from Orders
group by Ship_Mode

--31- the first 2 characters of order_id represents the country of order placed.
--write a query to print total no of orders placed in each country
--(an order can have 2 rows in the data when more than 1 item was purchased in the order but it should be considered as 1 order)

select COUNTRY_CODE, count(*) as TOTAL_NO_OF_ORDER_PLACED from(
select Order_ID, LEFT(Order_ID,2) as COUNTRY_CODE
from Orders
group by Order_ID) A
group by COUNTRY_CODE

select left(order_id,2) as country, count(distinct order_id) as total_orders
from orders 
group by left(order_id,2)

--32- Find average order value. One order can have multiple orders.

select AVG(TOTAL_SALES) as AVERAGE_SALE
from(
	select Order_ID, SUM(Sales) as TOTAL_SALES
	from Orders
	group by Order_ID) A

--33- Find order with sales more than average order value. (1379 rows)

select Order_ID
from Orders
group by Order_ID
having sum(Sales)>(select AVG(TOTAL_SALES) 
					from(
						select Order_ID, SUM(Sales) as TOTAL_SALES
						from Orders
						group by Order_ID) A)


--34- write a query to find premium customers from orders data. 
--Premium customers are those who have done more orders than average no of orders per customer.

select * from(
	select Customer_ID, COUNT(distinct Order_ID) as NO_OF_ORDERS
	from Orders
	group by Customer_ID)B
	where NO_OF_ORDERS>(
		select AVG(COUNT_ORDERS) as AVERAGE_NO_OF_ORDERS
		from(
			select Customer_ID, COUNT(distinct Order_ID) as COUNT_ORDERS
			from Orders
			group by Customer_ID) A)


--35- write a query to print product id and total sales of highest selling products (by no of units sold) in each category

select * from(
	select Category,Product_ID,SUM(Quantity) as TOTAL_QTY_SOLD
	from Orders
	group by Category,Product_ID) ABC
	inner join(
		select category, MAX(total_quantity) AS max_quantity 
		FROM (
			SELECT category, product_id, SUM(quantity) AS total_quantity
			FROM orders 
			GROUP BY category, product_id
			) XYZ 
			GROUP BY category) DEF
	on ABC.Category=DEF.Category
	where abc.TOTAL_QTY_SOLD=DEF.max_quantity


--36- Print top 5 selling products from each category by sales

select * from(
		select *, ROW_NUMBER() over(partition by category order by total_sales desc) as rn
		from (
			select Category,Product_ID, SUM(Sales) as total_sales 
			from Orders
			group by Category,Product_ID) A) B
			where rn<=5



--37- write a query to find top 3 and bottom 3 products by sales in each region. (24 rows)

select *,CASE 
           WHEN top3 <= 3 THEN 'Top 3' 
           ELSE 'Bottom 3' 
       	END AS top_bottom from(
select *, ROW_NUMBER() over(partition by region order by total_sales desc) as top3
, ROW_NUMBER() over(partition by region order by total_sales ) as bottom3
			from (
				select region,Product_ID, SUM(Sales) as total_sales
				from Orders
				group by Product_ID,Region) A)B
where top3<4 or bottom3<4

--38- Among all the sub categories. Which sub category had highest month over month growth by sales in Jan 2020.

SELECT TOP 1 *,
       100 * (TOTAL_SALES - PREV_MONTH_SALE) / PREV_MONTH_SALE AS MONTH_BYMONTH_GROWTH
FROM (
    SELECT *,
           LAG(TOTAL_SALES) OVER (PARTITION BY Sub_Category ORDER BY year_month) AS PREV_MONTH_SALE
    FROM (
        SELECT Sub_Category,
               SUM(Sales) AS TOTAL_SALES,
               FORMAT(order_date, 'yyyyMM') AS year_month
        FROM Orders
        GROUP BY Sub_Category, FORMAT(order_date, 'yyyyMM')
    ) AS A
) AS B
WHERE year_month = '202001'
ORDER BY MONTH_BYMONTH_GROWTH;

--39- write a query to print top 3 products in each category by year over year sales growth in year 2020.

SELECT * FROM (
    SELECT *,RANK() OVER (PARTITION BY category ORDER BY (sales - prev_year_sales) / prev_year_sales DESC) AS rn
    FROM (
        SELECT *, LAG(sales) OVER (PARTITION BY category, product_id ORDER BY order_year) AS prev_year_sales
        FROM (
            SELECT category, product_id, DATEPART(YEAR, order_date) AS order_year, SUM(sales) AS sales
            FROM orders
            GROUP BY category, product_id, DATEPART(YEAR, order_date)
        ) AS cat_sales
    ) AS prev_year_data
    WHERE order_year = 2020
) AS rnk
WHERE rn <= 3;

--40- write a sql to find top 3 products in each category by highest rolling 3 months total sales for Jan 2020.

SELECT *
FROM (
    SELECT *, RANK() OVER(PARTITION BY category ORDER BY roll3_sales DESC) AS rn
    FROM (
        SELECT *,SUM(sales) OVER (PARTITION BY category, product_id  ORDER BY yo, mo ROWS BETWEEN 2 PRECEDING AND CURRENT ROW ) AS roll3_sales
        FROM (
            SELECT  category, product_id, DATEPART(YEAR, order_date) AS yo, DATEPART(MONTH, order_date) AS mo,SUM(sales) AS sales
            FROM orders
            GROUP BY category, product_id, DATEPART(YEAR, order_date), DATEPART(MONTH, order_date)
			) AS xxx
			) AS yyyy
    WHERE yo = 2020 AND mo = 1
) AS A
WHERE rn <= 3;

--41- write a query to find products for which month over month sales has never declined.(272 record)
select distinct Product_ID from(
	select *,LAG(sales, 1, 0) over(partition by product_id order by yo,mo) as LAG_SALES
	from(
		select Product_ID, YEAR(Order_Date) as YO, MONTH(Order_Date) as MO, sum(sales) as sales
		from Orders
		group by product_id ,YEAR(Order_Date),MONTH(Order_Date))A)B
		where Product_ID not in (select Product_ID from(
									select *,LAG(sales, 1, 0) 
											over(partition by product_id order by yo,mo) as LAG_SALES
									from(
										select Product_ID,YEAR(Order_Date) as YO
														 ,MONTH(Order_Date) as MO
														 ,sum(sales) as sales
										from Orders
										group by product_id ,YEAR(Order_Date),MONTH(Order_Date))A)B
										where sales<LAG_SALES
										group by Product_ID )

--42- write a query to find month wise sales for each category for months where sales is more than the combined sales of previous 2 months for that category. (21 rows)

select * from (
	select *,SUM(TOTAL_SALES) over(partition by category order by yo,mo
		rows between 2 preceding and 1 preceding) as MONTH2_SALES
	from(
		select Category, YEAR(Order_Date) as YO, MONTH(Order_Date) as MO
				   , SUM(Sales) as TOTAL_SALES
		from Orders
		group by Category,MONTH(Order_Date),YEAR(Order_Date)) A) B
		where TOTAL_SALES>MONTH2_SALES

