import streamlit as st
import psycopg2
import pandas as pd

#connect the function to postgresql Database
def database_connection():
    conn = psycopg2.connect(
        host="dbproject1.cvwmae0i6nho.ap-south-1.rds.amazonaws.com",
        port="5432",
        database="dbproject1",
        user="postgres",
        password="pocketpocket"
    )
    return conn
#function execute query
def run_query(query):
    conn = database_connection()
    if conn is None:
        return None 
    try:
        df = pd.read_sql_query(query, conn)
        return df
    except Exception as e:
        st.error(f"error executing query: {e}")
        return None 
    finally:
        conn.close()

queries=[

"""select r2.sub_category,sum(r2.sale_price * r2.quantity) as total_revenue
from retail2 r2 group by r2.sub_category order by total_revenue desc limit 10;
""",
"""select r1.city, avg((r2.profit / r2.sale_price) * 100) as avg_profit_margin
from retail1 r1 join retail2 r2 on r1.order_id = r2.order_id where  r2.sale_price > 0  -- Avoid division by zero
group by r1.city order by avg_profit_margin desc limit 5;""",
"""select r1.category,sum(r2.discount) as total_discount from retail1 r1 join retail2 r2 on r1.order_id=r2.order_id
group by r1.category order by total_discount desc;
""",
"""select r1.category,avg(r2.sale_price) as avg_sale_price from retail1 r1 join retail2 r2 
on r1.order_id=r2.order_id group by r1.category;
""",
"""select r1.region,avg(r2.sale_price) as highest_avg_sale_price from retail1 r1 join retail2 r2 on r1.order_id = r2.order_id
group by r1.region order by highest_avg_sale_price desc limit 1;
""",
"""select r1.category,sum(r2.profit) as total_profit from retail1 r1 join retail2 r2 on r1.order_id=r2.order_id
group by r1.category order by total_profit desc;
""",
"""select r1.segment,max(r2.quantity) as highest_quantity from retail1 r1 join retail2 r2 on r1.order_id=r2.order_id
group by r1.segment order by highest_quantity desc limit 3;
""",
"""select r1.region,avg(r2.discount_percent) as avg_discount_percentage from retail1 r1 join retail2 r2  
on r1.order_id=r2.order_id group by r1.region order by avg_discount_percentage desc;
""",
"""select r1.category,sum(r2.profit) as total_profit from retail1 r1 join retail2 r2 on r1.order_id=r2.order_id
group by r1.category order by total_profit desc limit 1;
""",
"""select extract(year from r1.order_date) as year, sum(r2.sale_price * r2.quantity) as total_revenue
from retail1 r1 join retail2 r2 on r1.order_id = r2.order_id group by year order by year;
""",
"""select r1.region,count(distinct r1.order_id) as total_no_of_orders from retail1 r1
group by r1.region;""",
"""select r1.category,sum(r2.sale_price*r2.quantity) as total_revenue from retail1 r1 join retail2 r2
on r1.order_id=r2.order_id group by r1.category;
""",
"""select r1.order_id,r1.ship_mode,sum(r2.quantity) as total_quantity from retail1 r1
join retail2 r2 on r1.order_id=r2.order_id group by r1.order_id order by total_quantity desc;
""",
"""select r1.segment,sum(r2.sale_price*r2.quantity) as maximum_revenue from retail1 r1 join retail2 r2 
on r1.order_id=r2.order_id group by r1.segment order by maximum_revenue desc limit 1;
""",
"""select r1.ship_mode,sum(r2.discount) as total_discount from retail1 r1 join retail2 r2 
on r1.order_id=r2.order_id group by r1.ship_mode;
""",
"""select r1.city,count(distinct r2.product_id) as nunique_product from retail1 r1 join retail2 r2 
on r1.order_id=r2.order_id group by r1.city order by nunique_product desc;
""",
"""select r1.category,avg((r2.profit/r2.sale_price)*100) as avg_profit_margin from retail1 r1 join retail2 r2
on r1.order_id=r2.order_id where r2.sale_price > 0 group by r1.category;
""",
"""select r1.city,sum(r2.sale_price*r2.quantity) as highest_revenue from retail1 r1 join retail2 r2
on r1.order_id=r2.order_id group by r1.city order by highest_revenue desc limit 1;
""",
"""select r1.country,count(distinct r1.order_id) as total_no_of_orders, 
sum(r2.sale_price*r2.quantity) as total_revenue from retail1 r1 join retail2 r2
on r1.order_id=r2.order_id group by r1.country;
""",
"""select r1.category,sum(r2.profit) as total_profit from retail1 r1 join retail2 r2
on r1.order_id=r2.order_id group by r1.category order by total_profit desc;
"""
]

query_questions=[
"""find the top 10 highest revenue-generating products""",
"""Find the top 5 cities with the highest profit margins""",
"""Calculate the total discount given for each category""",
"""Find the average sale price per product category""",
"""Find the region with the highest average sale price""",
"""Find the total profit per category""",
"""Identify the top 3 segments with the highest quantity of orders""",
"""Determine the average discount percentage given per region""",
"""Find the product category with the highest total profit""",
"""Calculate the total revenue generated per year""",
"""Find the total number of orders placed in each region""",
"""calculate the total revenue generated per product category""",
"""list all orders with their corresponding ship mode and total quantity ordered""",
"""Find the segment with maximum revenue""",
"""Find the total discount given for each ship mode""",
"""Determine the number of unique product sold in each city""",
"""Calculate the average profit margin for each product category""",
"""Find the top 3 cities with the highest revenue""",
"""Find the total number of orders and total revenue for each country""",
"""list the top profitable products in each category"""
]

st.title("PROJECT1:RETAIL ORDER SALES ANALYSIS")
st.header("SQL Queries")
st.subheader("GUVI Provided Queries (1 to 10)")
st.subheader("SELF Provided Queries (11 to 20)")


#Loop to display questions, queries, and results
for i, (question, query) in enumerate(zip(query_questions, queries), start=1):
    st.subheader(f"Query {i}: {question}")
    st.text(query)
    try:
        result_df = run_query(query)
        if result_df is not None:
            st.dataframe(result_df)
        else:
            st.error(f"Query {i} failed to execute.")
    except Exception as e:
        st.error(f"Error running query {i}: {e}")

st.header("Thank you")

    
