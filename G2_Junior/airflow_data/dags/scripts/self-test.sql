/* checks that all rows from stg would be inserted into oda and then will be deleted */
select 'stg.orders', src_id, max(order_id), min(order_id), count(*) from  stg.orders group by src_id
union all
select 'stg.products', src_id, max(product_id), min(product_id), count(*) from  stg.products group by src_id
union all
select 'stg.customers', src_id, max(customer_id), min(customer_id), count(*) from  stg.customers group by src_id
union all
select 'oda.orders', src_id, max(order_id), min(order_id), count(*) from  oda.orders group by src_id
union all
select 'oda.products', src_id, max(product_id), min(product_id), count(*) from  oda.products group by src_id
union all
select 'oda.customers', src_id, max(customer_id), min(customer_id), count(*) from  oda.customers group by src_id
order by 2,1;