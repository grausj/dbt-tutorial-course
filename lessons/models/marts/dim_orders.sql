WITH

order_item_measures AS (
select
order_id,
sum(item_sale_price) as total_sale_price,
sum(product_cost) as total_product_cost,
sum(item_profit) as total_profit,
sum(item_discount) as total_discount

	from {{ ref("int_ecommerce__order_items_products") }}
	group by 1
)

select
	-- Diemnsions from our staging orders table
	od.order_id,
	od.created_at,
	od.shipped_at,
	od.delivered_at,
	od.returned_at,
	od.status,
	od.num_items_ordered,

	-- Metrics on an order level
	om.total_sale_price,
	om.total_product_cost,
	om.total_profit,
	om.total_discount

from {{ ref("stg_ecommerce__orders") }} as od
left join order_item_measures as om
on od.order_id = om.order_id