{{
  config(
	materialized = 'incremental',
	unique_key='event_id',
	on_schema_change='sync_all_columns'
	)
}}

with source as (
select *
from {{ source('thelook_ecommerce', 'events') }}
--where created_at <= '2023-01-01'
)

select
	id AS event_id,
	user_id,
	sequence_number,
	session_id,
	created_at,
	ip_address,
	city,
	state,
	postal_code,
	browser,
	traffic_source,
	uri AS web_link,
	event_type,
	{# Look in macros/macro_get_brand_name.sql to see how this function is defined #}
	--{{ target.schema }}.get_brand_name(uri) AS brand_name


FROM source

{% if is_incremental() %}

where created_at > (select max(created_at) from {{ this}})
  --and prefix.date_col >= coalesce((select max(date_col) from {{ this }}), '1900-01-01')
{% endif %}