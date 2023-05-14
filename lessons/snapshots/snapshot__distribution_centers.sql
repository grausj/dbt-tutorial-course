{% {% snapshot snapshot__distribution_centers %}

{{
   config(
	   target_schema='dbt_snapshot',
	   unique_key='id',

	   strategy='check',
	   check_cols=['name','latitude','longitude']

   )
}}

select *

from {{ source('thelook_ecommerce', 'distribution_centers') }}

{% endsnapshot %}%}