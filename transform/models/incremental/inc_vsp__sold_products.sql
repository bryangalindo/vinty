{% set today = var("TODAY") %}
{% set yesterday = var("YESTERDAY") %}


WITH today AS (
    SELECT *
    FROM {{ ref('stg_vsp__products') }}
    WHERE ingestion_date = '{{ today }}'
), yesterday AS (
    SELECT *
    FROM {{ ref('stg_vsp__products') }}
    WHERE ingestion_date = '{{ yesterday }}'
)

select
    yday.id as id,
    yday.vendor as vendor,
    yday.title AS title,
    yday.price AS price,
    yday.product_type AS product_type,
    yday.days_on_market AS days_on_market,
    yday.store AS store,
    '{{ today }}' AS sold_date
from
    yesterday yday
left join today tday
on yday.id = tday.id
where tday.id is null

{% if is_incremental() %}
    and '{{ today }}' > (select COALESCE(max(sold_date), '1970-01-01') from {{ this }})
{% endif %}