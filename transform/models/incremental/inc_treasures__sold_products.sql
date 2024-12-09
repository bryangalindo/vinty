{% set today = var("TODAY") %}
{% set yesterday = var("YESTERDAY") %}


with
    today as (
        select *
        from {{ ref('stg_treasures__products') }}
        where ingestion_date = '{{ today }}'
    )
    
    , yesterday as (
        select *
        from {{ ref('stg_treasures__products') }}
        where ingestion_date = '{{ yesterday }}'
    )

select
    yday.id
    , yday.vendor
    , yday.title
    , yday.price
    , yday.product_type
    , yday.days_on_market
    , yday.store
    , '{{ today }}' as sold_date
from
    yesterday as yday
left join today as tday
    on yday.id = tday.id
where tday.id is null

    {% if is_incremental() %}
        and '{{ today }}' > (select coalesce(max(sold_date), '1970-01-01') from {{ this }})
    {% endif %}
