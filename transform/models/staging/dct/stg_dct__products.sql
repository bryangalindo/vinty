{% set products_table = var("products_table") %}

with ranked as (
    select
        *,
        row_number() over (partition by id order by updated_at desc) as row_num
    from {{ source('dct', products_table) }}
), deduped as (
    select *
    from ranked
    where row_num = 1
)
select
    *,
    'dct' as store,
    cast(variants[1].price as decimal(10, 2)) as price,
    concat('https://dct-ep-vintageluxurystore.com/products/', handle) as url,
    date_diff(
        'day', date(from_iso8601_timestamp(published_at)), date(current_date)
    ) as days_on_market
from deduped
