{% set products_table = var("products_table") %}

with ranked as (
    select
        *,
        row_number() over (partition by id order by updated_at desc) as row_num
    from {{ source('rebag', products_table) }}
), deduped as (
    select *
    from ranked
    where row_num = 1
)
select
    *,
    'rebag' as store,
    cast(variants[1].price AS DECIMAL(10, 2)) as price,
    concat('https://shop.rebag.com/products/', handle) as url,
    date_diff(
        'day', date(from_iso8601_timestamp(published_at)), date(current_date)
    ) as days_on_market,
    regexp_extract(body_html, '<b>Condition:</b>\s*([A-Za-z ]+).', 1)
        as condition,
    regexp_extract(
        body_html, '<b>Condition:</b> [A-Za-z ]+\. ([^<]+).<br>', 1
    ) as condition_details,
    regexp_extract(body_html, '<b>Accessories:</b> (.+?) <br>', 1)
        as accessories,
    regexp_extract(body_html, 'Height (\d+)', 1) as height_inches,
    regexp_extract(body_html, 'Width (\d+)', 1) as width_inches,
    regexp_extract(body_html, 'Depth (\d+)', 1) as depth_inches,
    regexp_extract(body_html, '<b>Exterior Material:</b> (.+?) <br>', 1)
        as exterior_material,
    regexp_extract(body_html, '<b>Exterior Color:</b> (.+?) <br>', 1)
        as exterior_color,
    regexp_extract(body_html, '<b>Interior Material:</b> (.+?) <br>', 1)
        as interior_material,
    regexp_extract(body_html, '<b>Interior Color:</b> (.+?) <br>', 1)
        as interior_color,
    regexp_extract(body_html, '<b>Hardware Color:</b> (.+?) <br>', 1)
        as hardware_color
from deduped
