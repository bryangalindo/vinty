{% set products_table = var("products_table") %}

with ranked as (
    select
      *,
      row_number() over (partition by id order by updated_at desc) as row_num
    from {{ source('rebag', products_table) }}
), deduped AS (
    select *
    from ranked
    where row_num = 1
)
select
    *,
    'rebag' as store,
    cast(variants[1].price AS DECIMAL(10, 2)) AS price,
    concat('https://shop.rebag.com/products/', handle) AS url,
    date_diff('day', date(from_iso8601_timestamp(published_at)), date(current_date)) AS days_on_market,
    regexp_extract(body_html, '<b>Condition:</b>\s*([A-Za-z ]+).', 1) AS condition,
    regexp_extract(body_html, '<b>Condition:</b> [A-Za-z ]+\. ([^<]+).<br>', 1) AS condition_details,
    regexp_extract(body_html, '<b>Accessories:</b> (.+?) <br>', 1) AS accessories,
    regexp_extract(body_html, 'Height (\d+)', 1) AS height_inches,
    regexp_extract(body_html, 'Width (\d+)', 1) AS width_inches,
    regexp_extract(body_html, 'Depth (\d+)', 1) AS depth_inches,
    regexp_extract(body_html, '<b>Exterior Material:</b> (.+?) <br>', 1) AS exterior_material,
    regexp_extract(body_html, '<b>Exterior Color:</b> (.+?) <br>', 1) AS exterior_color,
    regexp_extract(body_html, '<b>Interior Material:</b> (.+?) <br>', 1) AS interior_material,
    regexp_extract(body_html, '<b>Interior Color:</b> (.+?) <br>', 1) AS interior_color,
    regexp_extract(body_html, '<b>Hardware Color:</b> (.+?) <br>', 1) AS hardware_color
from deduped