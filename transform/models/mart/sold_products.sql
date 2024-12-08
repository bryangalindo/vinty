{% set tables = [
        ref('inc_vsp__sold_products'),
        ref('inc_rebag__sold_products'),
        ref('inc_dct__sold_products'),
        ref('inc_treasures__sold_products')] %}

{% for table in tables %}
    select
        vendor,
        title,
        price,
        days_on_market,
        store,
        sold_date
    from
        {{ table }}
    {% if not loop.last %}
        UNION ALL
    {% endif %}
{% endfor %}
