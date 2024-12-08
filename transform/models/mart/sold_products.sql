{% set tables = [
        ref('inc_vsp__products'),
        ref('inc_rebag__products'),
        ref('inc_dct__products'),
        ref('inc_treasures__products')] %}

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
