{% test valid_vendors(model, column_name) %}

WITH invalid_rows AS (
    SELECT DISTINCT {{ column_name }}
    FROM {{ model }}
    WHERE {{ column_name }} NOT IN (SELECT vendor FROM {{ ref('valid_vendors') }})
)

SELECT *
FROM invalid_rows

{% endtest %}