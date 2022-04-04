{#
    This macro sums the input column over the last 52 weeks
#}

{% macro sum_52(col) -%}

    SUM(CASE WHEN t2.tourney_date + INTERVAL 5 DAY BETWEEN t1.ranking_date - INTERVAL 365 DAY AND t1.ranking_date THEN t2.{{ col }} ELSE 0 END)

{%- endmacro %}
