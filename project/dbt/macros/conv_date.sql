{#
    This macro converts integer date 20220101 into BQ data type DATE('2022-01-01')
#}

{% macro conv_date(date_int) -%}

    DATE(
        CAST(SUBSTR(CAST({{ date_int }} AS STRING), 1, 4) AS INT64),
        CAST(SUBSTR(CAST({{ date_int }} AS STRING), 5, 2) AS INT64),
        CAST(SUBSTR(CAST({{ date_int }} AS STRING), 7, 2) AS INT64)
    )

{%- endmacro %}
