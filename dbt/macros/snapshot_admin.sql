{#- Drops the loan status snapshot (if it exists) so a replay can start from an empty history.
    Usage: dbt run-operation drop_loan_status_snapshot
    run-operation does not commit on its own, so the commit is explicit. -#}
{% macro drop_loan_status_snapshot() %}
    {% set relation = adapter.get_relation(database=target.database, schema='marts', identifier='snp_loan_status') %}
    {% if relation is not none %}
        {% do run_query('drop table if exists ' ~ relation) %}
        {% do adapter.commit() %}
        {{ log('Dropped ' ~ relation, info=true) }}
    {% else %}
        {{ log('No snapshot table to drop', info=true) }}
    {% endif %}
{% endmacro %}
