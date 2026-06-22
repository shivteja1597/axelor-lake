{% materialization table, adapter='duckdb' %}

  {%- set identifier = model['alias'] -%}
  {%- set target_relation = api.Relation.create(identifier=identifier,
                                               schema=schema,
                                               database=database,
                                               type='table') -%}

  -- run pre-hooks
  {{ run_hooks(pre_hooks, inside_transaction=False) }}
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  -- drop if exists
  {% call statement('drop_relation') -%}
    COMMIT;
    drop table if exists {{ target_relation.include(database=true) }};
  {%- endcall %}

  -- build model
  {% call statement('main') -%}
    create table {{ target_relation.include(database=true) }} as (
      {{ sql }}
    );
    BEGIN TRANSACTION;
  {%- endcall %}

  -- run post-hooks
  {{ run_hooks(post_hooks, inside_transaction=True) }}
  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
