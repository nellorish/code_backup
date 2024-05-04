{{ config(
    materialized='incremental',
    table_type='iceberg',
    incremental_strategy='merge',
    unique_key='sourceidentifier',
    on_schema_change = 'sync_all_columns',
    format='parquet',
    partitioned_by=['bucket(sourceidentifier, 36)'],
    table_properties={
    	'optimize_rewrite_delete_file_threshold': '2'
    	},
    post_hook=['{{ audit_changes() }}']
) }}


SELECT *
FROM {{ ref('int_fhir4_gaige_coverage')}}
where sourceidentifier  not in (
    '2ff937d5-49c0-4d88-b923-0f5bdcf3b04f-34460195-1526986421',
    '2ff937d5-49c0-4d88-b923-0f5bdcf3b04f-34474552-1529137355',
    '2ff937d5-49c0-4d88-b923-0f5bdcf3b04f-33754515-1455716168'
    )
