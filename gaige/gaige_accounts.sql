--int_fhir4_gaige_account

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
FROM {{ ref('int_fhir4_gaige_account')}}