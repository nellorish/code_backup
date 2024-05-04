
{{ config(
    materialized='incremental',
    table_type='iceberg',
    incremental_strategy='merge',
    unique_key='sourceidentifier',
    on_schema_change = 'sync_all_columns',
    format='parquet',
    table_properties={
    	'optimize_rewrite_delete_file_threshold': '2'
    	},
    post_hook=['{{ audit_changes() }}']
) }}


SELECT *
FROM {{ ref('int_fhir4_gaige_payment_reconcilation')}}