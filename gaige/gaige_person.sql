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


SELECT sourceidentifier,
       birthdate,
       gender,
       CAST(json_extract(activename,'$') as MAP<VARCHAR,VARCHAR>) as activename,
       CAST(json_extract(primarycontactpoint,'$') as MAP<VARCHAR,VARCHAR>) as primarycontactpoint,
       CAST(json_extract(activeaddress,'$') as MAP<VARCHAR,VARCHAR>) as activeaddress
FROM {{ ref('int_fhir4_gaige_person')}}
where sourceidentifier not in ('2ff937d5-49c0-4d88-b923-0f5bdcf3b04f-7192607'
    , '2ff937d5-49c0-4d88-b923-0f5bdcf3b04f-8238220'
    , '2ff937d5-49c0-4d88-b923-0f5bdcf3b04f-8399880'
    , '2ff937d5-49c0-4d88-b923-0f5bdcf3b04f-8595909')
