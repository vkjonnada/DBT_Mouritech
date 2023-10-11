{{
    config(
        materialized= 'incremental',
        unique_key='EMPLOYEE_ID',
        incremental_strategy='merge',
        post_hook = "delete from {{ ref('stg_dim_user')}}"
    )
}}

{% if is_incremental() %}
    with max_user_key AS(
        select max(user_key) as max_user_key from {{ this }}
    )
    select
        --CAST(ROW_NUMBER() OVER (ORDER BY EMPLOYEE_ID) as NUMBER(38,0)) as USER_KEY ,
    nvl(dim.USER_KEY,mx.max_user_key + ROW_NUMBER() OVER (order by stg.EMPLOYEE_ID)) as USER_KEY,
    stg.EMPLOYEE_ID ,
	stg.EMPLOYEE_NAME ,
	stg.DEPARTMENT_ID ,
	stg.EMAIL ,
    stg.PHONE ,
    stg.ADDRESS ,
	stg.HIRE_DATE ,
    stg.EMPLOYMENT_STATUS,
    stg.MD5_COLUMN,
    nvl(dim.SNOW_INSERT_TIME,current_timestamp) as SNOW_INSERT_TIME,
    current_timestamp as SNOW_UPDATE_TIME
    FROM {{ ref('stg_dim_user')}} stg
    LEFT JOIN {{ this }} dim
    ON nvl(stg.EMPLOYEE_ID,0) = nvl(dim.EMPLOYEE_ID,0)
    CROSS JOIN max_user_key mx
    where stg.MD5_COLUMN <> dim.MD5_COLUMN

{% else %}
    select
    CAST(ROW_NUMBER() OVER (ORDER BY EMPLOYEE_ID) as NUMBER(38,0)) as USER_KEY ,
    EMPLOYEE_ID ,
	EMPLOYEE_NAME ,
	DEPARTMENT_ID ,
	EMAIL ,
    PHONE ,
    ADDRESS ,
	HIRE_DATE ,
    EMPLOYMENT_STATUS,
    MD5_COLUMN,
    current_timestamp as SNOW_INSERT_TIME,
    current_timestamp as SNOW_UPDATE_TIME
    FROM {{ ref('stg_dim_user')}} 

{% endif %}