{{

    config(

        materialized= 'view'

 

    )

}}


WITH CTE AS
(
SELECT
    EMPLOYEE_ID ,
	EMPLOYEE_NAME ,
	DEPARTMENT_ID ,
	EMAIL ,
    PHONE ,
    ADDRESS ,
	HIRE_DATE ,
    EMPLOYMENT_STATUS 
FROM {{source('src','employee')}}
)

-- Main SELECT
SELECT 
    CAST(EMPLOYEE_ID as INTEGER) as EMPLOYEE_ID ,
	EMPLOYEE_NAME ,
	CAST(DEPARTMENT_ID as INTEGER) as  DEPARTMENT_ID ,
	EMAIL ,
    CAST( PHONE as INTEGER) as PHONE ,
    ADDRESS ,
	HIRE_DATE ,
    EMPLOYMENT_STATUS,
    md5(nvl(cast(EMPLOYEE_NAME as varchar()),'')||nvl(cast(DEPARTMENT_ID as varchar()),'')||nvl(cast(EMAIL as varchar()),'')||nvl(cast(PHONE as varchar()),'')||nvl(cast(ADDRESS as varchar()),'')||nvl(cast(HIRE_DATE as varchar()),'')||nvl(cast(EMPLOYMENT_STATUS as varchar()),'')  ) as md5_column,
    current_timestamp as SNOW_INSERT_TIME,
    current_timestamp as SNOW_UPDATE_TIME
from CTE
