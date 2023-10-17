--select '1', d.* from {{ ref('dim_user')}} as d where 1=1
--union all
select '2', d.* from {{ ref('dim_user')}} as d where 1=2