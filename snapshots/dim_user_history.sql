{% snapshot dim_user_history %}
 

{{

    config(

        unique_key= 'EMPLOYEE_ID',

        strategy='check',

        check_cols=['md5_column']

    )

 

}}

 

select * from {{ ref('dim_user')}}

 

{% endsnapshot %}