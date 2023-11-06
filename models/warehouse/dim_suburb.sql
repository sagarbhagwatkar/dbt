{{
    config(
        unique_key='host_id'
    )
}}


select t1.lga_code,t2.Suburb_name,t1.lga_name from  {{ ref('lga_stg') }} t1 
full join {{ ref('suburb_stg') }} t2
on upper(t1.lga_name)=upper(t2.lga_name)
    