with payment_type_lookup as (
    select *
    from {{ ref('payment_type_lookup') }}
)

, payment_types as (
    select payment_type,
           description as payment_type_detail
    from payment_type_lookup
)

select * from payment_types