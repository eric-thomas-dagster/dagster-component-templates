-- A singular test passes when it returns ZERO rows. No currency amount
-- on an order should ever be negative, and every order should have at
-- least one item -- these aren't expressible as a generic column test
-- (not_null/unique/accepted_values/relationships), so this is plain SQL
-- instead of pulling in dbt_utils just for an expression test on what's
-- otherwise a credential-free, zero-dependency quickstart.

select *
from {{ ref('stg_orders') }}
where subtotal < 0
   or shipping < 0
   or tax < 0
   or total < 0
   or num_items <= 0
