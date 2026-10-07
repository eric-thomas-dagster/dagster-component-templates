-- A singular test passes when it returns ZERO rows. The customers mart
-- coalesces number_of_orders/lifetime_order_value to 0 for a customer
-- with no orders, but never guarantees non-negativity on its own --
-- worth its own check since it's a derived aggregate, not a straight
-- passthrough from a source.

select *
from {{ ref('customers') }}
where number_of_orders < 0
   or lifetime_order_value < 0
