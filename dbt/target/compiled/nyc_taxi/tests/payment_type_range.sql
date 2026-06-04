-- Singular test: payment_type must be in 1..6
select payment_type
from "hive"."mart"."stg_trips"
where payment_type is not null
  and (payment_type < 1 or payment_type > 6)