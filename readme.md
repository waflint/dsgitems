**supporting:**

* All Sale Totals by Customer over a range of dates (Day, Week, Month, Quarter and Year)
  * ```select customer_id, SUM(discount_total), SUM(subtotal) from dsg.omni_transaction where extract('quarter' from transaction_date) = 2 and extract('year' from transaction_date) = 2022 group by customer_id```

* All Sale Totals by Store and Division over a range of dates (Day, Week, Month, Quarter and Year)
  * ```select store_no, SUM(discount_total), SUM(subtotal) from dsg.omni_transaction where extract('quarter' from transaction_date) = 2 and extract('year' from transaction_date) = 2022 group by store_no```
  * ```select division, SUM(discount_total), SUM(subtotal) from dsg.omni_transaction where extract('quarter' from transaction_date) = 2 and extract('year' from transaction_date) = 2022 group by store_division```

* All Sale Totals by Products Categories over a range of dates (Day, Week, Month, Quarter and Year)
  * ```select product_category, extract('quarter' from transaction_date) quarter, SUM(discount_total), SUM(subtotal) from dsg.omni_transaction where extract('year' from transaction_date) = 2022 group by product_category, extract('quarter' from transaction_date)```

* Customer sales by channel (in-store or online) a range of dates (Day, Week, Month, Quarter and Year)
  * ```select channel, extract('week' from transaction_date) week, count(distinct channel_transaction_id), SUM(subtotal) from dsg.omni_transaction where extract('year' from transaction_date) = 2022 group by channel, extract('week' from transaction_date)```
