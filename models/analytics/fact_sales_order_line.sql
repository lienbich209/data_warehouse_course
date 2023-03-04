SELECT 
  order_line_id as order_line_key, quantity, unit_price, quantity*unit_price as gross_amount,
  stock_item_id
FROM `vit-lam-data.wide_world_importers.sales__order_lines`