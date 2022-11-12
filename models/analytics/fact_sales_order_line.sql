SELECT
  order_line_id AS sales_order_line_key,
  quantity      AS quantity,
  unit_price    AS unit_price
FROM `vit-lam-data.wide_world_importers.sales__order_lines`