SELECT
    p.company_id,
    p.year,
    p.week,
    p.prediction_date,
    p.prediction as predicted_total_orders,
    a.num_total_orders
    FROM order_forecasting_num_total_orders_predictions p
    LEFT JOIN orders_weekly_aggregated a
        on p.company_id = a.company_id
        and p.year = a.year
        and p.week = a.week
