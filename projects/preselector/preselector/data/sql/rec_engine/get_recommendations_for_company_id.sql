declare @company uniqueidentifier = '{company_id}', @week int = '{week}', @year int = '{year}';

SELECT [agreement_id]
      ,[run_timestamp]
      ,[product_id]
      ,[order_of_relevance]
      ,[order_of_relevance_cluster]
      ,[cluster]
  FROM [ml_output].[latest_recommendations]
  WHERE company_id = @company
  AND week = @week
  AND year = @year
