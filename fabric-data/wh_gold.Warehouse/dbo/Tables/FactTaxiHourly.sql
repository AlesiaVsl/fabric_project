CREATE TABLE [dbo].[FactTaxiHourly] (

	[date_key] int NULL, 
	[pickup_hour] smallint NULL, 
	[day_of_week] smallint NULL, 
	[pickup_location_id] int NULL, 
	[trip_count] bigint NULL, 
	[total_revenue_usd] decimal(18,2) NULL, 
	[total_passengers] bigint NULL, 
	[total_distance] decimal(18,2) NULL, 
	[avg_fare_usd] decimal(10,2) NULL, 
	[avg_duration_min] decimal(10,2) NULL, 
	[avg_distance] decimal(10,2) NULL
);