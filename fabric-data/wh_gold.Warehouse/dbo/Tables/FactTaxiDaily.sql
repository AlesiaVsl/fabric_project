CREATE TABLE [dbo].[FactTaxiDaily] (

	[date_key] int NULL, 
	[location_id] int NULL, 
	[trip_count] int NULL, 
	[total_passengers] bigint NULL, 
	[total_distance_miles] float NULL, 
	[avg_distance_miles] float NULL, 
	[total_revenue_usd] float NULL, 
	[avg_fare_usd] float NULL, 
	[total_tips_usd] float NULL, 
	[avg_duration_min] float NULL
);


GO
ALTER TABLE [dbo].[FactTaxiDaily] ADD CONSTRAINT FK_Taxi_Date FOREIGN KEY ([date_key]) REFERENCES [dbo].[DimDate]([date_key]);
GO
ALTER TABLE [dbo].[FactTaxiDaily] ADD CONSTRAINT FK_Taxi_Zone FOREIGN KEY ([location_id]) REFERENCES [dbo].[DimZone]([location_id]);