CREATE TABLE [dbo].[FactAirQualityDaily] (

	[date_key] int NULL, 
	[measurement_date] date NULL, 
	[sensor_id] int NULL, 
	[parameter_name] varchar(8000) NULL, 
	[avg_value] float NULL, 
	[max_value] float NULL, 
	[min_value] float NULL, 
	[units] varchar(8000) NULL
);


GO
ALTER TABLE [dbo].[FactAirQualityDaily] ADD CONSTRAINT FK_AQ_Date FOREIGN KEY ([date_key]) REFERENCES [dbo].[DimDate]([date_key]);