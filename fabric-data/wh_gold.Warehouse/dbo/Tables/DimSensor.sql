CREATE TABLE [dbo].[DimSensor] (

	[sensor_id] bigint NULL, 
	[location_id] bigint NULL, 
	[location_name] varchar(8000) NULL, 
	[locality] varchar(8000) NULL, 
	[latitude] float NULL, 
	[longitude] float NULL, 
	[sensor_borough] varchar(20) NULL
);