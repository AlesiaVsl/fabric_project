CREATE TABLE [dbo].[DimZone] (

	[location_id] int NOT NULL, 
	[borough] varchar(50) NULL, 
	[zone_name] varchar(100) NULL, 
	[service_zone] varchar(50) NULL
);


GO
ALTER TABLE [dbo].[DimZone] ADD CONSTRAINT PK_DimZone primary key NONCLUSTERED ([location_id]);