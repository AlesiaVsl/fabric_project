CREATE TABLE [dbo].[DimFX] (

	[date_key] int NOT NULL, 
	[rate_date] date NULL, 
	[eur_usd_rate] decimal(10,6) NULL
);


GO
ALTER TABLE [dbo].[DimFX] ADD CONSTRAINT PK_DimFX primary key NONCLUSTERED ([date_key]);