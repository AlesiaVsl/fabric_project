CREATE TABLE [dbo].[DimGDP] (

	[year] int NOT NULL, 
	[gdp_current_usd] decimal(20,2) NULL
);


GO
ALTER TABLE [dbo].[DimGDP] ADD CONSTRAINT PK_DimGDP primary key NONCLUSTERED ([year]);