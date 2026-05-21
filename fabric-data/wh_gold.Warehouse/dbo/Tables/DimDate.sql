CREATE TABLE [dbo].[DimDate] (

	[date_key] int NOT NULL, 
	[full_date] date NULL, 
	[year] int NULL, 
	[month] int NULL, 
	[day] int NULL, 
	[day_of_week] int NULL, 
	[day_name] varchar(15) NULL, 
	[month_name] varchar(15) NULL, 
	[quarter] int NULL, 
	[is_weekend] bit NULL
);


GO
ALTER TABLE [dbo].[DimDate] ADD CONSTRAINT PK_DimDate primary key NONCLUSTERED ([date_key]);