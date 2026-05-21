[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "worldbank_usa_gdp_DataDestination", IsNewTarget = true], Settings = [Kind = "Manual", AllowCreation = true, ColumnSettings = [Mappings = {[SourceColumnName = "year", DestinationColumnName = "year"], [SourceColumnName = "gdp_current_usd", DestinationColumnName = "gdp_current_usd"]}], DynamicSchema = true, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared worldbank_usa_gdp = let
  Source = Json.Document(Web.Contents("https://api.worldbank.org/v2/country/USA/indicator/NY.GDP.MKTP.CD?format=json&date=2020:2025&per_page=100")),
  Navigation = Source{1},
  #"Converted to table" = Table.FromList(Navigation, Splitter.SplitByNothing(), null, null, ExtraValues.Error),
  #"Expanded Column1" = Table.ExpandRecordColumn(#"Converted to table", "Column1", {"date", "value"}, {"date", "value"}),
  #"Renamed columns" = Table.RenameColumns(#"Expanded Column1", {{"date", "year"}, {"value", "gdp_current_usd"}}),
  #"Changed column type" = Table.TransformColumnTypes(#"Renamed columns", {{"year", type text}, {"gdp_current_usd", type number}})
in
  #"Changed column type";
shared worldbank_usa_gdp_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false, HierarchicalNavigation = null, EnableVorder = true, OutputMetadataRefresh = true]),
  Navigation_1 = Pattern{[workspaceId = "7b50e941-562a-45d8-84ca-2f60673e114d"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "366e598f-5f6e-45ff-8358-b2d0268dc08f"]}[Data],
  TableNavigation = Navigation_2{[Id = "worldbank_usa_gdp", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
