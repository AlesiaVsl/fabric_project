[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Query1_DataDestination", IsNewTarget = true], Settings = [Kind = "Manual", AllowCreation = true, ColumnSettings = [Mappings = {[SourceColumnName = "location_id", DestinationColumnName = "location_id"], [SourceColumnName = "location_name", DestinationColumnName = "location_name"], [SourceColumnName = "locality", DestinationColumnName = "locality"], [SourceColumnName = "sensor_id", DestinationColumnName = "sensor_id"], [SourceColumnName = "sensor_name", DestinationColumnName = "sensor_name"], [SourceColumnName = "parameter_name", DestinationColumnName = "parameter_name"], [SourceColumnName = "units", DestinationColumnName = "units"], [SourceColumnName = "latitude", DestinationColumnName = "latitude"], [SourceColumnName = "longitude", DestinationColumnName = "longitude"]}], DynamicSchema = true, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared Query1 = let
    apiKey = "06963539458c464a6f5c2231823946f69d91f607173c79bbb78815089a912458",
    bbox = "-74.05,40.68,-73.90,40.88",
    response = Json.Document(
        Web.Contents("https://api.openaq.org/v3/locations",
            [
                Headers = [#"X-API-Key" = apiKey],
                Query = [bbox = bbox, limit = "1000"]
            ])),
    locations = response[results],
    locTable = Table.FromList(locations, Splitter.SplitByNothing(), null, null, ExtraValues.Error),
    expLoc = Table.ExpandRecordColumn(locTable, "Column1", 
        {"id", "name", "locality", "sensors", "coordinates"}, 
        {"location_id", "location_name", "locality", "sensors", "coordinates"}),
    expCoord = Table.ExpandRecordColumn(expLoc, "coordinates",
        {"latitude", "longitude"},
        {"latitude", "longitude"}),
    expSensors = Table.ExpandListColumn(expCoord, "sensors"),
    expSensorRec = Table.ExpandRecordColumn(expSensors, "sensors", 
        {"id", "name", "parameter"}, 
        {"sensor_id", "sensor_name", "parameter"}),
    expParam = Table.ExpandRecordColumn(expSensorRec, "parameter",
        {"name", "units"},
        {"parameter_name", "units"}),
    typed = Table.TransformColumnTypes(expParam, {
        {"location_id", Int64.Type},
        {"location_name", type text},
        {"locality", type text},
        {"latitude", type number},
        {"longitude", type number},
        {"sensor_id", Int64.Type},
        {"sensor_name", type text},
        {"parameter_name", type text},
        {"units", type text}
    })
in
    typed;
shared Query1_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false, HierarchicalNavigation = true, EnableVorder = true, OutputMetadataRefresh = true]),
  Navigation_1 = Pattern{[workspaceId = "7b50e941-562a-45d8-84ca-2f60673e114d"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "366e598f-5f6e-45ff-8358-b2d0268dc08f"]}[Data],
  Navigation_3 = Navigation_2{[Id = "dbo", ItemKind = "Schema"]}[Data],
  TableNavigation = Navigation_3{[Id = "openaq_locations", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
