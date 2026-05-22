[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "openaq_nyc_daily_DataDestination", IsNewTarget = true], Settings = [Kind = "Manual", AllowCreation = true, ColumnSettings = [Mappings = {[SourceColumnName = "value", DestinationColumnName = "value"], [SourceColumnName = "parameter_name", DestinationColumnName = "parameter_name"], [SourceColumnName = "units", DestinationColumnName = "units"], [SourceColumnName = "utc", DestinationColumnName = "utc"], [SourceColumnName = "sensor_id", DestinationColumnName = "sensor_id"]}], DynamicSchema = false, UpdateMethod = [Kind = "Append"], TypeSettings = [Kind = "Table"]]]}]
shared openaq_nyc_daily = let
   
    apiKey = openaq_api_key,   
    dateTo   = DateTime.Date(DateTime.LocalNow()),
    dateFrom = Date.AddDays(dateTo, -10),
    dateFromText = Date.ToText(dateFrom, "yyyy-MM-dd"),
    dateToText   = Date.ToText(dateTo, "yyyy-MM-dd"),

    locationsResponse = Json.Document(
        Web.Contents(
            "https://api.openaq.org/v3/locations",
            [
                Headers = [#"X-API-Key" = apiKey],
                Query = [
                    bbox = "-74.05,40.68,-73.90,40.88",
                    parameters_id = "2",
                    limit = "100"
                ]
            ]
        )
    ),

    locations = locationsResponse[results],
    locTable = Table.FromList(locations, Splitter.SplitByNothing(), null, null, ExtraValues.Error),
    expLoc = Table.ExpandRecordColumn(locTable, "Column1", {"id", "name", "sensors"}, {"location_id", "location_name", "sensors"}),
    expSensors = Table.ExpandListColumn(expLoc, "sensors"),
    expSensorRec = Table.ExpandRecordColumn(expSensors, "sensors", {"id", "name"}, {"sensor_id", "sensor_name"}),

    filtered = Table.SelectRows(expSensorRec, each Text.Contains([sensor_name], "pm25") or Text.Contains([sensor_name], "no2")),

   
    GetDays = (sid as number) =>
        let
          
            delayed = Function.InvokeAfter(
                () =>
                    try Json.Document(
                        Web.Contents(
                            "https://api.openaq.org/v3/sensors/" & Text.From(sid) & "/days",
                            [
                                Headers = [#"X-API-Key" = apiKey],
                                Query = [
                                    date_from = dateFromText,
                                    date_to   = dateToText,
                                    limit     = "100"
                                ],
                                
                                ManualStatusHandling = {429, 500, 502, 503}
                            ]
                        )
                    ) otherwise null,
                #duration(0, 0, 0, 0.5)
            ),
            resp = delayed,
            results = if resp <> null then try resp[results] otherwise null else null,
            tbl = if results <> null and List.Count(results) > 0 then
                let
                    t = Table.FromList(results, Splitter.SplitByNothing(), null, null, ExtraValues.Error),
                    e1 = Table.ExpandRecordColumn(t, "Column1", {"value", "parameter", "period"}, {"value", "parameter", "period"}),
                    e2 = Table.ExpandRecordColumn(e1, "parameter", {"name", "units"}, {"parameter_name", "units"}),
                    e3 = Table.ExpandRecordColumn(e2, "period", {"datetimeFrom"}, {"date"}),
                    e4 = Table.AddColumn(e3, "sensor_id", each sid)
                in e4
            else Table.FromRecords({})
        in tbl,

    sensorIds = List.Distinct(filtered[sensor_id]),
    allData = List.Transform(sensorIds, each GetDays(_)),
    nonEmpty = List.Select(allData, each Table.RowCount(_) > 0),
    combined = if List.Count(nonEmpty) > 0 then Table.Combine(nonEmpty) else Table.FromRecords({}),
    #"Expanded date" = Table.ExpandRecordColumn(combined, "date", {"utc"}, {"utc"}),
    #"Changed column type" = Table.TransformColumnTypes(#"Expanded date", {{"value", type number}, {"parameter_name", type text}, {"units", type text}, {"utc", type text}, {"sensor_id", type text}})
in
    #"Changed column type";
shared openaq_api_key = "739ce404a4d90cbfdf84621253dbbf6248a1e750b4cf5dfdcae77507737b57b5" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type text];
shared openaq_nyc_daily_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false, HierarchicalNavigation = null, EnableVorder = true, OutputMetadataRefresh = true]),
  Navigation_1 = Pattern{[workspaceId = "7b50e941-562a-45d8-84ca-2f60673e114d"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "366e598f-5f6e-45ff-8358-b2d0268dc08f"]}[Data],
  TableNavigation = Navigation_2{[Id = "openaq_nyc_daily", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
