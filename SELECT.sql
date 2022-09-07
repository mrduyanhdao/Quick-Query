  SELECT
    TIMESTAMP_MICROS(Event_Timestamp) AS EventTimestamp,
    Event_Date AS EventDate,
    Platform AS Platform,
    App_Info.Version AS AppVersion,
    'StationSelect' AS EventName,
    User_Pseudo_Id AS UserPseudoId,
  FROM  {{ source('b401_ha_firebasebuyer','trf_b201_sendo_station_location_click') }}
  WHERE 1 = 1
    AND Event_Date >= (SELECT InputDate FROM Daterange)
    AND Event_Date < (SELECT OutputDate FROM Daterange)