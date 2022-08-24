With click as
(   
    SELECT 
    'View' as EventName,
     CASE WHEN (SELECT value.string_value from unnest (user_properties) where key in ("firebase_exp_288","firebase_exp_289") ) = '0' then 'Baseline'
     
     when (SELECT value.string_value from unnest (user_properties) where key in ("firebase_exp_288","firebase_exp_289") ) = '1' then 'Experiment 1 ' 
       
     when (SELECT value.string_value from unnest (user_properties) where key in ("firebase_exp_288","firebase_exp_289") ) = '2' then 'Experiment 2 ' 
       
     when (SELECT value.string_value from unnest (user_properties) where key in ("firebase_exp_288","firebase_exp_289") ) = '3' then 'Experiment 3 ' 
       
    END AS ABType ,
     event_date, 
     platform,
     user_pseudo_id
    FROM sendo-data-platform.b401_ha_firebasebuyer.trf_b201_og_item_impression
    where 1 =1 
    and event_date >= '2022-08-10'
    and event_date <= '2022-08-22'
    and (SELECT key from unnest(user_properties) where key in ("firebase_exp_288","firebase_exp_289") ) is not null 
     
     
      and (select value.string_value from unnest (event_params) where key = 'page_id' ) in ("og_home") 
     
    
    
    
   

    UNION ALL 
   
    SELECT 
    'Click' as EventName,
     CASE WHEN (SELECT value.string_value from unnest (user_properties) where key in ("firebase_exp_288","firebase_exp_289") ) = '0' then 'Baseline'
     
     when (SELECT value.string_value from unnest (user_properties) where key in ("firebase_exp_288","firebase_exp_289") ) = '1' then 'Experiment 1 ' 
       
     when (SELECT value.string_value from unnest (user_properties) where key in ("firebase_exp_288","firebase_exp_289") ) = '2' then 'Experiment 2 ' 
       
     when (SELECT value.string_value from unnest (user_properties) where key in ("firebase_exp_288","firebase_exp_289") ) = '3' then 'Experiment 3 ' 
       
    END AS ABType ,
    event_date, 
     platform,
     user_pseudo_id
    FROM sendo-data-platform.b401_ha_firebasebuyer.trf_b201_og_adjust
    where 1 =1 
    and event_date >= '2022-08-10'
    and event_date <= '2022-08-22'
    and (SELECT key from unnest(user_properties) where key in ("firebase_exp_288","firebase_exp_289") ) is not null 
      
     
      and (select value.string_value from unnest (event_params) where key = 'page_id' ) in ("og_home") 
     
      and (select value.string_value from unnest (event_params) where key = 'type' ) in ("add") 
     
     
    
  



    UNION ALL 
  
    SELECT 
    'Click' as EventName,
    CASE WHEN (SELECT value.string_value from unnest (user_properties) where key in ("firebase_exp_288","firebase_exp_289") ) = '0' then 'Baseline'
     
     when (SELECT value.string_value from unnest (user_properties) where key in ("firebase_exp_288","firebase_exp_289") ) = '1' then 'Experiment 1 ' 
       
     when (SELECT value.string_value from unnest (user_properties) where key in ("firebase_exp_288","firebase_exp_289") ) = '2' then 'Experiment 2 ' 
       
     when (SELECT value.string_value from unnest (user_properties) where key in ("firebase_exp_288","firebase_exp_289") ) = '3' then 'Experiment 3 ' 
       
    END AS ABType ,
     event_date, 
     platform,
     user_pseudo_id
    FROM sendo-data-platform.b401_ha_firebasebuyer.trf_b201_og_view_item_add_to_cart
    where 1 =1 
    and event_date >= '2022-08-10'
    and event_date <= '2022-08-22'
    and (SELECT key from unnest(user_properties) where key in ("firebase_exp_288","firebase_exp_289") ) is not null 
    
       
        and (select value.string_value from unnest (event_params) where key = 'source_page_id' ) in ("og_home") 
       
    
    
  






),
Total_Raw AS (SELECT 
 event_date,
 platform,
 EventName, 
 ABtype,
 user_pseudo_id,
 COUNT(distinct user_pseudo_id||event_date) User,
 COUNT (user_pseudo_id) Event
from click 


GROUP BY 1,2,3,4,5),
Total_Per AS(
  SELECT 
  event_date,
  platform,
  EventName, 
  ABtype,
  user_pseudo_id,
  User,
  Event,
  PERCENTILE_CONT(Event, 0.998) OVER(partition BY event_date,platform, EventName, ABType ) AS Percentile99Event
  
  FROM Total_Raw
  WHERE 1=1
  )
,
Total_Clean AS(
  SELECT 
  event_date,
  platform,
  EventName, 
  ABtype,
  SUM(User) TotalUser,
  SUM(Event) TotalEvent,
  FROM Total_Per
  WHERE 1=1
  AND Event < Percentile99Event
  GROUP BY 1, 2, 3, 4

)
SELECT * from Total_Clean
Pivot (SUM(TotalUser) TotalUser, SUM(TotalEvent) TotalEvent For EventName in ('Click','View','Purchase'))
