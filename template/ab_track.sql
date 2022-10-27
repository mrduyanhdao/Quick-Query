With click as
(  {% for tablename,alias in table1.items() %} 
    SELECT 
   
    '{{ alias }}' as EventName,
     CASE WHEN (SELECT value.string_value from unnest (event_params) where key ='algo' ) = 'oghome{{starting_variant}}' then 'Baseline'
    {% for variant in range(starting_variant+1, starting_variant+variant_num+1) %} 
    when (SELECT value.string_value from unnest (event_params) where key ='algo' ) = 'oghome{{variant}}' then 'Experiment {{variant}} ' 
      {% endfor %} 
    else 'Not_AB'
    END AS ABType ,
     event_date, 
     platform,
    (SELECT value.string_value from unnest(event_params) where key ='track_id') user_pseudo_id
    FROM {{ tablename }}
    where 1 =1 
    and event_date >= '{{ input_date }}'
    and event_date <= '{{ output_date }}'
   
     {% if in_check1 == True %}
    {% for key,value in in_condition1.items() %} 
      and (select value.string_value from unnest (event_params) where key = '{{key}}' ) in {{value}} 
    {% endfor %} 
    {% endif %}
    {% if notin_check1 == True %}
      {% for key,value in notin_condition1.items() %} 
       and (LOWER((select value.string_value from unnest (event_params) where key = '{{key}}' )) not in {{value}}
      OR (select value.string_value from unnest (event_params) where key = '{{key}}' ) is null)
      {% endfor %} 
    {% endif %}
    {% endfor %}
   
{% if action2== True %}
    UNION ALL 
  {% for tablename,alias in table2.items() %} 
    SELECT 
    '{{ alias }}' as EventName,
     CASE WHEN (SELECT value.string_value from unnest (event_params) where key ='algo' ) = 'oghome{{starting_variant}}' then 'Baseline'
    {% for variant in range(starting_variant+1, starting_variant+variant_num+1) %} 
    when (SELECT value.string_value from unnest (event_params) where key ='algo' ) = 'oghome{{variant}}' then 'Experiment {{variant}} ' 
      {% endfor %} 
    else 'Not_AB'
    END AS ABType ,
    event_date, 
     platform,
    (SELECT value.string_value from unnest(event_params) where key ='track_id') user_pseudo_id
    FROM {{ tablename }}
    where 1 =1 
    and event_date >= '{{ input_date }}'
    and event_date <= '{{ output_date }}'
    and (SELECT key from unnest(user_properties) where key in {{ ab_test }} ) is not null 
      {% if in_check2 == True %}
    {% for key,value in in_condition2.items() %} 
      and (select value.string_value from unnest (event_params) where key = '{{key}}' ) in {{value}} 
    {% endfor %} 
     {% endif %}
    {% if notin_check2 == True %}
      {% for key,value in notin_condition2.items() %} 
       and (LOWER((select value.string_value from unnest (event_params) where key = '{{key}}' )) not in {{value}}
      OR (select value.string_value from unnest (event_params) where key = '{{key}}' ) is null)
      {% endfor %} 
    {% endif %}
  {% endfor %}
{% endif %}

{% if action3 == True %}
    UNION ALL 
 {% for tablename,alias in table3.items() %} 
    SELECT 
    '{{ alias }}' as EventName,
     CASE WHEN (SELECT value.string_value from unnest (event_params) where key ='algo' ) = 'oghome{{starting_variant}}' then 'Baseline'
    {% for variant in range(starting_variant+1, starting_variant+variant_num+1) %} 
    when (SELECT value.string_value from unnest (event_params) where key ='algo' ) = 'oghome{{variant}}' then 'Experiment {{variant}} ' 
      {% endfor %} 
    else 'Not_AB'
    END AS ABType ,
     event_date, 
     platform,
      (SELECT value.string_value from unnest(event_params) where key ='track_id') user_pseudo_id
    FROM {{ tablename }}
    where 1 =1 
    and event_date >= '{{ input_date }}'
    and event_date <= '{{ output_date }}'
    and (SELECT key from unnest(user_properties) where key in {{ ab_test }} ) is not null 
    {% if in_check3 == True %}
      {% for key,value in in_condition3.items() %} 
        and (select value.string_value from unnest (event_params) where key = '{{key}}' ) in {{value}} 
      {% endfor %} 
    {% endif %}
    {% if notin_check3 == True %}
      {% for key,value in notin_condition3.items() %} 
       and (LOWER((select value.string_value from unnest (event_params) where key = '{{key}}' )) not in {{value}}
      OR (select value.string_value from unnest (event_params) where key = '{{key}}' ) is null)
      {% endfor %} 
    {% endif %}
  {% endfor %}
{% endif %}

{% if action4 == True %}
    UNION ALL 
 {% for tablename,alias in table4.items() %} 
    SELECT 
    '{{ alias }}' as EventName,
         CASE WHEN (SELECT value.string_value from unnest (event_params) where key ='algo' ) = 'oghome{{starting_variant}}' then 'Baseline'
    {% for variant in range(starting_variant+1, starting_variant+variant_num+1) %} 
    when (SELECT value.string_value from unnest (event_params) where key ='algo' ) = 'oghome{{variant}}' then 'Experiment {{variant}} ' 
      {% endfor %} 
    else 'Not_AB'
    END AS ABType ,
     event_date, 
     platform,
     (SELECT value.string_value from unnest(event_params) where key ='track_id') user_pseudo_id
    FROM {{ tablename }}
    where 1 =1 
    and event_date >= '{{ input_date }}'
    and event_date <= '{{ output_date }}'
    and (SELECT key from unnest(user_properties) where key in {{ ab_test }} ) is not null 
    {% if in_check4 == True %}
      {% for key,value in in_condition4.items() %} 
        and (select value.string_value from unnest (event_params) where key = '{{key}}' ) in {{value}} 
      {% endfor %} 
    {% endif %}
    {% if notin_check4 == True %}
      {% for key,value in notin_condition4.items() %} 
       and (LOWER((select value.string_value from unnest (event_params) where key = '{{key}}' )) not in {{value}}
      OR (select value.string_value from unnest (event_params) where key = '{{key}}' ) is null)
      {% endfor %} 
    {% endif %}
  {% endfor %}
{% endif %}

{% if action5 == True %}
    UNION ALL 
 {% for tablename,alias in table5.items() %} 
    SELECT 
    '{{ alias }}' as EventName,
        CASE WHEN (SELECT value.string_value from unnest (event_params) where key ='algo' ) = 'oghome{{starting_variant}}' then 'Baseline'
    {% for variant in range(starting_variant+1, starting_variant+variant_num+1) %} 
    when (SELECT value.string_value from unnest (event_params) where key ='algo' ) = 'oghome{{variant}}' then 'Experiment {{variant}} ' 
      {% endfor %} 
    else 'Not_AB'
    END AS ABType ,
    event_date, 
     platform,
      (SELECT value.string_value from unnest(event_params) where key ='track_id') user_pseudo_id
    FROM {{ tablename }}
    where 1 =1 
    and event_date >= '{{ input_date }}'
    and event_date <= '{{ output_date }}'
    and (SELECT key from unnest(user_properties) where key in {{ ab_test }} ) is not null 
    {% if in_check5 == True %}
      {% for key,value in in_condition5.items() %} 
        and (select value.string_value from unnest (event_params) where key = '{{key}}' ) in {{value}} 
      {% endfor %} 
    {% endif %}
    {% if notin_check5 == True %}
      {% for key,value in notin_condition5.items() %} 
     and (LOWER((select value.string_value from unnest (event_params) where key = '{{key}}' )) not in {{value}}
      OR (select value.string_value from unnest (event_params) where key = '{{key}}' ) is null)
      {% endfor %} 
    {% endif %}
  {% endfor %}
{% endif %}

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
