/*
Defining the granularity - allowing selection of daily, weekly, monthly, or total figures within a specified date range for investigation.
*/
WITH date_range as (
  SELECT
    -- 'DAILY' as Granularity         --Use this when you want daily figures
    'WEEKLY' as Granularity           --Use this when you want weekly figures
    --'MONTHLY' as Granularity        --Use this when you want monthly figures
    --'TOTAL' as Granularity          --Use this when you want total figures for a custom date range
    ,date ('2024-06-10') as start_    --The first date of the date range you want to investigate
    ,date ('2024-06-16') as end_      --The last date of the date range you want to investigate                  
   )

-- [[[ END OF CONTROL PANEL - Run Script ]]] 

/* 
Filters data to voice events only,
then pulls together voice reach and the number of utterances.
These metrics are then grouped by voice method 
*/
,base AS (
  select distinct
      CASE (SELECT Granularity FROM date_range)       --Aggregates the data to the specified granularity and returns the first date for the reporting period  
        WHEN 'DAILY' THEN date (event_timestamp_utc)
        WHEN 'WEEKLY' THEN date (timestamp_trunc(event_timestamp_utc, week(monday)))
        WHEN 'MONTHLY' THEN date (timestamp_trunc(event_timestamp_utc, month))
        WHEN 'TOTAL' THEN (SELECT start_ FROM date_range)
      END as period_start_date
      ,count (distinct CASE WHEN action_error_message IS NULL OR action_error_message NOT IN ('EPG_AccidentalPress','AS_FFV_WAKEWORD_FAIL','SHORT_UTTERANCE') THEN serial_number end) as active_base_panels
      ,count (distinct CASE WHEN action_error_message IS NULL OR action_error_message IN ('EPG_NavigationalCommand','EPG_NoResults','Voice search not supported.','EPG_NoError') THEN serial_number END) as successful_active_base_panels
      ,count (CASE WHEN action_error_message IS NULL OR action_error_message NOT IN ('EPG_AccidentalPress','AS_FFV_WAKEWORD_FAIL','SHORT_UTTERANCE') THEN CONCAT(serial_number, CAST(event_timestamp_utc AS STRING)) END) as total_valid_voice_utterances   
  ,count (CASE WHEN action_error_message IS NULL OR action_error_message IN ('EPG_NavigationalCommand','EPG_NoResults','Voice search not supported.','EPG_NoError') THEN CONCAT(serial_number, CAST(event_timestamp_utc AS STRING)) END) as total_successful_voice_utterances
  ,count(distinct serial_number) glass_users
FROM `[TABLE HIDDEN DUE TO DATA PRIVACY REASONS]`  
      WHERE  
        session_end_date between (select start_ from date_range) and (select date_add (end_, INTERVAL 5 DAY) from date_range)       --Filters the data to the specified partition window
        AND (date (event_timestamp_utc) BETWEEN (SELECT start_ FROM date_range) AND (SELECT end_ FROM date_range))                     --Filters the data to the specified date range
        AND action_id in ('01605')              --Filters the data to voice actions only
        group by 
        period_start_date
)

/* 
OUTPUT - the table that gets displayed
*/
select
  period_start_date
  -- ,voice_method
  ,total_valid_voice_utterances as total_pa_utt                      --Total number of voice utterances/commands
  ,total_successful_voice_utterances as successful_pa_utt            --Total number of successful voice utterances/commands
  ,active_base_panels as total_active_devices                           --Total voice reach
  ,successful_active_base_panels as successful_active_devices           --Total successful voice reach
  ,glass_users
  ,ROUND((active_base_panels/glass_users)*100,0) percentage_using_voice
from base
order by 
  period_start_date desc
