-- Remember to change path to files

CREATE EVENT SESSION [KunLogin] ON SERVER 
ADD EVENT sqlserver.error_reported(
    ACTION(sqlserver.client_app_name,sqlserver.client_hostname,sqlserver.database_id,sqlserver.database_name,
	sqlserver.nt_username,sqlserver.server_instance_name,sqlserver.server_principal_name,
	sqlserver.session_nt_username,sqlserver.session_server_principal_name,sqlserver.username)
    WHERE ([severity]=(14) AND [error_number]=(18456) AND [package0].[greater_than_int64]([state],(1)))),
ADD EVENT sqlserver.login(SET collect_options_text=(1)
    ACTION(sqlserver.client_app_name,sqlserver.client_hostname,sqlserver.database_id,sqlserver.database_name,
	sqlserver.nt_username,sqlserver.server_instance_name,sqlserver.server_principal_name,
	sqlserver.session_nt_username,sqlserver.session_server_principal_name,sqlserver.username))
ADD TARGET package0.event_file(SET filename=N'X:\xEvent\test\Logins.xel',max_file_size=(100),max_rollover_files=(1000))
WITH (MAX_MEMORY=8192 KB,
      EVENT_RETENTION_MODE=ALLOW_SINGLE_EVENT_LOSS,
	  MAX_DISPATCH_LATENCY=30 SECONDS,
	  MAX_EVENT_SIZE=0 KB,
	  MEMORY_PARTITION_MODE=NONE,
	  TRACK_CAUSALITY=ON,
	  STARTUP_STATE=ON)
GO

ALTER EVENT SESSION KunLogin
ON SERVER
STATE=START
GO

ALTER EVENT SESSION KunLogin
ON SERVER
STATE=STOP
GO
--TRUNCATE TABLE LoginOpsamling.dbo.LoginEvent;

-- initial load
SELECT file_name,
       file_offset,
	   CAST(SUBSTRING(RIGHT(file_name,22),1,18) AS BIGINT) as file_time,
	   CAST(event_data AS XML) AS event_data
into event_staging
FROM sys.fn_xe_file_target_read_file('X:\xEvent\test\log*.xel', null, null, null)

DECLARE @filename nvarchar(500),
        @file_offset bigint;

SELECT TOP 1 @filename = FILE_NAME, @file_offset = file_offset from [LoginOpsamling].[dbo].[event_staging] order by file_time desc

INSERT INTO event_staging
SELECT file_name,
       file_offset,
	   CAST(SUBSTRING(RIGHT(file_name,22),1,18) AS BIGINT) as file_time,
	   CAST(event_data AS XML) AS event_data
FROM sys.fn_xe_file_target_read_file('X:\xEvent\test\log*.xel', null, @filename, @file_offset)

INSERT INTO LoginOpsamling.dbo.LoginEvent
SELECT 
    iif(n.value('(@name)[1]','varchar(50)') = 'error_reported','failed login',n.value('(@name)[1]','varchar(50)')) AS event_name,
    DATEADD(hh, DATEDIFF(hh, GETUTCDATE(), CURRENT_TIMESTAMP), n.value('(@timestamp)[1]', 'datetime2')) AS [event_timestamp],
	n.value('(action[@name="server_instance_name"]/value)[1]'   , 'varchar(100)') as server_instance_name,
	n.value('(action[@name="session_server_principal_name"]/value)[1]'  , 'varchar(100)') as session_server_principal_name,
    n.value('(action[@name="username"]/value)[1]'               , 'varchar(100)') as username,
	n.value('(action[@name="server_principal_name"]/value)[1]'  , 'varchar(100)') as server_principal_name,
	n.value('(action[@name="nt_username"]/value)[1]'            , 'varchar(100)') as nt_username,
	n.value('(action[@name="session_nt_username"]/value)[1]'    , 'varchar(100)') as session_nt_username,
	n.value('(action[@name="database_id"]/value)[1]'            , 'int') as database_id,
	n.value('(action[@name="database_name"]/value)[1]'          , 'varchar(128)') as database_name,
	n.value('(action[@name="client_hostname"]/value)[1]'        , 'varchar(100)') as client_hostname,
	n.value('(action[@name="client_app_name"]/value)[1]'        , 'varchar(200)') as client_app_name,
    isnull(n.value('(data[@name="options_text"]/value)[1]', 'varchar(max)'),n.value('(data[@name="message"]/value)[1]', 'varchar(max)')) as xmessage
FROM 
(SELECT event_data from event_staging
) as tab
CROSS APPLY event_data.nodes('event') as q(n)

