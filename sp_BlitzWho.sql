-- Line 573
-- Old
SET @StringToExecute = N'COALESCE( CONVERT(VARCHAR(20), (ABS(r.total_elapsed_time) / 1000) / 86400) + '':'' + CONVERT(VARCHAR(20), (DATEADD(SECOND, (r.total_elapsed_time / 1000), 0) + DATEADD(MILLISECOND, (r.total_elapsed_time % 1000), 0)), 114), CONVERT(VARCHAR(20), DATEDIFF(SECOND, s.last_request_start_time, GETDATE()) / 86400) + '':'' + CONVERT(VARCHAR(20), DATEADD(SECOND, DATEDIFF(SECOND, s.last_request_start_time, GETDATE()), 0), 114) ) AS [elapsed_time] ,
-- New
SET @StringToExecute = N'COALESCE( RIGHT(''00'' + CONVERT(VARCHAR(20), (ABS(r.total_elapsed_time) / 1000) / 86400), 2) + '':'' + CONVERT(VARCHAR(20), (DATEADD(SECOND, (r.total_elapsed_time / 1000), 0) + DATEADD(MILLISECOND, (r.total_elapsed_time % 1000), 0)), 114), CONVERT(VARCHAR(20), DATEDIFF(SECOND, s.last_request_start_time, GETDATE()) / 86400) + '':'' + CONVERT(VARCHAR(20), DATEADD(SECOND, DATEDIFF(SECOND, s.last_request_start_time, GETDATE()), 0), 114) ) AS [elapsed_time] ,

-- Line 778
-- Old
SELECT @StringToExecute = N'COALESCE( CONVERT(VARCHAR(20), (ABS(r.total_elapsed_time) / 1000) / 86400) + '':'' + CONVERT(VARCHAR(20), (DATEADD(SECOND, (r.total_elapsed_time / 1000), 0) + DATEADD(MILLISECOND, (r.total_elapsed_time % 1000), 0)), 114), CONVERT(VARCHAR(20), DATEDIFF(SECOND, s.last_request_start_time, GETDATE()) / 86400) + '':'' + CONVERT(VARCHAR(20), DATEADD(SECOND, DATEDIFF(SECOND, s.last_request_start_time, GETDATE()), 0), 114) ) AS [elapsed_time] ,
--New
SELECT @StringToExecute = N'COALESCE( RIGHT(''00'' + CONVERT(VARCHAR(20), (ABS(r.total_elapsed_time) / 1000) / 86400), 2) + '':'' + CONVERT(VARCHAR(20), (DATEADD(SECOND, (r.total_elapsed_time / 1000), 0) + DATEADD(MILLISECOND, (r.total_elapsed_time % 1000), 0)), 114), CONVERT(VARCHAR(20), DATEDIFF(SECOND, s.last_request_start_time, GETDATE()) / 86400) + '':'' + CONVERT(VARCHAR(20), DATEADD(SECOND, DATEDIFF(SECOND, s.last_request_start_time, GETDATE()), 0), 114) ) AS [elapsed_time] ,
