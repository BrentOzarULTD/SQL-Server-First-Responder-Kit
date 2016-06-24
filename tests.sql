SELECT 
	sp.name as SpName
    , p.name AS Parameter
FROM sys.procedures sp
JOIN sys.parameters p 
    ON sp.object_id = p.object_id
JOIN sys.types t
    ON p.system_type_id = t.system_type_id
WHERE 
	p.name like '%[_]%'
	and sp.name in ('sp_AskBrent', 'sp_Blitz', 'sp_BlitzCache', 'sp_BlitzIndex', 'sp_BlitzRS', 'sp_BlitzTrace')

IF @@ROWCOUNT > 0 
	RAISERROR ('Underscore(s) found in parameter names', 1,0);
