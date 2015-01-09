exec sp_BlitzTrace @Action='start', @SessionId=53, @TargetPath='S:\XEvents\Traces\'
GO

exec sp_BlitzTrace @Action='read'
GO


exec sp_BlitzTrace @Action='read', @TargetPath='S:\XEvents\Traces\'
GO



exec sp_BlitzTrace @Action='stop'
GO


exec sp_BlitzTrace @Action='drop'
GO


use StackOverflow;
GO
SELECT TOP 10 *
FROM dbo.Posts
ORDER by ViewCount DESC;
GO
/* This throws a compile error, but will still show up in the trace */
/* Result=ERROR */
SELECT TOP 10 *
FROM dbo.Posts
ORDER by ViewCount DESC (RECOMPILE);
GO
SELECT TOP 10 *
FROM dbo.Posts
ORDER by ViewCount DESC OPTION (RECOMPILE);
GO
