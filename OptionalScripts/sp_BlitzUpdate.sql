SET ANSI_NULLS ON;
SET ANSI_PADDING ON;
SET ANSI_WARNINGS ON;
SET ARITHABORT ON;
SET CONCAT_NULL_YIELDS_NULL ON;
SET QUOTED_IDENTIFIER ON;
SET STATISTICS IO OFF;
SET STATISTICS TIME OFF;
GO

IF OBJECT_ID('sys.sp_invoke_external_rest_endpoint') IS NULL
BEGIN
	DECLARE @msg VARCHAR(8000);
	SELECT @msg = 'Sorry, sp_BlitzUpdate requires sp_invoke_external_rest_endpoint, which needs SQL Server 2025 or Azure SQL DB.' + REPLICATE(CHAR(13), 7933);
	PRINT @msg;
	RETURN;
END;
GO

IF OBJECT_ID('dbo.sp_BlitzUpdate') IS NULL
	EXEC ('CREATE PROCEDURE dbo.sp_BlitzUpdate AS RETURN 0;');
GO

ALTER PROCEDURE [dbo].[sp_BlitzUpdate]
	@Branch             sysname        = N'main',
	@Repository         sysname        = N'BrentOzarULTD/SQL-Server-First-Responder-Kit',
	@FileName           sysname        = N'Install-All-Scripts.sql',
	@WhatIf             BIT            = 0,
	@Help               TINYINT        = 0,
	@Debug              TINYINT        = 0,
	@Version            VARCHAR(30)    = NULL OUTPUT,
	@VersionDate        DATETIME       = NULL OUTPUT,
	@VersionCheckMode   BIT            = 0
AS
BEGIN
	SET NOCOUNT ON;
	SET STATISTICS XML OFF;

	SELECT @Version = '0.1', @VersionDate = '20260515';

	IF @VersionCheckMode = 1
	BEGIN
		RETURN;
	END;

	IF @Help = 1
	BEGIN
		PRINT '
	/*
	sp_BlitzUpdate from http://FirstResponderKit.org

	Fetches a First Responder Kit file (default Install-All-Scripts.sql) from
	GitHub and installs it in the current database using SQL Server 2025''s
	sp_invoke_external_rest_endpoint. Intended for lab and dev machines that
	you want to keep current without sqlcmd / Powershell / RDP.

	This procedure lives in the OptionalScripts folder and is NOT part of the
	standard FRK install bundle. Copy it to a lab server, run it once, then
	call it whenever you want to refresh.

	Requirements:
	 - SQL Server 2025 (or Azure SQL DB) -- needs sp_invoke_external_rest_endpoint.
	 - sp_configure ''external rest endpoint enabled'', 1; RECONFIGURE; on boxed SQL.
	 - Outbound HTTPS to api.github.com on TCP 443.
	 - sysadmin (to install procs into master) and the privileges to call
	   sp_invoke_external_rest_endpoint.

	Trust model:
	 - Every call EXEC''s whatever GitHub serves at the requested ref. For
	   reproducible lab boxes, pass a tag to @Branch (for example ''20260407'')
	   instead of using ''main''. Anyone with write access to the chosen ref of
	   the chosen repo can run code on this server.

	Parameter explanations:

	@Branch         Branch or tag to pull from. Default ''main''. Date-based
	                release tags work too (e.g. ''20260407'').
	@Repository     ''owner/repo'' on GitHub. Default ''BrentOzarULTD/SQL-Server-First-Responder-Kit''.
	                Set to your fork if you maintain one.
	@FileName       Path to the file inside the repo. Default ''Install-All-Scripts.sql''.
	                Pass ''sp_Blitz.sql'' (etc.) to update one proc at a time.
	@WhatIf         0 = install (default). 1 = fetch and parse only; print the
	                batch count and exit. Use to preview before running.
	@Debug          0 = quiet. 1 = print per-batch length and progress.
	@Help           0 = run. 1 = print this help and return.

	Examples:

	  -- Latest from main:
	  EXEC dbo.sp_BlitzUpdate;

	  -- Pinned to a release tag:
	  EXEC dbo.sp_BlitzUpdate @Branch = ''20260407'';

	  -- Update a single proc:
	  EXEC dbo.sp_BlitzUpdate @FileName = ''sp_Blitz.sql'';

	  -- Preview without installing:
	  EXEC dbo.sp_BlitzUpdate @WhatIf = 1, @Debug = 1;

	  -- From a fork:
	  EXEC dbo.sp_BlitzUpdate @Repository = ''yourname/SQL-Server-First-Responder-Kit'',
	                          @Branch = ''dev'';

	Run this from the database you want the procs installed into. Most users
	want ''USE master;'' first, matching the regular FRK install workflow.

	GitHub''s anonymous API rate limit is 60 requests/hour per source IP. Each
	call uses one or two API requests. Plenty of headroom for routine lab use.

	MIT License

	Copyright (c) Brent Ozar Unlimited

	Permission is hereby granted, free of charge, to any person obtaining a copy
	of this software and associated documentation files (the "Software"), to deal
	in the Software without restriction, including without limitation the rights
	to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
	copies of the Software, and to permit persons to whom the Software is
	furnished to do so, subject to the following conditions:

	The above copyright notice and this permission notice shall be included in all
	copies or substantial portions of the Software.

	THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
	IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
	FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
	AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
	LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
	OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
	SOFTWARE.
	*/
';
		RETURN;
	END;

	/* Input validation. Two pieces are load-bearing:
	    - COLLATE Latin1_General_BIN2: without it, the default collation
	      silently strips NUL (0x00) bytes from both pattern and value, so a
	      NUL-terminated payload could slip past.
	    - The "-" sits immediately after "^" in the bracket class. Placed
	      anywhere else, T-SQL's LIKE parser treats it as a range start and
	      the literal hyphen is no longer in the allowed set -- a hyphenated
	      branch like "no-such-branch" or the default repo "BrentOzarULTD/
	      SQL-Server-First-Responder-Kit" would falsely fail. */
	IF @Branch IS NULL OR LEN(@Branch) = 0 OR LEN(@Branch) > 128
	   OR @Branch LIKE N'%[^-A-Za-z0-9._/]%' COLLATE Latin1_General_BIN2
	BEGIN
		RAISERROR('@Branch must be 1-128 chars of letters, digits, dot, underscore, slash, or hyphen only.', 16, 1);
		RETURN;
	END;

	IF @Repository IS NULL OR LEN(@Repository) = 0 OR LEN(@Repository) > 128
	   OR @Repository LIKE N'%[^-A-Za-z0-9._/]%' COLLATE Latin1_General_BIN2
	   OR @Repository NOT LIKE N'%/%' COLLATE Latin1_General_BIN2
	BEGIN
		RAISERROR('@Repository must be ''owner/repo'' (1-128 chars; letters, digits, dot, underscore, slash, hyphen).', 16, 1);
		RETURN;
	END;

	IF @FileName IS NULL OR LEN(@FileName) = 0 OR LEN(@FileName) > 256
	   OR @FileName LIKE N'%[^-A-Za-z0-9._/]%' COLLATE Latin1_General_BIN2
	   OR @FileName LIKE N'%..%' COLLATE Latin1_General_BIN2
	BEGIN
		RAISERROR('@FileName must be 1-256 chars (letters, digits, dot, underscore, slash, hyphen) with no ".." segments.', 16, 1);
		RETURN;
	END;

	DECLARE @contentsUrl  nvarchar(2048),
	        @blobUrl      nvarchar(2048),
	        @resp         nvarchar(max),
	        @httpCode     int,
	        @sha          nvarchar(64),
	        @size         bigint,
	        @encoding     nvarchar(32),
	        @b64          nvarchar(max),
	        @bin          varbinary(max),
	        @body         nvarchar(max),
	        @pos          int,
	        @next         int,
	        @batch        nvarchar(max),
	        @batchNum     int = 0,
	        @execCount    int = 0;

	SET @contentsUrl = N'https://api.github.com/repos/' + @Repository
	                 + N'/contents/' + @FileName
	                 + N'?ref=' + @Branch;

	IF @Debug = 1 PRINT 'sp_BlitzUpdate: GET ' + @contentsUrl;

	/* Step 1: Contents API. Returns sha for any file size; for files <= 1MB
	   the response also includes the full base64 content. */
	EXEC sys.sp_invoke_external_rest_endpoint
	     @url      = @contentsUrl,
	     @method   = N'GET',
	     @timeout  = 230,
	     @response = @resp OUTPUT;

	SET @httpCode = TRY_CAST(JSON_VALUE(@resp, '$.response.status.http.code') AS int);
	IF @httpCode <> 200
	BEGIN
		DECLARE @msg1 nvarchar(2048) = N'Contents API returned HTTP '
		     + ISNULL(CAST(@httpCode AS nvarchar(10)), N'(null)')
		     + N' for ' + @contentsUrl
		     + N'. Check @Branch, @Repository, @FileName, and outbound network.';
		RAISERROR(@msg1, 16, 1);
		RETURN;
	END;

	/* JSON_VALUE truncates at 4000 chars, so pull the base64 content via
	   OPENJSON which handles nvarchar(max). */
	SELECT @sha = sha, @size = size, @encoding = encoding, @b64 = content
	FROM OPENJSON(@resp, '$.result')
	WITH (
		sha       nvarchar(64)   '$.sha',
		size      bigint         '$.size',
		encoding  nvarchar(32)   '$.encoding',
		content   nvarchar(max)  '$.content'
	);

	IF @Debug = 1
		PRINT 'sp_BlitzUpdate: sha=' + ISNULL(@sha, '(null)')
		    + ' size=' + ISNULL(CAST(@size AS nvarchar(20)), '(null)')
		    + ' encoding=' + ISNULL(@encoding, '(null)');

	IF @sha IS NULL
	BEGIN
		RAISERROR('Contents API response had no sha field. Unexpected payload from GitHub.', 16, 1);
		RETURN;
	END;

	/* Step 2: if the Contents API did not include the content (file > 1MB),
	   fetch the blob by sha. The Blobs API returns base64 for any size up to
	   ~100MB. */
	IF @encoding <> N'base64' OR @b64 IS NULL OR LEN(@b64) = 0
	BEGIN
		SET @blobUrl = N'https://api.github.com/repos/' + @Repository
		             + N'/git/blobs/' + @sha;

		IF @Debug = 1 PRINT 'sp_BlitzUpdate: GET ' + @blobUrl;

		EXEC sys.sp_invoke_external_rest_endpoint
		     @url      = @blobUrl,
		     @method   = N'GET',
		     @timeout  = 230,
		     @response = @resp OUTPUT;

		SET @httpCode = TRY_CAST(JSON_VALUE(@resp, '$.response.status.http.code') AS int);
		IF @httpCode <> 200
		BEGIN
			DECLARE @msg2 nvarchar(2048) = N'Blobs API returned HTTP '
			     + ISNULL(CAST(@httpCode AS nvarchar(10)), N'(null)')
			     + N' for ' + @blobUrl;
			RAISERROR(@msg2, 16, 1);
			RETURN;
		END;

		SELECT @b64 = content, @encoding = encoding
		FROM OPENJSON(@resp, '$.result')
		WITH (
			content   nvarchar(max)  '$.content',
			encoding  nvarchar(32)   '$.encoding'
		);
	END;

	IF @b64 IS NULL OR LEN(@b64) = 0 OR @encoding <> N'base64'
	BEGIN
		RAISERROR('Could not obtain base64 content from GitHub.', 16, 1);
		RETURN;
	END;

	/* Decode base64 -> bytes via XML, then interpret bytes as UTF-8 via the
	   _SC_UTF8 collation cast. The implicit varchar->nvarchar conversion
	   produces UTF-16 that sp_executesql can run. */
	SET @bin  = CAST(N'' AS xml).value('xs:base64Binary(sql:variable("@b64"))', 'varbinary(max)');
	SET @body = CAST(@bin AS varchar(max)) COLLATE Latin1_General_100_CI_AS_SC_UTF8;

	IF @body IS NULL OR LEN(@body) < 100
	BEGIN
		RAISERROR('Decoded body is empty or too short to be a valid FRK script.', 16, 1);
		RETURN;
	END;

	/* Normalize line endings so the GO splitter pattern is unambiguous. */
	SET @body = REPLACE(@body, CHAR(13) + CHAR(10), CHAR(10));
	/* Strip a possible UTF-8 BOM (EF BB BF -> NCHAR(65279)). */
	IF LEFT(@body, 1) = NCHAR(65279)
		SET @body = SUBSTRING(@body, 2, LEN(@body));
	/* Collapse trailing whitespace on GO lines. The FRK install scripts have
	   at least one "GO " (with trailing space) line, and the splitter below
	   looks for an exact "<LF>GO<LF>". Stripping the whitespace here keeps
	   the loop simple. Repeated until idempotent so multiple spaces / tabs
	   are handled. */
	WHILE CHARINDEX(CHAR(10) + N'GO ',          @body) > 0
		SET @body = REPLACE(@body, CHAR(10) + N'GO ',          CHAR(10) + N'GO');
	WHILE CHARINDEX(CHAR(10) + N'GO' + CHAR(9), @body) > 0
		SET @body = REPLACE(@body, CHAR(10) + N'GO' + CHAR(9), CHAR(10) + N'GO');
	/* Append a terminating GO so the loop captures the last batch. */
	SET @body = @body + CHAR(10) + N'GO' + CHAR(10);

	IF @Debug = 1
		PRINT 'sp_BlitzUpdate: decoded ' + CAST(DATALENGTH(@bin) AS nvarchar(20))
		    + ' bytes; body length ' + CAST(LEN(@body) AS nvarchar(20)) + ' chars.';

	/* Walk the body, finding "<LF>GO<LF>" boundaries and exec'ing each
	   non-empty batch in order. No outer TRY/CATCH: if a batch fails, the
	   server raises with the offending text in the error stack, which is
	   more useful for debugging than a wrapped re-raise. */
	SET @pos = 1;
	WHILE 1 = 1
	BEGIN
		SET @next = CHARINDEX(CHAR(10) + N'GO' + CHAR(10), @body, @pos);
		IF @next = 0 BREAK;
		SET @batch = SUBSTRING(@body, @pos, @next - @pos);
		IF LEN(LTRIM(RTRIM(@batch))) > 0
		BEGIN
			SET @batchNum += 1;
			IF @Debug = 1
				PRINT 'sp_BlitzUpdate: batch ' + CAST(@batchNum AS nvarchar(10))
				    + ' length ' + CAST(LEN(@batch) AS nvarchar(20));
			IF @WhatIf = 0
			BEGIN
				EXEC sys.sp_executesql @batch;
				SET @execCount += 1;
			END;
		END;
		SET @pos = @next + 4;  /* past LF + 'GO' + LF */
	END;

	IF @WhatIf = 1
		PRINT 'sp_BlitzUpdate: WHATIF -- '
		    + CAST(@batchNum AS nvarchar(10))
		    + ' batches would run from '
		    + @Repository + N'@' + @Branch + N'/' + @FileName + N'.';
	ELSE
		PRINT 'sp_BlitzUpdate: '
		    + CAST(@execCount AS nvarchar(10))
		    + ' batches executed from '
		    + @Repository + N'@' + @Branch + N'/' + @FileName + N'.';
END;
GO
