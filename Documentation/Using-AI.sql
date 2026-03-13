/* You can put the configuration tables in any database, and I personally
prefer using a DBA utility database for this. I usually put the First Responder
Kit stored procs in the master database so that I can refer to 'em from any
database, but these tables will have prompts and configs that you worked hard
to build, and you're probably going to want to restore them if something goes
wrong, plus you may want to replicate them to other servers.

I'm going to use DBAtools, but you can use another name if you want: */
USE DBAtools;
GO

/* Add a list of AI providers: */
CREATE TABLE dbo.Blitz_AI_Providers
(Id INT IDENTITY(1,1) PRIMARY KEY CLUSTERED,
 Model_Nickname NVARCHAR(200),
 AI_Model NVARCHAR(100),
 AI_URL NVARCHAR(500),
 AI_Database_Scoped_Credential_Name NVARCHAR(500),
 AI_Parameters NVARCHAR(4000),
 Payload_Template NVARCHAR(4000),
 Timeout_Seconds TINYINT,
 Context INT,
 Default_Model BIT DEFAULT 0);

/* OpenAI - fast, cheap model, default: */
INSERT INTO dbo.Blitz_AI_Providers (Model_Nickname, AI_Model, AI_URL, AI_Database_Scoped_Credential_Name, Timeout_Seconds, Default_Model)
VALUES (N'ChatGPT Fast', N'gpt-5-nano', N'https://api.openai.com/v1/chat/completions',
    N'https://api.openai.com/', 60, 1);

/* OpenAI - highest quality, slowest, most expensive model: */
INSERT INTO dbo.Blitz_AI_Providers (Model_Nickname, AI_Model, AI_URL, AI_Database_Scoped_Credential_Name, Timeout_Seconds, Default_Model)
VALUES (N'ChatGPT Slow', N'gpt-5.4', N'https://api.openai.com/v1/chat/completions',
    N'https://api.openai.com/', 230, 0);

/* Gemini - fast, cheap model: */
INSERT INTO dbo.Blitz_AI_Providers (Model_Nickname, AI_Model, AI_URL, AI_Database_Scoped_Credential_Name, Timeout_Seconds, Default_Model)
VALUES (N'Gemini Fast', N'gemini-3-flash-preview', N'https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent',
    N'https://generativelanguage.googleapis.com/', 60, 0);

/* Gemini - highest quality, slowest, most expensive model: */
INSERT INTO dbo.Blitz_AI_Providers (Model_Nickname, AI_Model, AI_URL, AI_Database_Scoped_Credential_Name, Timeout_Seconds, Default_Model)
VALUES (N'Gemini Slow', N'gemini-3-1-pro-preview', N'https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent',
    N'https://generativelanguage.googleapis.com/', 230, 0);


/* Create a default set of prompts: */
CREATE TABLE dbo.Blitz_AI_Prompts
(Id INT IDENTITY(1,1) PRIMARY KEY CLUSTERED,
 Prompt_Nickname NVARCHAR(200),
 AI_System_Prompt NVARCHAR(4000),
 Default_Prompt BIT DEFAULT 0);

INSERT INTO dbo.Blitz_AI_Prompts (Prompt_Nickname, Default_Prompt, AI_System_Prompt)
  VALUES ('sp_BlitzCache Default', 1, 'You are a very senior database developer working with Microsoft SQL Server and Azure SQL DB. You focus on real-world, actionable advice that will make a big difference, quickly. You value everyone''s time, and while you are friendly and courteous, you do not waste time with pleasantries or emoji because you work in a fast-paced corporate environment.

    You have a query that isn''t performing to end user expectations. You have been tasked with making serious improvements to it, quickly. You are not allowed to change server-level settings or make frivolous suggestions like updating statistics. Instead, you need to focus on query changes or index changes. 
    
    Do not offer followup options: the customer can only contact you once, so include all necessary information, tasks, and scripts in your initial reply. Render your output in Markdown, as it will be shown in plain text to the customer.');

INSERT INTO dbo.Blitz_AI_Prompts (Prompt_Nickname, Default_Prompt, AI_System_Prompt)
  VALUES ('sp_BlitzCache Index Tuning', 0, 'You are a very senior database developer working with Microsoft SQL Server and Azure SQL DB. You focus on real-world, actionable advice that will make a big difference, quickly. You value everyone''s time, and while you are friendly and courteous, you do not waste time with pleasantries or emoji because you work in a fast-paced corporate environment.

    You have a query that isn''t performing to end user expectations. You have been tasked with making serious improvements to it, quickly, but you are only allowed to make index changes. You are not allowed to make changes to the query, server-level settings, database settings, etc.
    
    Do not offer followup options: the customer can only contact you once, so include all necessary information, tasks, and scripts in your initial reply. Render your output in Markdown, as it will be shown in plain text to the customer.');

INSERT INTO dbo.Blitz_AI_Prompts (Prompt_Nickname, Default_Prompt, AI_System_Prompt)
  VALUES ('sp_BlitzCache Deadlock Tuning', 0, 'You are a very senior database developer working with Microsoft SQL Server and Azure SQL DB. You focus on real-world, actionable advice that will make a big difference, quickly. You value everyone''s time, and while you are friendly and courteous, you do not waste time with pleasantries or emoji because you work in a fast-paced corporate environment.

    You have a query that is experiencing deadlocks and blocking. You have been tasked with making serious improvements to it, quickly. You are not allowed to change server-level or database-level settings nor make frivolous suggestions like updating statistics. Instead, you need to focus on query changes or index changes that will reduce blocking and deadlocks.
    
    Do not offer followup options: the customer can only contact you once, so include all necessary information, tasks, and scripts in your initial reply. Render your output in Markdown, as it will be shown in plain text to the customer.');

INSERT INTO dbo.Blitz_AI_Prompts (Prompt_Nickname, Default_Prompt, AI_System_Prompt)
  VALUES ('sp_BlitzCache Modernize', 0, 'You are a very senior database developer working with Microsoft SQL Server and Azure SQL DB. You focus on real-world, actionable advice that will make a big difference, quickly. You value everyone''s time, and while you are friendly and courteous, you do not waste time with pleasantries or emoji because you work in a fast-paced corporate environment.

    You have been given a legacy query that needs to be modernized. Our goals are to make the query run faster, make it easier to understand, easier to maintain, and to take advantage of new features up to and including SQL Server 2025. You have been tasked with making serious improvements to it, quickly, without touching server-level settings, database-level settings, indexes, or statistics.
    
    Do not offer followup options: the customer can only contact you once, so include all necessary information, tasks, and scripts in your initial reply. Render your output in Markdown, as it will be shown in plain text to the customer.');
GO




/* Now switch over to the user database where you'll be tuning: */
USE StackOverflow;
GO

/* Create a master key for the database to allow for encryption of your AI
provider keys: */
CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'SomeStrongPassword!123';

/* If you've already got one, you can just open it instead: */
OPEN MASTER KEY DECRYPTION BY PASSWORD = 'SomeStrongPassword!123';

/* Turn on API calls - remember, this only works in SQL Server 2025 or newer :*/
EXEC sp_configure 'external rest endpoint enabled', 1;
RECONFIGURE WITH OVERRIDE;
GO


/* Go get API keys for OpenAI or Gemini (or both), and then copy/paste them
into the secrets below. */
CREATE DATABASE SCOPED CREDENTIAL [https://api.openai.com/]
WITH IDENTITY = 'HTTPEndpointHeaders',
     SECRET   = '{"Authorization":"Bearer YourChatGPTKeyGoesHere"}';
GO
/* Note that in the ChatGPT example above, you have to leave the "Bearer " 
string, including a space after the word Bearer, then paste your key over 
the string "YourChatGPTKeyGoesHere". */

CREATE DATABASE SCOPED CREDENTIAL [https://generativelanguage.googleapis.com/]
WITH IDENTITY = 'HTTPEndpointHeaders',
     SECRET = N'{"x-goog-api-key":"YourGeminiKeyGoesHere"}';
GO



/* Grant permissions for that credential to your DBA group,
or developers, or whoever you want to grant it to. I'm going
to create a new database role: */
CREATE ROLE [DBA_AI];
GO
/* Add users to that role if necessary - you're in it already: */
ALTER ROLE [DBA_AI] ADD MEMBER [MyBestFriend];
GO

/* Let the role use the credential, so only our friends
can run up charges on our API keys: */
GRANT REFERENCES ON DATABASE SCOPED CREDENTIAL::[https://api.openai.com/] TO [DBA_AI];
GO
GRANT REFERENCES ON DATABASE SCOPED CREDENTIAL::[https://generativelanguage.googleapis.com/] TO [DBA_AI];
GO


/* Test ChatGPT with an API call using the ChatGPT defaults. 
This will take 30-60 seconds: */
sp_BlitzCache @Top = 1, @AI = 1
GO

/* Or if you used Gemini, use this, pointed at your config table: */
sp_BlitzCache @AI = 1, @Top = 1,
	@AIConfigTable = 'DBAtools.dbo.Blitz_AI_Providers',
	@AIModel = 'gemini-3-flash-preview';
GO

/* Scroll across to the AI Advice column, and make sure you got advice.

If you got an error, read it. If it's a timeout, that's fine, your query was
too complex for the default fast model to digest in the time allowed.

If it's any other error, read it - it's likely a configuration error or a
network connectivity error if your firewall is blocking the SQL Server's access
to the internet, which is fair. No hate there. 
*/




/* OPTIONAL: Getting Better Advice

Add a database-level extended property with your company's code standards.
Don't worry too much about formatting - you can copy/paste it straight out of
any standards doc you've got - but keep it to a couple pages or less.

Everything you add in here gets sent to AI with each request, and the more text
in here, the longer it'll take (and the more it'll cost) for each request. */
EXECUTE sp_addextendedproperty
    @name = N'CONSTITUTION.md',
    @value = N'Any objects and T-SQL in this database must comply with the organizational standards and guidelines outlined in this constitution document.
    
    ## Object Naming Standards
    
    Views must always be prefixed with vw_.
    Tables should never be prefixed with tbl_.
    Table and column names should be in PascalCase with a capitalized first letter, like UserProperties or SalesByMonth.
    Index names should be based on the key columns in the index. If the index has include columns, add an _inc suffix to the index name.
    Index names should never be prefixed with table names, idx_, ix_, or any variation thereof.
    
    ## Query Standards
    
    Queries should be written in a concise, easy-to-understand, performant way.
    Queries should prefer CTEs over temp tables unless that presents a performance issue for the query.';
GO
/* You can only have one Constitution per database. You can update it, which
writes over the existing content: */
EXECUTE sp_updateextendedproperty
    @name = N'CONSTITUTION.md',
    @value = N'These are our new, much better standards...';
GO

/* Or you can drop it: */
EXEC sp_dropextendedproperty
    @name = N'CONSTITUTION.md';
GO

