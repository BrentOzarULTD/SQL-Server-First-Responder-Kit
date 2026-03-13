# Using AI with the First Responder Kit

sp_BlitzCache and sp_BlitzIndex can build AI prompts with your query and index data, and optionally call an AI provider (OpenAI or Google Gemini) to return recommendations directly in the result set.

- **`@AI = 2`** - Builds the AI prompt and returns it in the result set so you can copy/paste it into ChatGPT, Gemini, or another AI tool. Works on all supported versions of SQL Server and Azure SQL DB, no setup required.
- **`@AI = 1`** - Does everything `@AI = 2` does, plus calls the AI API directly from SQL Server and returns advice in the result set. Requires SQL Server 2025 or Azure SQL DB (uses `sp_invoke_external_rest_endpoint`).

If you want a single script that will walk you through the setup (which does still require your intervention for stuff like API keys), check out the [Using_AI.sql](Using_AI.sql) file.

## Getting Started: Generate Prompts with @AI = 2

The fastest way to try the AI features is `@AI = 2`. No credentials, no config tables, no API keys - just run the proc and copy the prompt into your favorite AI tool. (We named this AI = 2 because in the really long term - like a decade from now - we expect everybody to be on SQL Server 2025 or Azure SQL DB, and they'll just use AI = 1.)

### sp_BlitzCache: Get Query Tuning Prompts

```sql
EXEC sp_BlitzCache @Top = 1, @AI = 2;
```

The result set includes an **AI Prompt** column containing a pre-built prompt with the query text, execution plan, and performance metrics. Copy and paste it into ChatGPT, Gemini, Claude, or any AI tool.

### sp_BlitzIndex: Get Index Advice Prompts

sp_BlitzIndex's AI feature works in single-table mode (when `@TableName` is specified):

```sql
EXEC sp_BlitzIndex
    @DatabaseName = 'YourDatabase',
    @SchemaName = 'dbo',
    @TableName = 'YourTable',
    @AI = 2;
```

The **AI Prompt** column contains a prompt with four data sections about your table, the same kinds of 

1. **Existing Indexes** - Index names, types, key columns, include columns, filters, uniqueness, primary key status, and usage statistics (seeks, scans, lookups, writes, row counts).
2. **Missing Index Suggestions** - SQL Server's missing index DMV data including equality/inequality/include columns, benefit numbers, and suggested CREATE INDEX statements.
3. **Column Data Types** - All columns in the table with their data types, nullability, and identity status.
4. **Foreign Keys** - Foreign key names, parent/referenced columns, and whether they are disabled or not trusted.

The AI result sets appear immediately after the missing index result set in sp_BlitzIndex's output.

### Building Your Own Custom Prompts

If you want to override the default prompts, create a table to store your prompt variations. Here's the structure we use, and a few sample ideas for sp_BlitzCache prompts:

```sql
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
```

When you want to use one of those custom prompts, call sp_BlitzCache or sp_BlitzIndex like this:

```sql
EXEC sp_BlitzCache @Top = 1, @AI = 2,
@AIPromptConfigTable = 'master.dbo.Blitz_AI_Prompts',
@AIPrompt = 'sp_BlitzCache Modernize';
```

### Using a Database Constitution for Company Standards

Microsoft has implemented [database instructions](https://learn.microsoft.com/en-us/ssms/github-copilot/database-instructions) to influence the advice of GitHub Copilot and SSMS Copilot, and we support that in the First Responder Kit too.

Both sp_BlitzCache and sp_BlitzIndex support a database-level "constitution" - an extended property that provides additional context to the AI about your database's specific rules and constraints. Here's an example constitution:

```sql
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
```

When present, the constitution text is included with the AI prompt, giving the AI additional context about your environment.

You can also set up instructions at the object level (example below), but as of this writing, we don't include that with the AI prompt yet. Only one agents property per object is allowed, so you'll want to consolidate all of your information about that object in one property.

```sql
EXECUTE sp_addextendedproperty
    @name = N'AGENTS.md',
    @value = N'The Views column represents the number of times other people have viewed this user profile.
        The AboutMe column is an NVARCHAR(MAX), but only 4000 characters of content should be allowed for inserts and updates.',
    @level0type = N'SCHEMA',
    @level0name = N'dbo',
    @level1type = N'TABLE',
    @level1name = N'Users';
```
	

## Setting Up for @AI = 1: Direct API Calls

To have SQL Server call the AI API directly, you need database-scoped credentials and (optionally) configuration tables.

**Important Note:** Your database context is about to matter, a LOT. When you use @AI = 1, and we call your AI provider for you, we need access to your API keys. The way Microsoft does database security, those keys have to be set up in whatever database where you'll be running the FRK scripts from. Not where they're *installed*, but your *database context* when you execute them.

For example, if you're not using @AI = 1, you might install the procs in the master database, and then run them from any database. But once you turn on @AI = 1, the context matters:

* If you're in the StackOverflow database, and you run sp_BlitzIndex @AI = 1, then the keys need to be in the StackOverflow Database
* If you're in the DBAtools database, and you run sp_BlitzIndex @DatabaseName = 'StackOverflow', @AI = 1, then the keys need to be in the DBAtools database
* If you're in the DBAtools database, and you run sp_BlitzIndex @GetAllDatabases = 1, @AI = 1, then the keys only need to be in the DBAtools database

So you have to make an executive decision about how you want to run the FRK scripts with @AI = 1. There are two ways you could do this:

* Set up the key, credential, and role in a utility database like DBAtools, and always call the FRK procs from there when you want to call AI providers for advice, or
* Set up the key, credential, and role in every user database, and run the FRK scripts from anywhere without concern.

Either way, follow the next few steps in every database where you want to run @AI = 1.

### Create a Master Key

Each database that stores credentials needs a master key:

```sql
USE YourDatabase;
CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'YourStrongPassword!';
```

### Create Credentials for OpenAI (ChatGPT)

```sql
CREATE DATABASE SCOPED CREDENTIAL [https://api.openai.com/]
WITH IDENTITY = 'HTTPEndpointHeaders',
SECRET = '{"Authorization":"Bearer YOUR_OPENAI_API_KEY_HERE"}';
```

### Create Credentials for Google Gemini

```sql
CREATE DATABASE SCOPED CREDENTIAL [https://generativelanguage.googleapis.com/]
WITH IDENTITY = 'HTTPEndpointHeaders',
SECRET = N'{"x-goog-api-key":"YOUR_GEMINI_API_KEY_HERE"}';
```

### Grant Access to the Credential

Create a role so you can control who can use the AI features:

```sql
CREATE ROLE [DBA_AI];
GRANT REFERENCES ON DATABASE SCOPED CREDENTIAL::[https://api.openai.com/] TO [DBA_AI];
/* Or for Gemini: */
GRANT REFERENCES ON DATABASE SCOPED CREDENTIAL::[https://generativelanguage.googleapis.com/] TO [DBA_AI];
```

**Repeat the above key, credential, and access steps for all databases where you'll run the FRK procs from with the @AI = 1 parameter on.**

### Enable External REST Endpoints

You only have to do this once, and it takes effect at the server level. It's required in SQL Server 2025 or newer, but not necessary in some flavors of Azure SQL DB.

```sql
EXEC sp_configure 'external rest endpoint enabled', 1;
RECONFIGURE WITH OVERRIDE;
```

### Configuration Tables (Optional)

You can create configuration tables to store AI provider settings and prompt templates. This avoids passing parameters every time and lets you switch between providers easily. If you don't create these tables, the procs use built-in defaults (OpenAI gpt-5-nano).

#### AI Providers Table

```sql
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
    N'https://api.openai.com/', 30, 1);

/* OpenAI - highest quality, slowest, most expensive model: */
INSERT INTO dbo.Blitz_AI_Providers (Model_Nickname, AI_Model, AI_URL, AI_Database_Scoped_Credential_Name, Timeout_Seconds, Default_Model)
VALUES (N'ChatGPT Slow', N'gpt-5.4', N'https://api.openai.com/v1/chat/completions',
    N'https://api.openai.com/', 230, 0);

/* Gemini - fast, cheap model: */
INSERT INTO dbo.Blitz_AI_Providers (Model_Nickname, AI_Model, AI_URL, AI_Database_Scoped_Credential_Name, Timeout_Seconds, Default_Model)
VALUES (N'Gemini Fast', N'gemini-3-flash-preview', N'https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent',
    N'https://generativelanguage.googleapis.com/', 30, 0);

/* Gemini - highest quality, slowest, most expensive model: */
INSERT INTO dbo.Blitz_AI_Providers (Model_Nickname, AI_Model, AI_URL, AI_Database_Scoped_Credential_Name, Timeout_Seconds, Default_Model)
VALUES (N'Gemini Slow', N'gemini-3-1-pro-preview', N'https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent',
    N'https://generativelanguage.googleapis.com/', 230, 0);
```


## sp_BlitzCache with @AI = 1

Once credentials are set up, sp_BlitzCache can call the AI API directly and return advice in the result set.

### Quick Start with OpenAI

```sql
EXEC sp_BlitzCache @Top = 1, @AI = 1;
```

That's it - the defaults use OpenAI's `gpt-5-nano` model. The result set includes the AI's query tuning recommendations.

### Using Google Gemini

Gemini requires specifying the model and URL:

```sql
EXEC sp_BlitzCache @Top = 1, @AI = 1,
    @AIModel = N'gemini-2.5-flash',
    @AIURL = N'https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent';
```

### Using Configuration Tables

```sql
/* Use the default model from your config table */
EXEC sp_BlitzCache @Top = 1, @AI = 1,
    @AIConfigTable = 'master.dbo.Blitz_AI_Providers';

/* Use a specific model from your config table */
EXEC sp_BlitzCache @Top = 1, @AI = 1,
    @AIConfigTable = 'master.dbo.Blitz_AI_Providers',
    @AIModel = 'ChatGPT Slow';

/* Use a specific model AND prompt */
EXEC sp_BlitzCache @Top = 1, @AI = 1,
    @AIConfigTable = 'master.dbo.Blitz_AI_Providers',
    @AIModel = 'ChatGPT Slow';
    @AIPromptConfigTable = 'master.dbo.Blitz_AI_Prompts',
    @AIPrompt = 'sp_BlitzCache Deadlock Tuning';
```

### sp_BlitzCache AI Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `@AI` | 0 | 0 = off, 1 = call AI API, 2 = generate prompt only |
| `@AIModel` | `gpt-5-nano` | Model name or nickname. Matches against both `AI_Model` and `Model_Nickname` in the providers table. If it starts with `gemini`, the Gemini URL and payload template are used automatically. |
| `@AIURL` | `https://api.openai.com/v1/chat/completions` | API endpoint URL. Auto-detected for Gemini models. |
| `@AICredential` | Auto-detected from URL | Database-scoped credential name. Defaults to the root of your `@AIURL` with trailing slash. |
| `@AIConfigTable` | NULL | Three-part name of your providers config table (e.g., `master.dbo.Blitz_AI_Providers`). |
| `@AIPromptConfigTable` | NULL | Three-part name of your prompts config table. |
| `@AIPrompt` | NULL | Which prompt nickname to use from the prompts table. |

### sp_BlitzCache AI Result Sets

With `@AI = 1`, the result set includes:

- **AI Advice** - The AI's recommendations as XML text
- **AI Prompt** - The full prompt that was sent (system prompt + query data)
- **AI Payload** - The raw JSON payload sent to the API
- **AI Raw Response** - The full API response JSON (useful for debugging)

With `@AI = 2`, only the **AI Prompt** column is included in the result set.

## sp_BlitzIndex with @AI = 1

Once credentials are set up, sp_BlitzIndex can call the AI API directly and return index recommendations. This only works in single-table mode (when `@TableName` is specified).

To just run it with the default ChatGPT model:

```sql
EXEC sp_BlitzIndex
    @DatabaseName = 'YourDatabase',
    @SchemaName = 'dbo',
    @TableName = 'YourTable',
    @AI = 1;
```

Using the configuration tables works basically the same as the sp_BlitzCache examples above:

```sql
EXEC sp_BlitzIndex
    @DatabaseName = 'YourDatabase',
    @SchemaName = 'dbo',
    @TableName = 'YourTable',
	@AI = 1,
    @AIConfigTable = 'master.dbo.Blitz_AI_Providers',
    @AIModel = 'ChatGPT Slow'
    @AIPromptConfigTable = 'master.dbo.Blitz_AI_Prompts',
    @AIPrompt = 'sp_BlitzIndex OLTP'; /* Or whatever custom prompt you inserted */
```

### sp_BlitzIndex AI Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `@AI` | 0 | 0 = off, 1 = call AI API, 2 = generate prompt only |
| `@AIModel` | `gpt-5-nano` | Model name or nickname. Matches against both `AI_Model` and `Model_Nickname` in the providers table. Gemini models auto-detect URL and payload template. |
| `@AIURL` | `https://api.openai.com/v1/chat/completions` | API endpoint URL. |
| `@AICredential` | Auto-detected from URL | Database-scoped credential name. |
| `@AIConfigTable` | NULL | Three-part name of your providers config table. |
| `@AIPromptConfigTable` | NULL | Three-part name of your prompts config table. |
| `@AIPrompt` | NULL | Which prompt nickname to use from the prompts table. |

### sp_BlitzIndex AI Result Sets

The AI result sets appear immediately after the missing index result set.

With `@AI = 1`:

- **AI Advice** - The AI's index recommendations as XML text
- **AI Prompt** - The full prompt sent to the AI (system prompt + all four data sections)
- **AI Payload** - The raw JSON payload sent to the API
- **AI Raw Response** - The full API response JSON

With `@AI = 2`:

- **AI Prompt** - The full prompt, ready to copy/paste into your AI tool of choice

## Tips

- **Start with `@AI = 2`** to review the prompt before spending API credits. You can paste it into any AI tool to verify the output quality.
- **Database context matters** for `@AI = 1`: you must run the query in the database where your credentials are stored, or the API call will fail.
- **Timeout**: The default timeout is 230 seconds. Larger models may need the full timeout; smaller models like `gpt-5-nano` respond in seconds.
- **Cost**: Each call sends your query/index data to the AI provider and costs API credits. Use `@Top = 1` with sp_BlitzCache to limit costs during testing.
- **Context size**: If the `Context` column in your AI Providers table has a value greater than 0, the payload will be trimmed to that length to avoid exceeding the model's context window. Set `Context` to 0 or NULL for unlimited payload size.
- **Model nicknames**: The `@AIModel` parameter matches against both the `AI_Model` and `Model_Nickname` columns in your providers table. This lets you use friendly names like `'ChatGPT Slow'` instead of model identifiers like `'gpt-5.4'`.
- **Security**: Your query text, index definitions, and table structures are sent to the AI provider's API. Do not use this feature if your data or schema is subject to restrictions on external sharing.
