# Using AI with the First Responder Kit

sp_BlitzCache and sp_BlitzIndex can send your query and index data to an AI provider (OpenAI or Google Gemini) and return AI-generated recommendations. There are two modes:

- **`@AI = 1`** - Calls the AI API directly from SQL Server and returns advice in the result set. Requires SQL Server 2025 or Azure SQL DB (uses `sp_invoke_external_rest_endpoint`).
- **`@AI = 2`** - Builds the AI prompt but does not call the API. Returns the prompt in the result set so you can copy/paste it into ChatGPT, Gemini, or another AI tool. Works on all supported SQL Server versions.

## Setting Up Database-Scoped Credentials

To use `@AI = 1`, you need database-scoped credentials in a user database (not master). These store your API key so SQL Server can authenticate with the AI provider.

### Enable External REST Endpoints (SQL Server 2025 only)

```sql
/* Not needed on Azure SQL DB */
EXEC sp_configure 'external rest endpoint enabled', 1;
RECONFIGURE WITH OVERRIDE;
```

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

## Setting Up Configuration Tables (Optional)

You can create configuration tables to store AI provider settings and prompt templates. This avoids passing parameters every time and lets you switch between providers easily.

### AI Providers Table

```sql
CREATE TABLE dbo.Blitz_AI_Providers
(Id INT PRIMARY KEY CLUSTERED,
 AI_Model NVARCHAR(100),
 AI_URL NVARCHAR(500),
 AI_Database_Scoped_Credential_Name NVARCHAR(500),
 AI_Parameters NVARCHAR(4000),
 Timeout_Seconds TINYINT,
 Context INT,
 DefaultModel BIT DEFAULT 0);

/* OpenAI example: */
INSERT INTO dbo.Blitz_AI_Providers (Id, AI_Model, AI_URL, AI_Database_Scoped_Credential_Name, Timeout_Seconds, DefaultModel)
VALUES (1, N'gpt-5-nano', N'https://api.openai.com/v1/chat/completions',
    N'https://api.openai.com/', 230, 1);

/* Gemini example: */
INSERT INTO dbo.Blitz_AI_Providers (Id, AI_Model, AI_URL, AI_Database_Scoped_Credential_Name, Timeout_Seconds, DefaultModel)
VALUES (2, N'gemini-2.5-flash', N'https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent',
    N'https://generativelanguage.googleapis.com/', 230, 0);
```

### AI Prompts Table

```sql
CREATE TABLE dbo.Blitz_AI_Prompts
(Id INT PRIMARY KEY CLUSTERED,
 PromptNickname NVARCHAR(200),
 AI_System_Prompt NVARCHAR(4000),
 Payload_Template NVARCHAR(4000),
 DefaultPrompt BIT DEFAULT 0);
```

You can store custom system prompts and payload templates here. If you don't create these tables, the procs use built-in defaults.

## Using sp_BlitzCache with @AI

sp_BlitzCache analyzes your plan cache and can send query details (query text, execution plan, and performance metrics) to an AI provider for tuning advice.

### Quick Start with OpenAI

```sql
/* Direct AI call - requires credentials set up above */
EXEC sp_BlitzCache @Top = 1, @AI = 1;

/* Just generate the prompt to copy/paste */
EXEC sp_BlitzCache @Top = 1, @AI = 2;
```

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
    @AIModel = 'gemini-2.5-flash';

/* Use a custom prompt from your prompts table */
EXEC sp_BlitzCache @Top = 1, @AI = 1,
    @AIConfigTable = 'master.dbo.Blitz_AI_Providers',
    @AIPromptConfigTable = 'master.dbo.Blitz_AI_Prompts',
    @AIPrompt = 'index_focused';
```

### sp_BlitzCache AI Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `@AI` | 0 | 0 = off, 1 = call AI API, 2 = generate prompt only |
| `@AIModel` | `gpt-5-nano` | Model name. If it starts with `gemini`, the Gemini URL and payload template are used automatically. |
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

## Using sp_BlitzIndex with @AI

sp_BlitzIndex analyzes indexes on a specific table and can send index data to an AI provider for recommendations about which indexes to add, remove, or modify.

**Important:** The AI feature in sp_BlitzIndex only works in single-table mode (when `@TableName` is specified).

### Quick Start with OpenAI

```sql
/* Direct AI call */
EXEC sp_BlitzIndex
    @DatabaseName = 'YourDatabase',
    @SchemaName = 'dbo',
    @TableName = 'YourTable',
    @AI = 1;

/* Just generate the prompt to copy/paste */
EXEC sp_BlitzIndex
    @DatabaseName = 'YourDatabase',
    @SchemaName = 'dbo',
    @TableName = 'YourTable',
    @AI = 2;
```

### Using Google Gemini

```sql
EXEC sp_BlitzIndex
    @DatabaseName = 'YourDatabase',
    @SchemaName = 'dbo',
    @TableName = 'YourTable',
    @AI = 1,
    @AIModel = N'gemini-2.5-flash',
    @AIURL = N'https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent';
```

### Using Configuration Tables

```sql
EXEC sp_BlitzIndex
    @DatabaseName = 'YourDatabase',
    @SchemaName = 'dbo',
    @TableName = 'YourTable',
    @AI = 1,
    @AIConfigTable = 'master.dbo.Blitz_AI_Providers';
```

### What Data Gets Sent to the AI

sp_BlitzIndex sends four data sets for the specified table:

1. **Existing Indexes** - Index names, types, key columns, include columns, filters, uniqueness, primary key status, and usage statistics (seeks, scans, lookups, writes, row counts).
2. **Missing Index Suggestions** - SQL Server's missing index DMV data including equality/inequality/include columns, benefit numbers, and suggested CREATE INDEX statements.
3. **Column Data Types** - All columns in the table with their data types, nullability, and identity status.
4. **Foreign Keys** - Foreign key names, parent/referenced columns, and whether they are disabled or not trusted.

### sp_BlitzIndex AI Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `@AI` | 0 | 0 = off, 1 = call AI API, 2 = generate prompt only |
| `@AIModel` | `gpt-5-nano` | Model name. Gemini models auto-detect URL and payload template. |
| `@AIURL` | `https://api.openai.com/v1/chat/completions` | API endpoint URL. |
| `@AICredential` | Auto-detected from URL | Database-scoped credential name. |
| `@AIConfigTable` | NULL | Three-part name of your providers config table. |
| `@AIPromptConfigTable` | NULL | Three-part name of your prompts config table. |
| `@AIPrompt` | NULL | Which prompt nickname to use from the prompts table. |

### sp_BlitzIndex AI Result Sets

The AI result sets appear immediately after the missing index result set.

With `@AI = 1`:

- **AI Prompt** - The full prompt sent to the AI (system prompt + all four data sections)
- **AI Advice** - The AI's index recommendations as XML text
- **AI Payload** - The raw JSON payload sent to the API
- **AI Raw Response** - The full API response JSON

With `@AI = 2`:

- **AI Prompt** - The full prompt, ready to copy/paste into your AI tool of choice

## Database Constitution

Both sp_BlitzCache and sp_BlitzIndex support a database-level "constitution" - an extended property that provides additional context to the AI about your database's specific rules and constraints.

To set one up:

```sql
EXEC sp_addextendedproperty
    @name = N'CONSTITUTION.md',
    @value = N'This is an OLTP database supporting a web application.
Tables in the dbo schema are the primary transactional tables.
We prefer filtered indexes over full indexes where possible.
The Users table is rarely updated but frequently queried by Location.';
```

When present, the constitution text is prepended to the AI prompt, giving the AI additional context about your environment.

## Tips

- **Start with `@AI = 2`** to review the prompt before spending API credits. You can paste it into any AI tool to verify the output quality.
- **Database context matters** for `@AI = 1`: you must run the query in the database where your credentials are stored, or the API call will fail.
- **Timeout**: The default timeout is 230 seconds. Larger models may need the full timeout; smaller models like `gpt-5-nano` respond in seconds.
- **Cost**: Each call sends your query/index data to the AI provider and costs API credits. Use `@Top = 1` with sp_BlitzCache to limit costs during testing.
- **Security**: Your query text, index definitions, and table structures are sent to the AI provider's API. Do not use this feature if your data or schema is subject to restrictions on external sharing.
