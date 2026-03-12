# Using AI with the First Responder Kit

sp_BlitzCache and sp_BlitzIndex can build AI prompts with your query and index data, and optionally call an AI provider (OpenAI or Google Gemini) to return recommendations directly in the result set.

- **`@AI = 2`** - Builds the AI prompt and returns it in the result set so you can copy/paste it into ChatGPT, Gemini, or another AI tool. Works on all supported SQL Server versions, no setup required.
- **`@AI = 1`** - Does everything `@AI = 2` does, plus calls the AI API directly from SQL Server and returns advice in the result set. Requires SQL Server 2025 or Azure SQL DB (uses `sp_invoke_external_rest_endpoint`).

## Getting Started: Generate Prompts with @AI = 2

The fastest way to try the AI features is `@AI = 2`. No credentials, no config tables, no API keys - just run the proc and copy the prompt into your favorite AI tool.

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

The **AI Prompt** column contains a prompt with four data sections about your table:

1. **Existing Indexes** - Index names, types, key columns, include columns, filters, uniqueness, primary key status, and usage statistics (seeks, scans, lookups, writes, row counts).
2. **Missing Index Suggestions** - SQL Server's missing index DMV data including equality/inequality/include columns, benefit numbers, and suggested CREATE INDEX statements.
3. **Column Data Types** - All columns in the table with their data types, nullability, and identity status.
4. **Foreign Keys** - Foreign key names, parent/referenced columns, and whether they are disabled or not trusted.

The AI result sets appear immediately after the missing index result set in sp_BlitzIndex's output.

## Setting Up for @AI = 1: Direct API Calls

To have SQL Server call the AI API directly, you need database-scoped credentials and (optionally) configuration tables.

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

### Configuration Tables (Optional)

You can create configuration tables to store AI provider settings and prompt templates. This avoids passing parameters every time and lets you switch between providers easily. If you don't create these tables, the procs use built-in defaults (OpenAI gpt-5-nano).

#### AI Providers Table

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

#### AI Prompts Table

```sql
CREATE TABLE dbo.Blitz_AI_Prompts
(Id INT PRIMARY KEY CLUSTERED,
 PromptNickname NVARCHAR(200),
 AI_System_Prompt NVARCHAR(4000),
 Payload_Template NVARCHAR(4000),
 DefaultPrompt BIT DEFAULT 0);
```

You can store custom system prompts and payload templates here. The `PromptNickname` column lets you select a prompt by name with the `@AIPrompt` parameter.

### Database Constitution (Optional)

Both sp_BlitzCache and sp_BlitzIndex support a database-level "constitution" - an extended property that provides additional context to the AI about your database's specific rules and constraints.

```sql
EXEC sp_addextendedproperty
    @name = N'CONSTITUTION.md',
    @value = N'This is an OLTP database supporting a web application.
Tables in the dbo schema are the primary transactional tables.
We prefer filtered indexes over full indexes where possible.
The Users table is rarely updated but frequently queried by Location.';
```

When present, the constitution text is prepended to the AI prompt, giving the AI additional context about your environment.

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

## sp_BlitzIndex with @AI = 1

Once credentials are set up, sp_BlitzIndex can call the AI API directly and return index recommendations. This only works in single-table mode (when `@TableName` is specified).

### Quick Start with OpenAI

```sql
EXEC sp_BlitzIndex
    @DatabaseName = 'YourDatabase',
    @SchemaName = 'dbo',
    @TableName = 'YourTable',
    @AI = 1;
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
- **Security**: Your query text, index definitions, and table structures are sent to the AI provider's API. Do not use this feature if your data or schema is subject to restrictions on external sharing.
