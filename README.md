# data-migration-agent

Automated Migration and Reconciliation for SQL Server to Snowflake

This project provides a robust, Python-based agent to handle the end-to-end migration of database tables from SQL Server (including AWS RDS) to Snowflake. Its primary goal is to deliver a verified, auditable, and reliable migration with minimal manual effort.

## How the Agent Works

The agent follows a deterministic, seven-step workflow:

1. **Snowflake Table Check**: Determines if the target table exists, preventing accidental overwrites or redundant creation steps.

2. **SQL Server DDL Extraction**: Connects securely to the source database to fetch the current table schema.

3. **DDL Conversion & Correction**: Automatically converts the SQL Server schema into a Snowflake-compatible DDL, correcting data types and syntax errors.

4. **Table Creation**: Safely executes the converted DDL in Snowflake.

5. **Batch Data Migration**: Transfers data using chunked reads to manage memory, converting all column names to UPPERCASE during the process.

6. **Error Handling & Documentation Lookup**: When execution or migration errors are detected, the agent attempts to resolve issues by automatically searching relevant Snowflake documentation.

7. **Data Reconciliation & Reporting**: The critical validation step. It compares the source and target on key metrics to guarantee data integrity, then generates a structured report summarizing any discrepancies:
   - Row Counts
   - Column Counts
   - Column-level Null Counts
   - Unique Counts

## Technology Stack

The agent is built using reliable, industry-standard tools:

- **Python**
- **pytds**: Used for sandbox-safe and secure connectivity to SQL Server
- **pandas**: Leveraged for high-performance batch data manipulation and reconciliation logic
- **Snowflake External Access Integration**: Ensures secure, managed network access for connections

## Use Case

This solution is perfect for engineering teams performing repeated, large-scale data migrations and who require full validation with audit-ready reconciliation reports to confirm the accuracy of migrated data.
