-- ############### Tools
---- Snowflake docs knowledge extension
CREATE OR REPLACE PROCEDURE search_snowflake_documentation(
    query STRING
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python','snowflake-ml-python')
HANDLER = 'search_snowflake_documentation'
AS
$$
import json
from snowflake.core import Root
def search_snowflake_documentation(session, query: str) -> str:
    """Searches Snowflake documentation for information related to the query.

    Args:
        query (str): The query to search in Snowflake documentation.

    Returns:
        str: The search results from Snowflake documentation.
    """
    root = Root(session)

    # fetch service
    my_service = (root
        .databases["SNOWFLAKE_DOCUMENTATION"]
        .schemas["SHARED"]
        .cortex_search_services["CKE_SNOWFLAKE_DOCS_SERVICE"]
    )

    # query service
    resp = my_service.search(
        query=query,
        columns=["CHUNK"],
        limit=3,
    )

    json_resp = json.loads(resp.to_json())

    context =  ""
    for result in json_resp["results"]:
        context += f"Chunk: {result['CHUNK']}\n"

    return context
$$;

call search_snowflake_documentation('snowflake intelligence');

------ Execute query tool
CREATE OR REPLACE PROCEDURE execute_snowflake_ddl(
    query STRING
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'execute_snowflake_ddl'
AS
$$
def execute_snowflake_ddl(session, query:str) -> str:
    """Executes a given query on snowflake and returns the status (either success or failure) and error message if any.

    Args:
        query (str): The snowflake SQL query to execute.

    Returns:
        str: A message indicating the status ("Success" or "Failure") and an error message if any.
    """
    if not query.endswith(";"):
        query += ";"
    try:
        session.sql(query).collect()
        return "Success"
    except Exception as e:
        return f"Failure due to error : {str(e)}"
$$;

call execute_snowflake_ddl('select current_date()');


---- DDL conversion tool
CREATE OR REPLACE PROCEDURE ddl_conversion(
    sqlserver_ddl STRING,
    previously_generated_snowflake_ddl STRING DEFAULT '',
    documentation_context STRING DEFAULT 'No additional documentation context provided.',
    error_message STRING DEFAULT 'No error message provided.'
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python','snowflake-ml-python')
HANDLER = 'ddl_conversion'
AS
$$
def ddl_conversion(session, sqlserver_ddl:str, previously_generated_snowflake_ddl:str, documentation_context:str, error_message:str) -> str:
    """
    Converts SQL Server DDL to Snowflake-compatible DDL or corrects existing Snowflake DDL.
    
    This tool supports two primary use cases:
    1. Initial conversion: Transforms SQL Server DDL syntax to Snowflake-compatible DDL
    2. Error correction: Takes faulty Snowflake DDL along with error messages and documentation 
       context to generate corrected DDL
    
    Args:
        sqlserver_ddl (str): The original SQL Server DDL statement to convert. This is the 
            primary input for initial conversions.
        previously_generated_snowflake_ddl (str): Previously generated Snowflake DDL 
            that encountered errors or needs improvement. Used in correction scenarios.
        documentation_context (str): Relevant Snowflake documentation, syntax guides, 
            or contextual information to inform the conversion or correction process.
        error_message (str): Error message returned by Snowflake when executing the 
            DDL. Used to identify specific issues and generate appropriate fixes.

    Returns:
        str: Snowflake-compatible DDL statement, either converted from SQL Server or 
             corrected based on the provided error context.
             
    Examples:
        # Initial conversion
        result = await ddl_conversion("CREATE TABLE dbo.Users (ID int IDENTITY(1,1) PRIMARY KEY)")
        
        # Error correction
        result = await ddl_conversion(
            sqlserver_ddl="...",
            previously_generated_snowflake_ddl="CREATE TABLE Users (ID NUMBER AUTOINCREMENT)",
            error_message="SQL compilation error: syntax error at 'AUTOINCREMENT'"
        )
    """
            
    if len(previously_generated_snowflake_ddl.strip()) == 0:
        previously_generated_snowflake_ddl = ""
    if len(documentation_context.strip()) == 0:
        documentation_context = "No additional documentation context provided."
    if len(error_message.strip()) == 0:
        error_message = "No error message provided." 
        
    prompt = f"""
        You are a SQL DDL conversion and correction assistant. Your primary responsibilities are:

        1. **Initial Conversion**: Convert SQL Server DDL statements into Snowflake-compatible DDL
        2. **Error Correction**: Fix faulty Snowflake DDL based on error messages and documentation context

        ## Data Type Conversion Mapping:
        int ⟶ INTEGER
        bigint ⟶ BIGINT
        smallint ⟶ SMALLINT
        tinyint ⟶ NUMBER(3, 0)
        bit ⟶ BOOLEAN
        decimal(p,s) ⟶ NUMBER(p,s)
        numeric(p,s) ⟶ NUMBER(p,s)
        money ⟶ NUMBER(19,4)
        smallmoney ⟶ NUMBER(10,4)
        float ⟶ FLOAT
        real ⟶ FLOAT
        datetime, datetime2, smalldatetime ⟶ TIMESTAMP_NTZ
        date ⟶ DATE
        time ⟶ TIME
        char(n) ⟶ VARCHAR
        varchar(n) ⟶ VARCHAR
        varchar(max) ⟶ VARCHAR
        nchar(n) ⟶ CHAR(n)
        nvarchar(n) ⟶ VARCHAR
        nvarchar(max) ⟶ VARCHAR
        text, ntext ⟶ STRING
        binary(n), varbinary(n) ⟶ BINARY
        varbinary(max), image ⟶ BINARY
        uniqueidentifier ⟶ STRING

        ## Constraint and Syntax Conversion:
        - PRIMARY KEY: Convert to PRIMARY KEY clause (column or table level)
        - FOREIGN KEY: Use standard foreign key syntax (note: constraints are not enforced in Snowflake)
        - DEFAULT value: Apply DEFAULT clauses as in SQL Server
        - IDENTITY(1,1): Replace with AUTOINCREMENT for column definition
        - CHECK: Use CHECK constraints as in SQL Server
        - Indexes: Omit INDEX definitions (Snowflake doesn't support explicit index creation)
        - Collation: Ignore any COLLATE statements
        - Comments: Convert to COMMENT clauses if present
        - Functions: Replace GETDATE() with CURRENT_TIMESTAMP() for defaults

        ## Processing Logic:

        **For Initial Conversion (when only SQL Server DDL is provided):**
        - Convert all DDL components following the above mappings
        - Remove or adjust elements that don't map to Snowflake
        - Mark any unconvertible features in comments

        **For Error Correction (when error message and/or previously generated DDL is provided):**
        - Analyze the error message to identify specific issues
        - Use documentation context to understand proper Snowflake syntax
        - Correct the previously generated Snowflake DDL based on error details
        - Apply fixes while maintaining the original intent of the conversion

        ## Output Requirements:
        - Return valid Snowflake DDL only
        - Provide response as separate key-value pairs: "sql" and "explanation"
        - Ensure SQL statement ends with semicolon (;)
        - Include brief explanation of changes made (especially for error corrections)
        - No extraneous comments in the SQL output

        ## Error Resolution Priority:
        When error message and documentation context are provided, prioritize:
        1. Syntax errors based on error message details
        2. Best practices from documentation context
        3. Snowflake-specific requirements and limitations
        4. Maintaining functional equivalence to original SQL Server DDL

        ---

        **Input Parameters:**
        SQL Server DDL: {sqlserver_ddl}
        Previously Generated Snowflake DDL: {previously_generated_snowflake_ddl}
        Documentation Context: {documentation_context}
        Error Message: {error_message}

    """
    import snowflake.cortex as cortex
    completion_response = cortex.complete(
            model="mistral-large2",
            prompt=prompt,
            session=session,
        )
    return completion_response
$$;

call ddl_conversion('CREATE TABLE Employees (EmployeeID int IDENTITY(1,1) PRIMARY KEY,FirstName varchar(50) NOT NULL,LastName varchar(50) NOT NULL,DepartmentID int,Salary decimal(10,2),HireDate date NOT NULL, IsActive bit DEFAULT 1,)');


-- snowflake_table_check
CREATE OR REPLACE PROCEDURE snowflake_table_check(
    table_name STRING
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'snowflake_table_check'
AS
$$
def snowflake_table_check(session, table_name:str) -> str:
    """Checks if a table exists in Snowflake.

    Args:
        table_name (str): The name of the table to check.

    Returns:
        str: A message indicating whether the table exists or not.
    """
    query = f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'LLM_DEV' AND LOWER(TABLE_NAME) = '{table_name.lower()}';"
    result = session.sql(query).collect()
    if result[0][0] > 0:
        return "Table exists."
    else:
        return "Table does not exist."
$$;

call snowflake_table_check('employees');

---- sqlserver tools
CREATE OR REPLACE NETWORK RULE sqlserver_rds_rule
  MODE = EGRESS
  TYPE = HOST_PORT
  VALUE_LIST = ('database-1.cvaqfnvzqtkv.ap-south-1.rds.amazonaws.com:1433','database-1.cvaqfnvzqtkv.ap-south-1.rds.amazonaws.com','rds.amazonaws.com','rds.ap-south-1.api.aws','rds.ap-south-1.amazonaws.com');

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION sqlserver_rds_integration
  ALLOWED_NETWORK_RULES = (sqlserver_rds_rule)
  ENABLED = TRUE;

CREATE OR REPLACE PROCEDURE get_sqlserver_ddl(
    table_name STRING
)
RETURNS OBJECT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
external_access_integrations = (sqlserver_rds_integration)
ARTIFACT_REPOSITORY = snowflake.snowpark.pypi_shared_repository
PACKAGES = ('snowflake-snowpark-python','python-tds')
HANDLER = 'get_sqlserver_ddl'
AS
$$
import pytds
def get_sqlserver_ddl(session, table_name:str) -> dict:
    """Fetches the DDL for a specific table from SQL Server.
    
    Args:
        table_name (str): The name of the table to fetch DDL for.

    Returns:
        dict: A dictionary containing the SQL Server DDL statement for the specified table.
    """
    server = "database-1.cvaqfnvzqtkv.ap-south-1.rds.amazonaws.com"
    username = ""
    password = ""
    port = 1433
    with pytds.connect(
        server=server,
        database="demodb",
        user=username,
        password=password,
        port=port,
        as_dict=True
    ) as conn:
        with conn.cursor() as cur:
            cur.execute("EXEC sp_generate_table_ddl @TableName=%s, @SchemaName=%s", (table_name, 'dbo'))
            rows = cur.fetchall()
            return {"sqlserver_ddl": rows[0]['TableDDL']}
$$;

call get_sqlserver_ddl('employees');


CREATE OR REPLACE PROCEDURE migrate_data_sqlserver_to_snowflake(
    table_name STRING
)
RETURNS OBJECT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
external_access_integrations = (sqlserver_rds_integration)
ARTIFACT_REPOSITORY = snowflake.snowpark.pypi_shared_repository
PACKAGES = ('python-tds','pandas','snowflake-snowpark-python[pandas]')
HANDLER = 'migrate_data_sqlserver_to_snowflake'
AS
$$
import pytds
import pandas as pd
def migrate_data_sqlserver_to_snowflake(session, table_name:str) -> dict:
    """
    Function to Migrate data from SQL Server table -> Snowflake table

    Args:
        table_name (str): The name of the table to migrate.

    Returns:
        dict: A dictionary containing the status of the migration process.
    """
    
    server = "database-1.cvaqfnvzqtkv.ap-south-1.rds.amazonaws.com"
    username = ""
    password = ""
    port = 1433
    with pytds.connect(
        server=server,
        database="demodb",
        user=username,
        password=password,
        port=port,
    ) as conn:
        query = f"SELECT * FROM dbo.{table_name}"
        df_iter = pd.read_sql(query, conn)  # pytds works with pandas read_sql

    # Step 3: Upload each batch to Snowflake
    # After reading into df
    df_iter.columns = [col.upper() for col in df_iter.columns]

    cols = ', '.join(df_iter.columns)
    values_list = []
    
    for _, row in df_iter.iterrows():
        vals = []
        for val in row:
            if val is None:
                vals.append("NULL")
            elif isinstance(val, (str, object)):
                # Wrap everything that is not numeric in quotes
                safe_val = str(val).replace("'", "''")
                vals.append(f"'{safe_val}'")
            else:
                vals.append(str(val))
        values_list.append(f"({', '.join(vals)})")
    
    insert_sql = f"INSERT INTO {table_name} ({cols}) VALUES " + ', '.join(values_list)
    session.sql(insert_sql).collect()
    
    # Upload to Snowflake temp stage
    #df_iter.to_csv('/tmp/temp.csv', index=False)
    #session.file.put('/tmp/temp.csv', "@int_stage/DE_AGENT", auto_compress=True)
    
    # Copy into target table
    #session.sql(f"COPY INTO {table_name} FROM @int_stage/DE_AGENT/temp.csv FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY='\"')").collect()

    #snow_df_iter = session.create_dataframe(df_iter)
    #snow_df_iter.write.mode("overwrite").save_as_table(table_name.upper())
    #session.write_pandas(df_iter, table_name=table_name.upper(), schema="LLM_DEV", auto_create_table=False, overwrite=False, use_logical_type=True).collect()

    return {"status": f"successfully inserted data into Snowflake table : {table_name}"}
$$;

call migrate_data_sqlserver_to_snowflake('employees');

list @int_stage;

CREATE  OR  REPLACE    TABLE  EMPLOYEES("EMPLOYEEID" BIGINT, "FIRSTNAME" STRING(16777216), "LASTNAME" STRING(16777216), "EMAIL" STRING(16777216), "HIREDATE" DATE, "SALARY" DOUBLE);

truncate table employees;

select * from employees;

CREATE OR REPLACE PROCEDURE reconcile_table_data(
    sqlserver_table STRING,
    snowflake_table STRING
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
external_access_integrations = (sqlserver_rds_integration)
ARTIFACT_REPOSITORY = snowflake.snowpark.pypi_shared_repository
PACKAGES = ('snowflake-snowpark-python[pandas]','python-tds','pandas')
HANDLER = 'reconcile_table_data'
AS
$$
import pytds
import pandas as pd
def reconcile_table_data(session, sqlserver_table, snowflake_table) -> dict:
    """
    Reconcile data between SQL Server and Snowflake by loading into pandas DataFrames.
    
    Args:
        sqlserver_table (str): table name in SQL Server
        snowflake_table (str): table name in Snowflake
        
    Returns:
        dict summary of reconciliation
    """
    
    # Step 1: Connect to SQL Server
    server = "database-1.cvaqfnvzqtkv.ap-south-1.rds.amazonaws.com"
    username = ""
    password = ""
    port = 1433
    with pytds.connect(
        server=server,
        database="demodb",
        user=username,
        password=password,
        port=port
    ) as conn:
        query = f"SELECT * FROM dbo.{sqlserver_table}"
        df_sql = pd.read_sql(query, conn)  # pytds works with pandas read_sql
    df_sf  = session.table(snowflake_table).to_pandas()
    
    # standardize column names
    df_sql.columns = df_sql.columns.str.upper()
    df_sf.columns  = df_sf.columns.str.upper()
    
    # restrict to chosen columns
    common_cols = list(set(df_sql.columns) & set(df_sf.columns))
    df_sql = df_sql[common_cols]
    df_sf  = df_sf[common_cols]
    columns = common_cols
    
    # --- Build report ---
    report = {
        "row_count": {"sqlserver": len(df_sql), "snowflake": len(df_sf)},
        "row_count_match": len(df_sql) == len(df_sf),
        "columns": {}
    }
    
    for col in columns:
        col_report = {}
        
        if col not in df_sql.columns:
            col_report["exists_in_sqlserver"] = False
            report["columns"][col] = col_report
            continue
        if col not in df_sf.columns:
            col_report["exists_in_snowflake"] = False
            report["columns"][col] = col_report
            continue
        
        col_report["exists_in_sqlserver"] = True
        col_report["exists_in_snowflake"] = True
        
        # null counts
        null_sql = df_sql[col].isna().sum()
        null_sf  = df_sf[col].isna().sum()
        
        # unique counts
        uniq_sql = df_sql[col].nunique(dropna=True)
        uniq_sf  = df_sf[col].nunique(dropna=True)
        
        col_report.update({
            "null_count": {"sqlserver": int(null_sql), "snowflake": int(null_sf)},
            "unique_count": {"sqlserver": int(uniq_sql), "snowflake": int(uniq_sf)},
            "null_count_match": null_sql == null_sf,
            "unique_count_match": uniq_sql == uniq_sf
        })
        
        report["columns"][col] = col_report
    
    return report
$$;

call reconcile_table_data('employees','EMPLOYEES');
