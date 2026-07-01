ALTER AUTHORIZATION
ON DATABASE::AdventureWorks2008R2
TO AmrDBA;

EXEC sys.sp_cdc_enable_db;

CREATE TABLE dbo.dw_table_config(
schemaName		SYSNAME NOT NULL,
tableName		SYSNAME NOT NULL,
enable_CDC		BIT NOT NULL DEFAULT 1,
status			NVARCHAR(20) NULL,
updated_at		DATETIME2 NULL,
error_message   NVARCHAR(MAX) NULL,

CONSTRAINT PK_dw_table_config
        PRIMARY KEY(schemaName, tableName)
);
GO


CREATE OR ALTER PROC dbo.usp_loadDatabaseTables
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    BEGIN TRY

        INSERT INTO dbo.dw_table_config
        (
            schemaName,
            tableName
        )
        SELECT
            s.name,
            t.name
        FROM sys.tables AS t
        INNER JOIN sys.schemas AS s
            ON s.schema_id = t.schema_id
        WHERE t.is_ms_shipped = 0
          AND NOT EXISTS
          (
              SELECT 1
              FROM dbo.dw_table_config AS c
              WHERE c.schemaName = s.name
                AND c.tableName  = t.name
          );

        DECLARE @RowsInserted INT = @@ROWCOUNT;

        PRINT CONCAT('Inserted ', @RowsInserted, ' table(s).');

    END TRY
    BEGIN CATCH

        THROW;

    END CATCH
END;
GO



CREATE OR ALTER PROC dbo.usp_enable_cdc
AS
BEGIN
	SET NOCOUNT ON;
	DECLARE @schemaName SYSNAME,
			@tableName SYSNAME
			
	
	SELECT schemaName,tableName 
	INTO #tables
	FROM dbo.dw_table_config 
	WHERE enable_CDC = 1;

	DECLARE cur CURSOR FAST_FORWARD FOR
	SELECT schemaName,tableName FROM #tables;
	OPEN cur;

	FETCH NEXT FROM cur 
	INTO @schemaName,@tableName

	WHILE @@FETCH_STATUS = 0
		BEGIN
			--CHECKING

		IF EXISTS(
				SELECT 1 
				FROM 
				sys.tables t
				JOIN sys.schemas s
				ON s.schema_id=t.schema_id
				WHERE t.is_tracked_by_cdc = 1
				AND s.name=@schemaName
				AND t.name=@tableName
				)
			BEGIN
			UPDATE c
				SET c.status = 'ENABLED',
					c.updated_at=SYSDATETIME()
				FROM dbo.dw_table_config c
				WHERE C.schemaName=@schemaName
					AND C.tableName=@tableName;
			END
		ELSE
			BEGIN TRY
				EXEC sys.sp_cdc_enable_table
					@source_schema = @schemaName,
					@source_name   = @tableName,
					@role_name     = NULL;

				UPDATE c
					SET c.status = 'ENABLED',
						c.updated_at=SYSDATETIME()
					FROM dbo.dw_table_config c
					WHERE C.schemaName=@schemaName
						AND C.tableName=@tableName;
			END TRY
		BEGIN CATCH
			UPDATE c
					SET c.status = 'FAILED',
						c.error_message=ERROR_MESSAGE(),
						c.updated_at=SYSDATETIME()
					FROM dbo.dw_table_config c
					WHERE C.schemaName=@schemaName
						AND C.tableName=@tableName;
		END CATCH



		FETCH NEXT FROM cur 
		INTO @schemaName,@tableName
	END
CLOSE cur;
DEALLOCATE cur;
END
GO



