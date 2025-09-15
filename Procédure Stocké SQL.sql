USE goldexotest;
GO

CREATE OR ALTER PROC CreateSQLServerlessView_gold @ViewName NVARCHAR(100)
AS
BEGIN
    DECLARE @statement NVARCHAR(MAX);

    SET @statement = N'
        CREATE OR ALTER VIEW ' + QUOTENAME(@ViewName) + N' AS
        SELECT *
        FROM OPENROWSET(
            BULK ''https://dtlakeexotest.dfs.core.windows.net/gold/SalesLT/' + @ViewName + '/'',
            FORMAT = ''DELTA''
        ) AS [result];';

    EXEC (@statement);
END;
GO
