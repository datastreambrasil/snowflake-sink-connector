CREATE DATABASE test;
GO

USE test;
GO

CREATE TABLE dbo.events (
    ID int IDENTITY(1,1) primary key,
    TYPE varchar(50) not null
);
GO

EXEC sys.sp_cdc_enable_db;
GO

EXEC sys.sp_cdc_enable_table
@source_schema = N'dbo',
@source_name   = N'events',
@role_name     = N'cdc_role',
@filegroup_name = NULL,
@supports_net_changes = 0;
GO