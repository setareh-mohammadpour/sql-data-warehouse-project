use master;
-- Drop and recreate the 'DataWarehouse' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouse;
END;
GO
create database Datawarehouse;
use Datawarehouse;
create schema bronze;
go
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
go

