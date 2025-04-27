-- =============================================================
-- Create Database and Schemas
-- =============================================================
-- Script Purpose:
--     This script creates a new database named 'datawarehouse.' 
--     Additionally, it sets up three schemas 
--     within the database: 'bronze', 'silver', and 'gold'.
--
-- =============================================================

-- Create the 'datawarehouse' database
CREATE DATABASE datawarehouse;

-- Create Schemas
CREATE SCHEMA bronze;
CREATE SCHEMA silver;
CREATE SCHEMA gold;
