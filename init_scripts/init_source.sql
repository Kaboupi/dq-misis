-- INIT SCRIPT
CREATE SCHEMA IF NOT EXISTS misis_project;

DROP TABLE IF EXISTS misis_project.sales_data;
CREATE TABLE misis_project.sales_data (
    "Date" DATE NOT NULL
	, "Day" INT NOT NULL
	, "Month" VARCHAR(20) NOT NULL
	, "Year" INT NOT NULL
	, "Customer_Age" INT NOT NULL
	, "Age_Group" VARCHAR(20) NOT NULL
	, "Customer_Gender" CHAR(1) NOT NULL
	, "Country" VARCHAR(50) NOT NULL
	, "State" VARCHAR(50) NOT NULL
	, "Product_Category" VARCHAR(50) NOT NULL
	, "Sub_Category" VARCHAR(50) NOT NULL
	, "Product" VARCHAR(100) NOT NULL
	, "Order_Quantity" INT NOT NULL
	, "Unit_Cost" NUMERIC(10, 2) NOT NULL
	, "Unit_Price" NUMERIC(10, 2) NOT NULL
	, "Profit" NUMERIC(10, 2) NOT NULL
	, "Cost" NUMERIC(10, 2) NOT NULL
	, "Revenue" NUMERIC(10, 2) NOT NULL
);

COPY misis_project.sales_data(
	"Date"
	, "Day"
	, "Month"
	, "Year"
	, "Customer_Age"
	, "Age_Group"
	, "Customer_Gender"
	, "Country"
	, "State"
	, "Product_Category"
	, "Sub_Category"
	, "Product"
	, "Order_Quantity"
	, "Unit_Cost"
	, "Unit_Price"
	, "Profit"
	, "Cost"
	, "Revenue"
)
FROM '/init_data/sales_data.csv'
DELIMITER ',' CSV HEADER;