# Import Necessary Libraries
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id
import os, glob
from dotenv import load_dotenv
import psycopg2

# Set Java Environment
#os.environ['JAVA_HOME'] = "/mnt/c/java8"

#Initialize Spark Session
spark = SparkSession.builder \
        .appName("NYC Payroll ETL") \
        .config("spark.jars", "postgresql-42.7.4.jar") \
        .getOrCreate()

#Load Master Data
employee_df = spark.read.csv('dataset/raw/EmpMaster.csv', header=True, inferSchema=True)
agency_df = spark.read.csv('dataset/raw/AgencyMaster.csv', header=True, inferSchema=True)
jobtitle_df = spark.read.csv('dataset/raw/TitleMaster.csv', header=True, inferSchema=True)

#Dynamically Loading and Merging all Payroll data

# Define directory containing payroll CSV files
payroll_dir = "dataset/payroll_data" 

# Find all payroll CSV files dynamically
payroll_files = glob.glob(os.path.join(payroll_dir, "nycpayroll_*.csv"))

# Check if any files were found
if not payroll_files:
    raise ValueError("No payroll files found in the directory!")

# Load and merge payroll data
def load_payroll_data(files):
    dataframes = [spark.read.csv(file, header=True, inferSchema=True) for file in files]
    merged_df = dataframes[0]
    for df in dataframes[1:]:
        merged_df = merged_df.union(df)
    return merged_df.dropDuplicates(["EmployeeID", "FiscalYear"])

# Load and process payroll data
payroll_df = load_payroll_data(payroll_files)

# Show the schema and first few rows for validation
payroll_df.printSchema()
payroll_df.show()

# Convert AgencyStartDate to Date datatype
payroll_df = payroll_df.withColumn("AgencyStartDate", pyspark.sql.functions.to_date(payroll_df["AgencyStartDate"], "M/d/yyyy"))

# Check for Null values
# for column in payroll_df.columns:
#     print(column, 'Nulls: ', payroll_df.filter(payroll_df[column].isNull()).count())

# Fill null values with defaults
for col_name, dtype in payroll_df.dtypes:
    if dtype == "string":
        payroll_df = payroll_df.fillna({col_name: "Unknown"})
    elif dtype in ["double", "float"]:
        payroll_df = payroll_df.fillna({col_name: 0.0})
    elif dtype in ["int", "bigint"]:
        payroll_df = payroll_df.fillna({col_name: 0})

# Merge all data together
merged_data = payroll_df \
    .join(employee_df, ["EmployeeID", "LastName", "FirstName"], "left") \
    .join(agency_df, ["AgencyID", "AgencyName"], "left") \
    .join(jobtitle_df, ["TitleCode", "TitleDescription"], "left")

# Create Employee Dimension Table
employee_dim = merged_data.select('EmployeeID', 'LastName', 'FirstName', 'WorkLocationBorough', 'LeaveStatusasofJune30').dropDuplicates(['EmployeeID'])

#Create Agency Dimension Table
agency_dim = merged_data.select('AgencyID', 'AgencyName').dropDuplicates()

#Create Job_Title Dimension Table
jobtitle_dim = merged_data.select('TitleCode', 'TitleDescription').dropDuplicates() 

#Create Time Dimension Table
time_dim = merged_data.select('FiscalYear').dropDuplicates() \
        .withColumn('TimeID', monotonically_increasing_id()) \
        .select('TimeID', 'FiscalYear')

# # Create Payroll_Fact_table
payroll_fact_tbl = merged_data.join(employee_dim.alias('e'), ['LastName', 'FirstName', 'LeaveStatusasofJune30', 'WorkLocationBorough'], 'inner') \
                .join(agency_dim.alias('a'), ['AgencyName'], 'inner') \
                .join(jobtitle_dim.alias('t'), ['TitleDescription'], 'inner') \
                .join(time_dim, ['FiscalYear'], 'inner') \
                .withColumn('PayrollID', monotonically_increasing_id()) \
                .select('PayrollID','e.EmployeeID', 'a.AgencyID', 't.TitleCode', 'TimeID', 'PayrollNumber', 'BaseSalary', 'PayBasis', 'AgencyStartDate', 'RegularHours', 'RegularGrossPaid', 'OTHours', 'TotalOTPaid', 'TotalOtherPay')

# Save tables to Cleaned_data folder using parquet
employee_dim.write.mode("overwrite").parquet("dataset/cleaned_data/employee_dim")
agency_dim.write.mode("overwrite").parquet("dataset/cleaned_data/agency_dim")
jobtitle_dim.write.mode("overwrite").parquet("dataset/cleaned_data/jobtitle_dim")
time_dim.write.mode("overwrite").parquet("dataset/cleaned_data/time_dim")
payroll_fact_tbl.write.mode("overwrite").parquet("dataset/cleaned_data/payroll_fact_table")

# Function to rename all columns to lowercase
def rename_columns_to_lowercase(df):
    return df.toDF(*[col.lower() for col in df.columns])

# Apply to all dimension tables
employee_dim = rename_columns_to_lowercase(employee_dim)
agency_dim = rename_columns_to_lowercase(agency_dim)
jobtitle_dim = rename_columns_to_lowercase(jobtitle_dim)
time_dim = rename_columns_to_lowercase(time_dim)
payroll_fact_tbl = rename_columns_to_lowercase(payroll_fact_tbl)

# Develop a function to get the Database connection
load_dotenv()

DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PORT = os.getenv("DB_PORT")
DB_PASS = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOSTS")

def get_db_connection():
    connection = psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        port=DB_PORT,
        options="-c search_path=nyc_payroll"
    )
    return connection

#connect to our database
conn = get_db_connection()

# Create a function create tables
def create_tables():
    conn = get_db_connection()
    cursor = conn.cursor()
    create_table_query = '''

                        CREATE SCHEMA IF NOT EXISTS nyc_payroll;

                        DROP TABLE IF EXISTS nyc_payroll.employee CASCADE;
                        DROP TABLE IF EXISTS nyc_payroll.agency CASCADE;
                        DROP TABLE IF EXISTS nyc_payroll.jobtitle CASCADE;
                        DROP TABLE IF EXISTS nyc_payroll.time CASCADE;
                        DROP TABLE IF EXISTS nyc_payroll.fact_table CASCADE;


                        CREATE TABLE IF NOT EXISTS nyc_payroll.employee(
                            employeeid INT PRIMARY KEY,
                            lastname VARCHAR(1000),
                            firstname VARCHAR(1000),
                            worklocationborough VARCHAR(1000),
                            leavestatusasofjune30 VARCHAR(1000)
                        );

                        CREATE TABLE IF NOT EXISTS nyc_payroll.agency(
                            agencyID INT PRIMARY KEY,
                            agencyName VARCHAR(1000)
                        );

                        CREATE TABLE IF NOT EXISTS nyc_payroll.jobtitle(
                            titleCode INT PRIMARY KEY,
                            titleDescription VARCHAR(1000)
                        );

                        CREATE TABLE IF NOT EXISTS nyc_payroll.time(
                            timeID INT PRIMARY KEY,
                            fiscalYear INT
                        );

                        CREATE TABLE IF NOT EXISTS nyc_payroll.fact_table(
                            payrollID INT PRIMARY KEY,
                            employeeID INT,
                            agencyID INT,
                            titleCode INT,
                            timeID INT,
                            payrollNumber INT,
                            basesalary DECIMAL(10,2),
                            paybasis VARCHAR(1000),
                            agencystartDate DATE,
                            regularhours DECIMAL(10,2),
                            regulargrossPaid DECIMAL(10,2),
                            othours DECIMAL(10,2),
                            totalotpaid DECIMAL(10,2),
                            totalotherpay DECIMAL(10,2),
                            FOREIGN KEY (employeeid) REFERENCES nyc_payroll.employee(employeeid),
                            FOREIGN KEY (agencyid) REFERENCES nyc_payroll.agency(agencyid),
                            FOREIGN KEY (titlecode) REFERENCES nyc_payroll.jobtitle(titlecode),
                            FOREIGN KEY (timeid) REFERENCES nyc_payroll.time(timeid)

                        );

        '''
    cursor.execute(create_table_query)
    conn.commit()
    cursor.close()
    conn.close()

create_tables()

url = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"
properties = {
    "user": DB_USER,
    "password": DB_PASS,
    "driver": "org.postgresql.Driver"
}
try:
    employee_dim.write.jdbc(url=url, table="nyc_payroll.employee",  mode="append", properties=properties)
    agency_dim.write.jdbc(url=url, table="nyc_payroll.agency",  mode="append", properties=properties)
    jobtitle_dim.write.jdbc(url=url, table="nyc_payroll.jobtitle",  mode="append", properties=properties)
    time_dim.write.jdbc(url=url, table="nyc_payroll.time", mode="append", properties=properties)
    payroll_fact_tbl.write.jdbc(url=url, table="nyc_payroll.fact_table",  mode="append", properties=properties)
    print('database, table and data loaded successfully')
except Exception as e:
    print("Data loading Failed!", e)
