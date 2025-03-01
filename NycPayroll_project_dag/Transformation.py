# Import Necessary Libraries
import pyspark
from pyspark.sql.functions import monotonically_increasing_id
from spark_utils import getSparkSession

def run_transformation():
    try:
        #Calling Spark Session
        spark = getSparkSession()

        #Load Master Data
        employee_df = spark.read.csv('dataset/raw/EmpMaster.csv', header=True, inferSchema=True)
        agency_df = spark.read.csv('dataset/raw/AgencyMaster.csv', header=True, inferSchema=True)
        jobtitle_df = spark.read.csv('dataset/raw/TitleMaster.csv', header=True, inferSchema=True)
        payroll_df = spark.read.csv('Nyc_Payroll_dag/dataset/payroll_merged_data/payroll_merged.csv', header=True, inferSchema=True)

        # Convert AgencyStartDate to Date datatype
        payroll_df = payroll_df.withColumn("AgencyStartDate", pyspark.sql.functions.to_date(payroll_df["AgencyStartDate"], "M/d/yyyy"))

        # Check for Null values
        for column in payroll_df.columns:
            print(column, 'Nulls: ', payroll_df.filter(payroll_df[column].isNull()).count())

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
        employee_dim.write.csv('Nyc_Payroll_dag/dataset/cleaned_data/employee_dim.csv', header=True)
        agency_dim.write.csv('Nyc_Payroll_dag/dataset/cleaned_data/agency_dim.csv', header=True)
        jobtitle_dim.write.csv('Nyc_Payroll_dag/dataset/cleaned_data/jobtitle_dim.csv', header=True)
        time_dim.write.csv('Nyc_Payroll_dag/dataset/cleaned_data/time_dim.csv', header=True)
        payroll_fact_tbl.write.csv('Nyc_Payroll_dag/dataset/cleaned_data/payroll_fact_tbl.csv', header=True)

        # Function to rename all columns to lowercase
        def rename_columns_to_lowercase(df):
            return df.toDF(*[col.lower() for col in df.columns])

        # Apply to all dimension tables
        employee_dim = rename_columns_to_lowercase(employee_dim)
        agency_dim = rename_columns_to_lowercase(agency_dim)
        jobtitle_dim = rename_columns_to_lowercase(jobtitle_dim)
        time_dim = rename_columns_to_lowercase(time_dim)
        payroll_fact_tbl = rename_columns_to_lowercase(payroll_fact_tbl)
        print("Transformation completed successfully")
    except Exception as e:
        print("Transformation Failed", e)