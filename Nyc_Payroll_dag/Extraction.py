# Import Necessary Libraries
import pyspark
from pyspark.sql import SparkSession
import os, glob


# Data Extraction
def run_extraction():
    try:
        # Set Java Environment
        #os.environ['JAVA_HOME'] = "C:/java8"

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
        print("Data extracted successfully")
    except Exception as e:
        print("An error occured: {e}")