# Import Necessary Libraries
import os, glob
from spark_utils import getSparkSession


# Data Extraction
def run_extraction():
    try:
        # Set Java Environment
        #os.environ['JAVA_HOME'] = "C:/java8"

        spark = getSparkSession()

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

        payroll_df.write.csv('Nyc_Payroll_dag/dataset/payroll_merged_data/payroll_merged.csv', header=True)
        print("Data extracted successfully")
    except Exception as e:
        print("An error occured: {e}")