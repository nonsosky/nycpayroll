# Import Necessary Libraries
import os
from dotenv import load_dotenv
import psycopg2
from spark_utils import getSparkSession

def run_loading():
    try:
        # Calling spark session function
        spark = getSparkSession()
        
        #Load Master Data
        employee_dim = spark.read.csv('Nyc_Payroll_dag/dataset/cleaned_data/employee_dim.csv', header=True, inferSchema=True)
        agency_dim = spark.read.csv('Nyc_Payroll_dag/dataset/cleaned_data/agency_dim.csv', header=True, inferSchema=True)
        jobtitle_dim = spark.read.csv('Nyc_Payroll_dag/dataset/cleaned_data/jobtitle_dim.csv', header=True, inferSchema=True)
        time_dim = spark.read.csv('Nyc_Payroll_dag/dataset/cleaned_data/time_dim.csv', header=True, inferSchema=True)
        payroll_fact_tbl = spark.read.csv('Nyc_Payroll_dag/dataset/cleaned_data/payroll_fact_tbl.csv', header=True, inferSchema=True)

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
        
        employee_dim.write.jdbc(url=url, table="nyc_payroll.employee",  mode="append", properties=properties)
        agency_dim.write.jdbc(url=url, table="nyc_payroll.agency",  mode="append", properties=properties)
        jobtitle_dim.write.jdbc(url=url, table="nyc_payroll.jobtitle",  mode="append", properties=properties)
        time_dim.write.jdbc(url=url, table="nyc_payroll.time", mode="append", properties=properties)
        payroll_fact_tbl.write.jdbc(url=url, table="nyc_payroll.fact_table",  mode="append", properties=properties)
        print('database, table and data loaded successfully')
    except Exception as e:
        print("Data loading Failed!", e)
