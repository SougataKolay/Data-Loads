import sys
import logging
from urllib import response
from urllib import response
import boto3, re, traceback
from datetime import datetime
from urllib.parse import urlparse
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.job import Job
from pyspark.sql.utils import AnalysisException
from awsglue.utils import getResolvedOptions
from pyspark.sql import SQLContext
from pyspark.sql.functions import col, date_format,substring, trim
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import current_timestamp, lit

# -------------------------------------------------------------------------
# Logging Setup
# -------------------------------------------------------------------------
logger = logging.getLogger()
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
handler.setFormatter(formatter)
if logger.hasHandlers():
    logger.handlers.clear()
logger.addHandler(handler)
logger.setLevel(logging.INFO)

print("==== JOB STARTED ====")

# -------------------------------------------------------------------------
# Resolve Arguments
# -------------------------------------------------------------------------
print("Resolving job parameters...")

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "database_name",
        "table_name",
        "CATALOG",
        "raw_file_folder",
        "data_bucket",
        "log_bucket"
    ]
)

print("Resolved parameters:")
for k, v in args.items():
    print(f"  {k} = {v}")

TGT_DB = args["database_name"]
TGT_TBL = args["table_name"]
CATALOG = args["CATALOG"]
raw_file_folder = args["raw_file_folder"].strip().rstrip("/")
data_bucket = args["data_bucket"].strip().rstrip("/")
log_bucket = args["log_bucket"]

TARGET_IDENTIFIER = f"{CATALOG}.{TGT_DB}.{TGT_TBL}"

# -------------------------------------------------------------------------
# Spark / Iceberg Initialization
# -------------------------------------------------------------------------
print("Initializing Spark session with Iceberg configs...")

sc = SparkContext.getOrCreate()
s3 = boto3.client("s3")

spark = (
    SparkSession.builder
    .config("spark.sql.defaultCatalog", CATALOG)
    .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
    .config(f"spark.sql.catalog.{CATALOG}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
    .config(f"spark.sql.catalog.{CATALOG}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.iceberg.handle-timestamp-without-timezone", "true")
    .getOrCreate()
)

glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# -------------------------------------------------------------------------
# Utility Functions
# -------------------------------------------------------------------------

def write_log(target_table, message):
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    safe_table = re.sub(r'[^A-Za-z0-9_]+', '_', target_table)
    
    LOG_BUCKET = args["log_bucket"]
    log_key = f"glue_logs/{safe_table}_{timestamp}.txt"

    resp = s3.list_objects_v2(Bucket=LOG_BUCKET, Prefix="glue_logs/", MaxKeys=1)
    if 'Contents' not in resp:
        s3.put_object(Bucket=LOG_BUCKET, Key="glue_logs/", Body=b'')
        
    s3.put_object(
        Bucket=LOG_BUCKET,
        Key=log_key,
        Body=message.encode("utf-8")
    )

    logger.info(f"Log written to s3://{LOG_BUCKET}/{log_key}")
    
# Archive input file upon success
def archive_raw_file(target_table, csv_key):
    ts = datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S")
    filename = csv_key.split("/")[-1]
    archive_key = f"{raw_file_folder}_archive/{target_table}/{target_table}_{ts}_{filename}"

    s3.copy_object(
        Bucket=data_bucket,
        CopySource={"Bucket": data_bucket, "Key": csv_key},
        Key=archive_key
    )

    s3.delete_object(Bucket=data_bucket, Key=csv_key)

    logger.info(f"Archived file to s3://{data_bucket}/{archive_key}")

# -------------------------------------------------------------------------
# Lookup TXT in input folder (must be exactly one)
# -------------------------------------------------------------------------
def get_raw_file(bucket, prefix):
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    if "Contents" not in resp:
        raise ValueError(f"No files found under s3://{bucket}/{prefix}")

    txt_files = [
        obj["Key"]
        for obj in resp["Contents"]
        if obj["Key"].endswith(".txt")
    ]

    if len(txt_files) != 1:
        raise ValueError(f"Expected 1 TXT file under {prefix}, found {len(txt_files)}")

    return txt_files[0]

print("==== JOB STARTED ====")

print("Spark session created successfully")
# -------------------------------------------------------------------------
# Main ETL Logic
# -------------------------------------------------------------------------

def process_file(csv_key):

    full_path = f"s3://{data_bucket}/{csv_key}"
    logger.info(f"Reading file: {full_path}")

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .option("trimValues", "true")
        .csv(full_path)
    )

    logger.info(f"Detected columns: {df.columns}")

    # Column Mapping
    column_mapping = {
        "CLUB NAME": "ClubName",
        "CLUB NO": "ClubNo",
        "CLUB REGION": "RegionNo",
        "GROUP": "GroupCode",
        "SUB GROUP": "SubGroupCode",
        "AGENT": "AgentType",
        "CAMPAIGN": "CampaignCode",
        "CAMPAIGN TYPE": "CampaignType",
        "TRAN DATE": "TransactionDate",
        "ADMIN FEE": "AdminFee",
        "PRIMARY": "PrimaryIndicator",
        "ADULT": "AdultIndicator",
        "DEPENDENT": "DependantIndicator",
        "IND PLUS": "PlusProductIndicator",
        "FAM PLUS": "FamilyPlusIndicator",
        "IND-PREMIER": "PremierProductIndicator",
        "FAM PREMIER": "FamilyPremierIndicator",
        "RV/CYL": "RVCyIIndicator",
        "A/R": "ARIndicator",
        "MEMBER #": "MemberNo",
        "SALES REGION": "SalesRegion",
        "DO": "OfficeNo",
        "DO NAME": "OfficaName",
        "EMPLOYEE #": "EmployeeNo",
        "AGENT ID": "AgentID",
        "JOB": "JobCode",
        "ROLE": "Rolecode",
        "TRANS": "TransType"
    }

    # Rename columns
    for old_col, new_col in column_mapping.items():
        if old_col in df.columns:
            df = df.withColumnRenamed(old_col, new_col)

    # Add missing Extract_T column
    df = df.withColumn("Extract_T", current_timestamp())

    # Reorder columns
    final_columns = [
        "ClubName", "ClubNo", "RegionNo", "GroupCode", "SubGroupCode",
        "AgentType", "CampaignCode", "CampaignType", "TransactionDate",
        "AdminFee", "PrimaryIndicator", "AdultIndicator",
        "DependantIndicator", "PlusProductIndicator",
        "FamilyPlusIndicator", "PremierProductIndicator",
        "FamilyPremierIndicator", "RVCyIIndicator", "ARIndicator",
        "MemberNo", "SalesRegion", "OfficeNo", "OfficaName",
        "EmployeeNo", "AgentID", "JobCode", "Rolecode",
        "TransType", "Extract_T"
    ]

    df = df.select(final_columns)

    logger.info("Writing to Iceberg table")

    df.createOrReplaceTempView("staging_table")

    spark.sql(f"""
        CREATE OR REPLACE TABLE {TARGET_IDENTIFIER}
        USING iceberg
        AS
        SELECT * FROM staging_table
    """)

    logger.info("Iceberg write completed")

    archive_raw_file(TGT_TBL, csv_key)
    write_log(TGT_TBL, "Job completed successfully")


# -------------------------------------------------------------------------
# Driver
# -------------------------------------------------------------------------

def main():
    prefix = f"{raw_file_folder}/{TGT_TBL}/"

    try:
        csv_key = get_raw_file(data_bucket, prefix)
        process_file(csv_key)
        job.commit()
        logger.info("Glue job completed successfully")

    except Exception as e:
        error_message = f"Job failed: {str(e)}\n{traceback.format_exc()}"
        write_log(TGT_TBL, error_message)
        raise


if __name__ == "__main__":
    main()
