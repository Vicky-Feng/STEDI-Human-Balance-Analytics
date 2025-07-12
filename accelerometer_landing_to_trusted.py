import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Customer Trusted
CustomerTrusted_node1752218724630 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1752218724630")

# Script generated for node Accelerometer Landing Zone
AccelerometerLandingZone_node1752218695267 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_landing", transformation_ctx="AccelerometerLandingZone_node1752218695267")

# Script generated for node Join Customer
JoinCustomer_node1752218747019 = Join.apply(frame1=CustomerTrusted_node1752218724630, frame2=AccelerometerLandingZone_node1752218695267, keys1=["email"], keys2=["user"], transformation_ctx="JoinCustomer_node1752218747019")

# Script generated for node SQL Query
SqlQuery2392 = '''
select
x , y, z, user, timestamp
from myDataSource
'''
SQLQuery_node1752218772644 = sparkSqlQuery(glueContext, query = SqlQuery2392, mapping = {"myDataSource":JoinCustomer_node1752218747019}, transformation_ctx = "SQLQuery_node1752218772644")

# Script generated for node Accelerometer Trusted
EvaluateDataQuality().process_rows(frame=SQLQuery_node1752218772644, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1752218692728", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AccelerometerTrusted_node1752218874750 = glueContext.getSink(path="s3://yingqi-stedi-lakehouse/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AccelerometerTrusted_node1752218874750")
AccelerometerTrusted_node1752218874750.setCatalogInfo(catalogDatabase="stedi",catalogTableName="accelerometer_trusted")
AccelerometerTrusted_node1752218874750.setFormat("json")
AccelerometerTrusted_node1752218874750.writeFrame(SQLQuery_node1752218772644)
job.commit()