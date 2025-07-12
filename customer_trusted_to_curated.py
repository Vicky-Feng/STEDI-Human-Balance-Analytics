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

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1752220484136 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1752220484136")

# Script generated for node Customer Trusted
CustomerTrusted_node1752220498632 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1752220498632")

# Script generated for node Join
Join_node1752220507120 = Join.apply(frame1=AccelerometerTrusted_node1752220484136, frame2=CustomerTrusted_node1752220498632, keys1=["user"], keys2=["email"], transformation_ctx="Join_node1752220507120")

# Script generated for node SQL Query
SqlQuery2208 = '''
select distinct serialnumber, registrationdate, lastupdatedate, 
sharewithresearchasofdate, sharewithpublicasofdate,
sharewithfriendsasofdate from myDataSource
'''
SQLQuery_node1752220621652 = sparkSqlQuery(glueContext, query = SqlQuery2208, mapping = {"myDataSource":Join_node1752220507120}, transformation_ctx = "SQLQuery_node1752220621652")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1752220621652, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1752219576810", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1752220675172 = glueContext.getSink(path="s3://yingqi-stedi-lakehouse/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1752220675172")
AmazonS3_node1752220675172.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_curated")
AmazonS3_node1752220675172.setFormat("json")
AmazonS3_node1752220675172.writeFrame(SQLQuery_node1752220621652)
job.commit()