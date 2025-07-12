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

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1752298957468 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_trusted", transformation_ctx="StepTrainerTrusted_node1752298957468")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1752298993466 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1752298993466")

# Script generated for node SQL Query
SqlQuery238 = '''
select * from
accel
inner join step
on accel.timestamp = step.sensorreadingtime
'''
SQLQuery_node1752299005797 = sparkSqlQuery(glueContext, query = SqlQuery238, mapping = {"step":StepTrainerTrusted_node1752298957468, "accel":AccelerometerTrusted_node1752298993466}, transformation_ctx = "SQLQuery_node1752299005797")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1752299005797, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1752298950137", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1752299063024 = glueContext.getSink(path="s3://yingqi-stedi-lakehouse/machine_learning_curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1752299063024")
AmazonS3_node1752299063024.setCatalogInfo(catalogDatabase="stedi",catalogTableName="machine_learning_curated")
AmazonS3_node1752299063024.setFormat("json")
AmazonS3_node1752299063024.writeFrame(SQLQuery_node1752299005797)
job.commit()