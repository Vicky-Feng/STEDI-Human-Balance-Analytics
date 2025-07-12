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
CustomerTrusted_node1752297487001 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1752297487001")

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1752297316063 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_landing", transformation_ctx="StepTrainerLanding_node1752297316063")

# Script generated for node SQL Query
SqlQuery219 = '''
select distinct
    step_trainer.sensorreadingtime
    , step_trainer.serialnumber
    , step_trainer.distancefromobject
FROM step_trainer
INNER JOIN customer
ON step_trainer.serialnumber = customer.serialnumber
'''
SQLQuery_node1752298053255 = sparkSqlQuery(glueContext, query = SqlQuery219, mapping = {"step_trainer":StepTrainerLanding_node1752297316063, "customer":CustomerTrusted_node1752297487001}, transformation_ctx = "SQLQuery_node1752298053255")

# Script generated for node Step Trainer Trusted
EvaluateDataQuality().process_rows(frame=SQLQuery_node1752298053255, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1752296917115", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
StepTrainerTrusted_node1752297580530 = glueContext.getSink(path="s3://yingqi-stedi-lakehouse/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="StepTrainerTrusted_node1752297580530")
StepTrainerTrusted_node1752297580530.setCatalogInfo(catalogDatabase="stedi",catalogTableName="step_trainer_trusted")
StepTrainerTrusted_node1752297580530.setFormat("json")
StepTrainerTrusted_node1752297580530.writeFrame(SQLQuery_node1752298053255)
job.commit()