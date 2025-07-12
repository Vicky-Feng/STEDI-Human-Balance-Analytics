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

# Script generated for node Customer Landing
CustomerLanding_node1752296444579 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_landing", transformation_ctx="CustomerLanding_node1752296444579")

# Script generated for node Share with Research
SqlQuery258 = '''
select * from myDataSource
WHERE sharewithresearchasofdate IS NOT NULL
'''
SharewithResearch_node1752296469015 = sparkSqlQuery(glueContext, query = SqlQuery258, mapping = {"myDataSource":CustomerLanding_node1752296444579}, transformation_ctx = "SharewithResearch_node1752296469015")

# Script generated for node ustomer Trusted
EvaluateDataQuality().process_rows(frame=SharewithResearch_node1752296469015, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1752296235507", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
ustomerTrusted_node1752296521428 = glueContext.getSink(path="s3://yingqi-stedi-lakehouse/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="ustomerTrusted_node1752296521428")
ustomerTrusted_node1752296521428.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_trusted")
ustomerTrusted_node1752296521428.setFormat("json")
ustomerTrusted_node1752296521428.writeFrame(SharewithResearch_node1752296469015)
job.commit()