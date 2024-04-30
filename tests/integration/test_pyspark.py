from pyspark.sql import SparkSession
from unittest import TestCase

from metadata.generated.schema.entity.services.connections.metadata.openMetadataConnection import (
    OpenMetadataConnection,
)
from metadata.generated.schema.entity.data.pipeline import Pipeline
from metadata.generated.schema.entity.data.table import Table
from metadata.generated.schema.entity.services.pipelineService import PipelineService
from metadata.generated.schema.security.client.openMetadataJWTClientConfig import (
    OpenMetadataJWTClientConfig,
)
from metadata.ingestion.ometa.ometa_api import OpenMetadata


JWT_TOKEN = (
    "eyJraWQiOiJHYjM4OWEtOWY3Ni1nZGpzLWE5MmotMDI0MmJrOTQzNTYiLCJ0eXAiOiJKV1QiLCJhbGciOiJSU"
    "zI1NiJ9.eyJzdWIiOiJhZG1pbiIsImlzQm90IjpmYWxzZSwiaXNzIjoib3Blbi1tZXRhZGF0YS5vcmciLCJpYX"
    "QiOjE2NjM5Mzg0NjIsImVtYWlsIjoiYWRtaW5Ab3Blbm1ldGFkYXRhLm9yZyJ9.tS8um_5DKu7HgzGBzS1VTA5"
    "uUjKWOCU0B_j08WXBiEC0mr0zNREkqVfwFDD-d24HlNEbrqioLsBuFRiwIWKc1m_ZlVQbG7P36RUxhuv2vbSp80"
    "FKyNM-Tj93FDzq91jsyNmsQhyNv_fNr3TXfzzSPjHt8Go0FMMP66weoKMgW2PbXlhVKwEuXUHyakLLzewm9UMeQ"
    "aEiRzhiTMU3UkLXcKbYEJJvfNFcLwSl9W8JCO_l0Yj3ud-qt_nQYEZwqW6u5nfdQllN133iikV4fM5QZsMCnm8R"
    "q1mvLR0y9bmJiD7fwM1tmJ791TUWqmKaTnP49U493VanKpUAfzIiOiIbhg"
)

PIPELINE_NAME = "sparkPipeline"

PIPELINE_SERVICE_NAME = "SparkAgentService"

PIPELINE_DESCRIPTION = "This is my ETL"

PIPELINE_SOURCE_URL = "http://localhost:8080"

class PySparkTest(TestCase):
    @classmethod
    def setUpClass(cls):
        spark = (
            SparkSession.builder.master("local")
            .appName("localTestApp")
            .config(
                "spark.jars",
                "target/openmetadata-spark-agent.jar,tests/resources/mysql-connector-java-8.0.30.jar",
            )
            .config(
                "spark.extraListeners",
                "org.openmetadata.spark.agent.OpenMetadataSparkListener",
            )
            .config("spark.openmetadata.transport.hostPort", "http://localhost:8585")
            .config("spark.openmetadata.transport.type", "openmetadata")
            .config("spark.openmetadata.transport.pipelineName", PIPELINE_NAME)
            .config("spark.openmetadata.transport.jwtToken", JWT_TOKEN)
            .config(
                "spark.openmetadata.transport.pipelineSourceUrl",
                PIPELINE_SOURCE_URL,
            )
            .config(
                "spark.openmetadata.transport.pipelineServiceName", PIPELINE_SERVICE_NAME
            )
            .config(
                "spark.openmetadata.transport.pipelineDescription", PIPELINE_DESCRIPTION
            )
            .config(
                "spark.openmetadata.transport.databaseServiceNames",
                "random,local_mysql",
            )
            .config("spark.openmetadata.transport.timeout", "30")
            .getOrCreate()
        )

        # Read table using jdbc()

        # Read from MySQL Table
        employee_df = (
            spark.read.format("jdbc")
            .option("url", "jdbc:mysql://localhost:3306/openmetadata_db")
            .option("driver", "com.mysql.cj.jdbc.Driver")
            .option("dbtable", "employee")
            .option("user", "openmetadata_user")
            .option("password", "openmetadata_password")
            .load()
        )

        # Write data to the new employee_new table
        (
            employee_df.write.format("jdbc")
            .option("url", "jdbc:mysql://localhost:3306/openmetadata_db")
            .option("driver", "com.mysql.cj.jdbc.Driver")
            .option("dbtable", "employee_new")
            .option("user", "openmetadata_user")
            .option("password", "openmetadata_password")
            .mode("overwrite")
            .save()
        )

        # Stop the Spark session
        spark.stop()

    def __init__(self, methodName: str = "runTest") -> None:
        super().__init__(methodName)
        server_config = OpenMetadataConnection(
            hostPort="http://localhost:8585/api",
            authProvider="openmetadata",
            enableVersionValidation=False,
            securityConfig=OpenMetadataJWTClientConfig(jwtToken=JWT_TOKEN),
        )
        self.metadata = OpenMetadata(server_config)

    def test_pipeline_creation(self):
        pipeline_service = self.metadata.get_by_name(entity=PipelineService, fqn=PIPELINE_SERVICE_NAME)
        self.assertIsNotNone(pipeline_service)
        pipeline: Pipeline = self.metadata.get_by_name(
            entity=Pipeline, 
            fqn=f"{PIPELINE_SERVICE_NAME}.{PIPELINE_NAME}"
        )
        self.assertIsNotNone(pipeline)
        self.assertEqual(pipeline.sourceUrl.__root__, PIPELINE_SOURCE_URL)
        self.assertEqual(pipeline.description.__root__, PIPELINE_DESCRIPTION)

    
    def test_lineage(self):
        lineage = self.metadata.get_lineage_by_name(
            entity=Table, 
            fqn="local_mysql.default.openmetadata_db.employee"
        )

        # check if lineage got created 
        self.assertIsNotNone(lineage)
        self.assertEqual(len(lineage.get("downstreamEdges")), 1)


        linage_details = lineage.get("downstreamEdges")[0].get("lineageDetails")
        self.assertIsNotNone(linage_details)

        # validate column level lineage
        self.assertIsNotNone(len(linage_details.get("columnsLineage")), 2)

        # validate lineage pipeline 
        self.assertIsNotNone(linage_details.get("pipeline"))
        

