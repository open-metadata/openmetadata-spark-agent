from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.master("local")
    .appName("localTestApp")
    .config(
        "spark.jars","openmetadata-spark-agent.jar,mysql-connector-java-8.0.30.jar"
    )
    .config(
        "spark.extraListeners", "org.openmetadata.spark.agent.OpenMetadataSparkListener"
    )
    .config("spark.openmetadata.transport.url", "http://localhost:8585")
    .config("spark.openmetadata.transport.type", "openmetadata")
    .config("spark.openmetadata.transport.pipelineName", "sparkPipeline12")
    .config(
        "spark.openmetadata.transport.authToken",
        "eyJraWQiOiJHYjM4OWEtOWY3Ni1nZGpzLWE5MmotMDI0MmJrOTQzNTYiLCJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJhZG1pbiIsImlzQm90IjpmYWxzZSwiaXNzIjoib3Blbi1tZXRhZGF0YS5vcmciLCJpYXQiOjE2NjM5Mzg0NjIsImVtYWlsIjoiYWRtaW5Ab3Blbm1ldGFkYXRhLm9yZyJ9.tS8um_5DKu7HgzGBzS1VTA5uUjKWOCU0B_j08WXBiEC0mr0zNREkqVfwFDD-d24HlNEbrqioLsBuFRiwIWKc1m_ZlVQbG7P36RUxhuv2vbSp80FKyNM-Tj93FDzq91jsyNmsQhyNv_fNr3TXfzzSPjHt8Go0FMMP66weoKMgW2PbXlhVKwEuXUHyakLLzewm9UMeQaEiRzhiTMU3UkLXcKbYEJJvfNFcLwSl9W8JCO_l0Yj3ud-qt_nQYEZwqW6u5nfdQllN133iikV4fM5QZsMCnm8Rq1mvLR0y9bmJiD7fwM1tmJ791TUWqmKaTnP49U493VanKpUAfzIiOiIbhg",
    )
    .config("spark.openmetadata.transport.pipelineServiceUrl", "http://localhost:8080")
    .config("spark.openmetadata.transport.pipelineServiceName", "SparkAgentService12")
    .config("spark.openmetadata.transport.pipelineDescription", "This is my ETL")
    .config("spark.openmetadata.transport.timeout", "30")
    .getOrCreate()
)

# Read table using jdbc()

# Read from MySQL Table
employee_df = (
    spark.read.format("jdbc")
    .option("url", "jdbc:mysql://localhost:3306/openmetadata_db")
    .option("driver", "com.mysql.cj.jdbc.Driver")
    .option("dbtable", "empoyee")
    .option("user", "openmetadata_user")
    .option("password", "openmetadata_password")
    .load()
)


# Show the schema and a few rows of the employee dataframe
employee_df.printSchema()
employee_df.show()


# Write data to the new employee_new table
(
    employee_df.write.format("jdbc")
    .option("url", "jdbc:mysql://localhost:3306/openmetadata_db")
    .option("driver", "com.mysql.cj.jdbc.Driver")
    .option("dbtable", "empoyee_new")
    .option("user", "openmetadata_user")
    .option("password", "openmetadata_password")
    .mode("overwrite").save()
)

# Stop the Spark session

employee_df.show()

spark.stop()
