# OpenMetadata Spark Agent

> [!WARNING]
> **This project is deprecated and no longer maintained.**
>
> OpenMetadata and Collate now ingest Spark lineage directly from the standard
> [OpenLineage Spark integration](https://openlineage.io/docs/integrations/spark/)
> through the native `/api/v1/openlineage/lineage` endpoint. You no longer need a
> custom agent. `openmetadata-spark-agent.jar` will not receive new releases or fixes.
>
> Please migrate using the
> [Spark Lineage Ingestion guide](https://docs.getcollate.io/ai-2-0/how-to-guides/guide-for-data-users/ingestion/lineage/spark-lineage#spark-lineage-ingestion).

## Migrating to OpenLineage

1. Replace `openmetadata-spark-agent.jar` with the upstream `io.openlineage:openlineage-spark` package:
   - Spark 3.x: `io.openlineage:openlineage-spark_2.12:1.37.0` (or newer)
   - Spark 2.4.x: `io.openlineage:openlineage-spark_2.12:1.36.0` (the last release that supports Spark 2)
2. Replace the `spark.openmetadata.transport.*` properties with `spark.openlineage.transport.*`:

```properties
spark.extraListeners io.openlineage.spark.agent.OpenLineageSparkListener
spark.openlineage.transport.type http
spark.openlineage.transport.url https://<your-openmetadata-host>
spark.openlineage.transport.endpoint /api/v1/openlineage/lineage
spark.openlineage.transport.auth.type api_key
spark.openlineage.transport.auth.apiKey <your-bot-jwt>
spark.openlineage.namespace <your-namespace>
```

For Databricks, AWS Glue and PySpark examples, see the
[Spark Lineage Ingestion guide](https://docs.getcollate.io/ai-2-0/how-to-guides/guide-for-data-users/ingestion/lineage/spark-lineage#spark-lineage-ingestion).

## License

Apache License 2.0. See [LICENSE](LICENSE).
