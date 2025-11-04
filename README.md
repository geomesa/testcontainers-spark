# testcontainers-spark

This project is for running a Spark cluster in unit tests.

## Quick Start

Add the following dependencies:

    <dependency>
      <groupId>org.geomesa.testcontainers</groupId>
      <artifactId>testcontainers-spark</artifactId>
      <version>1.0.0</version>
      <scope>test</scope>
    </dependency>
    <!-- only required for GeoMesa support - use the appropriate spark-runtime for your setup -->
    <dependency>
      <groupId>org.locationtech.geomesa</groupId>
      <artifactId>geomesa-gt-spark-runtime_2.12</artifactId>
      <version>5.4.0</version>
      <scope>test</scope>
    </dependency>

Write unit tests against Spark:

    import org.geomesa.testcontainers.spark.SparkCluster

    val cluster = new SparkCluster()
    spark.start()
    sys.addShutdownHook(spark.stop())    

    val session = spark.getOrCreateSession();
    val df =
      session.read
        .format("geomesa")
        .options(dsParams) // data store parameters to connect to your store
        .option("geomesa.feature", "chicago") // feature type name
        .load();
    // execute tests
