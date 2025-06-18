import org.apache.spark.sql.SparkSession
import Layer.{BronzeLayer, SilverLayer, GoldLayer}
import org.apache.log4j.{Level, Logger}

object Main {
  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.INFO)
    val spark = SparkSession.builder()
      .appName("MoviePipeline")
      .enableHiveSupport()
      .master("local[*]")
      .getOrCreate()

    implicit val s: SparkSession = spark

    val (df1, df2) = BronzeLayer.run()
    val silverDF = SilverLayer.run(df1,df2)(spark)

    val url = "jdbc:postgresql://localhost:5432/postgres"
    val user = "postgres"
    val password = "diarms"

    val goldDF = GoldLayer.run(silverDF, url, user, password)


    spark.stop()
  }
}
