import org.apache.spark.sql.SparkSession
import Layer.{BronzeLayer, SilverLayer, GoldLayer}
import org.apache.log4j.{Level, Logger}

object Main {
  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.INFO)
    val spark = SparkSession.builder()
      .appName("MoviePipeline")
      .master("local[*]")
      .getOrCreate()

    implicit val s: SparkSession = spark

    val bronzeDF = BronzeLayer.run()
    //val silverDF = SilverLayer.run(bronzeDF)
    //val GoldLayer.run(silverDF)

    spark.stop()
  }
}
