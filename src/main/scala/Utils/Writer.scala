package Utils

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.log4j.Logger

object Writer {
  val logger: Logger = Logger.getLogger("Writer")

  def writeToCsv(df: DataFrame, outputPath: String): Boolean = {
    try {
      df.write
        .mode("overwrite")
        .option("header", "true")
        .csv(outputPath)
      logger.info(s"Fichier écrit avec succès dans $outputPath")
      true
    } catch {
      case e: Exception =>
        logger.error(s"Erreur lors de l’écriture dans $outputPath", e)
        false
    }
  }

  def writeToParquet(df: DataFrame, outputPath: String): Boolean = {
    try {
      df.write
        .mode("overwrite")
        .parquet(outputPath)
      logger.info(s"Fichier Parquet écrit avec succès dans $outputPath")
      true
    } catch {
      case e: Exception =>
        logger.error(s"Erreur lors de l’écriture Parquet dans $outputPath", e)
        false
    }
  }

  def writeToPostgres(
                       df: DataFrame,
                       url: String,
                       table: String,
                       user: String,
                       password: String,
                     )(implicit spark: SparkSession): Unit = {
    df.write
      .mode("overwrite")
      .format("jdbc")
      .option("url", url)
      .option("dbtable", table)
      .option("user", user)
      .option("password", password)
      .option("driver", "org.postgresql.Driver")
      .save()
  }

}
