package Utils

import org.apache.spark.sql.DataFrame
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
}
