package Layer

import org.apache.spark.sql.{DataFrame, SparkSession}
import Utils.Reader
import Utils.Writer

object BronzeLayer {
  def run()(implicit spark: SparkSession): (DataFrame, DataFrame) = {
    val df1 = Reader.sourceFromCsv("src/data/Source/championnats_stats.csv")
    Writer.writeToParquet(df1, "src/data/Bronze/Championnats")

    val url = "jdbc:postgresql://localhost:5432/postgres"
    val table = "public.players_stats"
    val user = "postgres"
    val password = "diarms"

    val df2 = Reader.sourceFromPostgres(url, table, user, password)
    Writer.writeToParquet(df2, "src/data/Bronze/Players")

    (df1, df2)
  }

}