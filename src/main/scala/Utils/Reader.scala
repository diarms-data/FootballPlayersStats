package Utils

import org.apache.spark.sql.{DataFrame, SparkSession}

object Reader {
  def sourceFromCsv(path: String)(implicit spark: SparkSession): DataFrame = {
    spark.read
      .option("header", "true")
      .option("delimiter", ";")
      .option("encoding", "ISO-8859-1")
      .csv(path)
  }

  def sourceFromPostgres(
                          url: String,
                          table: String,
                          user: String,
                          password: String
                        )(implicit spark: SparkSession): DataFrame = {
    spark.read
      .format("jdbc")
      .option("url", url)
      .option("dbtable", table)
      .option("user", user)
      .option("password", password)
      .option("driver", "org.postgresql.Driver")
      .load()
  }
}
