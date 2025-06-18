package Layer

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object SilverLayer {

  def run(df1: DataFrame, df2: DataFrame)(implicit spark: SparkSession): DataFrame = {

    //val spark = SparkSession.builder.appName("EquipeStatsNettoyage").getOrCreate()

    // Chargement depuis la Bronze
    // val matchDf = spark.read.parquet("src/data/Bronze/Championnats")

    // Colonnes utiles
    val equipeCols = Seq("Div", "Date", "HomeTeam", "AwayTeam", "FTHG", "FTAG", "FTR",
      "HS", "AS", "HST", "AST", "HC", "AC",
      "HF", "AF", "HY", "AY", "HR", "AR", "country", "season")

    // Sélection des colonnes utiles
    val cleanedMatchDf = df1.select(equipeCols.map(col): _*)

    // Chargement depuis la Bronze
    //val playerDf = spark.read.parquet("src/data/Bronze/Players")

    // Colonnes utiles
    val playerCols = Seq("Player", "Nation", "Pos", "Squad", "Comp", "Age", "Born",
      "MP", "Starts", "Min", "Goals", "Shots", "SoT", "Assists", "PasTotCmp",
      "PasTotAtt", "PasTotCmp%", "PasTotDist", "PasTotPrgDist", "PasProg",
      "SCA", "GCA", "Tkl", "TklWon", "Int", "Tkl+Int", "Clr", "Err",
      "Touches", "Carries", "CarTotDist", "CarPrgDist", "CarProg",
      "Car3rd", "CPA", "CarMis", "CarDis", "Rec", "RecProg",
      "CrdY", "CrdR", "Fls", "Fld", "Off", "OG", "PKwon", "PKcon", "AerWon", "AerLost", "AerWon%", "season")

    // Sélection des colonnes utiles
    val cleanedPlayerDf = df2.select(playerCols.map(col): _*)

    //Suppression des valeurs nulles
    val cleanedMatchDfNonNull = cleanedMatchDf
      .na.drop() // Supprime toutes les lignes avec au moins une valeur null
      .filter(
        col("FTHG") >= 0 && col("FTAG") >= 0 &&
          col("HS") >= 0 && col("AS") >= 0 &&
          col("HST") >= 0 && col("AST") >= 0 &&
          col("HC") >= 0 && col("AC") >= 0 &&
          col("HF") >= 0 && col("AF") >= 0 &&
          col("HY") >= 0 && col("AY") >= 0 &&
          col("HR") >= 0 && col("AR") >= 0
      )

    val cleanedPlayerDfNonNull = cleanedPlayerDf
      .na.drop() // Supprime les lignes avec null
      .filter(
        col("Age") >= 15 && col("Age") <= 50 &&
          col("Min") >= 0 && col("Goals") >= 0 && col("Shots") >= 0 &&
          col("SoT") >= 0 && col("Assists") >= 0 &&
          col("PasTotCmp") >= 0 && col("PasTotAtt") >= 0 &&
          col("SCA") >= 0 && col("GCA") >= 0 &&
          col("Tkl") >= 0 && col("TklWon") >= 0 && col("Int") >= 0 &&
          col("Touches") >= 0 && col("Carries") >= 0 &&
          col("CarTotDist") >= 0 && col("CarPrgDist") >= 0
      )

    // Colonnes Home
    val home_df = cleanedMatchDf.select(
      col("HomeTeam").alias("team"),
      col("season"),
      lit("home").alias("match_type"),
      col("FTHG").alias("goals_scored"),
      col("FTAG").alias("goals_conceded"),
      col("HS").alias("shots"),
      col("HST").alias("shots_on_target"),
      col("HC").alias("corners"),
      col("HF").alias("fouls_committed"),
      col("HY").alias("yellow_cards"),
      col("HR").alias("red_cards")
    )

    // Colonnes Away
    val away_df = cleanedMatchDf.select(
      col("AwayTeam").alias("team"),
      col("season"),
      lit("away").alias("match_type"),
      col("FTAG").alias("goals_scored"),
      col("FTHG").alias("goals_conceded"),
      col("AS").alias("shots"),
      col("AST").alias("shots_on_target"),
      col("AC").alias("corners"),
      col("AF").alias("fouls_committed"),
      col("AY").alias("yellow_cards"),
      col("AR").alias("red_cards")
    )

    val team_matches = home_df.unionByName(away_df)

    val homeAwayPivot = team_matches
      .groupBy("team", "season")
      .pivot("match_type", Seq("home", "away")) // Pivot sur "home" et "away"
      .agg(
        count("*").alias("matches_played"),
        sum("goals_scored").alias("total_goals"),
        sum("goals_conceded").alias("total_goals_conceded"),
        avg("shots").alias("avg_shots"),
        avg("shots_on_target").alias("avg_shots_on_target"),
        avg("corners").alias("avg_corners"),
        avg("fouls_committed").alias("avg_fouls_committed"),
        sum("yellow_cards").alias("total_yellow_cards"),
        sum("red_cards").alias("total_red_cards")
      )

    val enriched_players_df = cleanedPlayerDfNonNull
      .join(homeAwayPivot,
        cleanedPlayerDfNonNull("Squad") === homeAwayPivot("team") &&
          cleanedPlayerDfNonNull("season") === homeAwayPivot("season"),
        "left")
      .drop(homeAwayPivot("team")) // facultatif : pour éviter doublon
      .drop(homeAwayPivot("season"))

    // Crée la base si elle n’existe pas
    spark.sql("CREATE DATABASE IF NOT EXISTS silver")

    // Écriture dans Hive
    enriched_players_df.write
      .mode("overwrite")
      .option("path", "D:\\EFREI\\Cours\\TP_Spark_Scala\\untitled\\src\\data\\Silver")
      .format("parquet")
      .saveAsTable("silver.enriched_players") // Crée la table Hive dans la base `Silver`

    enriched_players_df
  }
}
