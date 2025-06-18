package Layer
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import Utils.Writer.writeToPostgres

object GoldLayer {
  def run(df: DataFrame, url: String, user: String, password: String)(implicit spark: SparkSession): Unit = {
    val enrichedDF = spark.table("silver.enriched_players")

    // Datamart 1: Performance Globale des joueurs
    val dm_player_perf = spark.table("silver.enriched_players")
      .groupBy("Player", "season")
      .agg(
        sum("Goals").as("total_goals"),
        sum("Assists").as("total_assists"),
        sum("Min").as("total_minutes"),
        sum("Shots").as("total_shots"),
        avg("PasTotCmp%").as("avg_pass_accuracy"),
        sum("SCA").as("total_sca"),
        sum("GCA").as("total_gca"),
        sum("CrdY").as("yellow_cards"),
        sum("CrdR").as("red_cards")
      )
    writeToPostgres(dm_player_perf, url, "dm_player_perf", user, password)

    // Datamart 2: Profil moyen par poste
    val dm_poste = spark.table("silver.enriched_players")
      .groupBy("Pos")
      .agg(
        avg("Goals").as("avg_goals"),
        avg("Tkl").as("avg_tackles"),
        avg("PasProg").as("avg_pass_prog"),
        avg("Carries").as("avg_carries"),
        avg("Touches").as("avg_touches")
      )
    writeToPostgres(dm_poste, url, "dm_poste", user, password)

    // Datamart 3: Joueurs rentables
    val dm_ratio = spark.table("silver.enriched_players")
      .selectExpr("Player", "season", "Goals", "Assists", "Min",
        "Goals*90/Min as G_per_90", "Assists*90/Min as A_per_90",
        "Shots/Goals as shots_per_goal")
    writeToPostgres(dm_ratio, url, "dm_ratio", user, password)

    // Datamart 4: Défense des équipes
    val dm_def = spark.table("silver.enriched_players")
      .groupBy("Squad", "season")
      .agg(
        sum("Tkl").as("tackles"),
        sum("Int").as("interceptions"),
        sum("Clr").as("clearances"),
        sum("Err").as("errors")
      )
    writeToPostgres(dm_def, url, "dm_def", user, password)

    // Datamart 5: Progression des joueurs
    val dm_progress = spark.table("silver.enriched_players")
      .select("Player", "season", "Goals", "Assists", "PasTotCmp%", "GCA", "Touches")
    writeToPostgres(dm_progress, url, "dm_progress", user, password)

  }
}
