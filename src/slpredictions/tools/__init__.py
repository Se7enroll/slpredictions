import logging
import duckdb as db
from .sldk_api import SLDK

logger = logging.getLogger(__name__)


def main() -> int:
    logger.info("Starting database update.")
    sldk = SLDK()
    with db.connect("src/sl.db") as con:
        setup_tables(con)

        logger.info("Getting seasons...")
        seasons_df = sldk.get_seasons()
        no_of_seasons = seasons_df["id"].count()
        con.sql("INSERT OR IGNORE INTO Seasons BY NAME SELECT * FROM seasons_df")
        logger.info("Done getting and saving seasons.")

        # iterate over all seasons:
        for s_idx, season_row in seasons_df.iterrows():
            season_id = season_row["id"]
            logger.info(
                "Getting matches for season %s out of %s seasons.",
                season_row["year"],
                no_of_seasons,
            )

            # Check if matches already exists if not download matches.
            logger.info("Getting matches for season id %s...", season_id)
            if not con.sql(
                f"SELECT * FROM Matches WHERE tournamentId = {season_id} LIMIT 1;"
            ).fetchone():
                logger.info("season %s already in database. Skipping.", season_id)
                continue

            matches_df = sldk.get_matches(season_id)

            if matches_df is None:
                logger.warning("Failed to get matches for %s!", season_id)
                continue
            no_of_matches = matches_df["eventId"].count()
            con.sql("INSERT OR IGNORE INTO Matches BY NAME SELECT * FROM matches_df")

            logger.info("Gettingg match stats for %s matches...", no_of_matches)
            for idx, match_row in matches_df.iterrows():
                if idx % 10 == 0:
                    logger.info("Fetting stats for match %s of %s.", idx, no_of_matches)
                if match_row["statusType"] == "finished":
                    if match_row["hasOpta"] is True:  # hasOpta can be nan
                        match_stats_df = sldk.get_match_stats(match_row["eventId"])
                        if match_stats_df is not None and not match_stats_df.empty:
                            con.sql(
                                "INSERT INTO MatchStats BY NAME SELECT * FROM match_stats_df"
                            )

                        match_xg_df = sldk.get_xg_time(match_row["eventId"])
                        if match_xg_df is not None and not match_xg_df.empty:
                            con.sql(
                                "INSERT INTO MatchXG BY NAME SELECT * FROM match_xg_df"
                            )

                    if match_row["hasOptaMomentum"] is True:  # hasMomentum can be nan
                        match_momentum_df = sldk.get_momentum(match_row["eventId"])
                        if (
                            match_momentum_df is not None
                            and not match_momentum_df.empty
                        ):
                            con.sql(
                                "INSERT INTO MatchMomentum BY NAME SELECT * FROM match_momentum_df"
                            )

            logger.info("Done getting data for all matches for %s.", season_id)

    logger.info("Done")
    return 0


def setup_tables(con):
    con.sql("CREATE TABLE IF NOT EXISTS SEASONS(id int64 PRIMARY KEY, year varchar);")
    con.sql(
        """CREATE TABLE IF NOT EXISTS Matches(
                tournamentId int64 REFERENCES Seasons(id), 
                eventId int64 PRIMARY KEY,
                roundNr int8, 
                homeId int16,
                awayId int16,
                homeName varchar,
                awayName varchar,
                detailedScore  varchar,
                stoppageTimeHT int8,
                stoppageTimeFT int8,
                hasOpta boolean,
                hasOptaMomentum boolean,
                statusType varchar
                );
        """.replace("\n", "")
    )
    con.sql(
        "CREATE TABLE IF NOT EXISTS MatchStats(teamId int16, eventId int64 REFERENCES Matches(eventId), variable varchar, value double);"
    )
    con.sql(
        """CREATE TABLE IF NOT EXISTS MatchMomentum(
                eventId int64 REFERENCES Matches(eventId),
                minute int16,
                endRecordMin int16,
                momentumValue int16,
                homePosessionValue double,
                awayPosessionValue double,
                homeMinutesWithMomentum int16,
                awayMinutesWithMomentum int16
                );
                """.replace("\n", "")
    )
    con.sql(
        """CREATE TABLE IF NOT EXISTS MatchXG(
                eventId int64 REFERENCES Matches(eventId),    
                min int16,
                sec int8,
                x float,
                y float,
                period_id int4,
                expectedGoalsValue double,
                situation varchar,
                type varchar,
                teamId int16
                );
                """.replace("\n", "")
    )
    con.sql(
        """CREATE TABLE IF NOT EXISTS MarketValues(
        team varchar,
        numPlayers int8,
        meanAge double,
        meanValue double,
        totalValue double,
        season varchar
        );
    """.replace("\n","")
    )
