import logging
import pandas as pd
import duckdb as db
from .data import get_seasons, get_matches, get_match_data

l = logging.getLogger(__name__)


def main() -> int:
    l.info("Starting database update.")
    with db.connect("sl.db") as con:
        # setup tables
        con.sql(
            "CREATE TABLE IF NOT EXISTS SEASONS(id int64 PRIMARY KEY, year varchar);"
        )
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
        );""".replace("\n", "")
        )
        con.sql(
            "CREATE TABLE IF NOT EXISTS MatchData(teamId int16, eventId int64 REFERENCES Matches(eventId), variable varchar, value double);"
        )

        seasons_df = get_seasons()
        # seasons_df = seasons_df.tail(2)        # take last n seasons
        # seasons_df = seasons_df.iloc[::-1]     # reverse order, so latest season is first
        no_of_seasons = seasons_df["id"].count()

        con.sql("INSERT OR IGNORE INTO Seasons BY NAME SELECT * FROM seasons_df")

        # iterate over all seasons:
        for s_idx, season_row in seasons_df.iterrows():
            l.info("Getting season %s out of %s.", season_row["year"], no_of_seasons)
            season_id = season_row["id"]
            l.info("Getting matches for season id %s...", season_id)

            if not con.sql(
                f"SELECT * FROM Matches WHERE tournamentId = {season_id} LIMIT 1;"
            ).fetchone():
                l.info("season %s already in database. Skipping.", season_id)
                continue

            matches_df = get_matches(season_id)

            if matches_df is None:
                l.warning("Failed to get matches for %s!", season_id)
                continue
            no_of_matches = matches_df["eventId"].count()

            con.sql("INSERT OR IGNORE INTO Matches BY NAME SELECT * FROM matches_df")

            l.info("Gettingg match data for %s matches...", no_of_matches)
            res = []
            for idx, match_row in matches_df.iterrows():
                if idx % 10 == 0:
                    l.info("Fetting match %s of %s.", idx, no_of_matches)
                if (
                    match_row["statusType"] == "finished"
                    and match_row["hasOpta"] is True
                ):  # hasOpta can be nan
                    res.append(get_match_data(match_row["eventId"]))
            l.info("Done getting matches.")

            if not res:
                l.warning("warning: no match data for season %s!", season_id)
                continue
            matchdata_df = pd.concat(res)
            value_cols = matchdata_df.columns
            matchdata_df.reset_index(inplace=True)
            matchdata_df.rename(columns={"level_0": "teamId"}, inplace=True)
            index_cols = ["teamId", "eventId"]
            matchdata_df = pd.melt(matchdata_df, index_cols, value_cols)

            con.sql("INSERT INTO MatchData SELECT * FROM matchdata_df")

    l.info("Done")
    return 0
