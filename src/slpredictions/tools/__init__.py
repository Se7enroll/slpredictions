import pandas as pd
import duckdb as db
from .data import get_seasons, get_matches, get_match_data


def main() -> int:
    print("Hello from slpredictions!")
    with db.connect("sl.db") as con:
        seasons_df = get_seasons()
        seasons_df = seasons_df.tail(2)
        print(seasons_df)
        no_of_seasons = seasons_df["id"].count()
        con.sql("CREATE OR REPLACE TABLE Seasons AS SELECT * FROM seasons_df")
        res = []
        # iterate over all seasons:
        for s_idx, season_row in seasons_df.iterrows():
            print(f"getting season {season_row['year']}")
            season_id = season_row["id"]
            print(f"getting matches for season id {season_id}...")
            matches_df = get_matches(season_id)

            print(matches_df)

            no_of_matches = matches_df["eventId"].count()

            con.sql("CREATE TABLE IF NOT EXISTS Matches AS SELECT * FROM matches_df")
            con.sql("INSERT INTO Matches SELECT * FROM matches_df")

            print(
                f"gettingg match data for {no_of_matches} matches ... this might take som time"
            )
            for idx, match_row in matches_df.iterrows():
                if idx % 10 == 0:
                    print(f"getting match {idx} of {no_of_matches}")
                if match_row["statusType"] == "finished" and match_row["hasOpta"]:
                    res.append(get_match_data(match_row["eventId"]))
            print("done getting matches")
            matchdata_df = pd.concat(res)
            con.sql("CREATE TABLE IF NOT EXISTS MatchData AS SELECT * FROM matchdata_df")
            con.sql("INSERT INTO MatchData SELECT * FROM matchdata_df")

        con.sql("select top 10 * from Seasons").show()
        con.sql("select top 10 * from Matches").show()

    print("done")
    return 0
