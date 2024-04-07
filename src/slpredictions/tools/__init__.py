import pandas as pd
import duckdb as db
from .data import get_seasons, get_matches, get_match_data


def main() -> int:
    print("Hello from slpredictions!")
    with db.connect("sl.db") as con:
        # setup tables
        con.sql("CREATE OR REPLACE TABLE SEASONS(id int64 PRIMARY KEY, year varchar);")
        con.sql(
            """CREATE OR REPLACE TABLE Matches(
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
            "CREATE OR REPLACE TABLE MatchData(teamId int16, eventId int64 REFERENCES Matches(eventId), variable varchar, value double);"
        )

        seasons_df = get_seasons()
        # seasons_df = seasons_df.tail(2)        # take last n seasons
        # seasons_df = seasons_df.iloc[::-1]     # reverse order, so latest season is first
        no_of_seasons = seasons_df["id"].count()

        con.sql("INSERT OR IGNORE INTO Seasons BY NAME SELECT * FROM seasons_df")

        # iterate over all seasons:
        for s_idx, season_row in seasons_df.iterrows():
            print(f"getting season {season_row['year']} out of {no_of_seasons}.")
            season_id = season_row["id"]
            print(f"getting matches for season id {season_id}...")
            matches_df = get_matches(season_id)

            if matches_df is None:
                print(f"Warning: failed to get matches for {season_id}!")
                continue
            no_of_matches = matches_df["eventId"].count()

            con.sql("INSERT OR IGNORE INTO Matches BY NAME SELECT * FROM matches_df")

            print(f"gettingg match data for {no_of_matches} matches...")
            res = []
            for idx, match_row in matches_df.iterrows():
                if idx % 10 == 0:
                    print(f"getting match {idx} of {no_of_matches}")
                if (
                    match_row["statusType"] == "finished"
                    and match_row["hasOpta"] is True
                ):  # hasOpta can be nan
                    res.append(get_match_data(match_row["eventId"]))
            print("done getting matches")

            if not res:
                print(f"warning: no match data for {season_id}!")
                continue
            matchdata_df = pd.concat(res)
            value_cols = matchdata_df.columns
            matchdata_df.reset_index(inplace=True)
            matchdata_df.rename(columns={"level_0": "teamId"}, inplace=True)
            index_cols = ["teamId", "eventId"]
            matchdata_df = pd.melt(matchdata_df, index_cols, value_cols)

            con.sql("INSERT INTO MatchData SELECT * FROM matchdata_df")

    print("done")
    return 0
