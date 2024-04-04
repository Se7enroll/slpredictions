from .data import get_seasons_json, get_matches, get_match_data
import pandas as pd


def main() -> int:
    print("Hello from slpredictions!")
    #seasons = get_seasons_json()
    # remove before fligt!
    season_id = 20962
    #event_id = 4449965
    print("getting matches...")
    matches = get_matches(season_id)
    no_of_matches = matches["roundNr"].max()
    print(f"gettingg match data for {no_of_matches} matches ... this might take som time")
    res = []
    for idx, row in matches.iterrows():
        if idx % 10 == 0: print(f"getting match {idx} of {no_of_matches}")
        if row["hasOpta"]: res.append(get_match_data(row["eventId"]))
    print("done getting matches")
    df = pd.concat(res)
    print(df)
    return 0
