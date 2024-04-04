import requests
import pandas as pd
from pandas import DataFrame

# Generate your own access token from superliga.dk
from secrets import access_token

season_id = 20962
event_id = 4449965

def get_seasons_json() -> DataFrame:
    """
        Gets the full season list from superliga.dk
    """
    try:
        r = requests.get(_get_season_endpoint())
        json = r.json()
        print(f"Seasons downloaded. Current season {json["seasonId"]}")
        return pd.DataFrame(json["seasons"])
    except:
        Exception("Error getting seasons.")

def get_matches(season_id: int) -> DataFrame:
    try:
        r = requests.get(_get_matches_endpoint(season_id))
        json = r.json()
        res = pd.DataFrame(json["events"])
        # filter out additional columns
        return res[["eventId", "homeId", "awayId", "tournamentId", "roundNr", "detailedScore", "stoppageTimeHT", "stoppageTimeFT", "hasOpta", "hasOptaMomentum", "statusType"]]
    except:
        Exception("Error while getting match details.")

def get_match_data(event_id: int) -> DataFrame:
    try:
        r = requests.get(_get_match_data_endpoint(event_id))
        json = r.json()
        spectators = json["spectators"]
        home_id = json["homeId"]
        away_id = json['awayId']
        homestat = pd.DataFrame(json["homeStats"],index=[home_id])
        awaystat = pd.DataFrame(json["awayStats"], index=[away_id])
        res = pd.concat([homestat, awaystat])
        res = res.assign(spectators=spectators)
        res = res.assign(eventId=event_id)
        res.set_index(["eventId"], append=True, inplace=True)
        return res
    except:
        Exception(f"Error getting match data for match: {event_id}")


# base end points
def _get_season_endpoint() -> str:
    return f"https://api.superliga.dk/tournaments/46?appName=superligadk&access_token={access_token}&env=production&locale=da"

def _get_matches_endpoint(season_id: int) -> str:
    return f"https://api.superliga.dk/events-v2?appName=dk.releaze.livecenter.spdk&access_token={access_token}&env=production&locale=da&seasonId={season_id}"

def _get_match_data_endpoint(match_id: int) -> str:
    return f"https://api.superliga.dk/opta-stats/events/{match_id}/teams?appName=superligadk&access_token={access_token}&env=production&locale=da"
