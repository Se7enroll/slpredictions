import requests
import pandas as pd
from diskcache import Cache
from pandas import DataFrame

# Generate your own access token from superliga.dk
from .at import access_token


def persist_to_file(file_name="apicache.dat"):
    def decorator(original_func):
        try:
            cache = Cache(file_name)
        except (IOError, ValueError):
            cache = {}

        def cached_func(*args, **kwargs):
            key = str(args) + str(kwargs)
            if key not in cache:
                cache[key] = original_func(*args, **kwargs)
            return cache[key]

        return cached_func

    return decorator


@persist_to_file()
def get_seasons() -> DataFrame:
    try:
        r = requests.get(_get_season_endpoint())
        json = r.json()
        print(f"Seasons downloaded. Current season {json['seasonId']}")
        return pd.DataFrame(json["seasons"])
    except Exception as e:
        Exception("Error getting seasons.", e)


@persist_to_file()
def get_matches(season_id: int) -> DataFrame:
    try:
        r = requests.get(_get_matches_endpoint(season_id))
        json = r.json()
        res = pd.DataFrame(json["events"])
        # filter out additional columns
        return res[
            [
                "tournamentId",
                "roundNr",
                "eventId",
                "homeId",
                "awayId",
                "homeName",
                "awayName",
                "detailedScore",
                "stoppageTimeHT",
                "stoppageTimeFT",
                "hasOpta",
                "hasOptaMomentum",
                "statusType",
            ]
        ]
    except Exception as e:
        Exception("Error while getting match details.", e)


@persist_to_file()
def get_match_data(event_id: int) -> DataFrame:
    try:
        r = requests.get(_get_match_data_endpoint(event_id))
        json = r.json()
        spectators = json["spectators"]
        home_id = json["homeId"]
        away_id = json["awayId"]
        homestat = pd.DataFrame(json["homeStats"], index=[home_id])
        awaystat = pd.DataFrame(json["awayStats"], index=[away_id])
        res = pd.concat([homestat, awaystat])
        res = res.assign(spectators=spectators)
        res = res.assign(eventId=event_id)
        res.set_index(["eventId"], append=True, inplace=True)
        return res
    except Exception as e:
        Exception(f"Error getting match data for match: {event_id}", e)


# base end points
def _get_season_endpoint() -> str:
    return f"https://api.superliga.dk/tournaments/46?appName=superligadk&access_token={access_token}&env=production&locale=da"


def _get_matches_endpoint(season_id: int) -> str:
    return f"https://api.superliga.dk/events-v2?appName=dk.releaze.livecenter.spdk&access_token={access_token}&env=production&locale=da&seasonId={season_id}"


def _get_match_data_endpoint(match_id: int) -> str:
    return f"https://api.superliga.dk/opta-stats/events/{match_id}/teams?appName=superligadk&access_token={access_token}&env=production&locale=da"
