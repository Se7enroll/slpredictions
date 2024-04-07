import requests
import logging
import pandas as pd
from diskcache import Cache
from pandas import DataFrame

# Generate your own access token from superliga.dk
from .at import access_token

logging.basicConfig(
    format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)


def _persist_to_file(file_name="apicache.dat"):
    def decorator(original_func):
        try:
            cache = Cache(file_name)
        except (IOError, ValueError):
            cache = {}

        def cached_func(*args, **kwargs):
            key = original_func.__name__ + str(args) + str(kwargs)
            if key not in cache:
                cache[key] = original_func(*args, **kwargs)
            return cache[key]

        return cached_func

    return decorator


@_persist_to_file()
def get_seasons() -> DataFrame:
    try:
        r = requests.get(_get_season_endpoint())
        if r.status_code != 200:
            raise Exception("API dit not respond OK", r.status_code)
        json = r.json()
        logger.info("Seasons downloaded. Current season %s", json["seasonId"])
        return pd.DataFrame(json["seasons"])
    except Exception:
        logger.error("Error getting seasons.", exc_info=True)


@_persist_to_file()
def get_matches(season_id: int) -> DataFrame:
    try:
        r = requests.get(_get_matches_endpoint(season_id))
        if r.status_code != 200:
            raise Exception("API dit not respond OK", r.status_code)
        json = r.json()
        cols = [
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
        placeholder = pd.DataFrame(
            columns=cols
        )  # ensure all columns even if not present in dataset
        res = pd.concat([placeholder, pd.DataFrame(json["events"])])
        # filter out additional columns
        return res[cols]  # extra specification to select only wanted cols
    except Exception:
        logger.error("Error while getting match details.", exc_info=True)


@_persist_to_file()
def get_match_stats(event_id: int) -> DataFrame:
    try:
        r = requests.get(_get_match_data_endpoint(event_id))
        if r.status_code != 200:
            raise Exception("API dit not respond OK", r.status_code)
        json = r.json()
        spectators = json["spectators"]
        home_id = json["homeId"]
        away_id = json["awayId"]
        homestat = pd.DataFrame(json["homeStats"], index=[0])
        homestat = homestat.assign(teamId=home_id)
        awaystat = pd.DataFrame(json["awayStats"], index=[1])
        awaystat = awaystat.assign(teamId=away_id)
        res = pd.concat([homestat, awaystat])
        res = res.assign(spectators=spectators)
        res = res.assign(eventId=event_id)
        index_cols = ["eventId", "teamId"]
        value_cols = [col for col in res.columns if col not in index_cols]
        res = pd.melt(res, index_cols, value_cols)
        return res
    except Exception:
        logger.error("Error getting match data for match: %i", event_id, exc_info=True)


@_persist_to_file()
def get_xg_time(event_id: int) -> DataFrame:
    try:
        r = requests.get(_get_xg_time_endpoint(event_id))
        if r.status_code != 200:
            raise Exception("API dit not respond OK", r.status_code)
        json = r.json()
        home_id = json["homeId"]
        away_id = json["awayId"]
        cols = [
            "min",
            "sec",
            "x",
            "y",
            "period_id",
            "expectedGoalsValue",
            "situation",
            "type",
        ]
        homestat = pd.DataFrame(json["expectedGoalsData"]["home"])[cols]
        homestat = homestat.assign(teamId=home_id)
        awaystat = pd.DataFrame(json["expectedGoalsData"]["away"])[cols]
        awaystat = awaystat.assign(teamId=away_id)
        res = pd.concat([homestat, awaystat]).assign(eventId=event_id)
        return res
    except Exception:
        logger.error("Failed to get detailed xG for match id %s", event_id)

@_persist_to_file()
def get_momentum(event_id: int) -> DataFrame:
    try:
        r = requests.get(_get_momentum_endpoint(event_id))
        if r.status_code != 200:
            raise Exception("API dit not respond OK", r.status_code)
        json = r.json()
        res = pd.DataFrame(json["momentum"])
        posession_value = pd.json_normalize(res["scores"]).add_suffix("PosessionValue")
        minuttes_with_momentum = pd.json_normalize(
            res["minutesWithMomentum"]
        ).add_suffix("MinutesWithMomentum")
        res = res[["minute", "endRecordMin", "momentumValue"]]
        res = pd.concat([res, posession_value, minuttes_with_momentum], axis=1).assign(
            eventId=event_id
        )
        return res
    except Exception:
        logger.error("Failed to get momentum for match id %s", event_id)


# base end points
def _get_season_endpoint() -> str:
    return f"https://api.superliga.dk/tournaments/46?appName=superligadk&access_token={access_token}&env=production&locale=da"


def _get_matches_endpoint(season_id: int) -> str:
    return f"https://api.superliga.dk/events-v2?appName=dk.releaze.livecenter.spdk&access_token={access_token}&env=production&locale=da&seasonId={season_id}"


def _get_match_data_endpoint(match_id: int) -> str:
    return f"https://api.superliga.dk/opta-stats/events/{match_id}/teams?appName=superligadk&access_token={access_token}&env=production&locale=da"


def _get_xg_time_endpoint(match_id: int) -> str:
    return f"https://api.superliga.dk/opta-stats/event/{match_id}/detail-expected-goals?appName=superligadk&access_token={access_token}&env=production&locale=da"


def _get_momentum_endpoint(match_id: int) -> str:
    return f"https://api.superliga.dk/opta-stats/events/4192417/momentum?appName=superligadk&access_token={access_token}&env=production&locale=da"
