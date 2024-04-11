import logging
import pandas as pd
from ayjay import AyJay
from pandas import DataFrame

# Generate your own access token from superliga.dk
from .at import access_token

logging.basicConfig(
    format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)


class SLDK:
    def __init__(self):
        self.ayjay = AyJay(cache_path="./")
        self._base_url = "https://api.superliga.dk/"
        self._base_params = {
            "appName": "superligadk",
            "access_token": access_token,
            "env": "production",
            "locale": "da",
        }

    def get_seasons(self) -> DataFrame:
        try:
            endpoint = (
                self._base_url + "tournaments/46"
            )  # 46 denotes the sport football.
            res = self.ayjay.get(endpoint, self._base_params)
            print(res)
            logger.info("Seasons downloaded. Current season %s", res["seasonId"])
            return pd.DataFrame(res["seasons"])
        except Exception:
            logger.error("Error getting seasons.", exc_info=True)

    def get_matches(self, season_id: int) -> DataFrame:
        try:
            endpoint = self._base_url + "/events-v2"
            params = self._base_params.update(
                {"seasonId": season_id, "appName": "dk.releaze.livecenter.spdk"}
            )
            json = self.ayjay.get(endpoint, params)
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

    def get_match_stats(self, event_id: int) -> DataFrame:
        try:
            endpoint = self._base_url + f"/opta-stats/events/{event_id}/teams"
            json = self.ayjay.get(endpoint, self._base_params)
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
            logger.error(
                "Error getting match data for match: %i", event_id, exc_info=True
            )

    def get_xg_time(self, event_id: int) -> DataFrame:
        try:
            endpoint = (
                self._base_url + f"/opta-stats/event/{event_id}/detail-expected-goals"
            )
            json = self.ayjay.get(endpoint, self._base_params)
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

    def get_momentum(self, event_id: int) -> DataFrame:
        try:
            endpoint = f"/opta-stats/events/{event_id}/momentum"
            json = self.ayjay.get(endpoint, self._base_params)
            res = pd.DataFrame(json["momentum"])
            posession_value = pd.json_normalize(res["scores"]).add_suffix(
                "PosessionValue"
            )
            minuttes_with_momentum = pd.json_normalize(
                res["minutesWithMomentum"]
            ).add_suffix("MinutesWithMomentum")
            res = res[["minute", "endRecordMin", "momentumValue"]]
            res = pd.concat(
                [res, posession_value, minuttes_with_momentum], axis=1
            ).assign(eventId=event_id)
            return res
        except Exception:
            logger.error("Failed to get momentum for match id %s", event_id)
