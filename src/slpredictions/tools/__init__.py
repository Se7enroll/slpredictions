from .data import get_seasons_json, get_matches, get_match_data


def main() -> int:
    print("Hello from slpredictions!")
    seasons = get_seasons_json()
    # remove before fligt!
    season_id = 20962
    event_id = 4449965
    print(seasons.describe())
    matches = get_matches(season_id)
    print(matches.describe())
    test = get_match_data(event_id)
    print(test.describe())
    return 0
