import re
import requests
import datetime
import logging
import lxml.html as lh
import pandas as pd
from pandas import DataFrame

logger = logging.getLogger(__name__)

# todo: fix downcasting...
pd.set_option("future.no_silent_downcasting", True)


def scrapeAllValues() -> DataFrame:
    currentYear = datetime.datetime.now().year
    data = scrapeValues(currentYear)
    i = 1
    while currentYear - 1 - i > 2000:
        data = pd.concat([data, scrapeValues(year=currentYear - i)], axis=0)
        i += 1
    return data


def scrapeValues(year: int = None) -> DataFrame:
    """Scrapes match data from url"""
    # Create a handle, page, to handle the contents of the website
    # Get contents and scrape all inside table row <tr></tr> in the main page
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36"
    }
    if year is None:
        url = r"https://www.transfermarkt.com/superligaen/startseite/wettbewerb/DK1/"
        year = datetime.datetime.now().year
    else:
        url = (
            r"https://www.transfermarkt.com/superligaen/startseite/wettbewerb/DK1/plus/?saison_id="
            + str(year)
        )

    page = requests.get(url, headers=headers)

    doc = lh.fromstring(page.content)
    rows = doc.xpath("//div/table/tbody/tr")

    # pattern matches team name + one word in front with single wild char, such as space
    # then matches a group of two numbers (squad size), then numbers with a . seperator and numbers (mean age)
    # then matches two groups after an euro sign with a million/thousands indicator trailing
    pattern = r"([a-zA-Zøæåö]*?.[[a-zA-Zøæåö]+).?(\d\d)(\d+\.*\d+)\€(\d+\.*\d*.)\€(\d+\.*\d*.)"

    teams = []
    for row in rows:
        text = row.text_content()
        # sanitize for regex
        text = text.replace("\n", "").replace("\t", "")
        # find all pattern matches - if none skip step
        if res := re.search(pattern, text):
            # add partial matches to list of list
            teams.append(
                [
                    res.group(1),  # team
                    res.group(2),  # num players
                    res.group(3),  # mean age
                    res.group(4),  # mean value
                    res.group(5),  # total value
                ]
            )

    # Convert to dataframe, add helper data if empty skip.
    data = pd.DataFrame(teams)
    if teams:
        data.columns = ["team", "numPlayers", "meanAge", "meanValue", "totalValue"]

        data["totalValue"] = _convertToNumber(data["totalValue"])
        data["meanValue"] = _convertToNumber(data["meanValue"])

        # clean mean age col
        data["meanAge"] = pd.to_numeric(data["meanAge"])

        # clean team names
        data["team"] = data["team"].str.replace("ö", "ø")
        data["team"] = data["team"].apply(_FixTeamName)

        data["season"] = str(year) + "/" + str(year + 1)

    else:
        logger.warning("No market values parsed for year %s.", year)
    return data


# convert 1m to 1.000.000 and 1k to 1.000
def _convertToNumber(df):
    df = df.replace(r"[km]+$", "", regex=True).astype(float) * df.str.extract(
        r"[\d\.]+([km]+)", expand=False
    ).fillna(1).replace(["k", "m"], [10**3, 10**6]).astype(int)
    return df


# Team names might be duplicated.
def _FixTeamName(row):
    if " " not in row:
        return row
    word = row.split()
    if word[0] == word[1]:
        return word[0]
    else:
        return word[0] + " " + word[1]
