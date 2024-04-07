# slpredictions

This repository contains a collection of tools to extract data from the superliga.dk api.

## sl.db

A duckdb database with the following tables and data:

**Seasons**
Table with seasonId and season name/year.

**Matches**
Table with all matches per season, with an eventId linking the following match data to the specific match up, the teamId and names, the round nr, a json struct with som match details including score, stoppage time, and indicators for extended opta data and lastly a status for wether the match has finnished.

**MatchStats**
Table with teamwise detailed opta stats.

**MatchXG**
Table with xG events for the match on a min, sec level as well as shot positions, situation and outcome type.

**MatchMomentum**
Table with momentum, i.e. team difference in possesion value on a minute basis.

## sldk_api

A collection of functions for accessing the somewhat hidden superliga.dk API. In order to use these functions a `at.py` file with the variable `access_token` set to your personal acess token for the API. *hint: you can obtain this token by inspecting a page with match data on superliga.dk*. 