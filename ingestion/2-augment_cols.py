# scrounges information for & adds new cols and tables
#  e.g. MBID, songs, etc.
# EXPECTS as ctx: a combined pandas dataframe
# RESULTS in: a dict of the form
#   {
#       "albums":   final derived albums dataframe
#       "songs":    final derived songs dataframe
#       "artists":  final derived artists dataframe
#       "misc":     final derived ids/liners/promos/etc dataframe
#   }
import pandas, requests, time, os
from sqlalchemy import create_engine
import db_utils

MBSEARCH_HEADERS = {
    'User-Agent': 'wuvt.vt.edu ROLLED/1',
    'Accept': 'application/json',
}

RG_MANIFEST_PATH = "/data/rg_manifest.json"
R_MANIFEST_PATH = "/data/r_manifest.json"
T_MANIFEST_PATH = "/data/t_manifest.json"

MBAPI_BASE = os.getenv("ING_MUSICBRAINZ_INSTANCE")

MBSEARCH_RELEASEGROUP_QUERY = MBAPI_BASE + "/ws/2/release-group/?query=releasegroup:{album_title}%20AND%20artist:{artist_name}&fmt=json&limit=1"
MBSEARCH_RELEASE = MBAPI_BASE + "/ws/2/release/?query=rgid:{mbid}&fmt=json"
MBLOOKUP_RELEASE = MBAPI_BASE + "/ws/2/release/{mbid}?inc=recordings&fmt=json"

QUERY_SLEEP = 0.5

def run(ctx, cfg):
    # leaving artists/misc off the proto
    res = {
        "albums":   ctx,
        "songs":    pandas.DataFrame(columns=[
            "id", "title", "artist", "release_id", "release-group_id"]),
        "artists":  pandas.DataFrame(),
        "misc":     pandas.DataFrame(),
    }

    #load cache from db
    pgc = cfg["postgres"]
    db = create_engine(
            f"postgresql://{pgc['username']}:{pgc['password']}@{pgc['host']}:{pgc['port']}/{pgc['database']}"
    )
    cache = {
        "albums":   db_utils.postgres_to_df(db, "albums"),
        "songs":    db_utils.postgres_to_df(db, "songs"),
        "artists":  db_utils.postgres_to_df(db, "artists"),
        "misc":     db_utils.postgres_to_df(db, "misc"),
    }
    skip_cache = {
        "albums":   cache["albums"] is None,
        "songs":    cache["songs"] is None,
        "artists":  cache["artists"] is None,
        "misc":     cache["misc"] is None,
    }

    # Create a empty release-group_id, release_id, and tracklist cols on the albums table
    if "release-group_id" not in res["albums"].columns:
        res["albums"]["release-group_id"] = pandas.Series([""]*len(res["albums"]), dtype="string")
    if "release_id" not in res["albums"].columns:
        res["albums"]["release_id"] = pandas.Series([""]*len(res["albums"]), dtype="string")
    res["albums"]["tracklist"] = pandas.Series(["/"]*len(res["albums"]), dtype="string")

    """
    populating albums with per-row MusicBrainz release-group MBIDs:
        - make a query to the search API (cached via postgres db)
            - rate-limited heavily. will take a while if not cached.
        - set if applicable. albums not found should have an mbid of ""
    """
    start_time = time.time()

    # load in already-cached search responses
    for index, row in res["albums"].iterrows():
        artist = str(row["artist_name"])
        title = str(row["album_title"])

        fetched = False

        if row["release-group_id"] != "":
            print("RG_ID: manual entry for", artist, title, ":",
                  row["release-group_id"])
            continue
        if not skip_cache["albums"]:
            cached_rgid = cache["albums"].at[index, "release-group_id"]
            res["albums"].at[index, "release-group_id"] = cached_rgid

        if skip_cache["albums"] or cached_rgid == "":
            fetched = True
            try:
                response = requests.get(
                    MBSEARCH_RELEASEGROUP_QUERY.format(
                        album_title=title,
                        # musicbrainz does not like commas on artist names
                        artist_name=artist.replace(',', '')
                    ),
                    headers=MBSEARCH_HEADERS
                ).json()
                time.sleep(QUERY_SLEEP)
            except:
                print("No release-group fetched for:", title, artist)
                continue

            if "release-groups" in response and len(response["release-groups"]) > 0:
                res["albums"].at[index, "release-group_id"] = response["release-groups"][0]["id"]

        print(
            "RG_ID:", res["albums"].at[index, "release-group_id"],
            time.time() - start_time,
            "At:", index, "Left:", len(res["albums"]) - index,
            "Fetched?:", "Y" if fetched else "N"
        )

    """
    - attempt to find correct-ish release for each release-group
        - prefer physical and US releases when possible
        - add to albums dataframe
    """
    start_time = time.time()
    # load in already-cached search responses
    for index, row in res["albums"].iterrows():
        rg_mbid = str(row["release-group_id"])
        label = str(row["label"])
        date = str(row["release_year"])
        countries = ["US", "XE"]

        fetched = False

        if rg_mbid == "":
            continue

        if row["release_id"] != "":
            print("R_ID: manual entry for", rg_mbid, ":", row["release_id"])
            continue

        if not skip_cache["albums"]:
            cached_rid = cache["albums"].at[index, "release_id"]
            res["albums"].at[index, "release_id"] = cached_rid

        if skip_cache["albums"] or cached_rid == "":
            fetched = True
            try:
                response = requests.get(
                    MBSEARCH_RELEASE.format(mbid=rg_mbid),
                    headers=MBSEARCH_HEADERS
                ).json()
                time.sleep(QUERY_SLEEP)
            except:
                print("No release fetched for:", rg_mbid)
                continue

            # If there are more than one release for the realease group
            # filter by date, label, and country.
            if "releases" in response and len(response["releases"]) > 0:
                releases = response["releases"]

                if len(releases) != 1:
                    releases = [
                        rel for rel in releases
                        if "date" in rel and rel["date"][:4] == date
                    ] or releases

                    if len(releases) != 1:
                        releases = [
                            rel for rel in releases
                            if "label" in rel and rel["label"] == label
                        ] or releases

                        if len(releases) != 1:
                            releases = [
                                rel for rel in releases
                                if "country" in rel and rel["country"] in countries
                            ] or releases

                res["albums"].at[index, "release_id"] = releases[0]["id"]

        print(
            "R_ID:", res["albums"].at[index, "release_id"],
            time.time() - start_time,
            "At:", index, "Left:", len(res["albums"]) - index,
            "Fetched?:", "Y" if fetched else "N"
        )

    # songs

    """
    - grab the track listing, store as string in albums dataframe,
      create rows in songs dataframe for each track, and its parent
      release, release-group
    """

    start_time = time.time()
    count = 0

    for index, row in res["albums"].iterrows():
        rg_mbid = str(row["release-group_id"])
        r_mbid = str(row["release_id"])

        fetched = False

        if r_mbid == "":
            count += 1
            continue

        if not skip_cache["albums"] and not skip_cache["songs"]:
            cached_tracklist = cache["albums"].at[index, "tracklist"]
            if cached_tracklist != "":
                res["songs"] = pandas.concat([
                    res["songs"],
                    cache["songs"][
                        cache["songs"]['release_id'] == r_mbid]
                    ], ignore_index=True)
                res["albums"].at[index, "tracklist"] = cached_tracklist

        if skip_cache["albums"] or skip_cache["songs"] or cached_tracklist == "":
            fetched = True
            try:
                response = requests.get(
                    MBLOOKUP_RELEASE.format(mbid=r_mbid),
                    headers=MBSEARCH_HEADERS
                ).json()
                time.sleep(QUERY_SLEEP)
            except:
                count += 1
                continue

            tracks = []
            try:
                for track in response["media"][0]["tracks"]:
                    tracks.append(track["title"])
                    res["songs"].loc[len(res["songs"])] = {
                        "id": track["id"],
                        "title": track["title"],
                        "artist": row["artist_name"],
                        "release_id": row["release_id"],
                        "release-group_id": rg_mbid
                    }
            except:
                count += 1
                continue

            res["albums"].at[index, "tracklist"] = " / ".join(tracks)

        count += 1
        print(
            "TRACKLIST:", res["albums"].at[index, "tracklist"],
            time.time() - start_time,
            "At:", count, "Left:", len(res["albums"]) - count,
            "Fetched?:", "Y" if fetched else "N"
        )

    # artists
    res["artists"]["artist_name"] = ctx["artist_name"]

    # TODO misc

    return res
