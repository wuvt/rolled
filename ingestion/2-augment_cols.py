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
import pandas, requests, time, orjson, os

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

"""
`cache` here refers to the name of the .json cache used.
"""
def cached_mb_get(endpoint, cache):
    cached_response = manifest.get(title+artist)
    response = requests.get(
        MBSEARCH_RELEASEGROUP_QUERY.format(
            album_title=title,
            artist_name=artist
        ), 
        headers=MBSEARCH_HEADERS
    ).json() if cached_response is None else cached_response

# Filter releases by date
def match_date(releases, date):
    matched = []
    for release in releases:
        if "date" not in release:
            continue
        if release["date"][:4] == date:
            matched.append(release)

    if len(matched) == 0:
        matched = releases

    return matched

# Filter releases by label
def match_label(releases, label):
    matched = []
    for release in releases:
        if "label" not in release:
            continue
        if release["label"] == label:
            matched.append(release)

    if len(matched) == 0:
        matched = releases

    return matched

# Filter releases by country
def match_country(releases, countries):
    matched = []
    for release in releases:
        if "country" not in release:
            continue
        if release["country"] in countries:
            matched.append(release)

    if len(matched) == 0:
        matched = releases

    return matched

def init_manifest(path):
    if not os.path.exists(path):
        print("creating fresh cache manifest...")
        with open(path, "w") as f:
            f.write("{}")
    with open(path) as f:
        manifest = orjson.loads(f.read())
    return manifest


def run(ctx, cfg):
    # leaving artists/misc off the proto
    res = {
        "albums":   ctx,
        "songs":    pandas.DataFrame(columns=["id", "title", "artist", "release_id", "release-group_id"]),
        "artists":  pandas.DataFrame(),
        "misc":     pandas.DataFrame(),
    }

    # Create a empty release-group_id, release_id, and tracklist cols on the albums table
    if "release-group_id" not in res["albums"].columns:
        res["albums"]["release-group_id"] = pandas.Series([""]*len(res["albums"]), dtype="string")
    if "release_id" not in res["albums"].columns:
        res["albums"]["release_id"] = pandas.Series([""]*len(res["albums"]), dtype="string")
    res["albums"]["tracklist"] = pandas.Series(["/"]*len(res["albums"]), dtype="string")

    """
    populating albums with per-row MusicBrainz release-group MBIDs:
        - make a query to the search API (cached, /data/xxx_manifest.json)
            - rate-limited heavily. will take a while if not cached.
        - set if applicable. albums not found should have an mbid of ""
    """
    start_time = time.time()

    # load in already-cached search responses
    manifest = init_manifest(RG_MANIFEST_PATH)
    for index, row in res["albums"].iterrows(): 
        artist = str(row["artist_name"])
        title = str(row["album_title"])
        if row["release-group_id"] != "":
            print("RG_ID: manual entry for", artist, title, ":",
                  row["release-group_id"])
            continue
        
        cached_response = manifest.get(title+artist)
        try:
            response = requests.get(
                MBSEARCH_RELEASEGROUP_QUERY.format(
                    album_title=title, 
                    # musicbrainz does not like commas on artist names
                    artist_name=artist.replace(',', '')
                ), 
                headers=MBSEARCH_HEADERS
            ).json() if cached_response is None else cached_response
            if cached_response is None: time.sleep(QUERY_SLEEP)
        except:
            print("No release-group fetched for:", title, artist)
            continue

        if "release-groups" in response and len(response["release-groups"]) > 0:
            res["albums"].at[index, "release-group_id"] = response["release-groups"][0]["id"]

            if cached_response is None:
                manifest[title+artist] = {
                    "release-groups": response["release-groups"]
                }
                with open(RG_MANIFEST_PATH, "wb") as f:
                    f.write(orjson.dumps(manifest, option=orjson.OPT_INDENT_2))

        print(
            "RG_ID:", res["albums"].at[index, "release-group_id"],
            time.time() - start_time,
            "At:", index, "Left:", len(res["albums"]) - index,
            "Fetched?:", "Y" if cached_response is None else "N"
        )        

    start_time = time.time()

    """
    - attempt to find correct-ish release for each release-group
        - prefer physical and US releases when possible
        - add to albums dataframe
    """
    # load in already-cached search responses
    r_manifest = init_manifest(R_MANIFEST_PATH)
    for index, row in res["albums"].iterrows():
        rg_mbid = str(row["release-group_id"])
        label = str(row["label"])
        date = str(row["release_year"])

        if rg_mbid == "":
            continue
        
        if row["release_id"] != "":
            print("R_ID: manual entry for", rg_mbid, ":", row["release_id"])
            continue

        cached_response = r_manifest.get(rg_mbid)
        try:
            response = requests.get(
                MBSEARCH_RELEASE.format(mbid=rg_mbid),
                headers=MBSEARCH_HEADERS
            ).json() if cached_response is None else cached_response
            if cached_response is None: time.sleep(QUERY_SLEEP)
        except:
            print("No release fetched for:", rg_mbid)
            continue

        # If there are more than one release for the realease group
        # filter by date, label, and country.
        if "releases" in response and len(response["releases"]) > 0:
            releases = response["releases"]
            if len(releases) != 1:
                releases = match_date(releases, date)
                if len(releases) != 1:
                    releases = match_label(releases, label)
                    if len(releases) != 1:
                        releases = match_country(releases, ["US", "XE"])

            res["albums"].at[index, "release_id"] = releases[0]["id"]

            if cached_response is None:
                r_manifest[rg_mbid] = {
                    "releases": response["releases"]
                }
                with open(R_MANIFEST_PATH, "wb") as f:
                    f.write(orjson.dumps(r_manifest, option=orjson.OPT_INDENT_2))

        print(
            "R_ID:", res["albums"].at[index, "release_id"],
            time.time() - start_time,
            "At:", index, "Left:", len(res["albums"]) - index,
            "Fetched?:", "Y" if cached_response is None else "N"
        )

    # songs

    """
    - grab the track listing, store as string in albums dataframe,
      create rows in songs dataframe for each track, and its parent
      release, release-group
    """

    start_time = time.time()
    count = 0

    t_manifest = init_manifest(T_MANIFEST_PATH)
    for index, row in res["albums"].iterrows():
        rg_mbid = str(row["release_id"])
        r_mbid = str(row["release_id"])

        if r_mbid == "":
            count += 1
            continue

        cached_response = t_manifest.get(r_mbid) 
        try:
            response = requests.get(
                MBLOOKUP_RELEASE.format(mbid=r_mbid),
                headers=MBSEARCH_HEADERS
            ).json() if cached_response is None else cached_response
            if cached_response is None: time.sleep(QUERY_SLEEP)
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

        if cached_response is None:
            t_manifest[r_mbid] = {
                "media": response["media"]
            }
            with open(T_MANIFEST_PATH, "wb") as f:
                f.write(orjson.dumps(t_manifest, option=orjson.OPT_INDENT_2))

        count += 1
        print(
            "TRACKLIST:", res["albums"].at[index, "tracklist"],
            time.time() - start_time,
            "At:", count, "Left:", len(res["albums"]) - count,
            "Fetched?:", "Y" if cached_response is None else "N"
        )

    # artists
    res["artists"]["artist_name"] = ctx["artist_name"]

    # misc
    pass

    return res
