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

def match_date(releases, date):
    matched = []
    for release in releases:
        try: #Some releases do not have the country specified.
            r_date = release["date"]
        except KeyError:
            r_date = ""
        if r_date[:4] == date:
            matched.append(release)
    return matched

def match_label(releases, label):
    matched = []
    for release in releases:
        try: #Some releases do not have the country specified.
            r_label = release["label"]
        except KeyError:
            r_label = ""
        if r_label == label:
            matched.append(release)
    return matched

def match_country(releases, countries):
    matched = []
    for release in releases:
        try: #Some releases do not have the country specified.
            country = release["country"]
        except KeyError:
            country = ""
        if country in countries:
            matched.append(release)
    return matched

def run(ctx, cfg):
    # leaving artists/misc off the proto
    res = {
        "albums":   ctx,
        "songs":    pandas.DataFrame(),
        "artists":  pandas.DataFrame(),
        "misc":     pandas.DataFrame(),
    }

    """
    populating albums with per-row MusicBrainz release-group MBIDs:
        - make a query to the search API (cached, /data/manifest.json)
            - rate-limited heavily. will take a while if not cached.
        - set if applicable. albums not found should have an mbid of ""
    """
    MBSEARCH_RELEASEGROUP_QUERY = "http://kissnightclub.wuvt.vt.edu:5000/ws/2/release-group/?query=album:{album_title}%20AND%20artist:{artist_name}&fmt=json&limit=1"
    rg_mbids = []
    start_time = time.time()
    count = 0

    # load in already-cached search responses
    RG_MANIFEST_PATH = "/data/rg_manifest.json"
    if not os.path.exists(RG_MANIFEST_PATH):
        print("creating fresh cache manifest...")
        with open(RG_MANIFEST_PATH, "w") as f:
            f.write("{}")
    with open(RG_MANIFEST_PATH) as f:
        manifest = orjson.loads(f.read())
    for index, row in res["albums"].iterrows(): 
        artist = str(row["artist_name"])
        title = str(row["album_title"])
        for attempt in range(5):
            try:
                cached_response = manifest.get(title+artist)
                response = requests.get(
                    MBSEARCH_RELEASEGROUP_QUERY.format(
                        album_title=title, 
                        artist_name=artist
                    ), 
                    headers=MBSEARCH_HEADERS
                ).json() if cached_response is None else cached_response
            except requests.exceptions.ConnectionError:
                print("got connectionerror. waiting a few seconds before trying again...")
                time.sleep(2)
                continue
            if "error" in response and "rate limit" in response["error"]:
                print("hit rate limit, cooling off...")
                time.sleep(1/2)
                continue

            if response.get("release-groups") is None:
                response["release-groups"] = []
            release_groups = response["release-groups"]
            mbid = release_groups[0]["id"] if len(release_groups) > 0 else ""
            rg_mbids.append(mbid)
            count += 1
            print("MBID:", mbid, time.time() - start_time, "At:", count, "Left:", len(res["albums"]) - count, "Fetched?:", "Y" if cached_response is None else "N")
            
            if cached_response is None:
                manifest[title+artist] = {
                    "release-groups": response["release-groups"]
                }
                with open(RG_MANIFEST_PATH, "wb") as f:
                    f.write(orjson.dumps(manifest, option=orjson.OPT_INDENT_2))

            break # we've succeeded -- break retry loop
    
    MBSEARCH_RELEASE = "http://kissnightclub.wuvt.vt.edu:5000/ws/2/release/?query=rgid:{mbid}&fmt=json"
    r_mbids = []
    start_time = time.time()
    count = 0
    
    # load in already-cached search responses
    R_MANIFEST_PATH = "/data/r_manifest.json"
    if not os.path.exists(R_MANIFEST_PATH):
        print("creating fresh cache manifest...")
        with open(R_MANIFEST_PATH, "w") as f:
            f.write("{}")
    with open(R_MANIFEST_PATH) as f:
        r_manifest = orjson.loads(f.read())
    for index, row in res["albums"].iterrows():
        if rg_mbids[index] == "":
            r_mbids.append("")
            continue
        
        mbid = rg_mbids[index]
        label = str(row["label"])
        date = str(row["release_year"])
        for attempt in range(5):
            try:
                cached_response = r_manifest.get(mbid)
                response = requests.get(
                    MBSEARCH_RELEASE.format(mbid=mbid),
                    headers={"User-Agent": "wuvt.vt.edu ROLLED/1"}
                ).json() if cached_response is None else cached_response
            except requests.exceptions.ConnectionError:
                print("got connectionerror. waiting a few seconds before trying again...")
                time.sleep(2)
                continue
            if "error" in response and "rate limit" in response["error"]:
                print("hit rate limit, cooling off...")
                time.sleep(1/2)
                continue

            if response.get("releases") is None:
                response["releases"] = []
            releases = response["releases"]
            if len(releases) == 0:
                mbid = ""
            elif len(releases) == 1:
                mbid = releases[0]["id"] 
            else:#this is ugly
                possible_rs = match_date(releases, date)
                if len(possible_rs) == 0:
                    possible_rs = match_label(releases, label)
                    if len(possible_rs) == 0:
                        possible_rs = match_country(releases, ["US", "XE"])
                    else:
                        temp_rs = match_country(possible_rs, ["US", "XE"])
                        if len(temp_rs) != 0:
                            possible_rs = temp_rs
                else:
                    temp_rs = match_label(possible_rs, label)
                    if len(temp_rs) == 0:
                        temp_rs = match_country(possible_rs, ["US", "XE"])
                        if len(temp_rs) != 0:
                            possible_rs = temp_rs
                    else:
                        temp_rs2 = match_country(temp_rs, ["US", "XE"])
                        if len(temp_rs2) == 0:
                            possible_rs = temp_rs
                        else:
                            possible_rs = temp_rs2

                mbid = possible_rs[0]["id"] if len(possible_rs) != 0 else releases[0]["id"] 
            r_mbids.append(mbid)
            count += 1
            print("R_MBID:", mbid, time.time() - start_time, "At:", count, "Left:", len(res["albums"]) - count, "Fetched?:", "Y" if cached_response is None else "N")
            
            if cached_response is None:
                r_manifest[mbid] = {
                    "releases": response["releases"]
                }
                with open(R_MANIFEST_PATH, "wb") as f:
                    f.write(orjson.dumps(r_manifest, option=orjson.OPT_INDENT_2))

            break # we've succeeded -- break retry loop
    res["albums"]["mbid"] = r_mbids


    # songs
    """
    - attempt to find correct-ish release for each release-group
        - prefer physical and US releases when possible
        - add to albums dataframe
    - grab the track listing, store as string in albums dataframe,
      create rows in songs dataframe for each track, and its parent
      release, release-group
    """
    
    MBLOOKUP_RELEASE = "http://kissnightclub.wuvt.vt.edu:5000/ws/2/release/{mbid}?inc=recordings&fmt=json"
    tracklists = []
    tracks_table = []
    for index, row in res["albums"].iterrows():
        if row["mbid"] == "":
            tracklists.append(" / ")
            continue
        
        release_data = requests.get(
            MBLOOKUP_RELEASE.format(mbid=str(row["mbid"])),
            headers={"User-Agent": "wuvt.vt.edu ROLLED/1"}
        ).json()
        try:
            tracks = [
                track["title"] for track in release_data["media"][0]["tracks"]
            ]
            
            trackrows = [
                {
                    "id": track["id"],
                    "title": track["title"],
                    "release": row["mbid"],
                    "release-group": rg_mbids[index]
                }
                for track in release_data["media"][0]["tracks"]
            ]
            tracks_table.extend(trackrows)

        except KeyError:
            tracks = []
        tracklists.append(" / ".join(tracks))
 
    res["albums"]["tracklist"] = tracklists
    
    trackrows_ids = []
    trackrows_titles = []
    trackrows_rs = []
    trackrows_rgs = []
    for trow in tracks_table:
        trackrows_ids.append(trow["id"])
        trackrows_titles.append(trow["title"])
        trackrows_rs.append(trow["release"])
        trackrows_rgs.append(trow["release-group"])
    
    res["songs"]["id"] = trackrows_ids
    res["songs"]["title"] = trackrows_titles
    res["songs"]["release"] = trackrows_rs
    res["songs"]["release-group"] = trackrows_rgs
    
    # artists
    res["artists"]["artist_name"] = ctx["artist_name"]


    # misc
    pass


    return res
