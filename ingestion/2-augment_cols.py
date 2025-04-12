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
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Accept': 'application/json',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
    'Cache-Control': 'no-cache',
    'Pragma': 'no-cache',
}

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
    MBSEARCH_RELEASEGROUP_QUERY = "https://musicbrainz.org/ws/2/release-group/?query=album:{album_title} AND artist:{artist_name}&fmt=json&limit=1"
    mbids = []
    start_time = time.time()
    count = 0

    # load in already-cached search responses
    RG_MANIFEST_PATH = "/data/manifest.json"
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
            mbids.append(mbid)
            count += 1
            print("MBID:", mbid, time.time() - start_time, "At:", count, "Left:", len(res["albums"]) - count, "Fetched?:", "Y" if cached_response is None else "N")
            
            if cached_response is None:
                manifest[title+artist] = {
                    "release-groups": response["release-groups"]
                }
                time.sleep(1/4) #API ratelimit of ?
                with open(RG_MANIFEST_PATH, "wb") as f:
                    f.write(orjson.dumps(manifest, option=orjson.OPT_INDENT_2))

            break # we've succeeded -- break retry loop
    res["albums"]["mbid"] = mbids


    # songs
    """
    - attempt to find correct-ish release for each release-group
        - prefer physical and US releases when possible
        - add to albums dataframe
    - grab the track listing, store as string in albums dataframe,
      create rows in songs dataframe for each track, and its parent
      release, release-group
    """
    """
    omitted until we 
    MBLOOKUP_RELEASEGROUP = "https://musicbrainz.org/ws/2/release-group/{mbid}?inc=releases&fmt=json"
    MBLOOKUP_RELEASE = "https://musicbrainz.org/ws/2/release/{mbid}?inc=recordings&fmt=json"
    tracklists = []
    tracks_table = []
    for index, row in res["albums"].iterrows():
        if row["mbid"] == "": continue
        releases = requests.get(
            MBLOOKUP_RELEASEGROUP.format(mbid=str(row["mbid"])),
            headers={"User-Agent": "wuvt.vt.edu ROLLED/1"}
        ).json()
        time.sleep(1)

        print("pulling releases for song #", index)
        if len(releases["releases"]) < 1:
            tracklists.append("")
            continue
        
        freleases = [
            rel 
            for rel in releases["releases"]
            if rel.get("country") is not None and 
                rel["country"] in ["US", "XW"]
        ]
        releases = freleases if len(freleases) > 0 else releases["releases"]
        
        for rel in releases:
            release_data = requests.get(
                MBLOOKUP_RELEASE.format(mbid=str(rel["id"])),
                headers={"User-Agent": "wuvt.vt.edu ROLLED/1"}
            ).json()
            time.sleep(1)
            if release_data["media"][0].get("format") is not None and not "Digital" in release_data["media"][0]["format"]:
                tracks = [
                    track["title"]
                    for track in release_data["media"][0]["tracks"]
                ]
                tracklists.append(" / ".join(tracks))

                trackrows = [
                    {
                        "id": track["id"],
                        "title": track["title"],
                        "release": rel["id"],
                        "release-group": row["mbid"]
                    }
                    for track in release_data["media"][0]["tracks"]
                ]
                tracks_table.extend(trackrows)
                break
    res["albums"]["tracklist"] = tracklists

    for colname in tracks_table.keys():
        res["songs"][colname] = [
            i["colname"]
            for i in tracks_table
        ]
    """

    # artists
    res["artists"]["artist_name"] = ctx["artist_name"]


    # misc
    pass


    return res
