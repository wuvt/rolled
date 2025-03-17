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
import pandas, requests, time, json, os

MBSEARCH_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Accept': 'application/json',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
    'Cache-Control': 'no-cache',
    'Pragma': 'no-cache',
}

MBSEARCH_RELEASEGROUP_QUERY = "https://musicbrainz.org/ws/2/release-group/?query=album:{album_title}&artist:{artist_name}&fmt=json&limit=1"

def run(ctx, cfg):
    # leaving songs/artists/misc off the proto
    res = {
        "albums":   ctx,
        "songs":    pandas.DataFrame(),
        "artists":  pandas.DataFrame(),
        "misc":     pandas.DataFrame(),
    }

    # albums


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
        manifest = json.loads(f.read())

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

            mbid = response["release-groups"][0]["id"]
            mbids.append(mbid)
            count += 1
            print("MBID:", mbid, time.time() - start_time, "At:", count, "Left:", len(res["albums"]) - count, "Fetched?:", "Y" if cached_response is None else "N")
            
            if cached_response is None:
                manifest[title+artist] = response
                time.sleep(1/4) #API ratelimit of ?
                with open(RG_MANIFEST_PATH, "w") as f:
                    f.write(json.dumps(manifest, indent=1))

            break # we've succeeded -- break retry loop

    res["albums"]["mbid"] = mbids


    # songs
    pass


    # artists
    res["artists"]["artist_name"] = ctx["artist_name"]


    # misc
    pass


    return res
