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
import pandas, requests, time

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
    for index, row in res["albums"].iterrows(): 
        artist_name = row["artist_name"]
        album_title = row["album_title"]
        headers = { #generic ai generated header (not final)
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'application/json',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Cache-Control': 'no-cache',
            'Pragma': 'no-cache',
        }
        data = requests.get(f"https://musicbrainz.org/ws/2/release-group/?query=album:{album_title}&artist:{artist_name}&fmt=json&limit=1", headers=headers)
        mbid = data.json()["release-groups"][0]["id"]
        mbids.append(mbid)
        count += 1
        print("MBID:", mbid, time.time() - start_time, "At:", count, "Left:", len(res["albums"]) - count)
        time.sleep(1/5) #API ratelimit of ?
    
    res["albums"]["mbid"] = mbids


    # songs
    pass


    # artists
    res["artists"]["artist_name"] = ctx["artist_name"]


    # misc
    pass


    return res
