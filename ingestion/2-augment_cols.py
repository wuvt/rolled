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
import pandas

def run(ctx, cfg):
    # leaving songs/artists/misc off the proto
    res = {
        "albums":   ctx,
        "songs":    pandas.DataFrame(),
        "artists":  pandas.DataFrame(),
        "misc":     pandas.DataFrame(),
    }

    # albums
    res["albums"]["mbid"] = 0


    # songs
    pass


    # artists
    res["artists"]["artist_name"] = ctx["artist_name"]


    # misc
    pass


    return res