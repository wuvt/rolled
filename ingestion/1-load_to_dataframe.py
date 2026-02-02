# EXPECTS as ctx: a list of *.xlsx files in "/data/sheets/"
# RESULTS in: a pandas dataframe of all the files
import pandas, pathlib

def run(ctx, cfg):
    basepath = str(pathlib.Path(__file__).parent.absolute() / "/data/sheets/")
    sheets = [
        (sheet[1], fn)
        for fn in ctx
        for sheet in pandas.read_excel(basepath+"/"+fn, sheet_name=None).items()
    ]

    for i in range(len(sheets)):
        # add a col for the original sheet it came from. poss. good to
        #  grab genre later, or something.
        sheets[i][0]["original_sheet"] = sheets[i][1]

        # re-name cols to something nice
        sheets[i] = sheets[i][0].rename(
        {
            k: v.lower().rstrip().replace(" ", "_").split("/")[0]
            for k, v in 
            zip(sheets[i][0].columns, sheets[i][0].columns)
        },
        axis = 1)

    # merge all the sheets. drop NaN rows.
    #  the second bit is needed because some of len's xlsx files contain
    #  entirely empty sheets.
    df = pandas.concat(sheets, ignore_index=True, sort=False)
    df = df[df['artist_name'].notna()]
    df = df[df['artist_name'].str.strip() != '']
    df = df[df['artist_name'].str.lower() != 'nan']
 
    # reset the index to be sequential again
    df = df.reset_index(drop=True)
 
    # enforce types
    df["flac_copy"] = df["flac_copy"] == 'FLAC'
    df["missing"] = df["missing"].fillna('')
    df["missing_date"] = df["missing"]
    df["missing"] = df["missing"] != ""
    # remove the possibility of having the year as a float
    df["release_year"] = df["release_year"].fillna('')
    df["release_year"] = df["release_year"].astype(str).str.strip().str[:4]
    
    #Sanitize location
    df["location"] = df["location"].str.replace("[", "").str.replace("]", "")

    return df
