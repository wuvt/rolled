# EXPECTS as ctx: a list of *.xlsx files in source/
# RESULTS in: a pandas dataframe of all the files
import pandas, pathlib

def run(ctx, cfg):
    basepath = str(pathlib.Path(__file__).parent.absolute() / "source")
    sheets = [
        (pandas.read_excel(basepath+"/"+fn), fn)
        for fn in ctx
    ]

    for i in range(len(sheets)):
        # add a col for the original sheet it came from. poss. good to
        #  grab genre later, or something.
        sheets[i][0]["original_sheet"] = sheets[i][1]

        # re-name cols to something nice
        sheets[i] = sheets[i][0].rename(
        {
            k: v.lower().replace(" ", "_").split("/")[0]
            for k, v in 
            zip(sheets[i][0].columns, sheets[i][0].columns)
        },
        axis = 1)

    # merge all the sheets.
    return pandas.concat(sheets, ignore_index=True, sort=False)
