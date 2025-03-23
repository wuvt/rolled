# this is where you would mount the gdrive, fetch from an updated
#  download link, or whatever.
# EXPECTS as ctx: nothing
# RESULTS in: a list of fresh *.xlsx files in source/
import os, pathlib

def run(ctx, cfg):
    # i manually plopped <s>the americana sheet</s> all the sheets
    #  len gave us in there for prototyping.
    basepath = str(pathlib.Path(__file__).parent.absolute() / "source")
    return [
        sheet
        for sheet in os.listdir(basepath)
        if sheet.split(".")[-1] in ["xls", "xlsx"]
    ]