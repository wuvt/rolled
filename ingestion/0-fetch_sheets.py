# this is where you would mount the gdrive, fetch from an updated
#  download link, or whatever.
# EXPECTS as ctx: nothing
# RESULTS in: a list of fresh *.xlsx files in source/

def run(ctx, cfg):
    # i manually plopped the americana sheet len gave us in there for prototyping.
    return ["INVENTORY_Americana.xlsx"]