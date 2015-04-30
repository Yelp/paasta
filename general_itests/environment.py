# import shutil


def before_scenario(context, scenario):
    # I can't do context.get("tmpdir") so I'll set it here to ensure it exists.
    # If a test sets it to something (because it creates a tmpdir), we'll clean
    # it up in after_scenario().
    context.tmpdir = None

def after_scenario(context, scenario):
    if context.tmpdir:
        print "### WOULD RMTREE %s" % context.tmpdir
        # shutil.rmtree(context.tmpdir)
