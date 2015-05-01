import shutil


def after_scenario(context, scenario):
    if getattr(context, "tmpdir"):
        shutil.rmtree(context.tmpdir)
