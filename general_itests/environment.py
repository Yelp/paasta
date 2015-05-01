import shutil


def after_scenario(context, scenario):
    if getattr(context, "tmpdir", None):
            shutil.rmtree(context.tmpdir)
