import yaml

try:
    from yaml import CSafeLoader as Loader
except ImportError:  # pragma: no cover
    from yaml import SafeLoader as Loader  # type: ignore


def safe_load_yaml(stream):
    """
    yaml.safe_load() equivalent that will use a CSafeLoader if available.

    This provides a significant speedup, but we allow falling back to the pure Python
    codepath as we'd rather have things be slower than crash outright.
    """
    return yaml.load(stream, Loader=Loader)
