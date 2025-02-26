import yaml

try:
    from yaml import CSafeLoader as Loader
except ImportError:  # pragma: no cover
    from yaml import SafeLoader as Loader  # type: ignore


def safe_load_yaml(stream):
    return yaml.load(stream, Loader=Loader)
