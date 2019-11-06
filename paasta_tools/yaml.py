import yaml

try:
    from yaml.cyaml import CSafeLoader as Loader  # type: ignore
except ImportError:  # pragma: no cover
    Loader = yaml.SafeLoader  # type: ignore

try:
    from yaml.cyaml import CSafeDumper as Dumper  # type: ignore
except ImportError:  # pragma: no cover
    Dumper = yaml.SafeDumper  # type: ignore


def dump(*args, **kwargs):
    kwargs["Dumper"] = Dumper
    return yaml.dump(*args, **kwargs)


def load(*args, **kwargs):
    kwargs["Loader"] = Loader
    return yaml.load(*args, **kwargs)
