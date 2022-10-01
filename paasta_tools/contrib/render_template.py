#!/usr/bin/env python3.7
import argparse
import os
import re

import yaml


def replace(s, values):
    s = re.sub(
        r"<%(.*?)%>",
        lambda x: values.get(
            x.group(0).replace("<%", "").replace("%>", ""), x.group(0)
        ),
        s,
    )
    return re.sub(
        r"\$\((.*?)\)",
        lambda x: os.environ.get(
            x.group(0).replace("$(", "").replace(")", ""), x.group(0)
        ),
        s,
    )


def render_file(src, dst, values):
    basename = os.path.basename(src)
    new_name = replace(basename, values)
    with open(f"{dst}/{new_name}", "w") as new:
        with open(f"{src}", "r") as old:
            new.write(replace(old.read(), values))


def render(src, dst, values={}, exclude={}):
    if os.path.isfile(src):
        render_file(src, dst, values)
        return
    for f in os.scandir(src):
        if f.name.startswith(".") or f.path in exclude:
            continue
        if os.path.isfile(f.path):
            render_file(f.path, dst, values)
        else:
            new_dst = replace(f"{dst}/{f.name}", values)
            try:
                os.makedirs(new_dst, exist_ok=True)
            except OSError as e:
                if e.errno != os.errno.EEXIST:
                    raise
            render(f.path, new_dst, values, exclude)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Replaces all <%%> in all files in src with values provided, and writes the results to dst folder. $() is reserved for environment variables. File/dir that starts with . are ignored"
    )
    parser.add_argument(
        "-s",
        "--src",
        type=str,
        dest="src",
        required=True,
        help="src can be either a valid folder of directory. Note that src directory itself is not rendered. .* files/dirs are ignored.",
    )
    parser.add_argument(
        "-d",
        "--dst",
        type=str,
        dest="dst",
        required=True,
        help="Dst needs to be a directory.",
    )
    parser.add_argument(
        "-v",
        "--values",
        type=str,
        dest="values",
        default=None,
        help="values need to be valid file if provided",
    )
    args = parser.parse_args()
    return args


def render_values(src: str, dst: str, values: str) -> None:
    if values is not None:
        values = os.path.abspath(values)
    # Validate src and values. Dst needs to be a directory. src can be either a valid folder of directory. values need to be valid file if provided.
    if not os.path.exists(src):
        raise Exception("src path is invalid")
    if not os.path.exists(dst) or not os.path.isdir(dst):
        raise Exception("dst path is invalid")
    if values and (not os.path.exists(values) or not os.path.isfile(values)):
        raise Exception("values path is invalid")
    # Lookup for values.yaml in src folder if values is not provided
    if os.path.isdir(src) and values is None and os.path.exists(f"{src}/values.yaml"):
        values = f"{src}/values.yaml"
    config_dict = {}
    if values is not None:
        with open(values) as f:
            config_dict = yaml.safe_load(f)
    # Replace environment variables in values.yaml with environment variables
    for k, v in config_dict.items():
        config_dict[k] = re.sub(
            r"\$\((.*?)\)",
            lambda x: os.environ.get(
                x.group(0).replace("$(", "").replace(")", ""), x.group(0)
            ),
            v,
        )
    render(src, dst, config_dict, {values})


def main():
    args = parse_args()
    src = os.path.abspath(args.src)
    dst = os.path.abspath(args.dst)
    values = args.values

    render_values(src, dst, values)


if __name__ == "__main__":
    main()
