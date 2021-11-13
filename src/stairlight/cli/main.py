import argparse
import json
from typing import Callable

from src.stairlight import ResponseType, StairLight


def command_check(stair_light, args):
    result = stair_light.check()
    if result:
        exit(
            f"'{result}' has created.\n"
            "Please map undefined tables and parameters, "
            "and append to your latest file."
        )


def command_up(stair_light, args):
    return execute_up_or_down(stair_light.up, args)


def command_down(stair_light, args):
    return execute_up_or_down(stair_light.down, args)


def execute_up_or_down(up_or_down: Callable, args):
    results = []
    for table_name in args.table:
        result = up_or_down(
            table_name=table_name,
            recursive=args.recursive,
            verbose=args.verbose,
            response_type=args.output,
        )
        if len(args.table) > 1:
            results.append(result)
        else:
            return result
    return results


def set_up_down_parser(parser):
    parser.add_argument(
        "-c",
        "--config",
        help="directory path contains stairlight configuration files.",
        type=str,
        default=".",
    )
    parser.add_argument(
        "-t",
        "--table",
        help=(
            "table name that stairlight searches for, "
            "can be specified multiple times."
        ),
        required=True,
        action="append",
    )
    parser.add_argument(
        "-o",
        "--output",
        help="output type",
        type=str,
        choices=[ResponseType.TABLE.value, ResponseType.FILE.value],
        default=ResponseType.TABLE.value,
    )
    parser.add_argument(
        "-v",
        "--verbose",
        help="return verbose results",
        action="store_true",
        default=False,
    )
    parser.add_argument(
        "-r",
        "--recursive",
        help="search recursive",
        action="store_true",
        default=False,
    )
    return parser


def _create_parser():
    description = (
        "A table-level data lineage tool, "
        "detects table dependencies from `CREATE TABLE AS SELECT` SQL files. "
        "Without positional arguments, "
        "return a table dependency map as JSON format."
    )
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument(
        "-c",
        "--config",
        help="directory path contains stairlight configuration files.",
        type=str,
        default=".",
    )

    subparsers = parser.add_subparsers()

    parser_check = subparsers.add_parser(
        "check", help="create a new configuration file about undefined mappings."
    )
    parser_check.set_defaults(handler=command_check)

    parser_up = subparsers.add_parser(
        "up", help="return upstream ( table | SQL file ) list"
    )
    parser_up.set_defaults(handler=command_up)
    parser_up = set_up_down_parser(parser_up)

    parser_down = subparsers.add_parser(
        "down", help="return downstream ( table | SQL file ) list"
    )
    parser_down.set_defaults(handler=command_down)
    parser_down = set_up_down_parser(parser_down)

    return parser


def main():
    parser = _create_parser()
    args = parser.parse_args()
    stair_light = StairLight(config_path=args.config)
    if not stair_light.has_strl_config():
        exit(f"'{args.config}/stairlight.y(a)ml' is not found.")

    result = None
    if hasattr(args, "handler"):
        result = args.handler(stair_light, args)
    else:
        result = stair_light.map

    if result:
        print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
