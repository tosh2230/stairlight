import argparse
import json

from src.stairlight import ResponseType, StairLight


def command_init(stair_light, args):
    result = stair_light.init()
    if result:
        exit(f"'{result}' has created.\nPlease map your tables and parameters.")


def command_up(stair_light, args):
    if len(args.table) > 1:
        results = []
        for table_name in args.table:
            result = stair_light.up(
                table_name=table_name,
                recursive=args.recursive,
                verbose=args.verbose,
                response_type=args.output,
            )
            results.append(result)
        return results
    else:
        return stair_light.up(
            table_name=args.table[0],
            recursive=args.recursive,
            verbose=args.verbose,
            response_type=args.output,
        )


def command_down(stair_light, args):
    if len(args.table) > 1:
        results = []
        for table_name in args.table:
            result = stair_light.down(
                table_name=table_name,
                recursive=args.recursive,
                verbose=args.verbose,
                response_type=args.output,
            )
            results.append(result)
        return results
    else:
        return stair_light.down(
            table_name=args.table[0],
            recursive=args.recursive,
            verbose=args.verbose,
            response_type=args.output,
        )


def set_common_parser(parser):
    parser.add_argument(
        "-t",
        "--table",
        help=(
            "table name that stairlight searches for, "
            "can be specified multiple times."
        ),
        # type=str,
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
    parser.add_argument(
        "--gcp_project",
        help="Google Cloud Platform project id",
        type=str,
        default=None,
    )
    return parser


def _create_parser():
    description = (
        "A table-level data lineage tool, "
        "detects table dependencies from `CREATE TABLE AS SELECT` SQL files."
        "Without positional arguments, "
        "return a table dependency map as JSON format."
    )
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument(
        "-c",
        "--config",
        help="directory path contains stairlight configuration files.",
        type=str,
        default="./config/",
    )

    subparsers = parser.add_subparsers()

    parser_init = subparsers.add_parser(
        "init", help="create a new mapping configuration file"
    )
    parser_init.set_defaults(handler=command_init)

    parser_up = subparsers.add_parser("up", help="return upstream table list(s)")
    parser_up.set_defaults(handler=command_up)
    parser_up = set_common_parser(parser_up)

    parser_down = subparsers.add_parser("down", help="return downstream table list(s)")
    parser_down.set_defaults(handler=command_down)
    parser_down = set_common_parser(parser_down)

    return parser


def main():
    parser = _create_parser()
    args = parser.parse_args()
    stair_light = StairLight(config_path=args.config)
    if not stair_light.has_strl_config():
        exit(f"'{args.config}stairlight.y(a)ml' is not found.")

    result = None
    if hasattr(args, "handler"):
        result = args.handler(stair_light, args)
    else:
        result = stair_light.maps

    if result:
        print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
