import argparse
import json

from src.stairlight import ResponseType, StairLight


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
            "Table name that stairlight searches for. "
            "It can be specified multiple times."
        ),
        # type=str,
        required=True,
        action="append",
    )
    parser.add_argument(
        "-c",
        "--config",
        help="Directory path contains stairlight configuration files.",
        type=str,
        default="./config/",
    )
    parser.add_argument(
        "-o",
        "--output",
        help="Output type",
        type=str,
        choices=[ResponseType.TABLE.value, ResponseType.FILE.value],
        default=ResponseType.TABLE.value,
    )
    parser.add_argument(
        "-v",
        "--verbose",
        help="Return verbose results",
        action="store_true",
        default=False,
    )
    parser.add_argument(
        "-r",
        "--recursive",
        help="Search recursive",
        action="store_true",
        default=False,
    )
    parser.add_argument(
        "--gcp_project",
        help="GCP project id",
        type=str,
        default=None,
    )
    return parser


def _create_parser():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()

    parser_up = subparsers.add_parser("up", help="Search upstream tables")
    parser_up.set_defaults(handler=command_up)
    parser_up = set_common_parser(parser_up)

    parser_down = subparsers.add_parser("down", help="Search downstream tables")
    parser_down.set_defaults(handler=command_down)
    parser_down = set_common_parser(parser_down)

    return parser


def main():
    result = None
    parser = _create_parser()
    args = parser.parse_args()

    stair_light = StairLight()
    stair_light.set()
    if not stair_light.maps:
        exit()

    if hasattr(args, "handler"):
        result = args.handler(stair_light, args)
    else:
        result = stair_light.maps

    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
