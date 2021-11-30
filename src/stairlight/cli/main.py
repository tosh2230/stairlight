import argparse
import json
from typing import Callable, Union

from stairlight import ResponseType, StairLight


def command_init(stairlight: StairLight, args: argparse.Namespace) -> None:
    """Execute init command

    Args:
        stairlight (StairLight): Stairlight class
        args (argparse.Namespace): CLI arguments
    """
    message = ""
    stairlight_template_file = stairlight.init()
    if stairlight_template_file:
        message = (
            f"'{stairlight_template_file}' has created.\n"
            "Please edit it to set your data sources."
        )
        exit(message)
    exit()


def command_check(stairlight: StairLight, args: argparse.Namespace) -> None:
    """Execute check command

    Args:
        stairlight (StairLight): Stairlight class
        args (argparse.Namespace): CLI arguments
    """
    mapping_template_file = stairlight.check()
    if mapping_template_file:
        message = (
            f"'{mapping_template_file}' has created.\n"
            "Please map undefined tables and parameters, "
            "and append to your latest file."
        )
        exit(message)
    exit()


def command_up(
    stairlight: StairLight,
    args: argparse.Namespace,
) -> Union[dict, list]:
    """Execute up command


    Args:
        stairlight (StairLight): Stairlight class
        args (argparse.Namespace): CLI arguments

    Returns:
        Union[dict, list]: Upstairs results
    """
    return execute_up_or_down(stairlight.up, args)


def command_down(
    stairlight: StairLight,
    args: argparse.Namespace,
) -> Union[dict, list]:
    """Execute down command

    Args:
        stairlight (StairLight): Stairlight class
        args (argparse.Namespace): CLI arguments

    Returns:
        Union[dict, list]: Downstairs results
    """
    return execute_up_or_down(stairlight.down, args)


def execute_up_or_down(
    up_or_down: Callable, args: argparse.Namespace
) -> Union[dict, list]:
    """Execute a command, up or down

    Args:
        up_or_down (Callable): Either command_up() or command_down()
        args (argparse.Namespace): CLI arguments

    Returns:
        Union[dict, list]: Results
    """
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


def set_config_parser(parser: argparse.ArgumentParser) -> None:
    """Set a '--config' argument

    Args:
        parser (argparse.ArgumentParser): ArgumentParser
    """
    parser.add_argument(
        "-c",
        "--config",
        help="set a Stairlight configuration directory.",
        type=str,
        default=".",
    )


def set_save_load_parser(parser: argparse.ArgumentParser) -> None:
    """Set arguments, '--save' and '--load'

    Args:
        parser (argparse.ArgumentParser): ArgumentParser
    """
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "-s",
        "--save",
        help="Save results to a file.",
        type=str,
        default=None,
    )
    group.add_argument(
        "-l",
        "--load",
        help="Load results from a file.",
        type=str,
        default=None,
    )


def set_up_down_parser(parser: argparse.ArgumentParser) -> None:
    """Set arguments used by up and down

    Args:
        parser (argparse.ArgumentParser): ArgumentParser
    """
    parser.add_argument(
        "-t",
        "--table",
        help=(
            "Table name that Stairlight searches for, "
            "can be specified multiple times."
        ),
        required=True,
        action="append",
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
        help="Return verbose results.",
        action="store_true",
        default=False,
    )
    parser.add_argument(
        "-r",
        "--recursive",
        help="Search recursively",
        action="store_true",
        default=False,
    )


def create_parser() -> argparse.ArgumentParser:
    """Create a argument parser

    Returns:
        argparse.ArgumentParser: ArgumentParser
    """
    description = (
        "A table-level data lineage tool, "
        "detects table dependencies from 'Transform' SQL files. "
        "Without positional arguments, "
        "return a table dependency map as JSON format."
    )
    parser = argparse.ArgumentParser(description=description)
    set_config_parser(parser=parser)
    set_save_load_parser(parser=parser)

    subparsers = parser.add_subparsers()

    # init
    parser_init = subparsers.add_parser(
        "init", help="Create a new Stairlight configuration file."
    )
    parser_init.set_defaults(handler=command_init)
    set_config_parser(parser=parser_init)

    # check
    parser_check = subparsers.add_parser(
        "check", help="Create a new configuration file about undefined mappings."
    )
    parser_check.set_defaults(handler=command_check)
    set_config_parser(parser=parser_check)

    # up
    parser_up = subparsers.add_parser(
        "up", help="Return upstairs ( table | SQL file ) list."
    )
    parser_up.set_defaults(handler=command_up)
    set_config_parser(parser=parser_up)
    set_save_load_parser(parser=parser_up)
    set_up_down_parser(parser=parser_up)

    # down
    parser_down = subparsers.add_parser(
        "down", help="Return downstairs ( table | SQL file ) list."
    )
    parser_down.set_defaults(handler=command_down)
    set_config_parser(parser=parser_down)
    set_save_load_parser(parser=parser_down)
    set_up_down_parser(parser=parser_down)

    return parser


def main() -> None:
    """CLI entrypoint"""
    parser = create_parser()
    args = parser.parse_args()
    stairlight = StairLight(
        config_dir=args.config, load_file=args.load, save_file=args.save
    )

    result = None
    if hasattr(args, "handler"):
        if args.handler == command_init and stairlight.has_strl_config():
            exit(f"'{args.config}/stairlight.y(a)ml' already exists.")
        elif args.handler != command_init and not stairlight.has_strl_config():
            exit(f"'{args.config}/stairlight.y(a)ml' is not found.")
        result = args.handler(stairlight, args)
    else:
        if not stairlight.has_strl_config():
            exit(f"'{args.config}/stairlight.y(a)ml' is not found.")
        result = stairlight.mapped

    if result:
        print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
