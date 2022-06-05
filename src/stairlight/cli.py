import argparse
import json
import textwrap
from typing import Callable, Union

from stairlight.stairlight import ResponseType, StairLight


def command_init(stairlight: StairLight, args: argparse.Namespace) -> str:
    """Execute init command

    Args:
        stairlight (StairLight): Stairlight class
        args (argparse.Namespace): CLI arguments

    Returns:
        str: return messages
    """
    message = ""
    stairlight_template_file = stairlight.init()
    if stairlight_template_file:
        message = (
            f"'{stairlight_template_file}' has created.\n"
            "Please edit it to set your data sources."
        )
    return message


def command_check(stairlight: StairLight, args: argparse.Namespace) -> str:
    """Execute check command

    Args:
        stairlight (StairLight): Stairlight class
        args (argparse.Namespace): CLI arguments

    Returns:
        str: return messages
    """
    message = ""
    mapping_template_file = stairlight.check()
    if mapping_template_file:
        message = (
            f"'{mapping_template_file}' has created.\n"
            "Please map undefined tables and parameters, "
            "and append to your latest configuration file."
        )
    elif not stairlight.unmapped:
        message = "Templates are not found."

    return message


def command_up(
    stairlight: StairLight, args: argparse.Namespace
) -> Union[dict, "list[dict]"]:
    """Execute up command

    Args:
        stairlight (StairLight): Stairlight class
        args (argparse.Namespace): CLI arguments

    Returns:
        Union[dict, list]: Upstairs results
    """
    return search(
        func=stairlight.up,
        args=args,
        tables=find_tables_to_search(stairlight=stairlight, args=args),
    )


def command_down(
    stairlight: StairLight, args: argparse.Namespace
) -> Union[dict, "list[dict]"]:
    """Execute down command

    Args:
        stairlight (StairLight): Stairlight class
        args (argparse.Namespace): CLI arguments

    Returns:
        Union[dict, list[dict]]: Downstairs results
    """
    return search(
        func=stairlight.down,
        args=args,
        tables=find_tables_to_search(stairlight=stairlight, args=args),
    )


def search(
    func: Callable, args: argparse.Namespace, tables: Union[str, "list[str]"]
) -> Union[dict, "list[dict]"]:
    """Search tables by executing stairlight.up() or stairlight.down()

    Args:
        func (Callable): Either stairlight.up() or stairlight.down()
        args (argparse.Namespace): CLI arguments
        tables (Union[str, list[str]]): Tables to search

    Returns:
        Union[dict, list[dict]]: Results
    """
    results = []
    for table_name in tables:
        result = func(
            table_name=table_name,
            recursive=args.recursive,
            verbose=args.verbose,
            response_type=args.output,
        )
        if len(tables) > 1:
            results.append(result)
        else:
            return result
    return results


def find_tables_to_search(
    stairlight: StairLight, args: argparse.Namespace
) -> "list[str]":
    """Find tables to search

    Args:
        stairlight (StairLight): Stairlight class
        args (argparse.Namespace): CLI arguments

    Returns:
        list[str]: Tables to search
    """
    tables_to_search = []
    if args.table:
        tables_to_search = args.table
    elif args.label:
        tables_to_search = stairlight.find_tables_by_labels(target_labels=args.label)
        if not tables_to_search:
            exit()
    return tables_to_search


def set_general_parser(parser: argparse.ArgumentParser) -> None:
    """Set general arguments

    Args:
        parser (argparse.ArgumentParser): ArgumentParser
    """
    parser.add_argument(
        "-c",
        "--config",
        help="set Stairlight configuration directory",
        type=str,
        default=".",
    )
    parser.add_argument(
        "-q",
        "--quiet",
        help="keep silence",
        action="store_true",
        default=False,
    )


def set_save_load_parser(parser: argparse.ArgumentParser) -> None:
    """Set arguments, '--save' and '--load'

    Args:
        parser (argparse.ArgumentParser): ArgumentParser
    """
    parser.add_argument(
        "--save",
        help="file path where results will be saved(File system or GCS)",
        type=str,
        default=None,
    )
    parser.add_argument(
        "--load",
        help=textwrap.dedent(
            """\
            file path in which results are saved(File system or GCS),
            can be specified multiple times
        """
        ),
        action="append",
    )


def set_search_parser(parser: argparse.ArgumentParser) -> None:
    """Set arguments used by up and down

    Args:
        parser (argparse.ArgumentParser): ArgumentParser
    """
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "-t",
        "--table",
        help=textwrap.dedent(
            """\
            table names that Stairlight searches for, can be specified multiple times.
            e.g. -t PROJECT_a.DATASET_b.TABLE_c -t PROJECT_d.DATASET_e.TABLE_f
        """
        ),
        action="append",
    )
    group.add_argument(
        "-l",
        "--label",
        help=textwrap.dedent(
            """\
            labels set for the table in mapping configuration,
            can be specified multiple times.
            The separator between key and value should be a colon(:).
            e.g. -l key_1:value_1 -l key_2:value_2
        """
        ),
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
        help="search recursively",
        action="store_true",
        default=False,
    )


def create_parser() -> argparse.ArgumentParser:
    """Create a argument parser

    Returns:
        argparse.ArgumentParser: ArgumentParser
    """
    description = (
        "An end-to-end data lineage tool, "
        "detects table dependencies by SELECT queries. "
        "Without positional arguments, "
        "return a table dependency map as JSON format."
    )
    parser = argparse.ArgumentParser(description=description)
    set_general_parser(parser=parser)
    set_save_load_parser(parser=parser)

    subparsers = parser.add_subparsers()

    # init
    parser_init = subparsers.add_parser(
        "init", help="create new Stairlight configuration file"
    )
    parser_init.set_defaults(handler=command_init)
    set_general_parser(parser=parser_init)

    # map | check
    parser_check = subparsers.add_parser(
        "map",
        aliases=["check"],
        help="create new configuration file about undefined mappings",
    )
    parser_check.set_defaults(handler=command_check)
    set_general_parser(parser=parser_check)

    # up
    parser_up = subparsers.add_parser(
        "up", help="return upstairs ( table | SQL file ) list"
    )
    parser_up.set_defaults(handler=command_up)
    set_general_parser(parser=parser_up)
    set_save_load_parser(parser=parser_up)
    set_search_parser(parser=parser_up)

    # down
    parser_down = subparsers.add_parser(
        "down", help="return downstairs ( table | SQL file ) list"
    )
    parser_down.set_defaults(handler=command_down)
    set_general_parser(parser=parser_down)
    set_save_load_parser(parser=parser_down)
    set_search_parser(parser=parser_down)

    return parser


def main() -> None:
    """CLI entrypoint"""
    parser = create_parser()
    args = parser.parse_args()
    stairlight = StairLight(
        config_dir=args.config, load_files=args.load, save_file=args.save
    )
    stairlight.create_map()

    result = None
    if hasattr(args, "handler"):
        if args.handler == command_init and stairlight.has_stairlight_config():
            exit(f"'{args.config}/stairlight.y(a)ml' already exists.")
        elif args.handler != command_init and not stairlight.has_stairlight_config():
            exit(f"'{args.config}/stairlight.y(a)ml' is not found.")
        result = args.handler(stairlight, args)
    else:
        if not stairlight.has_stairlight_config():
            exit(f"'{args.config}/stairlight.y(a)ml' is not found.")
        result = stairlight.mapped

    if args.quiet:
        return

    if result and isinstance(result, str):
        print(result)
    else:
        print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
