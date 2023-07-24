from __future__ import annotations

import argparse
import json
import textwrap
from typing import Any, Callable

from src import stairlight
from src.stairlight.map import MappedTemplate


def command_init(stairlight: stairlight.StairLight, args: argparse.Namespace) -> str:
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
        message = stairlight_template_file
    return message


def command_check(stairlight: stairlight.StairLight, args: argparse.Namespace) -> str:
    """Execute check command

    Args:
        stairlight (StairLight): Stairlight class
        args (argparse.Namespace): CLI arguments

    Returns:
        str: return messages
    """
    created_files = stairlight.check()
    return "\n".join([f for f in created_files if f != ""])


def command_list(
    stairlight: stairlight.StairLight, args: argparse.Namespace
) -> list[str]:
    """Execute list command

    Args:
        stairlight (StairLight): Stairlight class
        args (argparse.Namespace): CLI arguments

    Returns:
        list[str]: a list of (tables|URIs)
    """
    return stairlight.list_(response_type=args.output)


def command_up(
    stairlight: stairlight.StairLight, args: argparse.Namespace
) -> dict[str, Any] | list[dict[str, Any]]:
    """Execute up command

    Args:
        stairlight (StairLight): Stairlight class
        args (argparse.Namespace): CLI arguments

    Returns:
        dict[str, Any] | list[dict[str, Any]]: Upstairs results
    """
    return search(
        func=stairlight.up,
        args=args,
        tables=find_tables_to_search(stairlight=stairlight, args=args),
    )


def command_down(
    stairlight: stairlight.StairLight, args: argparse.Namespace
) -> dict[str, Any] | list[dict[str, Any]]:
    """Execute down command

    Args:
        stairlight (StairLight): Stairlight class
        args (argparse.Namespace): CLI arguments

    Returns:
        dict[str, Any] | list[dict[str, Any]]: Downstairs results
    """
    return search(
        func=stairlight.down,
        args=args,
        tables=find_tables_to_search(stairlight=stairlight, args=args),
    )


def search(
    func: Callable, args: argparse.Namespace, tables: str | list[str]
) -> dict[str, Any] | list[dict[str, Any]]:
    """Search tables by executing stairlight.up() or stairlight.down()

    Args:
        func (Callable): Either stairlight.up() or stairlight.down()
        args (argparse.Namespace): CLI arguments
        tables (str | list[str]): Tables to search

    Returns:
        dict[str, Any] | list[dict[str, Any]]: Results
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
    stairlight: stairlight.StairLight, args: argparse.Namespace
) -> list[str]:
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
        help="set a Stairlight configuration directory",
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
    parser.add_argument(
        "--version",
        help="version",
        action="version",
        version=stairlight.__version__,
    )


def set_save_load_parser(parser: argparse.ArgumentParser) -> None:
    """Set arguments, '--save' and '--load'

    Args:
        parser (argparse.ArgumentParser): ArgumentParser
    """
    parser.add_argument(
        "--save",
        help=textwrap.dedent(
            """\
            A file path where mapped results will be saved.
            You can choose from local file system, GCS, S3.
        """
        ),
        type=str,
        default=None,
    )
    parser.add_argument(
        "--load",
        help=textwrap.dedent(
            """\
            A file path where mapped results are saved.
            You can choose from local file system, GCS, S3.
            It can be specified multiple times.
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


def set_output_parser(parser: argparse.ArgumentParser) -> None:
    """Set arguments about outputs

    Args:
        parser (argparse.ArgumentParser): ArgumentParser
    """
    parser.add_argument(
        "-o",
        "--output",
        help="output type",
        type=str,
        choices=[
            stairlight.ResponseType.TABLE.value,
            stairlight.ResponseType.URI.value,
        ],
        default=stairlight.ResponseType.TABLE.value,
    )


def create_parser() -> argparse.ArgumentParser:
    """Create a argument parser

    Returns:
        argparse.ArgumentParser: ArgumentParser
    """
    description = (
        "An end-to-end data lineage tool, "
        "detects table dependencies by SQL SELECT statements. "
        "Without positional arguments, "
        "return a table dependency map as JSON format."
    )
    parser = argparse.ArgumentParser(prog="Stairlight", description=description)
    set_general_parser(parser=parser)
    set_save_load_parser(parser=parser)

    subparsers = parser.add_subparsers()

    # init
    parser_init = subparsers.add_parser(
        "init", help="create a new Stairlight configuration file"
    )
    parser_init.set_defaults(handler=command_init)
    set_general_parser(parser=parser_init)

    # map | check
    parser_check = subparsers.add_parser(
        "map",
        aliases=["check"],
        help=(
            "create a new configuration file about undefined mappings"
            " and templates not found"
        ),
    )
    parser_check.set_defaults(handler=command_check)
    set_general_parser(parser=parser_check)

    # list
    parser_list = subparsers.add_parser("list", help="return all ( tables | URIs )")
    parser_list.set_defaults(handler=command_list)
    set_general_parser(parser=parser_list)
    set_save_load_parser(parser=parser_list)
    set_output_parser(parser=parser_list)

    # up
    parser_up = subparsers.add_parser("up", help="return upstairs ( tables | URIs )")
    parser_up.set_defaults(handler=command_up)
    set_general_parser(parser=parser_up)
    set_save_load_parser(parser=parser_up)
    set_output_parser(parser=parser_up)
    set_search_parser(parser=parser_up)

    # down
    parser_down = subparsers.add_parser(
        "down", help="return downstairs ( tables | URIs )"
    )
    parser_down.set_defaults(handler=command_down)
    set_general_parser(parser=parser_down)
    set_save_load_parser(parser=parser_down)
    set_output_parser(parser=parser_down)
    set_search_parser(parser=parser_down)

    return parser


def main() -> None:
    """CLI entrypoint"""
    parser = create_parser()
    args = parser.parse_args()
    _stairlight = stairlight.StairLight(
        config_dir=args.config, load_files=args.load, save_file=args.save
    )
    _stairlight.create_map()

    result_command: Any = None
    result_mapped: dict[str, dict[str, list[MappedTemplate] | None] | None] = {}
    if hasattr(args, "handler"):
        if args.handler == command_init and _stairlight.has_stairlight_config():
            exit(f"'{args.config}/stairlight.y(a)ml' already exists.")
        elif args.handler != command_init and not _stairlight.has_stairlight_config():
            exit(f"'{args.config}/stairlight.y(a)ml' is not found.")
        result_command = args.handler(_stairlight, args)
    else:
        if not _stairlight.has_stairlight_config():
            exit(f"'{args.config}/stairlight.y(a)ml' is not found.")
        result_mapped = _stairlight.mapped

    if args.quiet or (not result_command and not result_mapped):
        return

    if result_command and isinstance(result_command, str):
        print(result_command)
    elif result_command:
        print(json.dumps(result_command, indent=2))
    elif result_mapped:
        result_dict: dict[str, Any] = _stairlight.cast_mapped_dict_all(
            mapped=result_mapped
        )
        print(json.dumps(result_dict, indent=2))


if __name__ == "__main__":
    main()
