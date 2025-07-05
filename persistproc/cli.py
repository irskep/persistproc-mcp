import argparse
import os
import shlex
import sys
from argparse import Namespace
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .logging_utils import CLI_LOGGER, setup_logging
from .run import run
from .serve import serve
from .tools import ALL_TOOL_CLASSES

ENV_PORT = "PERSISTPROC_PORT"
ENV_DATA_DIR = "PERSISTPROC_DATA_DIR"


@dataclass
class ServeAction:
    """Represents the 'serve' command."""

    port: int
    data_dir: Path
    verbose: int
    log_path: Path


@dataclass
class RunAction:
    """Represents the 'run' command."""

    command: str
    run_args: list[str]
    fresh: bool
    on_exit: str
    raw: bool
    port: int
    data_dir: Path
    verbose: int
    label: str | None


@dataclass
class ToolAction:
    """Represents a tool command."""

    args: Namespace
    tool: Any


CLIAction = ServeAction | RunAction | ToolAction


def get_default_data_dir() -> Path:
    """Return default data directory, honouring *PERSISTPROC_DATA_DIR*."""

    if ENV_DATA_DIR in os.environ and os.environ[ENV_DATA_DIR]:
        return Path(os.environ[ENV_DATA_DIR]).expanduser().resolve()

    if sys.platform == "darwin":
        return Path.home() / "Library" / "Application Support" / "persistproc"
    elif sys.platform.startswith("linux"):
        return (
            Path(os.environ.get("XDG_DATA_HOME", Path.home() / ".local" / "share"))
            / "persistproc"
        )
    return Path.home() / ".persistproc"


def get_default_port() -> int:
    """Return default port, honouring *PERSISTPROC_PORT*."""

    if ENV_PORT in os.environ:
        try:
            return int(os.environ[ENV_PORT])
        except ValueError:
            pass  # fall through to hard-coded default

    return 8947


def parse_command_and_args(program: str, args: list[str]) -> tuple[str, list[str]]:
    """Parse a program string and arguments list into command and args.

    If program contains spaces and no separate args are provided,
    shell-split the program string. Otherwise, return program and args as-is.
    """
    if " " in program and not args:
        parts = shlex.split(program)
        command = parts[0]
        run_args = parts[1:]
    else:
        command = program
        run_args = args
    return command, run_args


def parse_cli(argv: list[str]) -> tuple[CLIAction, Path]:
    """Parse command line arguments and return a CLIAction and log path."""
    parser = argparse.ArgumentParser(
        description="Process manager for multi-agent development workflows\n\nDocs: https://steveasleep.com/persistproc-mcp"
    )

    # ------------------------------------------------------------------
    # Logging setup (first lightweight parse just for logging config)
    # ------------------------------------------------------------------
    logging_parser = argparse.ArgumentParser(add_help=False)
    logging_parser.add_argument("--data-dir", type=Path, default=get_default_data_dir())
    logging_parser.add_argument("-v", "--verbose", action="count", default=0)
    logging_parser.add_argument("-q", "--quiet", action="count", default=0)
    logging_args, _ = logging_parser.parse_known_args(argv)

    log_path = setup_logging(
        logging_args.verbose - logging_args.quiet, logging_args.data_dir
    )

    # ------------------------------------------------------------------
    # Helper to avoid repeating common options on every sub-command
    # ------------------------------------------------------------------

    common_parser = argparse.ArgumentParser(add_help=False)

    def add_common_args(p: argparse.ArgumentParser) -> None:  # noqa: D401
        """Add --port, --data-dir and -v/--verbose options to *p* with SUPPRESS default."""

        p.add_argument(
            "--port",
            type=int,
            default=argparse.SUPPRESS,
            help=f"Server port (default: {get_default_port()}; env: ${ENV_PORT})",
        )
        p.add_argument(
            "--data-dir",
            type=Path,
            default=argparse.SUPPRESS,
            help=f"Data directory (default: {get_default_data_dir()}; env: ${ENV_DATA_DIR})",
        )
        p.add_argument(
            "-v",
            "--verbose",
            action="count",
            default=argparse.SUPPRESS,
            help="Increase verbosity; you can use -vv for more",
        )
        p.add_argument(
            "-q",
            "--quiet",
            action="count",
            default=argparse.SUPPRESS,
            help="Decrease verbosity. Passing -q once will show only warnings and errors.",
        )

    add_common_args(common_parser)

    # Root parser should also accept the common flags so they can appear before
    # the sub-command (e.g. `persistproc -v serve`).
    add_common_args(parser)

    # Main parser / sub-commands ------------------------------------------------

    subparsers = parser.add_subparsers(dest="command")

    # Serve command
    p_serve = subparsers.add_parser(  # noqa: F841
        "serve", help="Start the MCP server", parents=[common_parser]
    )

    # Run command
    p_run = subparsers.add_parser(
        "run",
        help="Make sure a process is running and tail its output (stdout and stderr) to stdout",
        parents=[common_parser],
    )
    p_run.add_argument(
        "--fresh",
        action="store_true",
        help="Stop an existing running instance of the same command before starting a new one.",
    )
    p_run.add_argument(
        "--on-exit",
        choices=["ask", "stop", "detach"],
        default="ask",
        help="Behaviour when you press Ctrl+C: ask (default), stop the process, or detach and leave it running.",
    )
    p_run.add_argument(
        "--raw",
        action="store_true",
        help="Show raw timestamped log lines (default strips ISO timestamps).",
    )
    p_run.add_argument(
        "--label",
        type=str,
        help="Custom label for the process (default: '<command> in <working_directory>').",
    )
    p_run.add_argument(
        "program",
        help="The program to run (e.g. 'python' or 'ls'). If the string contains spaces, it will be shell-split unless additional arguments are provided separately.",
    )
    p_run.add_argument("args", nargs="*", help="Arguments to the program")

    # ------------------------------------------------------------------
    # Tool sub-commands – accept *both* snake_case and kebab-case variants
    # ------------------------------------------------------------------

    tools = [tool_cls() for tool_cls in ALL_TOOL_CLASSES]

    tools_by_name: dict[str, Any] = {}

    for tool in tools:
        snake = tool.name  # canonical spelling in help
        kebab = tool.name.replace("_", "-")  # accepted alias

        # Create **one** sub-parser (canonical) and register alias via `aliases=` so it
        # does not appear twice in `--help` output.
        if snake not in subparsers.choices:
            p_tool = subparsers.add_parser(
                snake,
                help=tool.description,
                parents=[common_parser],
                aliases=[kebab] if kebab != snake else [],
            )
            tool.build_subparser(p_tool)

        # Map both spellings to the same tool object for later lookup.
        tools_by_name[snake] = tool
        tools_by_name[kebab] = tool

    # Argument parsing
    if not argv:
        # No arguments at all -> default to `serve`
        args = parser.parse_args(["serve"])
    else:
        # Detect the first *real* command in the argv list. We iterate over the
        # raw argument vector, skipping option flags (that start with "-") **and**
        # their values.  If an arg immediately follows an option flag we treat it as
        # that option's value – not as the sub-command.

        first_cmd: str | None = None
        i = 0
        while i < len(argv):
            token = argv[i]
            if token.startswith("-"):
                # Heuristics for skipping option flags before we find the sub-command.
                #
                # 1. "-v" / "-vv" / "-vvv" are boolean count flags → no value follows.
                if (
                    token.lstrip("-").startswith("v")
                    and set(token.lstrip("v-")) == set()
                ):
                    i += 1
                    continue
                # 1b. "-q" / "-qq" / "-qqq" are boolean count flags → no value follows.
                if (
                    token.lstrip("-").startswith("q")
                    and set(token.lstrip("q-")) == set()
                ):
                    i += 1
                    continue

                # 2. Long-form flags like "--port" or "--data-dir" may have the value
                #    as the *next* token unless provided as "--port=1234".
                if token.startswith("--") and "=" not in token:
                    # Assume next token is the value *unless* it looks like another flag.
                    skip = (
                        2
                        if i + 1 < len(argv) and not argv[i + 1].startswith("-")
                        else 1
                    )
                    i += skip
                    continue

                # 3. Any other flag (including forms with "=") – skip just the token itself.
                i += 1
                continue

            # Non-flag token found – treat it as the prospective sub-command.
            first_cmd = token
            break

        # Special-case: help flag with no explicit command – display top-level
        # help (listing all sub-commands) instead of defaulting to `serve`.
        if first_cmd is None:
            if any(flag in argv for flag in ("-h", "--help")):
                # argparse will handle printing help/exit.
                args = parser.parse_args(argv)
            else:
                # Only global flags were provided (or none). Assume `serve`.
                args = parser.parse_args(["serve"] + argv)
        elif first_cmd in subparsers.choices:
            # Explicit command provided.
            args = parser.parse_args(argv)
        else:
            # Implicit `run` command.
            args = parser.parse_args(["run"] + argv)

    # ------------------------------------------------------------
    # Action creation – derive common option values (may be missing)
    # ------------------------------------------------------------

    port_val = getattr(args, "port", get_default_port())
    data_dir_val = getattr(args, "data_dir", get_default_data_dir())
    verbose_val = getattr(args, "verbose", 0) - getattr(args, "quiet", 0)

    action: CLIAction
    if args.command == "serve":
        action = ServeAction(
            port=port_val,
            data_dir=data_dir_val,
            verbose=verbose_val,
            log_path=log_path,
        )
    elif args.command == "run":
        command, run_args = parse_command_and_args(args.program, args.args)
        action = RunAction(
            command=command,
            run_args=run_args,
            fresh=args.fresh,
            on_exit=args.on_exit,
            raw=args.raw,
            port=port_val,
            data_dir=data_dir_val,
            verbose=verbose_val,
            label=getattr(args, "label", None),
        )
    elif args.command in tools_by_name:
        # Ensure tool sub-commands always have a `port` attribute so
        # downstream code doesn't crash when the user omitted --port.
        if not hasattr(args, "port"):
            args.port = port_val
        tool = tools_by_name[args.command]
        action = ToolAction(args=args, tool=tool)
    else:
        parser.print_help()
        sys.exit(1)

    return action, log_path


def handle_cli_action(action: CLIAction, log_path: Path) -> None:
    """Execute the action determined by the CLI."""
    CLI_LOGGER.info("Verbose log for this run: %s", shlex.quote(str(log_path)))

    if isinstance(action, ServeAction):
        serve(action.port, action.verbose, action.data_dir, action.log_path)
    elif isinstance(action, RunAction):
        CLI_LOGGER.info(
            "Running command: %s %s", action.command, " ".join(action.run_args)
        )
        run(
            action.command,
            action.run_args,
            action.verbose,
            fresh=action.fresh,
            on_exit=action.on_exit,
            raw=action.raw,
            port=action.port,
            label=action.label,
        )
    elif isinstance(action, ToolAction):
        action.tool.call_with_args(action.args)


def cli() -> None:
    """Main CLI entry point."""
    try:
        action, log_path = parse_cli(sys.argv[1:])
        handle_cli_action(action, log_path)
    except SystemExit as e:
        if e.code != 0:
            # argparse prints help and exits, so we only need to re-raise for actual errors
            raise


__all__ = [
    "cli",
    "parse_cli",
    "handle_cli_action",
    "ServeAction",
    "RunAction",
    "ToolAction",
    "CLIAction",
]
