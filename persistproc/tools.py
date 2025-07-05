from __future__ import annotations

import abc
import argparse
import asyncio
import json
import logging
import os
import shlex
from argparse import ArgumentParser, Namespace
from pathlib import Path

from fastmcp import FastMCP
from fastmcp.tools import FunctionTool
from persistproc.client import make_client
from persistproc.logging_utils import CLI_LOGGER
from persistproc.process_manager import ProcessManager

from .process_types import (
    ListProcessesResult,
    ProcessLogPathsResult,
    ProcessOutputResult,
    ProcessStatusResult,
    RestartProcessResult,
    StartProcessResult,
    StopProcessResult,
    StreamEnum,
)

logger = logging.getLogger(__name__)


def _make_mcp_request(tool_name: str, port: int, payload: dict | None = None) -> None:
    """Make a request to the MCP server and print the response."""
    payload = payload or {}

    async def _do_call() -> None:
        async with make_client(port) as client:
            # Filter out None values from payload before sending
            json_payload = {k: v for k, v in payload.items() if v is not None}
            results = await client.call_tool(tool_name, json_payload)

            if not results:
                CLI_LOGGER.error(
                    "No response from server for tool '%s'. Is the server running?",
                    tool_name,
                )
                return

            # Result is a JSON string in the `text` attribute.
            result_data = json.loads(results[0].text)

            # Always pretty-print the raw JSON to stdout so machine parsers (tests) can
            # reliably consume the output regardless of whether there's an error.
            print(json.dumps(result_data, indent=2))

            if result_data.get("error"):
                CLI_LOGGER.error(result_data["error"])
                return

            # Special human-friendly output in addition to JSON.
            if tool_name == "list":
                procs = result_data.get("processes", [])
                if not procs:
                    CLI_LOGGER.info("No processes running.")

    try:
        asyncio.run(_do_call())
    except ConnectionError:
        CLI_LOGGER.error(
            "Cannot connect to persistproc server on port %d. Start it with 'persistproc serve'.",
            port,
        )
    except Exception as e:
        # Check if this is an MCP tool error response
        error_str = str(e)
        if error_str.startswith("Error calling tool"):
            # Extract the error message and output as JSON for tests
            error_msg = error_str.replace(f"Error calling tool '{tool_name}': ", "")
            error_response = {"error": error_msg}
            print(json.dumps(error_response, indent=2))
            CLI_LOGGER.error(error_msg)
        else:
            CLI_LOGGER.error(
                "Unexpected error while calling tool '%s': %s", tool_name, e
            )
            CLI_LOGGER.error(
                "Cannot reach persistproc server on port %d. Make sure it is running (`persistproc serve`) or specify the correct port with --port or PERSISTPROC_PORT.",
                port,
            )


class ITool(abc.ABC):
    """Abstract base class for a persistproc tool."""

    @property
    @abc.abstractmethod
    def name(self) -> str:
        """The name of the tool."""
        ...

    @property
    @abc.abstractmethod
    def description(self) -> str:
        """The description of the tool."""
        ...

    @abc.abstractmethod
    def register_tool(self, process_manager: ProcessManager, mcp: FastMCP) -> None:
        """Register the tool with the MCP server."""
        ...

    @abc.abstractmethod
    def build_subparser(self, parser: ArgumentParser) -> None:
        """Configure the CLI subparser for the tool."""
        ...

    @abc.abstractmethod
    def call_with_args(self, args: Namespace) -> None:
        """Execute the tool's CLI command."""
        ...


class StartProcessTool(ITool):
    """Tool to start a new long-running process."""

    name = "start"
    description = "Start a new long-running process. REQUIRED if the process is expected to never terminate. PROHIBITED if the process is short-lived."

    def register_tool(self, process_manager: ProcessManager, mcp: FastMCP) -> None:
        def start(
            command: str,
            working_directory: str | None = None,
            environment: dict[str, str] | None = None,
            label: str | None = None,
        ) -> StartProcessResult:
            """Start a new long-running process."""
            logger.info("start called – cmd=%s, cwd=%s", command, working_directory)
            return process_manager.start(
                command=command,
                working_directory=(
                    Path(working_directory) if working_directory else None
                ),
                environment=environment,
                label=label,
            )

        mcp.add_tool(
            FunctionTool.from_function(
                start, name=self.name, description=self.description
            )
        )

    def build_subparser(self, parser: ArgumentParser) -> None:
        parser.add_argument(
            "--working-directory",
            default=os.getcwd(),
            help="The working directory for the process.",
        )
        parser.add_argument(
            "--label",
            type=str,
            help="Custom label for the process (default: '<command> in <working_directory>').",
        )
        parser.add_argument("command_", metavar="COMMAND", help="The command to run.")
        parser.add_argument("args", nargs="*", help="Arguments to the command")

    def call_with_args(self, args: Namespace) -> None:
        # Construct the command string from command and args
        if args.args:
            command = shlex.join([args.command_] + args.args)
        else:
            command = args.command_

        payload = {
            "command": command,
            "working_directory": args.working_directory,
            "environment": dict(os.environ),
            "label": getattr(args, "label", None),
        }
        _make_mcp_request(self.name, args.port, payload)


class ListProcessesTool(ITool):
    """Tool to list all managed processes."""

    name = "list"
    description = "List all managed processes and their status."

    def register_tool(self, process_manager: ProcessManager, mcp: FastMCP) -> None:
        def list() -> ListProcessesResult:
            """List all managed processes and their status."""
            logger.debug("list called")
            return process_manager.list()

        mcp.add_tool(FunctionTool.from_function(list, name=self.name))

    def build_subparser(self, parser: ArgumentParser) -> None:
        pass

    def call_with_args(self, args: Namespace) -> None:
        _make_mcp_request(self.name, args.port)


class GetProcessStatusTool(ITool):
    """Tool to get the status of a specific process."""

    name = "get_status"
    description = "Get the detailed status of a specific process."

    def register_tool(self, process_manager: ProcessManager, mcp: FastMCP) -> None:
        def get_status(pid: int) -> ProcessStatusResult:
            """Get the detailed status of a specific process."""
            logger.debug("get_status called for pid=%s", pid)
            return process_manager.get_status(pid)

        mcp.add_tool(FunctionTool.from_function(get_status, name=self.name))

    def build_subparser(self, parser: ArgumentParser) -> None:
        parser.add_argument("pid", type=int, help="The process ID.")

    def call_with_args(self, args: Namespace) -> None:
        _make_mcp_request(self.name, args.port, {"pid": args.pid})


class StopProcessTool(ITool):
    """Tool to stop a running process."""

    name = "stop"
    description = "Stop a running process by its PID."

    def register_tool(self, process_manager: ProcessManager, mcp: FastMCP) -> None:
        def stop(
            pid: int | None = None,
            command: str | None = None,
            working_directory: str | None = None,
            force: bool = False,
            label: str | None = None,
        ) -> StopProcessResult:
            """Stop a running process by its PID."""
            logger.info(
                "stop called for pid=%s command=%s cwd=%s force=%s",
                pid,
                command,
                working_directory,
                force,
            )
            return process_manager.stop(
                pid=pid,
                command=command,
                working_directory=(
                    Path(working_directory) if working_directory else None
                ),
                force=force,
                label=label,
            )

        mcp.add_tool(FunctionTool.from_function(stop, name=self.name))

    def build_subparser(self, parser: ArgumentParser) -> None:
        parser.add_argument(
            "target",
            metavar="TARGET",
            help="The PID, label, or command to stop.",
        )
        parser.add_argument(
            "args", nargs=argparse.REMAINDER, help="Arguments to the command"
        )
        parser.add_argument(
            "--working-directory",
            default=os.getcwd(),
            help="The working directory for the process.",
        )
        parser.add_argument(
            "--force", action="store_true", help="Force stop the process."
        )

    def call_with_args(self, args: Namespace) -> None:
        pid = None
        command = None
        label = None

        if not args.args:
            # Single target argument - could be PID, label, or command
            try:
                pid = int(args.target)
            except ValueError:
                # Not a PID, could be label or command
                # Let the server determine if it's a label or command
                label = args.target
        else:
            # Multiple arguments - treat as command with args
            command = shlex.join([args.target] + args.args)

        payload = {
            "pid": pid,
            "command": command,
            "working_directory": args.working_directory,
            "force": args.force,
            "label": label,
        }
        _make_mcp_request(self.name, args.port, payload)


class RestartProcessTool(ITool):
    """Tool to restart a running process."""

    name = "restart"
    description = "Stops a process and starts it again with the same parameters."

    def register_tool(self, process_manager: ProcessManager, mcp: FastMCP) -> None:
        def restart(
            pid: int | None = None,
            command: str | None = None,
            working_directory: str | None = None,
            label: str | None = None,
        ) -> RestartProcessResult:
            """Stops a process and starts it again with the same parameters."""
            logger.info(
                "restart called for pid=%s, command=%s, cwd=%s",
                pid,
                command,
                working_directory,
            )
            return process_manager.restart(
                pid=pid,
                command=command,
                working_directory=(
                    Path(working_directory) if working_directory else None
                ),
                label=label,
            )

        mcp.add_tool(FunctionTool.from_function(restart, name=self.name))

    def build_subparser(self, parser: ArgumentParser) -> None:
        parser.add_argument(
            "target",
            metavar="TARGET",
            help="The PID, label, or command to restart.",
        )
        # Remaining args will be parsed manually.
        parser.add_argument("args", nargs="*")
        parser.add_argument(
            "--working-directory",
            default=os.getcwd(),
            help="The working directory for the process.",
        )

    def call_with_args(self, args: Namespace) -> None:
        pid = None
        command = None
        label = None

        if not args.args:
            # Single target argument - could be PID, label, or command
            try:
                pid = int(args.target)
            except ValueError:
                # Not a PID, could be label or command
                # Let the server determine if it's a label or command
                label = args.target
        else:
            # Multiple arguments - treat as command with args
            command = shlex.join([args.target] + args.args)

        # Default working directory is set at argparse level
        working_directory = args.working_directory

        payload = {
            "pid": pid,
            "command": command,
            "working_directory": working_directory,
            "label": label,
        }
        _make_mcp_request(self.name, args.port, payload)


class GetProcessOutputTool(ITool):
    """Tool to retrieve captured output from a process."""

    name = "get_output"
    description = "Retrieve captured output from a process."

    def register_tool(self, process_manager: ProcessManager, mcp: FastMCP) -> None:
        def get_output(
            pid: int,
            stream: StreamEnum,
            lines: int | None = None,
            before_time: str | None = None,
            since_time: str | None = None,
        ) -> ProcessOutputResult:
            """Retrieve captured output from a process."""
            logger.debug(
                "get_output called pid=%s stream=%s lines=%s before=%s since=%s",
                pid,
                stream,
                lines,
                before_time,
                since_time,
            )
            return process_manager.get_output(pid, stream, lines)

        mcp.add_tool(FunctionTool.from_function(get_output, name=self.name))

    def build_subparser(self, parser: ArgumentParser) -> None:
        parser.add_argument("pid", type=int, help="The process ID.")
        parser.add_argument(
            "stream",
            choices=["stdout", "stderr", "combined"],
            default="combined",
            help="The output stream to read.",
        )
        parser.add_argument(
            "--lines", type=int, help="The number of lines to retrieve."
        )
        parser.add_argument(
            "--before-time", help="Retrieve logs before this timestamp."
        )
        parser.add_argument("--since-time", help="Retrieve logs since this timestamp.")

    def call_with_args(self, args: Namespace) -> None:
        payload = {
            "pid": args.pid,
            "stream": args.stream,
            "lines": args.lines,
            "before_time": args.before_time,
            "since_time": args.since_time,
        }
        _make_mcp_request(self.name, args.port, payload)


class GetProcessLogPathsTool(ITool):
    """Tool to get the log file paths for a process."""

    name = "get_log_paths"
    description = "Get the paths to the log files for a specific process."

    def register_tool(self, process_manager: ProcessManager, mcp: FastMCP) -> None:
        def get_log_paths(pid: int) -> ProcessLogPathsResult:
            """Get the paths to the log files for a specific process."""
            logger.debug("get_log_paths called for pid=%s", pid)
            return process_manager.get_log_paths(pid)

        mcp.add_tool(FunctionTool.from_function(get_log_paths, name=self.name))

    def build_subparser(self, parser: ArgumentParser) -> None:
        parser.add_argument("pid", type=int, help="The process ID.")

    def call_with_args(self, args: Namespace) -> None:
        _make_mcp_request(self.name, args.port, {"pid": args.pid})


class KillPersistprocTool(ITool):
    """Tool to kill all managed processes and get the server's PID."""

    name = "kill_persistproc"
    description = (
        "Kill all managed processes and get the PID of the persistproc server."
    )

    def register_tool(self, process_manager: ProcessManager, mcp: FastMCP) -> None:
        def kill_persistproc() -> dict[str, int]:
            """Kill all managed processes and get the PID of the persistproc server."""
            logger.debug("kill_persistproc called")
            return process_manager.kill_persistproc()

        mcp.add_tool(FunctionTool.from_function(kill_persistproc, name=self.name))

    def build_subparser(self, parser: ArgumentParser) -> None:
        pass

    def call_with_args(self, args: Namespace) -> None:
        _make_mcp_request(self.name, args.port)


ALL_TOOL_CLASSES = [
    StartProcessTool,
    ListProcessesTool,
    GetProcessStatusTool,
    StopProcessTool,
    RestartProcessTool,
    GetProcessOutputTool,
    GetProcessLogPathsTool,
    KillPersistprocTool,
]
