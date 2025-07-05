import subprocess
import sys
from argparse import Namespace
from pathlib import Path
from unittest.mock import patch

import pytest

from persistproc.cli import (
    RunAction,
    ServeAction,
    ToolAction,
    get_default_data_dir,
    get_default_port,
    parse_cli,
)


@pytest.fixture
def mock_setup_logging():
    with patch(
        "persistproc.cli.setup_logging", return_value=Path("/fake/log/path")
    ) as mock:
        yield mock


def test_parse_cli_no_args(mock_setup_logging):
    """Test `persistproc` -> `serve` default."""
    action, log_path = parse_cli([])
    assert isinstance(action, ServeAction)
    assert action.port == get_default_port()
    assert action.data_dir == get_default_data_dir()
    assert action.verbose == 0
    assert log_path == Path("/fake/log/path")
    mock_setup_logging.assert_called_once()


def test_parse_cli_serve_command(mock_setup_logging):
    """Test `persistproc serve --port ...`."""
    action, _ = parse_cli(["serve", "--port", "1234", "-vv"])
    assert isinstance(action, ServeAction)
    assert action.port == 1234
    assert action.verbose == 2


def test_parse_cli_implicit_serve_with_flags(mock_setup_logging):
    """Test `persistproc --port ...` -> `serve`."""
    action, _ = parse_cli(["--port", "4321"])
    assert isinstance(action, ServeAction)
    assert action.port == 4321


def test_parse_cli_implicit_run(mock_setup_logging):
    """Test `persistproc my-script.py` -> `run`."""
    action, _ = parse_cli(["my-script.py", "arg1"])
    assert isinstance(action, RunAction)
    assert action.command == "my-script.py"
    assert action.run_args == ["arg1"]


def test_parse_cli_global_flags_before_subcommand(mock_setup_logging):
    """Global flags like -v and --port should be accepted before the subcommand."""
    action, _ = parse_cli(["-v", "serve", "--port", "12345"])
    assert isinstance(action, ServeAction)
    assert action.verbose == 1
    assert action.port == 12345


def test_parse_cli_explicit_run(mock_setup_logging):
    """Test `persistproc run ...`."""
    # Now requires -- separator for flags
    action, _ = parse_cli(["run", "python", "--", "-m", "http.server"])
    assert isinstance(action, RunAction)
    assert action.command == "python"
    assert action.run_args == ["-m", "http.server"]


def test_parse_cli_run_with_quoted_string(mock_setup_logging):
    """Test `persistproc run \"echo 'hello world'\"`."""
    action, _ = parse_cli(["run", "echo 'hello world'"])
    assert isinstance(action, RunAction)
    assert action.command == "echo"
    assert action.run_args == ["hello world"]


def test_parse_cli_tool_command(mock_setup_logging):
    """Test `persistproc start ...`."""
    action, _ = parse_cli(["start", "sleep 10"])
    assert isinstance(action, ToolAction)
    assert isinstance(action.args, Namespace)
    assert action.args.command == "start"
    assert action.args.command_ == "sleep 10"


def test_parse_cli_tool_with_common_args(mock_setup_logging):
    """Test tool command with shared arguments like --port."""
    action, _ = parse_cli(["list", "--port", "9999"])
    assert isinstance(action, ToolAction)
    assert action.args.port == 9999


def test_parse_cli_restart_process_by_pid(mock_setup_logging):
    """Test `persistproc restart 123`."""
    action, _ = parse_cli(["restart", "123"])
    assert isinstance(action, ToolAction)
    assert action.tool.name == "restart"
    assert action.args.target == "123"
    assert not action.args.args


def test_parse_cli_restart_process_by_command(mock_setup_logging):
    """Test `persistproc restart sleep 10`."""
    action, _ = parse_cli(["restart", "sleep", "10"])
    assert isinstance(action, ToolAction)
    assert action.tool.name == "restart"
    assert action.args.target == "sleep"
    assert action.args.args == ["10"]


def test_parse_cli_restart_process_by_command_and_cwd(mock_setup_logging):
    """Test `persistproc restart sleep 10 --working-directory /tmp`."""
    action, _ = parse_cli(["restart", "--working-directory", "/tmp", "sleep", "10"])
    assert isinstance(action, ToolAction)
    assert action.tool.name == "restart"
    assert action.args.target == "sleep"
    assert action.args.args == ["10"]
    assert action.args.working_directory == "/tmp"


def test_parse_cli_data_dir_and_verbose_for_logging(mock_setup_logging):
    """Check that logging setup receives the correct arguments."""
    data_dir = Path("/custom/data")
    parse_cli(["serve", "--data-dir", str(data_dir), "-vvv"])
    mock_setup_logging.assert_called_with(3, data_dir)


def test_root_help_displays_subcommands():
    """`persistproc --help` lists available sub-commands (serve, run, etc.)."""
    cmd = [sys.executable, "-m", "persistproc", "--help"]
    proc = subprocess.run(cmd, text=True, capture_output=True, check=False)

    # Exit code 0 and key sub-commands in help output.
    assert proc.returncode == 0, proc.stderr
    assert "serve" in proc.stdout
    assert "list-processes" in proc.stdout or "list" in proc.stdout


def test_parse_cli_stop_process_by_pid(mock_setup_logging):
    """Test `persistproc stop 123`."""
    action, _ = parse_cli(["stop", "123"])
    assert isinstance(action, ToolAction)
    assert action.tool.name == "stop"
    assert action.args.target == "123"
    assert not action.args.args


def test_parse_cli_stop_process_by_command(mock_setup_logging):
    """Test `persistproc stop sleep 10`."""
    action, _ = parse_cli(["stop", "sleep", "10"])
    assert isinstance(action, ToolAction)
    assert action.tool.name == "stop"
    assert action.args.target == "sleep"
    assert action.args.args == ["10"]


def test_parse_cli_start_with_label(mock_setup_logging):
    """Test `persistproc start echo hello --label my-label`."""
    action, _ = parse_cli(["start", "echo", "hello", "--label", "my-label"])
    assert isinstance(action, ToolAction)
    assert action.tool.name == "start"
    assert action.args.command_ == "echo"
    assert action.args.args == ["hello"]
    assert action.args.label == "my-label"


def test_parse_cli_start_without_label(mock_setup_logging):
    """Test `persistproc start echo hello` (no label)."""
    action, _ = parse_cli(["start", "echo", "hello"])
    assert isinstance(action, ToolAction)
    assert action.tool.name == "start"
    assert action.args.command_ == "echo"
    assert action.args.args == ["hello"]
    assert getattr(action.args, "label", None) is None


def test_parse_cli_run_with_label(mock_setup_logging):
    """Test `persistproc run --label my-label echo hello`."""
    action, _ = parse_cli(["run", "--label", "my-label", "echo", "hello"])
    assert isinstance(action, RunAction)
    assert action.command == "echo"
    assert action.run_args == ["hello"]
    assert action.label == "my-label"


def test_parse_cli_run_without_label(mock_setup_logging):
    """Test `persistproc run echo hello` (no label)."""
    action, _ = parse_cli(["run", "echo", "hello"])
    assert isinstance(action, RunAction)
    assert action.command == "echo"
    assert action.run_args == ["hello"]
    assert action.label is None


# Tests for -- separator and argument parsing behavior
# NOTE: Current implementation uses argparse.REMAINDER which is deprecated due to bugs
# See: https://bugs.python.org/issue17050
def test_parse_cli_run_with_double_dash_separator(mock_setup_logging):
    """Test `persistproc run python -- script.py arg1 arg2`."""
    action, _ = parse_cli(["run", "python", "--", "script.py", "arg1", "arg2"])
    assert isinstance(action, RunAction)
    assert action.command == "python"
    assert action.run_args == ["script.py", "arg1", "arg2"]


def test_parse_cli_run_no_separator_simple_command(mock_setup_logging):
    """Test `persistproc run ls` with no args - should work without --."""
    action, _ = parse_cli(["run", "ls"])
    assert isinstance(action, RunAction)
    assert action.command == "ls"
    assert action.run_args == []


def test_parse_cli_run_with_run_flags_before_separator(mock_setup_logging):
    """Test `persistproc run --fresh python -- script.py`."""
    action, _ = parse_cli(["run", "--fresh", "python", "--", "script.py"])
    assert isinstance(action, RunAction)
    assert action.command == "python"
    assert action.run_args == ["script.py"]
    assert action.fresh is True


def test_parse_cli_run_with_label_and_separator(mock_setup_logging):
    """Test `persistproc run --label my-app python -- -m myapp --port 8080`."""
    action, _ = parse_cli(
        ["run", "--label", "my-app", "python", "--", "-m", "myapp", "--port", "8080"]
    )
    assert isinstance(action, RunAction)
    assert action.command == "python"
    assert action.run_args == ["-m", "myapp", "--port", "8080"]
    assert action.label == "my-app"


def test_parse_cli_run_ambiguous_without_separator(mock_setup_logging):
    """Test ambiguous case: `persistproc run python script.py` without --."""
    # Without --, additional args are treated as args to the command
    action, _ = parse_cli(["run", "python", "script.py"])
    assert isinstance(action, RunAction)
    assert action.command == "python"
    assert action.run_args == ["script.py"]


def test_parse_cli_run_with_raw_flag_and_separator(mock_setup_logging):
    """Test `persistproc run --raw npm -- run dev`."""
    action, _ = parse_cli(["run", "--raw", "npm", "--", "run", "dev"])
    assert isinstance(action, RunAction)
    assert action.command == "npm"
    assert action.run_args == ["run", "dev"]
    assert action.raw is True


# Tests for start command with -- separator
def test_parse_cli_start_with_double_dash_separator(mock_setup_logging):
    """Test `persistproc start python -- script.py arg1 arg2`."""
    action, _ = parse_cli(["start", "python", "--", "script.py", "arg1", "arg2"])
    assert isinstance(action, ToolAction)
    assert action.tool.name == "start"
    assert action.args.command_ == "python"
    assert action.args.args == ["script.py", "arg1", "arg2"]


def test_parse_cli_start_with_label_and_separator(mock_setup_logging):
    """Test `persistproc start --label backend python -- -m uvicorn app:main`."""
    action, _ = parse_cli(
        ["start", "--label", "backend", "python", "--", "-m", "uvicorn", "app:main"]
    )
    assert isinstance(action, ToolAction)
    assert action.tool.name == "start"
    assert action.args.command_ == "python"
    assert action.args.args == ["-m", "uvicorn", "app:main"]
    assert action.args.label == "backend"


def test_parse_cli_start_with_working_dir_and_separator(mock_setup_logging):
    """Test `persistproc start --working-directory /app node -- server.js --port 3000`."""
    action, _ = parse_cli(
        [
            "start",
            "--working-directory",
            "/app",
            "node",
            "--",
            "server.js",
            "--port",
            "3000",
        ]
    )
    assert isinstance(action, ToolAction)
    assert action.tool.name == "start"
    assert action.args.command_ == "node"
    assert action.args.args == ["server.js", "--port", "3000"]
    assert action.args.working_directory == "/app"


def test_parse_cli_start_no_separator_simple(mock_setup_logging):
    """Test `persistproc start echo` - simple command without args."""
    action, _ = parse_cli(["start", "echo"])
    assert isinstance(action, ToolAction)
    assert action.tool.name == "start"
    assert action.args.command_ == "echo"
    assert action.args.args == []


# Edge cases and special scenarios
def test_parse_cli_run_command_with_spaces_in_quotes(mock_setup_logging):
    """Test command with spaces: `persistproc run "npm run dev"`."""
    action, _ = parse_cli(["run", "npm run dev"])
    assert isinstance(action, RunAction)
    # The command should be parsed as shell-split
    assert action.command == "npm"
    assert action.run_args == ["run", "dev"]


def test_parse_cli_run_complex_shell_command(mock_setup_logging):
    """Test complex command: `persistproc run bash -- -c "echo hello && echo world"`."""
    action, _ = parse_cli(["run", "bash", "--", "-c", "echo hello && echo world"])
    assert isinstance(action, RunAction)
    assert action.command == "bash"
    assert action.run_args == ["-c", "echo hello && echo world"]


def test_parse_cli_start_command_with_equals_in_args(mock_setup_logging):
    """Test args with equals: `persistproc start python -- script.py --config=prod.ini`."""
    action, _ = parse_cli(["start", "python", "--", "script.py", "--config=prod.ini"])
    assert isinstance(action, ToolAction)
    assert action.tool.name == "start"
    assert action.args.command_ == "python"
    assert action.args.args == ["script.py", "--config=prod.ini"]


def test_parse_cli_run_with_multiple_run_flags(mock_setup_logging):
    """Test multiple run flags: `persistproc run --fresh --raw --on-exit stop npm -- start`."""
    action, _ = parse_cli(
        ["run", "--fresh", "--raw", "--on-exit", "stop", "npm", "--", "start"]
    )
    assert isinstance(action, RunAction)
    assert action.command == "npm"
    assert action.run_args == ["start"]
    assert action.fresh is True
    assert action.raw is True
    assert action.on_exit == "stop"


def test_parse_cli_global_flags_with_run_and_separator(mock_setup_logging):
    """Test global flags: `persistproc -v --port 8080 run python -- app.py`."""
    action, _ = parse_cli(["-v", "--port", "8080", "run", "python", "--", "app.py"])
    assert isinstance(action, RunAction)
    assert action.command == "python"
    assert action.run_args == ["app.py"]
    assert action.verbose == 1
    assert action.port == 8080


# Tests for common mistakes and error cases
def test_parse_cli_run_invalid_flag_without_separator(mock_setup_logging):
    """Test mistake: `persistproc run python -m http.server` without --."""
    # Now properly rejects unrecognized flags without --
    with pytest.raises(SystemExit):
        parse_cli(["run", "python", "-m", "http.server"])


def test_parse_cli_start_invalid_flag_without_separator(mock_setup_logging):
    """Test mistake: `persistproc start npm run dev` without --."""
    # Without --, 'run' and 'dev' are treated as separate args, not npm args
    # This parses but probably not what user intended
    action, _ = parse_cli(["start", "npm", "run", "dev"])
    assert isinstance(action, ToolAction)
    assert action.tool.name == "start"
    assert action.args.command_ == "npm"
    assert action.args.args == [
        "run",
        "dev",
    ]  # User probably wanted "npm run dev" as one command


def test_parse_cli_run_mistaken_double_command(mock_setup_logging):
    """Test mistake: `persistproc run start python script.py`."""
    # User confused 'run' and 'start' - 'start' becomes the command to run
    action, _ = parse_cli(["run", "start", "python", "script.py"])
    assert isinstance(action, RunAction)
    assert action.command == "start"  # Not what user intended!
    assert action.run_args == ["python", "script.py"]


def test_parse_cli_run_flags_after_command_without_separator(mock_setup_logging):
    """Test mistake: `persistproc run node --inspect server.js` without --."""
    # Now properly rejects unrecognized flags without --
    with pytest.raises(SystemExit):
        parse_cli(["run", "node", "--inspect", "server.js"])


def test_parse_cli_correct_usage_with_separator_for_flags(mock_setup_logging):
    """Test correct: `persistproc run node -- --inspect server.js`."""
    action, _ = parse_cli(["run", "node", "--", "--inspect", "server.js"])
    assert isinstance(action, RunAction)
    assert action.command == "node"
    assert action.run_args == ["--inspect", "server.js"]


def test_parse_cli_start_with_port_ambiguity(mock_setup_logging):
    """Test ambiguous: `persistproc start python app.py --port 8080` without --."""
    # --port is consumed by the global parser, not passed to python!
    action, _ = parse_cli(["start", "python", "app.py", "--port", "8080"])
    assert isinstance(action, ToolAction)
    assert action.tool.name == "start"
    assert action.args.command_ == "python"
    assert action.args.args == ["app.py"]  # --port 8080 was consumed!
    assert action.args.port == 8080  # --port went to persistproc


def test_parse_cli_start_correct_port_usage(mock_setup_logging):
    """Test correct: `persistproc start python -- app.py --port 8080`."""
    action, _ = parse_cli(["start", "python", "--", "app.py", "--port", "8080"])
    assert isinstance(action, ToolAction)
    assert action.tool.name == "start"
    assert action.args.command_ == "python"
    assert action.args.args == ["app.py", "--port", "8080"]


def test_parse_cli_run_missing_command_with_separator(mock_setup_logging):
    """Test error: `persistproc run -- arg1 arg2` missing command."""
    # When the command contains spaces, it gets shell-split
    # So "-- arg1 arg2" becomes command="--" with args=["arg1", "arg2"]
    # But argparse actually treats this as command="arg1" with args=["arg2"]
    action, _ = parse_cli(["run", "--", "arg1", "arg2"])
    assert isinstance(action, RunAction)
    assert action.command == "arg1"
    assert action.run_args == ["arg2"]


def test_parse_cli_run_help_flag_ambiguity(mock_setup_logging):
    """Test ambiguous: `persistproc run python --help` without --."""
    # --help is interpreted as help for persistproc run, shows help and exits
    with pytest.raises(SystemExit):
        parse_cli(["run", "python", "--help"])


def test_parse_cli_run_verbose_flag_position(mock_setup_logging):
    """Test mistake: `persistproc run python -m script.py` without --."""
    # -m should cause an error since it's not a recognized persistproc flag
    with pytest.raises(SystemExit):
        parse_cli(["run", "python", "-m", "script.py"])


# Test cases showing the DESIRED behavior (now implemented!)
def test_parse_cli_ideal_run_requires_separator_for_flags(mock_setup_logging):
    """IDEAL: `persistproc run python -m http.server` should require --."""
    # Without --, flags after command are errors
    with pytest.raises(SystemExit):
        parse_cli(["run", "python", "-m", "http.server"])


def test_parse_cli_ideal_run_with_separator(mock_setup_logging):
    """IDEAL: `persistproc run python -- -m http.server` is correct usage."""
    # This is the correct way to pass flags to the command
    action, _ = parse_cli(["run", "python", "--", "-m", "http.server"])
    assert isinstance(action, RunAction)
    assert action.command == "python"
    assert action.run_args == ["-m", "http.server"]


# Tests for stop/restart with -- separator
def test_parse_cli_stop_with_double_dash_separator(mock_setup_logging):
    """Test `persistproc stop python -- -m http.server`."""
    action, _ = parse_cli(["stop", "python", "--", "-m", "http.server"])
    assert isinstance(action, ToolAction)
    assert action.tool.name == "stop"
    assert action.args.target == "python"
    assert action.args.args == ["-m", "http.server"]


def test_parse_cli_stop_flags_without_separator(mock_setup_logging):
    """Test `persistproc stop python -m http.server` without --."""
    # Should parse but -m might be misinterpreted
    action, _ = parse_cli(["stop", "python", "-m", "http.server"])
    assert isinstance(action, ToolAction)
    assert action.tool.name == "stop"
    assert action.args.target == "python"
    # With nargs="*", -m and http.server are captured as args
    assert action.args.args == ["-m", "http.server"]


def test_parse_cli_restart_with_double_dash_separator(mock_setup_logging):
    """Test `persistproc restart node -- --inspect server.js`."""
    action, _ = parse_cli(["restart", "node", "--", "--inspect", "server.js"])
    assert isinstance(action, ToolAction)
    assert action.tool.name == "restart"
    assert action.args.target == "node"
    assert action.args.args == ["--inspect", "server.js"]


def test_parse_cli_restart_with_working_dir_and_separator(mock_setup_logging):
    """Test `persistproc restart --working-directory /app python -- -m myapp`."""
    action, _ = parse_cli(
        ["restart", "--working-directory", "/app", "python", "--", "-m", "myapp"]
    )
    assert isinstance(action, ToolAction)
    assert action.tool.name == "restart"
    assert action.args.target == "python"
    assert action.args.args == ["-m", "myapp"]
    assert action.args.working_directory == "/app"


def test_parse_cli_stop_by_label(mock_setup_logging):
    """Test `persistproc stop my-app-label`."""
    action, _ = parse_cli(["stop", "my-app-label"])
    assert isinstance(action, ToolAction)
    assert action.tool.name == "stop"
    assert action.args.target == "my-app-label"
    assert action.args.args == []


def test_parse_cli_restart_by_label(mock_setup_logging):
    """Test `persistproc restart my-app-label`."""
    action, _ = parse_cli(["restart", "my-app-label"])
    assert isinstance(action, ToolAction)
    assert action.tool.name == "restart"
    assert action.args.target == "my-app-label"
    assert action.args.args == []


# Additional edge case tests
def test_parse_cli_run_empty_args_after_separator(mock_setup_logging):
    """Test `persistproc run python --` with no args after separator."""
    action, _ = parse_cli(["run", "python", "--"])
    assert isinstance(action, RunAction)
    assert action.command == "python"
    assert action.run_args == []


def test_parse_cli_run_only_separator(mock_setup_logging):
    """Test `persistproc run --` edge case."""
    # Now properly requires a program argument
    with pytest.raises(SystemExit):
        parse_cli(["run", "--"])


def test_parse_cli_start_mixed_flags_and_args(mock_setup_logging):
    """Test complex: `persistproc start --label web python -- -m uvicorn app:main --port 8000`."""
    action, _ = parse_cli(
        [
            "start",
            "--label",
            "web",
            "python",
            "--",
            "-m",
            "uvicorn",
            "app:main",
            "--port",
            "8000",
        ]
    )
    assert isinstance(action, ToolAction)
    assert action.tool.name == "start"
    assert action.args.command_ == "python"
    assert action.args.args == ["-m", "uvicorn", "app:main", "--port", "8000"]
    assert action.args.label == "web"
