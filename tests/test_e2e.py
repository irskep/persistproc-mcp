import os
import time
from pathlib import Path

from tests.helpers import extract_json, run_cli, start_run, stop_run

COUNTER_SCRIPT = Path(__file__).parent / "scripts" / "counter.py"


def test_list_no_processes(server):
    """Test that the server runs and responds to a simple request."""
    proc = run_cli("list")
    assert proc.returncode == 0
    assert extract_json(proc.stdout) == {"processes": []}


# ---------------------------------------------------------------------------
# New test – start a process, verify it appears, then stop it
# ---------------------------------------------------------------------------


def test_start_list_stop(server):
    """Start one process, ensure it runs, then stop it."""

    # 1. Start the counter script (runs indefinitely with --num-iterations 0).
    start_cmd = f"python {COUNTER_SCRIPT} --num-iterations 0"
    start = run_cli("start", start_cmd)
    data = extract_json(start.stdout)
    pid = data["pid"]

    # 2. Confirm it appears in the list and is running.
    listed = run_cli("list")
    info = extract_json(listed.stdout)
    procs = info["processes"]
    match = next(p for p in procs if p["pid"] == pid)
    assert match["status"] == "running"

    # 3. Stop the process.
    stop = run_cli("stop", str(pid))
    extract_json(stop.stdout)  # ensure JSON present no error

    # 4. Verify it is no longer running (status != running).
    after = run_cli("list")
    info_after = extract_json(after.stdout)
    match_after = next(p for p in info_after["processes"] if p["pid"] == pid)
    assert match_after["status"] != "running"


def test_process_restart(server):
    """Start a process, restart it, verify PID changes and remains running."""

    start_cmd = f"python {COUNTER_SCRIPT} --num-iterations 0"
    start = run_cli("start", start_cmd)
    data = extract_json(start.stdout)
    old_pid = data["pid"]

    # Restart the process.
    restart = run_cli("restart", str(old_pid))
    restart_info = extract_json(restart.stdout)
    new_pid = restart_info["pid"]

    assert new_pid != old_pid

    # Confirm only new process is running.
    listed = run_cli("list")
    info = extract_json(listed.stdout)
    procs = info["processes"]

    # There should be exactly one entry with new_pid and status running.
    matches = [p for p in procs if p["pid"] == new_pid]
    assert len(matches) == 1
    assert matches[0]["status"] == "running"


def test_process_has_output(server):
    """Start a process, verify it has output, then stop it."""
    start_cmd = f"python {COUNTER_SCRIPT} --num-iterations 0"
    start = run_cli("start", start_cmd)
    data = extract_json(start.stdout)
    pid = data["pid"]

    time.sleep(1)

    # Get the output of the process.
    output = run_cli("get-output", str(pid), "stdout", "--lines", "10")
    output_lines = extract_json(output.stdout)["output"]
    assert isinstance(output_lines, list)
    assert len(output_lines) > 0
    # Check that the output contains the expected odd numbers
    output_text = "".join(output_lines)
    assert "1" in output_text
    assert "3" in output_text

    # Stop the process.
    stop = run_cli("stop", str(pid))
    extract_json(stop.stdout)  # ensure JSON present no error


# ---------------------------------------------------------------------------
# Core functionality tests
# ---------------------------------------------------------------------------


def test_get_process_status(server):
    """Test get_status tool."""
    start_cmd = f"python {COUNTER_SCRIPT} --num-iterations 0"
    start = run_cli("start", start_cmd)
    data = extract_json(start.stdout)
    pid = data["pid"]

    # Get detailed status
    status = run_cli("get-status", str(pid))
    status_data = extract_json(status.stdout)

    assert status_data["pid"] == pid
    assert status_data["status"] == "running"
    assert "command" in status_data
    assert "working_directory" in status_data
    assert isinstance(status_data["command"], list)

    # Cleanup
    run_cli("stop", str(pid))


def test_get_process_log_paths(server):
    """Test get_log_paths tool."""
    start_cmd = f"python {COUNTER_SCRIPT} --num-iterations 0"
    start = run_cli("start", start_cmd)
    data = extract_json(start.stdout)
    pid = data["pid"]

    # Get log paths
    paths = run_cli("get-log-paths", str(pid))
    paths_data = extract_json(paths.stdout)

    assert "stdout" in paths_data
    assert "stderr" in paths_data
    assert isinstance(paths_data["stdout"], str)
    assert isinstance(paths_data["stderr"], str)

    # Cleanup
    run_cli("stop", str(pid))


def test_start_process_with_working_directory(server):
    """Test start with working_directory parameter."""
    # Use a different directory (parent of current script location)
    work_dir = str(Path(__file__).parent.parent)

    # CLI expects: start --working-directory DIR COMMAND
    start_cmd = f"python {COUNTER_SCRIPT} --num-iterations 5"
    start = run_cli("start", "--working-directory", work_dir, start_cmd)
    data = extract_json(start.stdout)
    pid = data["pid"]

    # Verify the process started
    status = run_cli("get-status", str(pid))
    status_data = extract_json(status.stdout)
    assert status_data["pid"] == pid
    assert "working_directory" in status_data

    # Wait for process to complete naturally
    time.sleep(2)


def test_start_process_with_environment(server):
    """Test start inherits environment from shell."""
    # Environment variables are inherited from shell, not passed via CLI
    start_cmd = f"python {COUNTER_SCRIPT} --num-iterations 5"
    start = run_cli("start", start_cmd)
    data = extract_json(start.stdout)
    pid = data["pid"]

    # Verify the process started
    status = run_cli("get-status", str(pid))
    status_data = extract_json(status.stdout)
    assert status_data["pid"] == pid

    # Wait for process to complete naturally
    time.sleep(2)


def test_stop_process_with_force(server):
    """Test stop with force=True."""
    start_cmd = f"python {COUNTER_SCRIPT} --num-iterations 0"
    start = run_cli("start", start_cmd)
    data = extract_json(start.stdout)
    pid = data["pid"]

    # Stop with force flag - put --force before the PID
    stop = run_cli("stop", "--force", str(pid))
    stop_data = extract_json(stop.stdout)
    assert "exit_code" in stop_data or "error" not in stop_data

    # Verify it's no longer running
    time.sleep(1)
    after = run_cli("list")
    info_after = extract_json(after.stdout)
    match_after = next((p for p in info_after["processes"] if p["pid"] == pid), None)
    if match_after:
        assert match_after["status"] != "running"


def test_get_process_output_stderr(server):
    """Test get_output with stderr stream."""
    # Use a script that writes to stderr
    start_cmd = "python -c \"import sys; import time; [print('error', i, file=sys.stderr) or time.sleep(0.1) for i in range(20)]\""
    start = run_cli("start", start_cmd)
    data = extract_json(start.stdout)
    pid = data["pid"]

    time.sleep(1)

    # Get stderr output
    output = run_cli("get-output", str(pid), "stderr", "--lines", "5")
    output_data = extract_json(output.stdout)
    output_lines = output_data["output"]
    assert isinstance(output_lines, list)
    assert len(output_lines) > 0
    # Check that stderr output contains "error"
    stderr_text = "".join(output_lines)
    assert "error" in stderr_text

    # Cleanup
    run_cli("stop", str(pid))


def test_get_process_output_with_lines_limit(server):
    """Test get_output with lines parameter."""
    start_cmd = f"python {COUNTER_SCRIPT} --num-iterations 0"
    start = run_cli("start", start_cmd)
    data = extract_json(start.stdout)
    pid = data["pid"]

    time.sleep(2)  # Let it generate some output

    # Get limited output
    output = run_cli("get-output", str(pid), "stdout", "--lines", "3")
    output_data = extract_json(output.stdout)
    output_lines = output_data["output"]
    assert isinstance(output_lines, list)
    assert len(output_lines) <= 3

    # Cleanup
    run_cli("stop", str(pid))


def test_get_process_output_with_time_filters(server):
    """Test get_output with before_time and since_time parameters."""
    start_cmd = f"python {COUNTER_SCRIPT} --num-iterations 0"
    start = run_cli("start", start_cmd)
    data = extract_json(start.stdout)
    pid = data["pid"]

    time.sleep(1)

    # Record a timestamp
    import datetime

    timestamp = datetime.datetime.now().isoformat()

    time.sleep(1)

    # Get output since timestamp
    output = run_cli(
        "get-output",
        str(pid),
        "stdout",
        "--lines",
        "10",
        "--since-time",
        timestamp,
    )
    output_data = extract_json(output.stdout)
    assert isinstance(output_data["output"], list)

    # Get output before a future timestamp
    future_timestamp = datetime.datetime.now().isoformat()
    output = run_cli(
        "get-output",
        str(pid),
        "stdout",
        "--lines",
        "10",
        "--before-time",
        future_timestamp,
    )
    output_data = extract_json(output.stdout)
    assert isinstance(output_data["output"], list)

    # Cleanup
    run_cli("stop", str(pid))


# ---------------------------------------------------------------------------
# Tests for `persistproc run`
# ---------------------------------------------------------------------------


def test_run_kills_process_on_exit(server):
    """`run` starts new process and stops it on Ctrl+C when --on-exit stop."""

    cmd_tokens = ["python", str(COUNTER_SCRIPT), "--num-iterations", "0"]
    run_proc = start_run(cmd_tokens, on_exit="stop")

    time.sleep(3)

    # Verify that a process was actually started and get its PID
    initial_list = run_cli("list")
    initial_info = extract_json(initial_list.stdout)
    assert len(initial_info["processes"]) >= 1, (
        f"No process was started. Server response: {initial_list.stdout}"
    )

    target_pid = initial_info["processes"][0]["pid"]

    # Verify the process is actually running at the OS level
    try:
        os.kill(target_pid, 0)  # Signal 0 just checks if process exists
        process_exists_before = True
    except (OSError, ProcessLookupError):
        process_exists_before = False

    assert process_exists_before, (
        f"Process {target_pid} was not actually running at OS level"
    )

    # Terminate run gracefully.
    stop_run(run_proc)

    # Wait for the process to actually be stopped (up to 10 seconds)
    # This fixes the race condition where the test checks status before the stop completes
    deadline = time.time() + 30.0
    stopped_successfully = False
    while time.time() < deadline:
        listed = run_cli("list")
        info = extract_json(listed.stdout)
        # Process is considered stopped if either:
        # 1. It exists but is not running, or
        # 2. It has been completely removed from the list
        if (
            len(info["processes"]) == 1 and info["processes"][0]["status"] != "running"
        ) or (len(info["processes"]) == 0):
            stopped_successfully = True
            break
        time.sleep(0.5)

    # After run exits, verify the process was properly stopped
    listed = run_cli("list")
    info = extract_json(listed.stdout)

    # The process should either be stopped or completely removed
    assert stopped_successfully, "Process was not stopped within the timeout period"

    if len(info["processes"]) == 1:
        # Process exists but should not be running
        assert info["processes"][0]["status"] != "running"
    else:
        # Process was completely removed, which is also acceptable
        assert len(info["processes"]) == 0

    # Most importantly: verify the process is actually dead at the OS level
    try:
        os.kill(target_pid, 0)  # Signal 0 just checks if process exists
        process_still_exists = True
    except (OSError, ProcessLookupError):
        process_still_exists = False

    assert not process_still_exists, (
        f"Process {target_pid} is still running at OS level after stop"
    )


def test_run_detach_keeps_process_running(server):
    """`run` with --on-exit detach leaves the managed process running."""

    # 1. Start `run` with `detach` and let it create a new process.
    cmd_tokens = ["python", str(COUNTER_SCRIPT), "--num-iterations", "0"]
    run_proc = start_run(cmd_tokens, on_exit="detach")

    # Give it a moment to start up and for the server to register it.
    time.sleep(3)

    # 2. Find the PID of the process managed by `run`.
    listed = run_cli("list")
    info = extract_json(listed.stdout)
    procs = info["processes"]
    assert len(procs) == 1, "Expected exactly one process to be running"
    proc_dict = procs[0]
    assert proc_dict["status"] == "running"
    pid = proc_dict["pid"]

    # 3. Terminate the `run` command itself.
    stop_run(run_proc)

    # 4. Verify the managed process is still running because of `detach`.
    after = run_cli("list")
    info_after = extract_json(after.stdout)
    proc_after = next((p for p in info_after["processes"] if p["pid"] == pid), None)

    assert proc_after is not None, (
        f"Process with PID {pid} disappeared after run detached"
    )
    assert proc_after["status"] == "running"

    # 5. Cleanup.
    run_cli("stop", str(pid))
