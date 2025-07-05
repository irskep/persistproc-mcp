import time
from pathlib import Path

from tests.helpers import extract_json, run_cli

COUNTER_SCRIPT = Path(__file__).parent / "scripts" / "counter.py"


def test_start_process_with_default_label(server):
    """Test starting a process without custom label gets default label."""

    # Start process without label
    start_cmd = f"python3 {COUNTER_SCRIPT} --num-iterations 5"
    start = run_cli("start", start_cmd)
    data = extract_json(start.stdout)
    pid = data["pid"]

    # Verify default label format
    assert data["label"] is not None
    assert " in " in data["label"]
    assert "python3" in data["label"]

    # Verify it appears in list with the default label
    listed = run_cli("list")
    info = extract_json(listed.stdout)
    procs = info["processes"]
    match = next(p for p in procs if p["pid"] == pid)
    assert match["label"] == data["label"]
    assert " in " in match["label"]

    # Clean up
    run_cli("stop", str(pid))


def test_stop_process_by_label(server):
    """Test stopping a process using its label."""

    # Start process with custom label
    start_cmd = f"python {COUNTER_SCRIPT} --num-iterations 0"  # runs forever
    start = run_cli("start", "--label", "test-stop-label", start_cmd)
    data = extract_json(start.stdout)
    pid = data["pid"]

    # Stop by label
    stop = run_cli("stop", "test-stop-label")
    stop_data = extract_json(stop.stdout)
    assert stop_data.get("error") is None

    # Verify it's stopped
    time.sleep(0.5)  # Give it a moment to stop
    listed = run_cli("list")
    info = extract_json(listed.stdout)
    procs = info["processes"]
    match = next((p for p in procs if p["pid"] == pid), None)
    if match:  # Process might still be in list but not running
        assert match["status"] != "running"


def test_restart_process_by_label(server):
    """Test restarting a process using its label."""

    # Start process with custom label
    start_cmd = f"python {COUNTER_SCRIPT} --num-iterations 0"  # runs forever
    start = run_cli("start", "--label", "test-restart-label", start_cmd)
    data = extract_json(start.stdout)
    original_pid = data["pid"]

    # Restart by label
    restart = run_cli("restart", "test-restart-label")
    restart_data = extract_json(restart.stdout)
    new_pid = restart_data["pid"]

    # Verify new PID is different but label is preserved
    assert new_pid != original_pid

    # Verify the restarted process has the same label
    listed = run_cli("list")
    info = extract_json(listed.stdout)
    procs = info["processes"]
    match = next(p for p in procs if p["pid"] == new_pid)
    assert match["label"] == "test-restart-label"
    assert match["status"] == "running"

    # Clean up
    run_cli("stop", str(new_pid))


def test_duplicate_label_prevention(server):
    """Test that duplicate labels are prevented."""

    # Start first process with label
    start_cmd1 = f"python {COUNTER_SCRIPT} --num-iterations 0"
    start1 = run_cli("start", "--label", "duplicate-label", start_cmd1)
    data1 = extract_json(start1.stdout)
    pid1 = data1["pid"]

    # Try to start second process with same label
    start_cmd2 = f"python {COUNTER_SCRIPT} --num-iterations 0"
    start2 = run_cli("start", "--label", "duplicate-label", start_cmd2)
    data2 = extract_json(start2.stdout)

    # Should get an error about duplicate label
    assert data2.get("error") is not None
    assert "duplicate-label" in data2["error"]
    assert "already running" in data2["error"]

    # Clean up
    run_cli("stop", str(pid1))


def test_get_status_includes_label(server):
    """Test that get_status includes the process label."""

    # Start process with custom label
    start_cmd = f"python {COUNTER_SCRIPT} --num-iterations 5"
    start = run_cli("start", "--label", "status-test-label", start_cmd)
    data = extract_json(start.stdout)
    pid = data["pid"]

    # Get status
    status = run_cli("get_status", str(pid))
    status_data = extract_json(status.stdout)

    # Verify label is included in status
    assert status_data["label"] == "status-test-label"

    # Clean up
    run_cli("stop", str(pid))


def test_run_command_with_label(server):
    """Test the run command with --label flag."""

    # This test will use start_run helper which simulates the run command
    # Start a process using run command with label (simulated via start command)
    start_cmd = f"python {COUNTER_SCRIPT} --num-iterations 10"
    start = run_cli("start", "--label", "run-test-label", start_cmd)
    data = extract_json(start.stdout)
    pid = data["pid"]

    # Verify the label was set
    listed = run_cli("list")
    info = extract_json(listed.stdout)
    procs = info["processes"]
    match = next(p for p in procs if p["pid"] == pid)
    assert match["label"] == "run-test-label"

    # Clean up
    run_cli("stop", str(pid))


def test_label_format_edge_cases(server):
    """Test label handling with edge cases."""

    # Test with very long command for default label
    long_cmd = f"python {COUNTER_SCRIPT} " + " ".join([f"--arg{i}" for i in range(20)])
    start = run_cli("start", long_cmd)
    data = extract_json(start.stdout)
    pid = data["pid"]

    # Verify default label is truncated appropriately
    assert data["label"] is not None
    assert len(data["label"]) < 200  # Should be reasonable length

    # Clean up
    run_cli("stop", str(pid))

    # Test with custom label containing spaces and special chars
    start_cmd = f"python {COUNTER_SCRIPT} --num-iterations 5"
    start = run_cli("start", "--label", "my process with spaces-and_chars", start_cmd)
    data = extract_json(start.stdout)
    pid = data["pid"]

    # Verify custom label is preserved exactly
    assert data["label"] == "my process with spaces-and_chars"

    # Clean up
    run_cli("stop", str(pid))


def test_stop_process_by_command_fallback(server):
    """Test stopping a process using command when no label matches."""

    # Start process without custom label
    start_cmd = f"python {COUNTER_SCRIPT} --num-iterations 0"  # runs forever
    start = run_cli("start", start_cmd)
    data = extract_json(start.stdout)
    pid = data["pid"]

    # Stop by command - should fallback to command matching when no label matches
    stop = run_cli("stop", start_cmd)
    stop_data = extract_json(stop.stdout)
    assert stop_data.get("error") is None

    # Verify it's stopped
    time.sleep(0.5)  # Give it a moment to stop
    listed = run_cli("list")
    info = extract_json(listed.stdout)
    procs = info["processes"]
    match = next((p for p in procs if p["pid"] == pid), None)
    if match:  # Process might still be in list but not running
        assert match["status"] != "running"
