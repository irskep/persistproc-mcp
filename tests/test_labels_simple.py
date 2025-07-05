from pathlib import Path

from tests.helpers import extract_json, run_cli

COUNTER_SCRIPT = Path(__file__).parent / "scripts" / "counter.py"


def test_start_process_with_default_label(server):
    """Test starting a process without custom label gets default label."""

    # Start process without label
    start_cmd = f"python3 {COUNTER_SCRIPT} --num-iterations 3"
    start = run_cli("start", start_cmd)
    data = extract_json(start.stdout)

    # Verify we got a successful start
    assert data.get("error") is None
    assert data.get("pid") is not None

    # Verify label field exists and has expected format
    assert "label" in data
    assert data["label"] is not None
    assert " in " in data["label"]
    assert "python3" in data["label"]

    # Clean up
    pid = data["pid"]
    run_cli("stop", str(pid))


def test_start_process_with_custom_label(server):
    """Test starting a process with a custom label."""

    # Start process with custom label (using full command string)
    start_cmd = f"python3 {COUNTER_SCRIPT} --num-iterations 3"
    start = run_cli("start", "--label", "my-test-label", start_cmd)
    data = extract_json(start.stdout)

    # Verify we got a successful start
    assert data.get("error") is None
    assert data.get("pid") is not None

    # Verify the custom label is returned
    assert data["label"] == "my-test-label"

    # Verify it appears in list with the custom label
    listed = run_cli("list")
    info = extract_json(listed.stdout)
    procs = info["processes"]
    pid = data["pid"]
    match = next(p for p in procs if p["pid"] == pid)
    assert match["label"] == "my-test-label"

    # Clean up
    run_cli("stop", str(pid))
