from __future__ import annotations

# Comprehensive ProcessManager implementation.
# Standard library imports
import logging
import os
import shlex
import signal
import subprocess
import threading
import time
import traceback
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from persistproc.process_types import (
    ListProcessesResult,
    ProcessInfo,
    ProcessLogPathsResult,
    ProcessOutputResult,
    ProcessStatusResult,
    RestartProcessResult,
    StartProcessResult,
    StopProcessResult,
)

__all__ = ["ProcessManager"]

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Small utilities (duplicated from *before_rewrite.utils* to avoid dependency)
# ---------------------------------------------------------------------------


def _get_iso_ts() -> str:  # noqa: D401 – helper
    return (
        datetime.now(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


def _escape_cmd(cmd: str, max_len: int = 50) -> str:  # noqa: D401 – helper
    """Return *cmd* sanitised for use in filenames."""
    import re

    cmd = re.sub(r"\s+", "_", cmd)
    cmd = re.sub(r"[^a-zA-Z0-9_-]", "", cmd)
    return cmd[:max_len]


def get_label(
    explicit_label: str | None, command: str, working_directory: str | None
) -> str:
    """Generate a process label from explicit label or command + working directory."""
    if explicit_label:
        return explicit_label

    # Generate default label in format '<cmd> in <wd>'
    cmd_display = command if len(command) <= 50 else command[:47] + "..."
    wd_display = working_directory or "."
    return f"{cmd_display} in {wd_display}"


# Interval for the monitor thread (overridable for tests)
_POLL_INTERVAL = float(os.environ.get("PERSISTPROC_TEST_POLL_INTERVAL", "1.0"))


@dataclass
class _ProcEntry:  # noqa: D401 – internal state
    pid: int
    command: list[str]
    working_directory: str | None
    environment: dict[str, str] | None
    start_time: str
    status: str  # running | exited | terminated | failed
    log_prefix: str
    label: str
    exit_code: int | None = None
    exit_time: str | None = None
    # Keep a reference so we can signal/poll. Excluded from comparisons.
    proc: subprocess.Popen | None = field(repr=False, compare=False, default=None)


class _LogManager:
    """Handle per-process log files & pump threads."""

    @dataclass(slots=True)
    class _LogPaths:  # noqa: D401 – lightweight value object
        stdout: Path
        stderr: Path
        combined: Path

        # Make the instance behave *partly* like a mapping for legacy uses.
        def __getitem__(self, item: str) -> Path:  # noqa: D401 – mapping convenience
            return getattr(self, item)

        def __contains__(self, item: str) -> bool:  # noqa: D401 – mapping convenience
            return hasattr(self, item)

    def __init__(self, base_dir: Path):
        self._dir = base_dir
        self._dir.mkdir(parents=True, exist_ok=True)

    # -------------------------------
    # Public helpers
    # -------------------------------

    def paths_for(self, prefix: str) -> _LogPaths:  # noqa: D401
        return self._LogPaths(
            stdout=self._dir / f"{prefix}.stdout",
            stderr=self._dir / f"{prefix}.stderr",
            combined=self._dir / f"{prefix}.combined",
        )

    def start_pumps(self, proc: subprocess.Popen, prefix: str) -> None:  # noqa: D401
        paths = self.paths_for(prefix)

        # open in text mode – we add timestamps manually
        stdout_fh = paths.stdout.open("a", encoding="utf-8")
        stderr_fh = paths.stderr.open("a", encoding="utf-8")
        comb_fh = paths.combined.open("a", encoding="utf-8")

        def _pump(src: subprocess.PIPE, primary, secondary) -> None:  # type: ignore[type-arg]
            # Blocking read; releases GIL.
            for b_line in iter(src.readline, b""):
                line = b_line.decode("utf-8", errors="replace")
                ts_line = f"{_get_iso_ts()} {line}"
                primary.write(ts_line)
                primary.flush()
                secondary.write(ts_line)
                secondary.flush()
            src.close()
            primary.close()

        threading.Thread(
            target=_pump, args=(proc.stdout, stdout_fh, comb_fh), daemon=True
        ).start()
        threading.Thread(
            target=_pump, args=(proc.stderr, stderr_fh, comb_fh), daemon=True
        ).start()

        def _close_combined() -> None:
            proc.wait()
            comb_fh.close()

        threading.Thread(target=_close_combined, daemon=True).start()


class ProcessManager:  # noqa: D101
    def __init__(self) -> None:  # noqa: D401 – simple init
        self.data_dir: Path | None = None
        self._log_dir: Path | None = None
        self._server_log_path: Path | None = None

        self._processes: dict[int, _ProcEntry] = {}
        self._lock = threading.Lock()
        self._stop_evt = threading.Event()

        # monitor thread is started on first *bootstrap*
        self._monitor_thread: threading.Thread | None = None
        self._log_mgr: _LogManager | None = None

    # ------------------------------------------------------------------
    # Lifecycle helpers
    # ------------------------------------------------------------------

    def bootstrap(self, data_dir: Path, server_log_path: Path | None = None) -> None:  # noqa: D401
        """Must be called exactly once after CLI parsed *--data-dir*."""
        self.data_dir = data_dir
        self._log_dir = data_dir / "process_logs"
        self._server_log_path = server_log_path
        self._log_mgr = _LogManager(self._log_dir)

        if self._monitor_thread is None:
            self._monitor_thread = threading.Thread(
                target=self._monitor_loop, daemon=True
            )
            self._monitor_thread.start()

        logger.debug("ProcessManager bootstrapped dir=%s", data_dir)

    def shutdown(self) -> None:  # noqa: D401
        """Signal the monitor thread to exit (used by tests)."""
        self._stop_evt.set()
        if self._monitor_thread:
            self._monitor_thread.join(timeout=2)

    # ------------------------------------------------------------------
    # Core API – exposed via CLI & MCP tools
    # ------------------------------------------------------------------

    # NOTE: The docstrings are intentionally minimal – rich help is provided
    #       in *tools.py* and the CLI.

    def start(
        self,
        command: str,
        working_directory: Path | None = None,
        environment: dict[str, str] | None = None,
        label: str | None = None,
    ) -> StartProcessResult:  # noqa: D401
        if self._log_mgr is None:
            raise RuntimeError("ProcessManager.bootstrap() must be called first")

        logger.debug("start: received command=%s type=%s", command, type(command))

        # Generate label before duplicate check
        process_label = get_label(
            label, command, str(working_directory) if working_directory else None
        )

        # Prevent duplicate *running* labels (helps humans)
        logger.debug("start: acquiring lock")
        with self._lock:
            logger.debug("start: lock acquired")
            for ent in self._processes.values():
                # Check for duplicate labels in running processes
                if ent.label == process_label and ent.status == "running":
                    raise ValueError(
                        f"Process with label '{process_label}' already running with PID {ent.pid}."
                    )
        logger.debug("start: lock released")

        if working_directory and not working_directory.is_dir():
            raise ValueError(f"Working directory '{working_directory}' does not exist.")

        diagnostic_info_for_errors = {
            "command": command,
            "working_directory": str(working_directory) if working_directory else None,
        }

        try:
            proc = subprocess.Popen(  # noqa: S603 – user command
                shlex.split(command),
                cwd=str(working_directory) if working_directory else None,
                env={**os.environ, **(environment or {})},
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                close_fds=True,
                # Put the child in a different process group so a SIGINT will
                # kill only the child, not the whole process group.
                preexec_fn=os.setsid if os.name != "nt" else None,
            )
        except FileNotFoundError as exc:
            return StartProcessResult(
                error=f"Command not found: {exc.filename}\n\n{diagnostic_info_for_errors}"
            )
        except PermissionError as exc:
            return StartProcessResult(
                error=f"Permission denied: {exc.filename}\n\n{diagnostic_info_for_errors}"
            )
        except Exception as exc:  # pragma: no cover – safety net
            return StartProcessResult(
                error=f"Failed to start process: {exc}\n\n{traceback.format_exc()}"
            )

        prefix = f"{proc.pid}.{_escape_cmd(command)}"
        self._log_mgr.start_pumps(proc, prefix)

        ent = _ProcEntry(
            pid=proc.pid,
            command=shlex.split(command),
            working_directory=str(working_directory) if working_directory else None,
            environment=environment,
            start_time=_get_iso_ts(),
            status="running",
            log_prefix=prefix,
            label=process_label,
            proc=proc,
        )

        logger.debug("start: acquiring lock for update")
        with self._lock:
            logger.debug("start: lock acquired for update")
            self._processes[proc.pid] = ent
        logger.debug("start: lock released after update")

        logger.info("Process %s started", proc.pid)
        logger.debug(
            "event=start pid=%s cmd=%s cwd=%s log_prefix=%s",
            proc.pid,
            shlex.join(ent.command),
            ent.working_directory,
            prefix,
        )
        return StartProcessResult(
            pid=proc.pid,
            log_stdout=self._log_mgr.paths_for(prefix).stdout,
            log_stderr=self._log_mgr.paths_for(prefix).stderr,
            log_combined=self._log_mgr.paths_for(prefix).combined,
            label=process_label,
        )

    # ------------------------------------------------------------------
    # Query helpers
    # ------------------------------------------------------------------

    def list(self) -> ListProcessesResult:  # noqa: D401
        logger.debug("list: acquiring lock")
        with self._lock:
            logger.debug("list: lock acquired")
            res = [self._to_public_info(ent) for ent in self._processes.values()]
        logger.debug("list: lock released")
        return ListProcessesResult(processes=res)

    def get_status(self, pid: int) -> ProcessStatusResult:  # noqa: D401
        logger.debug("get_status: acquiring lock for pid=%d", pid)
        with self._lock:
            logger.debug("get_status: lock acquired for pid=%d", pid)
            try:
                ent = self._require_unlocked(pid)
                result = ProcessStatusResult(
                    pid=ent.pid,
                    command=ent.command,
                    working_directory=ent.working_directory,
                    status=ent.status,
                    label=ent.label,
                )
                logger.debug("get_status: lock released for pid=%d", pid)
                return result
            except ValueError as e:
                logger.debug("get_status: lock released for pid=%d", pid)
                # Re-raise as a standard error type for the tool wrapper
                raise ValueError(str(e)) from e

    # ------------------------------------------------------------------
    # Control helpers
    # ------------------------------------------------------------------

    def stop(
        self,
        pid: int | None = None,
        command: str | None = None,
        working_directory: Path | None = None,
        force: bool = False,
        label: str | None = None,
    ) -> StopProcessResult:  # noqa: D401
        if pid is None and command is None and label is None:
            return StopProcessResult(
                error="Either pid, command, or label must be provided to stop"
            )

        pid_to_stop: int | None = None

        if pid is not None:
            pid_to_stop = pid
        elif label is not None:
            # Find PID from label, if not found try as command
            logger.debug("stop: acquiring lock to find pid by label")
            with self._lock:
                logger.debug("stop: lock acquired to find pid by label")
                # First try to match by label
                for p in self._processes.values():
                    if p.label == label and p.status == "running":
                        pid_to_stop = p.pid
                        break

                # If not found by label, try to match as command with working directory
                # Note: This fallback is only for when a full command string is passed as a label
                if pid_to_stop is None:
                    for p in self._processes.values():
                        if (
                            p.command == shlex.split(label)
                            and p.working_directory == str(working_directory)
                            and p.status == "running"
                        ):
                            pid_to_stop = p.pid
                            break
            logger.debug("stop: lock released after finding pid by label/command")
        elif command is not None:
            # Find PID from command + CWD
            logger.debug("stop: acquiring lock to find pid")
            with self._lock:
                logger.debug("stop: lock acquired to find pid")
                for p in self._processes.values():
                    if (
                        p.command == shlex.split(command)
                        and p.working_directory
                        == (str(working_directory) if working_directory else "")
                        and p.status == "running"
                    ):
                        pid_to_stop = p.pid
                        break
            logger.debug("stop: lock released after finding pid")
        else:
            raise ValueError("stop requires pid, command, or label")

        if pid_to_stop is None:
            return StopProcessResult(error="Process not found")

        logger.debug("stop: acquiring lock for pid=%d", pid_to_stop)
        with self._lock:
            logger.debug("stop: lock acquired for pid=%d", pid_to_stop)
            try:
                ent = self._require_unlocked(pid_to_stop)
            except ValueError as e:
                logger.debug("stop: lock released for pid=%d", pid_to_stop)
                return StopProcessResult(error=str(e))
        logger.debug("stop: lock released for pid=%d", pid_to_stop)

        if ent.status != "running":
            return StopProcessResult(error=f"Process {pid_to_stop} is not running")

        # Send SIGTERM first for graceful shutdown
        try:
            self._send_signal(pid_to_stop, signal.SIGTERM)
            logger.debug("Sent SIGTERM to pid=%s", pid_to_stop)
        except ProcessLookupError:
            # Process already gone
            pass

        timeout = 8.0  # XXX TIMEOUT – graceful wait
        exited = self._wait_for_exit(ent.proc, timeout)
        if not exited and not force:
            # Escalate to SIGKILL once and wait briefly.
            try:
                self._send_signal(pid_to_stop, signal.SIGKILL)
                logger.warning("Escalated to SIGKILL pid=%s", pid_to_stop)
            except ProcessLookupError:
                pass  # Process vanished between checks.

            exited = self._wait_for_exit(ent.proc, 2.0)  # XXX TIMEOUT – short

        if not exited:
            logger.error("event=stop_timeout pid=%s", pid_to_stop)
            return StopProcessResult(error="timeout")

        # Process exited – record metadata.
        with self._lock:
            ent.status = "terminated"
            ent.proc = None
            if ent.exit_code is None:
                ent.exit_code = 0
            ent.exit_time = _get_iso_ts()

        logger.debug("event=stopped pid=%s exit_code=%s", pid_to_stop, ent.exit_code)
        return StopProcessResult(exit_code=ent.exit_code)

    def restart(
        self,
        pid: int | None = None,
        command: str | None = None,
        working_directory: Path | None = None,
        label: str | None = None,
    ) -> RestartProcessResult:  # noqa: D401
        """Attempt to stop then start *pid*.

        On success returns ``RestartProcessResult(pid=new_pid)`` for parity with
        :py:meth:`stop`.  If stopping timed-out the same
        ``RestartProcessResult`` with ``error='timeout'`` is propagated so callers
        can decide how to handle the failure.
        """
        logger.debug(
            "restart: pid=%s, command=%s, cwd=%s",
            pid,
            command,
            working_directory,
        )

        pid_to_restart: int | None = pid

        if pid_to_restart is None and label:
            # Find PID from label, if not found try as command
            logger.debug("restart: acquiring lock to find pid by label")
            with self._lock:
                logger.debug("restart: lock acquired to find pid by label")
                # First try to match by label
                for p in self._processes.values():
                    if p.label == label and p.status == "running":
                        pid_to_restart = p.pid
                        break

                # If not found by label, try to match as command with working directory
                # Note: This fallback is only for when a full command string is passed as a label
                if pid_to_restart is None:
                    for p in self._processes.values():
                        if (
                            p.command == shlex.split(label)
                            and p.working_directory == str(working_directory)
                            and p.status == "running"
                        ):
                            pid_to_restart = p.pid
                            break
            logger.debug("restart: lock released after finding pid by label/command")
        elif pid_to_restart is None and command:
            logger.debug("restart: acquiring lock to find pid")
            with self._lock:
                logger.debug("restart: lock acquired to find pid")
                for p in self._processes.values():
                    if (
                        p.command == shlex.split(command)
                        and p.working_directory
                        == (str(working_directory) if working_directory else "")
                        and p.status == "running"
                    ):
                        pid_to_restart = p.pid
                        break
            logger.debug("restart: lock released after finding pid")

        if pid_to_restart is None:
            return RestartProcessResult(error="Process not found to restart.")

        logger.debug("restart: acquiring lock for pid=%d", pid_to_restart)
        with self._lock:
            logger.debug("restart: lock acquired for pid=%d", pid_to_restart)
            try:
                original_entry = self._require_unlocked(pid_to_restart)
            except ValueError:
                logger.debug("restart: lock released for pid=%d", pid_to_restart)
                return RestartProcessResult(
                    error=f"Process with PID {pid_to_restart} not found."
                )
        logger.debug("restart: lock released for pid=%d", pid_to_restart)

        # Retain original parameters for restart
        original_command_list = original_entry.command
        logger.debug(
            "restart: original_command_list=%s type=%s",
            original_command_list,
            type(original_command_list),
        )
        original_command_str = shlex.join(original_command_list)
        logger.debug(
            "restart: original_command_str=%s type=%s",
            original_command_str,
            type(original_command_str),
        )
        cwd = (
            Path(original_entry.working_directory)
            if original_entry.working_directory
            else None
        )
        env = original_entry.environment

        stop_res = self.stop(pid_to_restart, force=False)
        if stop_res.error is not None:
            # Forward failure.
            return RestartProcessResult(error=stop_res.error)

        logger.debug(
            "restart: calling start with command=%s type=%s",
            original_command_str,
            type(original_command_str),
        )
        start_res = self.start(
            original_command_str,
            working_directory=cwd,
            environment=env,
            label=original_entry.label,
        )

        logger.debug(
            "event=restart pid_old=%s pid_new=%s", pid_to_restart, start_res.pid
        )

        return RestartProcessResult(pid=start_res.pid)

    # ------------------------------------------------------------------
    # Output helpers
    # ------------------------------------------------------------------

    def get_output(
        self,
        pid: int,
        stream: str,
        lines: int | None = None,
        before_time: str | None = None,
        since_time: str | None = None,
    ) -> ProcessOutputResult:  # noqa: D401
        logger.debug("get_output: acquiring lock for pid=%d", pid)
        with self._lock:
            logger.debug("get_output: lock acquired for pid=%d", pid)
            try:
                ent = self._require_unlocked(pid)
            except ValueError:
                logger.debug("get_output: lock released for pid=%d", pid)
                return ProcessOutputResult(output=[])  # Soft fail
        logger.debug("get_output: lock released for pid=%d", pid)

        if self._log_mgr is None:
            raise RuntimeError("Log manager not available")

        if pid == 0:
            # Special case – read the main CLI/server log file if known.
            if self._server_log_path and self._server_log_path.exists():
                with self._server_log_path.open("r", encoding="utf-8") as fh:
                    all_lines = fh.readlines()
                return ProcessOutputResult(output=all_lines)
            return ProcessOutputResult(output=[])  # Unknown path – empty

        paths = self._log_mgr.paths_for(ent.log_prefix)
        if stream not in paths:
            raise ValueError("stream must be stdout|stderr|combined")
        path = paths[stream]
        if not path.exists():
            return ProcessOutputResult(output=[])

        with path.open("r", encoding="utf-8") as fh:
            all_lines = fh.readlines()

        # Optional ISO filtering (copied from previous implementation)
        def _parse_iso(ts: str) -> datetime:
            if ts.endswith("Z"):
                ts = ts[:-1] + "+00:00"
            return datetime.fromisoformat(ts)

        if since_time:
            since_dt = _parse_iso(since_time)
            all_lines = [
                ln for ln in all_lines if _parse_iso(ln.split(" ", 1)[0]) >= since_dt
            ]
        if before_time:
            before_dt = _parse_iso(before_time)
            all_lines = [
                ln for ln in all_lines if _parse_iso(ln.split(" ", 1)[0]) < before_dt
            ]

        if lines is not None:
            all_lines = all_lines[-lines:]

        return ProcessOutputResult(output=all_lines)

    def get_log_paths(self, pid: int) -> ProcessLogPathsResult:  # noqa: D401
        logger.debug("get_log_paths: acquiring lock for pid=%d", pid)
        with self._lock:
            logger.debug("get_log_paths: lock acquired for pid=%d", pid)
            ent = self._require_unlocked(pid)
            logger.debug("get_log_paths: lock released for pid=%d", pid)

        if self._log_mgr is None:
            raise RuntimeError("Log manager not available")

        paths = self._log_mgr.paths_for(ent.log_prefix)
        return ProcessLogPathsResult(stdout=str(paths.stdout), stderr=str(paths.stderr))

    def kill_persistproc(self) -> dict[str, int]:  # noqa: D401
        """Kill all managed processes and then kill the server process."""
        server_pid = os.getpid()
        logger.info("event=kill_persistproc_start server_pid=%s", server_pid)

        # Get a snapshot of all processes to kill
        with self._lock:
            processes_to_kill = list(self._processes.values())

        if not processes_to_kill:
            logger.debug("event=kill_persistproc_no_processes")
        else:
            logger.debug(
                "event=kill_persistproc_killing_processes count=%s",
                len(processes_to_kill),
            )

        # Kill each process
        for ent in processes_to_kill:
            if ent.status == "running":
                logger.debug(
                    "event=kill_persistproc_stopping pid=%s command=%s",
                    ent.pid,
                    " ".join(ent.command),
                )
                try:
                    self.stop(ent.pid, force=True)
                    logger.debug("event=kill_persistproc_stopped pid=%s", ent.pid)
                except Exception as e:
                    logger.warning(
                        "event=kill_persistproc_failed pid=%s error=%s", ent.pid, e
                    )
            else:
                logger.debug(
                    "event=kill_persistproc_skip pid=%s status=%s", ent.pid, ent.status
                )

        logger.info("event=kill_persistproc_complete server_pid=%s", server_pid)

        # Schedule server termination after a brief delay to allow response to be sent
        def _kill_server():
            time.sleep(0.1)  # Brief delay to allow response to be sent
            logger.info("event=kill_persistproc_terminating_server pid=%s", server_pid)
            os.kill(server_pid, signal.SIGTERM)

        threading.Thread(target=_kill_server, daemon=True).start()

        return {"pid": server_pid}

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _require(self, pid: int) -> _ProcEntry:  # noqa: D401 – helper
        with self._lock:
            if pid not in self._processes:
                raise ValueError(f"PID {pid} not found")
            return self._processes[pid]

    def _require_unlocked(self, pid: int) -> _ProcEntry:  # noqa: D401 – helper (assumes lock held)
        if pid not in self._processes:
            raise ValueError(f"PID {pid} not found")
        return self._processes[pid]

    def _to_public_info(self, ent: _ProcEntry) -> ProcessInfo:  # noqa: D401 – helper
        return ProcessInfo(
            pid=ent.pid,
            command=ent.command,
            working_directory=ent.working_directory or "",
            status=ent.status,
            label=ent.label,
        )

    def _monitor_loop(self) -> None:  # noqa: D401 – thread target
        logger.debug("Monitor thread starting")

        while not self._stop_evt.is_set():
            logger.debug("event=monitor_tick_start num_procs=%d", len(self._processes))

            logger.debug("monitor_loop: acquiring lock")
            with self._lock:
                logger.debug("monitor_loop: lock acquired")
                procs_to_check = list(self._processes.values())

                for ent in procs_to_check:
                    if ent.status != "running" or ent.proc is None:
                        continue  # Skip non-running processes

                    if ent.proc.poll() is not None:
                        # Process has exited.
                        ent.status = "exited"
                        ent.exit_code = ent.proc.returncode
                        ent.exit_time = _get_iso_ts()
                        logger.info(
                            "Process %s exited with code %s", ent.pid, ent.exit_code
                        )
                logger.debug(
                    "monitor_loop: lock released, checked %d procs",
                    len(procs_to_check),
                )

            logger.debug("event=monitor_tick_end")
            time.sleep(_POLL_INTERVAL)

        logger.debug("Monitor thread exiting")

    # ------------------ signal helpers ------------------

    @staticmethod
    def _send_signal(pid: int, sig: signal.Signals) -> None:  # noqa: D401
        if os.name == "nt":
            # Windows – no process groups, best-effort
            os.kill(pid, sig.value)  # type: ignore[attr-defined]
        else:
            os.killpg(os.getpgid(pid), sig)  # type: ignore[arg-type]

    @staticmethod
    def _wait_for_exit(proc: subprocess.Popen | None, timeout: float) -> bool:  # noqa: D401
        if proc is None:
            return True
        logger.debug(
            "event=wait_for_exit pid=%s timeout=%s", getattr(proc, "pid", None), timeout
        )
        try:
            proc.wait(timeout=timeout)
            logger.debug(
                "event=wait_for_exit_done pid=%s exited=True",
                getattr(proc, "pid", None),
            )
            return True
        except subprocess.TimeoutExpired:
            logger.debug(
                "event=wait_for_exit_done pid=%s exited=False",
                getattr(proc, "pid", None),
            )
            return False
