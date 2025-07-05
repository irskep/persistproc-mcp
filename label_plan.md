# Custom Process Label Implementation Plan

## Overview

Add support for custom labels to make process identification more user-friendly. Currently, processes are identified by their command + working directory combination. This plan adds optional custom labels while maintaining the existing PID-based runtime identification.

## Current Identity Model

**Primary Identifier**: PID (process ID) - used for all operations after process starts  
**Secondary Identifier**: Command + Working Directory combination - used for:
- Preventing duplicate processes (process_manager.py:202-210)
- Finding processes by command for stop/restart (process_manager.py:338-345, 428-435)
- Identity check: `ent.command == shlex.split(command) and ent.working_directory == str(working_directory)`

## Proposed Label System

**Default Label**: Generated from command + working directory if not provided  
**Custom Label**: User-provided string for better identification  
**Benefits**:
- Better identification of multiple similar processes (like `cat` from piping)
- Human-friendly names for complex commands
- Easier management via agents
- Semantic meaning for processes

## Implementation Checklist

### 1. Data Structure Updates

- [ ] Add `label: str | None` field to `_ProcEntry` dataclass in process_manager.py:59
- [ ] Add `label: str` field to `ProcessInfo` dataclass in process_types.py:48
- [ ] Add `label: str` field to `ProcessStatusResult` dataclass in process_types.py:61
- [ ] Update `StartProcessResult` to include the assigned label

### 2. Process Manager Core Changes

- [ ] Modify `start()` method to accept optional `label` parameter
- [ ] Generate default label from command + working_directory if not provided
- [ ] Store label in `_ProcEntry` when creating process
- [ ] Update duplicate detection logic to use labels instead of command+cwd
- [ ] Modify `_to_public_info()` to include label in ProcessInfo

### 3. Process Lookup Updates

- [ ] Add label-based lookup to `stop()` method (alongside pid and command)
- [ ] Add label-based lookup to `restart()` method
- [ ] Update `get_status()` to optionally accept label
- [ ] Consider making labels unique (enforce or warn on duplicates)

### 4. CLI Updates

- [ ] Add `--label` flag to `persistproc run` command
- [ ] Add `--label` flag to `persistproc start` command
- [ ] Update `persistproc capture` alias to auto-generate meaningful labels
- [ ] Allow stop/restart commands to accept labels

### 5. Tool Updates

- [ ] Update `start` tool to accept label parameter
- [ ] Update `stop` tool to accept label for process identification
- [ ] Update `restart` tool to accept label
- [ ] Update `list` tool output to show labels

### 6. Display/Logging

- [ ] Update process list display to show labels prominently
- [ ] Consider using label in log file naming (or keep PID-based for stability)
- [ ] Update logging to include label in debug messages

### 7. Backwards Compatibility

- [ ] Ensure all operations work without labels (use default)
- [ ] Make label optional in all APIs
- [ ] Handle migration for existing running processes without labels

### 8. Testing

- [ ] Test duplicate label detection/handling
- [ ] Test label-based stop/restart
- [ ] Test default label generation
- [ ] Test special characters in labels
- [ ] Test very long labels

## Usage Examples

### With Custom Labels
```bash
# Start processes with custom labels
persistproc run --label "my-api" npm start
persistproc run --label "frontend" npm run dev

# Capture with labels
my-server | persistproc capture --label "my-api"
my-frontend | persistproc capture --label "react-dev"

# Stop/restart by label
persistproc stop --label "my-api"
persistproc restart --label "frontend"
```

### Default Label Generation
```bash
# Without custom label, generates default from command + working directory
persistproc run npm start
# Default label: "npm start (/path/to/project)"

docker-compose up | persistproc capture
# Default label: "cat (/path/to/project)"
```

## Key Design Decisions

1. **Labels are optional** - All existing functionality works without labels
2. **Default label generation** - Combines command + working directory for backward compatibility
3. **Label uniqueness** - Should warn or prevent duplicate labels for clarity
4. **PID remains primary** - Runtime operations still use PID for efficiency
5. **Flexible lookup** - Stop/restart can use PID, command, or label

## Benefits for Capture Use Case

This feature is particularly valuable for the pipe/capture functionality:

```bash
# Multiple capture processes become distinguishable
docker-compose up | persistproc capture --label "docker-stack"
npm run build:watch | persistproc capture --label "build-watcher"
tail -f /var/log/app.log | persistproc capture --label "app-logs"
```

Instead of multiple generic "cat" processes, agents will see clearly labeled processes that indicate their purpose and source.