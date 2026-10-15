# opencode-watchtower

A `monitor` tool for [OpenCode](https://opencode.ai). It runs a command, tails a log file, or re-runs a probe on an interval, and wakes the agent only when output lines match a filter. Watches run in the background, so waiting on a build, a deploy, or a log does not cost the agent turns.

## Requirements

- OpenCode v1.x
- Node >= 20 (Bun runtime, as used by OpenCode itself)
- Linux, macOS, or Windows

## Install

Add the package to your OpenCode config.

`opencode.json`:

```json
{
  "$schema": "https://opencode.ai/config.json",
  "plugin": ["opencode-watchtower"]
}
```

OpenCode installs the package with Bun at startup. Restart your session. The `monitor` tool is then available to the agent alongside the built-in tools.

To try it without installing, copy `index.ts` to `~/.config/opencode/plugin/watchtower.ts`.

## What the tool does

`monitor` takes an `op` of `start`, `stop`, or `list`.

`op: "start"` arms a watch and returns immediately with an id. Three modes:

- `exec` (default): spawn a command and watch its stdout/stderr.
- `follow`: tail a log file directly, no shell needed. Resumes across rotation and truncation.
- `poll`: re-run a command on an interval and wake only when its output changes.

Each matched line is appended to a spool file on disk (lossless), and matching lines are batched into one wake message. The agent is woken with the batch; the wake text is fenced and treated as untrusted data.

Under load, near-identical lines collapse into repeat counts and wake bodies degrade to digests, but the spool keeps the full history. An `action` command can run on every matched batch without waking the agent, for scripted side effects.

`op: "list"` shows active watches. `op: "stop"` stops one.

## Examples

Watch a test run for failures:

```
monitor({
  op: "start",
  command: "npm run test -- --watch",
  description: "unit tests",
  filter: "FAIL|PASS|Error",
  timeout_ms: 600000,
  max_events: 20
})
```

Tail an app log for errors:

```
monitor({
  op: "start",
  mode: "follow",
  file: "/var/log/app.log",
  filter: "ERROR|FATAL",
  timeout_ms: 0,
  max_events: 0
})
```

Poll a server until it is ready:

```
monitor({
  op: "start",
  mode: "poll",
  command: "curl -sf localhost:3000/health",
  poll_interval_ms: 5000
})
```

## Arguments

| Field                  | Default      | Meaning                                                        |
| ---------------------- | ------------ | -------------------------------------------------------------- |
| `op`                   | `start`      | `start`, `stop`, `list`                                        |
| `command`              | required     | Shell command to run (`exec` and `poll` modes)                 |
| `file`                 | —            | Log file to tail (`follow` mode)                               |
| `mode`                 | `exec`       | `exec`, `follow`, `poll`                                       |
| `filter`               | match all    | Case-insensitive substring, `\|`-separated alternatives, or `re:/pattern/flags` |
| `description`          | —            | Label shown in wake messages                                   |
| `timeout_ms`           | per mode     | Watch deadline. `0` = no deadline                              |
| `max_events`           | 25           | Auto-stop after this many matches. `0` = unlimited             |
| `poll_interval_ms`     | 30000        | Poll period for `poll` mode. `0` = continuous (1s)             |
| `from_beginning`       | false        | `follow`: start at current end of file                         |
| `shell`                | auto         | `auto`, `bash`, `powershell`, `cmd`                            |
| `action`               | —            | Command run on every matched batch, before any wake            |
| `action_without_wake`  | false        | With `action` set, never wake the agent                        |
| `id`                   | —            | Watch id for `op: "stop"`                                      |

Defaults: `exec` watches stop after 5 minutes, `follow` and `poll` after 15 minutes (up to 30 minutes / 2 hours with `timeout_ms`). At most 8 watches at once.

## Permissions and safety

- `exec` and `poll` obey the same permission rules as the built-in bash tool.
- `follow` needs read access to the tailed file only.
- Matched output is delivered fenced and labeled as observed data. It is never executed or treated as instructions.

## Platform notes

- On Windows, watched commands run in `cmd` by default; pass `shell: "powershell"` to override.
- On Linux and macOS, watched commands run in `bash`.
- The orphan sweep on startup uses `/proc/<pid>/cmdline`. It works on Linux and is skipped on macOS; live watches are unaffected.

## License

MIT
