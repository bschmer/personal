#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
cmd_logger.py — capture a command’s stdout (or piped stdin) into rotating log files,
serve Prometheus-style metrics and a live stream, support live reconfiguration,
and provide precise idle detection without polling.

REGEN PROMPT (copy-paste below into any code-gen assistant to recreate this script):

You are to write a single self-contained Python 3 script named `cmd_logger.py` (stdlib only; no external deps).
It captures a command’s stdout OR stdin, writes to rotating log files with rotation ONLY between line breaks
(unless explicitly forced), serves metrics and live stream over HTTP, and supports live reconfiguration.

### Input sources
- Run a command (default: `bpftrace BPFTRACE -p PID:pd-protod`) OR read from stdin.
- If stdin is piped AND the user did not explicitly set `--cmd`, ignore the default command and read stdin.

### CLI (short+long)
- `-c, --cmd` string; default `bpftrace BPFTRACE -p PID:pd-protod`.
- `-b, --bpftrace` (repeatable): Each item can be a **file path**, a **glob pattern**, or **inline code**.
  Resolution order: glob matches (sorted) → plain file → inline (use `inline:...` to force inline).
  Concatenate all resolved sources into one temp file and replace `BPFTRACE` in `--cmd` with that file path.
  Default source is `/usr/bin/replication_monitor.bt` if no `-b` provided.
- Replace `PID:<procname>` in `--cmd` with `pidof -s <procname>` (if missing, leave token and warn to stderr).
- `-o, --out-dir` (default `.`); `-n, --prefix` (default `capture`).
- `-s, --max-bytes` rotate by size; accepts `123`, `10K`, `25M`, `1G`, ... (uncompressed byte count).
- `-t, --interval` rotate by time; accepts seconds or `10s`, `5m`, `2h`, `1d` (rotation still occurs only on line breaks).
- `-z, --compress` one of `none|inline|after`:
  - none: write plaintext
  - inline: write gzip as-you-go (`*.current.log.gz`)
  - after: write plaintext then gzip the rotated file asynchronously
- `-r, --retain` keep newest N rotated logs (default 10).
- `-u, --flush-interval` seconds (float) to flush buffers periodically (0 disables).
- `-v, --env KEY=VALUE` repeatable; extra env for the command.
- `-w, --cwd` working directory for the command.
- `-e, --echo` tee each line to stdout.
- `-T, --timestamp` prefix each emitted line (file/echo/stream) with ISO-8601 timestamp + space.
- `-I, --idle-alert` seconds; if no data arrives for this long, inject `[IDLE for Ns]` (no polling; use a re-armable timer).
- `-m, --metrics` enable HTTP server; `-M, --metrics-bind` bind (host:port). `:0` or `0` auto-picks a free port (print chosen URL).
- `-R, --ring-size` replay this many recent lines to new stream subscribers (default 25).

### Rotation & files
- Active file: `<prefix>.current.log` or `.current.log.gz` (inline mode).
- On rotation: rename to `<prefix>-YYYYMMDD-HHMMSS-<monoSuffix>.log[.gz]`.
- Rotate due to size and/or timer **only between lines** (respect line boundaries).
- Retention: delete older rotated files beyond N.

### HTTP server (BaseHTTPRequestHandler + ThreadingMixIn)
Start only with `-m`. On startup print:
`HTTP listening at http://HOST:PORT (paths: /metrics, /healthz, /stream, /rotate, /config)`

Endpoints:
- `GET /metrics` — Prometheus text format (no client lib).
- `GET /healthz` — returns `ok\n`.
- `GET /stream` — SSE (`text/event-stream`) streaming; replay last N lines, then push live lines.
  Heartbeat comment every ~10s. `?raw=1` streams raw text (no SSE framing).
- `/rotate` — **GET and POST**, with or without trailing slash. Immediate rotation (“hard”) by default:
  close current, finalize/rename/gzip as needed, and open a new current file. Optional `?soft=1` schedules
  a rotation at next line boundary instead. Response JSON: `{ ok, message, previous_file, current_file }`.
- `/config` — normalize trailing slash:
  - `GET /config` → JSON with current runtime config and state (incl. current file path and uncompressed bytes).
  - `POST /config` → JSON body to update live parameters (everything except the command):
    - `out_dir`, `prefix`, `compress (none|inline|after)`, `retain`, `max_bytes`, `interval`,
      `echo`, `timestamp`, `ring_size`, `idle_alert_secs`.
    - For options that require a new file (`out_dir`, `prefix`, `compress`), set `rotate_after_this_line = True`.
      Support `"rotate_now": true` to rotate immediately.
  - Return `{ ok, message, updated }` or 400 on validation errors.

### Streaming hub
- Maintain a ring buffer (deque) of recent lines; publish non-blocking to each subscriber queue (drop if full).

### Idle detection (no polling)
- Implement `IdleMonitor` using `threading.Timer`:
  - `arm(threshold)` sets/changes threshold (0 disables).
  - `poke()` on each real line write to re-arm.
  - On fire, verify still idle; inject `[IDLE for Ns]\n`, update metrics, and do not auto-rearm.
  - Clear idle-active gauge when input resumes.

### Metrics (Prometheus text; names & types as below)
Counters:
- `cmdlogger_lines_total`, `cmdlogger_bytes_total`, `cmdlogger_files_rotated_total`,
  `cmdlogger_metrics_scrapes_total`, `cmdlogger_streams_connected_total`,
  `cmdlogger_streams_disconnected_total`, `cmdlogger_stream_bytes_sent_total`,
  `cmdlogger_idle_alerts_total`

Gauges:
- `cmdlogger_current_file_bytes`, `cmdlogger_current_file_disk_bytes`,
  `cmdlogger_current_compression_ratio`, `cmdlogger_start_time_seconds`,
  `cmdlogger_uptime_seconds`, `cmdlogger_last_rotation_time_seconds`,
  `cmdlogger_last_write_time_seconds`, `cmdlogger_process_running{mode="cmd|stdin"}`,
  `cmdlogger_streams_current`, `cmdlogger_idle_active`, `cmdlogger_idle_threshold_seconds`,
  `cmdlogger_config_info{compress,retain,max_bytes,interval_seconds,echo,timestamp,ring_size}=1`,
  `cmdlogger_current_file_info{path="..."}=1`,
  `cmdlogger_metrics_bind_info{host,port}=1`

### Signals & shutdown
Handle SIGINT/SIGTERM: if needed, finish pending rotation, close file, stop timers/threads, shut down HTTP, cleanup temp files.

### Implementation constraints
- Python 3 stdlib only. Clear classes: RotatingWriter, IdleMonitor, _StreamHub, _Metrics, threaded HTTP server/handler.
- Performance: no periodic polling for idle detection; rotation-by-interval may use a lightweight periodic thread (not blocking writes).
- Thread safety: minimize contention; drop stream messages if subscriber queue is full.

### Acceptance checks (examples)
- `./cmd_logger.py -m -M :0 -n demo -z inline -s 5M -t 10m -r 5 -e -T -R 100 -c "yes hello"`
  shows files rotating near 5M, endpoints working, `/rotate` returns previous/current paths.
- Pipe mode: `yes data | ./cmd_logger.py -m -M :0` reads stdin (no default cmd).
- Idle: `-I 5` emits `[IDLE for 5s]` once per idle period; metrics reflect state.

END PROMPT
"""

import argparse
import gzip
import json
import os
import re
import shlex
import shutil
import signal
import sys
import tempfile
import threading
import time
import glob
from collections import deque
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from socket import gaierror
from socketserver import ThreadingMixIn
from subprocess import Popen, PIPE, CalledProcessError, check_output
from typing import Optional, IO, Union, Tuple, List, Dict, Any
from urllib.parse import urlparse, parse_qs
from queue import Queue, Empty

# ---------- Parsing helpers ----------

_SIZE_RE = re.compile(r"^\s*(\d+(?:\.\d+)?)([KMGTP]?)B?\s*$", re.I)
_MULT = {"": 1, "K": 1024, "M": 1024**2, "G": 1024**3, "T": 1024**4, "P": 1024**5}
_PID_TOKEN_RE = re.compile(r"PID:([A-Za-z0-9_.\-]+)")

DEFAULT_CMD = "bpftrace BPFTRACE -p PID:pd-protod"
DEFAULT_BPFTRACE = "/usr/bin/replication_monitor.bt"

def parse_size_to_int(s: Optional[str]) -> Optional[int]:
    if s is None:
        return None
    if isinstance(s, (int, float)):
        return int(s)
    m = _SIZE_RE.match(str(s))
    if not m:
        raise ValueError(f"Invalid size: {s}")
    num, unit = m.groups()
    return int(float(num) * _MULT[unit.upper()])

def parse_interval_to_secs(s: Optional[str]) -> Optional[int]:
    if s is None:
        return None
    if isinstance(s, (int, float)):
        return int(s)
    st = str(s).strip().lower()
    if st.isdigit():
        return int(st)
    m = re.match(r"^(\d+)([smhd])$", st)
    if not m:
        raise ValueError(f"Invalid interval: {s}")
    val, unit = m.groups()
    val = int(val)
    return {"s": val, "m": val * 60, "h": val * 3600, "d": val * 86400}[unit]

def iso_now() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat()

def human_ts():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def pidof_one(procname:: str) -> Optional[str]:
    try:
        out = check_output(["pidof", "-s", procname], text=True).strip()
        return out if out else None
    except (CalledProcessError, FileNotFoundError):
        return None

def replace_pid_tokens(cmd_str: str) -> str:
    def _sub(m):
        name = m.group(1)
        pid = pidof_one(name)
        if pid is None:
            sys.stderr.write(f"[{human_ts()}] WARNING: pidof could not find '{name}', leaving placeholder.\n")
            return m.group(0)
        return pid
    return _PID_TOKEN_RE.sub(_sub, cmd_str)

# ---------- BPFTRACE resolution (single flag supports file|glob|inline) ----------

def _materialize_inline_bt(text: str) -> str:
    fd, tmp = tempfile.mkstemp(prefix="bpftrace_", suffix=".bt")
    with os.fdopen(fd, "w") as f:
        f.write(text)
        if not text.endswith("\n"):
            f.write("\n")
    return tmp

def _resolve_bpftrace_mixed(specs: List[str]) -> Tuple[str, List[str]]:
    """
    Each spec can be: glob pattern, file path, or inline. `inline:...` forces inline.
    Returns (combined_file_path, temp_files_to_cleanup).
    """
    # If user passed any -b, keep them as-is; else use default
    use_specs = specs if specs else [DEFAULT_BPFTRACE]

    files: List[str] = []
    cleanup: List[str] = []

    for s in use_specs:
        if s.startswith("inline:"):
            text = s[len("inline:"):]
            p = _materialize_inline_bt(text)
            cleanup.append(p)
            files.append(p)
            continue

        matches = sorted(glob.glob(s))
        if matches:
            for m in matches:
                if os.path.isfile(m):
                    files.append(os.path.abspath(m))
            continue

        if os.path.isfile(s):
            files.append(os.path.abspath(s))
            continue

        # Fallback to inline
        p = _materialize_inline_bt(s)
        cleanup.append(p)
        files.append(p)

    if not files:
        p = os.path.abspath(DEFAULT_BPFTRACE)
        if not os.path.isfile(p):
            raise FileNotFoundError(f"Default BPFTrace file not found: {p}")
        files.append(p)

    # Concatenate in order
    fd, combined_path = tempfile.mkstemp(prefix="bpftrace_combined_", suffix=".bt")
    with os.fdopen(fd, "w") as out:
        out.write("// ---- Combined BPFTrace program (auto-generated) ----\n")
        for i, fp in enumerate(files, 1):
            out.write(f"// [{i}] {fp}\n")
        out.write("// -------------------------------------------------\n\n")
        for i, fp in enumerate(files, 1):
            out.write(f"// ---- BEGIN [{i}] {fp} ----\n")
            with open(fp, "r") as src:
                out.write(src.read())
            out.write("\n// ---- END ----\n\n")
    cleanup.append(combined_path)
    return combined_path, cleanup

def replace_bpftrace_token(cmd_str: str, bpftrace_path: Optional[str]) -> str:
    if bpftrace_path is None:
        return cmd_str
    return cmd_str.replace("BPFTRACE", shlex.quote(bpftrace_path))

# ---------- Streaming hub (ring buffer + fan-out) ----------

class _StreamHub:
    def __init__(self, ring_size: int = 25):
        self._subs = set()
        self._lock = threading.Lock()
        self._ring = deque(maxlen=max(1, ring_size))

    def set_ring_size(self, n: int):
        n = max(1, int(n))
        with self._lock:
            old = list(self._ring)
            self._ring = deque(old[-n:], maxlen=n)

    def snapshot(self) -> List[str]:
        with self._lock:
            return list(self._ring)

    def subscribe(self) -> Queue:
        q = Queue(maxsize=1000)
        with self._lock:
            self._subs.add(q)
        return q

    def unsubscribe(self, q: Queue):
        with self._lock:
            self._subs.discard(q)

    def publish(self, line_text: str):
        with self._lock:
            self._ring.append(line_text)
            subs = list(self._subs)
        for q in subs:
            try:
                q.put_nowait(line_text)
            except Exception:
                pass

STREAM_HUB = _StreamHub()

# ---------- Metrics (no deps) ----------

class _Metrics:
    def __init__(self):
        self._lock = threading.RLock()
        self.lines_total = 0
        self.bytes_total = 0
        self.files_rotated_total = 0
        self.current_file_bytes = 0
        self.start_time = time.time()
        self.last_rotation_time = 0.0
        self.mode = "unknown"
        self.process_running = 0
        self.last_write_time = 0.0
        self.scrapes_total = 0
        self.current_file_disk_bytes = 0
        self.current_compression_ratio = 1.0
        self.streams_connected_total = 0
        self.streams_disconnected_total = 0
        self.stream_bytes_sent_total = 0
        self.streams_current = 0
        self.idle_alerts_total = 0
        self.idle_threshold_seconds = 0.0
        self.idle_active = 0
        self.config = {
            "compress": "none",
            "retain": "0",
            "max_bytes": "0",
            "interval_seconds": "0",
            "echo": "false",
            "timestamp": "false",
            "ring_size": "25",
        }
        self.current_file_path = ""
        self.metrics_bind = ("", 0)

    def _with(self, fn):
        with self._lock:
            fn(self)

    def set_mode(self, mode: str):                     self._with(lambda s: setattr(s, "mode", mode))
    def set_running(self, running: bool):              self._with(lambda s: setattr(s, "process_running", 1 if running else 0))
    def incr_lines(self, n: int = 1):                  self._with(lambda s: setattr(s, "lines_total", s.lines_total + n))
    def add_bytes(self, n: int):                       self._with(lambda s: (setattr(s, "bytes_total", s.bytes_total + n), setattr(s, "current_file_bytes", s.current_file_bytes + n)))
    def set_current_file_bytes(self, v: int):          self._with(lambda s: setattr(s, "current_file_bytes", v))
    def rotated(self):                                 self._with(lambda s: (setattr(s, "files_rotated_total", s.files_rotated_total + 1), setattr(s, "last_rotation_time", time.time()), setattr(s, "current_file_bytes", 0), setattr(s, "current_file_disk_bytes", 0), setattr(s, "current_compression_ratio", 1.0)))
    def set_config(self, **cfg):                       self._with(lambda s: s.config.update({k: str(v) for k, v in cfg.items()}))
    def set_current_path(self, path: str):             self._with(lambda s: setattr(s, "current_file_path", path))
    def set_metrics_bind(self, host: str, port: int):  self._with(lambda s: setattr(s, "metrics_bind", (host, port)))
    def set_last_write_time(self, t: float):           self._with(lambda s: setattr(s, "last_write_time", t))
    def incr_scrapes(self):                            self._with(lambda s: setattr(s, "scrapes_total", s.scrapes_total + 1))
    def set_current_file_disk_bytes(self, n: int):     self._with(lambda s: setattr(s, "current_file_disk_bytes", n))
    def set_current_compression_ratio(self, r: float): self._with(lambda s: setattr(s, "current_compression_ratio", r))
    def stream_connected(self):                        self._with(lambda s: (setattr(s, "streams_connected_total", s.streams_connected_total + 1), setattr(s, "streams_current", s.streams_current + 1)))
    def stream_disconnected(self):                     self._with(lambda s: (setattr(s, "streams_disconnected_total", s.streams_disconnected_total + 1), setattr(s, "streams_current", max(0, s.streams_current - 1))))
    def add_stream_bytes(self, n: int):                self._with(lambda s: setattr(s, "stream_bytes_sent_total", s.stream_bytes_sent_total + n))
    def idle_triggered(self):                          self._with(lambda s: (setattr(s, "idle_alerts_total", s.idle_alerts_total + 1), setattr(s, "idle_active", 1)))
    def idle_reset(self):                              self._with(lambda s: setattr(s, "idle_active", 0))
    def set_idle_threshold(self, secs: float):         self._with(lambda s: setattr(s, "idle_threshold_seconds", float(secs)))

    def render(self) -> bytes:
        with self._lock:
            now = time.time()
            host, port = self.metrics_bind
            L = []
            L += [
                "# TYPE cmdlogger_lines_total counter",
                f"cmdlogger_lines_total {self.lines_total}",
                "# TYPE cmdlogger_bytes_total counter",
                f"cmdlogger_bytes_total {self.bytes_total}",
                "# TYPE cmdlogger_files_rotated_total counter",
                f"cmdlogger_files_rotated_total {self.files_rotated_total}",
                "# TYPE cmdlogger_metrics_scrapes_total counter",
                f"cmdlogger_metrics_scrapes_total {self.scrapes_total}",
                "# TYPE cmdlogger_streams_connected_total counter",
                f"cmdlogger_streams_connected_total {self.streams_connected_total}",
                "# TYPE cmdlogger_streams_disconnected_total counter",
                f"cmdlogger_streams_disconnected_total {self.streams_disconnected_total}",
                "# TYPE cmdlogger_stream_bytes_sent_total counter",
                f"cmdlogger_stream_bytes_sent_total {self.stream_bytes_sent_total}",
                "# TYPE cmdlogger_idle_alerts_total counter",
                f"cmdlogger_idle_alerts_total {self.idle_alerts_total}",
            ]
            L += [
                "# TYPE cmdlogger_current_file_bytes gauge",
                f"cmdlogger_current_file_bytes {self.current_file_bytes}",
                "# TYPE cmdlogger_current_file_disk_bytes gauge",
                f"cmdlogger_current_file_disk_bytes {self.current_file_disk_bytes}",
                "# TYPE cmdlogger_current_compression_ratio gauge",
                f"cmdlogger_current_compression_ratio {self.current_compression_ratio}",
                "# TYPE cmdlogger_start_time_seconds gauge",
                f"cmdlogger_start_time_seconds {self.start_time}",
                "# TYPE cmdlogger_uptime_seconds gauge",
                f"cmdlogger_uptime_seconds {now - self.start_time}",
                "# TYPE cmdlogger_last_rotation_time_seconds gauge",
                f"cmdlogger_last_rotation_time_seconds {self.last_rotation_time}",
                "# TYPE cmdlogger_last_write_time_seconds gauge",
                f"cmdlogger_last_write_time_seconds {self.last_write_time}",
                "# TYPE cmdlogger_process_running gauge",
                f'cmdlogger_process_running{{mode="{self.mode}"}} {self.process_running}',
                "# TYPE cmdlogger_streams_current gauge",
                f"cmdlogger_streams_current {self.streams_current}",
                "# TYPE cmdlogger_idle_active gauge",
                f"cmdlogger_idle_active {self.idle_active}",
                "# TYPE cmdlogger_idle_threshold_seconds gauge",
                f"cmdlogger_idle_threshold_seconds {self.idle_threshold_seconds}",
            ]
            cfg_lbls = ",".join([f'{k}="{v}"' for k, v in sorted(self.config.items())])
            L += [
                "# TYPE cmdlogger_config_info gauge",
                f"cmdlogger_config_info{{{cfg_lbls}}} 1",
            ]
            if self.current_file_path:
                L += [
                    "# TYPE cmdlogger_current_file_info gauge",
                    f'cmdlogger_current_file_info{{path="{self.current_file_path}"}} 1',
                ]
            if port:
                L += [
                    "# TYPE cmdlogger_metrics_bind_info gauge",
                    f'cmdlogger_metrics_bind_info{{host="{host}",port="{port}"}} 1',
                ]
            return ("\n".join(L) + "\n").encode("utf-8")

METRICS = _Metrics()

# ---------- Idle monitor (no polling) ----------

class IdleMonitor:
    def __init__(self, writer, runtime_cfg):
        self.writer = writer
        self.runtime_cfg = runtime_cfg
        self._timer = None
        self._lock = threading.Lock()
        self._last_write = time.monotonic()
        self._threshold = float(runtime_cfg.get("idle_alert_secs", 0) or 0)
        METRICS.set_idle_threshold(self._threshold)

    def _cancel_timer(self):
        t = self._timer
        self._timer = None
        if t:
            t.cancel()

    def _fire(self):
        try:
            now = time.monotonic()
            since = int(now - self._last_write)
            if self._threshold <= 0 or since < self._threshold:
                return
            METRICS.idle_triggered()
            msg = f"[IDLE for {since}s]\n"
            self.writer.begin_line()
            self.writer.write_line(msg.encode("utf-8"))
        finally:
            pass

    def _arm_locked(self):
        self._cancel_timer()
        if self._threshold > 0:
            self._timer = threading.Timer(self._threshold, self._fire)
            self._timer.daemon = True
            self._timer.start()

    def poke(self):
        with self._lock:
            self._last_write = time.monotonic()
            self._arm_locked()
            if METRICS.idle_active:
                METRICS.idle_reset()

    def arm(self, threshold_secs: float):
        with self._lock:
            self._threshold = float(threshold_secs or 0)
            METRICS.set_idle_threshold(self._threshold)
            self._arm_locked()

    def stop(self):
        with self._lock:
            self._cancel_timer()

# ---------- HTTP server & handlers ----------

def _norm_path(path: str) -> str:
    p = path.rstrip("/")
    return p if p else "/"

class _Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        parsed = urlparse(self.path)
        path = _norm_path(parsed.path)
        qs = parse_qs(parsed.query)

        if path == "/metrics":
            METRICS.incr_scrapes()
            out = METRICS.render()
            self._ok(b"text/plain; version=0.0.4; charset=utf-8", out)
            return

        if path == "/healthz":
            self._ok(b"text/plain", b"ok\n")
            return

        if path == "/stream":
            raw = qs.get("raw", ["0"])[0] in ("1", "true", "yes")
            self._stream(raw)
            return

        if path == "/config":
            self._config_get()
            return

        if path == "/rotate":
            soft = qs.get("soft", ["0"])[0] in ("1", "true", "yes")
            ok, msg, prev_file, new_file = self.server.rotate_now(soft=soft)
            body = json.dumps({"ok": ok, "message": msg, "previous_file": prev_file, "current_file": new_file}).encode("utf-8")
            self._ok(b"application/json", body, code=200 if ok else 500)
            return

        self._not_found()

    def do_POST(self):
        parsed = urlparse(self.path)
        path = _norm_path(parsed.path)
        qs = parse_qs(parsed.query)

        if path == "/rotate":
            soft = qs.get("soft", ["0"])[0] in ("1", "true", "yes")
            ok, msg, prev_file, new_file = self.server.rotate_now(soft=soft)
            body = json.dumps({"ok": ok, "message": msg, "previous_file": prev_file, "current_file": new_file}).encode("utf-8")
            self._ok(b"application/json", body, code=200 if ok else 500)
            return

        if path == "/config":
            payload = self._read_json()
            ok, msg, updated = self.server.apply_config(payload)
            body = json.dumps({"ok": ok, "message": msg, "updated": updated}, indent=2).encode("utf-8")
            self._ok(b"application/json", body, code=200 if ok else 400)
            return

        self._not_found()

    # --- helpers ---
    def _ok(self, ctype: bytes, body: bytes, code: int = 200):
        self.send_response(code)
        self.send_header("Content-Type", ctype.decode())
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _not_found(self):
        self.send_response(404)
        self.end_headers()

    def _read_json(self) -> Dict[str, Any]:
        length = int(self.headers.get("Content-Length", "0"))
        data = self.rfile.read(length) if length > 0 else b"{}"
        try:
            return json.loads(data.decode("utf-8"))
        except Exception:
            return {}

    def _config_get(self):
        state = self.server.get_config_state()
        body = json.dumps(state, indent=2, sort_keys=True).encode("utf-8")
        self._ok(b"application/json", body)

    def _stream(self, raw: bool):
        if raw:
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Cache-Control", "no-cache")
            self.send_header("Connection", "close")
            self.end_headers()
        else:
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream; charset=utf-8")
            self.send_header("Cache-Control", "no-cache")
            self.send_header("Connection", "keep-alive")
            self.end_headers()
            self.wfile.write(b": stream opened\n\n")
            self.wfile.flush()

        METRICS.stream_connected()
        try:
            # replay
            snapshot = STREAM_HUB.snapshot()
            for line in snapshot:
                payload = line.encode("utf-8", errors="replace")
                if raw:
                    self.wfile.write(payload)
                    self.wfile.flush()
                    METRICS.add_stream_bytes(len(payload))
                else:
                    for part in payload.splitlines(True):
                        frame = b"data: " + part
                        self.wfile.write(frame)
                        METRICS.add_stream_bytes(len(part))
                    if not payload.endswith(b"\n"):
                        self.wfile.write(b"\n")
                    self.wfile.write(b"\n")
                    self.wfile.flush()

            # live
            q = STREAM_HUB.subscribe()
            heartbeat_every = 10.0
            last_beat = time.time()
            while True:
                now = time.time()
                timeout = max(0.5, heartbeat_every - (now - last_beat))
                try:
                    line = q.get(timeout=timeout)
                    payload = line.encode("utf-8", errors="replace")
                    if raw:
                        self.wfile.write(payload)
                        self.wfile.flush()
                        METRICS.add_stream_bytes(len(payload))
                    else:
                        for part in payload.splitlines(True):
                            frame = b"data: " + part
                            self.wfile.write(frame)
                            METRICS.add_stream_bytes(len(part))
                        if not payload.endswith(b"\n"):
                            self.wfile.write(b"\n")
                        self.wfile.write(b"\n")
                        self.wfile.flush()
                except Empty:
                    if not raw:
                        self.wfile.write(b": heartbeat\n\n")
                        self.wfile.flush()
                    last_beat = time.time()
        except (BrokenPipeError, ConnectionResetError):
            pass
        finally:
            try:
                STREAM_HUB.unsubscribe(q)
            except Exception:
                pass
            METRICS.stream_disconnected()

class _ThreadingHTTPServer(ThreadingMixIn, HTTPServer):
    daemon_threads = True

    def configure(self, writer, runtime_cfg):
        self.writer = writer
        self.runtime_cfg = runtime_cfg

    def rotate_now(self, soft: bool = False):
        try:
            if soft:
                self.writer.rotate_after_this_line = True
                return True, "scheduled", None, str(self.writer.current_path)
            prev_current = str(self.writer.current_path) if self.writer.current_path else None
            self.writer.rotate()  # immediate: close old, open new
            return True, "rotated", prev_current, str(self.writer.current_path)
        except Exception as e:
            return False, f"rotate failed: {e}", None, None

    def get_config_state(self) -> Dict[str, Any]:
        st = dict(self.runtime_cfg)
        st.update({
            "current_file": str(self.writer.current_path) if self.writer.current_path else "",
            "bytes_written_uncompressed": self.writer.bytes_written_uncompressed,
        })
        return st

    def apply_config(self, payload: Dict[str, Any]):
        updated = {}
        try:
            if "echo" in payload:
                self.writer.echo = bool(payload["echo"])
                self.runtime_cfg["echo"] = self.writer.echo
                updated["echo"] = self.writer.echo
                METRICS.set_config(echo=self.writer.echo)

            if "timestamp" in payload:
                self.writer.timestamp_flag = bool(payload["timestamp"])
                self.runtime_cfg["timestamp"] = self.writer.timestamp_flag
                updated["timestamp"] = self.writer.timestamp_flag
                METRICS.set_config(timestamp=self.writer.timestamp_flag)

            if "ring_size" in payload:
                rn = int(payload["ring_size"])
                STREAM_HUB.set_ring_size(rn)
                self.runtime_cfg["ring_size"] = rn
                updated["ring_size"] = rn
                METRICS.set_config(ring_size=rn)

            if "idle_alert_secs" in payload:
                v = float(payload["idle_alert_secs"])
                self.runtime_cfg["idle_alert_secs"] = v
                updated["idle_alert_secs"] = v
                METRICS.set_idle_threshold(v)
                if hasattr(self.writer, "idle_monitor") and self.writer.idle_monitor:
                    self.writer.idle_monitor.arm(v)

            if "retain" in payload:
                self.writer.retain = max(0, int(payload["retain"]))
                self.runtime_cfg["retain"] = self.writer.retain
                updated["retain"] = self.writer.retain
                METRICS.set_config(retain=self.writer.retain)

            if "max_bytes" in payload:
                self.writer.max_bytes = parse_size_to_int(payload["max_bytes"])
                self.runtime_cfg["max_bytes"] = self.writer.max_bytes or 0
                updated["max_bytes"] = self.writer.max_bytes
                METRICS.set_config(max_bytes=self.writer.max_bytes or 0)

            if "interval" in payload:
                self.writer.interval_s = parse_interval_to_secs(payload["interval"])
                self.runtime_cfg["interval_seconds"] = self.writer.interval_s or 0
                updated["interval_seconds"] = self.writer.interval_s or 0
                METRICS.set_config(interval_seconds=self.writer.interval_s or 0)

            needs_rotation = False

            if "prefix" in payload:
                self.writer.prefix = str(payload["prefix"])
                self.runtime_cfg["prefix"] = self.writer.prefix
                updated["prefix"] = self.writer.prefix
                needs_rotation = True

            if "out_dir" in payload:
                new_dir = Path(payload["out_dir"]).resolve()
                new_dir.mkdir(parents=True, exist_ok=True)
                self.writer.out_dir = new_dir
                self.runtime_cfg["out_dir"] = str(new_dir)
                updated["out_dir"] = str(new_dir)
                needs_rotation = True

            if "compress" in payload:
                cv = str(payload["compress"]).lower()
                if cv not in ("none", "inline", "after"):
                    raise ValueError("compress must be one of: none, inline, after")
                self.writer.compress = cv
                self.runtime_cfg["compress"] = cv
                updated["compress"] = cv
                METRICS.set_config(compress=cv)
                needs_rotation = True

            if payload.get("rotate_now") or needs_rotation:
                self.writer.rotate_after_this_line = True

            return True, "applied", updated
        except Exception as e:
            return False, str(e), updated

def _parse_bind(bind: str) -> Tuple[str, int]:
    s = bind.strip()
    if not s:
        return "0.0.0.0", 9108
    if ":" in s:
        host, port_s = s.rsplit(":", 1)
        host = host or "0.0.0.0"
        port = int(port_s) if port_s else 0
        return host, port
    try:
        port = int(s)
        return "0.0.0.0", port
    except ValueError:
        return s, 0

def _start_server(bind: str, writer, runtime_cfg, try_span: int = 50):
    host, port = _parse_bind(bind)
    last_err = None
    candidates = [0] if port == 0 else list(range(port, port + try_span + 1)) + [0]
    for p in candidates:
        try:
            srv = _ThreadingHTTPServer((host, p), _Handler)
            bound_host, bound_port = srv.server_address
            METRICS.set_metrics_bind(bound_host, bound_port)
            srv.configure(writer, runtime_cfg)
            t = threading.Thread(target=srv.serve_forever, daemon=True)
            t.start()
            sys.stderr.write(
                f"[{human_ts()}] HTTP listening at http://{bound_host}:{bound_port} "
                f"(paths: /metrics, /healthz, /stream, /rotate, /config)\n"
            )
            sys.stderr.flush()
            return srv
        except OSError as e:
            last_err = e
            continue
        except gaierror as e:
            last_err = e
            continue
    raise RuntimeError(f"Failed to bind server on {host}:{port} (last error: {last_err})")

# ---------- RotatingWriter ----------

class RotatingWriter:
    def __init__(
        self,
        out_dir: Path,
        prefix: str,
        compress: str,
        retain: int,
        time_fmt: str,
        max_bytes: Optional[int],
        interval_s: Optional[int],
        echo: bool = False,
        timestamp_flag: bool = False,
    ):
        self.out_dir = out_dir
        self.prefix = prefix
        self.compress = compress
        self.retain = max(0, retain)
        self.time_fmt = time_fmt
        self.max_bytes = max_bytes
        self.interval_s = interval_s
        self.echo = echo
        self.timestamp_flag = timestamp_flag

        self.current_fp: Optional[Union[IO[bytes], gzip.GzipFile]] = None
        self.current_path: Optional[Path] = None
        self.bytes_written_uncompressed = 0

        self.rotate_due_flag = False
        self.rotate_after_this_line = False

        self._compress_threads = set()
        self._compress_lock = threading.Lock()

        self.out_dir.mkdir(parents=True, exist_ok=True)

        if interval_s:
            self._timer_t = threading.Thread(target=self._timer_loop, daemon=True)
            self._timer_t.start()
        else:
            self._timer_t = None

        METRICS.set_config(compress=self.compress, retain=self.retain,
                           max_bytes=self.max_bytes or 0, interval_seconds=self.interval_s or 0,
                           echo=self.echo, timestamp=self.timestamp_flag)

        # `idle_monitor` will be attached from run().

    def _make_current_path(self) -> Path:
        ext = ".log.gz" if self.compress == "inline" else ".log"
        return self.out_dir / f"{self.prefix}.current{ext}"

    def _make_rotated_name(self, was_gz: bool) -> Path:
        ts = datetime.now().strftime(self.time_fmt)
        ext = ".log.gz" if (self.compress == "inline" or was_gz) else ".log"
        suffix = f"{int(time.monotonic()*1000)%100000:05d}"
        return self.out_dir / f"{self.prefix}-{ts}-{suffix}{ext}"

    def open_new(self):
        self.close_current(finalize=False)
        self.current_path = self._make_current_path()
        mode = "ab"
        if self.compress == "inline":
            self.current_fp = gzip.open(self.current_path, mode)  # type: ignore
        else:
            self.current_fp = open(self.current_path, mode)
        self.bytes_written_uncompressed = 0
        METRICS.set_current_file_bytes(0)
        METRICS.set_current_file_disk_bytes(0)
        METRICS.set_current_compression_ratio(1.0)
        METRICS.set_current_path(str(self.current_path))

    def _timer_loop(self):
        next_fire = time.time() + self.interval_s
        while True:
            time.sleep(0.5)
            if time.time() >= next_fire:
                self.rotate_due_flag = True
                self.rotate_after_this_line = True
                next_fire = time.time() + self.interval_s

    def _prune_retention(self):
        patt = f"{self.prefix}-"
        files = [p for p in self.out_dir.iterdir()
                 if p.is_file()
                 and p.name.startswith(patt)
                 and (p.suffix == ".log" or p.suffixes[-2:] == [".log", ".gz"])]
        files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
        for p in files[self.retain:]:
            try:
                p.unlink()
            except FileNotFoundError:
                pass

    def rotate(self):
        if not self.current_fp:
            self.open_new()
            return
        old_fp = self.current_fp
        old_path = self.current_path
        was_gz = old_path.suffix == ".gz"

        self.current_fp = None
        self.current_path = None
        self.bytes_written_uncompressed = 0
        METRICS.rotated()
        self.rotate_due_flag = False
        self.rotate_after_this_line = False

        self.open_new()

        def finalize():
            try:
                old_fp.close()
            except Exception:
                pass
            if self.compress == "after" and not was_gz and old_path.exists():
                gz_path = old_path.with_suffix(old_path.suffix + ".gz")
                try:
                    with open(old_path, "rb") as src, gzip.open(gz_path, "wb") as dst:
                        shutil.copyfileobj(src, dst, length=1024 * 1024)
                    old_path.unlink(missing_ok=True)
                    final_path = gz_path
                except Exception:
                    final_path = old_path
            else:
                final_path = old_path
            try:
                rotated_name = self._make_rotated_name(final_path.suffix.endswith(".gz"))
                if final_path.name.endswith(".current.log") or final_path.name.endswith(".current.log.gz"):
                    final_path.rename(rotated_name)
                else:
                    shutil.move(str(final_path), str(rotated_name))
            except Exception:
                pass
            try:
                self._prune_retention()
            except Exception:
                pass

        t = threading.Thread(target=finalize, daemon=True)
        with self._compress_lock:
            self._compress_threads.add(t)
        t.start()

        with self._compress_lock:
            dead = [th for th in self._compress_threads if not th.is_alive()]
            for th in dead:
                self._compress_threads.remove(th)

    def begin_line(self):
        if self.rotate_after_this_line:
            self.rotate()

    def _update_disk_metrics(self):
        try:
            if self.current_path and self.current_path.exists():
                disk = os.path.getsize(self.current_path)
                METRICS.set_current_file_disk_bytes(disk)
                if self.compress == "inline":
                    ratio = (self.bytes_written_uncompressed / max(disk, 1))
                else:
                    ratio = 1.0
                METRICS.set_current_compression_ratio(ratio)
        except Exception:
            pass

    def _prefix_ts(self, line: bytes) -> bytes:
        if not self.timestamp_flag:
            return line
        ts = iso_now() + " "
        return ts.encode("utf-8") + line

    def write_line(self, line: bytes):
        if not self.current_fp:
            self.open_new()

        line = self._prefix_ts(line)

        if self.echo:
            try:
                sys.stdout.buffer.write(line)
                sys.stdout.buffer.flush()
            except Exception:
                pass

        self.current_fp.write(line)
        try:
            self.current_fp.flush()
        except Exception:
            pass

        self.bytes_written_uncompressed += len(line)
        METRICS.incr_lines()
        METRICS.add_bytes(len(line))
        now = time.time()
        METRICS.set_last_write_time(now)

        if hasattr(self, "idle_monitor") and self.idle_monitor:
            self.idle_monitor.poke()

        try:
            text = line.decode("utf-8", errors="replace")
        except Exception:
            text = repr(line) + "\n"
        STREAM_HUB.publish(text)

        self._update_disk_metrics()

        if self.max_bytes is not None and self.bytes_written_uncompressed >= self.max_bytes:
            self.rotate_after_this_line = True
        if self.rotate_due_flag:
            self.rotate_after_this_line = True

    def close_current(self, finalize: bool = True):
        if self.current_fp:
            try:
                self.current_fp.close()
            except Exception:
                pass
        self.current_fp = None
        path = self.current_path
        self.current_path = None
        self.bytes_written_uncompressed = 0
        METRICS.set_current_file_bytes(0)
        METRICS.set_current_file_disk_bytes(0)
        METRICS.set_current_compression_ratio(1.0)
        if finalize and path and path.exists():
            was_gz = path.suffix == ".gz"
            rotated = self._make_rotated_name(was_gz)
            try:
                path.rename(rotated)
            except Exception:
                pass
            try:
                self._prune_retention()
            except Exception:
                pass

    def stop(self):
        with self._compress_lock:
            threads = list(self._compress_threads)
        for t in threads:
            t.join(timeout=2.0)

# ---------- Input handling (line mode) ----------

def stream_loop_lines(input_stream, writer: RotatingWriter, flush_interval: float):
    last_flush = time.time()
    for line in iter(input_stream.readline, b""):
        writer.begin_line()
        writer.write_line(line)
        if flush_interval and (time.time() - last_flush) >= flush_interval:
            if writer.current_fp:
                try:
                    writer.current_fp.flush()
                except Exception:
                    pass
            last_flush = time.time()
    if writer.rotate_after_this_line:
        writer.rotate()

# ---------- Main ----------

def run():
    ap = argparse.ArgumentParser(description="Capture stdout or stdin to rolling log files (rotate only on line breaks).")
    ap.add_argument("-c", "--cmd", default=DEFAULT_CMD,
                    help=f'Command to run (default: "{DEFAULT_CMD}").')
    ap.add_argument("-b", "--bpftrace", action="append", default=[],
                    help=("BPFTrace source spec (repeatable). Each item may be a file path, a glob pattern, "
                          "or inline code. Use 'inline:...' to force inline if a same-named file exists. "
                          f"Default when omitted: {DEFAULT_BPFTRACE}"))
    ap.add_argument("-o", "--out-dir", default=".", help="Directory for logs.")
    ap.add_argument("-n", "--prefix", default="capture", help="Log filename prefix.")
    ap.add_argument("-s", "--max-bytes", type=parse_size_to_int, help="Rotate after this many bytes (e.g., 100M).")
    ap.add_argument("-t", "--interval", type=parse_interval_to_secs, help="Rotate after this much time (e.g., 10m).")
    ap.add_argument("-z", "--compress", choices=["none", "inline", "after"], default="none", help="Compression mode.")
    ap.add_argument("-r", "--retain", type=int, default=10, help="Keep newest N rotated logs.")
    ap.add_argument("-u", "--flush-interval", type=float, default=0.0, help="Extra flush cadence in seconds (0 disables).")
    ap.add_argument("-v", "--env", action="append", default=[], help="Env var KEY=VALUE to add (can repeat).")
    ap.add_argument("-w", "--cwd", default=None, help="Working directory for the command.")
    ap.add_argument("-e", "--echo", action="store_true", help="Echo each line to stdout in addition to writing to the log.")
    ap.add_argument("-T", "--timestamp", action="store_true", help="Prefix each output line with an ISO-8601 timestamp.")
    ap.add_argument("-I", "--idle-alert", type=float, default=0.0,
                    help="If no input arrives for this many seconds, inject an [IDLE for Ns] line (0 to disable).")
    ap.add_argument("-m", "--metrics", action="store_true",
                    help="Serve HTTP: /metrics, /healthz, /stream, /rotate, /config.")
    ap.add_argument("-M", "--metrics-bind", default="0.0.0.0:9108",
                    help="Bind address:port (e.g. 127.0.0.1:9200). Use ':0' or '0' for auto-assign.")
    ap.add_argument("-R", "--ring-size", type=int, default=25,
                    help="Number of recent lines to replay on /stream connect (default 25).")
    args = ap.parse_args()

    out_dir = Path(args.out_dir).resolve()

    STREAM_HUB.set_ring_size(args.ring_size)
    METRICS.set_config(ring_size=args.ring_size)

    stdin_is_piped = not sys.stdin.isatty()
    user_set_cmd = "--cmd" in sys.argv or "-c" in sys.argv
    cmd_str = args.cmd
    if stdin_is_piped and not user_set_cmd:
        cmd_str = ""
        sys.stderr.write(f"[{human_ts()}] Detected piped stdin; ignoring default command.\n")

    env = os.environ.copy()
    for kv in args.env:
        if "=" not in kv:
            ap.error(f"--env expects KEY=VALUE, got: {kv}")
        k, v = kv.split("=", 1)
        env[k] = v

    writer = RotatingWriter(out_dir=out_dir, prefix=args.prefix, compress=args.compress,
                            retain=args.retain, time_fmt="%Y%m%d-%H%M%S",
                            max_bytes=args.max_bytes, interval_s=args.interval,
                            echo=args.echo, timestamp_flag=args.timestamp)
    writer.open_new()

    runtime_cfg = {
        "out_dir": str(out_dir),
        "prefix": args.prefix,
        "compress": args.compress,
        "retain": args.retain,
        "max_bytes": args.max_bytes or 0,
        "interval_seconds": args.interval or 0,
        "echo": args.echo,
        "timestamp": args.timestamp,
        "ring_size": args.ring_size,
        "idle_alert_secs": float(args.idle_alert or 0),
    }

    idle_monitor = IdleMonitor(writer, runtime_cfg)
    writer.idle_monitor = idle_monitor
    idle_monitor.arm(runtime_cfg["idle_alert_secs"])

    srv = None
    if args.metrics:
        srv = _start_server(args.metrics_bind, writer, runtime_cfg)

    METRICS.set_mode("cmd" if cmd_str else "stdin")
    METRICS.set_running(True)

    tmp_files: List[str] = []
    try:
        if cmd_str:
            bt_specs = args.bpftrace if args.bpftrace else [DEFAULT_BPFTRACE]
            combined_bt, cleanup_bt = _resolve_bpftrace_mixed(bt_specs)
            tmp_files.extend(cleanup_bt)
            cmd_str = replace_bpftrace_token(cmd_str, combined_bt)
            cmd_str = replace_pid_tokens(cmd_str)

            cmd_argv = shlex.split(cmd_str)
            proc = Popen(cmd_argv, stdout=PIPE, stderr=PIPE, bufsize=0, cwd=args.cwd, env=env)

            def pump_stderr():
                try:
                    for line in iter(proc.stderr.readline, b""):
                        sys.stderr.buffer.write(line)
                        sys.stderr.buffer.flush()
                except Exception:
                    pass
            threading.Thread(target=pump_stderr, daemon=True).start()

            try:
                stream_loop_lines(proc.stdout, writer, args.flush_interval)
            finally:
                try:
                    proc.wait(timeout=2.0)
                except Exception:
                    pass
        else:
            sys.stderr.write(f"[{human_ts()}] Reading from STDIN...\n")
            stream_loop_lines(sys.stdin.buffer, writer, args.flush_interval)

        if writer.rotate_after_this_line:
            writer.rotate()
        writer.close_current(True)
        writer.stop()
        METRICS.set_running(False)
        if srv:
            try:
                srv.shutdown()
            except Exception:
                pass
        idle_monitor.stop()
        sys.stderr.write(f"[{human_ts()}] Done.\n")
    finally:
        for p in tmp_files:
            try:
                os.unlink(p)
            except Exception:
                pass

if __name__ == "__main__":
    run()
