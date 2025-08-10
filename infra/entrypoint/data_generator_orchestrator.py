"""
Data Generator Orchestrator (single-file, idempotent)

What this does:
- Discovers generator scripts in configured directories
- Runs them on randomized schedules with randomized inputs
- Exposes a small web UI to start/stop each script and view history/logs
- Records run history into SQLite
- Cleans old orchestrator logs
- Provides lightweight JSON status APIs
"""

from __future__ import annotations

import asyncio
import glob
import html
import logging
import os
import random
import sqlite3
import subprocess
import sys
import time
import traceback
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, Optional, List, Tuple

from aiohttp import web
from urllib.parse import quote

# ============================ Configuration ============================= #
# You can override these with environment variables. Defaults keep your current behavior.
ORCH_PORT: int = int(os.environ.get("ORCH_PORT", "1212"))
LOG_DIR: str = os.environ.get("ORCH_LOG_DIR", "/app/logs")
DB_FILE: str = os.environ.get("ORCH_DB_FILE", "/app/storage/orchestrator.db")
EXEC_TIMEOUT_SEC: Optional[int] = (int(os.environ["ORCH_EXEC_TIMEOUT_SEC"])
                                   if os.environ.get("ORCH_EXEC_TIMEOUT_SEC")
                                   else None)
# Comma-separated; defaults to your two directories
SCRIPT_DIRS: List[str] = [
    p.strip() for p in os.environ.get(
        "ORCH_SCRIPT_DIRS",
        "/app/scripts/support_insights,/app/scripts/python_scripts_orchestrator",
    ).split(",") if p.strip()
]

# interval and input ranges
NEXT_DELAY_MIN: int = int(os.environ.get("ORCH_NEXT_DELAY_MIN", "30"))
NEXT_DELAY_MAX: int = int(os.environ.get("ORCH_NEXT_DELAY_MAX", "300"))
INPUT_MIN: int = int(os.environ.get("ORCH_INPUT_MIN", "100"))
INPUT_MAX: int = int(os.environ.get("ORCH_INPUT_MAX", "100000"))

# How many days to keep orchestrator logs
LOG_RETENTION_DAYS: int = int(os.environ.get("ORCH_LOG_RETENTION_DAYS", "7"))

# =============================== Logging ================================ #
SCRIPT_LOGGER = logging.getLogger("orchestrator")
SCRIPT_LOGGER.setLevel(logging.INFO)

os.makedirs(LOG_DIR, exist_ok=True)
_log_file_path = os.path.join(
    LOG_DIR,
    f"data_generator_orchestrator_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.log",
)
_formatter = logging.Formatter(
    "timestamp=%(asctime)s level=%(levelname)s script=%(name)s message=%(message)s"
)
_file_handler = logging.FileHandler(_log_file_path)
_file_handler.setFormatter(_formatter)
_console_handler = logging.StreamHandler(sys.stdout)
_console_handler.setFormatter(_formatter)
SCRIPT_LOGGER.addHandler(_file_handler)
SCRIPT_LOGGER.addHandler(_console_handler)

# Quiet down aiohttp access logs
logging.getLogger("aiohttp.access").setLevel(logging.WARNING)

# ============================ Global State ============================== #
# Per your structure: we track status in-memory and write run history to SQLite.
ScriptStatus = Dict[str, Dict[str, Any]]
script_status: ScriptStatus = {}
script_locks: Dict[str, asyncio.Lock] = (
    {})  # prevent accidental concurrent runs per script


# ============================== Utilities =============================== #
def setup_logging() -> None:
    """Log basic startup info."""
    SCRIPT_LOGGER.info("----- Setting Up Logging -----")
    SCRIPT_LOGGER.info("Log file: %s", _log_file_path)
    SCRIPT_LOGGER.info("-----------------------------")


def cleanup_old_logs() -> None:
    """Delete orchestrator log files older than retention."""
    SCRIPT_LOGGER.info("----- Cleaning Up Old Logs -----")
    threshold = time.time() - LOG_RETENTION_DAYS * 24 * 3600
    deleted = 0
    SCRIPT_LOGGER.info(
        "Scanning directory: %s, threshold: %s",
        LOG_DIR,
        datetime.fromtimestamp(threshold).strftime("%Y-%m-%d %H:%M:%S"),
    )
    for path in glob.glob(os.path.join(LOG_DIR, "*.log")):
        try:
            mtime = os.path.getmtime(path)
            if mtime < threshold:
                os.remove(path)
                deleted += 1
                SCRIPT_LOGGER.info(
                    "Deleted old log file: %s (last modified: %s)",
                    path,
                    datetime.fromtimestamp(mtime).strftime(
                        "%Y-%m-%d %H:%M:%S"),
                )
        except OSError as e:
            SCRIPT_LOGGER.error("Failed to delete %s: %s\n%s", path, str(e),
                                traceback.format_exc())
    if deleted == 0:
        SCRIPT_LOGGER.info("No old log files found to delete")
    SCRIPT_LOGGER.info("--------------------------------")


def normalize_name(name: str) -> str:
    """Normalize a script/folder key to a readable label."""
    words = name.replace("-", "_").split("_")
    display_name = " ".join(word.capitalize() for word in words)
    SCRIPT_LOGGER.debug("Normalized name: %s -> %s", name, display_name)
    return display_name


def discover_scripts() -> ScriptStatus:
    """Discover Python scripts under configured directories."""
    SCRIPT_LOGGER.info("----- Discovering Scripts -----")
    scripts: ScriptStatus = {}

    for script_dir in SCRIPT_DIRS:
        if not os.path.exists(script_dir):
            SCRIPT_LOGGER.info("Directory %s not found (skipping)", script_dir)
            continue
        SCRIPT_LOGGER.info("Scanning directory: %s", script_dir)
        for root, _, files in os.walk(script_dir):
            for filename in files:
                if not filename.endswith(".py"):
                    continue
                if filename in {
                        "data_generator_orchestrator.py", "db_operations.py"
                }:
                    continue
                script_name = os.path.splitext(filename)[0]
                display_name = normalize_name(script_name)
                script_path = os.path.join(root, filename)
                folder_name = os.path.basename(script_dir)

                scripts[script_name] = {
                    "script_path": script_path,
                    "folder": folder_name,
                    "display_name": display_name,
                    "last_run": None,
                    "last_input": None,
                    "status": "Stopped",
                    "last_error": None,
                    "last_log_file": None,
                    "last_duration": None,
                    "next_run": None,
                    "running": False,
                    "task": None,
                }
                script_locks.setdefault(script_name, asyncio.Lock())
                SCRIPT_LOGGER.info(
                    "Discovered script: name=%s, path=%s, folder=%s, display_name=%s",
                    script_name,
                    script_path,
                    folder_name,
                    display_name,
                )

    SCRIPT_LOGGER.info("Total scripts discovered: %d", len(scripts))
    SCRIPT_LOGGER.info("------------------------------")
    return scripts


# ============================== SQLite I/O ============================== #
def ensure_db_schema(conn: sqlite3.Connection) -> None:
    """Create run_history table if it doesn't exist (idempotent)."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS run_history (
            run_id            INTEGER PRIMARY KEY AUTOINCREMENT,
            script_name       TEXT NOT NULL,
            input_value       INTEGER,
            records_generated INTEGER,
            start_time        TEXT,
            end_time          TEXT,
            duration          REAL,
            status            TEXT,
            error_message     TEXT,
            log_file          TEXT
        );
        """)
    conn.commit()


def init_sqlite_db(db_file: str) -> sqlite3.Connection:
    """Open SQLite DB and ensure schema exists."""
    SCRIPT_LOGGER.info("----- Initializing SQLite Database -----")
    SCRIPT_LOGGER.info("Connecting to SQLite database: %s", db_file)
    try:
        os.makedirs(os.path.dirname(db_file), exist_ok=True)
        conn = sqlite3.connect(db_file)
        ensure_db_schema(conn)
        SCRIPT_LOGGER.info("Connected & schema ensured.")
        SCRIPT_LOGGER.info("---------------------------------------")
        return conn
    except sqlite3.Error as e:
        SCRIPT_LOGGER.error(
            "Failed to connect to SQLite (%s): %s\n%s",
            db_file,
            str(e),
            traceback.format_exc(),
        )
        raise


def save_run_history(
    conn: sqlite3.Connection,
    script_name: str,
    input_value: int,
    records_generated: int,
    start_time: datetime,
    end_time: Optional[datetime],
    status: str,
    error_message: Optional[str],
    log_file: Optional[str],
) -> None:
    """Insert a run history record."""
    try:
        duration = (end_time -
                    start_time).total_seconds() if end_time else None
        conn.execute(
            """
            INSERT INTO run_history (
                script_name, input_value, records_generated, start_time, end_time,
                duration, status, error_message, log_file
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                script_name,
                input_value,
                records_generated,
                start_time.strftime("%Y-%m-%d %H:%M:%S"),
                end_time.strftime("%Y-%m-%d %H:%M:%S") if end_time else None,
                duration,
                status,
                error_message,
                log_file,
            ),
        )
        conn.commit()
        SCRIPT_LOGGER.info(
            "History saved: %s input=%d records=%d status=%s dur=%s log=%s",
            script_name,
            input_value,
            records_generated,
            status,
            duration,
            log_file,
        )
    except sqlite3.Error as e:
        SCRIPT_LOGGER.error("Failed to save run history: %s\n%s", str(e),
                            traceback.format_exc())
        raise


# ============================ Execution Core ============================ #
async def run_script(
    script_name: str,
    input_value: int,
    status_map: ScriptStatus,
    db_conn: sqlite3.Connection,
) -> None:
    """Run a discovered generator script once and persist its outcome."""
    script_path = status_map[script_name]["script_path"]
    log_path = os.path.join(
        LOG_DIR,
        f"{script_name}_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.log",
    )

    SCRIPT_LOGGER.info("----- Starting Script %s -----", script_name)
    SCRIPT_LOGGER.info("Script path: %s", script_path)
    SCRIPT_LOGGER.info("Input value: %d", input_value)
    SCRIPT_LOGGER.info("Logging to: %s", log_path)

    status_map[script_name].update({
        "status": "Running",
        "last_run": datetime.now(timezone.utc),
        "last_input": input_value,
        "last_error": None,
        "last_log_file": log_path,
        "next_run": None,
    })

    start_time = datetime.now(timezone.utc)
    try:
        env = os.environ.copy()
        env["SCRIPT_LOG_FILE"] = log_path

        if not os.path.isfile(script_path):
            raise FileNotFoundError(f"Script {script_path} not found")
        if not os.access(script_path, os.R_OK):
            SCRIPT_LOGGER.warning("Script %s may not be readable", script_path)

        proc_coro = asyncio.create_subprocess_exec(
            sys.executable,
            script_path,
            str(input_value),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env,
        )

        if EXEC_TIMEOUT_SEC:
            process = await asyncio.wait_for(proc_coro,
                                             timeout=EXEC_TIMEOUT_SEC)
        else:
            process = await proc_coro

        try:
            if EXEC_TIMEOUT_SEC:
                stdout, stderr = await asyncio.wait_for(
                    process.communicate(), timeout=EXEC_TIMEOUT_SEC)
            else:
                stdout, stderr = await process.communicate()
        except asyncio.TimeoutError:
            process.kill()
            await process.wait()
            raise TimeoutError(
                f"Script {script_name} timed out after {EXEC_TIMEOUT_SEC}s")

        duration = (datetime.now(timezone.utc) - start_time).total_seconds()
        status_map[script_name]["last_duration"] = round(duration, 2)

        # Placeholder: downstream scripts could print counts; we can parse if needed.
        records_generated = input_value

        if process.returncode == 0:
            status_map[script_name]["status"] = "Completed"
            SCRIPT_LOGGER.info("%s completed successfully in %.2f seconds",
                               script_name, duration)
            save_run_history(
                db_conn,
                script_name,
                input_value,
                records_generated,
                start_time,
                datetime.now(timezone.utc),
                "Completed",
                None,
                log_path,
            )
        else:
            error_msg = (stderr or b"").decode(errors="replace").strip()
            status_map[script_name]["status"] = "Failed"
            status_map[script_name]["last_error"] = error_msg
            SCRIPT_LOGGER.error("%s failed: %s", script_name, error_msg)
            save_run_history(
                db_conn,
                script_name,
                input_value,
                records_generated,
                start_time,
                datetime.now(timezone.utc),
                "Failed",
                error_msg,
                log_path,
            )
    except Exception as e:
        duration = (datetime.now(timezone.utc) - start_time).total_seconds()
        status_map[script_name]["last_duration"] = round(duration, 2)
        status_map[script_name]["status"] = "Failed"
        status_map[script_name]["last_error"] = str(e)
        SCRIPT_LOGGER.error(
            "Exception while running %s: %s\n%s",
            script_name,
            str(e),
            traceback.format_exc(),
        )
        save_run_history(
            db_conn,
            script_name,
            input_value,
            0,
            start_time,
            datetime.now(timezone.utc),
            "Failed",
            str(e),
            log_path,
        )
        raise
    finally:
        SCRIPT_LOGGER.info("----------------------------")


async def script_runner(
    script_name: str,
    status_map: ScriptStatus,
    db_conn: sqlite3.Connection,
) -> None:
    """Loop runner: run script, schedule next (randomized), stop when flagged."""
    SCRIPT_LOGGER.info("----- Starting Script Runner for %s -----",
                       script_name)
    lock = script_locks[script_name]

    try:
        while status_map[script_name]["running"]:
            # random input + delay
            input_value = random.randint(INPUT_MIN, INPUT_MAX)
            delay = random.randint(NEXT_DELAY_MIN, NEXT_DELAY_MAX)
            SCRIPT_LOGGER.info("Generated input_value=%d, delay=%d seconds",
                               input_value, delay)

            # prevent overlapping runs
            async with lock:
                await run_script(script_name, input_value, status_map, db_conn)

            if status_map[script_name]["running"]:
                next_run_time = datetime.now(
                    timezone.utc) + timedelta(seconds=delay)
                status_map[script_name]["next_run"] = next_run_time.timestamp()
                SCRIPT_LOGGER.info(
                    "%s scheduled to run again at %s",
                    script_name,
                    next_run_time.strftime("%Y-%m-%d %H:%M:%S"),
                )
                await asyncio.sleep(delay)

        # mark stopped and persist a "Stopped" record to mirror your behavior
        status_map[script_name]["status"] = "Stopped"
        save_run_history(
            db_conn,
            script_name,
            status_map[script_name]["last_input"] or 0,
            0,
            status_map[script_name]["last_run"] or datetime.now(timezone.utc),
            datetime.now(timezone.utc),
            "Stopped",
            None,
            status_map[script_name]["last_log_file"],
        )
        SCRIPT_LOGGER.info("%s stopped", script_name)
    finally:
        SCRIPT_LOGGER.info("-----------------------------------------")


# =============================== HTTP Handlers =========================== #
def _escape(s: Any) -> str:
    return html.escape(str(s), quote=True)


async def index_handler(request: web.Request) -> web.Response:
    """Main HTML UI."""
    status_map = request.app["script_status"]
    db_conn = request.app[
        "db_conn"]  # noqa: F841  # (retained for compatibility)

    scripts_by_folder: Dict[str, List[Tuple[str, Dict[str, Any]]]] = {}
    for name, st in status_map.items():
        scripts_by_folder.setdefault(st["folder"], []).append((name, st))

    tables_html = ""
    for folder in sorted(scripts_by_folder):
        display_folder = _escape(normalize_name(folder))
        rows = ""
        for script_name, st in sorted(scripts_by_folder[folder],
                                      key=lambda x: x[0]):
            status_class = f"status-{st['status'].lower().replace(' ', '-')}"
            log_path = st["last_log_file"]
            if log_path and os.path.exists(log_path):
                log_link = f'<a href="/logs?file={quote(log_path)}">Download</a>'
            else:
                log_link = '<span style="color: gray;">Download</span>'

            last_run_str = (st["last_run"].strftime("%B %d, %Y %H:%M:%S")
                            if st["last_run"] else "N/A")
            next_run = st["next_run"] or 0

            rows += f"""
                <tr>
                    <td style="width: 14%;"><a href="/history?script={_escape(script_name)}">{_escape(st['display_name'])}</a></td>
                    <td style="width: 14%;">
                        <input type="radio" name="control_{_escape(script_name)}" value="start" {'checked' if st['running'] else ''} onchange="toggleScript('{_escape(script_name)}', 'start')"> Start
                        <input type="radio" name="control_{_escape(script_name)}" value="stop" {'checked' if not st['running'] else ''} onchange="toggleScript('{_escape(script_name)}', 'stop')"> Stop
                    </td>
                    <td style="width: 8%;" class="{_escape(status_class)}" id="status-{_escape(script_name)}">{_escape(st['status'])}</td>
                    <td style="width: 14%;">{_escape(last_run_str)}</td>
                    <td style="width: 8%;">{_escape(st['last_input'] or 'N/A')}</td>
                    <td style="width: 8%;">{_escape(st['last_duration'] or 'N/A')}</td>
                    <td style="width: 10%;" class="countdown" id="next-run-{_escape(script_name)}" data-next-run="{next_run}">N/A</td>
                    <td style="width: 14%;">{_escape(st['last_error'] or 'None')}</td>
                    <td style="width: 10%;">{log_link}</td>
                </tr>
            """

        tables_html += f"""
            <h2>{display_folder}</h2>
            <table border="1" style="border-collapse: collapse; width: 100%;">
                <tr>
                    <th style="width: 14%;">Script Name</th>
                    <th style="width: 14%;">Control</th>
                    <th style="width: 8%;">Status</th>
                    <th style="width: 14%;">Last Run</th>
                    <th style="width: 8%;">Last Input</th>
                    <th style="width: 8%;">Duration (s)</th>
                    <th style="width: 10%;">Time to Next Run</th>
                    <th style="width: 14%;">Last Error</th>
                    <th style="width: 10%;">Log</th>
                </tr>
                {rows}
            </table>
        """

    if not tables_html:
        tables_html = "<p>No scripts found in any directories.</p>"

    orchestrator_log = request.app["log_file"]
    html_page = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Data Generator Workflow Monitor</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; }}
                table {{ margin-bottom: 20px; }}
                th, td {{ padding: 8px; text-align: left; border: 1px solid #ddd; word-wrap: break-word; }}
                th {{ background-color: #f2f2f2; }}
                .status-running {{ color: green; }}
                .status-completed {{ color: blue; }}
                .status-failed {{ color: red; }}
                .status-stopped {{ color: gray; }}
                a {{ text-decoration: none; color: blue; }}
                a:hover {{ text-decoration: underline; }}
                button {{ padding: 8px 16px; margin: 5px; cursor: pointer; }}
            </style>
            <script>
                let lastCountdownValues = {{}};
                function updateCountdown() {{
                    const now = Date.now() / 1000;
                    document.querySelectorAll('.countdown').forEach(cell => {{
                        const scriptName = cell.id.split('-').slice(2).join('-');
                        const statusCell = document.getElementById(`status-${{scriptName}}`);
                        const status = statusCell ? statusCell.textContent : 'Stopped';
                        const nextRun = parseFloat(cell.getAttribute('data-next-run'));
                        let secondsLeft;
                        if (status === 'Stopped') {{
                            secondsLeft = 0;
                            lastCountdownValues[scriptName] = 0;
                        }} else if (isNaN(nextRun) || nextRun <= now) {{
                            secondsLeft = 'N/A';
                            lastCountdownValues[scriptName] = undefined;
                        }} else {{
                            secondsLeft = Math.max(0, Math.round(nextRun - now));
                            lastCountdownValues[scriptName] = secondsLeft;
                        }}
                        cell.textContent = secondsLeft;
                    }});
                }}
                setInterval(updateCountdown, 1000);
                updateCountdown();

                async function toggleScript(scriptName, action) {{
                    try {{
                        const res = await fetch(`/toggle?script=${{encodeURIComponent(scriptName)}}&action=${{action}}`, {{ method: 'GET' }});
                        if (res.ok) window.location.reload();
                        else alert('Failed to toggle script: ' + res.statusText);
                    }} catch (err) {{
                        alert('Error toggling script: ' + err.message);
                    }}
                }}
            </script>
        </head>
        <body>
            <h1>Data Generator Workflow Monitor</h1>
            <p>
                <a href="/logs?file={quote(orchestrator_log)}"><button>Download Orchestrator Log</button></a>
                <button onclick="window.location.reload()">Refresh</button>
            </p>
            {tables_html}
        </body>
        </html>
    """
    return web.Response(text=html_page, content_type="text/html")


async def history_handler(request: web.Request) -> web.Response:
    """Render a run history table for a script."""
    script_name = request.query.get("script")
    limit_raw = request.query.get("limit", "5")
    try:
        limit = int(limit_raw)
    except ValueError:
        limit = 5
    if limit not in [5, 10, 25, 50, 100]:
        limit = 5

    status_map = request.app["script_status"]
    if not script_name:
        raise web.HTTPBadRequest(text="Missing script name")

    if script_name not in status_map:
        display_name = _escape(normalize_name(script_name))
        html_page = f"""
            <!DOCTYPE html>
            <html>
            <head>
                <title>Run History - {display_name}</title>
                <style>
                    body {{ font-family: Arial, sans-serif; margin: 20px; }}
                    a {{ text-decoration: none; color: blue; }}
                    a:hover {{ text-decoration: underline; }}
                    button {{ padding: 8px 16px; margin: 5px; cursor: pointer; }}
                </style>
            </head>
            <body>
                <p>
                    <a href="/">Back to Main Page</a>
                    <button onclick="window.location.reload()">Refresh</button>
                </p>
                <h2>Run History for {display_name}</h2>
                <p>Script not found. It may not have been discovered yet.</p>
            </body>
            </html>
        """
        return web.Response(text=html_page, content_type="text/html")

    db_conn: sqlite3.Connection = request.app["db_conn"]

    try:
        cur = db_conn.cursor()
        cur.execute(
            """
            SELECT
                COUNT(*) as total_runs,
                SUM(CASE WHEN status = 'Completed' THEN 1 ELSE 0 END) as successful_runs,
                SUM(CASE WHEN status = 'Failed' THEN 1 ELSE 0 END) as failed_runs,
                AVG(records_generated) as avg_records
            FROM run_history
            WHERE script_name = ?
            """,
            (script_name, ),
        )
        total_runs, successful_runs, failed_runs, avg_records = cur.fetchone()
        avg_records = avg_records if avg_records is not None else 0.0

        cur.execute(
            """
            SELECT run_id, input_value, records_generated, start_time, end_time,
                   duration, status, error_message, log_file
            FROM run_history
            WHERE script_name = ?
            ORDER BY start_time DESC
            LIMIT ?
            """,
            (script_name, limit),
        )
        runs = cur.fetchall()
    except sqlite3.Error:
        SCRIPT_LOGGER.error("DB error:\n%s", traceback.format_exc())
        raise web.HTTPInternalServerError(text="Database error")

    rows = ""
    if not runs:
        rows = "<tr><td colspan='9'>No run history available.</td></tr>"
    else:
        for run in runs:
            (
                run_id,
                input_value,
                records_generated,
                start_time,
                end_time,
                duration,
                status,
                error_message,
                log_file,
            ) = run
            status_class = f"status-{(status or '').lower().replace(' ', '-')}"
            log_link = (f'<a href="/logs?file={quote(log_file)}">Download</a>'
                        if log_file and os.path.exists(log_file) else
                        '<span style="color: gray;">Download</span>')
            duration_str = f"{duration:.2f}" if duration else "N/A"
            error_message = error_message or "None"

            rows += f"""
                <tr>
                    <td style="width: 5%;">{_escape(run_id)}</td>
                    <td style="width: 10%;">{_escape(input_value)}</td>
                    <td style="width: 10%;">{_escape(records_generated)}</td>
                    <td style="width: 15%;">{_escape(start_time)}</td>
                    <td style="width: 15%;">{_escape(end_time or 'N/A')}</td>
                    <td style="width: 10%;">{_escape(duration_str)}</td>
                    <td style="width: 10%;" class="{_escape(status_class)}">{_escape(status)}</td>
                    <td style="width: 15%;">{_escape(error_message)}</td>
                    <td style="width: 10%;">{log_link}</td>
                </tr>
            """

    summary_html = f"""
        <h2>Run History for {_escape(normalize_name(script_name))}</h2>
        <p>Total Runs: {_escape(total_runs)}</p>
        <p>Successful Runs: {_escape(successful_runs)}</p>
        <p>Failed Runs: {_escape(failed_runs)}</p>
        <p>Average Records per Run: {_escape(f'{avg_records:.2f}')}</p>
    """

    filter_html = f"""
        <form action="/history" method="get">
            <input type="hidden" name="script" value="{_escape(script_name)}">
            <label for="limit">Show runs: </label>
            <select name="limit" onchange="this.form.submit()">
                <option value="5" {'selected' if limit == 5 else ''}>5</option>
                <option value="10" {'selected' if limit == 10 else ''}>10</option>
                <option value="25" {'selected' if limit == 25 else ''}>25</option>
                <option value="50" {'selected' if limit == 50 else ''}>50</option>
                <option value="100" {'selected' if limit == 100 else ''}>100</option>
            </select>
        </form>
    """

    table_html = f"""
        <table border="1" style="border-collapse: collapse; width: 100%;">
            <tr>
                <th style="width: 5%;">Run ID</th>
                <th style="width: 10%;">Input Value</th>
                <th style="width: 10%;">Records Generated</th>
                <th style="width: 15%;">Start Time</th>
                <th style="width: 15%;">End Time</th>
                <th style="width: 10%;">Duration (s)</th>
                <th style="width: 10%;">Status</th>
                <th style="width: 15%;">Error Message</th>
                <th style="width: 10%;">Log</th>
            </tr>
            {rows}
        </table>
    """

    html_page = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Run History - {_escape(normalize_name(script_name))}</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; }}
                table {{ margin-top: 20px; margin-bottom: 20px; }}
                th, td {{ padding: 8px; text-align: left; border: 1px solid #ddd; word-wrap: break-word; }}
                th {{ background-color: #f2f2f2; }}
                .status-running {{ color: green; }}
                .status-completed {{ color: blue; }}
                .status-failed {{ color: red; }}
                .status-stopped {{ color: gray; }}
                a {{ text-decoration: none; color: blue; }}
                a:hover {{ text-decoration: underline; }}
                button {{ padding: 8px 16px; margin: 5px; cursor: pointer; }}
            </style>
        </head>
        <body>
            <p>
                <a href="/">Back to Main Page</a>
                <button onclick="window.location.reload()">Refresh</button>
            </p>
            {summary_html}
            {filter_html}
            {table_html}
        </body>
        </html>
    """
    return web.Response(text=html_page, content_type="text/html")


async def log_handler(request: web.Request) -> web.Response:
    """Serve a log file from the logs directory as an attachment."""
    file_path = request.query.get("file")
    if (not file_path or not file_path.startswith(LOG_DIR)
            or not os.path.exists(file_path)):
        raise web.HTTPNotFound(text="Log file not found or invalid path")
    try:
        resp = web.FileResponse(
            path=file_path,
            headers={
                "Content-Disposition":
                f"attachment; filename={os.path.basename(file_path)}"
            },
        )
        return resp
    except Exception as e:
        SCRIPT_LOGGER.error("Error serving log %s: %s\n%s", file_path, str(e),
                            traceback.format_exc())
        raise web.HTTPInternalServerError(
            text=f"Error serving log file: {str(e)}")


async def status_handler(request: web.Request) -> web.Response:
    """JSON status for all scripts (excludes the task object)."""
    status_map = request.app["script_status"]
    return web.json_response({
        k: {
            kk: vv
            for kk, vv in v.items() if kk != "task"
        }
        for k, v in status_map.items()
    })


async def toggle_handler(request: web.Request) -> web.Response:
    """Start/stop a script runner loop."""
    script_name = request.query.get("script")
    action = request.query.get("action")
    status_map = request.app["script_status"]
    db_conn = request.app["db_conn"]

    if not script_name or script_name not in status_map:
        raise web.HTTPBadRequest(text=f"Invalid script: {script_name}")

    if action not in {"start", "stop"}:
        raise web.HTTPBadRequest(text=f"Invalid action: {action}")

    if action == "start" and not status_map[script_name]["running"]:
        SCRIPT_LOGGER.info("Starting %s", script_name)
        status_map[script_name]["running"] = True
        status_map[script_name]["task"] = asyncio.create_task(
            script_runner(script_name, status_map, db_conn))
        return web.Response(text="Script started")

    if action == "stop" and status_map[script_name]["running"]:
        SCRIPT_LOGGER.info("Stopping %s", script_name)
        status_map[script_name]["running"] = False
        status_map[script_name]["status"] = "Stopped"  # immediate UI feedback

        # optionally let the loop finish naturally; if you prefer immediate cancel:
        if status_map[script_name]["task"]:
            try:
                status_map[script_name]["task"].cancel()
                await status_map[script_name]["task"]
            except asyncio.CancelledError:
                SCRIPT_LOGGER.info("Cancelled task for %s", script_name)
            finally:
                status_map[script_name]["task"] = None

        return web.Response(text="Script stopped")

    return web.Response(text="No action taken")


async def health_handler(_: web.Request) -> web.Response:
    """Lightweight readiness probe."""
    return web.json_response({"status": "ok"})


async def metrics_handler(request: web.Request) -> web.Response:
    """Simple metrics for dashboards/alerts."""
    status_map = request.app["script_status"]
    total = len(status_map)
    running = sum(1 for v in status_map.values() if v.get("running"))
    failed = sum(1 for v in status_map.values() if v.get("status") == "Failed")
    completed = sum(1 for v in status_map.values()
                    if v.get("status") == "Completed")
    return web.json_response({
        "scripts_total":
        total,
        "scripts_running":
        running,
        "scripts_failed":
        failed,
        "scripts_completed":
        completed,
        "time_utc":
        datetime.now(timezone.utc).isoformat(),
    })


async def favicon_handler(_: web.Request) -> web.Response:
    raise web.HTTPNotFound()


# =============================== Server Boot ============================ #
async def start_http_server(app: web.Application) -> None:
    """Bind and start the HTTP server."""
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", ORCH_PORT)
    await site.start()
    SCRIPT_LOGGER.info("HTTP server started on port %d", ORCH_PORT)


async def _shutdown(app: web.Application) -> None:
    """Graceful shutdown: cancel tasks, close DB."""
    status_map = app["script_status"]
    for name, st in status_map.items():
        st["running"] = False
        task = st.get("task")
        if task and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            st["task"] = None
    conn: sqlite3.Connection = app["db_conn"]
    try:
        conn.close()
    except Exception:
        pass
    SCRIPT_LOGGER.info("Graceful shutdown complete.")


async def main() -> None:
    """Entry point: init logs, DB, discovery, and web server."""
    SCRIPT_LOGGER.info("----- Starting Data Generator Orchestrator -----")
    SCRIPT_LOGGER.info("Logging to: %s", _log_file_path)
    SCRIPT_LOGGER.info(
        "Current time: %s",
        datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"))

    setup_logging()
    cleanup_old_logs()

    # Discover scripts
    global script_status
    script_status = discover_scripts()

    # Init DB
    try:
        db_conn = init_sqlite_db(DB_FILE)
    except Exception:
        sys.exit(1)

    # Web app
    app = web.Application()
    app["script_status"] = script_status
    app["db_conn"] = db_conn
    app["log_file"] = _log_file_path

    app.add_routes([
        web.get("/", index_handler),
        web.get("/history", history_handler),
        web.get("/logs", log_handler),
        web.get("/status", status_handler),
        web.get("/toggle", toggle_handler),
        web.post("/toggle", toggle_handler),
        web.get("/health", health_handler),
        web.get("/metrics", metrics_handler),
        web.get("/favicon.ico", favicon_handler),
    ])

    # graceful shutdown hooks
    app.on_shutdown.append(_shutdown)

    try:
        await start_http_server(app)
    except Exception:
        SCRIPT_LOGGER.error("Failed to start HTTP server:\n%s",
                            traceback.format_exc())
        db_conn.close()
        sys.exit(1)

    # keep event loop alive; log heartbeat hourly
    try:
        while True:
            SCRIPT_LOGGER.info("Event loop active; heartbeat ok")
            await asyncio.sleep(3600)
    finally:
        await _shutdown(app)


if __name__ == "__main__":
    asyncio.run(main())
