"""
Orchestrates data generator scripts, providing a web interface to monitor and control execution.
Discovers scripts in /app/scripts, runs them with random inputs, and stores run history and checkpoints
in SQLite. Supports UI enhancements for run history, log downloads, and orchestrator log access.
"""
import asyncio
import os
import sys
import random
import logging
import sqlite3
import subprocess
import traceback
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, Optional
from aiohttp import web
import time
import glob
from urllib.parse import quote

# Logger
SCRIPT_LOGGER = logging.getLogger(__name__)
SCRIPT_LOGGER.setLevel(logging.INFO)

# Use orchestrator-provided log file or fallback
log_file = f"/app/logs/data_generator_orchestrator_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.log"
formatter = logging.Formatter("timestamp=%(asctime)s level=%(levelname)s script=%(name)s message=%(message)s")
file_handler = logging.FileHandler(log_file)
file_handler.setFormatter(formatter)
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)
SCRIPT_LOGGER.addHandler(file_handler)
SCRIPT_LOGGER.addHandler(console_handler)

# Suppress aiohttp.access logs at INFO level
logging.getLogger("aiohttp.access").setLevel(logging.WARNING)

# Global status dictionary to track script execution
script_status: Dict[str, Dict[str, Any]] = {}

def setup_logging(log_file: str) -> None:
    """Configure logging for script events.

    Args:
        log_file: Path to the log file.

    Raises:
        OSError: If log file cannot be created or written to.
    """
    SCRIPT_LOGGER.info("----- Setting Up Logging -----")
    SCRIPT_LOGGER.info("Log file: %s", log_file)
    SCRIPT_LOGGER.info("-----------------------------")

def cleanup_old_logs() -> None:
    """Delete log files in /app/logs older than 7 days."""
    SCRIPT_LOGGER.info("----- Cleaning Up Old Logs -----")
    log_dir = "/app/logs"
    threshold = time.time() - 7 * 24 * 3600  # 7 days in seconds
    deleted = 0
    SCRIPT_LOGGER.info("Scanning directory: %s, threshold: %s", log_dir, datetime.fromtimestamp(threshold).strftime("%Y-%m-%d %H:%M:%S"))
    for log_file in glob.glob(f"{log_dir}/*.log"):
        try:
            mtime = os.path.getmtime(log_file)
            if mtime < threshold:
                os.remove(log_file)
                deleted += 1
                SCRIPT_LOGGER.info("Deleted old log file: %s (last modified: %s)", log_file, datetime.fromtimestamp(mtime).strftime("%Y-%m-%d %H:%M:%S"))
        except OSError as e:
            SCRIPT_LOGGER.error("Failed to delete log file %s: %s\n%s", log_file, str(e), traceback.format_exc())
    if deleted > 0:
        SCRIPT_LOGGER.info("Deleted %d old log files", deleted)
    else:
        SCRIPT_LOGGER.info("No old log files found to delete")
    SCRIPT_LOGGER.info("-------------------------------")

def normalize_name(name: str) -> str:
    """Normalize script or folder name for display.

    Args:
        name: Raw script or folder name.

    Returns:
        Capitalized name with underscores fixed.
    """
    words = name.replace("-", "_").split("_")
    display_name = " ".join(word.capitalize() for word in words)
    SCRIPT_LOGGER.debug("Normalized name: %s -> %s", name, display_name)
    return display_name

def discover_scripts() -> Dict[str, Dict[str, Any]]:
    """Discover Python scripts in /app/scripts directories.

    Returns:
        Dictionary of script metadata (path, folder, display name, etc.).
    """
    SCRIPT_LOGGER.info("----- Discovering Scripts -----")
    script_dirs = [
        "/app/scripts/support_insights",
        "/app/scripts/clickstream_telemetry",
        "/app/scripts/python_scripts_orchestrator"
    ]
    scripts: Dict[str, Dict[str, Any]] = {}
    for script_dir in script_dirs:
        if not os.path.exists(script_dir):
            SCRIPT_LOGGER.info("Directory %s not found", script_dir)
            continue
        SCRIPT_LOGGER.info("Scanning directory: %s", script_dir)
        for root, _, files in os.walk(script_dir):
            for filename in files:
                if filename.endswith(".py") and filename not in ["data_generator_orchestrator.py", "db_operations.py"]:
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
                        "task": None
                    }
                    SCRIPT_LOGGER.info(
                        "Discovered script: name=%s, path=%s, folder=%s, display_name=%s, initial_status=Stopped",
                        script_name, script_path, folder_name, display_name
                    )
    if not scripts:
        SCRIPT_LOGGER.info("No scripts found in specified directories")
    else:
        SCRIPT_LOGGER.info("Total scripts discovered: %d", len(scripts))
    SCRIPT_LOGGER.info("------------------------------")
    return scripts

def init_sqlite_db(db_file: str) -> sqlite3.Connection:
    """Initialize SQLite database connection.

    Args:
        db_file: Path to SQLite database file.

    Returns:
        SQLite connection object.

    Raises:
        sqlite3.Error: If database connection fails.
    """
    SCRIPT_LOGGER.info("----- Initializing SQLite Database -----")
    SCRIPT_LOGGER.info("Connecting to SQLite database: %s", db_file)
    try:
        conn = sqlite3.connect(db_file)
        SCRIPT_LOGGER.info("Successfully connected to SQLite database")
        SCRIPT_LOGGER.info("---------------------------------------")
        return conn
    except sqlite3.Error as e:
        SCRIPT_LOGGER.error("Failed to connect to SQLite database %s: %s\n%s", db_file, str(e), traceback.format_exc())
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
    log_file: Optional[str]
) -> None:
    """Save run details to run_history table.

    Args:
        conn: SQLite connection.
        script_name: Name of the script.
        input_value: Input value for the run.
        records_generated: Total records generated (including updates).
        start_time: Run start time.
        end_time: Run end time (optional).
        status: Run status (Running, Completed, Failed, Stopped).
        error_message: Error message if failed (optional).
        log_file: Path to log file (optional).

    Raises:
        sqlite3.Error: If database operation fails.
    """
    SCRIPT_LOGGER.info("----- Saving Run History for %s -----", script_name)
    try:
        duration = (end_time - start_time).total_seconds() if end_time else None
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO run_history (
                script_name, input_value, records_generated, start_time, end_time,
                duration, status, error_message, log_file
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                log_file
            )
        )
        conn.commit()
        SCRIPT_LOGGER.info(
            "Saved run history: input=%d, records_generated=%d, status=%s, duration=%s, log_file=%s",
            input_value, records_generated, status, duration, log_file
        )
        SCRIPT_LOGGER.info("------------------------------------")
    except sqlite3.Error as e:
        SCRIPT_LOGGER.error("Failed to save run history for %s: %s\n%s", script_name, str(e), traceback.format_exc())
        raise

async def run_script(script_name: str, input_value: int, script_status: Dict[str, Dict[str, Any]], db_conn: sqlite3.Connection) -> None:
    """Execute a generator script and update its status.

    Args:
        script_name: Name of the script.
        input_value: Input value for the script.
        script_status: Global script status dictionary.
        db_conn: SQLite connection for run history.

    Raises:
        Exception: If script execution fails.
    """
    script_path = script_status[script_name]["script_path"]
    log_file = f"/app/logs/{script_name}_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.log"
    SCRIPT_LOGGER.info("----- Starting Script %s -----", script_name)
    SCRIPT_LOGGER.info("Script path: %s", script_path)
    SCRIPT_LOGGER.info("Input value: %d", input_value)
    SCRIPT_LOGGER.info("Logging to: %s", log_file)
    
    script_status[script_name]["status"] = "Running"
    script_status[script_name]["last_run"] = datetime.now(timezone.utc)
    script_status[script_name]["last_input"] = input_value
    script_status[script_name]["last_error"] = None
    script_status[script_name]["last_log_file"] = log_file
    script_status[script_name]["next_run"] = None

    start_time = datetime.now(timezone.utc)
    try:
        env = os.environ.copy()
        env["SCRIPT_LOG_FILE"] = log_file
        SCRIPT_LOGGER.debug("Environment variables prepared for script execution")

        if not os.path.isfile(script_path):
            raise FileNotFoundError(f"Script {script_path} not found")
        if not os.access(script_path, os.X_OK):
            SCRIPT_LOGGER.warning("Script %s may not be executable", script_path)

        process = await asyncio.create_subprocess_exec(
            "python", script_path, str(input_value),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env
        )
        stdout, stderr = await process.communicate()
        duration = (datetime.now(timezone.utc) - start_time).total_seconds()
        script_status[script_name]["last_duration"] = round(duration, 2)

        records_generated = input_value  # Placeholder; scripts should report actual count
        if process.returncode == 0:
            script_status[script_name]["status"] = "Completed"
            SCRIPT_LOGGER.info("%s completed successfully in %.2f seconds", script_name, duration)
            save_run_history(
                db_conn, script_name, input_value, records_generated, start_time, datetime.now(timezone.utc),
                "Completed", None, log_file
            )
        else:
            error_msg = stderr.decode().strip()
            script_status[script_name]["status"] = "Failed"
            script_status[script_name]["last_error"] = error_msg
            SCRIPT_LOGGER.error("%s failed with error: %s", script_name, error_msg)
            save_run_history(
                db_conn, script_name, input_value, records_generated, start_time, datetime.now(timezone.utc),
                "Failed", error_msg, log_file
            )
    except Exception as e:
        duration = (datetime.now(timezone.utc) - start_time).total_seconds()
        script_status[script_name]["last_duration"] = round(duration, 2)
        script_status[script_name]["status"] = "Failed"
        script_status[script_name]["last_error"] = str(e)
        SCRIPT_LOGGER.error("Exception while running %s: %s\n%s", script_name, str(e), traceback.format_exc())
        save_run_history(
            db_conn, script_name, input_value, 0, start_time, datetime.now(timezone.utc),
            "Failed", str(e), log_file
        )
        raise
    finally:
        SCRIPT_LOGGER.info("----------------------------")

async def script_runner(script_name: str, script_status: Dict[str, Dict[str, Any]], db_conn: sqlite3.Connection) -> None:
    """Run a script at random intervals until stopped.

    Args:
        script_name: Name of the script.
        script_status: Global script status dictionary.
        db_conn: SQLite connection for run history.
    """
    SCRIPT_LOGGER.info("----- Starting Script Runner for %s -----", script_name)
    while script_status[script_name]["running"]:
        input_value = random.randint(1_000_000, 2_000_000) if random.random() < 0.05 else random.randint(100, 100_000)
        delay = random.randint(30, 300)
        SCRIPT_LOGGER.info("Generated input_value=%d, delay=%d seconds", input_value, delay)

        await run_script(script_name, input_value, script_status, db_conn)
        
        if script_status[script_name]["running"]:  # Only update next_run if still running
            next_run_time = datetime.now(timezone.utc) + timedelta(seconds=delay)
            script_status[script_name]["next_run"] = next_run_time.timestamp()
            SCRIPT_LOGGER.info("%s scheduled to run again at %s", script_name, next_run_time.strftime("%Y-%m-%d %H:%M:%S"))
            await asyncio.sleep(delay)
    
    script_status[script_name]["status"] = "Stopped"
    # Do not reset next_run to None to allow countdown to continue
    save_run_history(
        db_conn, script_name, script_status[script_name]["last_input"] or 0, 0,
        script_status[script_name]["last_run"] or datetime.now(timezone.utc), datetime.now(timezone.utc),
        "Stopped", None, script_status[script_name]["last_log_file"]
    )
    SCRIPT_LOGGER.info("%s stopped", script_name)
    SCRIPT_LOGGER.info("-----------------------------------------")

async def index_handler(request: web.Request) -> web.Response:
    """Handle GET requests to /, rendering the main monitor interface."""
    SCRIPT_LOGGER.info("----- Handling Index Request -----")
    script_status = request.app["script_status"]
    db_conn = request.app["db_conn"]
    scripts_by_folder = {}
    for script_name, status in script_status.items():
        folder = status["folder"]
        if folder not in scripts_by_folder:
            scripts_by_folder[folder] = []
        scripts_by_folder[folder].append((script_name, status))
        SCRIPT_LOGGER.debug("Grouped script %s under folder %s", script_name, folder)
    
    tables = ""
    for folder in sorted(scripts_by_folder):
        display_folder = normalize_name(folder)
        rows = ""
        for script_name, status in sorted(scripts_by_folder[folder], key=lambda x: x[0]):
            status_class = f"status-{status['status'].lower().replace(' ', '-')}"
            log_link = (
                f'<a href="/logs?file={quote(status["last_log_file"])}">Download</a>'
                if status["last_log_file"] and os.path.exists(status["last_log_file"])
                else '<span style="color: gray;">Download</span>'
            )
            last_run_str = status["last_run"].strftime("%B %d, %Y %H:%M:%S") if status["last_run"] else "N/A"
            next_run = status["next_run"] if status["next_run"] else 0
            
            rows += f"""
                <tr>
                    <td style="width: 14%;"><a href="/history?script={script_name}">{status['display_name']}</a></td>
                    <td style="width: 14%;">
                        <input type="radio" name="control_{script_name}" value="start" {'checked' if status['running'] else ''} onchange="toggleScript('{script_name}', 'start')"> Start
                        <input type="radio" name="control_{script_name}" value="stop" {'checked' if not status['running'] else ''} onchange="toggleScript('{script_name}', 'stop')"> Stop
                    </td>
                    <td style="width: 8%;" class="{status_class}" id="status-{script_name}">{status['status']}</td>
                    <td style="width: 14%;">{last_run_str}</td>
                    <td style="width: 8%;">{status['last_input'] or 'N/A'}</td>
                    <td style="width: 8%;">{status['last_duration'] or 'N/A'}</td>
                    <td style="width: 10%;" class="countdown" id="next-run-{script_name}" data-next-run="{next_run}">N/A</td>
                    <td style="width: 14%;">{status['last_error'] or 'None'}</td>
                    <td style="width: 10%;">{log_link}</td>
                </tr>
            """
        
        tables += f"""
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
    
    if not tables:
        tables = "<p>No scripts found in any directories.</p>"
    
    orchestrator_log = request.app["log_file"]
    html = f"""
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
                let lastCountdownValues = {{}}; // Store last countdown values for each script
                function updateCountdown() {{
                    const now = new Date().getTime() / 1000;
                    document.querySelectorAll('.countdown').forEach(cell => {{
                        const scriptName = cell.id.split('-')[2]; // Extract scriptName from "next-run-{{scriptName}}"
                        const statusCell = document.getElementById(`status-${{scriptName}}`);
                        const status = statusCell ? statusCell.textContent : 'Stopped';
                        let nextRun = parseFloat(cell.getAttribute('data-next-run'));
                        let secondsLeft;

                        if (status === 'Stopped') {{
                            // Immediately reset countdown to 0 when stopped
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
                        const response = await fetch(`/toggle?script=${{encodeURIComponent(scriptName)}}&action=${{action}}`, {{ method: 'GET' }});
                        if (response.ok) {{
                            window.location.reload();
                        }} else {{
                            alert('Failed to toggle script: ' + response.statusText);
                        }}
                    }} catch (error) {{
                        alert('Error toggling script: ' + error.message);
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
            {tables}
        </body>
        </html>
    """
    SCRIPT_LOGGER.info("Rendered index page with %d script folders", len(scripts_by_folder))
    SCRIPT_LOGGER.info("----------------------------------")
    return web.Response(text=html, content_type="text/html")

async def history_handler(request: web.Request) -> web.Response:
    """Handle GET requests to /history, showing run history for a script."""
    SCRIPT_LOGGER.info("----- Handling History Request -----")
    script_name = request.query.get("script")
    limit = int(request.query.get("limit", "5"))
    if limit not in [5, 10, 25, 50, 100]:
        limit = 5
    SCRIPT_LOGGER.info("Script name: %s, limit: %d", script_name, limit)
    
    script_status = request.app["script_status"]
    if not script_name:
        SCRIPT_LOGGER.error("Missing script name in history request")
        raise web.HTTPBadRequest(text="Missing script name")
    
    if script_name not in script_status:
        display_name = normalize_name(script_name)
        html = f"""
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
                <script>
                    function refreshPage() {{
                        window.location.reload();
                    }}
                </script>
            </head>
            <body>
                <p>
                    <a href="/">Back to Main Page</a>
                    <button onclick="refreshPage()">Refresh</button>
                </p>
                <h2>Run History for {display_name}</h2>
                <p>Script not found. It may not have been discovered yet.</p>
            </body>
            </html>
        """
        SCRIPT_LOGGER.info("Script %s not found, returning error page", script_name)
        SCRIPT_LOGGER.info("------------------------------------")
        return web.Response(text=html, content_type="text/html")
    
    db_conn = request.app["db_conn"]
    try:
        cursor = db_conn.cursor()
        # Summary
        cursor.execute(
            """
            SELECT
                COUNT(*) as total_runs,
                SUM(CASE WHEN status = 'Completed' THEN 1 ELSE 0 END) as successful_runs,
                SUM(CASE WHEN status = 'Failed' THEN 1 ELSE 0 END) as failed_runs,
                AVG(records_generated) as avg_records
            FROM run_history
            WHERE script_name = ?
            """,
            (script_name,)
        )
        summary = cursor.fetchone()
        total_runs, successful_runs, failed_runs, avg_records = summary
        avg_records = avg_records if avg_records is not None else 0.0
        SCRIPT_LOGGER.info(
            "Run history summary for %s: total_runs=%d, successful_runs=%d, failed_runs=%d, avg_records=%.2f",
            script_name, total_runs, successful_runs, failed_runs, avg_records
        )
        
        # History
        cursor.execute(
            """
            SELECT run_id, input_value, records_generated, start_time, end_time,
                   duration, status, error_message, log_file
            FROM run_history
            WHERE script_name = ?
            ORDER BY start_time DESC
            LIMIT ?
            """,
            (script_name, limit)
        )
        runs = cursor.fetchall()
        SCRIPT_LOGGER.info("Fetched %d run history entries for %s", len(runs), script_name)
    except sqlite3.Error as e:
        SCRIPT_LOGGER.error("Failed to fetch run history for %s: %s\n%s", script_name, str(e), traceback.format_exc())
        raise web.HTTPInternalServerError(text="Database error")
    
    summary_html = f"""
        <h2>Run History for {normalize_name(script_name)}</h2>
        <p>Total Runs: {total_runs}</p>
        <p>Successful Runs: {successful_runs}</p>
        <p>Failed Runs: {failed_runs}</p>
        <p>Average Records per Run: {avg_records:.2f}</p>
    """
    
    filter_html = """
        <form action="/history" method="get">
            <input type="hidden" name="script" value="{script_name}">
            <label for="limit">Show runs: </label>
            <select name="limit" onchange="this.form.submit()">
                <option value="5" {selected_5}>5</option>
                <option value="10" {selected_10}>10</option>
                <option value="25" {selected_25}>25</option>
                <option value="50" {selected_50}>50</option>
                <option value="100" {selected_100}>100</option>
            </select>
        </form>
    """.format(
        script_name=script_name,
        selected_5='selected' if limit == 5 else '',
        selected_10='selected' if limit == 10 else '',
        selected_25='selected' if limit == 25 else '',
        selected_50='selected' if limit == 50 else '',
        selected_100='selected' if limit == 100 else ''
    )
    
    rows = ""
    if not runs:
        rows = "<tr><td colspan='9'>No run history available.</td></tr>"
    else:
        for run in runs:
            run_id, input_value, records_generated, start_time, end_time, duration, status, error_message, log_file = run
            status_class = f"status-{status.lower().replace(' ', '-')}"
            log_link = f'<a href="/logs?file={quote(log_file)}">Download</a>' if log_file and os.path.exists(log_file) else '<span style="color: gray;">Download</span>'
            duration_str = f"{duration:.2f}" if duration else "N/A"
            error_message = error_message or "None"
            rows += f"""
                <tr>
                    <td style="width: 5%;">{run_id}</td>
                    <td style="width: 10%;">{input_value}</td>
                    <td style="width: 10%;">{records_generated}</td>
                    <td style="width: 15%;">{start_time}</td>
                    <td style="width: 15%;">{end_time or 'N/A'}</td>
                    <td style="width: 10%;">{duration_str}</td>
                    <td style="width: 10%;" class="{status_class}">{status}</td>
                    <td style="width: 15%;">{error_message}</td>
                    <td style="width: 10%;">{log_link}</td>
                </tr>
            """
    
    table = f"""
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
    
    html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Run History - {normalize_name(script_name)}</title>
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
            <script>
                function refreshPage() {{
                    window.location.reload();
                }}
            </script>
        </head>
        <body>
            <p>
                <a href="/">Back to Main Page</a>
                <button onclick="refreshPage()">Refresh</button>
            </p>
            {summary_html}
            {filter_html}
            {table}
        </body>
        </html>
    """
    SCRIPT_LOGGER.info("Rendered history page for %s with %d run entries", script_name, len(runs))
    SCRIPT_LOGGER.info("------------------------------------")
    return web.Response(text=html, content_type="text/html")

async def log_handler(request: web.Request) -> web.Response:
    """Handle GET requests to /logs, serving the requested log file."""
    SCRIPT_LOGGER.info("----- Handling Log Request -----")
    file_path = request.query.get("file")
    if not file_path or not os.path.exists(file_path) or not file_path.startswith("/app/logs/"):
        SCRIPT_LOGGER.error("Log file not found or invalid path: %s", file_path)
        raise web.HTTPNotFound(text="Log file not found or invalid path")
    
    try:
        response = web.FileResponse(
            path=file_path,
            headers={"Content-Disposition": f"attachment; filename={os.path.basename(file_path)}"}
        )
        SCRIPT_LOGGER.info("Successfully served log file: %s", file_path)
        SCRIPT_LOGGER.info("--------------------------------")
        return response
    except Exception as e:
        SCRIPT_LOGGER.error("Error serving log file %s: %s\n%s", file_path, str(e), traceback.format_exc())
        raise web.HTTPInternalServerError(text=f"Error serving log file: {str(e)}")

async def favicon_handler(request: web.Request) -> web.Response:
    """Handle requests for favicon.ico."""
    SCRIPT_LOGGER.info("Handling favicon request, returning 404")
    raise web.HTTPNotFound()

async def status_handler(request: web.Request) -> web.Response:
    """Handle GET requests to /status, returning current script status as JSON."""
    SCRIPT_LOGGER.info("----- Handling Status Request -----")
    script_status = request.app["script_status"]
    response = web.json_response({k: {key: val for key, val in v.items() if key != "task"} for k, v in script_status.items()})
    SCRIPT_LOGGER.info("Returned status for %d scripts", len(script_status))
    SCRIPT_LOGGER.info("-----------------------------------")
    return response

async def toggle_handler(request: web.Request) -> web.Response:
    """Handle GET and POST requests to /toggle to start or stop a script."""
    SCRIPT_LOGGER.info("----- Handling Toggle Request -----")
    script_name = request.query.get("script")
    action = request.query.get("action")
    SCRIPT_LOGGER.info("Received toggle request: script=%s, action=%s", script_name, action)
    
    script_status = request.app["script_status"]
    db_conn = request.app["db_conn"]
    
    if script_name not in script_status:
        SCRIPT_LOGGER.error("Invalid script name: %s", script_name)
        raise web.HTTPBadRequest(text=f"Invalid script name: {script_name}")
    
    if action not in ["start", "stop"]:
        SCRIPT_LOGGER.error("Invalid action: %s", action)
        raise web.HTTPBadRequest(text=f"Invalid action: {action}")
    
    if action == "start" and not script_status[script_name]["running"]:
        SCRIPT_LOGGER.info("Starting %s (previous state: %s)", script_name, script_status[script_name]["status"])
        script_status[script_name]["running"] = True
        script_status[script_name]["task"] = asyncio.create_task(script_runner(script_name, script_status, db_conn))
        SCRIPT_LOGGER.info("Script %s started successfully", script_name)
        SCRIPT_LOGGER.info("-----------------------------------")
        return web.Response(text="Script started")
    
    if action == "stop" and script_status[script_name]["running"]:
        SCRIPT_LOGGER.info("Stopping %s (previous state: %s)", script_name, script_status[script_name]["status"])
        script_status[script_name]["running"] = False
        script_status[script_name]["status"] = "Stopped"  # Ensure status is set to Stopped immediately
        if script_status[script_name]["task"]:
            script_status[script_name]["task"].cancel()
            try:
                await script_status[script_name]["task"]
            except asyncio.CancelledError:
                SCRIPT_LOGGER.info("Task for %s cancelled", script_name)
            script_status[script_name]["task"] = None
        SCRIPT_LOGGER.info("Script %s stopped successfully", script_name)
        SCRIPT_LOGGER.info("-----------------------------------")
        return web.Response(text="Script stopped")
    
    SCRIPT_LOGGER.info("No action taken for %s (action: %s, running: %s)", script_name, action, script_status[script_name]["running"])
    SCRIPT_LOGGER.info("-----------------------------------")
    return web.Response(text="No action taken")

async def start_http_server(app: web.Application) -> None:
    """Start the HTTP server on port 1212.

    Args:
        app: Aiohttp application instance.

    Raises:
        Exception: If server startup fails.
    """
    SCRIPT_LOGGER.info("----- Starting HTTP Server -----")
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', 1212)
    await site.start()
    SCRIPT_LOGGER.info("HTTP server started on port 1212")
    SCRIPT_LOGGER.info("Routes configured: %s", ", ".join([str(route) for route in app.router.routes()]))
    SCRIPT_LOGGER.info("--------------------------------")

async def main() -> None:
    """Main function to start the orchestrator and HTTP server."""
    SCRIPT_LOGGER.info("----- Starting Data Generator Orchestrator -----")
    SCRIPT_LOGGER.info("Logging to: %s", log_file)
    SCRIPT_LOGGER.info("Current time: %s", datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"))

    # Clean up old logs
    cleanup_old_logs()

    # Discover scripts
    script_status = discover_scripts()
    
    # Initialize SQLite database
    db_file = "/app/storage/orchestrator.db"
    try:
        db_conn = init_sqlite_db(db_file)
    except sqlite3.Error as e:
        SCRIPT_LOGGER.error("Failed to initialize SQLite database: %s\n%s", str(e), traceback.format_exc())
        sys.exit(1)

    # Start HTTP server
    app = web.Application()
    app["script_status"] = script_status
    app["db_conn"] = db_conn
    app["log_file"] = log_file
    app.add_routes([
        web.get('/', index_handler),
        web.get('/history', history_handler),
        web.get('/logs', log_handler),
        web.get('/status', status_handler),
        web.get('/favicon.ico', favicon_handler),
        web.get('/toggle', toggle_handler),
        web.post('/toggle', toggle_handler)
    ])
    
    try:
        await start_http_server(app)
    except Exception as e:
        SCRIPT_LOGGER.error("Failed to start HTTP server: %s\n%s", str(e), traceback.format_exc())
        db_conn.close()
        sys.exit(1)

    # Keep the event loop running
    try:
        while True:
            SCRIPT_LOGGER.info("Event loop active, waiting for next cycle")
            await asyncio.sleep(3600)
    finally:
        db_conn.close()
        SCRIPT_LOGGER.info("Closed SQLite database connection")
        SCRIPT_LOGGER.info("-----------------------------------------------")

if __name__ == "__main__":
    asyncio.run(main())