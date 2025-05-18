import asyncio
import random
import logging
import subprocess
from datetime import datetime, timedelta, timezone
from typing import Dict, Any
from aiohttp import web
import os

# Dynamic log file name with timestamp
log_file_name = f"/app/logs/data_generator_orchestrator_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.log"

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_file_name),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# List of expected environment variables
EXPECTED_ENV_VARS = [
    "POSTGRES_HOST",
    "POSTGRES_PORT",
    "POSTGRES_DATABASE_NAME",
    "DB_USER",
    "DB_PASSWORD",
    "MONGO_HOST",
    "MONGO_PORT",
    "MONGO_USER",
    "MONGO_PASSWORD",
    "MONGO_DB",
    "MINIO_HOST",
    "MINIO_PORT",
    "MINIO_USER",
    "MINIO_PASSWORD",
    "MINIO_CLIENT_GAMMA_STORAGE_BUCKET",
    "MINIO_CLICKSTREAM_TELEMETRY_BUCKET",
    "KAFKA_BOOTSTRAP_SERVERS",
    "KAFKA_CLICKSTREAM_USER_BEHAVIOUR_TOPIC",
    "KAFKA_TELEMETRY_VEHICLE_STATS_TOPIC",
    "KAFKA_ANALYTICS_SESSION_DURATION_TOPIC"
]

# Global status dictionary to track script execution
script_status: Dict[str, Dict[str, Any]] = {}

# HTML template with fixed tooltip, wider script name column, and Python icon
HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <meta http-equiv="refresh" content="15">
    <title>Data Generator Workflow Monitor</title>
    <link href="https://fonts.googleapis.com/css2?family=Roboto:wght@300;400;500&display=swap" rel="stylesheet">
    <style>
        :root {{
            --mint-primary: #A2D6C3;
            --mint-light: #B6E0CF;
            --mint-hover: #C9E8DC;
            --mint-border: #8BC7B0;
            --background: #E6F2ED;
            --text-dark: #1A3C34;
            --text-light: #fff;
            --start-btn: #A8D5BA;
            --start-btn-hover: #92C4A6;
            --stop-btn: #FF9999;
            --stop-btn-hover: #FF8080;
        }}
        body {{
            font-family: 'Roboto', sans-serif;
            margin: 30px;
            background: linear-gradient(135deg, var(--background), #D9EDE5);
            color: var(--text-dark);
            position: relative;
        }}
        h1 {{
            text-align: center;
            font-weight: 500;
            font-size: 2.4em;
            margin-bottom: 40px;
            text-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        h2 {{
            font-weight: 400;
            font-size: 1.6em;
            margin: 30px 0 15px;
        }}
        table {{
            width: 100%;
            border-spacing: 0;
            margin-top: 10px;
            background: linear-gradient(135deg, rgba(162, 214, 195, 0.9), rgba(162, 214, 195, 0.7));
            backdrop-filter: blur(10px);
            box-shadow: 0 6px 16px rgba(0,0,0,0.2);
            animation: fadeIn 0.5s ease-in;
            table-layout: fixed;
            border-radius: 10px;
            overflow: hidden;
        }}
        th, td {{
            padding: 14px 15px;
            text-align: left;
            border: 1px solid var(--mint-border);
        }}
        th {{
            background: linear-gradient(135deg, var(--mint-primary), var(--mint-border));
            color: var(--text-light);
            font-weight: 500;
            text-transform: uppercase;
            font-size: 0.9em;
            letter-spacing: 0.5px;
            position: sticky;
            top: 0;
            z-index: 1;
            transition: transform 0.2s ease, box-shadow 0.2s ease;
        }}
        th:hover {{
            transform: scale(1.02);
            box-shadow: 0 2px 8px rgba(0,0,0,0.2);
        }}
        th:first-child, tr:last-child td:first-child {{ border-left: none; }}
        th:last-child, tr:last-child td:last-child {{ border-right: none; }}
        tr:first-child th {{ border-top: none; }}
        tr:last-child td {{ border-bottom: none; }}
        th:first-child {{ border-top-left-radius: 10px; }}
        th:last-child {{ border-top-right-radius: 10px; }}
        tr:last-child td:first-child {{ border-bottom-left-radius: 10px; }}
        tr:last-child td:last-child {{ border-bottom-right-radius: 10px; }}
        th:nth-child(1), td:nth-child(1) {{ width: 30%; padding-right: 10px; position: relative; }}
        th:nth-child(2), td:nth-child(2) {{ width: 8%; }}
        th:nth-child(3), td:nth-child(3) {{ width: 10%; }}
        th:nth-child(4), td:nth-child(4) {{ width: 14%; }}
        th:nth-child(5), td:nth-child(5) {{ width: 7%; }}
        th:nth-child(6), td:nth-child(6) {{ width: 7%; }}
        th:nth-child(7), td:nth-child(7) {{ width: 9%; }}
        th:nth-child(8), td:nth-child(8) {{ width: 8%; }}
        th:nth-child(9), td:nth-child(9) {{ width: 7%; }}
        td:nth-child(1) span.script-name {{
            display: inline-block;
            overflow: hidden;
            text-overflow: ellipsis;
            white-space: nowrap;
            max-width: calc(100% - 20px);
            position: relative;
            vertical-align: middle;
            transition: text-shadow 0.2s ease;
        }}
        td:nth-child(1) span.script-name::before {{
            content: '🐍';
            display: inline-block;
            margin-right: 8px;
            font-size: 1.2em;
            color: #3776AB;
            vertical-align: middle;
        }}
        td:nth-child(1) span.script-name:hover {{
            text-shadow: 0 0 5px rgba(0,0,0,0.3);
        }}
        td:nth-child(1) span.script-name:hover .tooltip {{
            visibility: visible;
            opacity: 1;
            transition: opacity 0.2s ease 0.3s, visibility 0s linear 0.3s;
        }}
        td:nth-child(1) .tooltip {{
            visibility: hidden;
            opacity: 0;
            position: absolute;
            background: rgba(255, 255, 255, 0.95);
            color: var(--text-dark);
            padding: 8px 12px;
            border-radius: 6px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.15);
            z-index: 10;
            left: 0;
            top: 100%;
            white-space: normal;
            max-width: 300px;
            transition: opacity 0.2s ease, visibility 0s linear;
        }}
        td:not(:nth-child(1)) {{ overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }}
        tr:nth-child(even) {{ background-color: var(--mint-light); }}
        tr:hover {{
            background-color: var(--mint-hover);
            transform: scale(1.005);
            transition: background-color 0.2s ease, transform 0.2s ease;
        }}
        .status-running {{ color: #FFA500; font-weight: 500; }}
        .status-completed {{ color: #008000; font-weight: 500; }}
        .status-failed {{ color: #FF0000; font-weight: 500; }}
        .status-stopped {{ color: #666; font-weight: 500; }}
        .status-not-found {{ color: #666; font-weight: 500; }}
        td.status-running::before, td.status-completed::before, td.status-failed::before, td.status-stopped::before, td.status-not-found::before {{
            content: '';
            display: inline-block;
            width: 10px;
            height: 10px;
            border-radius: 50%;
            margin-right: 8px;
            vertical-align: middle;
        }}
        td.status-running::before {{ background-color: #FFA500; }}
        td.status-completed::before {{ background-color: #008000; }}
        td.status-failed::before {{ background-color: #FF0000; }}
        td.status-stopped::before {{ background-color: #666; }}
        td.status-not-found::before {{ background-color: #666; }}
        a {{ color: #0066cc; text-decoration: none; transition: color 0.2s ease; }}
        a:hover {{ color: #0033cc; text-decoration: underline; }}
        .toggle-btn {{
            padding: 8px 12px;
            border: none;
            cursor: pointer;
            font-size: 0.9em;
            border-radius: 6px;
            width: 100%;
            background: linear-gradient(135deg, rgba(255, 255, 255, 0.9), rgba(255, 255, 255, 0.7));
            color: #fff;
            transition: transform 0.2s ease, box-shadow 0.2s ease;
            border: 1px solid rgba(255, 255, 255, 0.5);
        }}
        .start-btn {{
            background: linear-gradient(135deg, var(--start-btn), var(--start-btn-hover));
        }}
        .stop-btn {{
            background: linear-gradient(135deg, var(--stop-btn), var(--stop-btn-hover));
        }}
        .toggle-btn:hover {{
            transform: scale(1.05);
            box-shadow: 0 0 8px rgba(255, 255, 255, 0.5);
        }}
        .container {{ max-width: 1600px; margin: 0 auto; position: relative; }}
        .loading {{
            display: none;
            position: fixed;
            top: 50%;
            left: 50%;
            transform: translate(-50%, -50%);
            width: 40px;
            height: 40px;
            border: 4px solid var(--mint-border);
            border-top: 4px solid var(--text-dark);
            border-radius: 50%;
            animation: spin 1s linear infinite;
            z-index: 1000;
        }}
        @keyframes fadeIn {{
            from {{ opacity: 0; transform: translateY(10px); }}
            to {{ opacity: 1; transform: translateY(0); }}
        }}
        @keyframes spin {{
            0% {{ transform: translate(-50%, -50%) rotate(0deg); }}
            100% {{ transform: translate(-50%, -50%) rotate(360deg); }}
        }}
    </style>
</head>
<body>
    <div class="container">
        <h1>Data Generator Workflow Monitor</h1>
        <div class="loading" id="loadingSpinner"></div>
        {tables}
    </div>
    <script>
        function updateCountdown() {{
            const now = new Date().getTime() / 1000;
            document.querySelectorAll('.countdown').forEach(cell => {{
                const nextRun = parseFloat(cell.getAttribute('data-next-run'));
                const secondsLeft = isNaN(nextRun) ? 'N/A' : Math.max(0, Math.round(nextRun - now));
                cell.textContent = secondsLeft;
            }});
        }}
        setInterval(updateCountdown, 1000);
        updateCountdown();

        async function toggleScript(scriptName, action) {{
            console.log('Toggling script:', scriptName, 'with action:', action);
            const spinner = document.getElementById('loadingSpinner');
            spinner.style.display = 'block';
            try {{
                const response = await fetch(`/toggle?script=${{encodeURIComponent(scriptName)}}&action=${{action}}`, {{ method: 'POST' }});
                if (response.ok) {{
                    console.log('Toggle successful, reloading page');
                    location.reload();
                }} else {{
                    console.error('Toggle failed:', response.status, response.statusText);
                    alert('Failed to toggle script: ' + response.statusText);
                }}
            }} catch (error) {{
                console.error('Error during toggle:', error);
                alert('Error toggling script: ' + error.message);
            }} finally {{
                spinner.style.display = 'none';
            }}
        }}
    </script>
</body>
</html>
"""

def check_env_vars() -> None:
    """Check for expected environment variables and log missing ones."""
    missing_vars = [var for var in EXPECTED_ENV_VARS if var not in os.environ]
    if missing_vars:
        logger.warning(f"Missing environment variables: {', '.join(missing_vars)}")
    else:
        logger.info("All expected environment variables are present")

def normalize_name(name: str) -> str:
    """Normalize script or folder name by removing underscores, fixing typos, and capitalizing words."""
    name = name.replace("cleint", "client").replace("vehilce", "vehicle")
    words = name.replace("-", "_").split("_")
    return " ".join(word.capitalize() for word in words)

def discover_scripts() -> None:
    """Dynamically discover Python scripts in mounted directories."""
    script_dirs = [
        "/app/scripts/support_insights",
        "/app/scripts/clickstream-telemetry",
        "/app/scripts/python_scripts_orchestrator"
    ]
    
    for script_dir in script_dirs:
        if not os.path.exists(script_dir):
            logger.warning(f"Directory {script_dir} not found")
            continue
        for root, _, files in os.walk(script_dir):
            for filename in files:
                if filename.endswith(".py") and filename not in ["data_generator_orchestrator.py", "allowed_values.py"]:
                    script_name = os.path.splitext(filename)[0]
                    display_name = normalize_name(script_name)
                    script_path = os.path.join(root, filename)
                    folder_name = os.path.basename(script_dir)
                    if script_name not in script_status:
                        script_status[script_name] = {
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
                        logger.info(f"Discovered script: {script_path} in folder {folder_name} (display: {display_name})")

async def run_script(script_name: str, input_value: int) -> None:
    """Execute a generator script with the given input and update its status."""
    script_path = script_status[script_name]["script_path"]
    log_file = f"/app/logs/{script_name}_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.log"
    logger.info(f"Starting {script_name} with input {input_value}, logging to {log_file}")
    
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
        for var in EXPECTED_ENV_VARS:
            if var in os.environ:
                env[var] = os.environ[var]

        if not os.path.isfile(script_path):
            raise FileNotFoundError(f"Script {script_path} not found")
        if not os.access(script_path, os.X_OK):
            logger.warning(f"Script {script_path} may not be executable")

        process = await asyncio.create_subprocess_exec(
            "python", script_path, str(input_value),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env
        )
        stdout, stderr = await process.communicate()
        
        duration = (datetime.now(timezone.utc) - start_time).total_seconds()
        script_status[script_name]["last_duration"] = round(duration, 2)

        if process.returncode == 0:
            logger.info(f"{script_name} completed successfully in {duration:.2f} seconds")
            script_status[script_name]["status"] = "Completed"
        else:
            error_msg = stderr.decode().strip()
            logger.error(f"{script_name} failed with error: {error_msg}")
            script_status[script_name]["status"] = "Failed"
            script_status[script_name]["last_error"] = error_msg
    except Exception as e:
        duration = (datetime.now(timezone.utc) - start_time).total_seconds()
        script_status[script_name]["last_duration"] = round(duration, 2)
        logger.error(f"Exception while running {script_name}: {str(e)}")
        script_status[script_name]["status"] = "Failed"
        script_status[script_name]["last_error"] = str(e)

async def script_runner(script_name: str) -> None:
    """Run a script at random intervals with random inputs until stopped."""
    logger.info(f"Starting script runner for {script_name}")
    while script_status[script_name]["running"]:
        input_value = random.randint(100, 100_000)
        delay = random.randint(30, 300)

        await run_script(script_name, input_value)
        
        next_run_time = datetime.now(timezone.utc) + timedelta(seconds=delay)
        script_status[script_name]["next_run"] = next_run_time.timestamp()
        
        logger.info(f"{script_name} scheduled to run again in {delay} seconds")
        await asyncio.sleep(delay)
    
    logger.info(f"{script_name} stopped")
    script_status[script_name]["status"] = "Stopped"
    script_status[script_name]["next_run"] = None

async def index_handler(request: web.Request) -> web.Response:
    """Handle HTTP GET requests to the root (/), returning the monitor interface."""
    scripts_by_folder = {}
    for script_name, status in script_status.items():
        folder = status["folder"]
        if folder not in scripts_by_folder:
            scripts_by_folder[folder] = []
        scripts_by_folder[folder].append((script_name, status))
    
    tables = ""
    for folder, scripts in sorted(scripts_by_folder.items()):
        display_folder = normalize_name(folder)
        rows = ""
        for script_name, status in sorted(scripts, key=lambda x: x[0]):
            status_class = f"status-{status['status'].lower().replace(' ', '-')}"
            log_link = f"<a href=\"/logs?file={status['last_log_file']}\">Download</a>" if status['last_log_file'] else "N/A"
            next_run = status['next_run'] if status['next_run'] else 0
            
            last_run_str = (
                status['last_run'].strftime("%B %d, %Y %H:%M:%S %Z")
                if status['last_run'] else "N/A"
            )
            
            button = (
                f"<button class=\"toggle-btn stop-btn\" onclick=\"toggleScript('{script_name}', 'stop')\">Stop</button>"
                if status["running"] else
                f"<button class=\"toggle-btn start-btn\" onclick=\"toggleScript('{script_name}', 'start')\">Start</button>"
            )
            
            rows += (
                f"<tr>"
                f"<td><span class=\"script-name\">{status['display_name']}<span class=\"tooltip\">{status['display_name']}</span></span></td>"
                f"<td>{button}</td>"
                f"<td class=\"{status_class}\">{status['status']}</td>"
                f"<td>{last_run_str}</td>"
                f"<td>{status['last_input'] or 'N/A'}</td>"
                f"<td>{status['last_duration'] or 'N/A'}</td>"
                f"<td class=\"countdown\" data-next-run=\"{next_run}\">N/A</td>"
                f"<td>{status['last_error'] or 'None'}</td>"
                f"<td>{log_link}</td>"
                f"</tr>"
            )
        
        table = (
            f"<h2>{display_folder}</h2>"
            f"<table>"
            f"<tr>"
            f"<th>Script Name</th>"
            f"<th>Control</th>"
            f"<th>Status</th>"
            f"<th>Last Run</th>"
            f"<th>Last Input</th>"
            f"<th>Duration (s)</th>"
            f"<th>Next Run In (s)</th>"
            f"<th>Last Error</th>"
            f"<th>Log File</th>"
            f"</tr>"
            f"{rows}"
            f"</table>"
        )
        tables += table
    
    if not tables:
        tables = "<p>No scripts found in any directories.</p>"
    
    html = HTML_TEMPLATE.format(tables=tables)
    return web.Response(text=html, content_type="text/html")

async def log_handler(request: web.Request) -> web.Response:
    """Handle HTTP GET requests to /logs, serving the requested log file."""
    file_path = request.query.get("file")
    if not file_path or not os.path.exists(file_path) or not file_path.startswith("/app/logs/"):
        raise web.HTTPNotFound(text="Log file not found or invalid path")
    
    return web.FileResponse(
        path=file_path,
        headers={
            "Content-Disposition": f"attachment; filename={os.path.basename(file_path)}"
        }
    )

async def favicon_handler(request: web.Request) -> web.Response:
    """Handle requests for favicon.ico."""
    raise web.HTTPNotFound()

async def status_handler(request: web.Request) -> web.Response:
    """Handle HTTP GET requests to /status, returning the current status as JSON."""
    return web.json_response({k: {key: val for key, val in v.items() if key != "task"} for k, v in script_status.items()})

async def toggle_handler(request: web.Request) -> web.Response:
    """Handle HTTP POST requests to /toggle to start or stop a script."""
    script_name = request.query.get("script")
    action = request.query.get("action")
    
    logger.info(f"Received toggle request for script: {script_name}, action: {action}")
    
    if script_name not in script_status:
        logger.error(f"Invalid script name: {script_name}")
        raise web.HTTPBadRequest(text=f"Invalid script name: {script_name}")
    
    if action not in ["start", "stop"]:
        logger.error(f"Invalid action: {action}")
        raise web.HTTPBadRequest(text=f"Invalid action: {action}")
    
    if action == "start" and not script_status[script_name]["running"]:
        logger.info(f"Starting {script_name}")
        script_status[script_name]["running"] = True
        script_status[script_name]["task"] = asyncio.create_task(script_runner(script_name))
        logger.debug(f"Created task for {script_name}: {script_status[script_name]['task']}")
        return web.Response(text="Script started")
    
    if action == "stop" and script_status[script_name]["running"]:
        logger.info(f"Stopping {script_name}")
        script_status[script_name]["running"] = False
        if script_status[script_name]["task"]:
            script_status[script_name]["task"].cancel()
            try:
                await script_status[script_name]["task"]
            except asyncio.CancelledError:
                logger.debug(f"Task for {script_name} cancelled")
            script_status[script_name]["task"] = None
        script_status[script_name]["status"] = "Stopped"
        script_status[script_name]["next_run"] = None
        return web.Response(text="Script stopped")
    
    logger.info(f"No action taken for {script_name} (action: {action}, running: {script_status[script_name]['running']})")
    return web.Response(text="No action taken")

async def start_http_server() -> None:
    """Start an HTTP server on port 1212 to expose the monitor interface."""
    app = web.Application()
    app.add_routes([
        web.get('/', index_handler),
        web.get('/favicon.ico', favicon_handler),
        web.get('/status', status_handler),
        web.get('/logs', log_handler),
        web.post('/toggle', toggle_handler)
    ])
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', 1212)
    await site.start()
    logger.info("HTTP server started on port 1212")

async def main() -> None:
    """Main function to start the orchestrator and HTTP server."""
    logger.info("Starting Data Generator Orchestrator")

    # Check environment variables
    check_env_vars()

    # Discover scripts dynamically
    discover_scripts()
    
    if not script_status:
        logger.warning("No scripts found in specified directories, continuing with empty script list")

    # Start HTTP server
    await start_http_server()

    # Keep the event loop running
    while True:
        await asyncio.sleep(3600)

if __name__ == "__main__":
    asyncio.run(main())