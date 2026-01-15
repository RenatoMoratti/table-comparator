import subprocess
import webbrowser
import sys
import os

# Path to your app.py
script_path = os.path.join(os.path.dirname(__file__), 'app.py')

# Start the Flask app in the background (no console window)
subprocess.Popen([
    sys.executable, script_path
], creationflags=subprocess.CREATE_NO_WINDOW)

# Open the browser to the app (adjust port if needed)
webbrowser.open('http://127.0.0.1:5000')
