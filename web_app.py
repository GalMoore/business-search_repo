from flask import Flask, render_template, request, jsonify, send_file
import os
import subprocess
import json
from datetime import datetime
import pytz
import csv
import threading
import time

app = Flask(__name__)

# Store running searches
running_searches = {}

def sanitize_filename(term):
    """Clean a filename from a search term"""
    import re
    return re.sub(r'[^\w\s-]', '', term).replace(' ', '_').lower()

def run_search_background(search_term, search_id):
    """Run the business search in background"""
    try:
        print(f"🔍 DEBUG: Starting search for '{search_term}' with ID {search_id}")
        running_searches[search_id]['debug_log'] = [f"Starting search for '{search_term}'"]
        running_searches[search_id]['all_runs'] = []  # Store all run progress
        
        # Use the virtual environment python directly instead of source
        venv_python = '/home/Devs/.venv/bin/python3'
        cmd = f"{venv_python} -u business_search_complete.py '{search_term}'"
        print(f"🔍 DEBUG: Running command: {cmd}")
        running_searches[search_id]['debug_log'].append(f"Running command: {cmd}")
        
        # Simulate run progress for immediate frontend feedback
        import time
        import threading
        
        def simulate_progress():
            for i in range(1, 11):
                time.sleep(2)  # Wait 2 seconds between runs
                if running_searches[search_id]['status'] == 'completed':
                    break
                run_text = f"  ▶ Run {i}/10"
                running_searches[search_id]['current_run'] = run_text
                running_searches[search_id]['all_runs'].append(run_text)
                running_searches[search_id]['debug_log'].append(run_text)
                print(f"🔍 SIMULATED PROGRESS: {run_text}")
        
        # Start progress simulation in background
        progress_thread = threading.Thread(target=simulate_progress)
        progress_thread.daemon = True
        progress_thread.start()
        
        # Run the actual command
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, cwd='/home/Devs')
        
        return_code = result.returncode
        full_stdout = result.stdout
        full_stderr = result.stderr
        
        print(f"🔍 DEBUG: Command completed with return code: {return_code}")
        print(f"🔍 DEBUG: STDOUT length: {len(full_stdout)}")
        print(f"🔍 DEBUG: STDERR: {full_stderr}")
        
        # Update search status
        running_searches[search_id]['status'] = 'completed'
        running_searches[search_id]['output'] = full_stdout
        running_searches[search_id]['error'] = full_stderr
        running_searches[search_id]['return_code'] = return_code
        running_searches[search_id]['debug_log'].append(f"Command completed with return code: {return_code}")
        running_searches[search_id]['debug_log'].append(f"STDOUT length: {len(full_stdout)}")
        if full_stderr:
            running_searches[search_id]['debug_log'].append(f"STDERR: {full_stderr}")
        
        # Find the generated CSV file
        israel_tz = pytz.timezone('Asia/Jerusalem')
        timestamp = datetime.now(israel_tz).strftime('%Y%m%d_%H%M%S')
        sanitized_term = sanitize_filename(search_term)
        
        # Look for the most recent folder matching this search
        business_searches_dir = '/home/Devs/business_searches'
        if os.path.exists(business_searches_dir):
            folders = [f for f in os.listdir(business_searches_dir) if f.startswith(sanitized_term)]
            if folders:
                # Get the most recent folder
                latest_folder = sorted(folders)[-1]
                csv_path = os.path.join(business_searches_dir, latest_folder, 'final', 'merged_cleaned_results.csv')
                if os.path.exists(csv_path):
                    running_searches[search_id]['csv_path'] = csv_path
                    
                    # Count results
                    with open(csv_path, 'r') as f:
                        reader = csv.DictReader(f)
                        count = sum(1 for row in reader)
                    running_searches[search_id]['result_count'] = count
        
    except Exception as e:
        running_searches[search_id]['status'] = 'error'
        running_searches[search_id]['error'] = str(e)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/search', methods=['POST'])
def start_search():
    data = request.get_json()
    search_term = data.get('search_term', '').strip()
    
    if not search_term:
        return jsonify({'error': 'Search term is required'}), 400
    
    # Generate unique search ID
    search_id = f"search_{int(time.time())}"
    
    # Initialize search status
    running_searches[search_id] = {
        'status': 'running',
        'search_term': search_term,
        'started_at': datetime.now().isoformat(),
        'output': '',
        'error': '',
        'csv_path': None,
        'result_count': 0
    }
    
    # Start search in background thread
    thread = threading.Thread(target=run_search_background, args=(search_term, search_id))
    thread.daemon = True
    thread.start()
    
    return jsonify({'search_id': search_id, 'status': 'started'})

@app.route('/status/<search_id>')
def get_status(search_id):
    if search_id not in running_searches:
        return jsonify({'error': 'Search not found'}), 404
    
    return jsonify(running_searches[search_id])

@app.route('/download/<search_id>')
def download_csv(search_id):
    if search_id not in running_searches:
        return jsonify({'error': 'Search not found'}), 404
    
    search_info = running_searches[search_id]
    if search_info['status'] != 'completed' or not search_info.get('csv_path'):
        return jsonify({'error': 'CSV not ready'}), 400
    
    csv_path = search_info['csv_path']
    if not os.path.exists(csv_path):
        return jsonify({'error': 'CSV file not found'}), 404
    
    # Generate a nice filename for download
    sanitized_term = sanitize_filename(search_info['search_term'])
    download_name = f"{sanitized_term}_contacts.csv"
    
    return send_file(csv_path, as_attachment=True, download_name=download_name)

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
