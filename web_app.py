from flask import Flask, render_template, request, jsonify, send_file
import os
import subprocess
import json
from datetime import datetime
import pytz
import csv
import threading
import time
import re

app = Flask(__name__)

# Store running searches
running_searches = {}

def sanitize_filename(term):
    """Clean a filename from a search term"""
    return re.sub(r'[^\w\s-]', '', term).replace(' ', '_').lower()

def run_search_background(search_term, search_id, iterations=10):
    """Run the business search in background"""
    try:
        print(f"🔍 DEBUG: Starting search for '{search_term}' with ID {search_id}, iterations: {iterations}")
        running_searches[search_id]['debug_log'] = [f"Starting search for '{search_term}' with {iterations} iterations"]
        running_searches[search_id]['all_runs'] = []  # Store all run progress
        
        # Use the virtual environment python directly instead of source
        venv_python = '/home/Devs/.venv/bin/python3'
        cmd = f"{venv_python} -u business_search_complete.py '{search_term}' --iterations {iterations}"
        print(f"🔍 DEBUG: Running command: {cmd}")
        running_searches[search_id]['debug_log'].append(f"Running command: {cmd}")
        
        # Simulate run progress for immediate frontend feedback
        import time
        import threading
        
        def simulate_progress():
            for i in range(1, iterations + 1):
                time.sleep(2)  # Wait 2 seconds between runs
                if running_searches[search_id]['status'] == 'completed':
                    break
                run_text = f"  ▶ Run {i}/{iterations}"
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
        running_searches[search_id]['completed_at'] = datetime.now(pytz.timezone('Asia/Jerusalem')).isoformat()
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

def run_multi_term_multi_location_search_background(search_term_list, location_list, search_id, iterations=10):
    """Run business search across multiple search terms and multiple locations (matrix search)"""
    try:
        total_searches = len(search_term_list) * len(location_list)
        print(f"🌍 DEBUG: Starting multi-term multi-location search for {len(search_term_list)} terms across {len(location_list)} locations ({total_searches} total searches)")
        running_searches[search_id]['debug_log'] = [f"Starting matrix search: {len(search_term_list)} terms × {len(location_list)} locations = {total_searches} searches"]
        running_searches[search_id]['all_runs'] = []
        
        all_csv_files = []
        search_counter = 0
        
        # Process each search term with each location (matrix)
        for search_term in search_term_list:
            for location in location_list:
                search_counter += 1
                running_searches[search_id]['current_search_term'] = search_term
                running_searches[search_id]['current_location'] = location
                running_searches[search_id]['completed_searches'] = search_counter - 1
                
                # Create combined search term with location
                combined_search_term = f"{location} {search_term}"
                print(f"🔍 Processing search {search_counter}/{total_searches}: '{search_term}' in '{location}'")
                
                running_searches[search_id]['debug_log'].append(f"Search {search_counter}/{total_searches}: {search_term} in {location}")
                
                # Use the virtual environment python directly
                venv_python = '/home/Devs/.venv/bin/python3'
                cmd = f"{venv_python} -u business_search_complete.py '{combined_search_term}' --iterations {iterations}"
                
                print(f"🔍 DEBUG: Running command: {cmd}")
                running_searches[search_id]['debug_log'].append(f"Running: {combined_search_term}")
                
                # Run the search for this term-location combination
                result = subprocess.run(cmd, shell=True, capture_output=True, text=True, cwd='/home/Devs')
                
                if result.returncode == 0:
                    # Find the generated CSV file for this search
                    business_searches_dir = '/home/Devs/business_searches'
                    if os.path.exists(business_searches_dir):
                        # Find the most recent directory that contains our search term
                        sanitized_combined_term = sanitize_filename(combined_search_term)
                        matching_dirs = []
                        
                        for dir_name in os.listdir(business_searches_dir):
                            if sanitized_combined_term.lower() in dir_name.lower():
                                dir_path = os.path.join(business_searches_dir, dir_name)
                                if os.path.isdir(dir_path):
                                    matching_dirs.append(dir_path)
                        
                        if matching_dirs:
                            # Get the most recent directory
                            latest_dir = max(matching_dirs, key=os.path.getctime)
                            
                            # Look for the final CSV file in the final subdirectory
                            final_dir = os.path.join(latest_dir, 'final')
                            if os.path.exists(final_dir):
                                csv_files = [f for f in os.listdir(final_dir) if f.endswith('.csv')]
                                if csv_files:
                                    csv_path = os.path.join(final_dir, csv_files[0])
                                    all_csv_files.append({
                                        'search_term': search_term,
                                        'location': location,
                                        'csv_path': csv_path
                                    })
                                    print(f"✅ Search {search_counter}/{total_searches} completed: {csv_path}")
                                else:
                                    print(f"⚠️ No CSV found in final directory for: {search_term} in {location}")
                            else:
                                print(f"⚠️ No final directory found for: {search_term} in {location}")
                        else:
                            print(f"⚠️ No matching directory found for: {search_term} in {location}")
                    else:
                        print(f"⚠️ Business searches directory not found")
                else:
                    print(f"❌ Search failed for: {search_term} in {location}")
                    print(f"❌ Error output: {result.stderr}")
                    running_searches[search_id]['debug_log'].append(f"Failed: {search_term} in {location} - {result.stderr}")
        
        # Update completion status
        running_searches[search_id]['completed_searches'] = total_searches
        
        # Merge all CSV files into one
        if all_csv_files:
            # Create output directory for merged results
            main_output_dir = f"/home/Devs/business_searches/{search_id}"
            os.makedirs(main_output_dir, exist_ok=True)
            
            merged_csv_path = merge_multi_term_location_csvs(all_csv_files, main_output_dir)
            
            running_searches[search_id]['status'] = 'completed'
            running_searches[search_id]['completed_at'] = datetime.now(pytz.timezone('Asia/Jerusalem')).isoformat()
            running_searches[search_id]['debug_log'].append(f"Multi-term multi-location search completed successfully")
            running_searches[search_id]['csv_path'] = merged_csv_path
            running_searches[search_id]['message'] = f'Multi-term multi-location search completed! Found results from {len(all_csv_files)} searches.'
            
            # Count total results
            if os.path.exists(merged_csv_path):
                with open(merged_csv_path, 'r') as f:
                    reader = csv.DictReader(f)
                    count = sum(1 for row in reader)
                running_searches[search_id]['result_count'] = count
        else:
            running_searches[search_id]['status'] = 'error'
            running_searches[search_id]['error'] = 'No results found from any search'
            
    except Exception as e:
        running_searches[search_id]['status'] = 'error'
        running_searches[search_id]['error'] = str(e)
        print(f"❌ Multi-term multi-location search error: {str(e)}")

def run_multi_location_search_background(search_term, location_list, search_id, iterations=10):
    """Run business search across multiple locations and merge results"""
    try:
        print(f"🌍 DEBUG: Starting multi-location search for '{search_term}' across {len(location_list)} locations")
        running_searches[search_id]['debug_log'] = [f"Starting multi-location search across {len(location_list)} locations"]
        running_searches[search_id]['all_runs'] = []
        
        location_csv_files = []
        
        # Process each location
        for location_idx, location in enumerate(location_list):
            running_searches[search_id]['current_location'] = location
            running_searches[search_id]['completed_locations'] = location_idx
            
            # Create combined search term with location
            combined_search_term = f"{location} {search_term}"
            print(f"🔍 Processing location {location_idx + 1}/{len(location_list)}: {location}")
            
            running_searches[search_id]['debug_log'].append(f"Processing location: {location}")
            
            # Use the virtual environment python directly
            venv_python = '/home/Devs/.venv/bin/python3'
            cmd = f"{venv_python} -u business_search_complete.py '{combined_search_term}' --iterations {iterations}"
            
            print(f"🔍 DEBUG: Running command: {cmd}")
            running_searches[search_id]['debug_log'].append(f"Running command: {cmd}")
            
            # Run the search for this location (let script create its own directories)
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True, cwd='/home/Devs')
            
            print(f"🔍 DEBUG: Command output: {result.stdout}")
            print(f"🔍 DEBUG: Command stderr: {result.stderr}")
            
            if result.returncode == 0:
                # The script creates its own directory structure, so we need to find the most recent one
                # Look for directories that match the search pattern
                business_searches_dir = '/home/Devs/business_searches'
                if os.path.exists(business_searches_dir):
                    # Find the most recent directory that contains our search term
                    sanitized_combined_term = sanitize_filename(combined_search_term)
                    matching_dirs = []
                    
                    for dir_name in os.listdir(business_searches_dir):
                        if sanitized_combined_term.lower() in dir_name.lower():
                            dir_path = os.path.join(business_searches_dir, dir_name)
                            if os.path.isdir(dir_path):
                                matching_dirs.append(dir_path)
                    
                    if matching_dirs:
                        # Get the most recent directory
                        latest_dir = max(matching_dirs, key=os.path.getctime)
                        
                        # Look for the final CSV file in the final subdirectory
                        final_dir = os.path.join(latest_dir, 'final')
                        if os.path.exists(final_dir):
                            csv_files = [f for f in os.listdir(final_dir) if f.endswith('.csv')]
                            if csv_files:
                                csv_path = os.path.join(final_dir, csv_files[0])
                                location_csv_files.append({
                                    'location': location,
                                    'csv_path': csv_path
                                })
                                print(f"✅ Location {location} completed: {csv_path}")
                            else:
                                print(f"⚠️ No CSV found in final directory for location: {location}")
                        else:
                            print(f"⚠️ No final directory found for location: {location}")
                    else:
                        print(f"⚠️ No matching directory found for location: {location}")
                else:
                    print(f"⚠️ Business searches directory not found")
            else:
                print(f"❌ Search failed for location: {location}")
                print(f"❌ Error output: {result.stderr}")
                running_searches[search_id]['debug_log'].append(f"Search failed for location: {location} - {result.stderr}")
        
        # Update completion status
        running_searches[search_id]['completed_locations'] = len(location_list)
        
        # Merge all location CSV files into one
        if location_csv_files:
            # Create output directory for merged results
            main_output_dir = f"/home/Devs/business_searches/{search_id}"
            os.makedirs(main_output_dir, exist_ok=True)
            
            merged_csv_path = merge_location_csvs(location_csv_files, main_output_dir)
            
            running_searches[search_id]['status'] = 'completed'
            running_searches[search_id]['completed_at'] = datetime.now(pytz.timezone('Asia/Jerusalem')).isoformat()
            running_searches[search_id]['csv_path'] = merged_csv_path
            running_searches[search_id]['message'] = f'Multi-location search completed! Found results from {len(location_csv_files)} locations.'
            
            # Count total results
            if os.path.exists(merged_csv_path):
                with open(merged_csv_path, 'r') as f:
                    reader = csv.DictReader(f)
                    count = sum(1 for row in reader)
                running_searches[search_id]['result_count'] = count
        else:
            running_searches[search_id]['status'] = 'error'
            running_searches[search_id]['error'] = 'No results found from any location'
            
    except Exception as e:
        running_searches[search_id]['status'] = 'error'
        running_searches[search_id]['error'] = str(e)
        print(f"❌ Multi-location search error: {str(e)}")

def merge_multi_term_location_csvs(search_csv_files, output_dir):
    """Merge CSV files from multiple search terms and locations and remove duplicates"""
    merged_csv_path = os.path.join(output_dir, "merged_all_searches.csv")
    
    seen_emails = set()
    all_rows = []
    
    for search_data in search_csv_files:
        search_term = search_data['search_term']
        location = search_data['location']
        csv_path = search_data['csv_path']
        
        if os.path.exists(csv_path):
            with open(csv_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    email = row.get('Email', '').strip().lower()
                    if email and email not in seen_emails and email != 'no email found':
                        seen_emails.add(email)
                        # Add search term and location info to the row
                        row['SearchTerm'] = search_term
                        row['Location'] = location
                        all_rows.append(row)
    
    # Write merged results
    if all_rows:
        fieldnames = ['URL', 'Email', 'SearchTerm', 'Location', 'SourceFile']
        with open(merged_csv_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(all_rows)
    
    print(f"📊 Merged {len(all_rows)} unique results from {len(search_csv_files)} searches")
    return merged_csv_path

def merge_location_csvs(location_csv_files, output_dir):
    """Merge CSV files from multiple locations and remove duplicates"""
    merged_csv_path = os.path.join(output_dir, "merged_all_locations.csv")
    
    seen_emails = set()
    all_rows = []
    
    for location_data in location_csv_files:
        location = location_data['location']
        csv_path = location_data['csv_path']
        
        if os.path.exists(csv_path):
            with open(csv_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    email = row.get('Email', '').strip().lower()
                    if email and email not in seen_emails and email != 'no email found':
                        seen_emails.add(email)
                        # Add location info to the row
                        row['Location'] = location
                        all_rows.append(row)
    
    # Write merged results
    if all_rows:
        fieldnames = ['URL', 'Email', 'Location', 'SourceFile']
        with open(merged_csv_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(all_rows)
    
    print(f"📊 Merged {len(all_rows)} unique results from {len(location_csv_files)} locations")
    return merged_csv_path

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/start_search', methods=['POST'])
def start_search():
    data = request.get_json()
    search_terms = data.get('search_term', '').strip()
    locations = data.get('locations', '').strip()
    iterations = data.get('iterations', 10)
    
    if not search_terms:
        return jsonify({'error': 'Search terms are required'}), 400
    
    if not locations:
        return jsonify({'error': 'Locations are required'}), 400
    
    # Parse search terms (comma-separated)
    search_term_list = [term.strip() for term in search_terms.split(',') if term.strip()]
    if not search_term_list:
        return jsonify({'error': 'At least one search term is required'}), 400
    
    # Parse locations (comma-separated)
    location_list = [loc.strip() for loc in locations.split(',') if loc.strip()]
    if not location_list:
        return jsonify({'error': 'At least one location is required'}), 400
    
    # Validate iterations
    try:
        iterations = int(iterations)
        if iterations < 1 or iterations > 50:
            return jsonify({'error': 'Iterations must be between 1 and 50'}), 400
    except (ValueError, TypeError):
        iterations = 10
    
    # Generate unique search ID
    search_id = f"search_{int(time.time())}"
    
    # Calculate total searches (terms × locations)
    total_searches = len(search_term_list) * len(location_list)
    
    # Initialize search status
    running_searches[search_id] = {
        'status': 'running',
        'search_terms': search_term_list,
        'locations': location_list,
        'iterations': iterations,
        'started_at': datetime.now(pytz.timezone('Asia/Jerusalem')).isoformat(),
        'output': '',
        'error': '',
        'csv_path': None,
        'result_count': 0,
        'current_search_term': '',
        'current_location': '',
        'completed_searches': 0,
        'total_searches': total_searches
    }
    
    # Start search in background thread
    thread = threading.Thread(target=run_multi_term_multi_location_search_background, args=(search_term_list, location_list, search_id, iterations))
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
    
    # Generate a simple, short filename for download
    # Use timestamp and search count for unique, short filenames
    israel_tz = pytz.timezone('Asia/Jerusalem')
    timestamp = datetime.now(israel_tz).strftime('%Y%m%d_%H%M%S')
    
    # Determine search type and count for filename
    if 'search_terms' in search_info and 'locations' in search_info:
        term_count = len(search_info['search_terms'])
        location_count = len(search_info['locations'])
        download_name = f"business_search_{term_count}terms_{location_count}locations_{timestamp}.csv"
    elif 'search_terms' in search_info:
        term_count = len(search_info['search_terms'])
        download_name = f"business_search_{term_count}terms_{timestamp}.csv"
    else:
        # Legacy single search
        download_name = f"business_search_{timestamp}.csv"
    
    return send_file(csv_path, as_attachment=True, download_name=download_name)

@app.route('/cancel/<search_id>', methods=['POST'])
def cancel_search(search_id):
    """Cancel a running search"""
    if search_id not in running_searches:
        return jsonify({'success': False, 'error': 'Search not found'}), 404
    
    search_info = running_searches[search_id]
    if search_info['status'] != 'running':
        return jsonify({'success': False, 'error': 'Search is not running'}), 400
    
    try:
        # Mark search as cancelled
        running_searches[search_id]['status'] = 'cancelled'
        running_searches[search_id]['completed_at'] = datetime.now(pytz.timezone('Asia/Jerusalem')).isoformat()
        running_searches[search_id]['error'] = 'Search cancelled by user'
        
        # Try to kill any running subprocess
        # Note: This is a basic implementation - for production you'd want to track process IDs
        import signal
        import psutil
        
        # Kill any business_search_complete.py processes
        for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
            try:
                if proc.info['cmdline'] and any('business_search_complete.py' in str(cmd) for cmd in proc.info['cmdline']):
                    proc.terminate()
                    proc.wait(timeout=3)
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.TimeoutExpired):
                pass
        
        return jsonify({'success': True, 'message': 'Search cancelled successfully'})
        
    except Exception as e:
        return jsonify({'success': False, 'error': f'Failed to cancel search: {str(e)}'}), 500

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
