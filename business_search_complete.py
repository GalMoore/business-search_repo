from tavily import TavilyClient
import os
import re
import csv
from urllib.parse import urlparse
import time
import argparse
from datetime import datetime
import pytz

def extract_email(text):
    """Extract first email found in a string"""
    if not text:
        return None
    
    # Find all potential email matches
    pattern = r'\b[a-zA-Z0-9]([a-zA-Z0-9._%-]*[a-zA-Z0-9])?@[a-zA-Z0-9]([a-zA-Z0-9.-]*[a-zA-Z0-9])?\.[a-zA-Z]{2,}\b'
    matches = re.findall(pattern, text)
    
    if not matches:
        return None
    
    # Reconstruct full matches and validate them
    full_matches = re.finditer(pattern, text)
    for match in full_matches:
        email = match.group(0)
        
        # Filter out common false positives
        if (
            # Skip image files and other file extensions
            email.lower().endswith(('.jpg', '.jpeg', '.png', '.gif', '.svg', '.webp', '.bmp')) or
            # Skip if it contains file-like patterns
            '@2x.' in email.lower() or
            '@3x.' in email.lower() or
            # Skip if domain part looks like a file extension
            email.split('@')[1].split('.')[0].isdigit() or
            # Skip very short domains (less than 2 chars before TLD)
            len(email.split('@')[1].split('.')[0]) < 2
        ):
            continue
            
        # Return the first valid email found
        return email
    
    return None



def sanitize_filename(term):
    """Clean a filename from a search term"""
    return re.sub(r'[^\w\s-]', '', term).replace(' ', '_').lower()

def search_businesses(search_term, output_folder, iterations=10):
    """Search for businesses and save results to CSV"""
    print(f"\n🔍 Running search for: {search_term} ({iterations} iterations)")
    
    filename = os.path.join(output_folder, f"{sanitize_filename(search_term)}.csv")
    seen_domains = set()

    # Load existing URLs to avoid duplicates
    if os.path.exists(filename):
        with open(filename, mode="r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                url = row.get("URL")
                if url:
                    parsed = urlparse(url)
                    if parsed.netloc:
                        seen_domains.add(parsed.netloc)

    for i in range(iterations):
        print(f"  ▶ Run {i + 1}/{iterations}", flush=True)

        # Perform the Tavily search
        search_response = tavily.search(
            search_term,
            max_results=20,
            include_raw_content=True,
            exclude_domains=list(seen_domains)
        )

        # Write results to CSV
        with open(filename, mode="a", newline="", encoding="utf-8") as csv_file:
            writer = csv.writer(csv_file)
            if csv_file.tell() == 0:
                writer.writerow(["URL", "Email"])  # Header

            for result in search_response.get("results", []):
                url = result.get("url")
                raw_content = result.get("raw_content")
                email = extract_email(raw_content)

                if url:
                    parsed = urlparse(url)
                    if parsed.netloc:
                        seen_domains.add(parsed.netloc)

                writer.writerow([url, email if email else "No email found"])
                print(f"    ✔ {url}, {email if email else 'No email found'}")

        # Optional delay to avoid rate limiting
        # time.sleep(2)

def merge_and_clean_results(input_folder, output_folder):
    """Merge and clean all CSV results into a single file"""
    print(f"\n🧹 Merging and cleaning results from {input_folder}")
    
    # Create output folder
    os.makedirs(output_folder, exist_ok=True)
    
    # Output file path
    output_file = os.path.join(output_folder, "merged_cleaned_results.csv")
    
    # Email validation regex
    email_regex = re.compile(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")
    
    # Track unique contacts (by email or URL if no email)
    seen_contacts = set()
    cleaned_rows = []
    
    # Process each CSV file in the input folder
    for file_name in os.listdir(input_folder):
        if file_name.endswith(".csv"):
            file_path = os.path.join(input_folder, file_name)
            with open(file_path, mode="r", encoding="utf-8") as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    email = row.get("Email", "").strip()
                    url = row.get("URL", "").strip()

                    # Clean up "No email found" entries
                    if email == "No email found":
                        email = ""

                    # Validate email
                    valid_email = email and email_regex.match(email)

                    # Skip if no valid email
                    if not valid_email:
                        continue

                    # Use email as primary key for deduplication
                    contact_key = email.lower()
                    if contact_key in seen_contacts:
                        continue  # Skip duplicate contacts

                    seen_contacts.add(contact_key)
                    cleaned_rows.append({
                        "URL": url,
                        "Email": email,
                        "SourceFile": file_name  # Add source file name
                    })

    # Write the final merged and cleaned CSV
    with open(output_file, mode="w", newline="", encoding="utf-8") as outfile:
        writer = csv.DictWriter(outfile, fieldnames=["URL", "Email", "SourceFile"])
        writer.writeheader()
        writer.writerows(cleaned_rows)

    print(f"✅ Cleaned CSV saved to: {output_file} ({len(cleaned_rows)} unique emails)")
    return output_file

def main():
    """Main function to run the complete business search and cleaning workflow"""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Search for business contact information and clean results')
    parser.add_argument('search_term', help='The search term to look for (e.g., "restaurants New York City", "law firms Boston")')
    parser.add_argument('--iterations', type=int, default=10, help='Number of search iterations (default: 10)')
    parser.add_argument('--skip-merge', action='store_true', help='Skip the merge and clean step')
    args = parser.parse_args()

    # Init Tavily
    api_key = os.getenv("TAVILY_API_KEY")
    if not api_key:
        print("❌ Error: TAVILY_API_KEY environment variable not set")
        return
    
    global tavily
    tavily = TavilyClient(api_key)

    # Setup unique folders based on search term and timestamp
    israel_tz = pytz.timezone('Asia/Jerusalem')
    timestamp = datetime.now(israel_tz).strftime('%Y%m%d_%H%M%S')
    sanitized_term = sanitize_filename(args.search_term)
    
    # Create nested folder structure
    parent_folder = "business_searches"
    run_folder = os.path.join(parent_folder, f"{sanitized_term}_{timestamp}")
    os.makedirs(run_folder, exist_ok=True)
    
    search_results_folder = os.path.join(run_folder, "search")
    final_results_folder = os.path.join(run_folder, "final")
    os.makedirs(search_results_folder, exist_ok=True)

    # Step 1: Search for businesses
    search_businesses(args.search_term, search_results_folder, args.iterations)
    
    # Step 2: Merge and clean results (unless skipped)
    if not args.skip_merge:
        final_file = merge_and_clean_results(search_results_folder, final_results_folder)
        print(f"\n🎉 Complete workflow finished! Final results in: {final_file}")
    else:
        print(f"\n✅ Search completed. Raw results in: {search_results_folder}")

if __name__ == "__main__":
    main()
