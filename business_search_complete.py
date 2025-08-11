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
    match = re.search(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}", text)
    return match.group(0) if match else None

def extract_phone(text):
    """Extract first phone number found in a string"""
    if not text:
        return None
    # Match various phone formats including international
    patterns = [
        r"\+972[\s-]?\d[\s-]?\d{3}[\s-]?\d{4}",  # Israeli format +972-X-XXX-XXXX
        r"0\d[\s-]?\d{3}[\s-]?\d{4}",  # Israeli local format 0X-XXX-XXXX
        r"\(\d{3}\)[\s-]?\d{3}[\s-]?\d{4}",  # (XXX) XXX-XXXX
        r"\d{3}[\s-]?\d{3}[\s-]?\d{4}",  # XXX-XXX-XXXX
        r"\+\d{1,3}[\s-]?\d{1,4}[\s-]?\d{3,4}[\s-]?\d{4}"  # International format
    ]
    
    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            return match.group(0)
    return None

def sanitize_filename(term):
    """Clean a filename from a search term"""
    return re.sub(r'[^\w\s-]', '', term).replace(' ', '_').lower()

def search_businesses(search_term, output_folder):
    """Search for businesses and save results to CSV"""
    print(f"\n🔍 Running search for: {search_term}")
    
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

    for i in range(10):  # Adjust the number of search iterations if needed
        print(f"  ▶ Run {i + 1}/10")

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
                writer.writerow(["URL", "Email", "Phone"])  # Header

            for result in search_response.get("results", []):
                url = result.get("url")
                raw_content = result.get("raw_content")
                email = extract_email(raw_content)
                phone = extract_phone(raw_content)

                if url:
                    parsed = urlparse(url)
                    if parsed.netloc:
                        seen_domains.add(parsed.netloc)

                writer.writerow([url, email if email else "No email found", phone if phone else "No phone found"])
                print(f"    ✔ {url}, {email if email else 'No email found'}, {phone if phone else 'No phone found'}")

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
    
    # Phone validation regex (basic check for digits and common separators)
    phone_regex = re.compile(r"^[\+]?[\d\s\-\(\)]{7,}$")
    
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
                    phone = row.get("Phone", "").strip()
                    url = row.get("URL", "").strip()

                    # Clean up "No X found" entries
                    if email == "No email found":
                        email = ""
                    if phone == "No phone found":
                        phone = ""

                    # Validate email and phone
                    valid_email = email and email_regex.match(email)
                    valid_phone = phone and phone_regex.match(phone.replace(" ", "").replace("-", ""))

                    # Skip if neither email nor phone is valid
                    if not valid_email and not valid_phone:
                        continue

                    # Use email as primary key, URL as fallback for deduplication
                    contact_key = email.lower() if valid_email else url.lower()
                    if contact_key in seen_contacts:
                        continue  # Skip duplicate contacts

                    seen_contacts.add(contact_key)
                    cleaned_rows.append({
                        "URL": url,
                        "Email": email,
                        "Phone": phone if phone != "No phone found" else "",
                        "SourceFile": file_name  # Add source file name
                    })

    # Write the final merged and cleaned CSV
    with open(output_file, mode="w", newline="", encoding="utf-8") as outfile:
        writer = csv.DictWriter(outfile, fieldnames=["URL", "Email", "Phone", "SourceFile"])
        writer.writeheader()
        writer.writerows(cleaned_rows)

    print(f"✅ Cleaned CSV saved to: {output_file} ({len(cleaned_rows)} unique emails)")
    return output_file

def main():
    """Main function to run the complete business search and cleaning workflow"""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Search for business contact information and clean results')
    parser.add_argument('search_term', help='The search term to look for (e.g., "restaurants New York City", "law firms Boston")')
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
    search_businesses(args.search_term, search_results_folder)
    
    # Step 2: Merge and clean results (unless skipped)
    if not args.skip_merge:
        final_file = merge_and_clean_results(search_results_folder, final_results_folder)
        print(f"\n🎉 Complete workflow finished! Final results in: {final_file}")
    else:
        print(f"\n✅ Search completed. Raw results in: {search_results_folder}")

if __name__ == "__main__":
    main()
