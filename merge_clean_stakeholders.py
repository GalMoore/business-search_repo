import os
import csv
import re

# Input and output folders
input_folder = "breast_cancer_stakeholders"
output_folder = "breast_cancer_stakeholders_final"
os.makedirs(output_folder, exist_ok=True)

# Output file path
output_file = os.path.join(output_folder, "merged_cleaned_stakeholders.csv")

# Email validation regex
email_regex = re.compile(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")

# Track unique emails
seen_emails = set()
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

                if not email or not email_regex.match(email):
                    continue  # Skip rows with invalid or missing email

                email_lower = email.lower()
                if email_lower in seen_emails:
                    continue  # Skip duplicate emails

                seen_emails.add(email_lower)
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

print(f"✅ Cleaned CSV with source info saved to: {output_file} ({len(cleaned_rows)} unique emails)")
