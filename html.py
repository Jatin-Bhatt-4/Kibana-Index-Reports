#This program generates 2 files - reports.csv & report.html such that - In the modified code, rows in the HTML table will be color-coded based on the following conditions:
#Red Background:
#Rows where the size difference between yesterday and the day before yesterday is more than 50% (indicating significant growth).
#For these rows, the background color will be set to red.
#Yellow Background:
#Rows where yesterday's size is "None" (indicating missing data for yesterday).
#For these rows, the background color will be set to yellow.

# Libraries
import requests
from datetime import datetime, timedelta
import csv

# Define Elasticsearch base URL
base_url = "http://logs.kibana.internal:9200"

# Function to fetch indices data
def fetch_indices_data():
    indices_response = requests.get(f"{base_url}/_cat/indices?v&h=index")
    indices_data = indices_response.text
    return indices_data

# Function to fetch indices size data
def fetch_indices_size_data():
    size_response = requests.get(f"{base_url}/_cat/indices?v&h=index,pri.store.size&bytes=kb")
    size_data = size_response.text
    return size_data

# Filter indices by year 2024 and exclude "preprod" or "alb" indices
def filter_indices(indices_data):
    filtered_indices = []
    for line in indices_data.splitlines():
        if "-2024." in line and "preprod" not in line and "alb" not in line:
            filtered_indices.append(line.replace("-2024.", " 2024."))
    return filtered_indices

# Main function to process data and write to CSV and HTML files
def main():
    indices_data = fetch_indices_data()
    size_data = fetch_indices_size_data()
    indices = filter_indices(indices_data)

    # Reverse date order (last date first)
    dates = [(datetime.now() - timedelta(days=i)).strftime("%Y.%m.%d") for i in range(10)][::-1]

    # Sort index names alphabetically
    sorted_indices = sorted(indices)

    data_dict = {}
    for index in sorted_indices:
        index_name = index.split()[0]
        data_dict[index_name] = {}

        for date in dates:
            size = next((line.split()[1] for line in size_data.splitlines() if index_name in line and date in line), "0")
            data_dict[index_name][date] = size

    # Remove rows with all 0 size values for the last 10 days
    filtered_data_dict = {index: sizes for index, sizes in data_dict.items() if not all(size == "0" for size in sizes.values())}

    # Generate the file name with the current date
    current_date = datetime.now().strftime("%Y%m%d")
    csv_file_name = f"reports_{current_date}.csv"
    html_file_name = f"report_{current_date}.html"

    with open(csv_file_name, "w", newline="") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(["Index Names"] + dates)
        for index_name, sizes in filtered_data_dict.items():
            row = [index_name] + [sizes.get(date, "0") for date in dates]
            writer.writerow(row)

    with open(html_file_name, "w") as html_file:
        html_file.write("<!DOCTYPE html>\n<html>\n<head>\n<title>Indices Report</title>\n</head>\n<body>\n<table border='1'>\n<tr><th>Index Names</th>")
        for date in dates:
            html_file.write(f"<th>{date}</th>")
        html_file.write("</tr>\n")

        for index_name, sizes in filtered_data_dict.items():
            row = [index_name] + [sizes.get(date, "0") for date in dates]
            size_diff_yesterday = float(row[-1]) - float(row[-2])
            size_diff_percentage = (size_diff_yesterday / float(row[-2])) * 100 if float(row[-2]) != 0 else 0

            if size_diff_percentage > 50:
                html_file.write(f"<tr bgcolor='red'><td>{index_name}</td>")
            elif row[-2] == "0":
                html_file.write(f"<tr bgcolor='yellow'><td>{index_name}</td>")
            else:
                html_file.write(f"<tr><td>{index_name}</td>")

            for size in row[1:]:
                html_file.write(f"<td>{size}</td>")
            html_file.write("</tr>\n")

        html_file.write("</table>\n</body>\n</html>")

if __name__ == "__main__":
    main()
