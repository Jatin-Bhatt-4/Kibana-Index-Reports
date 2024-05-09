#Libraries
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

# Filter indices by year 2024
def filter_indices(indices_data):
    filtered_indices = []
    for line in indices_data.splitlines():
        if "-2024." in line:
            filtered_indices.append(line.replace("-2024.", " 2024."))
    return filtered_indices

# Main function to process data and write to CSV file
def main():
    indices_data = fetch_indices_data()
    size_data = fetch_indices_size_data()
    indices = filter_indices(indices_data)

    # Reverse date order (last date first)
    dates = [(datetime.now() - timedelta(days=i)).strftime("%Y.%m.%d") for i in range(8)][::-1]

    # Sort index names alphabetically
    sorted_indices = sorted(indices)

    data_dict = {}
    for index in sorted_indices:
        index_name = index.split()[0]
        data_dict[index_name] = {}

        for date in dates:
            size = next((line.split()[1] for line in size_data.splitlines() if index_name in line and date in line), "0")
            data_dict[index_name][date] = size

    with open("reports.csv", "w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(["Index Names"] + dates)
        for index_name in data_dict.keys():
            row = [index_name] + [data_dict[index_name].get(date, "0") for date in dates]
            writer.writerow(row)

if __name__ == "__main__":
    main()
