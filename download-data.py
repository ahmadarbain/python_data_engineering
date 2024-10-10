import requests

# URL of the raw CSV file
url = "scooter.csv"

# Send a GET request to the URL
response = requests.get(url)

# Check if the request was successful
if response.status_code == 200:
    # Write the content to a file
    with open("scooter.csv", "wb") as file:
        file.write(response.content)
    print("File downloaded successfully.")
else:
    print(f"Failed to download file: {response.status_code}")
