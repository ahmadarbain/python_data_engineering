from faker import Faker
from elasticsearch import Elasticsearch, helpers

# Connect to the Elasticsearch instance running in Docker
es = Elasticsearch(
    ["http://localhost:9200"],
    request_timeout=30,  # Use request_timeout instead of timeout
    max_retries=10,
    retry_on_timeout=True
)

fake = Faker()

# Prepare the bulk data
actions = [
    {
        "_index": "users",
        "_source": {
            "name": fake.name(),
            "street": fake.street_address(),
            "city": fake.city(),
            "zip": fake.zipcode()
        }
    }
    for x in range(998)
]

# Insert the bulk data into Elasticsearch
success, failed = helpers.bulk(es, actions)

# Print the number of successful inserts
print(f"Successfully inserted {success} documents.")

# If there are failed actions, process them
if failed:
    print(f"Failed to insert {len(failed)} documents.")
    for item in failed:
        print(item)  # This will show the details of failed inserts
