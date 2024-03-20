import time
from datetime import datetime

from faker import Faker
import json
import random

fake = Faker()

def generate_fake_client(count):
    include_last_name = random.choice([True, False])
    include_email = random.choice([True, False])

    client = {
        "count": count,
        "client_id": fake.uuid4(),
        "client_name": fake.company(),
        "contact_person": {
            "first_name": fake.first_name(),
            "last_name": fake.last_name() if include_last_name else None,
        },
        "email": fake.email() if include_email else None,
        "phone": fake.phone_number(),
        "address": {
            "street": fake.street_address(),
            "city": fake.city(),
            "state": fake.state_abbr(),
            "postal_code": fake.zipcode()
        }
    }
    return client

if __name__ == "__main__":
    curr_time = datetime.now()
    count = 0

    while (datetime.now() - curr_time).seconds < 400:
        generated_data = generate_fake_client(count)
        json_data = json.dumps(generated_data)
        count += 1

        time.sleep(1)

        with open("fake_client_data.json", "a") as json_file:
            json_file.write(json_data + ',\n')

    print("Fake client data has been generated and saved to fake_client_data.json.")
