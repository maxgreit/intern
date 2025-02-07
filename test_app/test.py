from modules.env_tool import env_check
from woocommerce import API
import os

# Check uitvoering: lokaal of productie
env_check()

# Woocommerce variabelen
consumer_secret = os.getenv('WOOCOMMERCE_CONSUMER_SECRET')
consumer_key = os.getenv('WOOCOMMERCE_CONSUMER_KEY')
woocommerce_url = os.getenv('WOOCOMMERCE_URL')
secret_key = os.getenv('SECRET_KEY')


# Initialize WooCommerce API
wcapi = API(
    url=woocommerce_url,
    consumer_key=consumer_key,
    consumer_secret=consumer_secret,
    version="wc/v3"
)

email = 'mariarood@me.com'

response = wcapi.get("subscriptions", params={"search": email})

print(response)
print(response.json())