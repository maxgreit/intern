from flask import Flask, request, jsonify
from modules.env_tool import env_check
from woocommerce import API
import os

app = Flask(__name__)

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

@app.route('/abonnementen/<email>', methods=['GET'])
def get_subscriptions(email):
    try:
        # Haal alle informatie op
        response = wcapi.get("subscriptions", params={"search": email})
        
        if response.status_code != 200:
            return jsonify({"error": "Geen informatie kunnen ophalen", "status": response.status_code}), response.status_code
        
        subscriptions = response.json()
        
        return jsonify({"email": email, "abonnement": subscriptions})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8443)