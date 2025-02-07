from flask import Flask, request, jsonify
from modules.env_tool import env_check
from woocommerce import API
from flask_cors import CORS
import os

app = Flask(__name__)
CORS(app, resources={r"/abonnementen/*": {"origins": "*"}})

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
    return jsonify({"email": email})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000) 