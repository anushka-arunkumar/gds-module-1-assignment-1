# set variables using power shell
# $env:PROJECT_ID="your-project-id"
# $env:DATASET="your-dataset"
# $env:TABLE="your-table"


from flask import Flask, request
from datetime import datetime, timezone
from google.cloud import bigquery
import re
import os
import logging

app = Flask(__name__)
bq_client = bigquery.Client()

@app.route("/", methods = ["POST"])
def receive_data():

    data = request.get_json(silent = True)
    if not data:
        return {"error": "invalid request"}, 400
    
    validation_result = validate(data)
    if validation_result["is_valid"]:
        transformed_data = transform(data)
        return load_to_bq(transformed_data)
    return {"error": validation_result["error"]}, 400

def validate(data):
    
    # schema validation
    required_fields = ["transaction_id", "quantity", "price_per_unit"]
    for field in required_fields:
        if field not in data:
            return {"error": f"required field {field} not found", "is_valid": False}
        
    transaction_id = data["transaction_id"]
    quantity = data["quantity"]
    price_per_unit = data["price_per_unit"]
        
    # type checking
    if not isinstance(transaction_id, str):
        return {"error": "transaction_id must be of type str", "is_valid": False}
    
    if not isinstance(quantity, int):
        return {"error": "quantity must be of type int", "is_valid": False}
    
    if not isinstance(price_per_unit, (int, float)):
        return {"error": "price_per_unit must be of type int/float", "is_valid": False}
    
    # sanity checking
    if not re.match("^[Tt][0-9]+$", transaction_id):
        return {"error": "transaction_id must be in the correct format", "is_valid": False}
    
    if quantity <= 0:
        return {"error": "quantity must be greater than 0", "is_valid": False}
    
    if price_per_unit <= 0:
        return {"error": "price_per_unit must be greater than 0", "is_valid": False}
    
    return {"is_valid": True}

def transform(data):

    total_price = data["quantity"] * data["price_per_unit"]
    tax = round(total_price * 0.18, 2)
    final_price = round(total_price + tax, 2)

    timestamp = datetime.now(tz = timezone.utc).isoformat()

    transformed_data = {
        "transaction_id": data["transaction_id"],
        "quantity": data["quantity"],
        "price_per_unit": data["price_per_unit"],
        "total_price": total_price,
        "tax": tax,
        "final_price": final_price,
        "processed_at": timestamp
    }
    logging.info(f"Processed transaction: {transformed_data}")
    return transformed_data

def load_to_bq(transformed_data):

    PROJECT_ID = os.getenv("PROJECT_ID")
    DATASET = os.getenv("DATASET")
    TABLE = os.getenv("TABLE")
    table_id = f"{PROJECT_ID}.{DATASET}.{TABLE}"
    rows_to_insert = [transformed_data]
    errors = bq_client.insert_rows_json(table_id, rows_to_insert)

    if errors:
        return {"error": errors}, 500
    return {"message": "Data inserted successfully", "data": transformed_data}, 200


if __name__ == "__main__":
    app.run(host = "0.0.0.0", port = 8080)
