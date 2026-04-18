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
import time

app = Flask(__name__)
bq_client = bigquery.Client()

@app.route("/", methods = ["POST"])
def receive_data():

    try:
        PROJECT_ID = os.getenv("PROJECT_ID")
        DATASET = os.getenv("DATASET")
        TABLE = os.getenv("TABLE")
        if not all([PROJECT_ID, DATASET, TABLE]):
            return {"error": "Server misconfiguration"}, 500
        table_ref = f"{PROJECT_ID}.{DATASET}.{TABLE}"

        data = request.get_json(silent = True)
        if not data:
            return {"error": "invalid request"}, 400
        
        validation_result = validate(data)
        if validation_result["is_valid"]:

            if check_if_exists(data["transaction_id"], table_ref):
                return {"message": "Duplicate ignored"}, 200

            transformed_data = transform(data) 

            return load(transformed_data, table_ref)
        
        return {"error": validation_result["error"]}, 400
    except Exception as e:
        logging.exception("unexpected error occurred")
        return {"error": "internal server error"}, 500
    
def check_if_exists(transaction_id, table_ref):

    query = f"""
        SELECT 1
        FROM `{table_ref}`
        WHERE transaction_id = @transaction_id
        LIMIT 1
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters = [
            bigquery.ScalarQueryParameter("transaction_id", "STRING", transaction_id)
        ]
    )

    query_job = bq_client.query(query, job_config)
    results = query_job.result()

    return any(results)
    
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
    logging.info(f"Processed transaction {data['transaction_id']}")
    return transformed_data

def load(transformed_data, table_ref):

    rows_to_insert = [transformed_data]
    total_attempts = 3

    try:
        for attempt in range(total_attempts):
            errors = load_to_bq(table_ref, rows_to_insert)
        
            if not errors:
                logging.info(f"Success on attempt {attempt+1}")
                return {"message": "Data inserted successfully", "data": transformed_data}, 200

            logging.warning(f"Attempt {attempt+1} failed: {errors}")
            
            if attempt < total_attempts - 1:
                time.sleep(2)

        raise Exception("All attempts failed")
    
    except Exception:
        logging.exception(f"BigQuery insert failed after {total_attempts} attempts")
        return {"error": "BigQuery insert failed"}, 500
  
def load_to_bq(table_id, rows_to_insert):

    return bq_client.insert_rows_json(table_id, rows_to_insert)