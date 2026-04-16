# HTTP-Triggered ETL Microservice with CI/CD

Build a Cloud Run service that exposes an HTTP endpoint to accept JSON
payloads representing sales transactions.

## The service should:

-   Validate the incoming JSON against a predefined schema.
-   Transform it (e.g., calculate tax, enrich with timestamp).
-   Load the cleaned records into a BigQuery table.

## Requirements:

-   Containerize the service (any language) and deploy to Cloud Run.
-   Write unit tests for schema validation and transformation logic.
-   Configure a Cloud Build pipeline:
    -   Trigger on pushes to the main branch in your Git repo.
    -   Build the container, run tests, and deploy to Cloud Run (with
        traffic splitting for staging vs. prod).

------------------------------------------------------------------------

# Cloud Storage-Triggered File Processor

Create a Cloud Run function that processes CSV files dropped into a
`raw-data/` folder in a GCS bucket.

## For each new file:

-   Read the CSV using Pandas or similar.
-   Compute basic metrics (row count, null counts per column).
-   Write the metrics as a JSON file into a `reports/` folder in the
    same bucket.

## Requirements:

-   Use a Pub/Sub notification on the bucket so that every new file
    triggers your Cloud Run service.
-   Handle only `.csv` files; non-CSV should return HTTP 400.
-   Include retry logic and idempotency (e.g., track processed filenames
    in Firestore).
-   Set up a simple Cloud Build pipeline to:
    -   Lint and test your code.
    -   Deploy to Cloud Run on merges to develop.

------------------------------------------------------------------------

# Scheduled Batch Job Orchestration

Implement a daily batch job in Cloud Run that:

## Tasks:

-   Fetch all JSON files older than 24 hours from a GCS bucket prefix
    (e.g., `events/YYYY/MM/DD/`).
-   Aggregate them into hourly summary CSVs (e.g., total events per
    type).
-   Upload the summaries to another bucket or BigQuery.

## Requirements:

-   Define this as a Cloud Run Job (not a service).
-   Schedule it with Cloud Scheduler via an HTTP trigger or Pub/Sub.
-   Use a Serverless VPC connector if you need to reach private
    resources.
-   CI/CD - Cloud Build trigger on tags (e.g., `v*`), building and
    deploying the Job spec.

------------------------------------------------------------------------

# Submission Requirements

1.  Zip file of code base\
2.  Detailed document explaining the steps followed
