import os, json, base64
import pandas as pd
import dlt

def _adc_from_env_to_file() -> str:
    """
    read gcp sa json (raw or base 64) from env GCP_SA_JSON,
    write to /tmp/gcp.json
    return the path
    """
    raw = os.getenv("SECRET_GCP_CREDS")
    if not raw:
        raise RuntimeError("Missing SECRET_GCP_CREDS")
    try:
        text = raw if raw.lstrip().startswith("{") else base64.b64decode(raw).decode("utf-8")
        json.loads(text) #validate
    except Exception as e:
        raise RuntimeError(f"GCP_SA_JSON must be raw or base64 JSON: {e}")
    path = "/tmp/gcp.json"
    with open(path, "w") as f:
        f.write(text)
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = path
    return path

def _get_dlt_resource(table_name: str, csv_uri: str, creds_path: str):
  @dlt.resource(name=table_name, write_disposition="replace")
  def _fhv_from_gcs():
      for chunk in pd.read_csv(
          csv_uri, 
          chunksize=100_000, 
          storage_options={"token": creds_path}
      ):
          yield from chunk.to_dict("records")
  return _fhv_from_gcs

def main():
    _adc_from_env_to_file()
    dataset = os.getenv("BQ_DATASET", "taxi_rides")

    pipeline = dlt.pipeline(
        pipeline_name="fhv_to_bq",
        destination="bigquery",
        dataset_name=dataset,
        dev_mode=False,
    )

    stg_table_name = os.getenv("BQ_STG_TABLE")
    csv_uri = os.getenv("CSV_URI")
    creds_path = os.environ["GOOGLE_APPLICATION_CREDENTIALS"]

    resource = _get_dlt_resource(stg_table_name, csv_uri, creds_path)

    info = pipeline.run(resource)
    print(info)

if __name__ == "__main__":
    main()