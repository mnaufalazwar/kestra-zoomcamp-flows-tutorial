import os, json, base64
import pandas as pd
import dlt
import hashlib

# to handle different column names format
ALIASES = {
  "dispatching_base_num": ["dispatching_base_num"],
  "pickup_datetime": ["pickup_datetime"],
  "dropoff_datetime": ["dropoff_datetime", "dropOff_datetime"],
  "pu_location_id": ["PUlocationID", "PULocationID", "pulocationid"],
  "do_location_id": ["DOlocationID", "DOLocationID", "dolocationid"],
  "sr_flag": ["SR_Flag", "sr_flag"],
  "affiliated_base_number": ["Affiliated_base_number", "affiliated_base_number"]
}

def _standardized_col_names(df:pd.DataFrame) -> pd.DataFrame:
  rename_map = {}
  lower_to_actual_names = {col.lower() : col for col in df.columns}
  for canon, candidates in ALIASES.items():
    for candidate in candidates:
      actual = lower_to_actual_names.get(candidate.lower())
      if actual:
        rename_map[actual] = canon
        break
  return df.rename(columns=rename_map)

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
        chunk = _standardized_col_names(chunk)
        yield from chunk.to_dict("records")
  return _fhv_from_gcs

def _adding_unique_row_id_and_filename(chunk: pd.DataFrame):
  dispatching_base_num = chunk.get("dispatching_base_num").astype("string").fillna("")
  pickup_datetime = chunk.get("pickup_datetime").astype("string").fillna("")
  dropoff_datetime = chunk.get("dropoff_datetime").astype("string").fillna("")
  pu_location_id = chunk.get("pu_location_id").astype("string").fillna("")
  do_location_id = chunk.get("do_location_id").astype("string").fillna("")
  temp_id = dispatching_base_num + pickup_datetime + dropoff_datetime + pu_location_id + do_location_id
  chunk["unique_row_id"] = temp_id.apply(
    lambda x: hashlib.md5(x.encode("utf-8")).hexdigest()
  )

  chunk["file_name"] = os.getenv("FILE_NAME")

  return chunk

def _get_dlt_resource_main(table_name: str, csv_uri: str, creds_path: str):
  @dlt.resource(name=table_name, primary_key="unique_row_id", write_disposition="merge")
  def _fhv_from_gcs():
      for chunk in pd.read_csv(
          csv_uri, 
          chunksize=100_000, 
          storage_options={"token": creds_path}
      ):
        chunk = _standardized_col_names(chunk)
        chunk = _adding_unique_row_id_and_filename(chunk)
        yield from chunk.to_dict("records")
  return _fhv_from_gcs

def main():
    _adc_from_env_to_file()
    dataset = os.getenv("BQ_DATASET")
    csv_uri = os.getenv("CSV_URI")
    creds_path = os.environ["GOOGLE_APPLICATION_CREDENTIALS"]

    pipeline_stg = dlt.pipeline(
        pipeline_name="fhv_to_bq_stg",
        destination="bigquery",
        dataset_name=dataset,
        dev_mode=False,
    )
    stg_table_name = os.getenv("BQ_STG_TABLE")
    resource_stg = _get_dlt_resource(stg_table_name, csv_uri, creds_path)
    info_stg = pipeline_stg.run(resource_stg)
    print(info_stg)

    pipeline_main = dlt.pipeline(
        pipeline_name="fhv_to_bq_main",
        destination="bigquery",
        dataset_name=dataset,
        dev_mode=False,
    )
    main_table_name = os.getenv("BQ_MAIN_TABLE")
    resource_main = _get_dlt_resource_main(main_table_name, csv_uri, creds_path)
    info_main = pipeline_main.run(resource_main)
    print(info_main)

if __name__ == "__main__":
    main()