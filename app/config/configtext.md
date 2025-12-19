xuatsach
- rule_kn_folder
- rule_rd_folder
- lookup_file
- raw_HUB_folder
- kn_output_folder
- rd_output_folder

- rule_ttkt_folder
- raw_TTKT_folder
- output_folder_ttkt
- fast_mode

- input_type
- pipeline_select
- rule_rd
- rule_kn
- rule_ttkt


Continue with the ingest_raw function with raw, format and raw_folder as inputs, returning ingested_df, number of rows ingested and extracted report_date

format is a str that accepts: zip, xlsx, csv.

if format = zip, then raw will be the zip file name (full path = raw_folder + raw). extract all files (which will be either csv or xlsx). extract date from filename as the report_date, then read all files accordingly to a df

if format = csv or xlsx, it would be a list of streamlit uploaded files

- if format = csv, use pl.read_csv
- if format = xlsx, use function load_with_threads(files)