CREATE TABLE loans_ext_file ( received_date Date,  product Nullable(String),  sub_product Nullable(String),  issue Nullable(String),  sub_issue Nullable(String),  cli_narrative Nullable(String),  company_response Nullable(String),  company Nullable(String),  State Nullable(FixedString(2)),  zip_code Nullable(String),  tags Nullable(String),  consent_provided Nullable(String),  submitted_via Nullable(String),  sent_to_company_date Nullable(Date),  end_company_response Nullable(String),  is_timely Nullable(String),  is_disputed Nullable(String),  complaint_id Nullable(UInt64)) ENGINE = File('CSVWithNames')

CREATE TABLE loans_ext_hdfs ( received_date Date,  product Nullable(String),  sub_product Nullable(String),  issue Nullable(String),  sub_issue Nullable(String),  cli_narrative Nullable(String),  company_response Nullable(String),  company Nullable(String),  State Nullable(FixedString(2)),  zip_code Nullable(String),  tags Nullable(String),  consent_provided Nullable(String),  submitted_via Nullable(String),  sent_to_company_date Nullable(Date),  end_company_response Nullable(String),  is_timely Nullable(String),  is_disputed Nullable(String),  complaint_id Nullable(UInt64)) ENGINE = HDFS('CSVWithNames','127.0.0.1','/test.csv')