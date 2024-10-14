import pandas as pd
from scripts.postgres_helper import (
    create_schema_if_not_exists,
    read_query,
    read_table,
    upload_overwrite_table,
)


def create_postgres_schema(**kwargs):
    schema_name = kwargs.get("schema_name")
    assert isinstance(schema_name, str), "No schema name was provided!"

    create_schema_if_not_exists(schema_name)


def upload_csv_to_postgres(**kwargs):
    file_name = kwargs.get("file_name")
    schema_name = kwargs.get("schema_name")
    assert isinstance(file_name, str), "No file name was provided!"
    assert isinstance(schema_name, str), "No schema name was provided!"

    table_name = file_name.split(".")[0]

    raw_df = pd.read_csv(
        f"dags/scripts/data_examples/{file_name}",
        escapechar="\\",
    )

    upload_overwrite_table(df=raw_df, table_name=table_name, schema_name=schema_name)


def transform_postgres_data(**kwargs):
    file_name = kwargs.get("file_name")
    schema_name = kwargs.get("schema_name")
    assert isinstance(file_name, str), "No file name was provided!"
    assert isinstance(schema_name, str), "No schema name was provided!"

    table_name = file_name.split(".")[0]
    raw_df = read_table(table_name, "public")

    year_first_cols = [
        "date_created",
        "date_updated",
        "mcs_150_form_date",
        "carrier_safety_rating_rating_date",
        "carrier_safety_rating_review_date",
    ]
    month_first_cols = ["review_datetime_utc", "owner_answer_timestamp_datetime_utc"]
    bool_cols = ["hhg_authorization", "area_service", "verified"]

    for col in raw_df.columns:
        if col in year_first_cols:
            raw_df[col] = pd.to_datetime(raw_df[col], yearfirst=True)
        elif col in month_first_cols:
            raw_df[col] = raw_df[col].apply(convert_monthfirst_to_datetime)
        elif col in bool_cols:
            raw_df[col] = raw_df[col].apply(convert_bool_as_string)
        elif col == "state":
            raw_df[col] = raw_df[col].apply(convert_state_abbreviation)
        else:
            raw_df[col] = raw_df[col].apply(validate_data_field)

    df = raw_df.dropna(axis=1, how="all").copy()

    upload_overwrite_table(df=df, table_name=table_name, schema_name=schema_name)


def create_transformed_postgres_table(**kwargs):
    table_name = kwargs.get("table_name")
    schema_name = kwargs.get("schema_name")
    assert isinstance(table_name, str), "No table name was provided!"
    assert isinstance(schema_name, str), "No schema name was provided!"

    if table_name == "transformed_fmcsa_companies":
        query = """
        select
            usdot_num as company_id,
            fmcsa_companies.company_name,
            fmcsa_companies.city as company_city,
            fmcsa_companies.state as company_state,
            fmcsa_companies.location as company_location,
            fmcsa_company_snapshot.hhg_authorization,
            fmcsa_company_snapshot.num_of_trucks,
            fmcsa_company_snapshot.num_of_tractors,
            fmcsa_company_snapshot.num_of_trailers,
            fmcsa_safer_data.drivers,
            fmcsa_safer_data.entity_type,
            fmcsa_safer_data.operating_status,
            fmcsa_safer_data.operation_classification,
            fmcsa_safer_data.carrier_type,
            fmcsa_safer_data.mileage,
            fmcsa_safer_data.mileage_year,
            fmcsa_safer_data.oos_date
        from staging.fmcsa_companies
        left join staging.fmcsa_company_snapshot using (usdot_num)
        left join staging.fmcsa_safer_data using (usdot_num)
        """

    elif table_name == "fmcsa_companies_complaints":
        query = """
        select
            id as complaint_id,
            company_id,
            complaint_year,
            complaint_count,
            complaint_category,
            oos_date,
            date_created as complaint_date
        from staging.transformed_fmcsa_companies tfc
        join staging.fmcsa_complaints fc on tfc.company_id = fc.usdot_num
        """

    elif table_name == "fmcsa_companies":
        query = "select * from staging.transformed_fmcsa_companies"

    elif table_name == "transformed_google_maps_companies":
        query = """
        select
            google_id as company_id,
            name as company_name,
            site as company_site,
            type as company_type,
            subtypes as company_subtypes,
            verified as company_verified,
            business_status,
            phone as company_phone,
            full_address as company_full_address,
            city as company_city,
            state as company_state,
            city || ', ' || state as company_location,
            working_hours_old_format as working_hours,
            latitude,
            longitude,
            time_zone,
            rating as overall_rating,
            reviews_link,
            street_view as street_view_link,
            owner_id,
            owner_title as owner_name,
            owner_link,
            reviews as reviews_amount	
        from staging.company_profiles_google_maps
        """

    elif table_name == "google_maps_companies_reviews":
        query = """
        select
            review_id,
            google_id as company_id,
            author_id,
            author_title as author_name,
            author_link,
            author_reviews_count,
            review_rating,
            review_likes,
            review_link,
            review_text,
            owner_answer,
            review_datetime_utc as review_date,
            owner_answer_timestamp_datetime_utc as owner_answer_date
        from staging.customer_reviews_google
        """

    elif table_name == "google_maps_companies":
        query = "select * from staging.transformed_google_maps_companies"

    df = read_query(query=query)
    upload_overwrite_table(df=df, table_name=table_name, schema_name=schema_name)


def convert_monthfirst_to_datetime(date_str):
    try:
        return pd.to_datetime(date_str, format="%m/%d/%Y %H:%M:%S")
    except ValueError:
        return pd.to_datetime(date_str, format="%m/%d/%Y %H:%M")


def convert_bool_as_string(bool_obj):
    return "Yes" if bool_obj else "No"


def validate_data_field(field):
    if field is None:
        return None
    str_field = str(field).strip()
    if str_field in ("--", "None", "None_1", "{}", "$$"):
        return None
    if isinstance(field, str):
        return str_field
    return field


def convert_state_abbreviation(str_obj):
    state_codes = {
        "FL": "Florida",
        "TX": "Texas",
        "WA": "Washington",
        "GA": "Georgia",
        "OR": "Oregon",
    }
    if str_obj in state_codes.keys():
        return state_codes.get(str_obj)
    return str_obj
