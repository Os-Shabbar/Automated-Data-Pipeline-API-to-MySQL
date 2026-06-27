"""
Portfolio Sample: Anonymized KoboToolbox-to-SQL Server ETL Pipeline
Author: Osama Shabbar

Purpose
-------
This script demonstrates an ETL workflow for humanitarian programme monitoring data:
1. Extract records from multiple KoboToolbox forms through API calls.
2. Standardize and harmonize inconsistent field structures across forms.
3. Apply data quality checks, deduplication, date parsing, and numeric conversions.
4. Protect sensitive fields through hashing and location approximation.
5. Load cleaned analytical tables into SQL Server using batch upserts.

Confidentiality Note
--------------------
This is a portfolio-safe, anonymized version. Organization names, project IDs,
beneficiary identifiers, exact locations, personal details, and internal field names
have been removed or replaced with generic placeholders.

Before production use, replace the generic source-column names in the mapping lists
with the approved field names from your actual data collection tools.
"""

from __future__ import annotations

import hashlib
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd
import pyodbc
import requests


# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)


# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------

REQUEST_TIMEOUT_SECONDS = int(os.getenv("REQUEST_TIMEOUT_SECONDS", "30"))
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "3"))
ANONYMIZE_GEOLOCATION = os.getenv("ANONYMIZE_GEOLOCATION", "true").lower() == "true"
LOAD_TO_SQLSERVER = os.getenv("LOAD_TO_SQLSERVER", "true").lower() == "true"

# Hash salt should be stored securely as an environment variable in production.
# For portfolio/demo use, this fallback keeps the sample runnable but should not be
# used for real personal data.
HASH_SALT = os.getenv("HASH_SALT", "portfolio-demo-salt")


class ConfigError(Exception):
    """Raised when required configuration is missing."""


def get_required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ConfigError(f"Missing required environment variable: {name}")
    return value


def load_config() -> Dict[str, object]:
    """Load configuration from environment variables.

    Required environment variables:
        KOBO_BASE_URL          Example: https://kf.kobotoolbox.org/api/v2/assets/
        KOBO_API_TOKEN         Kobo API token
        KOBO_PROJECT_UID_1     Kobo form asset UID for modality/source 1
        KOBO_PROJECT_UID_2     Kobo form asset UID for modality/source 2
        KOBO_PROJECT_UID_3     Kobo form asset UID for update/source 3

    Required only when LOAD_TO_SQLSERVER=true:
        SQLSERVER_HOST
        SQLSERVER_USER
        SQLSERVER_PASSWORD
        SQLSERVER_DB
        SQLSERVER_PORT
        SQLSERVER_DRIVER
    """

    project_uids = [
        os.getenv("KOBO_PROJECT_UID_1"),
        os.getenv("KOBO_PROJECT_UID_2"),
        os.getenv("KOBO_PROJECT_UID_3"),
    ]
    project_uids = [uid for uid in project_uids if uid]

    if not project_uids:
        raise ConfigError("At least one KOBO_PROJECT_UID_* environment variable is required.")

    config: Dict[str, object] = {
        "kobo_base_url": get_required_env("KOBO_BASE_URL").rstrip("/") + "/",
        "kobo_api_token": get_required_env("KOBO_API_TOKEN"),
        "project_uids": project_uids,
    }

    if LOAD_TO_SQLSERVER:
        config.update(
            {
                "sqlserver_host": get_required_env("SQLSERVER_HOST"),
                "sqlserver_port": os.getenv("SQLSERVER_PORT", "1433"),
                "sqlserver_user": get_required_env("SQLSERVER_USER"),
                "sqlserver_password": get_required_env("SQLSERVER_PASSWORD"),
                "sqlserver_db": get_required_env("SQLSERVER_DB"),
                "sqlserver_driver": os.getenv(
                    "SQLSERVER_DRIVER",
                    "ODBC Driver 18 for SQL Server",
                ),
            }
        )

    return config


# -----------------------------------------------------------------------------
# Anonymized source field mappings
# -----------------------------------------------------------------------------
# Replace these generic source-column names with approved, non-sensitive source
# fields from your Kobo forms if you adapt the sample for another portfolio item.

ADMIN_1_SOURCE_COLUMNS = [
    "admin_1_option_a",
    "admin_1_option_b",
    "admin_1_option_c",
    "admin_1_option_d",
]

ADMIN_2_SOURCE_COLUMNS = [
    "admin_2_option_a",
    "admin_2_option_b",
    "admin_2_option_c",
    "admin_2_option_d",
    "admin_2_option_e",
    "admin_2_option_f",
]

EXTERNAL_ID_MAPPINGS = [
    (["external_id_scan_1", "external_id_manual_1", "national_id_hash_1"], "external_id_1_hash"),
    (["external_id_scan_2", "external_id_manual_2", "national_id_hash_2"], "external_id_2_hash"),
    (["external_id_scan_3", "external_id_manual_3", "national_id_hash_3"], "external_id_3_hash"),
    (["external_id_scan_4", "external_id_manual_4", "national_id_hash_4"], "external_id_4_hash"),
    (["external_id_scan_5", "external_id_manual_5", "national_id_hash_5"], "external_id_5_hash"),
]

PROFILE_SOURCE_COLUMNS = [
    "case_id",
    "submission_date",
    "enumerator_id",
    "participant_name_local",
    "participant_name_english",
    "contact_1",
    "contact_2",
    "external_id_1_hash",
    "external_id_2_hash",
    "external_id_3_hash",
    "external_id_4_hash",
    "external_id_5_hash",
    "activity_type",
    "nationality_group",
    "admin_1",
    "admin_2",
    "admin_3",
    "area_code",
    "assistance_code",
    "latitude_approx",
    "longitude_approx",
]

SERVICE_PROVIDER_SOURCE_COLUMNS = [
    "case_id",
    "provider_name",
    "provider_contact_1",
    "provider_contact_2",
    "provider_id",
    "authorization_flag",
    "proxy_name",
    "proxy_contact_1",
    "proxy_contact_2",
    "proxy_id",
    "submission_date",
    "activity_type",
    "assistance_code",
]

ASSISTANCE_SOURCE_COLUMNS = [
    "case_id",
    "signed_amount",
    "household_members_reported",
    "household_members_verified",
    "assistance_duration_months",
    "monthly_assistance_amount",
    "total_assistance_amount",
    "vulnerability_score",
    "component_cost_a",
    "component_cost_b",
    "remaining_assistance_value",
    "activity_type",
    "submission_date",
    "assistance_code",
]

AGREEMENT_STATUS_SOURCE_COLUMNS = [
    "case_id",
    "assistance_code",
    "activity_type",
    "enumerator_id",
    "agreement_start_date",
    "agreement_signed",
    "payment_modality",
    "proxy_present",
    "proxy_name",
    "proxy_contact_1",
    "proxy_contact_2",
    "proxy_id",
    "submission_date",
]

CASE_CLOSURE_SOURCE_COLUMNS = [
    "case_id",
    "assistance_code",
    "activity_type",
    "enumerator_id",
    "closure_reason_category",
    "closure_notes",
    "record_id",
    "submission_date",
]

UPDATE_SOURCE_COLUMNS = [
    "case_id",
    "participant_name_local",
    "participant_name_english",
    "contact_1",
    "contact_2",
    "household_adult_male_count",
    "household_adult_female_count",
    "household_girl_count",
    "household_boy_count",
    "external_id_1",
    "external_id_2",
    "external_id_3",
    "external_id_4",
    "external_id_5",
    "activity_type",
    "submission_date",
]


# -----------------------------------------------------------------------------
# Extraction
# -----------------------------------------------------------------------------


def fetch_kobo_records(asset_uid: str, config: Dict[str, object]) -> List[dict]:
    """Fetch records from a KoboToolbox asset."""

    headers = {"Authorization": f"Token {config['kobo_api_token']}"}
    url = f"{config['kobo_base_url']}{asset_uid}/data.json"

    try:
        response = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT_SECONDS)
        response.raise_for_status()
        payload = response.json()
        records = payload.get("results", [])
        logger.info("Fetched %s records from source %s", len(records), asset_uid)
        return records
    except requests.RequestException as exc:
        logger.error("Failed to fetch source %s: %s", asset_uid, exc)
        return []


def fetch_all_sources(config: Dict[str, object]) -> Dict[str, pd.DataFrame]:
    """Fetch all configured Kobo sources in parallel."""

    source_frames: Dict[str, pd.DataFrame] = {}
    project_uids: List[str] = config["project_uids"]  # type: ignore[assignment]

    with ThreadPoolExecutor(max_workers=min(MAX_WORKERS, len(project_uids))) as executor:
        future_to_uid = {
            executor.submit(fetch_kobo_records, uid, config): uid for uid in project_uids
        }

        for index, future in enumerate(as_completed(future_to_uid), start=1):
            uid = future_to_uid[future]
            records = future.result()
            frame_name = f"source_{index}"
            source_frames[frame_name] = normalize_dataframe(pd.DataFrame(records))
            logger.info("Prepared dataframe %s from source UID %s", frame_name, uid)

    return source_frames


# -----------------------------------------------------------------------------
# Transformation helpers
# -----------------------------------------------------------------------------


def standardize_column_name(column: str) -> str:
    """Convert nested Kobo field names to simple snake_case names."""

    column = str(column).split("/")[-1]
    column = column.strip().replace(" ", "_").replace("-", "_")
    return column.lower()


def normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize column names and return an empty dataframe safely."""

    if df.empty:
        return pd.DataFrame()
    df = df.copy()
    df.columns = [standardize_column_name(col) for col in df.columns]
    return df


def rename_if_present(df: pd.DataFrame, rename_map: Dict[str, str]) -> pd.DataFrame:
    """Rename columns only when they exist in the dataframe."""

    existing_map = {src: dst for src, dst in rename_map.items() if src in df.columns}
    return df.rename(columns=existing_map)


def merge_available_fields(
    df: pd.DataFrame,
    source_columns: Iterable[str],
    target_column: str,
    separator: str = " ",
) -> pd.DataFrame:
    """Merge multiple optional columns into one target column."""

    df = df.copy()
    source_columns = [standardize_column_name(col) for col in source_columns]
    available = [col for col in source_columns if col in df.columns]

    if not available:
        df[target_column] = ""
        return df

    df[target_column] = (
        df[available]
        .fillna("")
        .astype(str)
        .apply(lambda row: separator.join([v.strip() for v in row if v.strip()]), axis=1)
    )
    return df


def hash_value(value: object) -> Optional[str]:
    """Hash a sensitive value using SHA-256 and a salt."""

    if pd.isna(value) or str(value).strip() == "":
        return None
    raw_value = f"{HASH_SALT}|{str(value).strip()}"
    return hashlib.sha256(raw_value.encode("utf-8")).hexdigest()


def hash_columns(df: pd.DataFrame, columns: Iterable[str]) -> pd.DataFrame:
    """Replace sensitive columns with hashed versions and drop original values."""

    df = df.copy()
    for column in columns:
        column = standardize_column_name(column)
        if column in df.columns:
            df[f"{column}_hash"] = df[column].apply(hash_value)
            df = df.drop(columns=[column])
    return df


def process_external_id_fields(df: pd.DataFrame) -> pd.DataFrame:
    """Create hashed external ID fields from optional scanned/manual ID columns."""

    df = df.copy()
    for source_columns, target_column in EXTERNAL_ID_MAPPINGS:
        df = merge_available_fields(df, source_columns, target_column.replace("_hash", ""))
        df[target_column] = df[target_column.replace("_hash", "")].apply(hash_value)
        df = df.drop(columns=[target_column.replace("_hash", "")], errors="ignore")
    return df


def extract_approx_geolocation(df: pd.DataFrame) -> pd.DataFrame:
    """Extract approximate location from Kobo _geolocation while avoiding precision risk.

    Kobo commonly stores _geolocation as [latitude, longitude]. For confidentiality,
    this portfolio version rounds coordinates to two decimals by default.
    """

    df = df.copy()
    if "_geolocation" not in df.columns:
        df["latitude_approx"] = None
        df["longitude_approx"] = None
        return df

    geo = pd.DataFrame(df["_geolocation"].tolist(), index=df.index)
    if geo.shape[1] >= 2:
        latitude = pd.to_numeric(geo.iloc[:, 0], errors="coerce")
        longitude = pd.to_numeric(geo.iloc[:, 1], errors="coerce")
        if ANONYMIZE_GEOLOCATION:
            latitude = latitude.round(2)
            longitude = longitude.round(2)
        df["latitude_approx"] = latitude
        df["longitude_approx"] = longitude
    else:
        df["latitude_approx"] = None
        df["longitude_approx"] = None

    return df


def parse_datetime_column(df: pd.DataFrame, column: str) -> pd.DataFrame:
    df = df.copy()
    if column in df.columns:
        df[column] = pd.to_datetime(df[column], errors="coerce")
    return df


def convert_numeric_columns(df: pd.DataFrame, columns: Iterable[str]) -> pd.DataFrame:
    df = df.copy()
    for column in columns:
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors="coerce")
    return df


def calculate_household_size(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate household size from available demographic count fields."""

    df = df.copy()
    count_columns = [
        "household_adult_male_count",
        "household_adult_female_count",
        "household_girl_count",
        "household_boy_count",
    ]
    available = [col for col in count_columns if col in df.columns]

    if available:
        counts = df[available].apply(pd.to_numeric, errors="coerce").fillna(0)
        df["household_size"] = counts.sum(axis=1).astype("Int64")
    else:
        df["household_size"] = pd.NA

    return df


def format_duration_months(value: object) -> Optional[str]:
    """Format a decimal month value into an approximate readable period."""

    try:
        numeric_value = float(value)
    except (TypeError, ValueError):
        return None

    if not np.isfinite(numeric_value) or numeric_value <= 0:
        return None

    months = int(numeric_value)
    days = int(round((numeric_value - months) * 30))
    return f"{months} months and {days} days"


def latest_record_by_case(
    df: pd.DataFrame,
    case_column: str = "case_id",
    date_column: str = "submission_date",
) -> pd.DataFrame:
    """Keep the most recent record per case ID."""

    if df.empty or case_column not in df.columns:
        return df

    df = parse_datetime_column(df, date_column)
    df = df[(df[case_column].notna()) & (df[case_column].astype(str).str.strip() != "")].copy()
    return (
        df.sort_values(by=date_column, ascending=False)
        .drop_duplicates(subset=case_column, keep="first")
        .reset_index(drop=True)
    )


def reindex_columns(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    """Select expected columns and create missing columns as nulls."""

    return df.reindex(columns=columns, fill_value=None)


# -----------------------------------------------------------------------------
# Main transformation logic
# -----------------------------------------------------------------------------


def prepare_source_frames(source_frames: Dict[str, pd.DataFrame]) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Prepare three logical sources from the fetched frames.

    The original production workflow used multiple Kobo forms. In this anonymized
    sample, source_1 and source_2 represent two assistance modalities, while
    source_3 represents participant update records.
    """

    frames = list(source_frames.values())
    while len(frames) < 3:
        frames.append(pd.DataFrame())

    modality_a = frames[0].copy()
    modality_b = frames[1].copy()
    updates = frames[2].copy()

    # Harmonize alternative case ID column names into one standard case_id field.
    modality_a = rename_if_present(modality_a, {"participant_code": "case_id"})
    modality_b = rename_if_present(modality_b, {"participant_code": "case_id"})
    updates = rename_if_present(updates, {"registration_number": "case_id"})

    # Standardize submission timestamp naming.
    for frame_name, frame in [("modality_a", modality_a), ("modality_b", modality_b), ("updates", updates)]:
        if "_submission_time" in frame.columns and "submission_date" not in frame.columns:
            frame.rename(columns={"_submission_time": "submission_date"}, inplace=True)
        logger.info("%s columns prepared: %s", frame_name, len(frame.columns))

    # Create anonymized modality codes rather than exposing internal programme codes.
    if not modality_a.empty:
        modality_a["assistance_code"] = "MOD-A-" + modality_a.get("case_id", "").astype(str)
    if not modality_b.empty:
        modality_b["assistance_code"] = "MOD-B-" + modality_b.get("case_id", "").astype(str)

    return modality_a, modality_b, updates


def transform_data(source_frames: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
    """Transform raw Kobo records into analytical SQL-ready tables."""

    modality_a, modality_b, updates = prepare_source_frames(source_frames)

    for frame in (modality_a, modality_b):
        if not frame.empty:
            frame = merge_available_fields(frame, ADMIN_1_SOURCE_COLUMNS, "admin_1", separator="/")
            frame = merge_available_fields(frame, ADMIN_2_SOURCE_COLUMNS, "admin_2", separator="/")

    # Apply transformations separately and reassign because pandas frames are immutable in helpers.
    modality_a = merge_available_fields(modality_a, ADMIN_1_SOURCE_COLUMNS, "admin_1", separator="/")
    modality_a = merge_available_fields(modality_a, ADMIN_2_SOURCE_COLUMNS, "admin_2", separator="/")
    modality_a = process_external_id_fields(modality_a)
    modality_a = extract_approx_geolocation(modality_a)

    modality_b = merge_available_fields(modality_b, ADMIN_1_SOURCE_COLUMNS, "admin_1", separator="/")
    modality_b = merge_available_fields(modality_b, ADMIN_2_SOURCE_COLUMNS, "admin_2", separator="/")
    modality_b = process_external_id_fields(modality_b)
    modality_b = extract_approx_geolocation(modality_b)

    updates = calculate_household_size(updates)
    updates = process_external_id_fields(updates)

    # ------------------------------------------------------------------
    # Participant profile table
    # ------------------------------------------------------------------
    profile_a = reindex_columns(modality_a, PROFILE_SOURCE_COLUMNS)
    profile_b = reindex_columns(modality_b, PROFILE_SOURCE_COLUMNS)
    participant_profile = pd.concat([profile_a, profile_b], ignore_index=True)

    participant_profile = participant_profile[
        participant_profile["case_id"].notna()
        & (participant_profile["case_id"].astype(str).str.strip() != "")
    ].copy()

    # Keep only validation-type records where applicable. The generic value below
    # should be replaced with the approved activity type from the source tool.
    if "activity_type" in participant_profile.columns:
        participant_profile = participant_profile[
            participant_profile["activity_type"].fillna("").isin(["case_validation", "case_update", ""])
        ].copy()

    participant_profile = latest_record_by_case(participant_profile)

    # Hash direct identifiers after building the profile output.
    participant_profile = hash_columns(
        participant_profile,
        ["participant_name_local", "participant_name_english", "contact_1", "contact_2"],
    )

    # ------------------------------------------------------------------
    # Service provider table
    # ------------------------------------------------------------------
    provider_a = reindex_columns(modality_a, SERVICE_PROVIDER_SOURCE_COLUMNS)
    provider_b = reindex_columns(modality_b, SERVICE_PROVIDER_SOURCE_COLUMNS)
    service_provider = pd.concat([provider_a, provider_b], ignore_index=True)

    if not service_provider.empty:
        service_provider = service_provider[
            service_provider["case_id"].notna()
            & (service_provider["case_id"].astype(str).str.strip() != "")
        ].copy()
        service_provider = latest_record_by_case(service_provider)
        service_provider = hash_columns(
            service_provider,
            [
                "provider_name",
                "provider_contact_1",
                "provider_contact_2",
                "provider_id",
                "proxy_name",
                "proxy_contact_1",
                "proxy_contact_2",
                "proxy_id",
            ],
        )

    # ------------------------------------------------------------------
    # Assistance record table
    # ------------------------------------------------------------------
    assistance_a = reindex_columns(modality_a, ASSISTANCE_SOURCE_COLUMNS)
    assistance_b = reindex_columns(modality_b, ASSISTANCE_SOURCE_COLUMNS)
    assistance_record = pd.concat([assistance_a, assistance_b], ignore_index=True)

    assistance_record = convert_numeric_columns(
        assistance_record,
        [
            "signed_amount",
            "household_members_reported",
            "household_members_verified",
            "assistance_duration_months",
            "monthly_assistance_amount",
            "total_assistance_amount",
            "vulnerability_score",
            "component_cost_a",
            "component_cost_b",
            "remaining_assistance_value",
        ],
    )

    if not assistance_record.empty:
        assistance_record = assistance_record[
            assistance_record["case_id"].notna()
            & (assistance_record["case_id"].astype(str).str.strip() != "")
        ].copy()
        assistance_record = latest_record_by_case(assistance_record)

        # Example calculated fields for reporting and payment planning.
        assistance_record["assistance_duration_text"] = assistance_record[
            "assistance_duration_months"
        ].apply(format_duration_months)
        assistance_record["first_installment_amount"] = assistance_record[
            "total_assistance_amount"
        ].fillna(0) * 0.70
        assistance_record["final_installment_amount"] = assistance_record[
            "total_assistance_amount"
        ].fillna(0) * 0.30

    # ------------------------------------------------------------------
    # Agreement/status table
    # ------------------------------------------------------------------
    agreement_a = reindex_columns(modality_a, AGREEMENT_STATUS_SOURCE_COLUMNS)
    agreement_b = reindex_columns(modality_b, AGREEMENT_STATUS_SOURCE_COLUMNS)
    agreement_status = pd.concat([agreement_a, agreement_b], ignore_index=True)

    if not agreement_status.empty:
        agreement_status = latest_record_by_case(agreement_status)
        agreement_status = parse_datetime_column(agreement_status, "agreement_start_date")
        agreement_status = hash_columns(
            agreement_status,
            ["proxy_name", "proxy_contact_1", "proxy_contact_2", "proxy_id"],
        )

    # ------------------------------------------------------------------
    # Case closure table
    # ------------------------------------------------------------------
    closure_a = reindex_columns(modality_a, CASE_CLOSURE_SOURCE_COLUMNS)
    closure_b = reindex_columns(modality_b, CASE_CLOSURE_SOURCE_COLUMNS)
    case_closure = pd.concat([closure_a, closure_b], ignore_index=True)

    if not case_closure.empty:
        case_closure = case_closure[
            case_closure["case_id"].notna()
            & (case_closure["case_id"].astype(str).str.strip() != "")
        ].copy()
        case_closure = latest_record_by_case(case_closure)
        # Do not store sensitive free-text notes in analytical outputs.
        case_closure["closure_note_available"] = case_closure["closure_notes"].notna()
        case_closure = case_closure.drop(columns=["closure_notes"], errors="ignore")

    # ------------------------------------------------------------------
    # Participant update table
    # ------------------------------------------------------------------
    participant_updates = reindex_columns(updates, UPDATE_SOURCE_COLUMNS + ["household_size"])
    if not participant_updates.empty:
        participant_updates = latest_record_by_case(participant_updates)
        participant_updates = hash_columns(
            participant_updates,
            [
                "participant_name_local",
                "participant_name_english",
                "contact_1",
                "contact_2",
                "external_id_1",
                "external_id_2",
                "external_id_3",
                "external_id_4",
                "external_id_5",
            ],
        )

    outputs = {
        "participant_profile": participant_profile,
        "service_provider": service_provider,
        "assistance_record": assistance_record,
        "agreement_status": agreement_status,
        "case_closure": case_closure,
        "participant_updates": participant_updates,
    }

    return outputs


# -----------------------------------------------------------------------------
# Data quality reporting
# -----------------------------------------------------------------------------


def run_quality_checks(outputs: Dict[str, pd.DataFrame]) -> None:
    """Log simple quality checks for each output table."""

    for table_name, df in outputs.items():
        if df.empty:
            logger.warning("%s: no records prepared", table_name)
            continue

        missing_case_id = df["case_id"].isna().sum() if "case_id" in df.columns else "N/A"
        duplicate_case_id = df.duplicated(subset=["case_id"]).sum() if "case_id" in df.columns else "N/A"
        logger.info(
            "%s: %s records | missing case_id: %s | duplicate case_id: %s",
            table_name,
            len(df),
            missing_case_id,
            duplicate_case_id,
        )


# -----------------------------------------------------------------------------
# Loading
# -----------------------------------------------------------------------------


def clean_value(value: object) -> object:
    """Convert pandas/numpy nulls and timestamps to database-friendly values."""

    if pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()
    if isinstance(value, np.generic):
        return value.item()
    return value


def quote_sqlserver_identifier(identifier: str) -> str:
    """Safely quote a SQL Server identifier such as a table or column name."""

    return f"[{str(identifier).replace(']', ']]')}]"


def quote_sqlserver_table_name(table_name: str) -> str:
    """Quote table names, including optional schema names like dbo.table_name."""

    return ".".join(quote_sqlserver_identifier(part) for part in str(table_name).split("."))


def get_sqlserver_connection(config: Dict[str, object]) -> pyodbc.Connection:
    """Create SQL Server connection using pyodbc."""

    connection_string = (
        f"DRIVER={{{config['sqlserver_driver']}}};"
        f"SERVER={config['sqlserver_host']},{config['sqlserver_port']};"
        f"DATABASE={config['sqlserver_db']};"
        f"UID={config['sqlserver_user']};"
        f"PWD={config['sqlserver_password']};"
        "Encrypt=yes;"
        "TrustServerCertificate=yes;"
    )

    return pyodbc.connect(connection_string, autocommit=False)


def build_sqlserver_merge_query(
    table_name: str,
    columns: List[str],
    key_columns: List[str],
) -> str:
    """Build a SQL Server MERGE statement for one-row parameterized upserts."""

    missing_keys = [col for col in key_columns if col not in columns]
    if missing_keys:
        raise ValueError(f"Missing key columns for upsert into {table_name}: {missing_keys}")

    update_columns = [col for col in columns if col not in key_columns]

    table_sql = quote_sqlserver_table_name(table_name)
    source_sql = ", ".join(f"? AS {quote_sqlserver_identifier(col)}" for col in columns)
    match_sql = " AND ".join(
        f"target.{quote_sqlserver_identifier(col)} = source.{quote_sqlserver_identifier(col)}"
        for col in key_columns
    )
    insert_columns_sql = ", ".join(quote_sqlserver_identifier(col) for col in columns)
    insert_values_sql = ", ".join(f"source.{quote_sqlserver_identifier(col)}" for col in columns)

    if update_columns:
        update_sql = ", ".join(
            f"target.{quote_sqlserver_identifier(col)} = source.{quote_sqlserver_identifier(col)}"
            for col in update_columns
        )
        matched_clause = f"WHEN MATCHED THEN UPDATE SET {update_sql}"
    else:
        matched_clause = ""

    return f"""
        MERGE INTO {table_sql} WITH (HOLDLOCK) AS target
        USING (SELECT {source_sql}) AS source
        ON {match_sql}
        {matched_clause}
        WHEN NOT MATCHED THEN
            INSERT ({insert_columns_sql})
            VALUES ({insert_values_sql});
    """


def upsert_dataframe(
    connection: pyodbc.Connection,
    table_name: str,
    df: pd.DataFrame,
    key_columns: List[str],
) -> int:
    """Batch upsert a dataframe into SQL Server.

    Assumption: target tables already exist and have a PRIMARY KEY or UNIQUE
    constraint matching key_columns, usually case_id for these portfolio tables.
    """

    if df.empty:
        logger.info("Skipping %s: dataframe is empty", table_name)
        return 0

    df = df.copy().replace({np.nan: None, np.inf: None, -np.inf: None})
    columns = list(df.columns)
    query = build_sqlserver_merge_query(table_name, columns, key_columns)
    values = [tuple(clean_value(row[col]) for col in columns) for _, row in df.iterrows()]

    with connection.cursor() as cursor:
        cursor.fast_executemany = True
        cursor.executemany(query, values)

    logger.info("Upserted %s records into %s", len(values), table_name)
    return len(values)


def load_outputs_to_sqlserver(config: Dict[str, object], outputs: Dict[str, pd.DataFrame]) -> None:
    """Load all output tables into SQL Server in one transaction."""

    connection: Optional[pyodbc.Connection] = None
    try:
        connection = get_sqlserver_connection(config)

        for table_name, df in outputs.items():
            upsert_dataframe(connection, table_name, df, key_columns=["case_id"])

        connection.commit()
        logger.info("All outputs committed successfully.")

    except pyodbc.Error as exc:
        logger.error("SQL Server error: %s", exc)
        if connection:
            connection.rollback()
            logger.info("Transaction rolled back.")
        raise

    except Exception as exc:
        logger.error("Unexpected loading error: %s", exc)
        if connection:
            connection.rollback()
            logger.info("Transaction rolled back.")
        raise

    finally:
        if connection:
            connection.close()
            logger.info("Database connection closed.")


# -----------------------------------------------------------------------------
# Main entry point
# -----------------------------------------------------------------------------


def main() -> None:
    logger.info("=" * 70)
    logger.info("ANONYMIZED KOBO TO SQL SERVER ETL PIPELINE")
    logger.info("=" * 70)

    config = load_config()
    source_frames = fetch_all_sources(config)
    outputs = transform_data(source_frames)
    run_quality_checks(outputs)

    if LOAD_TO_SQLSERVER:
        load_outputs_to_sqlserver(config, outputs)
    else:
        logger.info("LOAD_TO_SQLSERVER=false; skipping database load step.")

    logger.info("ETL sync complete.")


if __name__ == "__main__":
    main()
