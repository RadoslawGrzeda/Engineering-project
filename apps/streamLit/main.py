from typing import Any
import time
import io
import os
from datetime import datetime

import streamlit as st
import streamlit_authenticator as stauth
from dotenv import load_dotenv
from yaml import safe_load
import pandas as pd
from minio_client import MinioClient
import structlog
from streamlit_authenticator import Authenticate


st.set_page_config(
    page_title="Data Loader",
    page_icon="📂",
    layout="centered",
    initial_sidebar_state="expanded",
)

structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.dev.ConsoleRenderer(),
    ],
)
log = structlog.get_logger()

CONTRACTS_DIR = os.path.join(os.path.dirname(__file__), "contracts")

BUCKET_OPTIONS: dict[str, list[str]] = {
    "sklep": ["test", "cos"],
    "produkt": ["chief", "segment", "sector", "department", "pos_information", "contractor", "product"],
}

BUCKET_LABELS: dict[str, str] = {
    "sklep": "Sklep",
    "produkt": "Produkt",
}

FRESHNESS_LABELS: dict[str, str] = {
    "when_file_arrives": "Przy wgraniu pliku",
    "daily": "Codziennie",
    "weekly": "Co tydzień",
    "monthly": "Co miesiąc",
}


def     load_contract(file_type: str) -> dict[str, Any] | None:
    path = os.path.join(CONTRACTS_DIR, f"{file_type}.yaml")
    if not os.path.exists(path):
        return None
    with open(path, "r", encoding="utf-8") as f:
        return safe_load(f)


def _validate_col_type(series: pd.Series, expected_type: str) -> bool:
    non_null = series.dropna()
    if non_null.empty:
        return True
    try:
        if expected_type == "integer":
            non_null.astype(float).apply(lambda x: int(x) if x == int(x) else (_ for _ in ()).throw(ValueError()))
        elif expected_type == "float":
            non_null.astype(float)
        elif expected_type == "boolean":
            valid = {"0", "1", "true", "false", "t", "f", "yes", "no", "y", "n"}
            if not non_null.astype(str).str.lower().isin(valid).all():
                return False
        elif expected_type == "date":
            pd.to_datetime(non_null)
    except (ValueError, TypeError):
        return False
    return True


def validate_against_contract(df: pd.DataFrame, contract: dict) -> dict[str, Any]:
    """Validate a DataFrame against a data contract.

    Returns a result dict with keys:
      valid, missing, extra, nullable_errors, type_errors
    """
    result: dict[str, Any] = {
        "valid": True,
        "missing": [],
        "extra": [],
        "nullable_errors": [],
        "type_errors": [],
    }

    contract_cols = {col["name"] for col in contract["columns"]}
    df_cols = set(df.columns)
    result["missing"] = sorted(contract_cols - df_cols)
    result["extra"] = sorted(df_cols - contract_cols)

    if result["missing"] or result["extra"]:
        result["valid"] = False
        return result

    for col_def in contract["columns"]:
        col_name = col_def["name"]
        col_type = col_def.get("type", "string")
        nullable = col_def.get("nullable", True)

        if not nullable and df[col_name].isnull().any():
            result["nullable_errors"].append(col_name)

        if not _validate_col_type(df[col_name], col_type):
            result["type_errors"].append(col_name)

    if result["nullable_errors"] or result["type_errors"]:
        result["valid"] = False

    return result


class StreamlitApp:
    def __init__(self):
        load_dotenv()
        self.client = MinioClient()

    def init_config(self, yaml_path: str) -> dict:
        with open(yaml_path, "r") as f:
            yaml_content = f.read()
        yaml_content = yaml_content.replace("COOKIE_KEY_PLACEHOLDER", os.getenv("API_KEY"))
        yaml_content = yaml_content.replace("USER_NAME_PLACEHOLDER", os.getenv("name"))
        yaml_content = yaml_content.replace("USER_PASSWORD_PLACEHOLDER", os.getenv("password"))
        yaml_content = yaml_content.replace("ADMIN_PASSWORD_PLACEHOLDER", os.getenv("admin_pass"))
        yaml_content = yaml_content.replace("ADMIN_NAME_PLACEHOLDER", os.getenv("admin_name"))
        return safe_load(yaml_content)

    def authentication(self, config: dict) -> Authenticate:
        return stauth.Authenticate(
            config["credentials"],
            config["cookie"]["name"],
            config["cookie"]["key"],
            config["cookie"]["expiry_days"],
        )

    def _render_sidebar(self, authenticator: Authenticate):
        with st.sidebar:
            st.markdown("## Profil użytkownika")
            name = st.session_state.get("name", "")
            roles = st.session_state.get("roles", [])
            st.markdown(f"**Użytkownik:** {name}")
            if roles:
                roles_str = ", ".join(roles) if isinstance(roles, list) else str(roles)
                st.markdown(f"**Role:** {roles_str}")
            st.divider()
            authenticator.logout(button_name="Wyloguj", location="sidebar")

    def _render_contract_info(self, contract: dict):
        with st.expander("Podgląd kontraktu danych", expanded=False):
            st.markdown(f"**Opis:** {contract.get('description', '—')}")

            col_sla, col_policy = st.columns(2)
            sla = contract.get("sla", {})
            policy = contract.get("change_policy", {})

            with col_sla:
                st.markdown("**SLA**")
                freshness_raw = sla.get("freshness", "—")
                freshness_label = FRESHNESS_LABELS.get(freshness_raw, freshness_raw)
                st.markdown(f"- Odświeżanie: `{freshness_label}`")
                st.markdown(f"- Maks. opóźnienie: `{sla.get('max_delay_hours', '—')} h`")
                st.markdown(f"- Właściciel: `{sla.get('owner', '—')}`")

            with col_policy:
                st.markdown("**Polityka zmian**")
                review = "Tak" if policy.get("review_required") else "Nie"
                st.markdown(f"- Review wymagany: `{review}`")
                breaking = policy.get("breaking_changes", "—")
                st.markdown(f"- Breaking changes: `{breaking}`")
                notify = policy.get("notify", [])
                if notify:
                    st.markdown(f"- Powiadomienia: `{', '.join(notify)}`")

            st.markdown("**Kolumny**")
            col_defs = contract.get("columns", [])
            df_contract = pd.DataFrame(col_defs)[["name", "type", "nullable"]]
            df_contract.columns = ["Kolumna", "Typ", "Nullable"]
            df_contract["Nullable"] = df_contract["Nullable"].map({True: "tak", False: "nie"})
            st.dataframe(df_contract, use_container_width=True, hide_index=True)

    def _render_upload_section(self, bucket: str, file_type: str):
        st.markdown("---")

        contract = load_contract(file_type)
        if contract:
            self._render_contract_info(contract)
        else:
            st.info("Brak kontraktu danych dla tego typu pliku – walidacja struktury pominięta.")

        st.subheader("Wgraj plik")
        uploaded_file = st.file_uploader(
            f"Wybierz plik CSV dla **{file_type}**",
            type=["csv"],
            key=f"uploaded_file_{st.session_state.uploaded_file_key}",
        )

        if uploaded_file is None:
            return

        try:
            df = pd.read_csv(uploaded_file)
            uploaded_file.seek(0)
        except Exception as e:
            st.error(f"Nie udało się wczytać pliku CSV: {e}")
            return

        if contract:
            result = validate_against_contract(df, contract)

            if result["valid"]:
                st.success("Struktura i typy danych są poprawne.")
            else:
                errors = []
                if result["missing"]:
                    errors.append(f"Brakujące kolumny: `{', '.join(result['missing'])}`")
                if result["extra"]:
                    errors.append(f"Nieoczekiwane kolumny: `{', '.join(result['extra'])}`")
                if result["nullable_errors"]:
                    errors.append(f"Puste wartości w kolumnach non-nullable: `{', '.join(result['nullable_errors'])}`")
                if result["type_errors"]:
                    errors.append(f"Niezgodność typów w kolumnach: `{', '.join(result['type_errors'])}`")
                st.error("Walidacja nie przeszła:\n\n" + "\n\n".join(errors))
                return

        with st.expander("Podgląd danych (pierwsze 10 wierszy)", expanded=True):
            st.dataframe(df.head(10), use_container_width=True, hide_index=True)

        st.markdown("---")
        col_info, col_btn = st.columns([3, 1])
        with col_info:
            size_kb = len(uploaded_file.getvalue()) / 1024
            st.markdown(f"**Plik:** `{uploaded_file.name}`")
            st.markdown(f"**Rozmiar:** `{size_kb:.1f} KB`  |  **Wierszy:** `{len(df)}`")
        with col_btn:
            send = st.button("Wyślij", type="primary", use_container_width=True)

        if send:
            filename = f"{datetime.now().strftime('%Y%m%d')}_{uploaded_file.name}"
            up_data = uploaded_file.getvalue()
            with st.spinner("Wysyłanie pliku do MinIO..."):
                try:
                    if not self.client.bucket_exists(bucket):
                        self.client.make_bucket(st.session_state["name"], bucket)
                        log.info("Bucket created", bucket=bucket)

                    result_msg = self.client.upload_file(
                        st.session_state["name"],
                        bucket,
                        filename,
                        io.BytesIO(up_data),
                        len(up_data),
                        content_type="application/csv",
                    )
                    log.info("File uploaded", filename=filename, bucket=bucket)
                    st.success(result_msg)
                    time.sleep(2)
                    st.session_state.uploaded_file_key += 1
                    st.rerun()
                except Exception as e:
                    log.error("Upload failed", error=str(e))
                    st.error(f"Błąd podczas wysyłania: {e}")

    def run(self, authenticator: Authenticate):
        if st.session_state["authentication_status"]:
            self._render_sidebar(authenticator)

            st.title("Data Loader")
            st.markdown("Wybierz temat i typ pliku, a następnie wgraj plik CSV.")
            st.markdown("---")

            st.subheader("Konfiguracja")
            col1, col2 = st.columns(2)
            with col1:
                bucket = st.selectbox(
                    "Temat",
                    options=list(BUCKET_OPTIONS.keys()),
                    format_func=lambda x: BUCKET_LABELS.get(x, x),
                )
            with col2:
                file_type = st.selectbox(
                    "Typ pliku",
                    options=BUCKET_OPTIONS[bucket],
                )

            self._render_upload_section(bucket, file_type)

        elif st.session_state["authentication_status"] is False:
            st.error("Błędne dane logowania. Spróbuj ponownie.")

        elif st.session_state["authentication_status"] is None:
            st.info("Zaloguj się, aby kontynuować.")


if __name__ == "__main__":
    yaml_path = "config.yaml"

    if "uploaded_file_key" not in st.session_state:
        st.session_state.uploaded_file_key = 0

    app = StreamlitApp()
    yml_config = app.init_config(yaml_path)
    authenticator = app.authentication(yml_config)
    authenticator.login("main")

    app.run(authenticator)
