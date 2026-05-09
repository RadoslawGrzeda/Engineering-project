{% macro ensure_all_scd_tables() %}

  {% set scd_tables = [
    {
      'schema':   'gold_customer',
      'table':    'gold_client_dim_address',
      'order_by': '(person_id, address_type, dbt_valid_from)',
      'columns':  'person_id String, address_type String, option_channel Nullable(String), street Nullable(String), zip_code Nullable(String), city Nullable(String), country Nullable(String), latitude Nullable(Float64), longitude Nullable(Float64)'
    },
    {
      'schema':   'gold_customer',
      'table':    'gold_client_dim_contact',
      'order_by': '(person_id, contact_type, dbt_valid_from)',
      'columns':  'person_id String, contact_type String, contact_value Nullable(String), flag_main_type Nullable(UInt8), preferred_channel Nullable(String), option_channel Nullable(String), flag_valid Nullable(UInt8)'
    },
    {
      'schema':   'gold_customer',
      'table':    'gold_client_dim_customer',
      'order_by': '(person_id, dbt_valid_from)',
      'columns':  'person_id String, first_name Nullable(String), middle_name Nullable(String), last_name Nullable(String), birth_date Nullable(Date), passport_number Nullable(String), gender_code Nullable(String), gender_name Nullable(String), salutation Nullable(String), civil_status_code Nullable(String), civil_status_name Nullable(String), language_code Nullable(String), language_name Nullable(String), language_level Nullable(String), nationality_code Nullable(String), nationality_name Nullable(String), registration_date Nullable(DateTime), is_deleted Nullable(UInt8)'
    },
    {
      'schema':   'gold_customer',
      'table':    'gold_client_dim_digital_access',
      'order_by': '(person_id, dbt_valid_from)',
      'columns':  'person_id String, username Nullable(String), email Nullable(String), is_active Nullable(UInt8), last_login_at Nullable(DateTime), portal_user_confirmation_at Nullable(DateTime)'
    },
    {
      'schema':   'gold_customer',
      'table':    'gold_client_fct_indicator',
      'order_by': '(person_id, indicator_type, dbt_valid_from)',
      'columns':  'person_id String, indicator_type String, polish_name Nullable(String), english_name Nullable(String), is_active Nullable(UInt8)'
    },
    {
      'schema':   'gold_customer',
      'table':    'gold_client_fct_loyalty',
      'order_by': '(identifier_id, dbt_valid_from)',
      'columns':  'identifier_id String, person_id String, status_code Nullable(String), status_name Nullable(String), status_rules Nullable(String), start_date Nullable(DateTime), end_date Nullable(DateTime), evaluation_at Nullable(DateTime)'
    },
    {
      'schema':   'gold_customer',
      'table':    'gold_client_fct_subscription',
      'order_by': '(person_id, communication_code, dbt_valid_from)',
      'columns':  'person_id String, communication_code String, communication_name Nullable(String), status Nullable(String), subscription_date Nullable(DateTime), unsubscription_date Nullable(DateTime), unsubscription_reason Nullable(String)'
    },
    {
      'schema':   'gold_product',
      'table':    'dim_chief',
      'order_by': '(chief_id, dbt_valid_from)',
      'columns':  'chief_id String, first_name Nullable(String), last_name Nullable(String), phone_number Nullable(String), email Nullable(String)'
    },
    {
      'schema':   'gold_product',
      'table':    'dim_product',
      'order_by': '(art_key, dbt_valid_from)',
      'columns':  'art_key Int32, art_number Nullable(String), brand Nullable(String), article_codification_date Nullable(Date), department_name Nullable(String), sector_code Nullable(String), sector_name Nullable(String), segment_code Nullable(String), segment_name Nullable(String), contractor_name Nullable(String)'
    },
    {
      'schema':   'gold_product',
      'table':    'dim_segment_chief',
      'order_by': '(segment_chief_id, dbt_valid_from)',
      'columns':  'segment_chief_id Int32, chief_id String, segment_id Nullable(Int32), segment_code Nullable(String), src_valid_from Nullable(Date)'
    },
    {
      'schema':   'gold_product',
      'table':    'dim_pos_information',
      'order_by': '(art_key, dbt_valid_from)',
      'columns':  'pos_information_id Int32, art_key Int32, ean Nullable(String), vat_rate Nullable(String), price_net Nullable(Float64), price_gross Nullable(Float64), src_valid_from Nullable(Date)'
    },
    {
      'schema':   'gold_shop',
      'table':    'dim_site',
      'order_by': '(site_unique_code, dbt_valid_from)',
      'columns':  'site_unique_code String, site_code Nullable(String), site_name Nullable(String), status_code Nullable(String), opening_date Nullable(Date), closing_date Nullable(Date), format_code Nullable(String), zip_code Nullable(String), city Nullable(String), street Nullable(String), city_code Nullable(String), country_code Nullable(String), latitude Nullable(Float64), longitude Nullable(Float64), contact_type Array(String), contact_value Array(String), contact_role Array(String)'
    }
  ] %}

  {% for t in scd_tables %}
    {% do adapter.execute("CREATE DATABASE IF NOT EXISTS " ~ t.schema) %}
    {% set ddl %}
      CREATE TABLE IF NOT EXISTS {{ t.schema }}.{{ t.table }} (
        {{ t.columns }},
        _row_hash        String,
        is_current       UInt8,
        dbt_valid_from   DateTime,
        dbt_valid_to     DateTime,
        dbt_updated_at   DateTime
      ) ENGINE = ReplacingMergeTree(dbt_updated_at)
      ORDER BY {{ t.order_by }}
    {% endset %}
    {% do adapter.execute(ddl) %}
  {% endfor %}

{% endmacro %}
