# language=PostgreSQL

import pandas as pd
import csv
from typing import List, Tuple, Type, TypeVar
from pydantic import ValidationError, BaseModel
import sqlalchemy as sa
from sqlalchemy import text
from dotenv import load_dotenv
import datetime
import os
import json
from apps.kafka_minio_consumer.load_file_develop.product_schema import Segment, Sector, Department, Chief, PosInformation, Product, Contractor
from apps.kafka_minio_consumer.load_file_develop.shops_schema import Site, SiteInfo, SiteFormat, SiteAddress, SiteContact
load_dotenv()

from apps.logger_config import get_logger

T = TypeVar('T', bound=BaseModel)

logger = get_logger("apps.kafka_minio_consumer.load_file_develop.file_processor.py", service="kafka-minio-consumer")

SCHEMA_MAP = {
    'chief': Chief,
    'sector': Sector,
    'department': Department,
    'segment': Segment,
    'pos_information': PosInformation,
    'product': Product,
    'contractor': Contractor,
    'site': Site,
    'site_info': SiteInfo,
    'site_format': SiteFormat,
    'site_address': SiteAddress,
    'site_contact': SiteContact,
}
# ProcessData
class DataLoader:
    def __init__ (self, df: pd.DataFrame,source_file: str, correlation_id: str = None,engine: sa.engine.Engine = None):
        self.df = df
        self.path = source_file
        self.correlation_id = correlation_id
        self.connection_string = os.getenv("postgress_connection")
        # self.engine = sa.create_engine(self.connection_string)
        self.engine = engine or sa.create_engine(self.connection_string)

    def validate_shape(self, schema_name: str):
        shape=SCHEMA_MAP[schema_name]
        records: List[T] = []
        errors: List[Tuple[int, dict, str]] = []
        line_no = 1

        try:
            for line_no, row in enumerate(self.df.to_dict(orient='records'), start=2):
                try:
                    record=shape.model_validate(row)
                    records.append(record)
                except ValidationError as e:
                    errors.append((line_no, row, e.json()))
                except Exception as e:
                    logger.error(f"Error processing row {line_no}", extra={
                        'class': self.__class__.__name__,
                        'method': "validate_shape",
                        "line_no": line_no,
                        "row_data": row,
                        "error_type": type(e).__name__,
                        "error": str(e),
                    }, exc_info=True)
        except Exception as e:
            logger.error("Failed to read file", extra={
                # "file_path": self.path,
                "class": self.__class__.__name__,
                "method": "validate_shape",
                "file_path": self.path,
                "error_type": type(e).__name__,
                "error": str(e),
            })
            raise

        logger.info("Validation file completed", extra={
            'class': self.__class__.__name__,
            'method': "validate_shape",
            "file_path": self.path,
            "total_rows_read": line_no - 1,
            "valid_records": len(records),
            "invalid_records": len(errors),
        })

        if errors:
            logger.warning("Validation errors found", extra={
                'class': self.__class__.__name__,
                'method': "validate_shape",
                "file_path": self.path,
                "invalid_records": len(errors),
            })
        df=pd.DataFrame([s.model_dump() for s in records])
        return df, errors
        

    def load_to_db(self, df: pd.DataFrame, table_name: str):
        handler_map = {
            'chief': self._load_chief,
            'sector': self._load_sector,
            'department': self._load_department,
            'segment': self._load_segment,
            'pos_information': self._load_pos_information,
            'contractor': self._load_contractor,
            'product': self._load_product,
            'site': self._load_site,
            'site_info': self._load_site_info,
            'site_format': self._load_site_format,
            'site_address': self._load_site_address,
            'site_contact': self._load_site_contact,
        }        
        if table_name not in handler_map:
            logger.error("Load in to database not implemented for table",
            extra={"table": table_name,"method": "load_to_db",'class': self.__class__.__name__})
            raise NotImplementedError(f"Load to DB not implemented for table: {table_name}")
        return handler_map[table_name](df)

    def load_to_dead_letter(self, row_data:List[Tuple[int, dict, str]], source_table):
        try:
            sql = text('''
                INSERT INTO dead_letter (source_table, source_file, raw_row, error_details, line_no, correlation_id)
                VALUES (:source_table, :source_file, :raw_row, :error_details, :line_no, :correlation_id)
            ''')
            records=[{
                "source_table": source_table,
                "source_file": self.path,
                "raw_row": json.dumps(row,default=str),
                "error_details": error_details,
                "line_no": line_no,
                "correlation_id": self.correlation_id,
            } for line_no, row, error_details in row_data]
            
            with self.engine.begin() as conn:
                try:            
                    logger.info("Inserting records into dead letter", extra={
                        "class": self.__class__.__name__,
                        "method": "load_to_dead_letter",
                        "table": "dead_letter",
                        "records_count": len(records),
                        "source_file": self.path,
                        "source_table": source_table,
                    })
                    conn.execute(sql, records)
                except Exception as e:
                    logger.error('Failed inserting into dead letter',extra={
                        'class': self.__class__.__name__,
                        'method': "load_to_dead_letter",
                        "table": "dead_letter",
                    }, exc_info=True)
        except Exception as e:
            logger.error("Failed to load to dead letter", extra={
                "source_table": source_table,
                "row_data": row_data,
                "error_type": type(e).__name__,
                "error": str(e),
            }, exc_info=True)
            raise

    def _load_sector(self, df: pd.DataFrame) -> None:
        source_file=self.path
        sql=text(f'''
                    INSERT INTO sector (sector_id, sector_name, sector_code,source_file)
                    VALUES (:sector_id, :sector_name, :sector_code,:source_file)
                    ON CONFLICT (sector_id) DO UPDATE SET
                    sector_name = EXCLUDED.sector_name,
                    sector_code = EXCLUDED.sector_code,
                    source_file = EXCLUDED.source_file;''')
                    
        records=df.to_dict(orient='records')
        records = [{**record, "source_file": source_file} for record in records]

        if df.empty:
            logger.warning("Data frame is empty", extra={
                'class': self.__class__.__name__,
                'method': "_load_sector",
                "table": "sector",
                "source_file": source_file,
            })
            raise ValueError("Empty data frame: No sector records to load")

        with self.engine.begin() as conn:
            try:
                logger.info("Inserting records", extra={
                    'class': self.__class__.__name__,
                    'method': "_load_sector",
                    "table": "sector",
                    "records_count": len(records),
                    "source_file": source_file,
                })
                conn.execute(sql, records)
                logger.info("Records inserted successfully", extra={
                    "class": self.__class__.__name__,
                    "method": "_load_sector",
                    "table": "sector",
                    "records_count": len(records),
                })
                return len(records)
            except Exception as e:
                logger.error("Error inserting records into table", extra={
                    "class": self.__class__.__name__,
                    "method": "_load_sector",
                    "table": "sector",
                    "source_file": source_file,
                    "error_type": type(e).__name__,
                    "error": str(e),
                }, exc_info=True)
                raise


    def _get_existing_sectors(self) -> set[int]:
        try:
            with self.engine.begin() as conn:
                rows=conn.execute(text("SELECT sector_id FROM sector"))
                return set(row[0] for row in rows)
        except Exception as e:
            logger.error("Error fetching existing sectors", extra={
                "class": self.__class__.__name__,
                "method": "_get_existing_sectors",
                "error_type": type(e).__name__,
                "error": str(e),
            }, exc_info=True)
            raise

    def _load_department(self, df: pd.DataFrame) -> None:
        source_file=self.path

        if df.empty:
            logger.warning("Data frame is empty", extra={
                'class': self.__class__.__name__,
                'method': "_load_department",
                "table": "department",
                "source_file": source_file,
            })
            raise ValueError("No department records to load: DataFrame is empty")


        existing_sectors = self._get_existing_sectors()
        invalid = df[~df['sector_id'].isin(existing_sectors)].copy()
        valid = df[df['sector_id'].isin(existing_sectors)].copy()


        sql=text(f"""
                    INSERT INTO department (department_id, department_name,sector_id,source_file)
                    VALUES (:department_id, :department_name, :sector_id, :source_file)
                    ON CONFLICT (department_id) DO UPDATE SET
                        department_name = EXCLUDED.department_name,
                        sector_id = EXCLUDED.sector_id,
                        source_file = EXCLUDED.source_file;
                    """)
        

        records=valid.to_dict(orient='records')
        records = [{**record, "source_file": source_file} for record in records]

        if not invalid.empty:
            logger.warning("Rows skipped due to missing sector_id", extra={
                'class': self.__class__.__name__,
                'method': "_load_department",
                "table": "department",
                "skipped_count": len(invalid),
                "invalid_sector_ids": invalid['sector_id'].unique().tolist(),
                "source_file": source_file,
            })

        with self.engine.begin() as conn:
            try:
                logger.info("Inserting records", extra={
                    'class': self.__class__.__name__,
                    'method': "_load_department",
                    "table": "department",
                    "records_count": len(records),
                    "source_file": source_file,
                })
                conn.execute(sql, records)
                logger.info("Records inserted successfully", extra={
                    "class": self.__class__.__name__,
                    "method": "_load_department",
                    "table": "department",
                    "records_count": len(records),
                })
                return len(records)
            except Exception as e:
                logger.error("Error inserting records into table", extra={
                    "class": self.__class__.__name__,
                    "method": "_load_department",
                    "table": "department",
                    "source_file": source_file,
                    "error_type": type(e).__name__,
                    "error": str(e),
                }, exc_info=True)
                raise


    def _load_segment(self, df:pd.DataFrame) -> None:
        source_file=self.path
        
        if df.empty:
            logger.warning("Data frame is empty", extra={
                'class': self.__class__.__name__,
                'method': "_load_segment",
                "table": "segment",
                "source_file": source_file,
            })
            raise ValueError("No segment records to load: DataFrame is empty")

        existing_sectors = self._get_existing_sectors()
        invalid = df[~df['sector_id'].isin(existing_sectors)].copy()
        valid = df[df['sector_id'].isin(existing_sectors)].copy()

        sql=text(f"""
                    INSERT INTO segment (segment_id, segment_code, segment_name, sector_id, source_file)
                    VALUES (:segment_id, :segment_code, :segment_name, :sector_id, :source_file)
                    ON CONFLICT (segment_id) DO UPDATE SET
                        segment_code = EXCLUDED.segment_code,
                        segment_name = EXCLUDED.segment_name,
                        sector_id = EXCLUDED.sector_id,
                        source_file = EXCLUDED.source_file;
                    """)

        update_sql=text(f'''
                        UPDATE segment_chief 
                        SET is_current = False,
                        valid_to=current_date
                        where segment_id = :segment_id
                        and chief_id != :chief_id
                        and valid_to is null;

                        ''')

        insert_relation_sql = text(f'''
                                INSERT INTO segment_chief (segment_id, chief_id, is_current, valid_from, valid_to, source_file)
                                VALUES (:segment_id, :chief_id, :is_current, :valid_from, :valid_to, :source_file)
                                ON CONFLICT (segment_id, chief_id) DO UPDATE SET
                                is_current = True,
                                valid_from = current_date,
                                valid_to = NULL,
                                source_file = EXCLUDED.source_file;
                                
                                ''')
        updateRecords=valid.copy().to_dict(orient='records')
        source_file=self.path
        for record in updateRecords:
            record['source_file']=source_file
            record['valid_from']=datetime.date(2023,1,1)
            record['valid_to']=None
            record['is_current']=True


        records=valid.copy().to_dict(orient='records')
        for record in records:
            record['source_file']=source_file

        if not invalid.empty:
            logger.warning("Rows skipped due to missing sector_id", extra={
                "class": self.__class__.__name__,
                "method": "_load_segment",
                "table": "segment",
                "skipped_count": len(invalid),
                "invalid_sector_ids": invalid['sector_id'].unique().tolist(),
                "source_file": source_file,
            })

        with self.engine.begin() as conn:
            try:
                logger.info("Updating segment_chief relations", extra={
                    "class": self.__class__.__name__,
                    "method": "_load_segment",
                    "table": "segment_chief",
                    "records_count": len(updateRecords),
                })
                conn.execute(update_sql, updateRecords)

                logger.info("Inserting records", extra={
                    'class': self.__class__.__name__,
                    'method': "_load_segment",
                    "table": "segment",
                    "records_count": len(records),
                    "source_file": source_file,
                })
                conn.execute(sql, records)

                logger.info("Inserting segment_chief relations", extra={
                    "class": self.__class__.__name__,
                    "method": "_load_segment",
                    "table": "segment_chief",
                    "records_count": len(updateRecords),
                })
                conn.execute(insert_relation_sql, updateRecords)

                logger.info("Records inserted successfully", extra={
                    "class": self.__class__.__name__,
                    "method": "_load_segment",
                    "table": "segment",
                    "records_count": len(records),
                })
                return len(records)
            except Exception as e:
                logger.error("DB insert failed", extra={
                    "class": self.__class__.__name__,
                    "method": "_load_segment",
                    "table": "segment",
                    "source_file": source_file,
                    "error_type": type(e).__name__,
                    "error": str(e),
                }, exc_info=True)
                raise


    def _load_chief(self, df: pd.DataFrame) -> None:
        source_file=self.path

        if df.empty:
            logger.warning("Data frame is empty", extra={
                'class': self.__class__.__name__,
                'method': "_load_chief",
                "table": "chief",
                "source_file": source_file,
            })
            raise ValueError("No chief records to load: DataFrame is empty")

        sql = text(f"""
                    INSERT INTO chief (chief_id, chief_first_name, chief_last_name, email_address, phone_number,source_file)
                    VALUES (:chief_id, :chief_first_name, :chief_last_name, :chief_email, :chief_phone, :source_file)
                    ON CONFLICT (chief_id) DO UPDATE SET
                    email_address = EXCLUDED.email_address,
                    phone_number = EXCLUDED.phone_number,
                    source_file = EXCLUDED.source_file
                        ;
                    """)
        

        today=datetime.date.today()
        df_chief=df.copy()
        df_chief['source_file']=source_file

        with self.engine.begin() as conn:
            try:
                record = df_chief.to_dict(orient='records')
                logger.info("Inserting records", extra={
                    "class": self.__class__.__name__,
                    "method": "_load_chief",
                    "table": "chief",
                    "records_count": len(record),
                    "source_file": source_file,
                })
                conn.execute(sql, record)
                logger.info("Records inserted successfully", extra={
                    "class": self.__class__.__name__,
                    "method": "_load_chief",
                    "table": "chief",
                    "records_count": len(record),
                })
                return len(record)
            except Exception as e:
                logger.error("DB insert failed", extra={
                    'class': self.__class__.__name__,
                    'method': "_load_chief",
                    "table": "chief",
                    "source_file": source_file,
                    "error_type": type(e).__name__,
                    "error": str(e),
                }, exc_info=True)
                raise

    
    def _load_contractor(self, df: pd.DataFrame) -> None:
        source_file=self.path
        sql = text(f'''
        INSERT INTO contractor (contractor_id, contractor_name, contractor_phone_number, contractor_email_address, contractor_address, source_file, created_at,updated_at)
        VALUES (:contractor_id, :contractor_name, :contractor_phone_number, :contractor_email_address, :contractor_address, :source_file, :created_at, :updated_at)
        ON CONFLICT (contractor_id) DO UPDATE SET
        contractor_name = EXCLUDED.contractor_name,
        contractor_phone_number = EXCLUDED.contractor_phone_number,
        contractor_email_address = EXCLUDED.contractor_email_address,
        contractor_address = EXCLUDED.contractor_address
        ''')

        if df.empty:
            logger.warning("Data frame is empty", extra={
                'class': self.__class__.__name__,
                'method': "_load_contractor",
                "table": "contractor",
                "source_file": source_file,
            })
            raise ValueError("No contractor records to load: DataFrame is empty")

        if 'contract_number' in df.columns:
            update_contract_sql = text(f''' 
            UPDATE contract 
            SET is_current = False,
            valid_to = current_date
            where 
            contractor_id = :contractor_id
            and contract_number != :contract_number
            and is_current = True
            and valid_to is null;
            ''')
            insert_contract_sql = text(f'''
            INSERT INTO contract (contractor_id,contract_number, signed_date,status,is_current,valid_from,valid_to,source_file)
            VALUES (:contractor_id, :contract_number, :signed_date, :status, :is_current, :valid_from, :valid_to, :source_file)
            ON CONFLICT (contractor_id,contract_number) DO UPDATE SET
            signed_date = EXCLUDED.signed_date,
            status = EXCLUDED.status,
            valid_from = EXCLUDED.valid_from
            ''')
        contractor_df=df[['contractor_id', 'contractor_name', 'contractor_phone_number', 'contractor_email_address', 'contractor_address']]
        
        contractor_df=contractor_df.to_dict(orient='records')
        for record in contractor_df:    
            record['created_at']=datetime.date.today()
            record['updated_at']=datetime.date.today()
            record['source_file']=source_file    

        if 'contract_number' in df.columns:
            update_contract_df=df[['contractor_id', 'contract_number']]
            update_contract_df['source_file']=source_file

            insert_df=update_contract_df.copy()
            insert_df['signed_date']=datetime.date(2025,1,1)
            insert_df['status']='active'
            insert_df['is_current']=True
            insert_df['valid_from']=datetime.datetime.now()
            insert_df['valid_to']=None
            insert_df['source_file']=source_file

            insert_df=insert_df.to_dict(orient='records')
            update_contract_df=update_contract_df.to_dict(orient='records')


        with self.engine.begin() as conn:
            try:
                logger.info("Inserting records", extra={
                    'class': self.__class__.__name__,
                    'method': "_load_contractor",
                    "table": "contractor",
                    "records_count": len(contractor_df),
                    "source_file": source_file,
                })
                conn.execute(sql, contractor_df)

                if 'contract_number' in df.columns:
                    logger.info("Updating  relations", extra={
                        'class': self.__class__.__name__,
                        'method': "_load_contractor",
                        "table": "contract",
                        "records_count": len(update_contract_df),
                        "source_file": source_file,
                    })
                    conn.execute(update_contract_sql, update_contract_df)

                    logger.info("Inserting records", extra={
                        'class': self.__class__.__name__,
                        'method': "_load_contractor",
                        "table": "contract",
                        "records_count": len(insert_df),
                        "source_file": source_file,
                    })
                    conn.execute(insert_contract_sql, insert_df)
                logger.info("Records inserted successfully", extra={
                    'class': self.__class__.__name__,
                    'method': "_load_contractor",
                    "table": "contractor",
                    "records_count": len(contractor_df),
                })
                return len(contractor_df)
            except Exception as e:
                logger.error("DB insert failed", extra={
                    'class': self.__class__.__name__,
                    'method': "_load_contractor",
                    "table": "contractor",
                    "source_file": source_file,
                    "error_type": type(e).__name__,
                    "error": str(e),
                }, exc_info=True)
                raise

    def _get_existing_art_keys(self) -> set[int]:
        try:
            with self.engine.begin() as conn:
                rows = conn.execute(text("SELECT art_key FROM product"))
                return set(row[0] for row in rows)
        except Exception as e:
            logger.error("Error fetching existing art_keys", extra={
                "class": self.__class__.__name__,
                "method": "_get_existing_art_keys",
                "error_type": type(e).__name__,
                "error": str(e),
            }, exc_info=True)
            raise

    def _get_current_pos_information(self, art_keys: set) -> pd.DataFrame:
        """Bieżące wiersze pos_information (date_end IS NULL) dla podanych art_key."""
        try:
            if not art_keys:
                return pd.DataFrame(columns=["art_key", "ean", "price_net", "price_gross", "vat_rate"])
            with self.engine.begin() as conn:
                q = text(
                    "SELECT art_key, ean, price_net, price_gross, vat_rate "
                    "FROM pos_information WHERE date_end IS NULL AND art_key = ANY(:keys)"
                )
                rows = conn.execute(q, {"keys": list(art_keys)})
                return pd.DataFrame(
                    rows.fetchall(),
                    columns=["art_key", "ean", "price_net", "price_gross", "vat_rate"],
                )
        except Exception as e:
            logger.error("Error fetching current pos information", extra={
                'class': self.__class__.__name__,
                'method': "_get_current_pos_information",
                "error_type": type(e).__name__,
                "error": str(e),
            }, exc_info=True)
            raise

    def _get_existing_contractor_ids(self) -> set[int]:
        try:
            with self.engine.begin() as conn:
                rows = conn.execute(text("SELECT contractor_id FROM contractor"))
                return set(row[0] for row in rows)
        except Exception as e:
            logger.error("Error fetching existing contractor_ids", extra={
                "class": self.__class__.__name__,
                "method": "_get_existing_contractor_ids",
                "error_type": type(e).__name__,
                "error": str(e),
            }, exc_info=True)
            raise

    def _get_existing_segment_ids(self) -> set[int]:
        try:
            with self.engine.begin() as conn:
                rows = conn.execute(text("SELECT segment_id FROM segment"))
                return set(row[0] for row in rows)
        except Exception as e:
            logger.error("Error fetching existing segment_ids", extra={
                "class": self.__class__.__name__,
                "method": "_get_existing_segment_ids",
                "error_type": type(e).__name__,
                "error": str(e),
            }, exc_info=True)
            raise

    def _get_existing_department_ids(self) -> set[int]:
        try:
            with self.engine.begin() as conn:
                rows = conn.execute(text("SELECT department_id FROM department"))
                return set(row[0] for row in rows)
        except Exception as e:
            logger.error("Error fetching existing department_ids", extra={
                "class": self.__class__.__name__,
                "method": "_get_existing_department_ids",
                "error_type": type(e).__name__,
                "error": str(e),
            }, exc_info=True)
            raise

    def _load_pos_information(self, df: pd.DataFrame) -> None:
        """Wstawia tylko wiersze nowe lub ze zmienioną ceną. Ten sam (art_key, ean) i ta sama cena = pomijamy."""

        source_file=self.path
        if df.empty:
            logger.warning("Data frame is empty", extra={
                'class': self.__class__.__name__,
                'method': "_load_pos_information",
                "table": "pos_information",
                "source_file": source_file,
            })
            raise ValueError("No pos_information records to load: DataFrame is empty")

        update_sql = text('''
            UPDATE pos_information
            SET date_end = :date_end,
                is_current = :is_current,
                last_modified_date = :last_modified_date,
                source_file = :source_file
            WHERE art_key = :art_key AND ean = :ean AND date_end IS NULL
        ''')

        insert_sql = text('''
            INSERT INTO pos_information (art_key, ean, vat_rate, price_net, price_gross,
                                        date_start, last_modified_date, source_file, is_current)
            VALUES (:art_key, :ean, :vat_rate, :price_net, :price_gross, :date_start,
                    :last_modified_date, :source_file, :is_current)
            
        ''')

        existing_art_keys = self._get_existing_art_keys()
        valid = df[df["art_key"].isin(existing_art_keys)].copy()

        if valid.empty:
            logger.info("pos_information: no valid art_keys found.")
            return 0

        # SELECT + compare + UPDATE + INSERT w jednej transakcji z blokowaniem wierszy (FOR UPDATE)
        # Zapobiega race condition gdy dwa pliki przetwarzają ten sam art_key równocześnie
        lock_sql = text(
            "SELECT art_key, ean, price_net, price_gross, vat_rate "
            "FROM pos_information WHERE date_end IS NULL AND art_key = ANY(:keys) "
            "FOR UPDATE"
        )

        with self.engine.begin() as conn:
            try:
                art_key_list = valid["art_key"].unique().tolist()
                rows = conn.execute(lock_sql, {"keys": art_key_list}).fetchall()
                current = pd.DataFrame(
                    rows, columns=["art_key", "ean", "price_net", "price_gross", "vat_rate"]
                )
                current = current.rename(columns={
                    "price_net": "price_net_db",
                    "price_gross": "price_gross_db",
                    "vat_rate": "vat_rate_db",
                })
                merged = valid.merge(current, on=["art_key", "ean"], how="left")

                price_unchanged = (
                    merged["price_net_db"].notna()
                    & (merged["price_net"] == merged["price_net_db"])
                    & (merged["price_gross"] == merged["price_gross_db"])
                    & (merged["vat_rate"] == merged["vat_rate_db"])
                )
                to_insert = (
                    merged[~price_unchanged]
                    .drop(columns=["price_net_db", "price_gross_db", "vat_rate_db"], errors="ignore")
                    .drop_duplicates(subset=["art_key", "ean"])
                )

                if to_insert.empty:
                    logger.info("pos_information: no new or changed rows to load.")
                    return 0

                to_insert = to_insert.copy()
                to_insert["date_start"] = datetime.date(2023, 1, 1)
                to_insert["source_file"] = source_file
                to_insert["last_modified_date"] = datetime.datetime.now()
                to_insert["date_end"] = None
                to_insert["is_current"] = True
                valid_df = to_insert.to_dict(orient="records")

                update_df = to_insert[["art_key", "ean"]].drop_duplicates()
                update_df["source_file"] = source_file
                update_df["date_end"] = datetime.date.today()
                update_df["is_current"] = False
                update_df["last_modified_date"] = datetime.datetime.now()
                update_rows = update_df.to_dict(orient="records")

                if update_rows:
                    logger.info("Closing outdated pos_information records", extra={
                        'class': self.__class__.__name__,
                        'method': "_load_pos_information",
                        "table": "pos_information",
                        "records_count": len(update_rows),
                        "source_file": source_file,
                    })
                    conn.execute(update_sql, update_rows)

                logger.info("Inserting new pos_information records", extra={
                    'class': self.__class__.__name__,
                    'method': "_load_pos_information",
                    "table": "pos_information",
                    "records_count": len(valid_df),
                    "source_file": source_file,
                })
                conn.execute(insert_sql, valid_df)
                logger.info("Records inserted successfully", extra={
                    'class': self.__class__.__name__,
                    'method': "_load_pos_information",
                    "table": "pos_information",
                    "records_count": len(valid_df),
                    "source_file": source_file,
                })
                return len(valid_df)
            except Exception as e:
                logger.error("Error with pos_information loading", extra={
                    'class': self.__class__.__name__,
                    'method': "_load_pos_information",
                    "table": "pos_information",
                    "source_file": source_file,
                    "error_type": type(e).__name__,
                    "error": str(e),
                }, exc_info=True)
                raise
            

    def _load_product(self, df: pd.DataFrame) -> None:
        """
        Load product records into DB. Table: product_id, art_key, art_number,
        contractor_id, segment_id, department_id, brand, article_codification_date,
        last_modified_at, source_file. Validates FK to contractor, segment, department.
        """
        source_file=self.path
        if df.empty:
            logger.warning("No product records to load", extra={
                'class': self.__class__.__name__,
                'method': "_load_product",
                "table": "product",
                "reason": "DataFrame is empty",
                "source_file": source_file
            })
            raise ValueError("No product records to load: DataFrame is empty")

        sql = text("""
                   INSERT INTO product (art_key, art_number, contractor_id, segment_id, department_id,
                                        brand, article_codification_date, last_modified_at, source_file)
                   VALUES (:art_key, :art_number, :contractor_id, :segment_id, :department_id,
                           :brand, :article_codification_date, :last_modified_at, :source_file)
                   ON CONFLICT (art_key) DO UPDATE SET
                     art_number = EXCLUDED.art_number,
                        contractor_id = EXCLUDED.contractor_id,
                        segment_id = EXCLUDED.segment_id,
                        department_id = EXCLUDED.department_id,
                        brand = EXCLUDED.brand,
                        article_codification_date = EXCLUDED.article_codification_date,
                        last_modified_at = EXCLUDED.last_modified_at,
                        source_file = EXCLUDED.source_file;
                   """)

        existing_contractors = self._get_existing_contractor_ids()
        existing_segments = self._get_existing_segment_ids()
        existing_departments = self._get_existing_department_ids()

        valid = df[
            df["contractor_id"].isin(existing_contractors)
            & df["segment_id"].isin(existing_segments)
            & df["departament_id"].isin(existing_departments)
            ].copy()
        invalid = df[
            ~df["contractor_id"].isin(existing_contractors)
            | ~df["segment_id"].isin(existing_segments)
            | ~df["departament_id"].isin(existing_departments)
            ].copy()

        if not invalid.empty:
            logger.warning("Rows skipped due to invalid FK", extra={
                'class': self.__class__.__name__,
                'method': "_load_product",
                "table": "product",
                "skipped_count": len(invalid),
                "source_file": self.path
            })
        if valid.empty:
            logger.warning("No valid product records to insert", extra={
                'class': self.__class__.__name__,
                'method': "_load_product",
                "table": "product",
                "source_file": self.path,
            })
            return 0

        valid = valid[
            [
                "art_key",
                "art_number",
                "segment_id",
                "departament_id",
                "contractor_id",
                "brand",
                "article_codification_date",
            ]
        ].copy()
        valid["department_id"] = valid["departament_id"]
        valid["source_file"] = self.path
        valid["last_modified_at"] = datetime.datetime.now()
        records = valid[["art_key", "art_number", "contractor_id", "segment_id", "department_id", "brand",
                         "article_codification_date", "last_modified_at", "source_file"]].to_dict(orient="records")

        with self.engine.begin() as conn:
            try:
                logger.info("Inserting records", extra={
                    'class': self.__class__.__name__,
                    'method': "_load_product",
                    "table": "product",
                    "records_count": len(records),
                    "source_file": self.path,
                })
                conn.execute(sql, records)
                logger.info("Records inserted successfully", extra={
                    'class': self.__class__.__name__,
                    'method': "_load_product",
                    "table": "product",
                    "records_count": len(records),
                })
                return len(records)
            except Exception as e:
                logger.error("DB insert failed", extra={
                    'class': self.__class__.__name__,
                    'method': "_load_product",
                    "table": "product",
                    "source_file": self.path,
                    "error_type": type(e).__name__,
                    "error": str(e),
                }, exc_info=True)
                raise
    '''
    methods for loading site tables
    '''
    def _load_site(self, df: pd.DataFrame) -> None:
        source_file=self.path

        if df.empty:
            logger.warning("Data frame is empty", extra={
                'class': self.__class__.__name__,
                'method': "_load_site",
                "table": "site",
                "source_file": source_file,
            })
            raise ValueError("No site records to load: DataFrame is empty")

        sql = text('''
                    INSERT INTO site (site_unique_code, site_code, site_name, created_at, updated_at, source_file)
                    VALUES (:site_unique_code, :site_code, :site_name, :created_at, :updated_at, :source_file)
                    ON CONFLICT (site_unique_code) DO UPDATE SET
                        site_code = EXCLUDED.site_code,
                        site_name = EXCLUDED.site_name,
                        updated_at = EXCLUDED.updated_at,
                        source_file = EXCLUDED.source_file;
                    ''')
        df_site=df.copy()
        df_site['created_at']=datetime.datetime.now()
        df_site['updated_at']=datetime.datetime.now()
        df_site['source_file']=source_file
        records=df_site.to_dict(orient='records')

        with self.engine.begin() as conn:
            try:
                logger.info("Inserting records", extra={
                    'class': self.__class__.__name__,
                    'method': "_load_site",
                    "table": "site",
                    "records_count": len(records),
                    "source_file": source_file,
                })
                conn.execute(sql, records)
                logger.info("Records inserted successfully", extra={
                    'class': self.__class__.__name__,
                    'method': "_load_site",
                    "table": "site",
                    "records_count": len(records),
                })
                return len(records)
            except Exception as e:
                logger.error("DB insert failed", extra={
                    'class': self.__class__.__name__,
                    'method': "_load_site",
                    "table": "site",
                    "source_file": source_file,
                    "error_type": type(e).__name__,
                    "error": str(e),
                }, exc_info=True)
                raise
    
    def _get_existing_site_codes(self) -> set[str]:
        try:
            with self.engine.begin() as conn:
                rows = conn.execute(text('SELECT site_unique_code FROM site'))
                return set(row[0] for row in rows)
        except Exception as e:
            logger.error("Error fetching existing site codes", extra={
                "class": self.__class__.__name__,
                "method": "_get_existing_site_codes",
                "error_type": type(e).__name__,
                "error": str(e),
            }, exc_info=True)
            raise

    def _load_site_info(self, df: pd.DataFrame) -> int:
        source_file = self.path
        if df.empty:
            logger.warning("Data frame is empty", extra={
                'class': self.__class__.__name__,
                'method': "_load_site_info",
                "table": "site_info",
                "source_file": source_file,
            })
            raise ValueError("No site_info records to load: DataFrame is empty")

        existing_site_codes = self._get_existing_site_codes()
        df_work = df.copy()

        invalid = df_work[~df_work['site_unique_code'].isin(existing_site_codes)].copy()
        valid = df_work[df_work['site_unique_code'].isin(existing_site_codes)].copy()

        if not invalid.empty:
            logger.warning("Rows skipped due to missing site_unique_code in site table", extra={
                'class': self.__class__.__name__,
                'method': "_load_site_info",
                "table": "site_info",
                "skipped_count": len(invalid),
                "invalid_site_codes": invalid['site_unique_code'].unique().tolist(),
                "source_file": source_file,
            })

        if valid.empty:
            logger.warning("No valid site_info records to insert", extra={
                'class': self.__class__.__name__,
                'method': "_load_site_info",
                "table": "site_info",
                "source_file": source_file,
            })
            return 0

        # SELECT z FOR UPDATE + compare + UPDATE + INSERT w jednej transakcji
        check_sql = text('''
            SELECT site_unique_code, site_status_code
            FROM site_info
            WHERE site_unique_code = ANY(:codes)
              AND is_current = True
            FOR UPDATE;
        ''')

        update_sql = text('''
            UPDATE site_info
            SET is_current = False,
                valid_to = current_date
            WHERE site_unique_code = :site_unique_code
              AND is_current = True;
        ''')

        insert_sql = text('''
            INSERT INTO site_info (site_unique_code, site_status_code, site_opening_date, site_closing_date,
                                   is_current, valid_from, valid_to, source_file)
            VALUES (:site_unique_code, :site_status_code, :site_opening_date, :site_closing_date,
                    :is_current, :valid_from, :valid_to, :source_file)
        ''')

        codes = valid['site_unique_code'].unique().tolist()

        with self.engine.begin() as conn:
            try:
                existing = conn.execute(check_sql, {"codes": codes}).fetchall()
                current_status = {row[0]: row[1] for row in existing}

                changed_mask = valid['site_unique_code'].apply(
                    lambda code: current_status.get(code) != valid.loc[valid['site_unique_code'] == code, 'site_status_code'].iloc[0]
                )
                new_mask = ~valid['site_unique_code'].isin(current_status.keys())
                valid = valid[changed_mask | new_mask].copy()

                if valid.empty:
                    logger.info("No status changes detected, skipping insert", extra={
                        'class': self.__class__.__name__,
                        'method': "_load_site_info",
                        "table": "site_info",
                        "source_file": source_file,
                    })
                    return 0

                if 'valid_from' in valid.columns and valid['valid_from'].notna().any():
                    pass
                else:
                    valid['valid_from'] = datetime.date.today()

                is_active = valid['site_status_code'] == "ACTIVE"

                if 'site_opening_date' in valid.columns and valid['site_opening_date'].notna().any():
                    valid['site_opening_date'] = valid['site_opening_date']
                else:
                    valid['site_opening_date'] = datetime.date.today()

                valid['valid_to'] = None
                valid.loc[~is_active, 'valid_to'] = datetime.date.today()

                if 'site_closing_date' not in valid.columns:
                    valid['site_closing_date'] = None
                valid.loc[is_active, 'site_closing_date'] = None
                valid.loc[~is_active, 'site_closing_date'] = valid.loc[~is_active, 'site_closing_date'].fillna(datetime.date.today())

                valid['is_current'] = True
                valid['source_file'] = source_file
                records = valid[['site_unique_code', 'site_status_code', 'site_opening_date', 'site_closing_date',
                                 'is_current', 'valid_from', 'valid_to', 'source_file']].to_dict(orient='records')

                update_records = valid[['site_unique_code']].drop_duplicates().to_dict(orient='records')

                if update_records:
                    logger.info("Closing outdated site_info records", extra={
                        'class': self.__class__.__name__,
                        'method': "_load_site_info",
                        "table": "site_info",
                        "records_count": len(update_records),
                        "source_file": source_file,
                    })
                    conn.execute(update_sql, update_records)

                logger.info("Inserting records", extra={
                    'class': self.__class__.__name__,
                    'method': "_load_site_info",
                    "table": "site_info",
                    "records_count": len(records),
                    "source_file": source_file,
                })
                conn.execute(insert_sql, records)
                logger.info("Records inserted successfully", extra={
                    'class': self.__class__.__name__,
                    'method': "_load_site_info",
                    "table": "site_info",
                    "records_count": len(records),
                })
                return len(records)
            except Exception as e:
                logger.error("DB insert failed", extra={
                    'class': self.__class__.__name__,
                    'method': "_load_site_info",
                    "table": "site_info",
                    "source_file": source_file,
                    "error_type": type(e).__name__,
                    "error": str(e),
                }, exc_info=True)
                raise

    def _load_site_format(self, df: pd.DataFrame) -> int:
        source_file = self.path

        if df.empty:
            logger.warning("Data frame is empty", extra={
                'class': self.__class__.__name__,
                'method': "_load_site_format",
                "table": "site_format",
                "source_file": source_file,
            })
            raise ValueError("No site_format records to load: DataFrame is empty")

        existing_site_codes = self._get_existing_site_codes()
        df_work = df.copy()

        invalid = df_work[~df_work['site_unique_code'].isin(existing_site_codes)].copy()
        valid = df_work[df_work['site_unique_code'].isin(existing_site_codes)].copy()

        if not invalid.empty:
            logger.warning("Rows skipped due to missing site_unique_code in site table", extra={
                'class': self.__class__.__name__,
                'method': "_load_site_format",
                "table": "site_format",
                "skipped_count": len(invalid),
                "invalid_site_codes": invalid['site_unique_code'].unique().tolist(),
                "source_file": source_file,
            })

        if valid.empty:
            logger.warning("No valid site_format records to insert", extra={
                'class': self.__class__.__name__,
                'method': "_load_site_format",
                "table": "site_format",
                "source_file": source_file,
            })
            return 0

        # SELECT z FOR UPDATE + compare + UPDATE + INSERT w jednej transakcji
        check_sql = text('''
            SELECT site_unique_code, site_format_unique_code
            FROM site_format
            WHERE site_unique_code = ANY(:codes)
              AND is_current = True
            FOR UPDATE;
        ''')

        update_sql = text('''
            UPDATE site_format
            SET is_current = False,
                valid_to = current_date
            WHERE site_unique_code = :site_unique_code
              AND is_current = True;
        ''')

        insert_sql = text('''
            INSERT INTO site_format (site_unique_code, site_format_unique_code, is_current, valid_from, valid_to, source_file)
            VALUES (:site_unique_code, :site_format_unique_code, :is_current, :valid_from, :valid_to, :source_file)
        ''')

        codes = valid['site_unique_code'].unique().tolist()

        with self.engine.begin() as conn:
            try:
                existing = conn.execute(check_sql, {"codes": codes}).fetchall()
                current_data = {row[0]: row[1] for row in existing}

                changed_mask = valid.apply(
                    lambda r: current_data.get(r['site_unique_code']) != r['site_format_unique_code'], axis=1
                )
                new_mask = ~valid['site_unique_code'].isin(current_data.keys())
                valid = valid[changed_mask | new_mask].copy()

                if valid.empty:
                    logger.info("No format changes detected, skipping insert", extra={
                        'class': self.__class__.__name__,
                        'method': "_load_site_format",
                        "table": "site_format",
                        "source_file": source_file,
                    })
                    return 0

                if 'valid_from' not in valid.columns or not valid['valid_from'].notna().any():
                    valid['valid_from'] = datetime.date.today()
                valid['valid_to'] = None
                valid['is_current'] = True
                valid['source_file'] = source_file
                records = valid[['site_unique_code', 'site_format_unique_code', 'is_current', 'valid_from',
                                 'valid_to', 'source_file']].to_dict(orient='records')

                update_records = valid[['site_unique_code']].drop_duplicates().to_dict(orient='records')

                if update_records:
                    logger.info("Closing outdated site_format records", extra={
                        'class': self.__class__.__name__,
                        'method': "_load_site_format",
                        "table": "site_format",
                        "records_count": len(update_records),
                        "source_file": source_file,
                    })
                    conn.execute(update_sql, update_records)

                logger.info("Inserting records", extra={
                    'class': self.__class__.__name__,
                    'method': "_load_site_format",
                    "table": "site_format",
                    "records_count": len(records),
                    "source_file": source_file,
                })
                conn.execute(insert_sql, records)
                logger.info("Records inserted successfully", extra={
                    'class': self.__class__.__name__,
                    'method': "_load_site_format",
                    "table": "site_format",
                    "records_count": len(records),
                })
                return len(records)
            except Exception as e:
                logger.error("DB insert failed", extra={
                    'class': self.__class__.__name__,
                    'method': "_load_site_format",
                    "table": "site_format",
                    "source_file": source_file,
                    "error_type": type(e).__name__,
                    "error": str(e),
                }, exc_info=True)
                raise

    def _load_site_address(self, df: pd.DataFrame) -> int:
        source_file = self.path

        if df.empty:
            logger.warning("Data frame is empty", extra={
                'class': self.__class__.__name__,
                'method': "_load_site_address",
                "table": "site_address",
                "source_file": source_file,
            })
            raise ValueError("No site_address records to load: DataFrame is empty")

        existing_site_codes = self._get_existing_site_codes()
        df_work = df.copy()

        invalid = df_work[~df_work['site_unique_code'].isin(existing_site_codes)].copy()
        valid = df_work[df_work['site_unique_code'].isin(existing_site_codes)].copy()

        if not invalid.empty:
            logger.warning("Rows skipped due to missing site_unique_code in site table", extra={
                'class': self.__class__.__name__,
                'method': "_load_site_address",
                "table": "site_address",
                "skipped_count": len(invalid),
                "invalid_site_codes": invalid['site_unique_code'].unique().tolist(),
                "source_file": source_file,
            })

        if valid.empty:
            logger.warning("No valid site_address records to insert", extra={
                'class': self.__class__.__name__,
                'method': "_load_site_address",
                "table": "site_address",
                "source_file": source_file,
            })
            return 0

        # SELECT z FOR UPDATE + compare + UPDATE + INSERT w jednej transakcji
        check_sql = text('''
            SELECT site_unique_code, site_address_zip_code, site_address_city, site_address_complement,
                   city_code, country_code, site_geo_coordinate_x_value, site_geo_coordinate_y_value
            FROM site_address
            WHERE site_unique_code = ANY(:codes)
              AND is_current = True
            FOR UPDATE;
        ''')

        update_sql = text('''
            UPDATE site_address
            SET is_current = False
            WHERE site_unique_code = :site_unique_code
              AND is_current = True;
        ''')

        insert_sql = text('''
            INSERT INTO site_address (site_unique_code, site_address_zip_code, site_address_city, site_address_complement,
                                      city_code, country_code, site_geo_coordinate_x_value, site_geo_coordinate_y_value,
                                      is_current, source_file)
            VALUES (:site_unique_code, :site_address_zip_code, :site_address_city, :site_address_complement,
                    :city_code, :country_code, :site_geo_coordinate_x_value, :site_geo_coordinate_y_value,
                    :is_current, :source_file)
        ''')

        codes = valid['site_unique_code'].unique().tolist()

        with self.engine.begin() as conn:
            try:
                existing = conn.execute(check_sql, {"codes": codes}).fetchall()
                current_data = {
                    row[0]: (str(row[1]), str(row[2]), str(row[3]), str(row[4]),
                             str(row[5]), str(row[6]), str(row[7]))
                    for row in existing
                }

                def _address_changed(r):
                    key = r['site_unique_code']
                    if key not in current_data:
                        return True
                    return current_data[key] != (
                        str(r.get('site_address_zip_code', '')),
                        str(r.get('site_address_city', '')),
                        str(r.get('site_address_complement', '')),
                        str(r.get('city_code', '')),
                        str(r.get('country_code', '')),
                        str(r.get('site_geo_coordinate_x_value', '')),
                        str(r.get('site_geo_coordinate_y_value', '')),
                    )

                valid = valid[valid.apply(_address_changed, axis=1)].copy()

                if valid.empty:
                    logger.info("No address changes detected, skipping insert", extra={
                        'class': self.__class__.__name__,
                        'method': "_load_site_address",
                        "table": "site_address",
                        "source_file": source_file,
                    })
                    return 0

                for col in ['site_address_zip_code', 'site_address_city', 'site_address_complement',
                            'city_code', 'country_code', 'site_geo_coordinate_x_value', 'site_geo_coordinate_y_value']:
                    if col not in valid.columns:
                        valid[col] = None

                valid['is_current'] = True
                valid['source_file'] = source_file
                records = valid[['site_unique_code', 'site_address_zip_code', 'site_address_city', 'site_address_complement',
                                 'city_code', 'country_code', 'site_geo_coordinate_x_value', 'site_geo_coordinate_y_value',
                                 'is_current', 'source_file']].to_dict(orient='records')

                update_records = valid[['site_unique_code']].drop_duplicates().to_dict(orient='records')

                if update_records:
                    logger.info("Closing outdated site_address records", extra={
                        'class': self.__class__.__name__,
                        'method': "_load_site_address",
                        "table": "site_address",
                        "records_count": len(update_records),
                        "source_file": source_file,
                    })
                    conn.execute(update_sql, update_records)

                logger.info("Inserting records", extra={
                    'class': self.__class__.__name__,
                    'method': "_load_site_address",
                    "table": "site_address",
                    "records_count": len(records),
                    "source_file": source_file,
                })
                conn.execute(insert_sql, records)
                logger.info("Records inserted successfully", extra={
                    'class': self.__class__.__name__,
                    'method': "_load_site_address",
                    "table": "site_address",
                    "records_count": len(records),
                })
                return len(records)
            except Exception as e:
                logger.error("DB insert failed", extra={
                    'class': self.__class__.__name__,
                    'method': "_load_site_address",
                    "table": "site_address",
                    "source_file": source_file,
                    "error_type": type(e).__name__,
                    "error": str(e),
                }, exc_info=True)
                raise

    def _load_site_contact(self, df: pd.DataFrame) -> int:
        source_file = self.path

        if df.empty:
            logger.warning("Data frame is empty", extra={
                'class': self.__class__.__name__,
                'method': "_load_site_contact",
                "table": "site_contact",
                "source_file": source_file,
            })
            raise ValueError("No site_contact records to load: DataFrame is empty")

        existing_site_codes = self._get_existing_site_codes()
        df_work = df.copy()

        invalid = df_work[~df_work['site_unique_code'].isin(existing_site_codes)].copy()
        valid = df_work[df_work['site_unique_code'].isin(existing_site_codes)].copy()

        if not invalid.empty:
            logger.warning("Rows skipped due to missing site_unique_code in site table", extra={
                'class': self.__class__.__name__,
                'method': "_load_site_contact",
                "table": "site_contact",
                "skipped_count": len(invalid),
                "invalid_site_codes": invalid['site_unique_code'].unique().tolist(),
                "source_file": source_file,
            })

        if valid.empty:
            logger.warning("No valid site_contact records to insert", extra={
                'class': self.__class__.__name__,
                'method': "_load_site_contact",
                "table": "site_contact",
                "source_file": source_file,
            })
            return 0

        # SELECT z FOR UPDATE + compare + UPDATE + INSERT w jednej transakcji
        check_sql = text('''
            SELECT site_unique_code, contact_type, contact_value, contact_role, is_primary
            FROM site_contact
            WHERE site_unique_code = ANY(:codes)
              AND valid_to IS NULL
            FOR UPDATE;
        ''')

        update_sql = text('''
            UPDATE site_contact
            SET valid_to = current_date
            WHERE site_unique_code = :site_unique_code
              AND valid_to IS NULL;
        ''')

        insert_sql = text('''
            INSERT INTO site_contact (site_unique_code, contact_type, contact_value, contact_role,
                                      valid_from, valid_to, is_primary, source_file)
            VALUES (:site_unique_code, :contact_type, :contact_value, :contact_role,
                    :valid_from, :valid_to, :is_primary, :source_file)
        ''')

        codes = valid['site_unique_code'].unique().tolist()

        with self.engine.begin() as conn:
            try:
                existing = conn.execute(check_sql, {"codes": codes}).fetchall()
                current_data = {
                    row[0]: (str(row[1]), str(row[2]), str(row[3]), str(row[4]))
                    for row in existing
                }

                def _contact_changed(r):
                    key = r['site_unique_code']
                    if key not in current_data:
                        return True
                    return current_data[key] != (
                        str(r.get('contact_type', '')),
                        str(r.get('contact_value', '')),
                        str(r.get('contact_role', '')),
                        str(r.get('is_primary', '')),
                    )

                valid = valid[valid.apply(_contact_changed, axis=1)].copy()

                if valid.empty:
                    logger.info("No contact changes detected, skipping insert", extra={
                        'class': self.__class__.__name__,
                        'method': "_load_site_contact",
                        "table": "site_contact",
                        "source_file": source_file,
                    })
                    return 0

                if 'valid_from' not in valid.columns or not valid['valid_from'].notna().any():
                    valid['valid_from'] = datetime.date.today()
                valid['valid_to'] = None
                if 'contact_role' not in valid.columns:
                    valid['contact_role'] = None
                if 'is_primary' not in valid.columns:
                    valid['is_primary'] = False
                valid['source_file'] = source_file
                records = valid[['site_unique_code', 'contact_type', 'contact_value', 'contact_role',
                                 'valid_from', 'valid_to', 'is_primary', 'source_file']].to_dict(orient='records')

                update_records = valid[['site_unique_code']].drop_duplicates().to_dict(orient='records')

                if update_records:
                    logger.info("Closing outdated site_contact records", extra={
                        'class': self.__class__.__name__,
                        'method': "_load_site_contact",
                        "table": "site_contact",
                        "records_count": len(update_records),
                        "source_file": source_file,
                    })
                    conn.execute(update_sql, update_records)

                logger.info("Inserting records", extra={
                    'class': self.__class__.__name__,
                    'method': "_load_site_contact",
                    "table": "site_contact",
                    "records_count": len(records),
                    "source_file": source_file,
                })
                conn.execute(insert_sql, records)
                logger.info("Records inserted successfully", extra={
                    'class': self.__class__.__name__,
                    'method': "_load_site_contact",
                    "table": "site_contact",
                    "records_count": len(records),
                })
                return len(records)
            except Exception as e:
                logger.error("Insert into Database failed", extra={
                    'class': self.__class__.__name__,
                    'method': "_load_site_contact",
                    "table": "site_contact",
                    "source_file": source_file,
                    "error_type": type(e).__name__,
                    "error": str(e),
                }, exc_info=True)
                raise
