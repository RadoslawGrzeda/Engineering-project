import pandas as pd
import csv
from typing import List, Tuple, Type, TypeVar
from pydantic import ValidationError, BaseModel
import sqlalchemy as sa
from sqlalchemy import text
from dotenv import load_dotenv
import datetime
import os
from shema import Segment, Sector, Department, Chief, PosInformation, Product, Contractor
load_dotenv()
from apps.logger_config import get_logger

T = TypeVar('T', bound=BaseModel)

logger = get_logger(__name__)

class process_data:
    def __init__ (self, path: str):
        self.path = path
        self.connection_string = os.getenv("postgress_connection")
        self.engine = sa.create_engine(self.connection_string)
    

    def validate_shape(self, shape: Type[T]):
        records: List[T] = []
        errors: List[Tuple[int, str]] = []
        line_no = 1

        with open(self.path, 'r', encoding='UTF-8', newline='') as f:
            try:
                reader = csv.DictReader(f)
                logger.info("Validating records starting", extra={"file_path": self.path})

                for line_no, row in enumerate(reader, start=2):
                    try:
                        records.append(shape.model_validate(row))
                    except ValidationError as e:
                        errors.append((line_no, row, e.json()))
                    except Exception as e:
                        logger.error("Unexpected row error", extra={
                            "file_path": self.path,
                            "line_no": line_no,
                            "row_data": row,
                            "error_type": type(e).__name__,
                            "error": str(e),
                        }, exc_info=True)
            except Exception as e:
                logger.error("Failed to read file", extra={
                    "file_path": self.path,
                    "error_type": type(e).__name__,
                    "error": str(e),
                })

        logger.info("Validation completed", extra={
            "file_path": self.path,
            "total_rows_read": line_no - 1,
            "valid_records": len(records),
            "invalid_records": len(errors),
        })

        if errors:
            logger.warning("Validation errors found", extra={
                "file_path": self.path,
                "invalid_records": len(errors),
            })

        '''
        duplicates = set()
        seen = set()
        for s in records:
            if s.model_dump()[0] in seen:
                duplicates.add(s.model_dump()[0])
            else:
                seen.add(s.model_dump()[0])

        if duplicates:
            logger.warning(f"Duplicate segment_id found: {sorted(duplicates)[:20]} (count={len(duplicates)})")
        else:
            logger.info("No duplicate segment_id found.")
        '''
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
        }        
        if table_name not in handler_map:
            logger.error("Load to DB not implemented for table", extra={"table": table_name})
            return
        handler_map[table_name](df)


    def _load_sector(self, df: pd.DataFrame) -> None:
        source_file=self.path
        sql=text(f'''
                    INSERT INTO sector (sector_id, sector_name, sector_code,source_file)
                    VALUES (:sector_id, :sector_name, :sector_code,{source_file!r})
                    ON CONFLICT (sector_id) DO UPDATE SET
                    sector_name = EXCLUDED.sector_name,
                    sector_code = EXCLUDED.sector_code,
                    source_file = EXCLUDED.source_file;''')
                    
        records=df.to_dict(orient='records')

        with self.engine.begin() as conn:
            try:
                logger.info("Inserting records", extra={
                    "table": "sector",
                    "records_count": len(records),
                    "source_file": source_file,
                })
                conn.execute(sql, records)
                logger.info("Records inserted successfully", extra={
                    "table": "sector",
                    "records_count": len(records),
                })
            except Exception as e:
                logger.error("DB insert failed", extra={
                    "table": "sector",
                    "source_file": source_file,
                    "error_type": type(e).__name__,
                    "error": str(e),
                }, exc_info=True)


    def _get_existing_sectors(self) -> set[int]:
        with self.engine.begin() as conn:
            rows=conn.execute(text("SELECT sector_id FROM sector"))
        return set(row[0] for row in rows)
    

    def _load_department(self, df: pd.DataFrame) -> None:
        source_file=self.path

        existing_sectors = self._get_existing_sectors()
        invalid = df[~df['sector_id'].isin(existing_sectors)].copy()
        valid = df[df['sector_id'].isin(existing_sectors)].copy()


        sql=text(f"""
                    INSERT INTO department (department_id, department_name,sector_id,source_file)
                    VALUES (:department_id, :department_name, :sector_id, {source_file!r})
                    ON CONFLICT (department_id) DO UPDATE SET
                        department_name = EXCLUDED.department_name,
                        sector_id = EXCLUDED.sector_id,
                        source_file = EXCLUDED.source_file;
                    """)
        

        records=valid.to_dict(orient='records')

        if not invalid.empty:
            logger.warning("Rows skipped due to missing sector_id", extra={
                "table": "department",
                "skipped_count": len(invalid),
                "invalid_sector_ids": invalid['sector_id'].unique().tolist(),
                "source_file": source_file,
            })

        with self.engine.begin() as conn:
            try:
                logger.info("Inserting records", extra={
                    "table": "department",
                    "records_count": len(records),
                    "source_file": source_file,
                })
                conn.execute(sql, records)
                logger.info("Records inserted successfully", extra={
                    "table": "department",
                    "records_count": len(records),
                })
            except Exception as e:
                logger.error("DB insert failed", extra={
                    "table": "department",
                    "source_file": source_file,
                    "error_type": type(e).__name__,
                    "error": str(e),
                }, exc_info=True)


    def _load_segment(self, df:pd.DataFrame) -> None:

        existing_sectors = self._get_existing_sectors()
        invalid = df[~df['sector_id'].isin(existing_sectors)].copy()
        valid = df[df['sector_id'].isin(existing_sectors)].copy()

        source_file=self.path
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
                "table": "segment",
                "skipped_count": len(invalid),
                "invalid_sector_ids": invalid['sector_id'].unique().tolist(),
                "source_file": source_file,
            })

        with self.engine.begin() as conn:
            try:
                logger.info("Updating segment_chief relations", extra={
                    "table": "segment_chief",
                    "records_count": len(updateRecords),
                })
                conn.execute(update_sql, updateRecords)

                logger.info("Inserting records", extra={
                    "table": "segment",
                    "records_count": len(records),
                    "source_file": source_file,
                })
                conn.execute(sql, records)

                logger.info("Inserting segment_chief relations", extra={
                    "table": "segment_chief",
                    "records_count": len(updateRecords),
                })
                conn.execute(insert_relation_sql, updateRecords)

                logger.info("Records inserted successfully", extra={
                    "table": "segment",
                    "records_count": len(records),
                })
            except Exception as e:
                logger.error("DB insert failed", extra={
                    "table": "segment",
                    "source_file": source_file,
                    "error_type": type(e).__name__,
                    "error": str(e),
                }, exc_info=True)


    def _load_chief(self, df: pd.DataFrame) -> None:
        
        sql = text(f"""
                    INSERT INTO chief (chief_id, chief_first_name, chief_last_name, email_address, phone_number,source_file)
                    VALUES (:chief_id, :chief_first_name, :chief_last_name, :chief_email, :chief_phone, :source_file)
                    ON CONFLICT (chief_id) DO UPDATE SET
                    email_address = EXCLUDED.email_address,
                    phone_number = EXCLUDED.phone_number,
                    source_file = EXCLUDED.source_file
                        ;
                    """)
        
        source_file=self.path

        today=datetime.date.today()
        df_chief=df.copy()
        df_chief['source_file']=source_file

        with self.engine.begin() as conn:
            try:
                record = df_chief.to_dict(orient='records')
                logger.info("Inserting records", extra={
                    "table": "chief",
                    "records_count": len(record),
                    "source_file": source_file,
                })
                conn.execute(sql, record)
                logger.info("Records inserted successfully", extra={
                    "table": "chief",
                    "records_count": len(record),
                })
            except Exception as e:
                logger.error("DB insert failed", extra={
                    "table": "chief",
                    "source_file": source_file,
                    "error_type": type(e).__name__,
                    "error": str(e),
                }, exc_info=True)

    
    def _load_contractor(self, df: pd.DataFrame) -> None:
        sql = text(f'''
        INSERT INTO contractor (contractor_id, contractor_name, contractor_phone_number, contractor_email_address, contractor_address, source_file, created_at,updated_at)
        VALUES (:contractor_id, :contractor_name, :contractor_phone_number, :contractor_email_address, :contractor_address, :source_file, :created_at, :updated_at)
        ON CONFLICT (contractor_id) DO UPDATE SET
        contractor_name = EXCLUDED.contractor_name,
        contractor_phone_number = EXCLUDED.contractor_phone_number,
        contractor_email_address = EXCLUDED.contractor_email_address,
        contractor_address = EXCLUDED.contractor_address
        ''')

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
        source_file=self.path
        contractor_df=df[['contractor_id', 'contractor_name', 'contractor_phone_number', 'contractor_email_address', 'contractor_address', 'contract_number']]
   
        
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
            insert_df['status']='signed'
            insert_df['is_current']=True
            insert_df['valid_from']=datetime.datetime.now()
            insert_df['valid_to']=None
            insert_df['source_file']=source_file

            insert_df=insert_df.to_dict(orient='records')
            update_contract_df=update_contract_df.to_dict(orient='records')


        with self.engine.begin() as conn:
            try:
                if 'contract_number' in df.columns:
                    logger.info("Updating contract relations", extra={
                        "table": "contract",
                        "records_count": len(update_contract_df),
                        "source_file": source_file,
                    })
                    conn.execute(update_contract_sql, update_contract_df)

                    logger.info("Inserting contract records", extra={
                        "table": "contract",
                        "records_count": len(insert_df),
                        "source_file": source_file,
                    })
                    conn.execute(insert_contract_sql, insert_df)

                logger.info("Inserting records", extra={
                    "table": "contractor",
                    "records_count": len(contractor_df),
                    "source_file": source_file,
                })
                conn.execute(sql, contractor_df)
                logger.info("Records inserted successfully", extra={
                    "table": "contractor",
                    "records_count": len(contractor_df),
                })
            except Exception as e:
                logger.error("DB insert failed", extra={
                    "table": "contractor",
                    "source_file": source_file,
                    "error_type": type(e).__name__,
                    "error": str(e),
                }, exc_info=True)


    def _get_existing_art_keys(self) -> set[int]:
        with self.engine.begin() as conn:
            rows = conn.execute(text("SELECT art_key FROM product"))
        return set(row[0] for row in rows)

    def _get_current_pos_information(self, art_keys: set) -> pd.DataFrame:
        """Bieżące wiersze pos_information (date_end IS NULL) dla podanych art_key."""
        if not art_keys:
            return pd.DataFrame(columns=["art_key", "ean", "price_net", "price_gross", "vat_rate"])
        with self.engine.begin() as conn:
            placeholders = ",".join(str(k) for k in art_keys)
            q = text(
                "SELECT art_key, ean, price_net, price_gross, vat_rate "
                "FROM pos_information WHERE date_end IS NULL AND art_key IN (" + placeholders + ")"
            )
            rows = conn.execute(q)
            return pd.DataFrame(
                rows.fetchall(),
                columns=["art_key", "ean", "price_net", "price_gross", "vat_rate"],
            )

    def _get_existing_contractor_ids(self) -> set[int]:
        with self.engine.begin() as conn:
            rows = conn.execute(text("SELECT contractor_id FROM contractor"))
        return set(row[0] for row in rows)

    def _get_existing_segment_ids(self) -> set[int]:
        with self.engine.begin() as conn:
            rows = conn.execute(text("SELECT segment_id FROM segment"))
        return set(row[0] for row in rows)

    def _get_existing_department_ids(self) -> set[int]:
        with self.engine.begin() as conn:
            rows = conn.execute(text("SELECT department_id FROM department"))
        return set(row[0] for row in rows)

    def _load_pos_information(self, df: pd.DataFrame) -> None:
        """Wstawia tylko wiersze nowe lub ze zmienioną ceną. Ten sam (art_key, ean) i ta sama cena = pomijamy."""
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

        source_file = self.path
        existing_art_keys = self._get_existing_art_keys()
        valid = df[df["art_key"].isin(existing_art_keys)].copy()

        if valid.empty:
            return

        # Pobierz bieżące ceny z bazy – wstawiamy tylko gdy (art_key, ean) nowe lub cena się zmieniła
        current = self._get_current_pos_information(set(valid["art_key"]))
        current = current.rename(columns={
            "price_net": "price_net_db",
            "price_gross": "price_gross_db",
            "vat_rate": "vat_rate_db",
        })
        merged = valid.merge(current, on=["art_key", "ean"], how="left")

        # Ten sam (art_key, ean) i ta sama cena = pomijamy (nie wstawiamy ponownie)
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
            return

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

        with self.engine.begin() as conn:
            try:
                if update_rows:
                    logger.info("Closing outdated pos_information records", extra={
                        "table": "pos_information",
                        "records_count": len(update_rows),
                        "source_file": source_file,
                    })
                    conn.execute(update_sql, update_rows)

                logger.info("Inserting new pos_information records", extra={
                    "table": "pos_information",
                    "records_count": len(valid_df),
                    "source_file": source_file,
                })
                conn.execute(insert_sql, valid_df)
                logger.info("Records inserted successfully", extra={
                    "table": "pos_information",
                    "records_count": len(valid_df),
                })
            except Exception as e:
                logger.error("DB insert failed", extra={
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
        if df.empty:
            logger.warning("No product records to load", extra={
                "table": "product",
                "reason": "DataFrame is empty",
            })
            return

        sql = text("""
                   INSERT INTO product (art_key, art_number, contractor_id, segment_id, department_id,
                                        brand, article_codification_date, last_modified_at, source_file)
                   VALUES (:art_key, :art_number, :contractor_id, :segment_id, :department_id,
                           :brand, :article_codification_date, :last_modified_at, :source_file)
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
                "table": "product",
                "skipped_count": len(invalid),
                "source_file": self.path
            })
        if valid.empty:
            logger.warning("No valid product records to insert", extra={
                "table": "product",
                "source_file": self.path,
            })
            return

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
                    "table": "product",
                    "records_count": len(records),
                    "source_file": self.path,
                })
                conn.execute(sql, records)
                logger.info("Records inserted successfully", extra={
                    "table": "product",
                    "records_count": len(records),
                })
            except Exception as e:
                logger.error("DB insert failed", extra={
                    "table": "product",
                    "source_file": self.path,
                    "error_type": type(e).__name__,
                    "error": str(e),
                }, exc_info=True)
                raise

# sector -> ok
# department -> ok
# #
# path='/Users/radoslaw/Desktop/Engineering-project/apps/load_file_develop/data/generated_masterdata_part1_v3/pos_information.csv'
# process_data=process_data(path)
# df_pos_information, errors=process_data.validate_shape(PosInformation)
# # print(errors)
# process_data.load_to_db(df_pos_information, 'pos_information')



