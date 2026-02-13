from hmac import new
import pandas as pd 
import csv
import logging
from typing import List, Tuple
from pydantic import ValidationError    
from typing import Type, TypeVar,Generic
from pydantic import BaseModel
import sqlalchemy as sa
from sqlalchemy import text
import psycopg2
from dotenv import load_dotenv
import datetime
import os
from shema import Segment, Sector, Department, Chief, PosInformation, Product
load_dotenv()


T=TypeVar('T', bound=BaseModel)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    )
logger = logging.getLogger(__name__)

class process_data:
    def __init__ (self, path: str):
        self.path = path
        self.connection_string = os.getenv("postgress_connection")
        self.engine = sa.create_engine(self.connection_string)
    

    def validate_shape(self, shape:Type[T]):
        records:   List[T] = []
        errors: List[Tuple[int,str]] = []

        with open(self.path, 'r', encoding='UTF-8', newline='') as f:
            try:
                reader = csv.DictReader(f)
                logger.info(f"Validating records from {self.path} starting...")
                
                for line_no, row in enumerate(reader, start=2):
                    try:
                        records.append(shape.model_validate(row))
                    except ValidationError as e:
                        errors.append((line_no, row, e.json()))
                    except Exception as e:
                        logger.error(f"Unexpected error at line {line_no} with row {row}: {e}")
            except Exception as e:
                logger.error(f"Failed to read file {self.path}: {e}")
        logging.info(f"Validation completed. Total records: {len(records)}")

        if errors:
            logger.error(f"Validation errors found: {len(errors)}, {[i for  i in errors]}")

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
        }        
        if table_name not in handler_map:
            logger.error(f"Load to DB not implemented for table: {table_name}")
            return
        handler_map[table_name](df)


    def _load_sector(self, df: pd.DataFrame) -> None:
        source_file=self.path
        sql=text(f"""
                    INSERT INTO sector (sector_id, sector_name, sector_code,source_file)
                    VALUES (:sector_id, :sector_name, :sector_code,{source_file!r})
                    ON CONFLICT (sector_id) DO UPDATE SET
                        sector_name = EXCLUDED.sector_name,
                        sector_code = EXCLUDED.sector_code,
                        source_file = EXCLUDED.source_file;
                    """)
                    
        records=df.to_dict(orient='records')

        with self.engine.begin() as conn:
            try: 
                logging.info(f"Inserting records into sector...")
                conn.execute(sql, records)
                logging.info(f"Inserted sector records successfully.")
            except Exception as e:
                logger.error(f"Error inserting records into sector: {e}")


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

        if not valid.empty: 
            logger.warning(f"Invalid sector_id found: {invalid['sector_id'].unique()}")

        with self.engine.begin() as conn:
                try: 
                    logging.info(f"Inserting records into department...")
                    conn.execute(sql, records)
                    logging.info(f"Inserted department records successfully.")
                except Exception as e:
                    logger.error(f"Error inserting records into department: {e}")


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

        if not valid.empty: 
            logger.warning(f"Invalid sector_id found: {invalid['sector_id'].unique()}")
            
        with self.engine.begin() as conn:
            try: 
                logging.info(f"Updating records in segment_chief...")
                conn.execute(update_sql, updateRecords)
                logging.info(f"Updated segment_chief records successfully.")

                logging.info(f"Inserting records into segment...")
                conn.execute(sql, records)
                logging.info(f"Inserted segment records successfully.")

                logging.info(f"Inserting records into segment_chief...")
                conn.execute(insert_relation_sql, updateRecords)
                logging.info(f"Inserted segment_chief records successfully.")
            except Exception as e:
                logger.error(f"Error inserting records into segment: {e}")


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
                logging.info(f"Inserting records into chief...")
                record=df_chief.to_dict(orient='records')
                conn.execute(sql, record)
                logging.info(f"Inserted chief records successfully.")

            except Exception as e:
                logger.error(f"Error inserting records into chief: {e}")

    
    def _load_contractor(self, df: pd.DataFrame) -> None:
        sql = text(f'''
        INSERT INTO contractor (contractor_id, contractor_name, contractor_phone_number, contractor_email, contractor_address, source_file,created_at,updated_at)
        VALUES (:contractor_id, :contractor_name, :contractor_phone_number, :contractor_email, :contractor_address, :source_file, :created_at, :updated_at)
        ON CONFLICT (contractor_id) DO UPDATE SET
        contractor_name = EXCLUDED.contractor_name,
        contractor_phone_number = EXCLUDED.contractor_phone_number,
        contractor_address = EXCLUDED.contractor_address,
        contractor_email = EXCLUDED.contractor_email,
        ''')

        if 'contract_number' in df.columns:
            contract_sql = text(f''' 
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
            INSERT INTO contract (contractor_id,contract_number, signed_at,status,is_current,valid_from,valid_to,source_file,)
            VALUES (:contractor_id, :contract_number, :signed_at, :status, :is_current, :valid_from, :valid_to, :source_file, :created_at, :updated_at)
            ON CONFLICT (contractor_id,contract_number) DO UPDATE SET
            signed_at = EXCLUDED.signed_at,
            status = EXCLUDED.status,
            valid_from = EXCLUDED.valid_from,
            ''')
        source_file=self.path
        # contractor_id,contractor_name,contact_phone_number,contact_phone_email,address,contract_number
        contractor_df=df[['contractor_name', 'contact_phone_number', 'contact_phone_email', 'address']].copy()
        for record in contractor_df:
            record['created_at']=datetime.datetime.now()
            record['updated_at']=datetime.datetime.now()
            record['source_file']=source_file
        contractor_df=contractor_df.to_dict(orient='records')

        if 'contract_number' in df.columns:
            update_contract_df=df[['contractor_id', 'contract_number']].copy()
            update_contract_df['source_file']=source_file
            update_contract_df=update_contract_df.to_dict(orient='records')

            insert_df=update_contract_df.copy()
            insert_df['signed_at']=datetime.datetime.now()
            insert_df['status']='active'
            insert_df['is_current']=True
            insert_df['valid_from']=datetime.datetime.now()
            insert_df['valid_to']=None
            insert_df['source_file']=source_file
            insert_df=insert_df.to_dict(orient='records')



        update_contract_df=update_contract_df.to_dict(orient='records')



        df['created_at']=datetime.datetime.now()
        df['updated_at']=datetime.datetime.now()
        df['source_file']=source_file
        records=df.to_dict(orient='records')

        with self.engine.begin() as conn:
            try:
                logging.info(f"Inserting records into contractor...")
                conn.execute(sql, records)
                logging.info(f"Inserted contractor records successfully.")
            except Exception as e:
                logger.error(f"Error inserting records into contractor: {e}")


    def _load_pos_information(self, df:pd.DataFrame) -> None:

        update_sql = text(f'''
            UPDATE  pos_information
            SET date_end = current_date,
            is_current = False,
            last_modified_date=current_date,
            source_file=:source_file
            where art_key=:art_key and ean != :ean 
            and date_end is null;    
            '''
        )

        insert_sql = text(f'''
            INSERT INTO pos_information( art_key, ean, vat_rate, price_net,
                                        price_gross,date_start,last_modified_date,source_file,is_current)
            values (:art_key, :ean, :vat_rate, :price_net, :price_gross, :date_start, :last_modified_date, :source_file, :is_current
            )
            on conflict (art_key, ean) do update set 
            vat_rate = :vat_rate,
            price_net = :price_net,
            price_gross = :price_gross,
            source_file= :source_file,
            last_modified_date=current_date,
            is_current= :is_current
        ''')

        source_file=self.path
        
        df=df[['art_key', 'ean', 'vat_rate', 'price_net', 'price_gross']]
        new_df=df.copy().to_dict(orient='records')
        
        for record in new_df:
            record['date_start']=datetime.date(2023,1,1)
            record['source_file']=source_file
            record['last_modified_date']=datetime.datetime.now()
            record['date_end']=None
            record['is_current']=True
    
        update_rows=df.copy().to_dict(orient='records')
        for record in update_rows:
            record['source_file']=source_file
            record['date_end']=datetime.date.today()
            record['is_current']=False
            record['last_modified_date']=datetime.datetime.now()


        with self.engine.begin() as conn:
            try:
                logging.info(f"Updating records in pos_information...")
                conn.execute(update_sql, update_rows)
                logging.info(f"Updated pos_information records successfully.")

                logging.info(f"Inserting records into pos_information...")
                conn.execute(insert_sql, new_df)
                logging.info(f"Inserted pos_information records successfully.")

            except Exception as e:
                logger.error(f"Error inserting records into pos_information: {e}")


    def _load_product(self, df:pd.DataFrame) -> None:
        sql = text(f'''
        INSERT INTO product ( art_key, art_number, contractor_id, segment_id, department_id, brand, article_codification_date, last_modified_at, source_file)
        VALUES (:art_key, :art_number, :contractor_id, :segment_id, :department_id, :brand, :article_codification_date, :last_modified_at, :source_file)
        ON CONFLICT (art_key) DO UPDATE SET
        art_number = EXCLUDED.art_number,
        brand = EXCLUDED.brand,
        article_codification_date = EXCLUDED.article_codification_date,
        last_modified_at = EXCLUDED.last_modified_at,
        source_file = EXCLUDED.source_file,
        ''')
        source_file=self.path()
        # product_key,art_key,art_number,art_name,segment_id,departament_id,contractor_id,brand,pos_information_id,
        # article_codification_date,is_current,start_date,end_date
        new_df=df[['art_key', 'art_number', 'art_name', 'segment_id', 'departament_id', 'contractor_id', 'brand', 'pos_information_id', 'article_codification_date']].copy()
        new_df=new_df.to_dict(orient='records')

        sectors=self._get_existing_sectors
        valid=new_df[new_df['sector_id'].isin(sectors)].copy()
        invalid=new_df[~new_df['sector_id'].isin(sectors)].copy()





        for record in valid:
            record['source_file']=source_file
            record['last_modified_at']=datetime.datetime.now()

        with self.engine.begin() as conn:
            try:
                logging('Starting inserting value into product')
                conn.execute(sql,valid)

                logging('Successfully end inserting value into product')

            except Exception as e:
                logging(f"Error with inserting value into product{e}")
                
                
                
                
                
# path='/Users/radoslaw/Desktop/Engineering-project/apps/load_file_develop/data/generated_masterdata_part1_v3/pos2.csv'
# process_data=process_data(path)
# df_pos_information, errors=process_data.validate_shape(PosInformation)
# process_data.load_to_db(df_pos_information, 'pos_information')
path='/Users/radoslaw/Desktop/Engineering-project/apps/load_file_develop/data/generated_masterdata_part1_v3/prod_test.csv'
process_data=process_data(path)
df_product, errors=process_data.validate_shape(Product)
process_data.load_to_db(df_product, 'product')
# print(errors)



