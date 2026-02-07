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
from shema import Segment, Sector, Department
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
        # self.connection_string = os.getenv("postgress_connection")
        # self.engine = sa.create_engine(self.connection_string)
    
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
        
        # duplicates = set()
        # seen = set()
        # for s in records:
        #     if s.model_dump()[0] in seen:
        #         duplicates.add(s.model_dump()[0])
        #     else:
        #         seen.add(s.model_dump()[0])

        # if duplicates:
        #     logger.warning(f"Duplicate segment_id found: {sorted(duplicates)[:20]} (count={len(duplicates)})")
        # else:
        #     logger.info("No duplicate segment_id found.")

        df=pd.DataFrame([s.model_dump() for s in records])
        return df, errors

        
    def load_to_db(self, df: pd.DataFrame, table_name: str):
        handler_map = {
            'chief': self._load_chief,
            'sector': self._load_sector,
            'department': self._load_department,
            'segment': self._load_segment,
        }
                  
        
        if table_name not in handler_map:
            logger.error(f"Load to DB not implemented for table: {table_name}")
            return
        handler_map[table_name](df)

    def _load_sector(self, df: pd.DataFrame) -> None:
        sql=text(f"""
                    INSERT INTO sector (sector_id, sector, sector_code)
                    VALUES (:sector_id, :sector, :sector_code)
                    ON CONFLICT (sector_id) DO UPDATE SET
                        sector = EXCLUDED.sector,
                        sector_code = EXCLUDED.sector_code;
                    """)
                    
        records=df.to_dict(orient='records')

        with self.engine.begin() as conn:
            try: 
                logging.info(f"Inserting records into sector...")
                conn.execute(sql, records)
                logging.info(f"Inserted sector records successfully.")
            except Exception as e:
                logger.error(f"Error inserting records into sector: {e}")

    def _load_department(self, df: pd.DataFrame) -> None:
          sql=text(f"""
                    INSERT INTO department (department_id, department_name)
                    VALUES (:department_id, :department_name)
                    ON CONFLICT (department_id) DO UPDATE SET
                        department_name = EXCLUDED.department_name;
                    """)
          
          records=df.to_dict(orient='records')

          with self.engine.begin() as conn:
                try: 
                    logging.info(f"Inserting records into department...")
                    conn.execute(sql, records)
                    logging.info(f"Inserted department records successfully.")
                except Exception as e:
                    logger.error(f"Error inserting records into department: {e}")

    def _load_segment(self,pd:pd.DataFrame) -> None:
        sql=text(f"""
                    INSERT INTO segment (segment_id, segment, segment_code, sector_id)
                    VALUES (:segment_id, :segment, :segment_code, :sector_id)
                    ON CONFLICT (segment_id) DO UPDATE SET
                        segment = EXCLUDED.segment,
                        segment_code = EXCLUDED.segment_code,
                        sector_id = EXCLUDED.sector_id;
                    """)

        records=pd.to_dict(orient='records')
        with self.engine.begin() as conn:
            try: 
                logging.info(f"Inserting records into segment...")
                conn.execute(sql, records)
                logging.info(f"Inserted segment records successfully.")
            except Exception as e:
                logger.error(f"Error inserting records into segment: {e}")

'''
    def _load_chief(self, df: pd.DataFrame) -> None:
        sqlUpdate=text(f"""
                    UPDATE segment_chief SET
                    is_current = False,
                    date_end = :today
                    Where 
                    segment_id = :segment_id
                    AND
                    chief_id <> :chief_id AND is_current = TRUE
                    """)
        ## segment_id, chief_id, is_current, date_start, date_end
        sqlINSERT = text(f"""
                    INSERT INTO segment_chief (segment_id, chief_id, is_current, date_start, date_end)
                    SELECT :segment_id, :chief_id, :is_current, :date_start, :date_end
                    Where not exists ( select 1 
                    from segment_chief sc
                    where sc.chief_id=:chief_id and sc.segment_id = :segment_id and sc.is_current=TRUE
                    );
                    """)
                
        ## chief_id, chief_first_name, chief_last_name, chief_email, chief_phone
        sql = text(f"""
                    INSERT INTO chief (chief_id, chief_first_name, chief_last_name, chief_email, chief_phone)
                    VALUES (:chief_id, :chief_first_name, :chief_last_name, :chief_email, :chief_phone)
                    ON CONFLICT (chief_id) DO UPDATE SET
                    chief_first_name = EXCLUDED.chief_first_name,
                    chief_last_name = EXCLUDED.chief_last_name,
                    chief_email = EXCLUDED.chief_email,
                    chief_phone = EXCLUDED.chief_phone
                    ;
                    """)
        
        today=datetime.date.today()
        
        df_chief=df[['chief_id', 'chief_first_name', 'chief_last_name', 'chief_email', 'chief_phone']].copy()
        df_chief=df_chief.drop_duplicates(subset=['chief_id'], keep='last')
        
        df_segment_chief=df[['segment_id', 'chief_id','is_current','date_start','date_end']].copy()
        df_segment_chief=df_segment_chief.drop_duplicates(subset=['segment_id'], keep='last')

        with self.engine.begin() as conn:
            try: 
                logging.info(f"Inserting records into chief...")

                record=df_chief.to_dict(orient='records')
                conn.execute(sql, record)
                logging.info(f"Inserted chief records successfully.")
                
                ## update segment_chief
                conn.execute(sqlUpdate, [{'today': today, **rec} for rec in df_segment_chief.to_dict(orient='records')])
                logging.info(f"Updated segment_chief records successfully, start insertion into segment_chief...")
                
                ## segment_chief
                record_segment_chief=df_segment_chief.to_dict(orient='records')                                
                conn.execute(sqlINSERT, record_segment_chief)
                logging.info(f"Inserted record_segment_chief  successfully.")

            except Exception as e:
                logger.error(f"Error inserting records into chief: {e}")




chief_path='/Users/radoslaw/Desktop/Python/products/chief.csv'
# chief_class=processData(chief_path)
# df_chief, errors=chief_class.validate_shape(Chief)
# chief_class.load_to_db(df_chief, 'chief')
segment_path='/Users/radoslaw/Desktop/Python/products/segment.csv'
segment_class=processData(segment_path)
segments, errors=segment_class.validate_shape(Segment)
segment_class.load_to_db(segments, 'segment')
# if errors:
#     for line_no, err in errors:
#         logger.error(f"Line {line_no}: {err}")
# else:
    # logger.info("All records are valid.")

# print(segments)
'''