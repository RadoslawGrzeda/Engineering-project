from typing import TypeVar
import csv
import time
from typing import Type
import streamlit as st
import streamlit_authenticator as stauth
import os
import io
from dotenv import load_dotenv
from yaml import safe_load
from datetime import datetime
from schemas import Chief, Segment, Sector, Department, Category, DepartmentSectors
from pydantic import BaseModel, ValidationError
import pandas as pd
from minIOClient import MinioClient
import structlog
from streamlit_authenticator import Authenticate


class streamLit_app:
    def __init__(self): 
        load_dotenv()
        self.client=MinioClient()
        self.T=TypeVar('T', bound=BaseModel)

    def init_config(self,yaml_path:str) -> dict:
        """Method for initialization a value from config file
        and replace placeholder with correct and secrets informations """
        with open(yaml_path,'r') as f:
            yaml_content = f.read()
            yaml_content = yaml_content.replace('COOKIE_KEY_PLACEHOLDER', os.getenv('API_KEY'))
            yaml_content = yaml_content.replace('USER_NAME_PLACEHOLDER', os.getenv('name'))
            yaml_content = yaml_content.replace('USER_PASSWORD_PLACEHOLDER', os.getenv('password'))
            yaml_content = yaml_content.replace('ADMIN_PASSWORD_PLACEHOLDER', os.getenv('admin_pass'))
            yaml_content = yaml_content.replace('ADMIN_NAME_PLACEHOLDER', os.getenv('admin_name'))
        return safe_load(yaml_content)


    def check_uploaded_file_columns(self,uploaded_file, schema: Type[TypeVar]) -> bool:
        '''
        Method used for test uploaded file before its was sent into minIo bucket,
        if it not contains correct specified columns the process is blocked
        '''
        try:
            df = pd.read_csv(uploaded_file)
        except Exception as e:
            return False

        df_cols = set(df.columns)
        model_cols = set(schema.model_fields.keys())

        if df_cols == model_cols:
            return True
        else:
            st.warning('eyo')
            return False

    def authentification(self,config:dict):
        authenticator = stauth.Authenticate(
        config['credentials'],
        config['cookie']['name'],
        config['cookie']['key'],
        config['cookie']['expiry_days'],
    # config['preauthorized']
        )
        return authenticator

# config=init_config('config.yaml')

# authenticator.login('main')
# structlog.configure(
#     processors=[
#         structlog.processors.TimeStamper(fmt="iso", utc=True),
#         structlog.processors.JSONRenderer(),
#     ],
# )
# log = structlog.get_logger()


    def run(self,authenticator:Authenticate):
        if st.session_state['authentication_status']:
            authenticator.logout(location='sidebar')

            st.title('''
                Witaj na stronie do ladowania plikow :)
            ''')
            name = st.session_state.get("name", "")
            roles = st.session_state.get("roles", [])
            st.info(f"Zalogowany jako {name} {roles}")

            database=st.radio('Temat przewodni ',['sklep','produkt'])
            if database=='sklep':
                text=st.selectbox('Jaki plik ladujesz',['test','cos'])
            elif database== 'produkt':
                text=st.selectbox('Plik: ',['chief','segment','sector','department','pos_information','contractor','product'])

            uploaded_file=st.file_uploader('Pick a file',type=['csv'],
            key=f'uploaded_file_{st.session_state.uploaded_file_key}')


            slownik = {
                'chief': Chief,
                'segment': Segment,
                'sector': Sector,
                'department': Department,  
                # 'brand': Brand,
                # 'pos_information': PosInformation,
                # 'contractor': Contractor,
            }


            if uploaded_file is not None:
                if streamLitApp.check_uploaded_file_columns(uploaded_file, slownik[text]):

                    st.success('Dane sa poprawne')
                else:
                    st.error('Dane sa niepoprawne')
                    st.stop()

                placeholder=st.empty()
                # placeholder.info(f'Wybrano plik {uploaded_file.name}')
                if st.button('Wyslij'):
                    try:
                        # filename=uploaded_file.name+'_'+datetime.now().strftime('%Y%m%d%S')
                        filename=f"{datetime.now().strftime('%Y%m%d')}_{uploaded_file.name}"
                        upData=uploaded_file.getvalue()
                        if database =='sklep' and self.client.bucket_exists('sklep'):
                            text=self.client.upload_file(st.session_state['name'],database,filename,io.BytesIO(upData),len(upData),content_type='application/csv')
                        elif database =='produkt' and self.client.bucket_exists('produkt'):
                            text=self.client.upload_file(st.session_state['name'],database,filename,io.BytesIO(upData),len(upData),content_type='application/csv')
                            placeholder.info(text)
                        else:
                            self.client.make_bucket(st.session_state['name'],database)
                            self.client.upload_file(st.session_state['name'],database,filename,io.BytesIO(upData),len(upData),content_type='application/csv')
                        # uploaded_file.delete()
                        # del uploaded_file
                        placeholder.success(text)
                        time.sleep(3)
                    
                        st.session_state.uploaded_file_key+=1
                        st.rerun()
                    except Exception as e:
                        print(e)

        elif st.session_state['authentication_status'] == False:
            st.error('Błedne dane logowania')
        elif st.session_state['authentication_status'] == None:
            st.warning('Wprowadz dane logowania')

        # if auth_status:

        #     authenticator.logout('Logout','sidebar')

if __name__ == '__main__':

    yaml_path='config.yaml'
    if 'uploaded_file_key' not in st.session_state:
        st.session_state.uploaded_file_key=0
    streamLitApp=streamLit_app()
    ymlConfig=streamLitApp.init_config(yaml_path)
    authenticator=streamLitApp.authentification(ymlConfig)
    authenticator.login('main')

    streamLitApp.run(authenticator)

