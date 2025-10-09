import os
import zipfile
import oracledb
import logging
from google.cloud import storage
from google.oauth2.service_account import Credentials
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import pytz
from datetime import datetime, timedelta

# 1. Timezone 설정 : 서울
tz = pytz.timezone('Asia/Seoul')


# 2. 기준일자 설정
"""
new_date : 오늘 일자(yyyy-mm-dd)
old_date : 오늘로부터 7일 전(yyyy-mm-dd)
"""
now = datetime.now(tz)
new_date = now.strftime('%Y%m%d')
old_date = (now - timedelta(days=7)).strftime('%Y%m%d')

logging.basicConfig(level = logging.INFO)
logger = logging.getLogger(__name__)

wallet_location = os.path.join(os.getcwd(), 'key')
STORAGE_NAME = os.getenv('STORAGE_NAME')
WALLET_FILE = os.getenv('WALLET_FILE')

if not os.path.exists(WALLET_FILE) :
    drive = {
        "type": os.getenv('GCP_TYPE'),
        "project_id": os.getenv('GCP_PROJECT_ID'),
        "private_key_id": os.getenv('GCP_PRIVATE_KEY_ID'),
        "private_key": os.getenv('GCP_PRIVATE_KEY').replace('\\n', '\n'),
        "client_email": os.getenv('GCP_CLIENT_EMAIL'),
        "client_id": os.getenv('GCP_CLIENT_ID'),
        "auth_uri": os.getenv('GCP_AUTH_URI'),
        "token_uri": os.getenv('GCP_TOKEN_URI'),
        "auth_provider_x509_cert_url": os.getenv('GCP_PROVIDER_URL'),
        "client_x509_cert_url": os.getenv('GCP_CLIENT_URL'),
        "universe_domain": os.getenv('GCP_UNIV_DOMAIN')
    }

    credentials = Credentials.from_service_account_info(drive)
    client = storage.Client(credentials=credentials)
    bucket = client.get_bucket(STORAGE_NAME)
    blob = bucket.get_blob(WALLET_FILE)
    blob.download_to_filename(WALLET_FILE)

    zip_file_path = os.path.join(os.getcwd(), WALLET_FILE)

    if not os.path.exists(wallet_location):
        os.makedirs(wallet_location, exist_ok=True)
    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        zip_ref.extractall(wallet_location)

pool = oracledb.create_pool(
    user=os.getenv('DB_USER'),
    password=os.getenv('DB_PASSWORD'),
    dsn=os.getenv('DB_DSN'),
    config_dir=wallet_location,
    wallet_location=wallet_location,
    wallet_password=os.getenv('DB_WALLET_PASSWORD'),
    min=1, max = 5, increment=1)

def get_connection():
    connection = pool.acquire()
    logger.info('Database connection acquired')
    return connection

engine = create_engine('oracle+oracledb://',
                           pool_pre_ping=True,
                           creator=get_connection)
SessionLocal = sessionmaker(bind=engine)

def get_db():
    db = SessionLocal()
    logger.info('Database connected')
    try:
        yield db
    except oracledb.DatabaseError as e :
        logger.error(f'Database Connection failed : {e}')
        raise
    finally:
        db.close()
        logger.info('Database connection closed')

telegramConfig = {
    '주식 급등일보🚀급등테마·대장주 탐색기': 'https://t.me/s/FastStockNews'
    , '핀터 - 국내공시 6줄 요약': 'https://t.me/s/finter_gpt'
    , 'AWAKE-일정, 테마, 이벤트드리븐': 'https://t.me/s/awake_schedule'
    , '52주 신고가 모니터링': 'https://t.me/s/awake_realtimeCheck'
    , 'SB 리포트 요약': 'https://t.me/s/stonereport'

}
