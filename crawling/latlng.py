import logging
from typing import Optional, Dict, Tuple
from sqlalchemy import Engine, create_engine
from dotenv import load_dotenv
from tqdm import tqdm


import pandas as pd

import requests

import os

logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def get_db_engine() -> Optional[Engine]:
    """환경변수를 로드하고 SQLAlchemy DB 엔진을 생성합니다."""
    load_dotenv()
    try:
        db_url = (
            f"postgresql+psycopg2://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}"
            f"@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
        )
        engine = create_engine(db_url)
        logging.info("데이터베이스 엔진 생성 성공.")
        return engine
    except Exception as e:
        logging.critical(f"DB 엔진 생성 실패: {e}")
        return None
    
def fetch_data_from_db(engine: Engine, table_name: str, company_col: str) -> pd.DataFrame:
    """데이터베이스에서 상호명이 담긴 데이터를 불러옵니다."""
    try:
        query = f'SELECT "{company_col}" FROM "{table_name}"'
        df = pd.read_sql_query(query, engine)
        logging.info(f"'{table_name}' 테이블에서 {len(df)}개의 데이터를 불러왔습니다.")
        return df
    except Exception as e:
        logging.error(f"DB에서 데이터 로딩 실패: {e}")
        return pd.DataFrame()
    
def get_place_info_from_naver(client_id: str, client_secret: str, company_name: str) -> Optional[Dict]:
    """네이버 지역 검색 API로 장소 정보를 검색합니다."""
    headers = {
        "X-Naver-Client-Id": client_id,
        "X-Naver-Client-Secret": client_secret,
    }
    params = {"query": company_name, "display": 1} # 가장 정확한 1개 결과만 요청
    url = "https://openapi.naver.com/v1/search/local.json"
    
    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status() # HTTP 에러 발생 시 예외 처리
        search_results = response.json()
        
        if search_results['items']:
            return search_results['items'][0] # 첫 번째 결과 반환
    except requests.exceptions.RequestException as e:
        logging.warning(f"'{company_name}' 지역 검색 API 호출 실패: {e}")
    return None

def get_coords_from_naver(client_id: str, client_secret: str, address: str) -> Optional[Tuple[float, float]]:
    """네이버 지오코딩 API로 주소를 위도/경도로 변환합니다."""
    headers = {
        "X-Naver-Client-Id": client_id,
        "X-Naver-Client-Secret": client_secret,
    }
    params = {"query": address}
    url = "https://openapi.naver.com/v1/map/geocode"

    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        geocode_results = response.json()

        if geocode_results.get("addresses"):
            addr_info = geocode_results["addresses"][0]
            # 네이버 지오코딩은 경도(x)가 먼저, 위도(y)가 나중에 옵니다.
            return (float(addr_info['y']), float(addr_info['x'])) # (위도, 경도) 순으로 반환
    except requests.exceptions.RequestException as e:
        logging.warning(f"'{address}' 지오코딩 API 호출 실패: {e}")
    return None

def get_lat_lng_and_add_dataframe():
    DB_TABLE_NAME = "campaign"      # 데이터를 가져올 테이블 이름
    COMPANY_COLUMN_NAME = "company"         # 상호명이 들어있는 컬럼 이름
    RESULT_TABLE_NAME = "review_campaigns_enriched" # 결과를 저장할 새 테이블 이름
    RESULT_CSV_PATH = "result_enriched.csv" # 결과를 저장할 CSV 파일 경로

    db_engine = get_db_engine()
    if not db_engine:
        exit()
    

    naver_client_id = os.getenv("NAVER_CLIENT_ID")
    naver_client_secret = os.getenv("NAVER_CLIENT_SECRET")
    if not (naver_client_id and naver_client_secret):
        logging.critical("환경변수에서 NAVER_CLIENT_ID와 NAVER_CLIENT_SECRET를 찾을 수 없습니다.")
        exit()

    original_df = fetch_data_from_db(db_engine, DB_TABLE_NAME, COMPANY_COLUMN_NAME)

    if not original_df.empty:
        results = []

        for company in tqdm(original_df[COMPANY_COLUMN_NAME], desc="상호 정보 검색 중"):
            place_info = get_place_info_from_naver(naver_client_id, naver_client_secret, company)

            if place_info:
                address = place_info.get('roadAddress', place_info.get('address'))
                thumbnail = place_info.get('link') # link 필드에 썸네일 정보가 포함된 경우가 많음
                coords = get_coords_from_naver(naver_client_id, naver_client_secret, address) if address else None


                results.append({
                    'address': address,
                    'lat': coords[0] if coords else None,
                    'lng': coords[1] if coords else None,
                    'thumbnail': thumbnail
                })
            else:
                # 검색 결과가 없는 경우
                results.append({'address': None, 'lat': None, 'lng': None, 'thumbnail': None})
        
        fatched_df = pd.DataFrame(results)
        final_df = pd.concat([original_df.reset_index(drop=True), fatched_df], axis=1)

        # 결과 저장
        logging.info(f"'{RESULT_TABLE_NAME}' 테이블에 결과 저장을 시도합니다.")
        final_df.to_sql(RESULT_TABLE_NAME, db_engine, if_exists='replace', index=False)

        logging.info(f"'{RESULT_CSV_PATH}' 파일에 결과 저장을 시도합니다.")
        final_df.to_csv(RESULT_CSV_PATH, index=False, encoding='utf-8-sig')

        logging.info("모든 작업이 완료되었습니다.")
        print("\n--- 최종 결과 (상위 5개) ---")
        print(final_df.head())

        

