import logging
from typing import Optional, Dict, Tuple
from sqlalchemy import Engine, create_engine
from dotenv import load_dotenv
from tqdm import tqdm
import time

from sqlalchemy import text


import pandas as pd

import requests

import os

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
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


def fetch_data_from_db(
    engine: Engine, table_name: str, company_col: str, id_col: str = "id"
) -> pd.DataFrame:
    """데이터베이스에서 ID와 상호명이 담긴 데이터를 불러옵니다."""
    try:
        # id 컬럼과 company 컬럼을 함께 조회
        query = f'SELECT "{id_col}", "{company_col}" FROM "{table_name}"'
        df = pd.read_sql_query(query, engine)
        logging.info(f"'{table_name}' 테이블에서 {len(df)}개의 데이터를 불러왔습니다.")
        return df
    except Exception as e:
        logging.error(f"DB에서 데이터 로딩 실패: {e}")
        return pd.DataFrame()


def get_place_info_from_naver(
    client_id: str, client_secret: str, company_name: str
) -> Optional[Dict]:
    """네이버 지역 검색 API로 장소 정보를 검색합니다."""
    headers = {
        "X-Naver-Client-Id": client_id,
        "X-Naver-Client-Secret": client_secret,
    }
    params = {"query": company_name, "display": 1}  # 가장 정확한 1개 결과만 요청
    url = "https://openapi.naver.com/v1/search/local.json"

    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()  # HTTP 에러 발생 시 예외 처리
        search_results = response.json()
        if search_results["items"]:
            return search_results["items"][0]  # 첫 번째 결과 반환
    except requests.exceptions.RequestException as e:
        logging.warning(f"'{company_name}' 지역 검색 API 호출 실패: {e}")
    return None


def get_coords_from_naver(
    client_id: str, client_secret: str, address: str
) -> Optional[Tuple[float, float]]:
    """네이버 지오코딩 API로 주소를 위도/경도로 변환합니다."""
    headers = {
        "x-ncp-apigw-api-key-id": client_id,
        "x-ncp-apigw-api-key": client_secret,
    }
    params = {"query": address}
    url = "https://maps.apigw.ntruss.com/map-geocode/v2/geocode"
    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        geocode_results = response.json()

        if geocode_results.get("addresses"):
            addr_info = geocode_results["addresses"][0]
            # 네이버 지오코딩은 경도(x)가 먼저, 위도(y)가 나중에 옵니다.
            return (
                float(addr_info["y"]),
                float(addr_info["x"]),
            )  # (위도, 경도) 순으로 반환
    except requests.exceptions.RequestException as e:
        logging.warning(f"'{address}' 지오코딩 API 호출 실패: {e}")
    return None


def update_campaign_data(engine: Engine, campaign_id: int, data: Dict):
    """주어진 ID의 캠페인 데이터를 DB에 업데이트합니다."""
    # 업데이트할 값들만 필터링 (None이 아닌 값만)
    update_values = {key: value for key, value in data.items() if value is not None}

    if not update_values:
        logging.info(f"ID {campaign_id}: 업데이트할 새로운 데이터가 없습니다.")
        return

    # SQL UPDATE 문의 SET 절을 동적으로 생성
    set_clause = ", ".join([f'"{key}" = :{key}' for key in update_values.keys()])

    # 파라미터에 업데이트할 id 추가
    update_params = {"id": campaign_id, **update_values}

    # SQL 쿼리 실행
    query = text(f'UPDATE "campaign" SET {set_clause} WHERE id = :id')

    try:
        with engine.connect() as connection:
            connection.execute(query, update_params)
            # 변경사항을 커밋해야 실제 DB에 반영됩니다.
            connection.commit()
        # logging.info(f"ID {campaign_id}: 성공적으로 업데이트했습니다.")
    except Exception as e:
        logging.error(f"ID {campaign_id} 업데이트 실패: {e}")


def enrich_and_update_db():
    DB_TABLE_NAME = "campaign"  # 데이터를 가져올 테이블 이름
    COMPANY_COLUMN_NAME = "company"  # 상호명이 들어있는 컬럼 이름
    RESULT_TABLE_NAME = "review_campaigns_enriched"  # 결과를 저장할 새 테이블 이름
    RESULT_CSV_PATH = "result_enriched.csv"  # 결과를 저장할 CSV 파일 경로
    ID_COLUMN_NAME = "id"

    db_engine = get_db_engine()
    if not db_engine:
        exit()

    naver_map_client_id = os.getenv("NAVER_MAP_CLIENT_ID")
    naver_map_client_secret = os.getenv("NAVER_MAP_CLIENT_SECRET")
    naver_search_client_id = os.getenv("NAVER_SEARCH_CLIENT_ID")
    naver_search_client_secret = os.getenv("NAVER_SEARCH_CLIENT_SECRET")

    if not (
        naver_map_client_id
        and naver_map_client_secret
        and naver_search_client_id
        and naver_search_client_secret
    ):
        logging.critical(
            "환경변수에서 NAVER_CLIENT_ID와 NAVER_CLIENT_SECRET를 찾을 수 없습니다."
        )
        exit()

    original_df = fetch_data_from_db(
        db_engine, DB_TABLE_NAME, COMPANY_COLUMN_NAME, ID_COLUMN_NAME
    )

    if original_df.empty:
        logging.info("처리할 데이터가 없습니다. 프로그램을 종료합니다.")
        return

        # --- 데이터 순회 및 DB 업데이트 ---
        # iterrows()를 사용하여 각 행의 id와 company에 접근
    for index, row in tqdm(
        original_df.iterrows(), total=original_df.shape[0], desc="DB 업데이트 중"
    ):
        campaign_id = row[ID_COLUMN_NAME]
        company_name = row[COMPANY_COLUMN_NAME]

        place_info = get_place_info_from_naver(
            client_id=naver_search_client_id,
            client_secret=naver_search_client_secret,
            company_name=company_name,
        )

        new_data = {}
        if place_info:
            address = place_info.get("roadAddress", place_info.get("address"))
            # 'thubnail'을 'img_url'로 매핑
            img_url = place_info.get("link")

            coords = (
                get_coords_from_naver(
                    naver_map_client_id, naver_map_client_secret, address
                )
                if address
                else None
            )

            new_data = {
                "address": address,
                "lat": coords[0] if coords else None,
                "lng": coords[1] if coords else None,
                "img_url": img_url,
            }

        # 새로운 정보가 있을 경우에만 DB 업데이트 함수 호출
        if new_data:
            update_campaign_data(db_engine, campaign_id, new_data)

        time.sleep(0.1)

    logging.info("모든 작업이 완료되었습니다.")
