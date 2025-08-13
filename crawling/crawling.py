import pandas as pd
import io
import time
import os
import logging
from tqdm import tqdm

# types
from typing import List
from pandas import DataFrame
from sqlalchemy import create_engine, Engine
from sqlalchemy.types import VARCHAR, TEXT, BIGINT # 데이터 타입 지정을 위해 추가

# selenium
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

# env
from dotenv import load_dotenv

import psycopg2


class AdvancedScraper:
    """
    웹사이트에서 키워드 기반으로 데이터를 스크래핑하고,
    처리 후 데이터베이스에 저장하는 고급 스크레이퍼 클래스.
    로깅, 진행률 표시 기능 포함.
    """
    def __init__(self, url: str, **kwargs):
        self.base_url = url
        self._setup_logger()
        self.logger.info("스크레이퍼 초기화를 시작합니다.")

        for key, value in kwargs.items():
            setattr(self, key, value)

        self.driver = self._initialize_driver()
        self.db_engine = self._get_db_engine()
        self.logger.info("스크레이퍼 초기화가 완료되었습니다.")

    def _setup_logger(self):
        """로거를 설정합니다. 파일과 콘솔에 모두 출력합니다."""
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.INFO)
        
        if self.logger.hasHandlers():
            self.logger.handlers.clear()

        stream_handler = logging.StreamHandler()
        stream_format = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        stream_handler.setFormatter(stream_format)
        self.logger.addHandler(stream_handler)

        file_handler = logging.FileHandler('scraper.log')
        file_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(file_format)
        self.logger.addHandler(file_handler)

    def _initialize_driver(self) -> webdriver.Chrome:
        """셀레니움 웹 드라이버를 초기화하고 반환합니다."""
        self.logger.info("Chrome 드라이버를 설정합니다...")
        service = ChromeService(executable_path=ChromeDriverManager().install())
        options = webdriver.ChromeOptions()

        if hasattr(self, "headless") and self.headless:
            options.add_argument("--headless")
            self.logger.info("헤드리스 모드로 실행됩니다.")
        
        # [추가] 불필요한 로그 메시지 숨기기
        options.add_experimental_option('excludeSwitches', ['enable-logging'])
        driver = webdriver.Chrome(service=service, options=options)
        return driver
    
    def _get_db_engine(self) -> Engine:
        """환경변수를 로드하고 SQLAlchemy DB 엔진을 생성합니다."""
        self.logger.info("데이터베이스 연결 엔진을 생성합니다.")
        load_dotenv()
        try:
            db_url = f"postgresql+psycopg2://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
            return create_engine(db_url)
        except Exception as e:
            self.logger.critical(f"DB 연결 정보 생성 실패: {e}")
            raise

    def _search_keyword(self, keyword: str):
        """주어진 키워드로 웹사이트에서 검색을 수행합니다."""
        try:
            self.logger.info(f"키워드 검색 시작: '{keyword}'")
            search_box = self.driver.find_element(By.CSS_SELECTOR, "#root > div > section.main > div.input_container > input[type=text]")
            search_box.clear()
            search_box.send_keys(keyword)
            
            search_btn = self.driver.find_element(By.CSS_SELECTOR, "#search")
            search_btn.click()
            
            # [수정] time.sleep() 대신 명시적 대기 사용
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.ID, "result_table"))
            )
            self.logger.info(f"'{keyword}' 검색 결과 로딩 완료.")
            
        except TimeoutException:
            self.logger.warning(f"'{keyword}' 검색 결과 테이블이 시간 내에 로드되지 않았습니다.")
        except NoSuchElementException as e:
            self.logger.error(f"검색 입력창 또는 버튼을 찾지 못했습니다: {e}")
            raise

    def _extract_dataframe_from_page(self, search_text: str = None) -> DataFrame:
        """현재 페이지에서 테이블의 각 행을 순회하며 데이터를 추출하고, 링크를 포함한 DataFrame을 생성합니다."""
        try:
            # 테이블이 나타날 때까지 대기
            wait = WebDriverWait(self.driver, 10)
            table_body = wait.until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "#result_table > tbody"))
            )
            self.logger.info("'result_table > tbody' 요소를 성공적으로 찾았습니다.")
            
            # 테이블의 모든 행(tr)을 가져옵니다.
            rows = table_body.find_elements(By.TAG_NAME, "tr")
            self.logger.info(f"{len(rows)}개의 행을 찾았습니다. 데이터 추출을 시작합니다.")

            # 각 행의 데이터를 저장할 리스트
            all_rows_data = []

            for row in rows:
                # 각 행(tr)의 모든 셀(td)을 가져옵니다.
                cells = row.find_elements(By.TAG_NAME, "td")
                
                if len(cells) < 5: # 컬럼 개수가 부족한 행은 건너뜁니다.
                    continue

                # 각 셀의 텍스트를 추출합니다.
                platform = cells[0].text
                company_name = cells[1].text
                offer = cells[2].text
                apply_deadline = cells[3].text
                review_deadline = cells[4].text

                # 두 번째 셀(업체) 안의 <a> 태그를 찾아 href 속성(링크)을 추출합니다.
                try:
                    company_link = cells[1].find_element(By.TAG_NAME, "a").get_attribute("href")
                except NoSuchElementException:
                    company_link = None # a 태그가 없는 경우를 대비
                    self.logger.warning(f"'{company_name}' 업체에서 링크(a 태그)를 찾지 못했습니다.")

                # 추출한 데이터를 딕셔너리 형태로 리스트에 추가합니다.
                all_rows_data.append({
                    'platform': platform,
                    'company': company_name,
                    'company_link': company_link, # 링크 컬럼 추가
                    'offer': offer,
                    'apply_deadline': apply_deadline,
                    'review_deadline': review_deadline,
                    'search_text': search_text,
                })

            if not all_rows_data:
                self.logger.warning("페이지에서 추출할 데이터가 없습니다.")
                return pd.DataFrame()

            # 리스트에 저장된 딕셔너리들을 DataFrame으로 변환합니다.
            raw_df = pd.DataFrame(all_rows_data)
            
            # 이전과 동일하게 정제 함수를 호출합니다.
            return self._clean_dataframe(raw_df)

        except TimeoutException:
            self.logger.warning("결과 테이블 로딩 시간 초과. 빈 데이터를 반환합니다.")
        return pd.DataFrame()

    def _clean_dataframe(self, df: DataFrame) -> DataFrame:
        """
        [최종 수정본] DataFrame을 정제하고 DB 스키마에 맞게 표준화합니다.
        SettingWithCopyWarning을 방지하고 NaT 값을 확실하게 None으로 변환합니다.
        """
        self.logger.info(f"데이터 정제 시작. 원본 데이터 {df.shape[0]} 행.")
        
        if df.empty:
            return df
        
        # SettingWithCopyWarning을 원천적으로 방지하기 위해 명시적으로 복사본을 만듭니다.
        df = df.copy()
            
        df.columns = ['platform', 'company', 'company_link', 'offer', 'apply_deadline', 'review_deadline', 'search_text']

        # --- 문자열 컬럼 처리 ---
        # inplace=True 대신, 결과를 다시 할당하여 경고를 피합니다.
        for col in ['platform', 'company', 'offer']:
            if df[col].dtype == 'object':
                # 1. 공백 제거
                df[col] = df[col].str.strip()
                # 2. 결측치(NaN)를 빈 문자열('')로 변환
                df[col] = df[col].fillna('')
        self.logger.info("'platform', 'company', 'offer' 컬럼 정제 및 null 값 처리 완료.")
                
        # --- 날짜/시간 컬럼 처리 ---
        current_year = pd.Timestamp.now().year
        for col in ['apply_deadline', 'review_deadline']:
            # 1. 문자열을 datetime 객체로 변환 (실패 시 NaT)
            date_series = pd.to_datetime(f'{current_year}/' + df[col].astype(str).str.lstrip('~').str.strip(), 
                                        errors='coerce', 
                                        format='%Y/%m/%d')
            
            # 2. 타임존 정보 부여 (결과에 NaT 포함)
            aware_datetime = date_series.dt.tz_localize('Asia/Seoul')
            
            # 3. [핵심] NaT를 Python의 None으로 변환. 
            #    DB 에러를 막기 위한 가장 확실한 방법입니다.
            #    .astype('object')를 사용하여 NaT를 일반 객체로 다루도록 강제합니다.
            df[col] = aware_datetime.astype('object').where(aware_datetime.notna(), None)
            
        self.logger.info("'deadline' 컬럼 날짜 변환, 타임존 적용, NaT 처리 완료.")

        # --- 최종 유효성 검사 및 스키마 정리 ---
        initial_rows = len(df)
        # offer는 빈 문자열로 처리했으므로, 나머지 필수 컬럼만 검사
        required_cols = ['platform', 'company', 'apply_deadline', 'review_deadline']
        df.dropna(subset=required_cols, inplace=True)
        removed_rows = initial_rows - len(df)
        if removed_rows > 0:
            self.logger.info(f"필수 정보가 누락된 {removed_rows}개 행을 제거했습니다.")
        
        db_only_cols = ['address', 'lat', 'lng', 'img_url']
        for col in db_only_cols:
            df[col] = None
        
        if 'keyword' in df.columns:
            df.rename(columns={'keyword': 'search_text'}, inplace=True)
        else:
            df['search_text'] = None

        self.logger.info(f"데이터 정제 완료. 최종 {len(df)} 행.")
        return df


    def _navigate_to(self, path: str = "/"):
        """베이스 URL 기준으로 특정 경로로 이동합니다."""
        target_url = f"{self.base_url}{path}"
        self.logger.info(f"페이지 이동: {target_url}")
        self.driver.get(target_url)

    def _upsert_data_to_db(self, df: DataFrame, table_name: str):
        """
        주어진 DataFrame을 데이터베이스에 UPSERT합니다.
        (ON CONFLICT ... DO UPDATE)
        """
        if df.empty:
            self.logger.warning("저장할 데이터가 없어 DB 저장을 건너뜁니다.")
            return

        self.logger.info(f"'{table_name}' 테이블에 {df.shape[0]}개 행 UPSERT 시작...")
        
        # DataFrame 컬럼 순서를 DB 테이블 컬럼 순서와 일치시킴
        # id, created_at, updated_at은 DB에서 자동으로 처리하므로 제외
        cols_in_order = [
            'platform', 'company', 'company_link', 'offer', 'apply_deadline', 'review_deadline', 'search_text',
            'address', 'lat', 'lng', 'img_url'
        ]
        df = df[cols_in_order]
        df_cleaned = df.where(pd.notna(df), None)

        # ON CONFLICT 대상 컬럼 (UNIQUE 제약조건을 설정한 컬럼들)
        conflict_cols = ['platform', 'company', 'offer']

        for col in conflict_cols:
            df_cleaned[col].fillna('', inplace=True)
            df_cleaned[col] = df_cleaned[col].str.strip()
            self.logger.info(f"'{col}' 컬럼의 null 값을 빈 문자열로 처리했습니다.")




        update_cols = [col for col in cols_in_order if col not in conflict_cols]
        
        sql_insert = f"INSERT INTO {table_name} ({', '.join(cols_in_order)}) VALUES %s"
        sql_conflict = f"ON CONFLICT ({', '.join(conflict_cols)}) DO UPDATE SET "
        sql_update = ", ".join([f"{col} = EXCLUDED.{col}" for col in update_cols])
        
        upsert_sql = sql_insert + sql_conflict + sql_update + ";"

        conn = None
        try:
            conn = self.db_engine.raw_connection()
            cursor = conn.cursor()
            
            # [수정] 정리된 df_cleaned를 사용합니다.
            values = [tuple(x) for x in df_cleaned.to_numpy()]
            
            psycopg2.extras.execute_values(cursor, upsert_sql, values)
            
            conn.commit()
            self.logger.info(f"데이터베이스에 성공적으로 UPSERT 했습니다.")
        except Exception as e:
            self.logger.error(f"데이터베이스 UPSERT 중 오류 발생: {e}")
            if conn:
                conn.rollback()
            raise
        finally:
            if conn:
                cursor.close()
                conn.close()

    def execute_scraping(self, keywords: List[str], table_name: str, implicitly_wait: int = 5):
        """전체 스크래핑 및 저장 워크플로우를 실행합니다."""
        self._navigate_to()
        self.driver.implicitly_wait(implicitly_wait)

        df_list = []
        for keyword in tqdm(keywords, desc="키워드 검색 진행률"):
            self._search_keyword(keyword)
            
            temp_df = self._extract_dataframe_from_page(search_text=keyword)
            if not temp_df.empty:
                temp_df['keyword'] = keyword # 나중에 search_text로 변환됨
                df_list.append(temp_df)
            
            # [수정] 검색 후 메인 페이지로 돌아갈 필요가 없다면 아래 라인 삭제 가능
            self._navigate_to() 
            time.sleep(5)

        if not df_list:
            self.logger.warning("수집된 데이터가 전혀 없습니다.")
            self.close()
            return pd.DataFrame()

        final_df = pd.concat(df_list, ignore_index=True)
        self.logger.info(f"총 {len(keywords)}개 키워드로부터 {final_df.shape[0]}개의 데이터를 수집했습니다.")


        final_df.drop_duplicates(subset=['platform', 'company', 'offer'], keep='last', inplace=True)
        self.logger.info(f"중복 제거 후 {final_df.shape[0]}개의 고유한 데이터를 확인했습니다.")
        
        
        # [수정] 새로운 UPSERT 메소드 호출
        self._upsert_data_to_db(final_df, table_name=table_name)
        
        return final_df

    def close(self):
        """드라이버를 종료합니다."""
        if self.driver:
            self.logger.info("드라이버를 종료합니다.")
            self.driver.quit()


