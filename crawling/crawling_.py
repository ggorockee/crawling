from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
import pandas as pd
import io

import time

# types
from typing import List
from pandas import DataFrame

# env
from dotenv import load_dotenv
import os

from sqlalchemy import create_engine
from sqlalchemy import Engine


class Crawling:
    def __init__(self, url: str = None, **kwargs):
        self.url = url
        self.service = ChromeService(executable_path=ChromeDriverManager().install())
        self.options = webdriver.ChromeOptions()

        for key, value in kwargs.items():
            setattr(self, key, value)

        if hasattr(self, "headless") and self.headless:
            self.options.add_argument("--headless")

        self.driver = webdriver.Chrome(service=self.service, options=self.options)

    def convert_df(self, tables: List[DataFrame]) -> DataFrame:
        for table in tables:
            # read_html은 table 태그의 속성을 직접 가져오진 않으므로, 컬럼명 등으로 원하는 테이블을 식별해야 합니다.
            # 또는, 특정 요소의 HTML만 가져오는 아래의 '대안' 방법을 사용할 수도 있습니다.
            if "플랫폼" in table.columns and "업체" in table.columns:
                df = table
                break
        print("성공: HTML을 Pandas 데이터프레임으로 변환했습니다.")

        # 데이터 정제
        for col in df.columns:
            if df[col].dtype == "object":
                df[col] = df[col].str.strip()

        new_cols = ["platform", "company", "offer", "apply_deadline", "review_deadline"]

        df.columns = new_cols

        # 변경 확인
        print("컬럼명이 영어로 변경되었습니다.")
        print(df.head())

        return df

    def extract_data_from_table(self) -> DataFrame:
        wait = WebDriverWait(self.driver, 10)
        wait.until(EC.presence_of_element_located((By.ID, "result_table")))
        print("성공: 'result_table' 요소를 찾았습니다.")

        html_source = self.driver.page_source
        print("성공: 페이지의 HTML 소스를 가져왔습니다.")

        tables = pd.read_html(io.StringIO(html_source))

        # --- 6. 결과 출력 ---
        print("\n--------- 최종 변환된 데이터프레임 ---------")
        df = self.convert_df(tables)
        return df

    def conn_db(self) -> Engine:
        load_dotenv()
        db_host = os.getenv("POSTGRES_HOST")
        db_user = os.getenv("POSTGRES_USER")
        db_password = os.getenv("POSTGRES_DB")
        db_port = os.getenv("POSTGRES_PORT")
        db_name = os.getenv("POSTGRES_DB")
        db_url = f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
        return create_engine(db_url)

    def save_db(self, df: DataFrame, table_name: str, if_exists: str = "replace"):
        conn = self.conn_db()
        df.to_sql(table_name, con=conn, if_exists=if_exists, index=False)
        print("'products_basic' 테이블에 저장 완료.")

    def search_keyword(self, search_text: str):
        search_box = self.driver.find_element(
            By.CSS_SELECTOR,
            "#root > div > section.main > div.input_container > input[type=text]",
        )
        search_query = search_text
        search_box.send_keys(search_query)

        time.sleep(2)
        search_btn_selector = "#search"
        search_btn = self.driver.find_element(By.CSS_SELECTOR, search_btn_selector)
        search_btn.click()

        time.sleep(2)

    def go_to(self, dst: str):
        self.driver.get(f"{self.url}{dst}")

    def run(self, implicitly_wait: int = 5, searh_texts: List[str] = []):
        self.driver.get(self.url)
        self.driver.implicitly_wait(implicitly_wait)

        df_list = []
        for search_text in searh_texts:
            self.search_keyword(search_text=search_text)

            temp_df = self.extract_data_from_table()
            if not temp_df.empty:  # 결과가 비어있지 않은 경우에만 추가
                df_list.append(temp_df)
            self.go_to("/")

        if df_list:  # 리스트가 비어있지 않다면 합칩니다.
            final_df = pd.concat(df_list, ignore_index=True)
            self.save_db(final_df, "product_basic", if_exists="replace")
        else:
            final_df = pd.DataFrame()
