from crawling import AdvancedScraper, get_lat_lng_and_add_dataframe
import logging


def scrape():
     # --- 실행 예시 ---
    BASE_URL = "https://inflexer.net" # 실제 스크래핑할 URL로 변경하세요.
    SEARCH_KEYWORDS = ["경기 김포", "경기 수원"] # 검색할 키워드 리스트

    scraper = None
    try:
        # 헤드리스 모드로 실행하려면 headless=True 전달
        scraper = AdvancedScraper(url=BASE_URL, headless=False)
        final_data = scraper.execute_scraping(keywords=SEARCH_KEYWORDS, table_name="campaign")
        print("\n--- 최종 통합 데이터 (일부) ---")
        print(final_data.head())
    except Exception as e:
        # 최상위 레벨에서 예외를 잡아 로깅
        logging.getLogger().critical(f"스크립트 실행 중 치명적인 오류 발생: {e}", exc_info=True)
    finally:
        # 오류 발생 여부와 관계없이 항상 드라이버 종료
        if scraper:
            scraper.close()



if __name__ == '__main__':
#    scrape()
    get_lat_lng_and_add_dataframe()