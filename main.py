import time

from crawling import AdvancedScraper, enrich_and_update_db
import logging


def scrape(table_name):
    # --- 실행 예시 ---
    BASE_URL = "https://inflexer.net"  # 실제 스크래핑할 URL로 변경하세요.
    SEARCH_KEYWORDS = [
        "서울 강남",
        "서울 강동",
        "서울 강북",
        "서울 강서",
        "서울 관악",
        "서울 광진",
        "서울 구로",
        "서울 금천",
        "서울 노원",
        "서울 도봉",
        "서울 동대문",
        "서울 동작",
        "서울 마포",
        "서울 서대문",
        "서울 서초",
        "서울 성동",
        "서울 성북",
        "서울 송파",
        "서울 양천",
        "서울 영등포",
        "서울 용산",
        "서울 은평",
        "서울 종로",
        "서울 중구",
        "서울 중랑",
        "경기 가평",
        "경기 고양",
        "경기 과천",
        "경기 광명",
        "경기 광주",
        "경기 구리",
        "경기 군포",
        "경기 김포",
        "경기 남양주",
        "경기 동두천",
        "경기 부천",
        "경기 성남",
        "경기 수원",
        "경기 시흥",
        "경기 안산",
        "경기 안성",
        "경기 안양",
        "경기 양주",
        "경기 양평",
        "경기 여주",
        "경기 연천",
        "경기 오산",
        "경기 용인",
        "경기 의왕",
        "경기 의정부",
        "경기 이천",
        "경기 파주",
        "경기 평택",
        "경기 포천",
        "경기 하남",
        "경기 화성",
        "인천 강화",
        "인천 계양",
        "인천 미추홀",
        "인천 남동",
        "인천 동구",
        "인천 부평",
        "인천 서구",
        "인천 연수",
        "인천 옹진",
        "인천 중구",
    ]  # 검색할 키워드 리스트

    scraper = None
    try:
        # 헤드리스 모드로 실행하려면 headless=True 전달
        scraper = AdvancedScraper(url=BASE_URL, headless=False)
        final_data = scraper.execute_scraping(
            keywords=SEARCH_KEYWORDS, table_name=table_name
        )
        print("\n--- 최종 통합 데이터 (일부) ---")
        print(final_data.head())
    except Exception as e:
        # 최상위 레벨에서 예외를 잡아 로깅
        logging.getLogger().critical(
            f"스크립트 실행 중 치명적인 오류 발생: {e}", exc_info=True
        )
    finally:
        # 오류 발생 여부와 관계없이 항상 드라이버 종료
        if scraper:
            scraper.close()


if __name__ == "__main__":
    scrape(table_name="campaign")
    time.sleep(5)
    enrich_and_update_db()
