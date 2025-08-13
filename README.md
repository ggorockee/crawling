CREATE TABLE campaign (
    id BIGSERIAL PRIMARY KEY, -- INT보다 큰 범위의 자동 증가 PK, BIGINT + SEQUENCE
    platform VARCHAR(20) NOT NULL,
    company VARCHAR(255) NOT NULL,
    offer VARCHAR(255) NOT NULL,
    apply_deadline TIMESTAMPTZ, -- 타임존을 포함하는 timestamp
    review_deadline TIMESTAMPTZ,
    address VARCHAR(255),
    lat DECIMAL(9, 6), -- 위도 (Latitude), 소수점 6자리까지의 정확도
    lng DECIMAL(9, 6), -- 경도 (Longitude)
    img_url VARCHAR(255),
    search_text VARCHAR(20),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), -- 생성 시각, 기본값으로 현재 시각 자동 입력
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()  -- 수정 시각, 기본값으로 현재 시각 자동 입력
);