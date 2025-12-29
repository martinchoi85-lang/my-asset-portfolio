1.스냅샷 평가가격 로직 개선 (current_price 반영 + 캐시/통화 예외 처리)
- valuation_price가 실제 시장가격을 반영하도록 정리
- cash는 1 고정, 그 외는 current_price 사용(단, V1.1은 “모든 날짜에 동일 current_price 적용” 한계 명시)

2.가격 업데이트 기능(Price Updater) 안정화
- yfinance 성공/실패 사유 로깅 및 UI 표시
- 한국 종목 ticker suffix(.KS/.KQ) 자동 시도
- 업데이트 후 “해당 계좌/자산 리빌드” 연결(옵션)

3.거래 입력 기능 고도화
- 거래 수정/삭제(Edit/Delete) 지원
- 실패 시 리빌드 재시도 버튼 안정화
- 입력 검증 강화(현금 거래는 cash 자산만, price=1 고정 등)

4.배당(DIVIDEND) 별도 Editor + 시각화
- dividends 테이블(또는 cash_flows 테이블) 설계
- 종목별/기간별 배당 합계 및 배당수익률 표시
- (선택) 배당을 현금 잔고로 반영하는 옵션(추후)

5.혼합 통화 처리(중요)
- 통화별 KPI 분리 표시 또는 FX 테이블 도입
- FX 손익 분해(거래 당시 fx_rate 저장 또는 일별 fx_rates 테이블)

6.스냅샷 자동 생성(스케줄링)
- 로컬 cron/Windows Task Scheduler 또는 GitHub Actions + Supabase
- 실패 알림, 재시도 정책

7.성능/정합성 강화
- 재생성 범위 최소화(증분 리빌드)
- 데이터 품질 체크(유니크 키 검증, 음수 잔고 방지)
- Materialized View 검토(ALL 집계 등)

8.운영 편의 기능
- 백업/복원(export/import)
- 사용자 설정(표시 통화, 기본 계좌, 기본 필터)