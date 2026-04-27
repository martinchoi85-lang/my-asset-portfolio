-- 1. 새로운 컬럼 추가
ALTER TABLE transactions 
ADD COLUMN IF NOT EXISTS is_external_flow BOOLEAN DEFAULT TRUE;

ALTER TABLE transactions 
ADD COLUMN IF NOT EXISTS parent_transaction_id BIGINT REFERENCES transactions(id) ON DELETE CASCADE;

-- 2. 기존 미러링 캐시 데이터 업데이트 (memo 기준 필터링)
UPDATE transactions
SET is_external_flow = FALSE
WHERE memo LIKE '%[AUTO] BUY cash mirror%'
   OR memo LIKE '%[AUTO] SELL cash mirror%';

-- 3. 기존 미러링 거래의 parent_transaction_id 매핑 연결
UPDATE transactions mirror
SET parent_transaction_id = origin.id
FROM transactions origin
WHERE mirror.is_external_flow = FALSE
  AND origin.is_external_flow = TRUE
  AND origin.account_id = mirror.account_id
  AND origin.transaction_date = mirror.transaction_date
  AND (
        (origin.trade_type = 'BUY' AND mirror.trade_type = 'WITHDRAW')
        OR 
        (origin.trade_type = 'SELL' AND mirror.trade_type = 'DEPOSIT')
      );
