CREATE OR REPLACE PROCEDURE r2d2_dba_test.create_customer()
BEGIN
  DECLARE id STRING;
  SET id = GENERATE_UUID();
  INSERT INTO `data-dev-base-1331.r2d2_dba_test.customers` (customer_id)
    VALUES(id);
  SELECT FORMAT("Created customer %s", id);
END