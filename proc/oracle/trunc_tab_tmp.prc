CREATE OR REPLACE PROCEDURE RWA_DEV.TRUNC_TAB_TMP IS
  /***************************************************************************
  create by LJZ on 2019/7/12
  清空临时表（带_T）的表数据
  ***************************************************************************/

  --表名
  V_TAB VARCHAR2(100) := '';
  --SQL代码
  V_SQL VARCHAR2(2000) := '';
  --总条数
  V_SUM NUMBER := 0;
  --成功条数
  V_SUCCESS NUMBER := 0;
  --定义游标
  CURSOR CUR IS
    SELECT T.TABLENAME || '_T'
      FROM DATAMART_STATUS T
     WHERE T.TABLENAME IS NOT NULL;

BEGIN
  --获取需要清空的临时表表名
  SELECT COUNT(1)
    INTO V_SUM
    FROM DATAMART_STATUS T
   WHERE T.TABLENAME IS NOT NULL;
  DBMS_OUTPUT.PUT_LINE('查询到需要清空的临时表：');
  OPEN CUR;
  LOOP
    FETCH CUR
      INTO V_TAB;
    EXIT WHEN CUR%NOTFOUND;
    DBMS_OUTPUT.put('   ');
    DBMS_OUTPUT.PUT_LINE(V_TAB);
    V_SQL := 'truncate table ' || V_TAB;
    --执行TRUNCATE TABLE
    EXECUTE IMMEDIATE V_SQL;
    V_SUCCESS := V_SUCCESS + 1;
  END LOOP;
  CLOSE CUR;
  DBMS_OUTPUT.PUT_LINE('清空完成！总计：' || V_SUM || '，成功：' || V_SUCCESS || '。');
END TRUNC_TAB_TMP;
/

