CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_TABLE_COUNT(INDATANO IN VARCHAR2) AS
  V_SQLA VARCHAR2(4000);
  V_SQLB VARCHAR2(4000);
  V_SQL  VARCHAR2(4000);

  --表名
  TABLE_NAME VARCHAR2(200);

  --列名
  COL_NAME VARCHAR2(200);

  --列名排序ID
  COLNUM_ID NUMBER;

  --定义游标
  CURSOR CUR_TAB_COL IS
    SELECT A.TABLE_NAME, A.COLUMN_NAME, A.COLUMN_ID
      FROM USER_TAB_COLUMNS A
     WHERE A.TABLE_NAME LIKE 'RWA%'
     ORDER BY A.TABLE_NAME, A.COLUMN_ID;

BEGIN

  DELETE FROM TABLE_COUNT_INFO WHERE DATANO = INDATANO;
  COMMIT;

  OPEN CUR_TAB_COL;
  LOOP
    FETCH CUR_TAB_COL
      INTO TABLE_NAME, COL_NAME, COLNUM_ID;
    EXIT WHEN CUR_TAB_COL%NOTFOUND;
    --SELECT 'AA' AS TABLE_NAME,COUNT(*) AS ALL_COUNT, 'ID1' AS COL_NAME, 1 FROM AA WHERE DATANO='20190331'
    V_SQLA := Q'[select /*parallel(t,4)*/ ']' || TABLE_NAME || Q'[' as table_name,]' ||
              Q'[count(*) as all_count, ]' || Q'[']' || COL_NAME ||
              Q'[' as col_name, ]' || COLNUM_ID || Q'[ from ]' ||
              TABLE_NAME || Q'[ t where datano=']' || INDATANO || Q'[']';
    --SELECT COUNT(*) AS NULL_COL_COUNT,'AA' AS TABLE_NAME,'id1' AS ID1 FROM AA WHERE DATANO='20190331' AND （AA.ID1 IS NULL OR AA.ID1='')
    V_SQLB := Q'[select /*parallel(t,4)*/ count(*) as null_col_count,']' || TABLE_NAME ||
              Q'[' as table_name, ']' || COL_NAME ||
              Q'[' as col_name from ]' || TABLE_NAME ||
              Q'[ t where datano=']' || INDATANO || Q'[' and (]' || COL_NAME ||
              Q'[ is null or ]' || COL_NAME || Q'[='')]';
    --SELECT A.*,B.NULL_COL_COUNT,B.NULL_COL_COUNT/A.ALL_COUNT*100,'20190331'
    --FROM A INNER JOIN B ON A.TABLE_NAME=B.TABLE_NAME AND A.COL_NAME=B.COL_NAME
    V_SQL := Q'[insert into table_count_info select a.*,b.null_col_count,decode(a.all_count,0,0,b.null_col_count/a.all_count*100),']' ||
             INDATANO || Q'[' from (]' || V_SQLA || Q'[) a inner join (]' ||
             V_SQLB ||
             Q'[) b on a.table_name=b.table_name and a.col_name=b.col_name]';
    --DBMS_OUTPUT.ENABLE(BUFFER_SIZE => NULL);  --表示输出BUFFER不受限制
    DBMS_OUTPUT.PUT_LINE(TABLE_NAME);
    DBMS_OUTPUT.PUT_LINE(COL_NAME);
    EXECUTE IMMEDIATE V_SQL;
    COMMIT;
  END LOOP;

  CLOSE CUR_TAB_COL;

END PRO_TABLE_COUNT;
/

