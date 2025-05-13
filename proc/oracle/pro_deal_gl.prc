CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_DEAL_GL(P_DATA_DT_STR IN VARCHAR2,
                                        P_PO_RTNCODE  OUT VARCHAR2, --返回编号 1 成功,0 失败
                                        P_PO_RTNMSG   OUT VARCHAR2 --返回描述
                                        ) IS
  /***************************************************************************
  CREATE BY LJZ ON 2020/12/23
  新财务总账表加工到旧财务总账表
  NFIN_GL_BALANCE_CK ---> FNS_GL_BALANCE
  ***************************************************************************/

  V_SQL   VARCHAR2(2000) := ''; --SQL代码
  V_TABLE VARCHAR2(100) := 'FNS_GL_BALANCE'; --目标表
  V_COUNT NUMBER := 0; --
  V_DATE  VARCHAR2(10) := P_DATA_DT_STR; --数据日期
BEGIN
  -- 1.分区存在则truncate，分区不存在则add
  SELECT COUNT(1)
    INTO V_COUNT
    FROM USER_TAB_PARTITIONS
   WHERE TABLE_NAME = V_TABLE
     AND PARTITION_NAME = 'SRC' || V_DATE;

  IF V_COUNT <> 0 THEN
    V_SQL := 'ALTER TABLE ' || V_TABLE || ' TRUNCATE PARTITION SRC' ||
             V_DATE;
  ELSE
    V_SQL := 'ALTER TABLE ' || V_TABLE || ' ADD PARTITION SRC' || V_DATE ||
             ' VALUES(''' || V_DATE || ''')';
  END IF;
  --DBMS_OUTPUT.PUT_LINE(V_SQL);
  EXECUTE IMMEDIATE V_SQL;

  -- 2.DEAL FNS_GL_BALANCE
  INSERT INTO FNS_GL_BALANCE
    (DATANO,
     ORG_ID,
     SUBJECT_NO,
     CURRENCY_CODE,
     BALANCE_D,
     BALANCE_C,
     BALANCE_D_BEQ,
     BALANCE_C_BEQ)
    SELECT DATANO,
           ORG_CODE,
           SUBJCODE,
           CCYNBR,
           DRBALANCE,
           CRBALANCE,
           NULL,
           NULL
      FROM NFIN_GL_BALANCE_CK
     WHERE DATANO = V_DATE;

  COMMIT;

  P_PO_RTNMSG := '成功';
  
  
  --定义异常
EXCEPTION
  WHEN OTHERS THEN
    ROLLBACK;
    P_PO_RTNCODE := SQLCODE;
    P_PO_RTNMSG  := '财务总账表加工失败！' || SQLERRM || ';错误行数为:' ||
                    DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
    RETURN;
END PRO_DEAL_GL;
/

