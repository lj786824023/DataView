CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_DEAL_GL(P_DATA_DT_STR IN VARCHAR2,
                                        P_PO_RTNCODE  OUT VARCHAR2, --���ر�� 1 �ɹ�,0 ʧ��
                                        P_PO_RTNMSG   OUT VARCHAR2 --��������
                                        ) IS
  /***************************************************************************
  CREATE BY LJZ ON 2020/12/23
  �²������˱�ӹ����ɲ������˱�
  NFIN_GL_BALANCE_CK ---> FNS_GL_BALANCE
  ***************************************************************************/

  V_SQL   VARCHAR2(2000) := ''; --SQL����
  V_TABLE VARCHAR2(100) := 'FNS_GL_BALANCE'; --Ŀ���
  V_COUNT NUMBER := 0; --
  V_DATE  VARCHAR2(10) := P_DATA_DT_STR; --��������
BEGIN
  -- 1.����������truncate��������������add
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

  P_PO_RTNMSG := '�ɹ�';
  
  
  --�����쳣
EXCEPTION
  WHEN OTHERS THEN
    ROLLBACK;
    P_PO_RTNCODE := SQLCODE;
    P_PO_RTNMSG  := '�������˱�ӹ�ʧ�ܣ�' || SQLERRM || ';��������Ϊ:' ||
                    DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
    RETURN;
END PRO_DEAL_GL;
/

