CREATE OR REPLACE PROCEDURE RWA_DEV.TRUNC_TAB_TMP IS
  /***************************************************************************
  create by LJZ on 2019/7/12
  �����ʱ����_T���ı�����
  ***************************************************************************/

  --����
  V_TAB VARCHAR2(100) := '';
  --SQL����
  V_SQL VARCHAR2(2000) := '';
  --������
  V_SUM NUMBER := 0;
  --�ɹ�����
  V_SUCCESS NUMBER := 0;
  --�����α�
  CURSOR CUR IS
    SELECT T.TABLENAME || '_T'
      FROM DATAMART_STATUS T
     WHERE T.TABLENAME IS NOT NULL;

BEGIN
  --��ȡ��Ҫ��յ���ʱ�����
  SELECT COUNT(1)
    INTO V_SUM
    FROM DATAMART_STATUS T
   WHERE T.TABLENAME IS NOT NULL;
  DBMS_OUTPUT.PUT_LINE('��ѯ����Ҫ��յ���ʱ��');
  OPEN CUR;
  LOOP
    FETCH CUR
      INTO V_TAB;
    EXIT WHEN CUR%NOTFOUND;
    DBMS_OUTPUT.put('   ');
    DBMS_OUTPUT.PUT_LINE(V_TAB);
    V_SQL := 'truncate table ' || V_TAB;
    --ִ��TRUNCATE TABLE
    EXECUTE IMMEDIATE V_SQL;
    V_SUCCESS := V_SUCCESS + 1;
  END LOOP;
  CLOSE CUR;
  DBMS_OUTPUT.PUT_LINE('�����ɣ��ܼƣ�' || V_SUM || '���ɹ���' || V_SUCCESS || '��');
END TRUNC_TAB_TMP;
/

