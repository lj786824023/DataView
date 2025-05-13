CREATE OR REPLACE PROCEDURE RWA_DEV.pro_rwa_cd_guaranty_qualified(
                                       v_data_dt_str  IN   VARCHAR2,   --��������
                                       v_po_rtncode  OUT  VARCHAR2,     --���ر��
                                       v_po_rtnmsg    OUT  VARCHAR2     --��������
                                        ) AS
/*
    �洢��������:pro_rwa_cd_guaranty_qualified
    ʵ�ֹ���:ʵ��(UPDATE)�����ҵ��ĺϸ�֤�����϶�
    ���ݿھ�:ȫ��
    ����Ƶ��:��ĩ
    ��  ��  :V1.0.0
    ��д��  :YUSJ
    ��дʱ��:2015-07-03
    ��  λ  :�Ϻ���˶��Ϣ�����ɷ����޹�˾
    Դ  ��1  :�ϸ�֤�����
    Ŀ���  :��֤���
    ������  :��
    ��   ע��
    �����¼(�޸���|�޸�ʱ��|�޸�����)��
  */
  --������µ�sql���
  v_update_sql VARCHAR2(4000);
  --����ƥ�������ļ�¼
  v_count number(18) := 0;
  --��֤���
  GUARANTY_TYPE VARCHAR2(300);
  --����һ��������
  REGIST_STATE_CODE VARCHAR2(600);
  --�ϸ��ʶ
  QUALIFIED_FLAG VARCHAR2(10);
BEGIN
  DECLARE
   --ͬ���α��ȡ��Ҫ���ж�����
  CURSOR c_cursor IS
  SELECT
    CASE WHEN GUARANTY_TYPE IS NOT NULL
         THEN ' AND EXISTS(SELECT 1 FROM LOAN_APPEND  T1 WHERE T.GuaranteeID = T1.append_no AND T.GuaranteeConID= T1.APPEND_NO AND T1.REG_VALID = 0 AND T1.APPEND_TYPE='''||GUARANTY_TYPE||''') '
         ELSE ''
    END AS GUARANTY_TYPE    --��֤���
     ,CASE WHEN REGIST_STATE_CODE IS NOT NULL and CLIENT_SUB_TYPE IS NOT NULL and  COUNTRY_LEVEL_NO IS NOT NULL
         THEN ' AND EXISTS(SELECT 1 FROM RWA_EI_CLIENT T2 WHERE T.GuarantorID=T2.ClientID AND T.DATANO=T2.DATANO AND T2.RegistState='''||REGIST_STATE_CODE||''' AND T2.ClientSubType='''||CLIENT_SUB_TYPE||''' AND T2.RCERating'||COUNTRY_LEVEL_NO||') '
         WHEN REGIST_STATE_CODE IS NULL and CLIENT_SUB_TYPE IS NOT NULL and  COUNTRY_LEVEL_NO IS NOT NULL
         THEN ' AND EXISTS(SELECT 1 FROM RWA_EI_CLIENT T2 WHERE T.GuarantorID=T2.ClientID AND T.DATANO=T2.DATANO AND T2.ClientSubType='''||CLIENT_SUB_TYPE||''' AND T2.RCERating'||COUNTRY_LEVEL_NO||') '
         WHEN REGIST_STATE_CODE IS NOT NULL and CLIENT_SUB_TYPE IS NULL and  COUNTRY_LEVEL_NO IS NOT NULL
         THEN ' AND EXISTS(SELECT 1 FROM RWA_EI_CLIENT T2 WHERE T.GuarantorID=T2.ClientID AND T.DATANO=T2.DATANO AND T2.RegistState='''||REGIST_STATE_CODE||''' AND T2.RCERating'||COUNTRY_LEVEL_NO||') '
         WHEN REGIST_STATE_CODE IS NOT NULL and CLIENT_SUB_TYPE IS NOT NULL and  COUNTRY_LEVEL_NO IS NULL
         THEN ' AND EXISTS(SELECT 1 FROM RWA_EI_CLIENT T2 WHERE T.GuarantorID=T2.ClientID AND T.DATANO=T2.DATANO AND T2.RegistState='''||REGIST_STATE_CODE||''' AND T2.ClientSubType='''||CLIENT_SUB_TYPE||''') '
         WHEN REGIST_STATE_CODE IS NULL and CLIENT_SUB_TYPE IS NULL and  COUNTRY_LEVEL_NO IS NOT NULL
         THEN ' AND EXISTS(SELECT 1 FROM RWA_EI_CLIENT T2 WHERE T.GuarantorID=T2.ClientID AND T.DATANO=T2.DATANO AND T2.RCERating'||COUNTRY_LEVEL_NO||') '
         WHEN REGIST_STATE_CODE IS NULL and CLIENT_SUB_TYPE IS NOT NULL and  COUNTRY_LEVEL_NO IS NULL
         THEN ' AND EXISTS(SELECT 1 FROM RWA_EI_CLIENT T2 WHERE T.GuarantorID=T2.ClientID AND T.DATANO=T2.DATANO AND T2.ClientSubType='''||CLIENT_SUB_TYPE||''') '
         WHEN REGIST_STATE_CODE IS NOT NULL and CLIENT_SUB_TYPE IS NULL and  COUNTRY_LEVEL_NO IS NULL
         THEN ' AND EXISTS(SELECT 1 FROM RWA_EI_CLIENT T2 WHERE T.GuarantorID=T2.ClientID AND T.DATANO=T2.DATANO AND T2.RegistState='''||REGIST_STATE_CODE||''' ) '
         ELSE ''
    END AS REGIST_STATE_CODE    --ע����ҵ�������
    ,QUALIFIED_FLAG
    FROM RWA_CD_GUARANTY_QUALIFIED;
  BEGIN
    --�����α�
    OPEN c_cursor;
    --ͨ��ѭ�������������α�
    DBMS_OUTPUT.PUT_LINE('>>>>>>Update��俪ʼִ����>>>>>>>');
    LOOP
      v_count := v_count + 1;
      -- ���α��ȡ��ֵ���趨���ƥ������
      FETCH c_cursor INTO
        GUARANTY_TYPE
       ,REGIST_STATE_CODE
       ,QUALIFIED_FLAG
       ;
      --���α������ɺ��˳��α�
      EXIT WHEN c_cursor%NOTFOUND;
      IF QUALIFIED_FLAG='01' THEN
        v_update_sql:='UPDATE RWA_EI_GUARANTEE T SET T.QUALFLAGSTD=''1'', T.QUALFLAGFIRB=''1''  WHERE T.datadate=TO_DATE('''||v_data_dt_str||''',''YYYYMMDD'')';
      ELSIF QUALIFIED_FLAG ='02' THEN
        v_update_sql:='UPDATE RWA_EI_GUARANTEE T SET T.QUALFLAGSTD=''0'', T.QUALFLAGFIRB=''1''  WHERE T.datadate=TO_DATE('''||v_data_dt_str||''',''YYYYMMDD'')';
      ELSE
        v_update_sql:='UPDATE RWA_EI_GUARANTEE T SET T.QUALFLAGSTD=''0'', T.QUALFLAGFIRB=''0''  WHERE T.datadate=TO_DATE('''||v_data_dt_str||''',''YYYYMMDD'')';
      END IF;
      --�ϲ�sql��ƴ��where����
      v_update_sql := v_update_sql
      ||GUARANTY_TYPE
      ||REGIST_STATE_CODE
      ;
      DBMS_OUTPUT.PUT_LINE(v_update_sql);
      --ִ��sql
      EXECUTE IMMEDIATE v_update_sql;
      COMMIT;
      --����ѭ��
    END LOOP;
    DBMS_OUTPUT.PUT_LINE('�������ܼ�¼Ϊ��'|| v_count);
    DBMS_OUTPUT.PUT_LINE('Update����Ѿ�ִ�н���������');
      --�ر��α�
    CLOSE c_cursor;
  END;
    UPDATE RWA_EI_GUARANTEE SET QUALFLAGSTD='0' WHERE datadate=TO_DATE(v_data_dt_str,'YYYYMMDD')AND QUALFLAGSTD IS NULL;
    UPDATE RWA_EI_GUARANTEE SET QUALFLAGFIRB='0' WHERE datadate=TO_DATE(v_data_dt_str,'YYYYMMDD') AND QUALFLAGFIRB IS NULL;
    COMMIT;
  v_po_rtncode := '1';
  v_po_rtnmsg  := '�ɹ�';
    --�����쳣
    EXCEPTION
    WHEN OTHERS THEN
         Dbms_output.Put_line('������,�������Ϊ:'||sqlcode||';������ϢΪ:'||sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace);
          ROLLBACK;
         v_po_rtncode := sqlcode;
         v_po_rtnmsg  := '�ϸ�֤ӳ�����'|| sqlerrm;
         RETURN;
END pro_rwa_cd_guaranty_qualified;
/

