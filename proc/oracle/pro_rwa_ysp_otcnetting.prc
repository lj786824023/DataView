CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_YSP_OTCNETTING(P_DATA_DT_STR IN VARCHAR2, --�������� yyyyMMdd
                                                   P_PO_RTNCODE  OUT VARCHAR2, --���ر�� 1 �ɹ�,0 ʧ��
                                                   P_PO_RTNMSG   OUT VARCHAR2 --��������
                                                   )
/*
  �洢��������:RWA_DEV.PRO_RWA_YSP_OTCNETTING
  ʵ�ֹ���:����ϵͳ-����Ʒҵ��-�����������߾�������
  ���ݿھ�:ȫ��
  ����Ƶ��:�³�����
  ��  ��  :V1.0.0
  ��д��  :CHENGANG
  ��дʱ��:2019-04-17
  ��  λ  :�Ϻ���˶��Ϣ�����ɷ����޹�˾
  Դ  ��1 :RWA_DEV.BRD_SWAP|������
  Դ  ��2 :RWA.ORG_INFO|������Ϣ��
  Դ  ��3 :RWA_DEV.NCM_CUSTOMER_INFO|�ͻ���Ϣ��
  Դ  ��4 :RWA.CODE_LIBRARY|������
  �����¼(�޸���|�޸�ʱ��|�޸�����):
  
  */
 AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  V_PRO_NAME VARCHAR2(200) := 'RWA_DEV.RWA_YSP_OTCNETTING';
  --�����쳣����
  V_RAISE EXCEPTION;
  --���嵱ǰ����ļ�¼��
  V_COUNT INTEGER;

BEGIN
  --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

  /*�����ȫ�����ݼ��������Ŀ���*/
  --1.���Ŀ����е�ԭ�м�¼
  EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_YSP_OTCNETTING';
/*
  --2.���������������ݴ�Դ����뵽Ŀ�����
  INSERT INTO RWA_DEV.RWA_YSP_OTCNETTING
    (DATADATE, --��������
     DATANO, --������ˮ��
     VALIDNETAGREEMENTID, --��Ч�������Э��ID
     COUNTERPARTYID, --���׶���ID
     ORGSORTNO, --���������
     ORGID, --��������ID
     ORGNAME, --������������
     INDUSTRYID, --������ҵ����
     INDUSTRYNAME, --������ҵ����
     BUSINESSLINE, --����
     ASSETTYPE, --�ʲ�����
     ASSETSUBTYPE, --�ʲ�С��
     BUSINESSTYPESTD, --Ȩ�ط�ҵ������
     EXPOCLASSSTD, --Ȩ�ط���¶����
     EXPOSUBCLASSSTD, --Ȩ�ط���¶С��
     EXPOCLASSIRB, --��������¶����
     EXPOSUBCLASSIRB, --��������¶С��
     BOOKTYPE, --�˻����
     REPOTRANFLAG, --�ع����ױ�ʶ
     CLAIMSLEVEL, --ծȨ����
     ORIGINALMATURITY, --ԭʼ����
     PRINCIPAL, --���屾��
     IRATING, --�ڲ�����
     PD, --ΥԼ����
     GROUPID --������
     )
    SELECT TO_DATE(p_data_dt_str, 'YYYYMMDD'), --01��������
           p_data_dt_str, --02������ˮ��
           'JEJS' || T1.DEALNO || T1.SEQ, --��Ч�������Э��ID
           NVL(T3.TAXID, 'OPI' || T2.CNO), --���׶���ID
           '1290', --���������
           '6001', --��������ID
           '�������йɷ����޹�˾����ҵ��', --������������
           'J6621', --������ҵ����
           '��ҵ���з���', --������ҵ����
           '0102', --ҵ������
           '223', --�ʲ�����
           '22301', --�ʲ�С��
           '01', --Ȩ�ط�ҵ������
           '', --Ȩ�ط���¶����
           '', --Ȩ�ط���¶С��
           '', --��������¶����
           '', --��������¶С��
           CASE
             WHEN SUBSTR(T2.COST, 1, 4) = '3' THEN
              '01'
             ELSE
              '02'
           END, --�˻����
           '0', --�ع����ױ�ʶ
           '02', --ծȨ����
           CASE
             WHEN (TO_DATE(T2.MATDATE, 'YYYY-MM-DD') -
                  TO_DATE(T2.STARTDATE, 'YYYY-MM-DD')) / 365 < 0 THEN
              0
             ELSE
              (TO_DATE(T2.MATDATE, 'YYYY-MM-DD') -
              TO_DATE(T2.STARTDATE, 'YYYY-MM-DD')) / 365
           END, --ԭʼ����
           T1.NOTCCYAMT, --���屾��
           '', --�ڲ�����
           '', --ΥԼ����
           '' --������
      FROM RWA_DEV.OPI_SWDT T1 --��������
      LEFT JOIN RWA_DEV.OPI_SWDH T2 --������ͷ 
        ON T1.DEALNO = T2.DEALNO
       AND T2.DATANO = p_data_dt_str
      LEFT JOIN RWA_DEV.OPI_CUST T3 --�ͻ���Ϣ
        ON T2.CNO = T3.CNO
       AND T3.DATANO = p_data_dt_str
     WHERE T1.DATANO = p_data_dt_str;

  COMMIT;*/

  DBMS_STATS.GATHER_TABLE_STATS(OWNNAME => 'RWA_DEV',
                                TABNAME => 'RWA_YSP_OTCNETTING',
                                CASCADE => TRUE);

  /*Ŀ�������ͳ��*/
  --ͳ�Ʋ���ļ�¼��
  SELECT COUNT(1) INTO V_COUNT FROM RWA_DEV.RWA_YSP_OTCNETTING;
  --Dbms_output.Put_line('RWA_DEV.RWA_TZ_CONTRACT��ǰ����Ĳ���ϵͳ-Ӧ�տ�Ͷ�����ݼ�¼Ϊ: ' || (v_count1 - v_count) || ' ��');
  --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̽��� ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

  P_PO_RTNCODE := '1';
  P_PO_RTNMSG  := '�ɹ�' || '-' || V_COUNT;
  --�����쳣
EXCEPTION
  WHEN OTHERS THEN
    --Dbms_output.Put_line('������,�������Ϊ:'||sqlcode||';������ϢΪ:'||sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace);
    ROLLBACK;
    P_PO_RTNCODE := SQLCODE;
    P_PO_RTNMSG  := '��ͬ��Ϣ(' || V_PRO_NAME || ')ETLת��ʧ�ܣ�' || SQLERRM ||
                    ';��������Ϊ:' || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
    RETURN;
END PRO_RWA_YSP_OTCNETTING;
/

