CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_YSP_CONTRACT(P_DATA_DT_STR IN VARCHAR2, --�������� yyyyMMdd
                                                      P_PO_RTNCODE  OUT VARCHAR2, --���ر�� 1 �ɹ�,0 ʧ��
                                                      P_PO_RTNMSG   OUT VARCHAR2 --��������
                                                      )
/*
  �洢��������:RWA_DEV.PRO_RWA_YSP_CONTRACT
  ʵ�ֹ���:����ϵͳ-����Ʒҵ��-��ͬ��Ϣ(������Դ����ϵͳ����ͬ�����Ϣ����RWA����Ʒ�ӿڱ��ͬ��Ϣ����)
  ���ݿھ�:ȫ��
  ����Ƶ��:�³�����
  ��  ��  :V1.0.0
  ��д��  :
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
  V_PRO_NAME VARCHAR2(200) := 'RWA_DEV.PRO_RWA_YSP_CONTRACT';
  --�����쳣����
  V_RAISE EXCEPTION;
  --���嵱ǰ����ļ�¼��
  V_COUNT INTEGER;

BEGIN
  --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

  /*�����ȫ�����ݼ��������Ŀ���*/
  --1.���Ŀ����е�ԭ�м�¼
  EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_YSP_CONTRACT';

  --2.���������������ݴ�Դ����뵽Ŀ�����
  INSERT INTO RWA_DEV.RWA_YSP_CONTRACT
    (DATADATE, --01��������
     DATANO, --02������ˮ��
     CONTRACTID, --03��ͬID
     SCONTRACTID, --04Դ��ͬID
     SSYSID, --05ԴϵͳID
     CLIENTID, --06��������ID
     SORGID, --07Դ����ID
     SORGNAME, --08Դ��������
     ORGSORTNO, --09�������������
     ORGID, --10��������ID
     ORGNAME, --11������������
     INDUSTRYID, --12������ҵ����
     INDUSTRYNAME, --13������ҵ����
     BUSINESSLINE, --14ҵ������
     ASSETTYPE, --15�ʲ�����
     ASSETSUBTYPE, --16�ʲ�С��
     BUSINESSTYPEID, --17ҵ��Ʒ�ִ���
     BUSINESSTYPENAME, --18ҵ��Ʒ������
     CREDITRISKDATATYPE, --19���÷�����������
     STARTDATE, --20��ʼ����
     DUEDATE, --21��������
     ORIGINALMATURITY, --22ԭʼ����
     RESIDUALM, --23ʣ������
     SETTLEMENTCURRENCY, --24�������
     CONTRACTAMOUNT, --25��ͬ�ܽ��
     NOTEXTRACTPART, --26��ͬδ��ȡ����
     UNCONDCANCELFLAG, --27�Ƿ����ʱ����������
     ABSUAFLAG, --28�ʲ�֤ȯ�������ʲ���ʶ
     ABSPOOLID, --29֤ȯ���ʲ���ID
     GROUPID, --30������
     GUARANTEETYPE, --31��Ҫ������ʽ
     ABSPROPORTION --32�ʲ�֤ȯ������
     )
    SELECT TO_DATE(P_DATA_DT_STR, 'YYYYMMDD'), --01��������
           P_DATA_DT_STR, --02������ˮ��
           T1.CONTRACTID, --03��ͬID
           T1.CONTRACTID, --04Դ��ͬID
           'YSP', --05ԴϵͳID
           T1.CLIENTID, --06��������ID
           T1.SORGID, --07Դ����ID
           T1.SORGNAME, --08Դ��������
           T1.ORGSORTNO, --09�������������
           T1.ORGID, --10��������ID
           T1.ORGNAME, --11������������
           T1.INDUSTRYID, --12������ҵ����
           T1.INDUSTRYNAME, --13������ҵ����
           T1.BUSINESSLINE, --14ҵ������  0401:ͬҵ-�����г���
           T1.ASSETTYPE, --15�ʲ�����
           T1.ASSETSUBTYPE, --16�ʲ�С��
           T1.BUSINESSTYPEID, --17ҵ��Ʒ�ִ���
           T1.BUSINESSTYPENAME, --18ҵ��Ʒ������
           '07', --19���÷����������� 07:���뽻�׶���
           T1.STARTDATE, --20��ʼ����
           T1.DUEDATE, --21��������
           T1.ORIGINALMATURITY, --22ԭʼ����
           T1.RESIDUALM, --23ʣ������
           T1.CURRENCY, --24�������
           T1.NORMALPRINCIPAL, --25��ͬ�ܽ��
           0, --26��ͬδ��ȡ����
           '0', --27�Ƿ����ʱ����������
           '0', --28�ʲ�֤ȯ�������ʲ���ʶ
           '', --29֤ȯ���ʲ���ID
           '', --30������
           '', --31��Ҫ������ʽ
           0 --32�ʲ�֤ȯ������
      FROM RWA_YSP_EXPOSURE T1
     WHERE T1.DATANO = P_DATA_DT_STR;

  COMMIT;

  DBMS_STATS.GATHER_TABLE_STATS(OWNNAME => 'RWA_DEV',
                                TABNAME => 'RWA_YSP_CONTRACT',
                                CASCADE => TRUE);

  /*Ŀ�������ͳ��*/
  --ͳ�Ʋ���ļ�¼��
  SELECT COUNT(1) INTO V_COUNT FROM RWA_DEV.RWA_YSP_CONTRACT;
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
END PRO_RWA_YSP_CONTRACT;
/

