CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_EI_OTCCOUNTERPARTY(
			 											P_DATA_DT_STR	IN	VARCHAR2,		--��������
       											P_PO_RTNCODE	OUT	VARCHAR2,		--���ر�� 1 �ɹ�,0 ʧ��
														P_PO_RTNMSG		OUT	VARCHAR2		--��������
				)
  /*
    �洢��������:RWA_DEV.PRO_RWA_EI_OTCCOUNTERPARTY
    ʵ�ֹ���:�����������߽��׶��ֱ�,���볡���������߽��׶��ֱ�
    ���ݿھ�:ȫ��
    ����Ƶ��:�³�
    ��  ��  :V1.0.0
    ��д��  :CHENGANG
    ��дʱ��:2019-04-19
    ��  λ	:�Ϻ���˶��Ϣ�����ɷ����޹�˾
    Դ  ��1	:RWA_DEV.RWA_ABS_INVEST_OTCCOUNTERPARTY|�����������߽��׶��ֱ�
    Ŀ���  :RWA_DEV.RWA_EI_OTCCOUNTERPARTY|�����������߽��׶��ֱ�
    ������  :��
    �����¼(�޸���|�޸�ʱ��|�޸�����)��
  	*/
  AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  V_PRO_NAME VARCHAR2(200) := 'RWA_DEV.PRO_RWA_EI_OTCCOUNTERPARTY';
  --�����ж�ֵ����
  V_COUNT INTEGER;
  --�����쳣����
  V_RAISE EXCEPTION;

  BEGIN
    --DBMS_OUTPUT.PUT_LINE('��ִ�� ' || V_PRO_NAME || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    DELETE FROM RWA_DEV.RWA_EI_OTCCOUNTERPARTY WHERE DATADATE = TO_DATE(P_DATA_DT_STR, 'YYYY/MM/DD');
    
    COMMIT;
    

    /*���볡���������߽��׶��ֱ�*/
    INSERT INTO RWA_DEV.RWA_EI_OTCCOUNTERPARTY(
              DATADATE,          --��������
              DATANO,          --������ˮ��
              NETTINGFLAG,          --��������ʶ
              COUNTERPARTYID,          --���׶���ID
              COUNTERPARTYNAME,          --���׶�������
              ORGSORTNO,          --���������
              ORGID,          --��������ID
              ORGNAME,          --������������
              CPERATING          --���׶����ⲿ����
              )
          SELECT DATADATE         AS DATADATE, --��������
                 DATANO           AS DATANO, --������ˮ��
                 NETTINGFLAG      AS NETTINGFLAG, --��������ʶ
                 COUNTERPARTYID   AS COUNTERPARTYID, --���׶���ID
                 COUNTERPARTYNAME AS COUNTERPARTYNAME, --���׶�������
                 ORGSORTNO        AS ORGSORTNO, --���������
                 ORGID            AS ORGID, --��������ID
                 ORGNAME          AS ORGNAME, --������������
                 CPERATING        AS CPERATING --���׶����ⲿ����
            FROM RWA_DEV.RWA_YSP_OTCCOUNTERPARTY
           WHERE DATADATE = TO_DATE(P_DATA_DT_STR, 'YYYYMMDD');
   
    COMMIT;


    /*Ŀ�������ͳ��*/
    --ͳ�Ʋ���ļ�¼
    SELECT COUNT(1) INTO V_COUNT FROM RWA_DEV.RWA_EI_OTCCOUNTERPARTY WHERE DATANO = P_DATA_DT_STR;
    --DBMS_OUTPUT.PUT_LINE('RWA_DEV.RWA_EI_OTCCounterparty��ǰ��������ݼ�¼Ϊ:' || V_COUNT1 || '��');

    --DBMS_OUTPUT.PUT_LINE('��ִ�� ' || V_PRO_NAME || ' �洢���̽��� ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    P_PO_RTNCODE := '1';
    P_PO_RTNMSG  := '�ɹ�'||'-'||V_COUNT;
    --�����쳣
    EXCEPTION
    WHEN OTHERS THEN
         --DBMS_OUTPUT.PUT_LINE('������,�������Ϊ:'||SQLCODE||';������ϢΪ:'||SQLERRM||';��������Ϊ:'||DBMS_UTILITY.FORMAT_ERROR_BACKTRACE);
          ROLLBACK;
          P_PO_RTNCODE := SQLCODE;
          P_PO_RTNMSG  := '�ʲ�֤ȯ���ĺ�Լ�����Ϣ��('|| v_pro_name ||')ETLת��ʧ�ܣ�'|| SQLERRM||';��������Ϊ:'||DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
         RETURN;
END PRO_RWA_EI_OTCCOUNTERPARTY;
/

