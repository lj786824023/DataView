CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_YSP_OTCCOUNTERPARTY(p_data_dt_str IN VARCHAR2, --�������� yyyyMMdd
                                                        p_po_rtncode  OUT VARCHAR2, --���ر�� 1 �ɹ�,0 ʧ��
                                                        p_po_rtnmsg   OUT VARCHAR2 --��������
                                                        )
/*
  �洢��������:RWA_DEV.PRO_RWA_YSP_OTCCOUNTERPARTY
  ʵ�ֹ���:����ϵͳ-����Ʒҵ��-�����������߽��׶��ֱ�
  ���ݿھ�:ȫ��
  ����Ƶ��:�³�����
  ��  ��  :V1.0.0
  ��д��  :CHENGANG
  ��дʱ��:2019-04-18
  ��  λ  :�Ϻ���˶��Ϣ�����ɷ����޹�˾
  Դ  ��1 :RWA_DEV.BRD_SWAP|������
  Դ  ��2 :RWA.ORG_INFO|������Ϣ��
  Դ  ��3 :RWA_DEV.NCM_CUSTOMER_INFO|�ͻ���Ϣ��
  �����¼(�޸���|�޸�ʱ��|�޸�����):
  chengang 2019/04/23 ����RWA_DEV.BRD_SWAP|������Ļ�����
  */
 AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_YSP_OTCCOUNTERPARTY';
  --�����쳣����
  v_raise EXCEPTION;
  --���嵱ǰ����ļ�¼��
  v_count INTEGER;

BEGIN
  --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

  /*�����ȫ�����ݼ��������Ŀ���*/
  --1.���Ŀ����е�ԭ�м�¼
  EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_YSP_OTCCOUNTERPARTY';
  
  --������Ч������������ҵ���µĽ��׶���
  INSERT INTO RWA_DEV.RWA_YSP_OTCCOUNTERPARTY
    (DATADATE,                    --��������
     DATANO,                      --������ˮ��
     NETTINGFLAG,                 --��������ʶ
     COUNTERPARTYID,              --���׶���ID
     COUNTERPARTYNAME,            --���׶�������
     ORGSORTNO,                   --���������
     ORGID,                       --��������ID
     ORGNAME,                     --������������
     CPERATING                    --���׶����ⲿ����
     )
    SELECT DISTINCT
         TO_DATE(p_data_dt_str, 'YYYYMMDD') AS DATADATE --��������
        ,p_data_dt_str
        ,'0'
        ,T1.CLIENTID AS CUSTOMERID --�ͻ����
        ,T2.CFN1 AS CUSTOMERNAME --�ͻ�����
        ,'1290'
        ,'6001'
        ,'�������йɷ����޹�˾����ҵ��'
        ,NVL(T3.CREDITLEVEL, '0207') AS CREDITLEVEL --������Ϣ Ĭ�ϣ�0207 δ����
    FROM RWA_DEV.RWA_YSP_EXPOSURE T1 --����Ʒ��¶��
    LEFT JOIN RWA_DEV.OPI_CUST T2 
           ON T1.CLIENTID = 'OPI' || T2.CNO
          AND T2.DATANO = p_data_dt_str
    LEFT JOIN RWA.RWA_WS_KHPJ_BL T3 --���׶����ⲿ������¼��
      ON TRIM(T3.CUSTOMERID) = T1.CLIENTID
     AND T3.DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD')
    WHERE T1.DATANO = p_data_dt_str
    ;
 
   COMMIT;

 
  dbms_stats.gather_table_stats(ownname => 'RWA_DEV',
                                tabname => 'RWA_YSP_OTCCOUNTERPARTY',
                                cascade => true);

  /*Ŀ�������ͳ��*/
  --ͳ�Ʋ���ļ�¼��
  SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_YSP_OTCCOUNTERPARTY;

  --Dbms_output.Put_line('RWA_DEV.RWA_TZ_CONTRACT��ǰ����Ĳ���ϵͳ-Ӧ�տ�Ͷ�����ݼ�¼Ϊ: ' || (v_count1 - v_count) || ' ��');
  --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̽��� ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

  p_po_rtncode := '1';
  p_po_rtnmsg  := '�ɹ�' || '-' || v_count;
  --�����쳣
EXCEPTION
  WHEN OTHERS THEN
    --Dbms_output.Put_line('������,�������Ϊ:'||sqlcode||';������ϢΪ:'||sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace);
    ROLLBACK;
    p_po_rtncode := sqlcode;
    p_po_rtnmsg  := '��ͬ��Ϣ(' || v_pro_name || ')ETLת��ʧ�ܣ�' || sqlerrm ||
                    ';��������Ϊ:' || dbms_utility.format_error_backtrace;
    RETURN;
END PRO_RWA_YSP_OTCCOUNTERPARTY;
/

