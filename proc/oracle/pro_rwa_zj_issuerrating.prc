CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_ZJ_ISSUERRATING(
                            p_data_dt_str IN  VARCHAR2,   --�������� yyyyMMdd
                            p_po_rtncode  OUT VARCHAR2,   --���ر�� 1 �ɹ�,0 ʧ��
                            p_po_rtnmsg   OUT VARCHAR2    --��������
        )
  /*
    �洢��������:RWA_DEV.PRO_RWA_ZJ_ISSUERRATING
    ʵ�ֹ���:�г�����-�ʽ�ϵͳ-������������Ϣ��
    ���ݿھ�:ȫ��
    ����Ƶ��:�³�����
    ��  ��  :V1.0.0
    ��д��  :CHENGANG
    ��дʱ��:2019-04-18
    ��  λ  :�Ϻ���˶��Ϣ�����ɷ����޹�˾
    Դ  ��1 :RWA_DEV.BRD_BOND|ծ����Ϣ��
    Դ  ��2 :RWA_DEV.NCM_CUSTOMER_RATING|�ͻ��ⲿ������
    Դ  ��3 :RWA_DEV.NCM_CUSTOMER_INFO|�ͻ���Ϣ��
    Դ  ��4 :RWA_DEV.RWA_CD_CODE_MAPPING\����ӳ��ת����
    Դ  ��4 :RWA_DEV.RWA_CD_RATING_MAPPING\�ⲿ���������ת����
    �����¼(�޸���|�޸�ʱ��|�޸�����):
    pxl 20190905 ����������������Ϣ��  һ���������ж�һ��� ����������
    */
  AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_ZJ_ISSUERRATING';
  --�����쳣����
  v_raise EXCEPTION;
  --���嵱ǰ����ļ�¼��
  v_count INTEGER;
  --v_count1 INTEGER;

  BEGIN
    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*�����ȫ�����ݼ��������Ŀ���*/
    --1.���Ŀ����е�ԭ�м�¼
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_ZJ_ISSUERRATING';
    
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.CREDIT_RATING_TMEP';

    --2.���������������ݴ�Դ����뵽Ŀ�����
    INSERT INTO CREDIT_RATING_TMEP
      (BOND_ID, ORG_CD, RATING_DATE, CREDIT_RATING)
    SELECT R.BOND_ID, 
             CASE 
              WHEN ORG_NAME = '�г��Ź������������������ι�˾' THEN '001'
              WHEN ORG_NAME = '�г���֤ȯ�������޹�˾' THEN '001'
              WHEN ORG_NAME = '��֤��Ԫ���������ɷ����޹�˾' THEN '002'  
              WHEN ORG_NAME = '��Ԫ�����������޹�˾' THEN '002' 
              WHEN ORG_NAME = '���������������޹�˾' THEN '003'
              WHEN ORG_NAME = '���������������޹�˾' THEN '003'
              WHEN ORG_NAME = '�󹫹��������������޹�˾' THEN '004'
              WHEN ORG_NAME = '������Ϲ��������������޹�˾' THEN '005'
              WHEN ORG_NAME = '��ծ���������������ι�˾' THEN '006'
              WHEN ORG_NAME = '�Ϻ���������������Ͷ�ʷ������޹�˾' THEN '007'
              WHEN ORG_NAME = 'Զ�������������޹�˾' THEN '008'                                                                          
              ELSE '099'
            END AS ORG_CD, 
            MAX(R.RATING_DATE) RATING_DATE, 
            CASE 
              WHEN R.CREDIT_RATING = 'AAA+' THEN '0101'
              WHEN R.CREDIT_RATING = 'AAA' THEN '0101'
              WHEN R.CREDIT_RATING = 'AAA-' THEN '0101'
              WHEN R.CREDIT_RATING = 'AA+' THEN '0102'
              WHEN R.CREDIT_RATING = 'AA' THEN '0102'
              WHEN R.CREDIT_RATING = 'AA-' THEN '0102'
              WHEN R.CREDIT_RATING = 'A+' THEN '0105'
              WHEN R.CREDIT_RATING = 'A' THEN '0106'
              WHEN R.CREDIT_RATING = 'A-' THEN '0107'
              WHEN R.CREDIT_RATING = 'BBB+' THEN '0108'
              WHEN R.CREDIT_RATING = 'BBB' THEN '0109'
              WHEN R.CREDIT_RATING = 'BBB-' THEN '0110'
              WHEN R.CREDIT_RATING = 'BB+' THEN '0111'
              WHEN R.CREDIT_RATING = 'BB' THEN '0112'
              WHEN R.CREDIT_RATING = 'BB-' THEN '0113'
              WHEN R.CREDIT_RATING = 'B+' THEN '0114'
              WHEN R.CREDIT_RATING = 'B' THEN '0115'
              WHEN R.CREDIT_RATING = 'B-' THEN '0116'
              WHEN R.CREDIT_RATING = 'CCC+' THEN '0117'
              WHEN R.CREDIT_RATING = 'CCC' THEN '0118'
              WHEN R.CREDIT_RATING = 'CCC-' THEN '0119'
              WHEN R.CREDIT_RATING = 'CC+' THEN '0120'
              WHEN R.CREDIT_RATING = 'CC' THEN '0120'
              WHEN R.CREDIT_RATING = 'CC-' THEN '0120'
              WHEN R.CREDIT_RATING = 'C+' THEN '0121'
              WHEN R.CREDIT_RATING = 'C' THEN '0121'
              WHEN R.CREDIT_RATING = 'C-' THEN '0121'
              WHEN R.CREDIT_RATING = 'A-1' THEN '0201'
              WHEN R.CREDIT_RATING = 'A-2' THEN '0202'
              WHEN R.CREDIT_RATING = 'A-3' THEN '0203'
              WHEN R.CREDIT_RATING = 'D' THEN '0207'
              ELSE '0207'
            END AS CREDIT_RATING
      FROM BRD_CREDIT_RATING R
       WHERE R.DATANO = p_data_dt_str
         AND R.BELONG_GROUP = '2'
         AND R.RATING_TYPE = 'S' --  S  1  ��������  C  2  �������� I  3  �ڲ����� Z  4  ծ������         
      GROUP BY R.BOND_ID, 
             CASE 
              WHEN ORG_NAME = '�г��Ź������������������ι�˾' THEN '001'
              WHEN ORG_NAME = '�г���֤ȯ�������޹�˾' THEN '001'
              WHEN ORG_NAME = '��֤��Ԫ���������ɷ����޹�˾' THEN '002'  
              WHEN ORG_NAME = '��Ԫ�����������޹�˾' THEN '002' 
              WHEN ORG_NAME = '���������������޹�˾' THEN '003'
              WHEN ORG_NAME = '���������������޹�˾' THEN '003'
              WHEN ORG_NAME = '�󹫹��������������޹�˾' THEN '004'
              WHEN ORG_NAME = '������Ϲ��������������޹�˾' THEN '005'
              WHEN ORG_NAME = '��ծ���������������ι�˾' THEN '006'
              WHEN ORG_NAME = '�Ϻ���������������Ͷ�ʷ������޹�˾' THEN '007'
              WHEN ORG_NAME = 'Զ�������������޹�˾' THEN '008'                                                                          
              ELSE '099'
            END, 
            CASE 
              WHEN R.CREDIT_RATING = 'AAA+' THEN '0101'
              WHEN R.CREDIT_RATING = 'AAA' THEN '0101'
              WHEN R.CREDIT_RATING = 'AAA-' THEN '0101'
              WHEN R.CREDIT_RATING = 'AA+' THEN '0102'
              WHEN R.CREDIT_RATING = 'AA' THEN '0102'
              WHEN R.CREDIT_RATING = 'AA-' THEN '0102'
              WHEN R.CREDIT_RATING = 'A+' THEN '0105'
              WHEN R.CREDIT_RATING = 'A' THEN '0106'
              WHEN R.CREDIT_RATING = 'A-' THEN '0107'
              WHEN R.CREDIT_RATING = 'BBB+' THEN '0108'
              WHEN R.CREDIT_RATING = 'BBB' THEN '0109'
              WHEN R.CREDIT_RATING = 'BBB-' THEN '0110'
              WHEN R.CREDIT_RATING = 'BB+' THEN '0111'
              WHEN R.CREDIT_RATING = 'BB' THEN '0112'
              WHEN R.CREDIT_RATING = 'BB-' THEN '0113'
              WHEN R.CREDIT_RATING = 'B+' THEN '0114'
              WHEN R.CREDIT_RATING = 'B' THEN '0115'
              WHEN R.CREDIT_RATING = 'B-' THEN '0116'
              WHEN R.CREDIT_RATING = 'CCC+' THEN '0117'
              WHEN R.CREDIT_RATING = 'CCC' THEN '0118'
              WHEN R.CREDIT_RATING = 'CCC-' THEN '0119'
              WHEN R.CREDIT_RATING = 'CC+' THEN '0120'
              WHEN R.CREDIT_RATING = 'CC' THEN '0120'
              WHEN R.CREDIT_RATING = 'CC-' THEN '0120'
              WHEN R.CREDIT_RATING = 'C+' THEN '0121'
              WHEN R.CREDIT_RATING = 'C' THEN '0121'
              WHEN R.CREDIT_RATING = 'C-' THEN '0121'
              WHEN R.CREDIT_RATING = 'A-1' THEN '0201'
              WHEN R.CREDIT_RATING = 'A-2' THEN '0202'
              WHEN R.CREDIT_RATING = 'A-3' THEN '0203'
              WHEN R.CREDIT_RATING = 'D' THEN '0207'
              ELSE '0207'
            END
     ;
         
    COMMIT;
    
    
    
    
    --�������������
    INSERT INTO RWA_DEV.RWA_ZJ_ISSUERRATING
      (DATADATE, --��������
       ISSUERID, --������ID
       ISSUERNAME, --����������
       RATINGORG, --��������
       RATINGRESULT, --�������
       RATINGDATE, --��������
       FETCHFLAG --ȡ����ʶ
      )
    SELECT DISTINCT TO_DATE(p_data_dt_str, 'YYYYMMDD'), --��������                    
           NVL(T2.ISSUER_CODE, T7.CUSTOMERID),    --������ID
           T2.ISSUER_NAME,   --����������
           NVL(T5.ORG_CD, 'WPJ'), --��������
           T5.CREDIT_RATING, --�������
           T5.RATING_DATE, --��������
           '1'    --ȡ����ʶ          
       FROM CREDIT_RATING_TMEP T5
   INNER JOIN BRD_SECURITY_POSI T1 --ծȯͷ����Ϣ
           ON T5.BOND_ID = T1.SECURITY_REFERENCE
          AND T1.DATANO = p_data_dt_str
   INNER JOIN BRD_BOND T2 --ծȯ
           ON T1.SECURITY_REFERENCE = T2.BOND_ID
          AND T2.DATANO = p_data_dt_str
          AND T2.BELONG_GROUP = '4' --�ʽ�ϵͳ       
   LEFT JOIN RWA_DEV.NCM_CUSTOMER_INFO T7 
          ON DECODE(T2.ISSUER_NAME, '�����г��н���Ͷ�ʿ������޹�˾', '�����г��н���Ͷ�ʿ�����˾', T2.ISSUER_NAME) = T7.CUSTOMERNAME --���������⴦��
          AND T7.DATANO = p_data_dt_str
          AND T7.CUSTOMERID <> 'ty2018120600000001' --�л����񹲺͹������� ���⴦��                       
    WHERE T1.SBJT_CD = '11010101'  --�Թ��ʼ�ֵ��������䶯���뵱������Ľ����ʲ�         
      AND T2.BOND_TYPE NOT IN ('TTC')   --�ų��ǹ�ծ  TTC �����ʱ�����
      ;
        
    COMMIT;
       
    

    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_ZJ_ISSUERRATING',cascade => true);

    /*Ŀ�������ͳ��*/
    --ͳ�Ʋ���ļ�¼��
    SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_ZJ_ISSUERRATING;

    p_po_rtncode := '1';
    p_po_rtnmsg  := '�ɹ�' || '-' || v_count;
    --�����쳣
    EXCEPTION
    WHEN OTHERS THEN
         --Dbms_output.Put_line('������,�������Ϊ:'||sqlcode||';������ϢΪ:'||sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace);
         ROLLBACK;
         p_po_rtncode := sqlcode;
         p_po_rtnmsg  := '������������Ϣ��('|| v_pro_name ||')ETLת��ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_ZJ_ISSUERRATING;
/

