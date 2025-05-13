CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_ZJ_BONDINFO(p_data_dt_str IN VARCHAR2, --�������� yyyyMMdd
                                                p_po_rtncode  OUT VARCHAR2, --���ر�� 1 �ɹ�,0 ʧ��
                                                p_po_rtnmsg   OUT VARCHAR2 --��������
                                                )
/*
  �洢��������:RWA_DEV.PRO_RWA_ZJ_BONDINFO
  ʵ�ֹ���:�г�����-�ʽ�ϵͳ-ծȯ��Ϣ��  
  ���ݿھ�:ȫ��
  ����Ƶ��:�³�����
  ��  ��  :V1.0.0
  ��д��  :CHENGANG
  ��дʱ��:2019-04-18
  ��  λ  :�Ϻ���˶��Ϣ�����ɷ����޹�˾
  Դ  ��1 :RWA_DEV.BRD_BOND|ծ����Ϣ��
  Դ  ��2 :RWA_DEV.NCM_BOND_INFO|ծ����Ϣ
  Դ  ��3 :RWA_DEV.NCM_CUSTOMER_INFO|�ͻ���Ϣ��
  Դ  ��4 :RWA_DEV.RWA_CD_CODE_MAPPING\����ӳ��ת����
  Դ  ��5 :RWA_DEV.BRD_CREDIT_RATING\ծ������
  �����¼(�޸���|�޸�ʱ��|�޸�����):
  pxl 2019/09/05 ծȯ��Ϣ�߼�����
  */
 AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_ZJ_BONDINFO';
  --�����쳣����
  v_raise EXCEPTION;
  --���嵱ǰ����ļ�¼��
  v_count INTEGER;
  --v_count1 INTEGER;

BEGIN
  --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

  /*�����ȫ�����ݼ��������Ŀ���*/
  --1.���Ŀ����е�ԭ�м�¼
  EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_ZJ_BONDINFO';
  
  --1.1 ծȯ���������ⲿ������ʱ��
  EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.CREDIT_RATING_TMEP';
  
  --1.2 ծȯ������ʱ��
  EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.CREDIT_RATING_ZQ_TMEP';
  
  --1.3 �ӹ�ծȯ���������ⲿ������Ϣ
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

  --1.4 �ӹ�ծȯ������Ϣ
  INSERT INTO CREDIT_RATING_ZQ_TMEP
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
            R.RATING_DATE, 
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
         AND R.RATING_TYPE = 'Z' --  S  1  ��������  C  2  �������� I  3  �ڲ����� Z  4  ծ������         
   ;
   
   COMMIT; 

  --2.1���������������ݴ�Դ����뵽Ŀ�����  
   INSERT INTO RWA_DEV.RWA_ZJ_BONDINFO
    (
     DATADATE, --��������
     BONDID, --ծȯID
     BONDNAME, --ծȯ����
     BONDTYPE, --ծȯ����
     ERATING, --�ⲿ����
     ISSUERID, --������ID
     ISSUERNAME, --����������
     ISSUERTYPE, --�����˴���
     ISSUERSUBTYPE, --������С��
     ISSUERREGISTSTATE, --������ע�����
     ISSUERSMBFLAG, --������С΢��ҵ��ʶ
     BONDISSUEINTENT, --ծȯ����Ŀ��
     REABSFLAG, --���ʲ�֤ȯ����ʶ
     ORIGINATORFLAG, --�Ƿ������
     STARTDATE, --��ʼ����
     DUEDATE, --��������
     ORIGINALMATURITY, --ԭʼ����
     RESIDUALM, --ʣ������
     RATETYPE, --��������
     EXECUTIONRATE, --ִ������
     NEXTREPRICEDATE, --�´��ض�����
     NEXTREPRICEM, --�´��ض�������
     MODIFIEDDURATION, --��������
     DENOMINATION, --���
     CURRENCY, --����
     SECURITY_REFERENCE  --֤ȯΨһ��ʾ
    ) 
     SELECT TO_DATE(p_data_dt_str, 'YYYYMMDD'), --��������         
           T1.ACCT_NO,  --ծȯID
           T2.BOND_FULL_NAME, --ծȯ����
           CASE
            WHEN T2.BOND_TYPE = 'TB' THEN '01'  --TB	��ծ             
            WHEN T2.BOND_TYPE = 'TBB' THEN '01'  --TBB	����Ʊ��
            WHEN T2.BOND_TYPE = 'PBB' THEN '01'  --PBB	����������ծȯ
            WHEN T2.BOND_TYPE = 'ABS' THEN '03'  --ABS	�ʲ�֧��֤ȯ            
            ELSE CASE 
                    WHEN T6.BOND_ID IS NOT NULL OR T6.BOND_ID <> '' THEN '02' -- ���������������BB+���ⲿ������Ϣ�� �ϸ�֤ȯ
                    ELSE '09'		--09	����֤ȯ
                 END         
           END, --ծȯ����
           T5.CREDIT_RATING, --�ⲿ����
           NVL(T2.ISSUER_CODE, T7.CUSTOMERID),    --������ID
           T2.ISSUER_NAME,   --����������
           '',       --�����˴��� ����������ͨ���ͻ����͹���ӹ�
           '',       --������С�� ����������ͨ���ͻ����͹���ӹ�
           '01' ,   --������ע�����
           NVL(T7.ISSUPERVISESTANDARSMENT, '0'),  --������С΢��ҵ��ʶ  Ĭ�� ��
           '02', --ծȯ����Ŀ��  Ĭ�� 02 ����
           '0',   --���ʲ�֤ȯ����ʶ
           '0',   --�Ƿ������
           T2.ISSUE_DATE,  --��ʼ����
           T2.MATU_DT,     --��������
           CASE
             WHEN (TO_DATE(T2.MATU_DT, 'YYYY-MM-DD') -
                  TO_DATE(T2.ISSUE_DATE, 'YYYY-MM-DD')) / 365 < 0 THEN
              0
             ELSE
              (TO_DATE(T2.MATU_DT, 'YYYY-MM-DD') -
              TO_DATE(T2.ISSUE_DATE, 'YYYY-MM-DD')) / 365
           END,  --ԭʼ����
           CASE
             WHEN (TO_DATE(T2.MATU_DT, 'YYYY-MM-DD') -
                  TO_DATE(p_data_dt_str, 'YYYYMMDD')) / 365 < 0 THEN
              0
             ELSE
              (TO_DATE(T2.MATU_DT, 'YYYY-MM-DD') -
              TO_DATE(p_data_dt_str, 'YYYYMMDD')) / 365
           END,  --ʣ������
           CASE 
             WHEN T2.RATE_TYPE = 'FI' THEN '01'
             WHEN T2.RATE_TYPE = 'FL' THEN '02'
             ELSE '@'
           END ,  --��������  --FIXED 01  �̶����� FLOATING  02 ��������
           T2.EXEC_INTST_RATE,  --ִ������
           CASE
             WHEN T2.RATE_TYPE = 'FI' AND T2.RPRIC_FREQ = 'Y' THEN
              TO_CHAR(ADD_MONTHS(TO_DATE((SUBSTR(p_data_dt_str, 1, 4)) ||
                                         SUBSTR(T2.ISSUE_DATE, 5, 4),
                                         'YYYYMMDD'),
                                 12),
                      'YYYYMMDD') --����һ��
             WHEN T2.RATE_TYPE = 'FI' AND T2.RPRIC_FREQ = 'S' THEN
              TO_CHAR(ADD_MONTHS(TO_DATE((SUBSTR(p_data_dt_str, 1, 4)) ||
                                         SUBSTR(T2.ISSUE_DATE, 5, 4),
                                         'YYYYMMDD'),
                                 6),
                      'YYYYMMDD') --���Ӱ���
             WHEN T2.RATE_TYPE = 'FI' AND T2.RPRIC_FREQ = 'S' THEN
              TO_CHAR(ADD_MONTHS(TO_DATE((SUBSTR(p_data_dt_str, 1, 4)) ||
                                         SUBSTR(T2.ISSUE_DATE, 5, 4),
                                         'YYYYMMDD'),
                                 3),
                      'YYYYMMDD') --����һ������
             WHEN T2.RATE_TYPE = 'FI' AND T2.RPRIC_FREQ = 'M' THEN
              TO_CHAR(ADD_MONTHS(TO_DATE((SUBSTR(p_data_dt_str, 1, 4)) ||
                                         SUBSTR(T2.ISSUE_DATE, 5, 4),
                                         'YYYYMMDD'),
                                 1),
                      'YYYYMMDD') --����һ����
             WHEN T2.RATE_TYPE = 'FI' AND T2.RPRIC_FREQ = 'DSD' THEN
              T2.MATU_DT --����һ����
             ELSE
              NULL
           END ,  --�´��ض�����
           CASE
             WHEN (TO_DATE(CASE
                             WHEN T2.RATE_TYPE = 'FI' AND T2.RPRIC_FREQ = 'Y' THEN
                              TO_CHAR(ADD_MONTHS(TO_DATE((SUBSTR(p_data_dt_str, 1, 4)) ||
                                                         SUBSTR(T2.ISSUE_DATE, 5, 4),
                                                         'YYYYMMDD'),
                                                 12),
                                      'YYYYMMDD') --����һ��
                             WHEN T2.RATE_TYPE = 'FI' AND T2.RPRIC_FREQ = 'S' THEN
                              TO_CHAR(ADD_MONTHS(TO_DATE((SUBSTR(p_data_dt_str, 1, 4)) ||
                                                         SUBSTR(T2.ISSUE_DATE, 5, 4),
                                                         'YYYYMMDD'),
                                                 6),
                                      'YYYYMMDD') --���Ӱ���
                             WHEN T2.RATE_TYPE = 'FI' AND T2.RPRIC_FREQ = 'S' THEN
                              TO_CHAR(ADD_MONTHS(TO_DATE((SUBSTR(p_data_dt_str, 1, 4)) ||
                                                         SUBSTR(T2.ISSUE_DATE, 5, 4),
                                                         'YYYYMMDD'),
                                                 3),
                                      'YYYYMMDD') --����һ������
                             WHEN T2.RATE_TYPE = 'FI' AND T2.RPRIC_FREQ = 'M' THEN
                              TO_CHAR(ADD_MONTHS(TO_DATE((SUBSTR(p_data_dt_str, 1, 4)) ||
                                                         SUBSTR(T2.ISSUE_DATE, 5, 4),
                                                         'YYYYMMDD'),
                                                 1),
                                      'YYYYMMDD') --����һ����
                             WHEN T2.RATE_TYPE = 'FI' AND T2.RPRIC_FREQ = 'DSD' THEN
                              T2.MATU_DT --����һ����
                             ELSE
                              NULL
                           END
             , 'YYYY-MM-DD') -
                  TO_DATE(p_data_dt_str, 'YYYYMMDD')) / 365 < 0 THEN 0
         ELSE
              (TO_DATE(CASE
                           WHEN T2.RATE_TYPE = 'FI' AND T2.RPRIC_FREQ = 'Y' THEN
                            TO_CHAR(ADD_MONTHS(TO_DATE((SUBSTR(p_data_dt_str, 1, 4)) ||
                                                       SUBSTR(T2.ISSUE_DATE, 5, 4),
                                                       'YYYYMMDD'),
                                               12),
                                    'YYYYMMDD') --����һ��
                           WHEN T2.RATE_TYPE = 'FI' AND T2.RPRIC_FREQ = 'S' THEN
                            TO_CHAR(ADD_MONTHS(TO_DATE((SUBSTR(p_data_dt_str, 1, 4)) ||
                                                       SUBSTR(T2.ISSUE_DATE, 5, 4),
                                                       'YYYYMMDD'),
                                               6),
                                    'YYYYMMDD') --���Ӱ���
                           WHEN T2.RATE_TYPE = 'FI' AND T2.RPRIC_FREQ = 'S' THEN
                            TO_CHAR(ADD_MONTHS(TO_DATE((SUBSTR(p_data_dt_str, 1, 4)) ||
                                                       SUBSTR(T2.ISSUE_DATE, 5, 4),
                                                       'YYYYMMDD'),
                                               3),
                                    'YYYYMMDD') --����һ������
                           WHEN T2.RATE_TYPE = 'FI' AND T2.RPRIC_FREQ = 'M' THEN
                            TO_CHAR(ADD_MONTHS(TO_DATE((SUBSTR(p_data_dt_str, 1, 4)) ||
                                                       SUBSTR(T2.ISSUE_DATE, 5, 4),
                                                       'YYYYMMDD'),
                                               1),
                                    'YYYYMMDD') --����һ����
                           WHEN T2.RATE_TYPE = 'FI' AND T2.RPRIC_FREQ = 'DSD' THEN
                            T2.MATU_DT --����һ����
                           ELSE
                            NULL
                         END, 'YYYY-MM-DD') -
              TO_DATE(p_data_dt_str, 'YYYYMMDD')) / 365
           END,               --�´��ض�������
           NULL,              --��������
           T1.PAR_VALUE,  --���
           DECODE(T2.CCY_CD, '156', 'CNY', T2.CCY_CD), ---����ͳһ
           T1.SECURITY_REFERENCE 
       FROM BRD_SECURITY_POSI T1 --ծȯͷ����Ϣ
       INNER JOIN BRD_BOND T2 --ծȯ
         ON T1.SECURITY_REFERENCE = T2.BOND_ID
        AND T2.DATANO = p_data_dt_str
        AND T2.BELONG_GROUP = '4' --�ʽ�ϵͳ
       LEFT JOIN (
            SELECT T.BOND_ID,
              MAX(T.RATING_DATE) RATING_DATE,
              MIN(T.CREDIT_RATING) CREDIT_RATING
           FROM CREDIT_RATING_ZQ_TMEP T  --ծȯ�ⲿ������Ϣ��ʱ��
          GROUP BY T.BOND_ID
       ) T5  --�������1
         ON T5.BOND_ID = T1.SECURITY_REFERENCE
       LEFT JOIN (
             SELECT BOND_ID, COUNT(*) AS CREDIT_RATING_NUM
               FROM CREDIT_RATING_TMEP T1   --ծȯ��������������Ϣ��ʱ��
              WHERE CREDIT_RATING < '0111' --BB+ ���ϵ�
              GROUP BY BOND_ID
             HAVING COUNT(*) >= 2 --�z������
       ) T6
         ON T6.BOND_ID = T1.SECURITY_REFERENCE 
       LEFT JOIN RWA_DEV.NCM_CUSTOMER_INFO T7 
              ON DECODE(T2.ISSUER_NAME, '�����г��н���Ͷ�ʿ������޹�˾', '�����г��н���Ͷ�ʿ�����˾', T2.ISSUER_NAME) = T7.CUSTOMERNAME --���������⴦��
              AND T7.DATANO = p_data_dt_str
              AND T7.CUSTOMERID <> 'ty2018120600000001' --�л����񹲺͹������� ���⴦��                       
      WHERE T1.DATANO = p_data_dt_str
        AND T1.SBJT_CD = '11010101'  --�Թ��ʼ�ֵ��������䶯���뵱������Ľ����ʲ�         
        AND T2.BOND_TYPE NOT IN ('TTC')   --�ų��ǹ�ծ  TTC �����ʱ�����      
    ;
commit;
----����Ʒծȯ��Ϣ
 INSERT INTO RWA_DEV.RWA_ZJ_BONDINFO
    (
     DATADATE, --��������
     BONDID, --ծȯID
     BONDNAME, --ծȯ����
     BONDTYPE, --ծȯ����
     ERATING, --�ⲿ����
     ISSUERID, --������ID
     ISSUERNAME, --����������
     ISSUERTYPE, --�����˴���
     ISSUERSUBTYPE, --������С��
     ISSUERREGISTSTATE, --������ע�����
     ISSUERSMBFLAG, --������С΢��ҵ��ʶ
     BONDISSUEINTENT, --ծȯ����Ŀ��
     REABSFLAG, --���ʲ�֤ȯ����ʶ
     ORIGINATORFLAG, --�Ƿ������
     STARTDATE, --��ʼ����
     DUEDATE, --��������
     ORIGINALMATURITY, --ԭʼ����
     RESIDUALM, --ʣ������
     RATETYPE, --��������
     EXECUTIONRATE, --ִ������
     NEXTREPRICEDATE, --�´��ض�����
     NEXTREPRICEM, --�´��ض�������
     MODIFIEDDURATION, --��������
     DENOMINATION, --���
     CURRENCY, --����
     SECURITY_REFERENCE  --֤ȯΨһ��ʾ
    ) 
     SELECT TO_DATE(p_data_dt_str, 'YYYYMMDD'), --��������      
       T.DEALNO || T.SEQ, --ծȯID
     '���ʻ���', --ծȯ����
     '02', --ծȯ����
     0207, --�ⲿ����
      nvl(C.TAXID,'OPI'||H.CNO), --������ID
     C.CFN1, --����������
     '', --�����˴���
     '', --������С��
     CASE WHEN C.CCODE = 'CN' THEN '01' ELSE '02' END, --������ע�����
     '0', --������С΢��ҵ��ʶ
     '02', --ծȯ����Ŀ��
     '0', --���ʲ�֤ȯ����ʶ
     '0', --�Ƿ������
      T.STARTDATE, --��ʼ����
     CASE WHEN T.FIXFLOATIND = 'X' THEN T.Matdate
     WHEN T.FIXFLOATIND = 'L' AND( SUBSTR(T.RATECODE, 1, 2) )= '6M'
     THEN TO_CHAR(ADD_MONTHS(TO_DATE(T.STARTDATE, 'YYYYMMDD'), 6), 'YYYYMMDD') 
     END,  --��������
      CASE
             WHEN (TO_DATE( (CASE WHEN T.FIXFLOATIND = 'X' THEN T.Matdate
     WHEN T.FIXFLOATIND = 'L' AND( SUBSTR(T.RATECODE, 1, 2) )= '6M'
     THEN TO_CHAR(ADD_MONTHS(TO_DATE(T.STARTDATE, 'YYYYMMDD'), 6), 'YYYYMMDD') 
     END), 'YYYY-MM-DD') -
                  TO_DATE( T.STARTDATE, 'YYYY-MM-DD')) / 365 < 0 THEN
              0
             ELSE
              (TO_DATE( (CASE WHEN T.FIXFLOATIND = 'X' THEN T.Matdate
     WHEN T.FIXFLOATIND = 'L' AND( SUBSTR(T.RATECODE, 1, 2) )= '6M'
     THEN TO_CHAR(ADD_MONTHS(TO_DATE(T.STARTDATE, 'YYYYMMDD'), 6), 'YYYYMMDD') 
     END), 'YYYY-MM-DD') -
              TO_DATE( T.STARTDATE, 'YYYY-MM-DD')) / 365
           END, --ԭʼ����
          CASE
             WHEN (TO_DATE( (CASE WHEN T.FIXFLOATIND = 'X' THEN T.Matdate
     WHEN T.FIXFLOATIND = 'L' AND( SUBSTR(T.RATECODE, 1, 2) )= '6M'
     THEN TO_CHAR(ADD_MONTHS(TO_DATE(T.STARTDATE, 'YYYYMMDD'), 6), 'YYYYMMDD') 
     END), 'YYYY-MM-DD') -
                  TO_DATE(P_DATA_DT_STR, 'YYYYMMDD')) / 365 < 0 THEN
              0
             ELSE
              (TO_DATE( (CASE WHEN T.FIXFLOATIND = 'X' THEN T.Matdate
     WHEN T.FIXFLOATIND = 'L' AND( SUBSTR(T.RATECODE, 1, 2) )= '6M'
     THEN TO_CHAR(ADD_MONTHS(TO_DATE(T.STARTDATE, 'YYYYMMDD'), 6), 'YYYYMMDD') 
     END), 'YYYY-MM-DD') -
              TO_DATE(P_DATA_DT_STR, 'YYYYMMDD')) / 365
           END, --ʣ������
     CASE WHEN T.FIXFLOATIND = 'X' THEN '01' --�̶�����
     WHEN T.FIXFLOATIND = 'L' THEN '02' 
      END,--�������� END, --��������
      T.INTRATE_8, --ִ������
      CASE 
      WHEN T.FIXFLOATIND = 'L' AND SUBSTR(T.RATECODE, 1, 2) = '6M' THEN TO_CHAR(ADD_MONTHS(TO_DATE(T.STARTDATE, 'YYYYMMDD'), 6), 'YYYYMMDD')  --L ���������ض������� = ��Ϣ���� + �ع�Ƶ��  ��������������������ع�Ƶ���㷨
      ELSE NULL
      END, --�´��ض�����
             CASE
             WHEN (TO_DATE( (CASE 
     WHEN T.FIXFLOATIND = 'L' AND( SUBSTR(T.RATECODE, 1, 2) )= '6M'
     THEN TO_CHAR(ADD_MONTHS(TO_DATE(T.STARTDATE, 'YYYYMMDD'), 6), 'YYYYMMDD') 
     END), 'YYYY-MM-DD') -
                  TO_DATE(P_DATA_DT_STR, 'YYYYMMDD')) / 365 < 0 THEN
              0
             ELSE
              (TO_DATE( (CASE 
     WHEN T.FIXFLOATIND = 'L' AND( SUBSTR(T.RATECODE, 1, 2) )= '6M'
     THEN TO_CHAR(ADD_MONTHS(TO_DATE(T.STARTDATE, 'YYYYMMDD'), 6), 'YYYYMMDD') 
     END), 'YYYY-MM-DD') -
              TO_DATE(P_DATA_DT_STR, 'YYYYMMDD')) / 365
           END, --�´��ض�������
      '', --��������
      T.NOTCCYAMT, --���
      T.INTCCY, --����
      ''  --֤ȯΨһ��ʾ
      FROM  OPI_SWDT T --��������
         LEFT JOIN OPI_SWDH H --������ͷ 
                ON T.DEALNO = H.DEALNO
               AND H.DATANO = p_data_dt_str
         LEFT JOIN  OPI_CUST C --�ͻ���Ϣ
                ON H.CNO = C.CNO 
               AND C.DATANO = p_data_dt_str
        WHERE T.DATANO = p_data_dt_str;
    
    commit;

  dbms_stats.gather_table_stats(ownname => 'RWA_DEV',
                                tabname => 'RWA_ZJ_BONDINFO',
                                cascade => true);

  /*Ŀ�������ͳ��*/
  --ͳ�Ʋ���ļ�¼��
  SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_ZJ_BONDINFO;
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
    p_po_rtnmsg  := 'ծȯ��Ϣ��-�ʽ�ϵͳ(' || v_pro_name || ')ETLת��ʧ�ܣ�' || sqlerrm ||
                    ';��������Ϊ:' || dbms_utility.format_error_backtrace;
    RETURN;
END PRO_RWA_ZJ_BONDINFO;
/

