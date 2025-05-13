CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_BL_CUSTOMER_GATHER(
                             p_data_dt_str  IN  VARCHAR2,    --�������� yyyyMMdd
                             p_po_rtncode  OUT  VARCHAR2,    --���ر�� 1 �ɹ�,0 ʧ��
                            p_po_rtnmsg    OUT  VARCHAR2    --��������
        )
  /*
    �洢��������:RWA_DEV.PRO_RWA_BL_CUSTOMER_GATHER
    ʵ�ֹ���:RWAϵͳ-��¼-�ͻ���Ϣ����(��RWAϵͳ������¼���аѿͻ���Ϣ����ȥ�غ��뵽ͳһ�ͻ�����)
    ���ݿھ�:ȫ��
    ����Ƶ��:�³�����
    ��  ��  :V1.0.0
    ��д��  :LISY
    ��дʱ��:2016-06-12
    ��  λ  :�Ϻ���˶��Ϣ�����ɷ����޹�˾
    Դ  ��1 :RWA.RWA_WS_XD_ISSUER|�Ŵ�ϵͳ����ѺƷ��������Ϣ��¼��
    Դ  ��2 :RWA.RWA_WS_DSBANK_ADV|ֱ�����е�¼��
    Դ  ��3 :RWA.RWA_WS_BO_BILLREDISCOUNT|�ⲿƱ��ת����_��Ʊ��¼��
    Դ  ��4 :RWA.RWA_WS_CO_BILLREDISCOUNT|�ⲿƱ��ת����_��Ʊ��¼��
    Դ  ��5 :RWA.RWA_WS_BI_BILLREDISCOUNT|�ڲ�Ʊ��ת����_��Ʊ��¼��
    Դ  ��6 :RWA.RWA_WS_INNERBANK|ͬҵ��貹¼��
    Դ  ��7 :RWA.RWA_WS_B_BILLREPURCHASE|Ʊ�ݻع���Ͷ�ʲ�¼��
    Դ  ��8 :RWA.RWA_WS_B_BONDREPURCHASE|ծȯ�ع���Ͷ�ʲ�¼��
    Դ  ��9 :RWA.RWA_WS_S_BONDREPURCHASE|ծȯ�ع���Ͷ�ʲ�¼��
    Դ  ��10:RWA.RWA_WS_BONDTRADE|ծȯͶ�ʲ�¼��
    Դ  ��11:RWA.RWA_WS_RECEIVABLE|Ӧ�տ�Ͷ�ʲ�¼��
    Դ  ��12:RWA.RWA_WS_B_RECEIVABLE|���뷵��Ӧ�տ�Ͷ�ʲ�¼��
    Դ  ��13:RWA.RWA_WS_FCII_BOND|���Ͷ��ծȯͶ��ҵ����Ϣ��¼��
    Դ  ��14:RWA.RWA_WS_FCII_PLAN|���Ͷ���ʹܼƻ�ҵ����Ϣ��¼��
    Դ  ��15:RWA_DEV.RWA_EI_UNCONSFIINVEST|��ȨͶ��ҳ�油¼��
    Դ  ��16:RWA_DEV.CBS_LNM|�������
    Դ  ��17:RWA_DEV.CBS_IAC|ͨ�÷ֻ���
    Դ  ��18:RWA_DEV.CMS_CUSTOMER_INFO|����ͳһ�ͻ���
    Դ  ��19:RWA.RWA_WP_DATASUPPLEMENT|��¼������ϸ��Ϣ��
    Ŀ���  :RWA_DEV.BL_CUSTOMER_INFO|ͳһ�ͻ���Ϣ��¼��
    ��ʱ��  :RWA_DEV.RWA_TMP_CUSTOMER_INFO|ͳһ�ͻ���Ϣ��¼��ʱ��
    �����¼(�޸���|�޸�ʱ��|�޸�����):
    */
  AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_BL_CUSTOMER_GATHER';
  --�����쳣����
  v_raise EXCEPTION;
  --���嵱ǰ����ļ�¼��
  v_count INTEGER;

  BEGIN
    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*�����ȫ�����ݼ��������Ŀ���*/
    --1.�����ʱ���е�ԭ�м�¼
    --1.1 ��ղ�¼�ͻ���Ϣ��ʱ��
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_TMP_CUSTOMER_INFO';

    --1.2 ����ծȯ�ع����׶��֡�ծȯ�����˵���֯��������
    --ծȯ�ع���¼���ݷ���������Ĭ����֯�������롣������-00001318-6(�л����񹲺͹�������)���й�����������-10001644-8�����ҿ�������-00001845-4(Ĭ�Ϲ��ҿ������йɷ����޹�˾)���й�ũҵ��չ����-10001704-5
    UPDATE RWA.RWA_WS_B_BONDREPURCHASE SET ORGANIZATIONCODE = CASE WHEN CLIENTNAME LIKE '%������%' THEN '00001318-6' WHEN CLIENTNAME LIKE '%������%��%' THEN '10001644-8' WHEN CLIENTNAME LIKE '%��%��%��%' THEN '00001845-4' WHEN CLIENTNAME LIKE '%ũ��%��%' OR CLIENTNAME LIKE '%ũҵ��%��%' THEN '10001704-5' ELSE ORGANIZATIONCODE END WHERE DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD');

    UPDATE RWA.RWA_WS_B_BONDREPURCHASE SET ISSUERORGCODE = CASE WHEN ISSUERNAME LIKE '%������%' THEN '00001318-6' WHEN ISSUERNAME LIKE '%������%��%' THEN '10001644-8' WHEN ISSUERNAME LIKE '%��%��%��%' THEN '00001845-4' WHEN ISSUERNAME LIKE '%ũ��%��%' OR ISSUERNAME LIKE '%ũҵ��%��%' THEN '10001704-5' ELSE ISSUERORGCODE END WHERE DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD');

		UPDATE RWA.RWA_WS_S_BONDREPURCHASE SET ORGANIZATIONCODE = CASE WHEN CLIENTNAME LIKE '%������%' THEN '00001318-6' WHEN CLIENTNAME LIKE '%������%��%' THEN '10001644-8' WHEN CLIENTNAME LIKE '%��%��%��%' THEN '00001845-4' WHEN CLIENTNAME LIKE '%ũ��%��%' OR CLIENTNAME LIKE '%ũҵ��%��%' THEN '10001704-5' ELSE ORGANIZATIONCODE END WHERE DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD');

		COMMIT;

    --2. ���������������ݴ�Դ����뵽��ʱ����
    --2.1 ���º���-�ڲ���Ʊת����-�жҷ����ܺ�Ŀͻ����
    UPDATE RWA.RWA_WS_BI_BILLREDISCOUNT T
		   SET T.CUSTID1 =
		       (WITH TMP_CUST AS (SELECT LNUCAR_NO,LNUCARNO,SUPPSERIALNO, 'NBYPZTCD' || p_data_dt_str || lpad(rownum, 4, '0') AS CUSTID
															  FROM (SELECT LNUCAR_NO,LNUCARNO,SUPPSERIALNO
															          FROM RWA.RWA_WS_BI_BILLREDISCOUNT
															         WHERE DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD')
															         ORDER BY LNUCAR_NO,LNUCARNO,SUPPSERIALNO))
		         SELECT T1.CUSTID
		           FROM TMP_CUST T1
		          WHERE T1.LNUCAR_NO = T.LNUCAR_NO
		          	AND T1.LNUCARNO = T.LNUCARNO
		          	AND T1.SUPPSERIALNO = T.SUPPSERIALNO)
		          WHERE T.DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD')
		;

		COMMIT;

    --2.2 �������-�ڲ���Ʊת����-�ж�����Ϣ
    INSERT INTO RWA_DEV.RWA_TMP_CUSTOMER_INFO(
                DATANO                             --�����ڴ�
                ,CUSTOMERID                        --�ͻ����
                ,CUSTOMERNAME                      --�ͻ�����
                ,CUSTOMERTYPE                      --�ͻ�����
                ,CERTTYPE                          --֤������
                ,CERTID                            --֤������
                ,COUNTRYCODE                       --���ڹ��Ҵ���
                ,INDUSTRYTYPE                      --��ҵ����
                ,ORGNATURE                         --�Թ��ͻ�����
                ,FINANCETYPE                       --���ڻ�������
                ,SCOPE                             --���Ų���ҵ��ģ
                ,ORGID                             --�ͻ���������
                ,CUSTOMERCATEGORY                  --�ͻ����
                ,ERATINGORG                        --�ⲿ��������
                ,ERATINGTYPE                       --�ⲿ��������
                ,ERATING                           --�ⲿ�������
                ,IRATING                           --�ڲ��������
                ,UPDATEDATE                        --��������
                ,UPDATETIME                        --����ʱ��
    )
    SELECT
                p_data_dt_str                                  AS DATANO                    --�����ڴ�
                ,T1.CUSTID1                                    AS CUSTOMERID                --�ͻ����
                ,T1.Acceptor                                   AS CUSTOMERNAME              --�ͻ�����
                ,'0321000002'                                  AS CUSTOMERTYPE              --�ͻ�����
                ,'Ent02'		                                   AS CERTTYPE                  --֤������
                ,T1.Acceptorgcode                              AS CERTID                    --֤������
                ,T1.Acceptcountrycode                          AS COUNTRYCODE               --���ڹ��Ҵ���
                ,T1.Acceptindustryid                           AS INDUSTRYTYPE              --��ҵ����
                ,''                                            AS ORGNATURE                 --�Թ��ͻ�����
                ,''                                            AS FINANCETYPE               --���ڻ�������
                ,''					                                   AS SCOPE                     --���Ų���ҵ��ģ
                ,T1.BELONGORGCODE                              AS ORGID                     --�ͻ���������
                ,T1.CLIENTCATEGORY                             AS CUSTOMERCATEGORY          --�ͻ����
                ,''                                            AS ERATINGORG                --�ⲿ��������
                ,''                                            AS ERATINGTYPE               --�ⲿ��������
                ,''                                            AS ERATING                   --�ⲿ�������
                ,''                                            AS IRATING                   --�ڲ��������
                ,''                                            AS UPDATEDATE                --��������
                ,''                                            AS UPDATETIME                --����ʱ��

    FROM        RWA.RWA_WS_BI_BILLREDISCOUNT T1
    WHERE 			T1.DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD')
    AND					T1.CUSTID1 IS NOT NULL
    ;

    COMMIT;

		--2.3 ���º���-�����е��׶��ֻ��ܺ�Ŀͻ����
    UPDATE RWA.RWA_WS_DSBANK_ADV T
		   SET T.CUSTID1 =
		       (WITH TMP_CUST AS (SELECT IACAC_NO,SUPPSERIALNO, 'ZXYHDKJY' || p_data_dt_str || lpad(rownum, 4, '0') AS CUSTID
															  FROM (SELECT IACAC_NO,SUPPSERIALNO
															          FROM RWA.RWA_WS_DSBANK_ADV
															         WHERE DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD')
															         ORDER BY IACAC_NO,SUPPSERIALNO))
		         SELECT T1.CUSTID
		           FROM TMP_CUST T1
		          WHERE T1.IACAC_NO = T.IACAC_NO
		          	AND T1.SUPPSERIALNO = T.SUPPSERIALNO)
		          WHERE T.DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD')
		;

		COMMIT;

    --2.4 �������-ֱ�����е��׶�����Ϣ
    INSERT INTO RWA_DEV.RWA_TMP_CUSTOMER_INFO(
                DATANO                             --�����ڴ�
                ,CUSTOMERID                        --�ͻ����
                ,CUSTOMERNAME                      --�ͻ�����
                ,CUSTOMERTYPE                      --�ͻ�����
                ,CERTTYPE                          --֤������
                ,CERTID                            --֤������
                ,COUNTRYCODE                       --���ڹ��Ҵ���
                ,INDUSTRYTYPE                      --��ҵ����
                ,ORGNATURE                         --�Թ��ͻ�����
                ,FINANCETYPE                       --���ڻ�������
                ,SCOPE                             --���Ų���ҵ��ģ
                ,ORGID                             --�ͻ���������
                ,CUSTOMERCATEGORY                  --�ͻ����
                ,ERATINGORG                        --�ⲿ��������
                ,ERATINGTYPE                       --�ⲿ��������
                ,ERATING                           --�ⲿ�������
                ,IRATING                           --�ڲ��������
                ,UPDATEDATE                        --��������
                ,UPDATETIME                        --����ʱ��
    )
    SELECT
                p_data_dt_str                                  AS DATANO                    --�����ڴ�
                ,T1.CUSTID1                                    AS CUSTOMERID                --�ͻ����
                ,T1.CLIENTNAME                                 AS CUSTOMERNAME              --�ͻ�����
                ,'0321000002'		                               AS CUSTOMERTYPE              --�ͻ�����
                ,'Ent02'		                                   AS CERTTYPE                  --֤������
                ,T1.ORGANIZATIONCODE                           AS CERTID                    --֤������
                ,T1.COUNTRYCODE                                AS COUNTRYCODE               --���ڹ��Ҵ���
                ,T1.INDUSTRYID	                               AS INDUSTRYTYPE              --��ҵ����
                ,''                                            AS ORGNATURE                 --�Թ��ͻ�����
                ,''                                            AS FINANCETYPE               --���ڻ�������
                ,''					                                   AS SCOPE                     --���Ų���ҵ��ģ
                ,T1.BELONGORGCODE                              AS ORGID                     --�ͻ���������
                ,T1.CLIENTCATEGORY                             AS CUSTOMERCATEGORY          --�ͻ����
                ,''                                            AS ERATINGORG                --�ⲿ��������
                ,''                                            AS ERATINGTYPE               --�ⲿ��������
                ,''                                            AS ERATING                   --�ⲿ�������
                ,''                                            AS IRATING                   --�ڲ��������
                ,''                                            AS UPDATEDATE                --��������
                ,''                                            AS UPDATETIME                --����ʱ��

    FROM        RWA.RWA_WS_DSBANK_ADV T1
    WHERE 			T1.DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD')
    AND					T1.CUSTID1 IS NOT NULL
    ;

    COMMIT;

    --2.5 ���º���-ͬҵ�����-���׶��ֻ��ܺ�Ŀͻ����
    UPDATE RWA.RWA_WS_INNERBANK T
		   SET T.CUSTID1 =
		       (WITH TMP_CUST AS (SELECT ACCSERIALNO,SUPPSERIALNO, 'TYCJCFJY' || p_data_dt_str || lpad(rownum, 4, '0') AS CUSTID
															  FROM (SELECT ACCSERIALNO,SUPPSERIALNO
															          FROM RWA.RWA_WS_INNERBANK
															         WHERE DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD')
															         ORDER BY ACCSERIALNO,SUPPSERIALNO))
		         SELECT T1.CUSTID
		           FROM TMP_CUST T1
		          WHERE T1.ACCSERIALNO = T.ACCSERIALNO
		          	AND T1.SUPPSERIALNO = T.SUPPSERIALNO)
		          WHERE T.DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD')
		;

		COMMIT;

    --2.6 �������-ͬҵ�����-���׶�����Ϣ
    INSERT INTO RWA_DEV.RWA_TMP_CUSTOMER_INFO(
                DATANO                             --�����ڴ�
                ,CUSTOMERID                        --�ͻ����
                ,CUSTOMERNAME                      --�ͻ�����
                ,CUSTOMERTYPE                      --�ͻ�����
                ,CERTTYPE                          --֤������
                ,CERTID                            --֤������
                ,COUNTRYCODE                       --���ڹ��Ҵ���
                ,INDUSTRYTYPE                      --��ҵ����
                ,ORGNATURE                         --�Թ��ͻ�����
                ,FINANCETYPE                       --���ڻ�������
                ,SCOPE                             --���Ų���ҵ��ģ
                ,ORGID                             --�ͻ���������
                ,CUSTOMERCATEGORY                  --�ͻ����
                ,ERATINGORG                        --�ⲿ��������
                ,ERATINGTYPE                       --�ⲿ��������
                ,ERATING                           --�ⲿ�������
                ,IRATING                           --�ڲ��������
                ,UPDATEDATE                        --��������
                ,UPDATETIME                        --����ʱ��
    )
    SELECT
                p_data_dt_str                                  AS DATANO                    --�����ڴ�
                ,T1.CUSTID1                                    AS CUSTOMERID                --�ͻ����
                ,T1.CLIENTNAME                                 AS CUSTOMERNAME              --�ͻ�����
                ,'0321000002'                                  AS CUSTOMERTYPE              --�ͻ�����
                ,'Ent02'                                   		 AS CERTTYPE                  --֤������
                ,T1.ORGANIZATIONCODE                           AS CERTID                    --֤������
                ,T1.COUNTRYCODE                                AS COUNTRYCODE               --���ڹ��Ҵ���
                ,T1.INDUSTRYID	                               AS INDUSTRYTYPE              --��ҵ����
                ,''                                            AS ORGNATURE                 --�Թ��ͻ�����
                ,''                                            AS FINANCETYPE               --���ڻ�������
                ,''                                            AS SCOPE                     --���Ų���ҵ��ģ
                ,T1.BELONGORGCODE                              AS ORGID                     --�ͻ���������
                ,T1.CLIENTCATEGORY                             AS CUSTOMERCATEGORY          --�ͻ����
                ,''                                            AS ERATINGORG                --�ⲿ��������
                ,''                                            AS ERATINGTYPE               --�ⲿ��������
                ,''                                            AS ERATING                   --�ⲿ�������
                ,''                                            AS IRATING                   --�ڲ��������
                ,''                                            AS UPDATEDATE                --��������
                ,''                                            AS UPDATETIME                --����ʱ��

    FROM        RWA.RWA_WS_INNERBANK T1
    WHERE 			T1.DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD')
    AND					T1.CUSTID1 IS NOT NULL
    ;

    COMMIT;

    --2.7 ���º���-���뷵��Ʊ�ݻع�-���׶��ֻ��ܺ�Ŀͻ����
    UPDATE RWA.RWA_WS_B_BILLREPURCHASE T
		   SET T.CUSTID1 =
		       (WITH TMP_CUST AS (SELECT INVACCNO, 'MRFSPJJY' || p_data_dt_str || lpad(rownum, 4, '0') AS CUSTID
															  FROM (SELECT INVACCNO
															          FROM RWA.RWA_WS_B_BILLREPURCHASE
															         WHERE DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD')
															       	 GROUP BY INVACCNO
															         ORDER BY INVACCNO))
		         SELECT T1.CUSTID
		           FROM TMP_CUST T1
		          WHERE T1.INVACCNO = T.INVACCNO)
		          WHERE T.DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD')
		;

		COMMIT;

    --2.8 �������-���뷵��Ʊ�ݻع�-���׶�����Ϣ
    INSERT INTO RWA_DEV.RWA_TMP_CUSTOMER_INFO(
                DATANO                             --�����ڴ�
                ,CUSTOMERID                        --�ͻ����
                ,CUSTOMERNAME                      --�ͻ�����
                ,CUSTOMERTYPE                      --�ͻ�����
                ,CERTTYPE                          --֤������
                ,CERTID                            --֤������
                ,COUNTRYCODE                       --���ڹ��Ҵ���
                ,INDUSTRYTYPE                      --��ҵ����
                ,ORGNATURE                         --�Թ��ͻ�����
                ,FINANCETYPE                       --���ڻ�������
                ,SCOPE                             --���Ų���ҵ��ģ
                ,ORGID                             --�ͻ���������
                ,CUSTOMERCATEGORY                  --�ͻ����
                ,ERATINGORG                        --�ⲿ��������
                ,ERATINGTYPE                       --�ⲿ��������
                ,ERATING                           --�ⲿ�������
                ,IRATING                           --�ڲ��������
                ,UPDATEDATE                        --��������
                ,UPDATETIME                        --����ʱ��
    )
    SELECT
                p_data_dt_str                                  AS DATANO                    --�����ڴ�
                ,T1.CUSTID1                                    AS CUSTOMERID                --�ͻ����
                ,T1.CLIENTNAME                                 AS CUSTOMERNAME              --�ͻ�����
                ,'0321000002'                                  AS CUSTOMERTYPE              --�ͻ�����
                ,'Ent02'		                                   AS CERTTYPE                  --֤������
                ,T1.ORGANIZATIONCODE                           AS CERTID                    --֤������
                ,T1.COUNTRYCODE                                AS COUNTRYCODE               --���ڹ��Ҵ���
                ,T1.INDUSTRYID	                               AS INDUSTRYTYPE              --��ҵ����
                ,''                                            AS ORGNATURE                 --�Թ��ͻ�����
                ,''                                            AS FINANCETYPE               --���ڻ�������
                ,''                                            AS SCOPE                     --���Ų���ҵ��ģ
                ,T1.BELONGORGCODE                              AS ORGID                     --�ͻ���������
                ,T1.CLIENTCATEGORY                             AS CUSTOMERCATEGORY          --�ͻ����
                ,''                                            AS ERATINGORG                --�ⲿ��������
                ,''                                            AS ERATINGTYPE               --�ⲿ��������
                ,''                                            AS ERATING                   --�ⲿ�������
                ,''                                            AS IRATING                   --�ڲ��������
                ,''                                            AS UPDATEDATE                --��������
                ,''                                            AS UPDATETIME                --����ʱ��

    FROM        RWA.RWA_WS_B_BILLREPURCHASE T1
    WHERE 			T1.ROWID IN  (SELECT MAX(T3.ROWID)
                               FROM RWA.RWA_WS_B_BILLREPURCHASE T3
						                  WHERE T3.DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD')
						                  	AND T3.CUSTID1 IS NOT NULL
                           GROUP BY T3.INVACCNO)
    AND 				T1.DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD')
    AND					T1.CUSTID1 IS NOT NULL
    ;

    COMMIT;

    --2.9 ���º���-���뷵��Ʊ�ݻع�-�жҷ����ܺ�Ŀͻ����
    UPDATE RWA.RWA_WS_B_BILLREPURCHASE T
		   SET T.CUSTID2 =
		       (WITH TMP_CUST AS (SELECT INVACCNO,BILLNO,SUPPSERIALNO, 'MRFSPJCD' || p_data_dt_str || lpad(rownum, 4, '0') AS CUSTID
															  FROM (SELECT INVACCNO,BILLNO,SUPPSERIALNO
															          FROM RWA.RWA_WS_B_BILLREPURCHASE
															         WHERE DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD')
															         ORDER BY INVACCNO,BILLNO,SUPPSERIALNO))
		         SELECT T1.CUSTID
		           FROM TMP_CUST T1
		          WHERE T1.INVACCNO = T.INVACCNO
		          	AND T1.BILLNO = T.BILLNO
		          	AND T1.SUPPSERIALNO = T.SUPPSERIALNO)
		          WHERE T.DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD')
		;

		COMMIT;

    --2.10 �������-���뷵��Ʊ�ݻع�-�жҷ���Ϣ
    INSERT INTO RWA_DEV.RWA_TMP_CUSTOMER_INFO(
                DATANO                             --�����ڴ�
                ,CUSTOMERID                        --�ͻ����
                ,CUSTOMERNAME                      --�ͻ�����
                ,CUSTOMERTYPE                      --�ͻ�����
                ,CERTTYPE                          --֤������
                ,CERTID                            --֤������
                ,COUNTRYCODE                       --���ڹ��Ҵ���
                ,INDUSTRYTYPE                      --��ҵ����
                ,ORGNATURE                         --�Թ��ͻ�����
                ,FINANCETYPE                       --���ڻ�������
                ,SCOPE                             --���Ų���ҵ��ģ
                ,ORGID                             --�ͻ���������
                ,CUSTOMERCATEGORY                  --�ͻ����
                ,ERATINGORG                        --�ⲿ��������
                ,ERATINGTYPE                       --�ⲿ��������
                ,ERATING                           --�ⲿ�������
                ,IRATING                           --�ڲ��������
                ,UPDATEDATE                        --��������
                ,UPDATETIME                        --����ʱ��
    )
    SELECT
                p_data_dt_str                                  AS DATANO                    --�����ڴ�
                ,T1.CUSTID2                                    AS CUSTOMERID                --�ͻ����
                ,T1.ISSUERNAME                                 AS CUSTOMERNAME              --�ͻ�����
                ,'0321000002'                                  AS CUSTOMERTYPE              --�ͻ�����
                ,'Ent02'		                                   AS CERTTYPE                  --֤������
                ,T1.ISSUERORGCODE                              AS CERTID                    --֤������
                ,T1.ISSUERCOUNTRYCODE                          AS COUNTRYCODE               --���ڹ��Ҵ���
                ,T1.ISSUERINDUSTRYID                           AS INDUSTRYTYPE              --��ҵ����
                ,''                                            AS ORGNATURE                 --�Թ��ͻ�����
                ,''                                            AS FINANCETYPE               --���ڻ�������
                ,''                                            AS SCOPE                     --���Ų���ҵ��ģ
                ,T1.BELONGORGCODE                              AS ORGID                     --�ͻ���������
                ,T1.ACCEPTCATEGORY                             AS CUSTOMERCATEGORY          --�ͻ����
                ,''                                            AS ERATINGORG                --�ⲿ��������
                ,''                                            AS ERATINGTYPE               --�ⲿ��������
                ,''                                            AS ERATING                   --�ⲿ�������
                ,''                                            AS IRATING                   --�ڲ��������
                ,''                                            AS UPDATEDATE                --��������
                ,''                                            AS UPDATETIME                --����ʱ��

    FROM        RWA.RWA_WS_B_BILLREPURCHASE T1
    WHERE 			T1.DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD')
    AND					T1.CUSTID2 IS NOT NULL
    ;

    COMMIT;

    --2.11 ���º���-���뷵��ծȯ�ع�-���׶��ֻ��ܺ�Ŀͻ����
    UPDATE RWA.RWA_WS_B_BONDREPURCHASE T
		   SET T.CUSTID1 =
		       (WITH TMP_CUST AS (SELECT INVACCNO, 'MRFSZQJY' || p_data_dt_str || lpad(rownum, 4, '0') AS CUSTID
															  FROM (SELECT INVACCNO
															          FROM RWA.RWA_WS_B_BONDREPURCHASE
															         WHERE DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD')
															       	 GROUP BY INVACCNO
															         ORDER BY INVACCNO))
		         SELECT T1.CUSTID
		           FROM TMP_CUST T1
		          WHERE T1.INVACCNO = T.INVACCNO)
		          WHERE T.DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD')
		;

		COMMIT;

    --2.12 �������-���뷵��ծȯ�ع�-���׶�����Ϣ
    INSERT INTO RWA_DEV.RWA_TMP_CUSTOMER_INFO(
                DATANO                             --�����ڴ�
                ,CUSTOMERID                        --�ͻ����
                ,CUSTOMERNAME                      --�ͻ�����
                ,CUSTOMERTYPE                      --�ͻ�����
                ,CERTTYPE                          --֤������
                ,CERTID                            --֤������
                ,COUNTRYCODE                       --���ڹ��Ҵ���
                ,INDUSTRYTYPE                      --��ҵ����
                ,ORGNATURE                         --�Թ��ͻ�����
                ,FINANCETYPE                       --���ڻ�������
                ,SCOPE                             --���Ų���ҵ��ģ
                ,ORGID                             --�ͻ���������
                ,CUSTOMERCATEGORY                  --�ͻ����
                ,ERATINGORG                        --�ⲿ��������
                ,ERATINGTYPE                       --�ⲿ��������
                ,ERATING                           --�ⲿ�������
                ,IRATING                           --�ڲ��������
                ,UPDATEDATE                        --��������
                ,UPDATETIME                        --����ʱ��
    )
    SELECT
                p_data_dt_str                                  AS DATANO                    --�����ڴ�
                ,T1.CUSTID1                                    AS CUSTOMERID                --�ͻ����
                ,T1.CLIENTNAME                                 AS CUSTOMERNAME              --�ͻ�����
                ,'0321000002'                                  AS CUSTOMERTYPE              --�ͻ�����
                ,'Ent02'		                                   AS CERTTYPE                  --֤������
                ,T1.ORGANIZATIONCODE                           AS CERTID                    --֤������
                ,T1.COUNTRYCODE                                AS COUNTRYCODE               --���ڹ��Ҵ���
                ,T1.INDUSTRYID	                               AS INDUSTRYTYPE              --��ҵ����
                ,''                                            AS ORGNATURE                 --�Թ��ͻ�����
                ,''                                            AS FINANCETYPE               --���ڻ�������
                ,''                                            AS SCOPE                     --���Ų���ҵ��ģ
                ,T1.BELONGORGCODE                              AS ORGID                     --�ͻ���������
                ,T1.CLIENTCATEGORY                             AS CUSTOMERCATEGORY          --�ͻ����
                ,''                                            AS ERATINGORG                --�ⲿ��������
                ,''                                            AS ERATINGTYPE               --�ⲿ��������
                ,''                                            AS ERATING                   --�ⲿ�������
                ,''                                            AS IRATING                   --�ڲ��������
                ,''                                            AS UPDATEDATE                --��������
                ,''                                            AS UPDATETIME                --����ʱ��

    FROM        RWA.RWA_WS_B_BONDREPURCHASE T1
    WHERE 			T1.ROWID IN  (SELECT MAX(T3.ROWID)
                               FROM RWA.RWA_WS_B_BONDREPURCHASE T3
						                  WHERE T3.DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD')
						                  	AND T3.CUSTID1 IS NOT NULL
                           GROUP BY T3.INVACCNO)
    AND 				T1.DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD')
    AND					T1.CUSTID1 IS NOT NULL
    ;

    COMMIT;

    --2.13 ���º���-���뷵��ծȯ�ع�-�����˻��ܺ�Ŀͻ����
    UPDATE RWA.RWA_WS_B_BONDREPURCHASE T
		   SET T.CUSTID2 =
		       (WITH TMP_CUST AS (SELECT INVACCNO,BONDCODE,SUPPSERIALNO, 'MRFSZQFX' || p_data_dt_str || lpad(rownum, 4, '0') AS CUSTID
															  FROM (SELECT INVACCNO,BONDCODE,SUPPSERIALNO
															          FROM RWA.RWA_WS_B_BONDREPURCHASE
															         WHERE DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD')
															         ORDER BY INVACCNO,BONDCODE,SUPPSERIALNO))
		         SELECT T1.CUSTID
		           FROM TMP_CUST T1
		          WHERE T1.INVACCNO = T.INVACCNO
		          	AND T1.BONDCODE = T.BONDCODE
		          	AND T1.SUPPSERIALNO = T.SUPPSERIALNO)
		          WHERE T.DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD')
		;

		COMMIT;

    --2.14 �������-���뷵��ծȯ�ع�-��������Ϣ
    INSERT INTO RWA_DEV.RWA_TMP_CUSTOMER_INFO(
                DATANO                             --�����ڴ�
                ,CUSTOMERID                        --�ͻ����
                ,CUSTOMERNAME                      --�ͻ�����
                ,CUSTOMERTYPE                      --�ͻ�����
                ,CERTTYPE                          --֤������
                ,CERTID                            --֤������
                ,COUNTRYCODE                       --���ڹ��Ҵ���
                ,INDUSTRYTYPE                      --��ҵ����
                ,ORGNATURE                         --�Թ��ͻ�����
                ,FINANCETYPE                       --���ڻ�������
                ,SCOPE                             --���Ų���ҵ��ģ
                ,ORGID                             --�ͻ���������
                ,CUSTOMERCATEGORY                  --�ͻ����
                ,ERATINGORG                        --�ⲿ��������
                ,ERATINGTYPE                       --�ⲿ��������
                ,ERATING                           --�ⲿ�������
                ,IRATING                           --�ڲ��������
                ,UPDATEDATE                        --��������
                ,UPDATETIME                        --����ʱ��
    )
    SELECT
                p_data_dt_str                                  AS DATANO                    --�����ڴ�
                ,T1.CUSTID2                                    AS CUSTOMERID                --�ͻ����
                ,T1.ISSUERNAME                                 AS CUSTOMERNAME              --�ͻ�����
                ,'0321000002'                                  AS CUSTOMERTYPE              --�ͻ�����
                ,'Ent02'		                                   AS CERTTYPE                  --֤������
                ,T1.ISSUERORGCODE                              AS CERTID                    --֤������
                ,T1.ISSUERCOUNTRYCODE                          AS COUNTRYCODE               --���ڹ��Ҵ���
                ,T1.ISSUERINDUSTRYID                           AS INDUSTRYTYPE              --��ҵ����
                ,''                                            AS ORGNATURE                 --�Թ��ͻ�����
                ,''                                            AS FINANCETYPE               --���ڻ�������
                ,T1.ISSUERSCOPE                                AS SCOPE                     --���Ų���ҵ��ģ
                ,T1.BELONGORGCODE                              AS ORGID                     --�ͻ���������
                ,T1.ISSUERCATEGORY                             AS CUSTOMERCATEGORY          --�ͻ����
                ,''                                            AS ERATINGORG                --�ⲿ��������
                ,''                                            AS ERATINGTYPE               --�ⲿ��������
                ,''                                            AS ERATING                   --�ⲿ�������
                ,''                                            AS IRATING                   --�ڲ��������
                ,''                                            AS UPDATEDATE                --��������
                ,''                                            AS UPDATETIME                --����ʱ��

    FROM        RWA.RWA_WS_B_BONDREPURCHASE T1
    WHERE 			T1.DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD')
    ;

    COMMIT;

    --2.15 ���º���-�����ع�ծȯ�ع�-���׶��ֻ��ܺ�Ŀͻ����
    UPDATE RWA.RWA_WS_S_BONDREPURCHASE T
		   SET T.CUSTID1 =
		       (WITH TMP_CUST AS (SELECT INVACCNO, 'MCHGZQJY' || p_data_dt_str || lpad(rownum, 4, '0') AS CUSTID
															  FROM (SELECT INVACCNO
															          FROM RWA.RWA_WS_S_BONDREPURCHASE
															         WHERE DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD')
															         	 AND REPURCHASETYPE = '02'
															       	 GROUP BY INVACCNO
															         ORDER BY INVACCNO))
		         SELECT T1.CUSTID
		           FROM TMP_CUST T1
		          WHERE T1.INVACCNO = T.INVACCNO)
		          WHERE T.DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD')
		;

		COMMIT;

    --2.16 �������-�����ع�ծȯ�ع�-���׶�����Ϣ
    INSERT INTO RWA_DEV.RWA_TMP_CUSTOMER_INFO(
                DATANO                             --�����ڴ�
                ,CUSTOMERID                        --�ͻ����
                ,CUSTOMERNAME                      --�ͻ�����
                ,CUSTOMERTYPE                      --�ͻ�����
                ,CERTTYPE                          --֤������
                ,CERTID                            --֤������
                ,COUNTRYCODE                       --���ڹ��Ҵ���
                ,INDUSTRYTYPE                      --��ҵ����
                ,ORGNATURE                         --�Թ��ͻ�����
                ,FINANCETYPE                       --���ڻ�������
                ,SCOPE                             --���Ų���ҵ��ģ
                ,ORGID                             --�ͻ���������
                ,CUSTOMERCATEGORY                  --�ͻ����
                ,ERATINGORG                        --�ⲿ��������
                ,ERATINGTYPE                       --�ⲿ��������
                ,ERATING                           --�ⲿ�������
                ,IRATING                           --�ڲ��������
                ,UPDATEDATE                        --��������
                ,UPDATETIME                        --����ʱ��
    )
    SELECT
                p_data_dt_str                                  AS DATANO                    --�����ڴ�
                ,T1.CUSTID1                                    AS CUSTOMERID                --�ͻ����
                ,T1.CLIENTNAME                                 AS CUSTOMERNAME              --�ͻ�����
                ,'0321000002'                                  AS CUSTOMERTYPE              --�ͻ�����
                ,'Ent02'		                                   AS CERTTYPE                  --֤������
                ,T1.ORGANIZATIONCODE                           AS CERTID                    --֤������
                ,T1.COUNTRYCODE                                AS COUNTRYCODE               --���ڹ��Ҵ���
                ,T1.INDUSTRYID	                               AS INDUSTRYTYPE              --��ҵ����
                ,''                                            AS ORGNATURE                 --�Թ��ͻ�����
                ,''                                            AS FINANCETYPE               --���ڻ�������
                ,''                                            AS SCOPE                     --���Ų���ҵ��ģ
                ,T1.BELONGORGCODE                              AS ORGID                     --�ͻ���������
                ,T1.CLIENTCATEGORY                             AS CUSTOMERCATEGORY          --�ͻ����
                ,''                                            AS ERATINGORG                --�ⲿ��������
                ,''                                            AS ERATINGTYPE               --�ⲿ��������
                ,''                                            AS ERATING                   --�ⲿ�������
                ,''                                            AS IRATING                   --�ڲ��������
                ,''                                            AS UPDATEDATE                --��������
                ,''                                            AS UPDATETIME                --����ʱ��

    FROM        RWA.RWA_WS_S_BONDREPURCHASE T1
    WHERE 			T1.ROWID IN  (SELECT MAX(T3.ROWID)
                               FROM RWA.RWA_WS_S_BONDREPURCHASE T3
						                  WHERE T3.DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD')
						                  	AND	T3.REPURCHASETYPE = '02'
						                  	AND T3.CUSTID1 IS NOT NULL
                           GROUP BY T3.INVACCNO)
    AND 				T1.DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD')
    AND					T1.REPURCHASETYPE = '02'
    AND					T1.CUSTID1 IS NOT NULL
    ;

    COMMIT;

		--2.17 ����̨��-��ȨͶ��-���׶��ֻ��ܺ�Ŀͻ����
    UPDATE RWA_DEV.RWA_EI_UNCONSFIINVEST T
		   SET T.CUSTID1 =
		       (WITH TMP_CUST AS (SELECT SERIALNO, 'TZGQTZJY' || p_data_dt_str || lpad(rownum, 4, '0') AS CUSTID
															  FROM (SELECT SERIALNO
															          FROM RWA_DEV.RWA_EI_UNCONSFIINVEST
															         WHERE DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD')
															         	 AND EQUITYINVESTTYPE LIKE '03%'
															         ORDER BY SERIALNO))
		         SELECT T1.CUSTID
		           FROM TMP_CUST T1
		          WHERE T1.SERIALNO = T.SERIALNO)
		          WHERE T.DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD')
		;

		COMMIT;

    --2.18 ����̨��-��ȨͶ��-���׶�����Ϣ
    INSERT INTO RWA_DEV.RWA_TMP_CUSTOMER_INFO(
                DATANO                             --�����ڴ�
                ,CUSTOMERID                        --�ͻ����
                ,CUSTOMERNAME                      --�ͻ�����
                ,CUSTOMERTYPE                      --�ͻ�����
                ,CERTTYPE                          --֤������
                ,CERTID                            --֤������
                ,COUNTRYCODE                       --���ڹ��Ҵ���
                ,INDUSTRYTYPE                      --��ҵ����
                ,ORGNATURE                         --�Թ��ͻ�����
                ,FINANCETYPE                       --���ڻ�������
                ,SCOPE                             --���Ų���ҵ��ģ
                ,ORGID                             --�ͻ���������
                ,CUSTOMERCATEGORY                  --�ͻ����
                ,ERATINGORG                        --�ⲿ��������
                ,ERATINGTYPE                       --�ⲿ��������
                ,ERATING                           --�ⲿ�������
                ,IRATING                           --�ڲ��������
                ,UPDATEDATE                        --��������
                ,UPDATETIME                        --����ʱ��
    )
    SELECT
                p_data_dt_str                                  AS DATANO                    --�����ڴ�
                ,T1.CUSTID1                                    AS CUSTOMERID                --�ͻ����
                ,T1.investeename                               AS CUSTOMERNAME              --�ͻ�����
                ,'0321000002'                                  AS CUSTOMERTYPE              --�ͻ�����
                ,'Ent02'		                                   AS CERTTYPE                  --֤������
                ,T1.organizationcode                           AS CERTID                    --֤������
                ,'CHN'				                                 AS COUNTRYCODE               --���ڹ��Ҵ���
                ,''                                            AS INDUSTRYTYPE              --��ҵ����
                ,''                                            AS ORGNATURE                 --�Թ��ͻ�����
                ,''                                            AS FINANCETYPE               --���ڻ�������
                ,''                                            AS SCOPE                     --���Ų���ҵ��ģ
                ,T1.orgId                                      AS ORGID                     --�ͻ���������
                ,T1.equityinvesttype                           AS CUSTOMERCATEGORY          --�ͻ����
                ,''                                            AS ERATINGORG                --�ⲿ��������
                ,''                                            AS ERATINGTYPE               --�ⲿ��������
                ,''                                            AS ERATING                   --�ⲿ�������
                ,''                                            AS IRATING                   --�ڲ��������
                ,''                                            AS UPDATEDATE                --��������
                ,''                                            AS UPDATETIME                --����ʱ��

    FROM        RWA_DEV.RWA_EI_UNCONSFIINVEST T1
    WHERE 			T1.DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD')
    AND 				T1.EQUITYINVESTTYPE LIKE '03%'
    AND					T1.CUSTID1 IS NOT NULL
    ;

    COMMIT;

    --2.19 ծȯͶ��-���һ���-������ID
		UPDATE RWA.RWA_WS_BONDTRADE_MF T
		   SET T.CUSTID1 =
		       (WITH TMP_BOND AS (SELECT BOND_ID,SUPPSERIALNO, 'ZQTZHBJJ' || p_data_dt_str || lpad(rownum, 4, '0') AS CUSTID1
															  FROM (SELECT BOND_ID,SUPPSERIALNO
															          FROM RWA.RWA_WS_BONDTRADE_MF
															         WHERE DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD')
															         	 AND GUARANTYTYPE IS NOT NULL
															         	 AND GUARANTORNAME IS NOT NULL
															         ORDER BY BOND_ID,SUPPSERIALNO))
		         SELECT T1.CUSTID1
		           FROM TMP_BOND T1
		          WHERE T1.BOND_ID = T.BOND_ID
		          	AND T1.SUPPSERIALNO = T.SUPPSERIALNO)
		          WHERE T.DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD')
		;

		COMMIT;

		--2.20 ծȯͶ��-���һ���-��������Ϣ
    INSERT INTO RWA_DEV.RWA_TMP_CUSTOMER_INFO(
                DATANO                             --�����ڴ�
                ,CUSTOMERID                        --�ͻ����
                ,CUSTOMERNAME                      --�ͻ�����
                ,CUSTOMERTYPE                      --�ͻ�����
                ,CERTTYPE                          --֤������
                ,CERTID                            --֤������
                ,COUNTRYCODE                       --���ڹ��Ҵ���
                ,INDUSTRYTYPE                      --��ҵ����
                ,ORGNATURE                         --�Թ��ͻ�����
                ,FINANCETYPE                       --���ڻ�������
                ,SCOPE                             --���Ų���ҵ��ģ
                ,ORGID                             --�ͻ���������
                ,CUSTOMERCATEGORY                  --�ͻ����
                ,ERATINGORG                        --�ⲿ��������
                ,ERATINGTYPE                       --�ⲿ��������
                ,ERATING                           --�ⲿ�������
                ,IRATING                           --�ڲ��������
                ,UPDATEDATE                        --��������
                ,UPDATETIME                        --����ʱ��
    )
    SELECT
                p_data_dt_str                                  AS DATANO                    --�����ڴ�
                ,T1.CUSTID1                                    AS CUSTOMERID                --�ͻ����
                ,T1.GUARANTORNAME                              AS CUSTOMERNAME              --�ͻ�����
                ,'0321000002'                                  AS CUSTOMERTYPE              --�ͻ�����
                ,''					                                   AS CERTTYPE                  --֤������
                ,''				                                     AS CERTID                    --֤������
                ,T1.GUARANTORCOUNTRYCODE                       AS COUNTRYCODE               --���ڹ��Ҵ���
                ,'J66'                                         AS INDUSTRYTYPE              --��ҵ����
                ,''                                            AS ORGNATURE                 --�Թ��ͻ�����
                ,''                                            AS FINANCETYPE               --���ڻ�������
                ,'00'                                          AS SCOPE                     --���Ų���ҵ��ģ
                ,T1.BELONGORGCODE                              AS ORGID                     --�ͻ���������
                ,T1.GUARANTORCATEGORY                          AS CUSTOMERCATEGORY          --�ͻ����
                ,''                                            AS ERATINGORG                --�ⲿ��������
                ,''                                            AS ERATINGTYPE               --�ⲿ��������
                ,''                                            AS ERATING                   --�ⲿ�������
                ,''                                            AS IRATING                   --�ڲ��������
                ,''                                            AS UPDATEDATE                --��������
                ,''                                            AS UPDATETIME                --����ʱ��

    FROM        RWA.RWA_WS_BONDTRADE_MF T1
    WHERE       T1.DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD')
		AND					T1.CUSTID1 IS NOT NULL
    ;

    COMMIT;


    /*Ŀ�������ͳ��*/
    --ͳ�Ʋ���ļ�¼��
    --SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_TMP_CUSTOMER_INFO;
    --Dbms_output.Put_line('RWA_DEV.RWA_TMP_CUSTOMER_INFO��ǰ����Ĳ�¼�ͻ�������Ϣ���ݼ�¼Ϊ: ' || v_count || ' ��');

    --3.���Ŀ����е�ԭ�м�¼
    --��ղ�¼�ͻ���ϢĿ���ͬ������
    DELETE FROM RWA_DEV.BL_CUSTOMER_INFO WHERE DATANO = p_data_dt_str;

    COMMIT;

    --4.���������������ݴ�Դ����뵽��¼�ͻ���Ϣ����
    --4.1 ���뱾�ڲ�¼�Ŀͻ�������Ϣ
    INSERT INTO RWA_DEV.BL_CUSTOMER_INFO(
                DATANO                             --�����ڴ�
                ,CUSTOMERID                        --�ͻ����
                ,CUSTOMERNAME                      --�ͻ�����
                ,CUSTOMERTYPE                      --�ͻ�����
                ,CERTTYPE                          --֤������
                ,CERTID                            --֤������
                ,COUNTRYCODE                       --���ڹ��Ҵ���
                ,INDUSTRYTYPE                      --��ҵ����
                ,ORGNATURE                         --�Թ��ͻ�����
                ,FINANCETYPE                       --���ڻ�������
                ,SCOPE                             --���Ų���ҵ��ģ
                ,ORGID                             --�ͻ���������
                ,CUSTOMERCATEGORY                  --�ͻ����
                ,ERATINGORG                        --�ⲿ��������
                ,ERATINGTYPE                       --�ⲿ��������
                ,ERATING                           --�ⲿ�������
                ,IRATING                           --�ڲ��������
                ,UPDATEDATE                        --��������
                ,UPDATETIME                        --����ʱ��
    )
    SELECT
                p_data_dt_str                                 AS DATANO                    --�����ڴ�
                ,T1.CUSTOMERID                                AS CUSTOMERID                --�ͻ����
                ,T1.CUSTOMERNAME         											AS CUSTOMERNAME              --�ͻ�����
                ,T1.CUSTOMERTYPE											        AS CUSTOMERTYPE              --�ͻ�����
                ,T1.CERTTYPE								                  AS CERTTYPE                  --֤������
                ,T1.CERTID                                    AS CERTID                    --֤������
                ,T1.COUNTRYCODE										            AS COUNTRYCODE               --���ڹ��Ҵ���
                ,T1.INDUSTRYTYPE										          AS INDUSTRYTYPE              --��ҵ����
                ,''					                                  AS ORGNATURE                 --�Թ��ͻ�����
                ,''						                                AS FINANCETYPE               --���ڻ�������
                ,T1.SCOPE							      									AS SCOPE                     --���Ų���ҵ��ģ
                ,T1.ORGID							      									AS ORGID                     --�ͻ���������
                ,T1.CUSTOMERCATEGORY													AS CUSTOMERCATEGORY          --�ͻ����
                ,T1.ERATINGORG                                AS ERATINGORG                --�ⲿ��������
                ,T1.ERATINGTYPE                               AS ERATINGTYPE               --�ⲿ��������
                ,T1.ERATING                                   AS ERATING                   --�ⲿ�������
                ,''						                                AS IRATING                   --�ڲ��������
                ,T1.UPDATEDATE                                AS UPDATEDATE                --��������
                ,T1.UPDATETIME                                AS UPDATETIME                --����ʱ��

    FROM        RWA_DEV.RWA_TMP_CUSTOMER_INFO T1
    ;

    COMMIT;

    --5.��ղ�¼�ͻ���Ϣ��ʱ��
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_TMP_CUSTOMER_INFO';

    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'BL_CUSTOMER_INFO',cascade => true);

    /*Ŀ�������ͳ��*/
    --ͳ�Ʋ���ļ�¼��
    SELECT COUNT(1) INTO v_count FROM RWA_DEV.BL_CUSTOMER_INFO WHERE DATANO = p_data_dt_str;
    --Dbms_output.Put_line('RWA_DEV.BL_CUSTOMER_INFO��ǰ����Ĳ�¼�ͻ�������Ϣ���ݼ�¼Ϊ: ' || v_count || ' ��');

    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̽��� ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    p_po_rtncode := '1';
    p_po_rtnmsg  := '�ɹ�' || '-' || v_count;
    --�����쳣
    EXCEPTION
    WHEN OTHERS THEN
         --Dbms_output.Put_line('������,�������Ϊ:'||sqlcode||';������ϢΪ:'||sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace);
          ROLLBACK;
          p_po_rtncode := sqlcode;
          p_po_rtnmsg  := '��¼�ͻ�ȥ�ػ���('|| v_pro_name ||')����ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_BL_CUSTOMER_GATHER;
/

