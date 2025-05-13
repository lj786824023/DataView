CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_ZQ_BONDINFO(
			 											p_data_dt_str	IN	VARCHAR2,		--�������� yyyyMMdd
       											p_po_rtncode	OUT	VARCHAR2,		--���ر�� 1 �ɹ�,0 ʧ��
														p_po_rtnmsg		OUT	VARCHAR2		--��������
				)
  /*
    �洢��������:RWA_DEV.PRO_RWA_ZQ_BONDINFO
    ʵ�ֹ���:����ϵͳ-ծȯ-�г�����-ծȯ��Ϣ(������Դ����ϵͳ��ҵ�������Ϣȫ������RWA�г�����ծȯ�ӿڱ�ծȯ��Ϣ����)
    ���ݿھ�:ȫ��
    ����Ƶ��:�³�����
    ��  ��  :V1.0.0
    ��д��  :LISY
    ��дʱ��:2016-04-12
    ��  λ  :�Ϻ���˶��Ϣ�����ɷ����޹�˾
    Դ  ��1 :RWA_DEV.FNS_BND_INFO_B|����ϵͳծȯ��Ϣ��
    Դ  ��2 :RWA_DEV.FNS_BND_BOOK_B|����ϵͳ������
    Դ  ��3 :RWA.RWA_WS_BONDTRADE|ծȯͶ�ʲ�¼��Ϣ��
    Դ  ��4 :RWA.RWA_WP_DATASUPPLEMENT|��¼������ϸ��Ϣ��
    Ŀ���1 :RWA_DEV.RWA_ZQ_BONDINFO|����ϵͳծȯ��Ϣ��
    �����¼(�޸���|�޸�ʱ��|�޸�����):
    */
  AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_ZQ_BONDINFO';
  --�����쳣����
  v_raise EXCEPTION;
  --���嵱ǰ����ļ�¼��
  v_count INTEGER;

  BEGIN
    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*�����ȫ�����ݼ��������Ŀ���*/
    --1.���Ŀ����е�ԭ�м�¼
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_ZQ_BONDINFO';

    --2.���������������ݴ�Դ����뵽Ŀ�����
    --2.1 ����ϵͳ-ծȯ-�����
    INSERT INTO RWA_DEV.RWA_ZQ_BONDINFO(
                DATADATE                               --��������
                ,BONDID                                --ծȯID
                ,BONDNAME                              --ծȯ����
                ,BONDTYPE                              --ծȯ����
                ,ERATING                               --�ⲿ����
                ,ISSUERID                              --������ID
                ,ISSUERNAME                            --����������
                ,ISSUERTYPE                            --�����˴���
                ,ISSUERSUBTYPE                         --������С��
                ,ISSUERREGISTSTATE                     --������ע�����
                ,ISSUERSMBFLAG                         --������С΢��ҵ��ʶ
                ,BONDISSUEINTENT                       --ծȯ����Ŀ��
                ,REABSFLAG                             --���ʲ�֤ȯ����ʶ
                ,ORIGINATORFLAG                        --�Ƿ������
                ,STARTDATE                             --��ʼ����
                ,DUEDATE                               --��������
                ,ORIGINALMATURITY                      --ԭʼ����
                ,RESIDUALM                             --ʣ������
                ,RATETYPE                              --��������
                ,EXECUTIONRATE                         --ִ������
                ,NEXTREPRICEDATE                       --�´��ض�����
                ,NEXTREPRICEM                          --�´��ض�������
                ,MODIFIEDDURATION                      --��������
                ,DENOMINATION                          --���
                ,CURRENCY                              --����

    )
    WITH TEMP_BND_BOOK AS (
    										SELECT BOND_ID,
												       INITIAL_COST,
												       INT_ADJUST,
												       MKT_VALUE_CHANGE,
												       RECEIVABLE_INT,
												       ACCOUNTABLE_INT,
												       PAR_VALUE
												  FROM (SELECT BOND_ID,
												               INITIAL_COST,
												               INT_ADJUST,
												               MKT_VALUE_CHANGE,
												               RECEIVABLE_INT,
												               ACCOUNTABLE_INT,
												               PAR_VALUE,
												               ROW_NUMBER() OVER(PARTITION BY BOND_ID ORDER BY SORT_SEQ DESC) RM
												          FROM FNS_BND_BOOK_B
												         WHERE AS_OF_DATE <= p_data_dt_str
												           AND DATANO = p_data_dt_str)
												 WHERE RM = 1
												   AND NVL(INITIAL_COST, 0) + NVL(INT_ADJUST, 0) + NVL(MKT_VALUE_CHANGE, 0) + NVL(ACCOUNTABLE_INT, 0) <> 0
		)
		, TMP_ABS_BOND AS (
												SELECT ABSR.ZQNM AS BOND_ID, ABSR.ZZCZQHBZ AS REABSFLAG
												  FROM RWA.RWA_WS_ABS_INVEST_EXPOSURE ABSR
												 INNER JOIN RWA.RWA_WP_DATASUPPLEMENT RWD
												    ON ABSR.SUPPORGID = RWD.ORGID
												   AND RWD.DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD')
												   AND RWD.SUPPTMPLID = 'M-0140'
												   AND RWD.SUBMITFLAG = '1'
												 WHERE ABSR.DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
    											 AND ABSR.YHJS = '02'																			--02 Ͷ�ʻ��� ����ȫ�ŵ����л�����¶��
												UNION
												SELECT RWAIE.ZQNM AS BOND_ID, RWAIE.ZZCZQHBZ AS REABSFLAG
												  FROM RWA.RWA_WS_ABS_ISSUE_EXPOSURE RWAIE
												 INNER JOIN RWA.RWA_WP_DATASUPPLEMENT RWD
												    ON RWAIE.SUPPORGID = RWD.ORGID
												   AND RWD.DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD')
												   AND RWD.SUPPTMPLID = 'M-0131'
												   AND RWD.SUBMITFLAG = '1'
												 WHERE RWAIE.DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
    											 AND RWAIE.YHJS <> '02'																		--���� 02 Ͷ�ʻ���  ����ȫ�ŵ����л�����¶��
		)
    SELECT
                TO_DATE(p_data_dt_str,'YYYYMMDD')            AS DATADATE                 --��������
                ,T1.BOND_ID														     	 AS BONDID                   --ծȯID
                ,T4.BONDNAME                    				 		 AS BONDNAME                 --ծȯ����
                ,T1.BOND_TYPE1                           		 AS BONDTYPE                 --ծȯ����
                ,RWA_DEV.GETSTANDARDRATING1(T4.BONDRATING)   AS ERATING                  --�ⲿ����          					 ת��Ϊ����
                ,CASE WHEN T1.BOND_ID = 'B200801010095' THEN 'MZXXZ'																														--ë��ϯ����Ĭ�ϲ�������
                			WHEN T3.BUSINESSTYPE = '1040102040' AND (T4.ISCOUNTTR = '1' OR T4.BONDNAME LIKE '%��ծ%') THEN 'ZGZYZF'														--�����ծȯͶ�ʹ�ծʱĬ�Ϸ�����Ϊ�й���������
                			WHEN T3.BUSINESSTYPE = '1040202010' AND T4.BONDFLAG04 = '1' AND T4.MARKETSCATEGORY = '01' THEN T4.BONDPUBLISHCOUNTRY || 'ZYZF'		--���ծȯͶ�ʾ�����������
                			WHEN T3.BUSINESSTYPE = '1040202010' AND T4.BONDFLAG04 = '1' AND T4.MARKETSCATEGORY = '02' THEN T4.BONDPUBLISHCOUNTRY || 'ZYYH'		--���ծȯͶ�ʾ�����������
                			WHEN T3.BUSINESSTYPE = '1040202010' AND T4.BONDFLAG04 = '1' AND T4.MARKETSCATEGORY = '03' THEN T4.BONDPUBLISHCOUNTRY || 'BMST'		--���ծȯͶ�ʾ�����һ����ע��Ĺ�������ʵ��
                			WHEN REPLACE(T4.BONDPUBLISHID,'NCM_','') IS NULL THEN 'XN-YBGS'
                 ELSE T4.BONDPUBLISHID
                 END						                      			 AS ISSUERID                 --������ID
                ,CASE WHEN T1.BOND_ID = 'B200801010095' THEN 'ë��ϯ��'																																	--ë��ϯ����Ĭ�ϲ�������
                			WHEN T3.BUSINESSTYPE = '1040102040' AND (T4.ISCOUNTTR = '1' OR T4.BONDNAME LIKE '%��ծ%') THEN '�й���������'															--�����ծȯͶ�ʹ�ծʱĬ�Ϸ�����Ϊ�й���������
                			WHEN T3.BUSINESSTYPE = '1040202010' AND T4.BONDFLAG04 = '1' AND T4.MARKETSCATEGORY = '01' THEN T4.BONDPUBLISHCOUNTRY || '��������'				--���ծȯͶ�ʾ�����������
                			WHEN T3.BUSINESSTYPE = '1040202010' AND T4.BONDFLAG04 = '1' AND T4.MARKETSCATEGORY = '02' THEN T4.BONDPUBLISHCOUNTRY || '��������'				--���ծȯͶ�ʾ�����������
                			WHEN T3.BUSINESSTYPE = '1040202010' AND T4.BONDFLAG04 = '1' AND T4.MARKETSCATEGORY = '03' THEN T4.BONDPUBLISHCOUNTRY || '��������ʵ��'		--���ծȯͶ�ʾ�����һ����ע��Ĺ�������ʵ��
                			WHEN REPLACE(T4.BONDPUBLISHID,'NCM_','') IS NULL THEN '����һ�㹫˾'
                 ELSE T5.CUSTOMERNAME
                 END								                         AS ISSUERNAME               --����������
                ,CASE WHEN T1.BOND_ID = 'B200801010095' THEN '02'																	--ë��ϯ����Ĭ�ϲ�������
                			WHEN T3.BUSINESSTYPE = '1040102040' AND (T4.ISCOUNTTR = '1' OR T4.BONDNAME LIKE '%��ծ%') THEN '01'					--�����ծȯͶ�ʹ�ծʱĬ�Ϸ�����Ϊ�й���������
                			WHEN T3.BUSINESSTYPE = '1040202010' AND T4.BONDFLAG04 = '1' THEN '01'				--���ծȯͶ�ʾ�����������
                			WHEN REPLACE(T4.BONDPUBLISHID,'NCM_','') IS NULL THEN '03'
                 ELSE SUBSTR(T5.RWACUSTOMERTYPE,1,2)
                 END													               AS ISSUERTYPE               --�����˴���        					 ����ӳ��
                ,CASE WHEN T1.BOND_ID = 'B200801010095' THEN '0205'																--ë��ϯ����Ĭ�ϲ�������
                			WHEN T3.BUSINESSTYPE = '1040102040' AND (T4.ISCOUNTTR = '1' OR T4.BONDNAME LIKE '%��ծ%') THEN '0101'
                			WHEN T3.BUSINESSTYPE = '1040202010' AND T4.BONDFLAG04 = '1' AND T4.MARKETSCATEGORY = '01' THEN '0102'				--���ծȯͶ�ʾ�����������
                			WHEN T3.BUSINESSTYPE = '1040202010' AND T4.BONDFLAG04 = '1' AND T4.MARKETSCATEGORY = '02' THEN '0104'				--���ծȯͶ�ʾ�����������
                			WHEN T3.BUSINESSTYPE = '1040202010' AND T4.BONDFLAG04 = '1' AND T4.MARKETSCATEGORY = '03' THEN '0107'				--���ծȯͶ�ʾ�����һ����ע��Ĺ�������ʵ��
                			WHEN REPLACE(T4.BONDPUBLISHID,'NCM_','') IS NULL THEN '0301'
                 ELSE T5.RWACUSTOMERTYPE
                 END							                           AS ISSUERSUBTYPE            --������С��        					 ����ӳ��
                ,CASE WHEN NVL(T5.COUNTRYCODE,'CHN') = 'CHN' THEN '01'
                 ELSE '02'
                 END								                  			 AS ISSUERREGISTSTATE        --������ע�����
                ,NVL(T5.ISSUPERVISESTANDARSMENT,'0')         AS ISSUERSMBFLAG            --������С΢��ҵ��ʶ					 Ĭ�ϣ���(0)
                ,SUBSTR(NVL(T4.BONDPUBLISHPURPOSE,'0020'),2,2)
                																             AS BONDISSUEINTENT          --ծȯ����Ŀ��      					 Ĭ�ϣ�����(02)
                ,NVL(T7.REABSFLAG,'0')                       AS REABSFLAG                --���ʲ�֤ȯ����ʶ  					 Ĭ�ϣ���(0)
                ,CASE WHEN REPLACE(T5.CERTID,'-','') = '202869177' THEN '1'
                 ELSE '0'
                 END																				 AS ORIGINATORFLAG   				 --�Ƿ������      					 1. ���������ƣ���������(202869177)����Ϊ�ǣ� 2. ����Ϊ��
                ,T1.ORIGINATION_DATE                         AS STARTDATE                --��ʼ����          					 ����Ϣ�����(FNS_BND_INFO_B.ORIGINATION_DATE)
                ,T1.MATURITY_DATE                            AS DUEDATE                  --��������
                ,CASE WHEN (TO_DATE(T1.MATURITY_DATE,'YYYYMMDD') - TO_DATE(T1.ORIGINATION_DATE,'YYYYMMDD')) / 365<0
                      THEN 0
                      ELSE (TO_DATE(T1.MATURITY_DATE,'YYYYMMDD') - TO_DATE(T1.ORIGINATION_DATE,'YYYYMMDD')) / 365
                 END                                         AS ORIGINALMATURITY         --ԭʼ����
                ,CASE WHEN (TO_DATE(T1.MATURITY_DATE,'YYYYMMDD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365<0
                      THEN 0
                      ELSE (TO_DATE(T1.MATURITY_DATE,'YYYYMMDD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365
                 END                                         AS RESIDUALM                --ʣ������
                ,CASE WHEN T1.RATE_TYPE	= '10' THEN '01'																 --�̶�����
                 ELSE '02'																															 --��������(���Զ��ض��ۺ��ֹ��ض���)
                 END				                                 AS RATETYPE                 --��������(��¼)
                ,NVL(T1.PAR_RATE,0) / 100                    AS EXECUTIONRATE            --ִ������
                ,CASE WHEN T1.RATE_TYPE = '10' OR T1.REPRICE_DATE < p_data_dt_str THEN T1.MATURITY_DATE
                 ELSE T1.REPRICE_DATE
                 END                                         AS NEXTREPRICEDATE          --�´��ض�����      					1. ���������ͣ��̶������´��ض����գ��������ڣ�2. ����ȡϵͳ�ֶ�
                ,CASE WHEN T1.RATE_TYPE = '10' THEN NULL
                 ELSE CASE WHEN (TO_DATE(CASE WHEN T1.REPRICE_DATE < p_data_dt_str THEN T1.MATURITY_DATE ELSE T1.REPRICE_DATE END,'YYYYMMDD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365 < 0 THEN 0
                 			ELSE (TO_DATE(CASE WHEN T1.REPRICE_DATE < p_data_dt_str THEN T1.MATURITY_DATE ELSE T1.REPRICE_DATE END,'YYYYMMDD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365
                 			END
                 END																				 AS NEXTREPRICEM             --�´��ض�������							 1. ���������ͣ��̶�����Ĭ��Ϊ��NULL��2. ����ȡ�´��ض�����-�������ڣ���λ����
                ,NULL                                        AS MODIFIEDDURATION         --��������
                ,T2.PAR_VALUE            										 AS DENOMINATION             --���              					 ��¼
                ,NVL(T1.CURRENCY_CODE,'CNY')                 AS CURRENCY                 --����

    FROM				RWA_DEV.FNS_BND_INFO_B T1
		INNER JOIN	TEMP_BND_BOOK T2
		ON					T1.BOND_ID = T2.BOND_ID
		LEFT JOIN		RWA_DEV.NCM_BUSINESS_DUEBILL T3														--�Ŵ���ݱ�
		ON					'CW_IMPORTDATA' || T1.BOND_ID = T3.THIRDPARTYACCOUNTS
		--AND					T3.BUSINESSTYPE IN ('1040102040','1040202010')						--1040102040-�����ծȯͶ��;1040202010-���ծȯͶ��
		AND					T3.DATANO = p_data_dt_str
		LEFT JOIN		RWA_DEV.NCM_BOND_INFO T4																	--�Ŵ�ծȯ��Ϣ��
	  ON					T3.RELATIVESERIALNO2 = T4.OBJECTNO
		AND					T4.OBJECTTYPE = 'BusinessContract'
		AND					T4.DATANO = p_data_dt_str
		LEFT JOIN		RWA_DEV.NCM_CUSTOMER_INFO T5															--ͳһ�ͻ���Ϣ��
	  ON					T4.BONDPUBLISHID = T5.CUSTOMERID
	  AND					T5.DATANO = p_data_dt_str
		LEFT JOIN		TMP_ABS_BOND T7
		ON					T1.BOND_ID = T7.BOND_ID
		AND					T7.REABSFLAG = '1'
		WHERE 			T1.ASSET_CLASS = '10'																			--���������˻������г�����
		AND					T1.DATANO = p_data_dt_str
		AND 				T1.BOND_CODE IS NOT NULL																	--�ų���Ч��ծȯ����
	  ;

    COMMIT;

    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_ZQ_BONDINFO',cascade => true);

    /*Ŀ�������ͳ��*/
    --ͳ�Ʋ���ļ�¼��
    SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_ZQ_BONDINFO;
    --Dbms_output.Put_line('RWA_DEV.RWA_ZQ_BONDINFO��ǰ����Ĳ���ϵͳ-ծȯ(�г�����)-ծȯ��Ϣ��¼Ϊ: ' || v_count || ' ��');



    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̽��� ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    p_po_rtncode := '1';
	  p_po_rtnmsg  := '�ɹ�' || '-' || v_count;
		--�����쳣
		EXCEPTION
    WHEN OTHERS THEN
				 --Dbms_output.Put_line('������,�������Ϊ:'||sqlcode||';������ϢΪ:'||sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace);
       	 ROLLBACK;
   			 p_po_rtncode := sqlcode;
   			 p_po_rtnmsg  := 'ծȯ��Ϣ('|| v_pro_name ||')ETLת��ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_ZQ_BONDINFO;
/

