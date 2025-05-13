CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_ZQ_ISSUERRATING(
			 											p_data_dt_str	IN	VARCHAR2,		--�������� yyyyMMdd
       											p_po_rtncode	OUT	VARCHAR2,		--���ر�� 1 �ɹ�,0 ʧ��
														p_po_rtnmsg		OUT	VARCHAR2		--��������
				)
  /*
    �洢��������:RWA_DEV.PRO_RWA_ZQ_ISSUERRATING
    ʵ�ֹ���:����ϵͳ-ծȯ-�г�����-������������Ϣ(������Դ��¼���н�ծȯ�����Ϣȫ������RWA�г�����ծȯ�ӿڱ�����������Ϣ����)
    ���ݿھ�:ȫ��
    ����Ƶ��:�³�����
    ��  ��  :V1.0.0
    ��д��  :LISY
    ��дʱ��:2016-04-28
    ��  λ  :�Ϻ���˶��Ϣ�����ɷ����޹�˾
    Դ  ��1 :RWA.RWA_WS_BONDTRADE|ծȯͶ�ʲ�¼��Ϣ��
    Դ  ��2 :RWA.RWA_WP_DATASUPPLEMENT|��¼������ϸ��Ϣ��
    Դ  ��3 :RWA_DEV.FNS_BND_INFO_B|����ϵͳծȯ��Ϣ��
    Ŀ���  :RWA_DEV.RWA_ZQ_ISSUERRATING|����ϵͳծȯ�෢����������Ϣ��
    �����¼(�޸���|�޸�ʱ��|�޸�����):
    */
  AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_ZQ_ISSUERRATING';
  --�����쳣����
  v_raise EXCEPTION;
  --���嵱ǰ����ļ�¼��
  v_count INTEGER;

  BEGIN
    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*�����ȫ�����ݼ��������Ŀ���*/
    --1.���Ŀ����е�ԭ�м�¼
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_ZQ_ISSUERRATING';

    --2.���������������ݴ�Դ����뵽Ŀ�����
    --2.1 ����ϵͳ-ծȯͶ��-�ǹ�ծ�ҷ������Ȩ
    INSERT INTO RWA_DEV.RWA_ZQ_ISSUERRATING(
                DATADATE                               --��������
                ,ISSUERID                           	 --������ID
                ,ISSUERNAME                    	 	 		 --����������
                ,RATINGORG                     	 	 		 --��������
                ,RATINGRESULT                  	 	 		 --�������
                ,RATINGDATE                    	 	 		 --��������
                ,FETCHFLAG                     	 	 		 --ȡ����ʶ
    )
    WITH TEMP_BND_BOOK AS (
    										SELECT BOND_ID,
												       INITIAL_COST,
												       INT_ADJUST,
												       MKT_VALUE_CHANGE,
												       RECEIVABLE_INT,
												       ACCOUNTABLE_INT
												  FROM (SELECT BOND_ID,
												               INITIAL_COST,
												               INT_ADJUST,
												               MKT_VALUE_CHANGE,
												               RECEIVABLE_INT,
												               ACCOUNTABLE_INT,
												               ROW_NUMBER() OVER(PARTITION BY BOND_ID ORDER BY SORT_SEQ DESC) RM
												          FROM FNS_BND_BOOK_B
												         WHERE AS_OF_DATE <= p_data_dt_str
												           AND DATANO = p_data_dt_str)
												 WHERE RM = 1
												   AND NVL(INITIAL_COST, 0) + NVL(INT_ADJUST, 0) + NVL(MKT_VALUE_CHANGE, 0) + NVL(ACCOUNTABLE_INT, 0) <> 0
		)
		, TEMP_BND_CUST AS (
												SELECT DISTINCT
															 T4.BONDPUBLISHID			AS CUSTOMERID
													FROM RWA_DEV.FNS_BND_INFO_B T1
										INNER JOIN TEMP_BND_BOOK T2
														ON T1.BOND_ID = T2.BOND_ID
										INNER JOIN RWA_DEV.NCM_BUSINESS_DUEBILL T3														--�Ŵ���ݱ�
														ON 'CW_IMPORTDATA' || T1.BOND_ID = T3.THIRDPARTYACCOUNTS
													 AND T3.DATANO = p_data_dt_str
										INNER JOIN RWA_DEV.NCM_BOND_INFO T4																		--�Ŵ�ծȯ��Ϣ��
	  												ON T3.RELATIVESERIALNO2 = T4.OBJECTNO
													 AND T4.OBJECTTYPE = 'BusinessContract'
													 AND NVL(T4.ISCOUNTTR,'2') <> '1'                               --�����ծȯ���ǹ�ծ
                           AND NVL(T4.BONDFLAG04,'2') <> '1'                              --���ծȯ������Ȩ��
													 AND T4.DATANO = p_data_dt_str
												 WHERE T1.ASSET_CLASS = '10'																			--���������˻������г�����
													 AND T1.DATANO = p_data_dt_str
													 AND T1.BOND_CODE IS NOT NULL																		--�ų���Ч��ծȯ����
		)
		SELECT 			DATADATE
    						,ISSUERID
    						,ISSUERNAME
    						,RATINGORG
    						,RATINGRESULT
    						,RATINGDATE
    						,FETCHFLAG
   	FROM
   	(
    SELECT
    						TO_DATE(p_data_dt_str,'YYYYMMDD')            AS DATADATE                 --��������
                ,T1.CUSTOMERID													     AS ISSUERID               	 --������ID
                ,T4.CUSTOMERNAME	           								 AS ISSUERNAME          	 	 --����������
                ,T2.EVALUTEORG					  									 AS RATINGORG        	 	 	 	 --��������
                ,T3.DESCRATING															 AS RATINGRESULT        	 	 --�������
                ,T2.EVALUTEDATE															 AS RATINGDATE          	 	 --��������
                ,''                         				 				 AS FETCHFLAG           	 	 --ȡ����ʶ
                ,RANK() OVER(PARTITION BY T1.CUSTOMERID,T2.EVALUTEORG ORDER BY T2.EVALUTEDATE DESC) AS RK
    						,ROW_NUMBER() OVER(PARTITION BY T1.CUSTOMERID,T2.EVALUTEORG,T2.EVALUTEDATE ORDER BY T3.DESCRATING) AS RM
    						,COUNT(1) OVER(PARTITION BY T1.CUSTOMERID,T2.EVALUTEORG,T2.EVALUTEDATE) AS RN

    FROM				TEMP_BND_CUST T1
    INNER JOIN	RWA_DEV.NCM_CUSTOMER_RATING T2
    ON					T1.CUSTOMERID = T2.CUSTOMERID
    AND					T2.DATANO = p_data_dt_str
	  INNER JOIN	RWA_DEV.RWA_CD_RATING_MAPPING T3
	  ON					T2.EVALUTEORG = T3.SRCRATINGORG
	  AND					T2.EVALUTELEVEL = T3.SRCRATINGNAME
	  AND					T3.MAPPINGTYPE = '01'																			--ȫ������
	  AND					T3.SRCRATINGTYPE = '01'																		--��������
	  LEFT JOIN		RWA_DEV.NCM_CUSTOMER_INFO T4															--ͳһ�ͻ���Ϣ��
	  ON					T1.CUSTOMERID = T4.CUSTOMERID
	  AND					T4.DATANO = p_data_dt_str
	  )
	  WHERE				RK = 1
		AND					RM = (CASE WHEN RN = 1 THEN 1 ELSE 2 END)									--����ͬһ�ͻ���ͬһ�챻ͬһ�һ���������Σ�ȡ�ڶ��õ��������
		;

    COMMIT;


    --2.2 ����ϵͳ-ծȯͶ��-��ծ
    INSERT INTO RWA_DEV.RWA_ZQ_ISSUERRATING(
                DATADATE                               --��������
                ,ISSUERID                           	 --������ID
                ,ISSUERNAME                    	 	 		 --����������
                ,RATINGORG                     	 	 		 --��������
                ,RATINGRESULT                  	 	 		 --�������
                ,RATINGDATE                    	 	 		 --��������
                ,FETCHFLAG                     	 	 		 --ȡ����ʶ
    )
    WITH TEMP_BND_BOOK AS (
    										SELECT BOND_ID,
												       INITIAL_COST,
												       INT_ADJUST,
												       MKT_VALUE_CHANGE,
												       RECEIVABLE_INT,
												       ACCOUNTABLE_INT
												  FROM (SELECT BOND_ID,
												               INITIAL_COST,
												               INT_ADJUST,
												               MKT_VALUE_CHANGE,
												               RECEIVABLE_INT,
												               ACCOUNTABLE_INT,
												               ROW_NUMBER() OVER(PARTITION BY BOND_ID ORDER BY SORT_SEQ DESC) RM
												          FROM FNS_BND_BOOK_B
												         WHERE AS_OF_DATE <= p_data_dt_str
												           AND DATANO = p_data_dt_str)
												 WHERE RM = 1
												   AND NVL(INITIAL_COST, 0) + NVL(INT_ADJUST, 0) + NVL(MKT_VALUE_CHANGE, 0) + NVL(ACCOUNTABLE_INT, 0) <> 0
		)
    SELECT      DISTINCT
    						TO_DATE(p_data_dt_str,'YYYYMMDD')            AS DATADATE                 --��������
                ,'ZGZYZF'																     AS ISSUERID               	 --������ID
                ,'�й���������'		           								 AS ISSUERNAME          	 	 --����������
                ,'01'										  									 AS RATINGORG        	 	 	 	 --��������
                ,(SELECT RATINGRESULT FROM RWA.RWA_WP_COUNTRYRATING WHERE COUNTRYCODE = 'CHN' AND ISINUSE = '1')
                																						 AS RATINGRESULT        	 	 --�������                 ��ת��Ϊ����
                ,(SELECT REPLACE(RATINGSTARTDATE,'/','') FROM RWA.RWA_WP_COUNTRYRATING WHERE COUNTRYCODE = 'CHN' AND ISINUSE = '1')
                																						 AS RATINGDATE          	 	 --��������
                ,''                         				 				 AS FETCHFLAG           	 	 --ȡ����ʶ

    FROM				RWA_DEV.FNS_BND_INFO_B T1
		INNER JOIN	TEMP_BND_BOOK T2
		ON					T1.BOND_ID = T2.BOND_ID
		INNER JOIN	RWA_DEV.NCM_BUSINESS_DUEBILL T3														--�Ŵ���ݱ�
		ON					'CW_IMPORTDATA' || T1.BOND_ID = T3.THIRDPARTYACCOUNTS
		AND					T3.BUSINESSTYPE = '1040102040'														--�����ծȯͶ��
		AND					T3.DATANO = p_data_dt_str
		INNER JOIN	RWA_DEV.NCM_BOND_INFO T4																	--�Ŵ�ծȯ��Ϣ��
	  ON					T3.RELATIVESERIALNO2 = T4.OBJECTNO
		AND					T4.OBJECTTYPE = 'BusinessContract'
		AND					(T4.ISCOUNTTR = '1' OR T4.BONDNAME LIKE '%��ծ%')					--��ծ
		AND					T4.DATANO = p_data_dt_str
		WHERE 			T1.ASSET_CLASS = '10'																			--���������˻������г�����
		AND					T1.DATANO = p_data_dt_str
		AND 				T1.BOND_CODE IS NOT NULL																	--�ų���Ч��ծȯ����
		;

    COMMIT;


    --2.3 ����ϵͳ-ծȯͶ��-�����Ȩ
    INSERT INTO RWA_DEV.RWA_ZQ_ISSUERRATING(
                DATADATE                               --��������
                ,ISSUERID                           	 --������ID
                ,ISSUERNAME                    	 	 		 --����������
                ,RATINGORG                     	 	 		 --��������
                ,RATINGRESULT                  	 	 		 --�������
                ,RATINGDATE                    	 	 		 --��������
                ,FETCHFLAG                     	 	 		 --ȡ����ʶ
    )
    WITH TEMP_BND_BOOK AS (
    										SELECT BOND_ID,
												       INITIAL_COST,
												       INT_ADJUST,
												       MKT_VALUE_CHANGE,
												       RECEIVABLE_INT,
												       ACCOUNTABLE_INT
												  FROM (SELECT BOND_ID,
												               INITIAL_COST,
												               INT_ADJUST,
												               MKT_VALUE_CHANGE,
												               RECEIVABLE_INT,
												               ACCOUNTABLE_INT,
												               ROW_NUMBER() OVER(PARTITION BY BOND_ID ORDER BY SORT_SEQ DESC) RM
												          FROM FNS_BND_BOOK_B
												         WHERE AS_OF_DATE <= p_data_dt_str
												           AND DATANO = p_data_dt_str)
												 WHERE RM = 1
												   AND NVL(INITIAL_COST, 0) + NVL(INT_ADJUST, 0) + NVL(MKT_VALUE_CHANGE, 0) + NVL(ACCOUNTABLE_INT, 0) <> 0
		)
    SELECT      DISTINCT
    						TO_DATE(p_data_dt_str,'YYYYMMDD')            AS DATADATE                 --��������
                ,CASE WHEN T4.MARKETSCATEGORY = '01' THEN T4.BONDPUBLISHCOUNTRY || 'ZYZF'													--������Ȩ���һ򾭼�ʵ���������������
                			WHEN T4.MARKETSCATEGORY = '02' THEN T4.BONDPUBLISHCOUNTRY || 'ZYYH'													--������������
                			ELSE T4.BONDPUBLISHCOUNTRY || 'BMST'																												--������һ����ע��Ĺ�������ʵ��
                 END																		     AS ISSUERID               	 --������ID
                ,CASE WHEN T4.MARKETSCATEGORY = '01' THEN T4.BONDPUBLISHCOUNTRY || '��������'											--������Ȩ���һ򾭼�ʵ���������������
                			WHEN T4.MARKETSCATEGORY = '02' THEN T4.BONDPUBLISHCOUNTRY || '��������'											--������������
                			ELSE T4.BONDPUBLISHCOUNTRY || '��������ʵ��'																								--������һ����ע��Ĺ�������ʵ��
                 END							           								 AS ISSUERNAME          	 	 --����������
                ,'01'										  									 AS RATINGORG        	 	 	 	 --��������
                ,T5.RATINGRESULT														 AS RATINGRESULT        	 	 --�������                 ��ת��Ϊ����
                ,REPLACE(T5.RATINGSTARTDATE,'/','')					 AS RATINGDATE          	 	 --��������
                ,''                         				 				 AS FETCHFLAG           	 	 --ȡ����ʶ

    FROM				RWA_DEV.FNS_BND_INFO_B T1
		INNER JOIN	TEMP_BND_BOOK T2
		ON					T1.BOND_ID = T2.BOND_ID
		INNER JOIN	RWA_DEV.NCM_BUSINESS_DUEBILL T3														--�Ŵ���ݱ�
		ON					'CW_IMPORTDATA' || T1.BOND_ID = T3.THIRDPARTYACCOUNTS
		AND					T3.BUSINESSTYPE = '1040202011'														--���ծȯͶ��
		AND					T3.DATANO = p_data_dt_str
		INNER JOIN	RWA_DEV.NCM_BOND_INFO T4																	--�Ŵ�ծȯ��Ϣ��
	  ON					T3.RELATIVESERIALNO2 = T4.OBJECTNO
		AND					T4.OBJECTTYPE = 'BusinessContract'
		AND					T4.BONDFLAG04 = '1'																				--��Ȩ��
		AND					T4.DATANO = p_data_dt_str
		INNER JOIN	RWA.RWA_WP_COUNTRYRATING T5
		ON					T4.BONDPUBLISHCOUNTRY = T5.COUNTRYCODE
		AND					T5.ISINUSE = '1'
		WHERE 			T1.ASSET_CLASS = '10'																			--���������˻������г�����
		AND					T1.DATANO = p_data_dt_str
		AND 				T1.BOND_CODE IS NOT NULL																	--�ų���Ч��ծȯ����
		;

    COMMIT;

    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_ZQ_ISSUERRATING',cascade => true);

    /*Ŀ�������ͳ��*/
    --ͳ�Ʋ���ļ�¼��
    SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_ZQ_ISSUERRATING;
    --Dbms_output.Put_line('RWA_DEV.RWA_ZQ_ISSUERRATING��ǰ����Ĳ���ϵͳ-ծȯ(�г�����)-������������Ϣ���ݼ�¼Ϊ: ' || v_count || ' ��');



    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̽��� ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    p_po_rtncode := '1';
	  p_po_rtnmsg  := '�ɹ�' || '-' || v_count;
		--�����쳣
		EXCEPTION
    WHEN OTHERS THEN
				 --Dbms_output.Put_line('������,�������Ϊ:'||sqlcode||';������ϢΪ:'||sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace);
       	 ROLLBACK;
   			 p_po_rtncode := sqlcode;
   			 p_po_rtnmsg  := '������������Ϣ('|| v_pro_name ||')ETLת��ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_ZQ_ISSUERRATING;
/

