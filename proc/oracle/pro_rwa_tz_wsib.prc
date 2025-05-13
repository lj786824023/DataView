CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_TZ_WSIB(
			 											p_data_dt_str	IN	VARCHAR2,		--�������� yyyyMMdd
       											p_po_rtncode	OUT	VARCHAR2,		--���ر�� 1 �ɹ�,0 ʧ��
														p_po_rtnmsg		OUT	VARCHAR2		--��������
				)
  /*
    �洢��������:RWA_DEV.PRO_RWA_TZ_WSIB
    ʵ�ֹ���:����ϵͳ-Ͷ��-��¼�̵�(������Դ����ϵͳ��ҵ�������Ϣȫ������RWAͶ�ʲ�¼�̵ױ���)
    ���ݿھ�:ȫ��
    ����Ƶ��:�³�����
    ��  ��  :V1.0.0
    ��д��  :LISY
    ��дʱ��:2017-07-03
    ��  λ  :�Ϻ���˶��Ϣ�����ɷ����޹�˾
    Դ  ��1 :RWA_DEV.FNS_BND_INFO_B|����ϵͳծȯ��Ϣ��
    Դ  ��2 :RWA_DEV.FNS_BND_BOOK_B|����ϵͳ������
    Դ  ��3 :RWA.ORG_INFO|������Ϣ��
    Դ  ��4 :RWA.RWA_WS_RESERVE|Ӧ�տ�Ͷ��׼����¼��
    Դ  ��5 :RWA.RWA_WP_SUPPTASKORG|��¼��������ַ����ñ�
    Դ  ��6 :RWA.RWA_WP_SUPPTASK|��¼���񷢲���
    Ŀ���1 :RWA.RWA_WSIB_RESERVE|Ӧ�տ�Ͷ��׼����¼�̵ױ�
    �����¼(�޸���|�޸�ʱ��|�޸�����):
    */
  AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_TZ_WSIB';
  --�����쳣����
  v_raise EXCEPTION;
  --���嵱ǰ����ļ�¼��
  v_count INTEGER;

  BEGIN
    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*�����ȫ�����ݼ��������Ŀ���*/
    --1.���Ŀ����е�ԭ�м�¼
    --���Ӧ�տ�Ͷ��׼����¼�̵ױ�
    --EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA.RWA_WSIB_RESERVE';
    --DELETE FROM RWA.RWA_WSIB_RESERVE WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD');

    --���ծȯͶ�ʻ��һ����̵ױ�
    --EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA.RWA_WSIB_BONDTRADE_MF';
    DELETE FROM RWA.RWA_WSIB_BONDTRADE_MF WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD');

    COMMIT;

    --2.���������������ݴ�Դ����뵽Ŀ�����
    --2.1 ����ϵͳ-Ӧ�տ�Ͷ��ҵ��׼����
    /*
    INSERT INTO RWA.RWA_WSIB_RESERVE(
                DATADATE                               --��������
                ,ORGID                                 --����ID
                ,BOND_ID                       				 --ծȯ����
                ,BOND_CODE                     				 --ծȯ����
                ,BOND_NAME                     				 --ծȯ����
                ,DEPARTMENT   								 				 --���˻���
                ,EFFECT_DATE  								 				 --��ʼ����
                ,MATURITY_DATE                 				 --��������
                ,CURRENCY_CODE                 				 --����
                ,BOND_BAL                      				 --���
                ,RESERVESUM                    				 --������
    )
    WITH TEMP_BND_BOOK AS (
    										SELECT BOND_ID,
												       NVL(INITIAL_COST, 0) + NVL(MKT_VALUE_CHANGE, 0) + NVL(ACCOUNTABLE_INT, 0) AS BOND_BAL
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
												   AND NVL(INITIAL_COST, 0) + NVL(MKT_VALUE_CHANGE, 0) + NVL(ACCOUNTABLE_INT, 0) <> 0 --NVL(INT_ADJUST, 0) + ����Ϣ�������⣬��Ϊ���ֹ�����
		)
		, TMP_SUPPORG AS (
								SELECT T1.ORGID AS ORGID
										   ,CASE WHEN T3.ORGLEVEL > 2 THEN T4.SORTNO ELSE T3.SORTNO END AS SORTNO
								  FROM RWA.RWA_WP_SUPPTASKORG T1
						INNER JOIN RWA.RWA_WP_SUPPTASK T2
								    ON T1.SUPPTASKID = T2.SUPPTASKID
								   AND T2.ENABLEFLAG = '01'
						 LEFT JOIN RWA.ORG_INFO T3
								    ON T1.ORGID = T3.ORGID
						 LEFT JOIN RWA.ORG_INFO T4
	                	ON T3.BELONGORGID = T4.ORGID
								 WHERE T1.SUPPTMPLID = 'M-0210'
							ORDER BY T3.SORTNO
		)
		SELECT
								DATADATE                               --��������
                ,ORGID                                 --����ID
                ,BOND_ID                       				 --ծȯ����
                ,BOND_CODE                     				 --ծȯ����
                ,BOND_NAME                     				 --ծȯ����
                ,DEPARTMENT   								 				 --���˻���
                ,EFFECT_DATE  								 				 --��ʼ����
                ,MATURITY_DATE                 				 --��������
                ,CURRENCY_CODE                 				 --����
                ,BOND_BAL                      				 --���
                ,RESERVESUM                    				 --������
		FROM (
    SELECT
                TO_DATE(p_data_dt_str,'YYYYMMDD')            AS DATADATE                 --��������
                ,NVL(T8.ORGID,(SELECT ORGID FROM TMP_SUPPORG WHERE ROWNUM = 1))
                												                     AS ORGID										 --����ID                     		���ղ�¼����������������������г�������ֲ�
                ,RANK() OVER(PARTITION BY T1.BOND_ID ORDER BY LENGTH(NVL(T8.SORTNO,(SELECT SORTNO FROM TMP_SUPPORG WHERE ROWNUM = 1))) DESC)
                																						 AS RECORDNO								 --�������
                ,T1.BOND_ID                             		 AS BOND_ID                  --ծȯ����
                ,T1.BOND_CODE 										 					 AS BOND_CODE                --ծȯ����
                ,T1.BOND_NAME                           		 AS BOND_NAME                --ծȯ����
                ,T1.DEPARTMENT															 AS DEPARTMENT   						 --���˻���
                ,TO_CHAR(TO_DATE(T1.EFFECT_DATE,'YYYYMMDD'),'YYYY-MM-DD')
                																						 AS EFFECT_DATE  						 --��ʼ����
                ,TO_CHAR(TO_DATE(T1.MATURITY_DATE,'YYYYMMDD'),'YYYY-MM-DD')
                													                   AS MATURITY_DATE            --��������
                ,NVL(T1.CURRENCY_CODE,'CNY')                 AS CURRENCY_CODE            --����
                ,T3.BOND_BAL														     AS BOND_BAL                 --���
                ,T4.RESERVESUM		         									 AS RESERVESUM               --������

    FROM				RWA_DEV.FNS_BND_INFO_B T1
		INNER JOIN	TEMP_BND_BOOK T3
		ON 					T1.BOND_ID = T3.BOND_ID
		LEFT JOIN		RWA.ORG_INFO T6
		ON					T1.DEPARTMENT = T6.ORGID
		LEFT JOIN   (SELECT BOND_ID
											 ,RESERVESUM
									 FROM RWA.RWA_WS_RESERVE
									WHERE DATADATE = (SELECT MAX(DATADATE) FROM RWA.RWA_WS_RESERVE WHERE DATADATE < TO_DATE(p_data_dt_str,'YYYYMMDD'))
								) T4																								--ȡ���һ�ڲ�¼�����̵�
    ON          T1.BOND_ID = T4.BOND_ID
    LEFT	JOIN  TMP_SUPPORG T8
    ON          T6.SORTNO LIKE T8.SORTNO || '%'
		WHERE 			T1.ASSET_CLASS IN ('50','60')												--ͨ���ʲ�������ȷ��ծȯ����Ӧ�տ�Ͷ�ʡ�
																																		--50 Ӧ�տ�����Ͷ��
																																		--60 Ӧ�տ�����Ͷ��-��Ʊ
		AND					T1.BOND_TYPE1 <> '060'
		AND					T1.BOND_ID NOT IN
								(SELECT ZQNM FROM RWA.RWA_WSIB_ABS_ISSUE_EXPOSURE WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
								 UNION ALL
								 SELECT ZQNM FROM RWA.RWA_WSIB_ABS_INVEST_EXPOSURE WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
								)
		AND 				T1.DATANO = p_data_dt_str														--ծȯ��Ϣ��,��ȡ��Ч��ծȯ��Ϣ
		AND					T1.BOND_CODE IS NOT NULL														--�ų���Ч��ծȯ����
		)
		WHERE RECORDNO = 1
		ORDER BY		BOND_ID,BOND_CODE
		;

    COMMIT;

    dbms_stats.gather_table_stats(ownname => 'RWA',tabname => 'RWA_WSIB_RESERVE',cascade => true);
    */

    --2.2 ����ϵͳ-ծȯͶ��ҵ��-���һ���
    INSERT INTO RWA.RWA_WSIB_BONDTRADE_MF(
                DATADATE                                --��������
                ,ORGID                             	 		--����ID
                ,BOND_ID                           	 		--�ڲ�����
                ,BOND_CODE                         	 		--Ͷ�ʽ���ͳһ����
                ,BOND_NAME                         	 		--Ͷ�ʽ�������
                ,BONDTYPE            								 		--ծȯ����
                ,BONDTYPE2           								 		--ծȯ����2
                ,BONDCURRENCY                      	 		--ծȯ����
                ,BONDBAL                           	 		--ծȯ���
                ,ISSUERID                          	 		--ծȯ������ID
                ,ISSUERNAME                        	 		--ծȯ����������
                ,ISSUERCATEGORY                    	 		--ծȯ�����˿ͻ�����
                ,BELONGORGCODE                     	 		--ҵ����������
                ,EFFECT_DATE                       	 		--��Ч��
                ,MATURITY_DATE       								 		--������
                ,GUARANTYTYPE                       	  --����������
                ,LETTERTYPE                         	  --�浥����
                ,LCISSUERTYPE        								 		--������Ʋ�Ʒ���л���
                ,BONDISSUEINTENT                   	 		--ծȯ����Ŀ��
                ,GUARANTORNAME                     	 		--�����˵Ŀͻ�����
                ,GUARANTORCATEGORY                    	--�����˵Ŀͻ�����
                ,GUARANTORCOUNTRYCODE								 		--�����˵�ע����Ҵ���
                ,GUARANTYCURRENCYCODE               	 	--��������
                ,GUARANTYSUM                        	 	--������ֵ��Ԫ��
    )
    WITH TEMP_BND_BOOK AS (
    						SELECT BOND_ID,
								       NVL(INITIAL_COST, 0) + NVL(MKT_VALUE_CHANGE, 0) + NVL(INT_ADJUST, 0) + NVL(ACCOUNTABLE_INT, 0) AS BOND_BAL
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
								   AND NVL(INITIAL_COST, 0) + NVL(MKT_VALUE_CHANGE, 0) + NVL(INT_ADJUST, 0) + NVL(ACCOUNTABLE_INT, 0) <> 0
		)
		, TMP_SUPPORG AS (
								SELECT T1.ORGID AS ORGID
										   ,CASE WHEN T3.ORGLEVEL > 2 THEN T4.SORTNO ELSE T3.SORTNO END AS SORTNO
								  FROM RWA.RWA_WP_SUPPTASKORG T1
						INNER JOIN RWA.RWA_WP_SUPPTASK T2
								    ON T1.SUPPTASKID = T2.SUPPTASKID
								   AND T2.ENABLEFLAG = '01'
						 LEFT JOIN RWA.ORG_INFO T3
								    ON T1.ORGID = T3.ORGID
						 LEFT JOIN RWA.ORG_INFO T4
	                	ON T3.BELONGORGID = T4.ORGID
								 WHERE T1.SUPPTMPLID = 'M-0071'
							ORDER BY T3.SORTNO
		)
		SELECT
								DATADATE                               	--��������
                ,ORGID                             	 		--����ID
                ,BOND_ID                           	 		--�ڲ�����
                ,BOND_CODE                         	 		--Ͷ�ʽ���ͳһ����
                ,BOND_NAME                         	 		--Ͷ�ʽ�������
                ,BONDTYPE            								 		--ծȯ����
                ,BONDTYPE2           								 		--ծȯ����2
                ,BONDCURRENCY                      	 		--ծȯ����
                ,BONDBAL                           	 		--ծȯ���
                ,ISSUERID                          	 		--ծȯ������ID
                ,ISSUERNAME                        	 		--ծȯ����������
                ,ISSUERCATEGORY                    	 		--ծȯ�����˿ͻ�����
                ,BELONGORGCODE                     	 		--ҵ����������
                ,EFFECT_DATE                       	 		--��Ч��
                ,MATURITY_DATE       								 		--������
                ,GUARANTYTYPE                       	  --����������
                ,LETTERTYPE                         	  --�浥����
                ,LCISSUERTYPE        								 		--������Ʋ�Ʒ���л���
                ,BONDISSUEINTENT                   	 		--ծȯ����Ŀ��
                ,GUARANTORNAME                     	 		--�����˵Ŀͻ�����
                ,GUARANTORCATEGORY                    	--�����˵Ŀͻ�����
                ,GUARANTORCOUNTRYCODE								 		--�����˵�ע����Ҵ���
                ,GUARANTYCURRENCYCODE               	 	--��������
                ,GUARANTYSUM                        	 	--������ֵ��Ԫ��
		FROM (
    SELECT
                TO_DATE(p_data_dt_str,'YYYYMMDD')            AS DATADATE                 --��������
                ,NVL(T8.ORGID,(SELECT ORGID FROM TMP_SUPPORG WHERE ROWNUM = 1))
                												                     AS ORGID										 --����ID                     		���ղ�¼����������������������г�������ֲ�
                ,RANK() OVER(PARTITION BY T1.BOND_ID ORDER BY LENGTH(NVL(T8.SORTNO,(SELECT SORTNO FROM TMP_SUPPORG WHERE ROWNUM = 1))) DESC)
                																						 AS RECORDNO								 --�������
                ,T1.BOND_ID                             		 AS BOND_ID               	 --�ڲ�����
                ,T1.BOND_CODE 										 					 AS BOND_CODE                --Ͷ�ʽ���ͳһ����
                ,T1.BOND_NAME                           		 AS BOND_NAME                --Ͷ�ʽ�������
                ,T1.BOND_TYPE1															 AS BONDTYPE            		 --ծȯ����
                ,T1.BOND_TYPE2															 AS BONDTYPE2                --ծȯ����2
                ,NVL(T1.CURRENCY_CODE,'CNY')                 AS BONDCURRENCY             --ծȯ����
                ,T3.BOND_BAL		 						                 AS BONDBAL                  --ծȯ���
                ,CASE WHEN T1.BOND_ID = 'B200801010095' THEN 'MZXXZ'																																														--ë��ϯ����Ĭ�ϲ�������
                			WHEN T9.BUSINESSTYPE = '1040102040' AND (T10.ISCOUNTTR = '1' OR T10.BONDNAME LIKE '%��ծ%') THEN 'ZGZYZF'																	--�����ծȯͶ�ʹ�ծʱĬ�Ϸ�����Ϊ�й���������
                			WHEN T9.BUSINESSTYPE = '1040202010' AND T10.BONDFLAG04 = '1' AND T10.MARKETSCATEGORY = '01' THEN T10.BONDPUBLISHCOUNTRY || 'ZYZF'					--���ծȯͶ�ʾ�����������
                			WHEN T9.BUSINESSTYPE = '1040202010' AND T10.BONDFLAG04 = '1' AND T10.MARKETSCATEGORY = '02' THEN T10.BONDPUBLISHCOUNTRY || 'ZYYH'					--���ծȯͶ�ʾ�����������
                			WHEN T9.BUSINESSTYPE = '1040202010' AND T10.BONDFLAG04 = '1' AND T10.MARKETSCATEGORY = '03' THEN T10.BONDPUBLISHCOUNTRY || 'BMST'					--���ծȯͶ�ʾ�����һ����ע��Ĺ�������ʵ��
                			WHEN REPLACE(T10.BONDPUBLISHID,'NCM_','') IS NULL THEN 'XN-YBGS'
                 ELSE T10.BONDPUBLISHID
                 END					     													 AS ISSUERID                 --ծȯ������ID
                ,CASE WHEN T1.BOND_ID = 'B200801010095' THEN 'ë��ϯ��'																																													--ë��ϯ����Ĭ�ϲ�������
                			WHEN T9.BUSINESSTYPE = '1040102040' AND (T10.ISCOUNTTR = '1' OR T10.BONDNAME LIKE '%��ծ%') THEN 'ZGZYZF'																	--�����ծȯͶ�ʹ�ծʱĬ�Ϸ�����Ϊ�й���������
                			WHEN T9.BUSINESSTYPE = '1040202010' AND T10.BONDFLAG04 = '1' AND T10.MARKETSCATEGORY = '01' THEN T10.BONDPUBLISHCOUNTRY || '��������'			--���ծȯͶ�ʾ�����������
                			WHEN T9.BUSINESSTYPE = '1040202010' AND T10.BONDFLAG04 = '1' AND T10.MARKETSCATEGORY = '02' THEN T10.BONDPUBLISHCOUNTRY || '��������'			--���ծȯͶ�ʾ�����������
                			WHEN T9.BUSINESSTYPE = '1040202010' AND T10.BONDFLAG04 = '1' AND T10.MARKETSCATEGORY = '03' THEN T10.BONDPUBLISHCOUNTRY || '��������ʵ��'	--���ծȯͶ�ʾ�����һ����ע��Ĺ�������ʵ��
                			WHEN REPLACE(T10.BONDPUBLISHID,'NCM_','') IS NULL THEN '����һ�㹫˾'
                 ELSE T11.CUSTOMERNAME
                 END														             AS ISSUERNAME               --ծȯ����������
                ,CASE WHEN T1.BOND_ID = 'B200801010095' THEN '0205'																																															--ë��ϯ����Ĭ�ϲ�������
                			WHEN T9.BUSINESSTYPE = '1040102040' AND (T10.ISCOUNTTR = '1' OR T10.BONDNAME LIKE '%��ծ%') THEN '0101'																		--�����ծȯͶ�ʹ�ծʱĬ�Ϸ�����Ϊ�й���������
                			WHEN T9.BUSINESSTYPE = '1040202010' AND T10.BONDFLAG04 = '1' AND T10.MARKETSCATEGORY = '01' THEN '0102'																		--���ծȯͶ�ʾ�����������
                			WHEN T9.BUSINESSTYPE = '1040202010' AND T10.BONDFLAG04 = '1' AND T10.MARKETSCATEGORY = '02' THEN '0104'																		--���ծȯͶ�ʾ�����������
                			WHEN T9.BUSINESSTYPE = '1040202010' AND T10.BONDFLAG04 = '1' AND T10.MARKETSCATEGORY = '03' THEN '0107'																		--���ծȯͶ�ʾ�����һ����ע��Ĺ�������ʵ��
                			WHEN REPLACE(T10.BONDPUBLISHID,'NCM_','') IS NULL THEN '0301'
                 ELSE T11.RWACUSTOMERTYPE
                 END													               AS ISSUERCATEGORY           --ծȯ�����˿ͻ�����
                ,T1.DEPARTMENT			              					 AS BELONGORGCODE            --ҵ����������
                ,TO_CHAR(TO_DATE(T1.EFFECT_DATE,'YYYYMMDD'),'YYYY-MM-DD')
                																						 AS EFFECT_DATE         		 --��Ч��
                ,TO_CHAR(TO_DATE(T1.MATURITY_DATE,'YYYYMMDD'),'YYYY-MM-DD')
                																			       AS MATURITY_DATE            --������
                ,T7.GUARANTYTYPE		              					 AS GUARANTYTYPE             --����������
                ,T7.LETTERTYPE															 AS LETTERTYPE          		 --�浥����
                ,T7.LCISSUERTYPE					 		 							 AS LCISSUERTYPE        	   --������Ʋ�Ʒ���л���
                ,NVL(T7.BONDISSUEINTENT,'02')  							 AS BONDISSUEINTENT          --ծȯ����Ŀ��
                ,T7.GUARANTORNAME                            AS GUARANTORNAME            --�����˵Ŀͻ�����
                ,T7.GUARANTORCATEGORY												 AS GUARANTORCATEGORY   		 --�����˵Ŀͻ�����
                ,NVL(T7.GUARANTORCOUNTRYCODE,'CHN')					 AS GUARANTORCOUNTRYCODE		 --�����˵�ע����Ҵ���
                ,NVL(T7.GUARANTYCURRENCYCODE,'CNY')					 AS GUARANTYCURRENCYCODE     --��������
                ,T7.GUARANTYSUM                              AS GUARANTYSUM              --������ֵ��Ԫ��


    FROM				RWA_DEV.FNS_BND_INFO_B T1
		INNER JOIN	TEMP_BND_BOOK T3
		ON 					T1.BOND_ID = T3.BOND_ID
		LEFT JOIN		RWA.ORG_INFO T6
		ON					T1.DEPARTMENT = T6.ORGID
		LEFT JOIN   (SELECT BOND_ID
											 ,GUARANTYTYPE
											 ,LETTERTYPE
											 ,LCISSUERTYPE
											 ,BONDISSUEINTENT
											 ,GUARANTORNAME
											 ,GUARANTORCATEGORY
											 ,GUARANTORCOUNTRYCODE
											 ,GUARANTYCURRENCYCODE
											 ,GUARANTYSUM
									 FROM RWA.RWA_WS_BONDTRADE_MF
									WHERE DATADATE = (SELECT MAX(DATADATE) FROM RWA.RWA_WS_BONDTRADE_MF WHERE DATADATE < TO_DATE(p_data_dt_str,'YYYYMMDD'))
								) T7																								--ȡ���һ�ڲ�¼�����̵�
    ON          T1.BOND_ID = T7.BOND_ID
    LEFT	JOIN  TMP_SUPPORG T8
    ON          T6.SORTNO LIKE T8.SORTNO || '%'
    LEFT JOIN		NCM_BUSINESS_DUEBILL T9
    ON					'CW_IMPORTDATA' || T1.BOND_ID = T9.THIRDPARTYACCOUNTS
    AND					T9.DATANO = p_data_dt_str
    LEFT JOIN		NCM_BOND_INFO T10
    ON					T10.OBJECTNO = T9.RELATIVESERIALNO2
    AND					T10.OBJECTTYPE = 'BusinessContract'
    AND					T10.DATANO = p_data_dt_str
    LEFT JOIN		NCM_CUSTOMER_INFO T11
    ON					DECODE(T10.BONDPUBLISHID,'NCM_',T9.CUSTOMERID,'',T9.CUSTOMERID,T10.BONDPUBLISHID) = T11.CUSTOMERID
    AND					T11.DATANO = p_data_dt_str
		WHERE 			(T1.ASSET_CLASS = '20' OR
								 (T1.ASSET_CLASS = '40' AND T1.BOND_TYPE2 NOT IN ('30','50')) OR 										  --T1.BOND_TYPE1 NOT IN ('091','099')
								 (T1.ASSET_CLASS = '40' AND T1.BOND_TYPE2 IN ('30','50') AND T1.CLOSED = '1')         --T1.BOND_TYPE1 IN ('091','099')
								)
		AND					T1.BOND_TYPE2 = '50'																--��ȡ���һ�������
		AND					T1.BOND_TYPE1 <> '060'
		AND					T1.BOND_ID NOT IN
								(SELECT ZQNM FROM RWA.RWA_WSIB_ABS_ISSUE_EXPOSURE WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
								 UNION ALL
								 SELECT ZQNM FROM RWA.RWA_WSIB_ABS_INVEST_EXPOSURE WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
								)
		AND 				T1.DATANO = p_data_dt_str														--ծȯ��Ϣ��,��ȡ��Ч��ծȯ��Ϣ
		)
		WHERE RECORDNO = 1
		ORDER BY		BOND_ID,BOND_CODE
		;

    COMMIT;

    dbms_stats.gather_table_stats(ownname => 'RWA',tabname => 'RWA_WSIB_BONDTRADE_MF',cascade => true);

    /*Ŀ�������ͳ��*/
    --ͳ�Ʋ���ļ�¼��
    SELECT COUNT(1) INTO v_count FROM RWA.RWA_WSIB_BONDTRADE_MF WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD');
    --Dbms_output.Put_line('RWA.RWA_WSIB_BONDTRADE_MF��ǰ����Ĳ���ϵͳ-ծȯͶ��-���һ����̵����ݼ�¼Ϊ: ' || v_count || ' ��');


    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̽��� ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    p_po_rtncode := '1';
	  p_po_rtnmsg  := '�ɹ�' || '-' || v_count;
		--�����쳣
		EXCEPTION
    WHEN OTHERS THEN
				 --Dbms_output.Put_line('������,�������Ϊ:'||sqlcode||';������ϢΪ:'||sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace);
       	 ROLLBACK;
   			 p_po_rtncode := sqlcode;
   			 p_po_rtnmsg  := 'Ͷ��ҵ��¼�����̵�('|| v_pro_name ||')����ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_TZ_WSIB;
/

