CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_HG_WSIB(
			 											p_data_dt_str	IN	VARCHAR2,		--�������� yyyyMMdd
       											p_po_rtncode	OUT	VARCHAR2,		--���ر�� 1 �ɹ�,0 ʧ��
														p_po_rtnmsg		OUT	VARCHAR2		--��������
				)
  /*
    �洢��������:RWA_DEV.PRO_RWA_HG_WSIB
    ʵ�ֹ���:����ϵͳ-�ع�-��¼�̵�(������Դ����ϵͳ��ҵ�������Ϣȫ������RWA�ع���¼�̵ױ���)
    ���ݿھ�:ȫ��
    ����Ƶ��:�³�����
    ��  ��  :V1.0.0
    ��д��  :LISY
    ��дʱ��:2016-06-06
    ��  λ  :�Ϻ���˶��Ϣ�����ɷ����޹�˾
    Դ  ��1 :RWA_DEV.CBS_BND|ծȯͶ�ʵǼǲ�
    Դ  ��2 :RWA_DEV.CBS_IAC|ͨ�÷ֻ���
    Դ  ��3 :RWA.ORG_INFO|������Ϣ��
    Դ  ��4 :RWA.RWA_WS_B_BILLREPURCHASE|���뷵��Ʊ�ݻع���¼��
    Դ  ��5 :RWA.RWA_WS_B_BONDREPURCHASE|���뷵��ծȯ�ع���¼��
    Դ  ��6 :RWA.RWA_WS_S_BONDREPURCHASE|�����ع�ծȯ�ع���¼��
    Դ  ��7 :RWA.RWA_WP_SUPPTASKORG|��¼��������ַ����ñ�
    Դ  ��8 :RWA.RWA_WP_SUPPTASK|��¼���񷢲���
    Դ  ��9 :RWA.RWA_WS_B_BILLREPURCHASE|���뷵��Ʊ�ݻع���¼��¼��
    Դ  ��10:RWA_DEV.BL_CUSTOMER_INFO|��¼�ͻ����ܱ�
    Ŀ���1 :RWA.RWA_WSIB_B_BILLREPURCHASE|���뷵��Ʊ�ݻع���¼�̵ױ�
    Ŀ���2 :RWA.RWA_WSIB_B_BONDREPURCHASE|���뷵��ծȯ�ع���¼�̵ױ�
    Ŀ���3 :RWA.RWA_WSIB_S_BONDREPURCHASE|�����ع�ծȯ�ع���¼�̵ױ�
    �����¼(�޸���|�޸�ʱ��|�޸�����):
    */
  AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_HG_WSIB';
  --�����쳣����
  v_raise EXCEPTION;
  --���嵱ǰ����ļ�¼��
  v_count INTEGER := 0;
  v_count1 INTEGER;
  v_count2 INTEGER;

  BEGIN
    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*�����ȫ�����ݼ��������Ŀ���*/
    --1.���Ŀ����е�ԭ�м�¼
    --������뷵��Ʊ�ݻع��̵ױ�
    DELETE FROM RWA.RWA_WSIB_B_BILLREPURCHASE WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD');
    --������뷵��ծȯ�ع��̵ױ�
    DELETE FROM RWA.RWA_WSIB_B_BONDREPURCHASE WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD');
    --��������ع�ծȯ�ع��̵ױ�
    DELETE FROM RWA.RWA_WSIB_S_BONDREPURCHASE WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD');

    COMMIT;

    --2.���������������ݴ�Դ����뵽Ŀ�����
    --2.1 ����ϵͳ-���뷴��Ʊ�ݻع�ҵ��(�̵����ڲ�¼����)
    INSERT INTO RWA.RWA_WSIB_B_BILLREPURCHASE(
                DATADATE                               --��������
                ,ORGID                             	 	 --����ID
                ,INVACCNO                          	 	 --�������κ�
                ,BELONGORGCODE                         --ҵ����������
                ,SUBJECTNO                       	 	 	 --��Ŀ
                ,BALANCE          	               	 	 --����ҵ�����
                ,INTEREST                          	 	 --Ӧ����Ϣ
                ,CURRENCY                          	 	 --���ױ���
                ,BEGINDATE        	               	 	 --���׿�ʼ����
                ,ENDDATE                           	 	 --���׵�������
                ,BILLNO                            	 	 --Ʊ�ݱ��
                ,CLIENTNAME       	               	 	 --���׶�������
                ,ORGANIZATIONCODE                  	 	 --���׶�����֯��������
                ,COUNTRYCODE      	               	 	 --���׶���ע����Ҵ���
                ,INDUSTRYID       	         			 	 	 --���׶���������ҵ����
                ,ISSUERNAME                        	 	 --�ж�������
                ,ISSUERORGCODE    	               	 	 --�ж�����֯��������
                ,ISSUERCOUNTRYCODE										 --�ж���ע����Ҵ���
                ,ISSUERINDUSTRYID                      --�ж���������ҵ����
                ,BILLBEGINDATE                         --Ʊ�ݷ�������
                ,BILLENDDATE                           --Ʊ�ݵ�������
                ,BILLCURRENCY                          --Ʊ�ݱ���
                ,BILLVALUE                             --Ʊ����
                ,CLIENTCATEGORY												 --���׶��ֿͻ�����
                ,ACCEPTCATEGORY												 --�ж��пͻ�����
    )
    SELECT
                TO_DATE(p_data_dt_str,'YYYYMMDD')            AS DATADATE                 --��������
                ,T1.SUPPORGID						                     AS ORGID                    --����ID
                ,T1.INVACCNO         						 						 AS INVACCNO                 --�������κ�
                ,T1.BELONGORGCODE            		 						 AS BELONGORGCODE            --ҵ����������
                ,T1.SUBJECTNO        					 	 						 AS SUBJECTNO                --��Ŀ
                ,T1.BALANCE               			 						 AS BALANCE          	       --����ҵ�����
                ,T1.INTEREST                 		 						 AS INTEREST                 --Ӧ����Ϣ
                ,T1.CURRENCY            				 						 AS CURRENCY                 --���ױ���
                ,T1.BEGINDATE                    						 AS BEGINDATE        	       --���׿�ʼ����
                ,T1.ENDDATE              				 						 AS ENDDATE                  --���׵�������
                ,T1.BILLNO                   		 						 AS BILLNO                   --Ʊ�ݱ��
                ,T1.CLIENTNAME                 	 						 AS CLIENTNAME       	       --���׶�������
                ,T1.ORGANIZATIONCODE         	 	 						 AS ORGANIZATIONCODE         --���׶�����֯��������
                ,T1.COUNTRYCODE                  						 AS COUNTRYCODE      	       --���׶���ע����Ҵ���
                ,T1.INDUSTRYID                   						 AS INDUSTRYID       	       --���׶���������ҵ����
                ,T1.ISSUERNAME       						 						 AS ISSUERNAME               --�ж�������
                ,T1.ISSUERORGCODE    						 						 AS ISSUERORGCODE    	       --�ж�����֯��������
                ,T1.ISSUERCOUNTRYCODE						 						 AS ISSUERCOUNTRYCODE				 --�ж���ע����Ҵ���
                ,T1.ISSUERINDUSTRYID             						 AS ISSUERINDUSTRYID         --�ж���������ҵ����
                ,T1.BILLBEGINDATE                						 AS BILLBEGINDATE            --Ʊ�ݷ�������
                ,T1.BILLENDDATE                  						 AS BILLENDDATE              --Ʊ�ݵ�������
                ,T1.BILLCURRENCY                 						 AS BILLCURRENCY             --Ʊ�ݱ���
                ,T1.BILLVALUE                    						 AS BILLVALUE                --Ʊ����
                ,T1.CLIENTCATEGORY													 AS CLIENTCATEGORY					 --���׶��ֿͻ�����
                ,T1.ACCEPTCATEGORY													 AS ACCEPTCATEGORY					 --�ж��пͻ�����

    FROM				RWA.RWA_WS_B_BILLREPURCHASE T1	             --���뷵��Ʊ�ݻع���¼��¼��ȡ���һ�ڲ�¼�����̵�
		WHERE				T1.DATADATE = (SELECT MAX(DATADATE) FROM RWA.RWA_WS_B_BILLREPURCHASE WHERE DATADATE < TO_DATE(p_data_dt_str,'YYYYMMDD'))
		ORDER BY		T1.INVACCNO, T1.BILLNO
		;

    COMMIT;

    dbms_stats.gather_table_stats(ownname => 'RWA',tabname => 'RWA_WSIB_B_BILLREPURCHASE',cascade => true);

    /*Ŀ�������ͳ��*/
    --ͳ�Ʋ���ļ�¼��
    --SELECT COUNT(1) INTO v_count FROM RWA.RWA_WSIB_B_BILLREPURCHASE WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD');
    --Dbms_output.Put_line('RWA.RWA_WSIB_B_BILLREPURCHASE��ǰ����ĺ���ϵͳ-���뷵��Ʊ�ݻع��̵����ݼ�¼Ϊ: ' || v_count || ' ��');


    --2.2 ����ϵͳ-���뷵��ծȯ�ع�ҵ��
    INSERT INTO RWA.RWA_WSIB_B_BONDREPURCHASE(
                DATADATE                               --��������
                ,ORGID                             	 	 --����ID
                ,INVACCNO						               	 	 --�����˺�
                ,BELONGORGCODE                         --ҵ����������
                ,CLRDATE															 --������ʼ��
                ,FDATE																 --���׵�����
                ,REPURCHASEVALUE			             	 	 --���׻ع����
                ,REPURCHASETYPE												 --�ع�����
                ,CLIENTNAME					               	 	 --���׶�������
                ,CLIENTCATEGORY												 --���׶��ֿͻ�����
                ,ORGANIZATIONCODE		               	 	 --���׶�����֯��������
                ,COUNTRYCODE					             	 	 --���׶��ֹ��Ҵ���
                ,INDUSTRYID					               	 	 --���׶�����ҵ����
                ,BONDCODE							             	 	 --ծȯ����
                ,BONDISSUEINTENT			             	 	 --ծȯ����Ŀ��
                ,ISSUERNAME					               	 	 --ծȯ����������
                ,ISSUERCATEGORY												 --ծȯ�����˿ͻ�����
                ,ISSUERORGCODE				             	 	 --ծȯ��������֯��������
                ,ISSUERCOUNTRYCODE		             	 	 --ծȯ������ע����Ҵ���
                ,ISSUERINDUSTRYID		               	 	 --ծȯ����������������ҵ
                ,ISSUERSCOPE					       			 	 	 --ծȯ��������ҵ��ģ
                ,ISSUERRATINGORGCODE	             	 	 --ծȯ��������������
                ,ISSUERRATING				  								 --ծȯ����������
                ,ISSUERRATINGDATE											 --ծȯ��������������
                ,ISSUERRATINGORGCODE2	             	 	 --ծȯ��������������2
                ,ISSUERRATING2				  							 --ծȯ����������2
                ,ISSUERRATINGDATE2										 --ծȯ��������������2
                ,BONDRATINGORGCODE		             	 	 --ծȯ��������
                ,BONDRATINGTYPE			               	 	 --ծȯ������������
                ,BONDRATING					               	 	 --ծȯ���еȼ�
                ,BONDRATINGDATE												 --ծȯ��������
                ,BONDRATINGORGCODE2		             	 	 --ծȯ��������2
                ,BONDRATINGTYPE2		               	 	 --ծȯ������������2
                ,BONDRATING2				               	 	 --ծȯ���еȼ�2
                ,BONDRATINGDATE2											 --ծȯ��������2
                ,BONDBEGINDATE												 --ծȯ��������
                ,BONDENDDATE													 --ծȯ��������
                ,BONDCURRENCY				               	 	 --ծȯ����
                ,BONDVALUE						             	 	 --ծȯ��ֵ
                ,BALANCE															 --ҵ�����

    )
    WITH TMP_SUPPORG AS (
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
								 WHERE T1.SUPPTMPLID = 'M-0064'
							ORDER BY T3.SORTNO
		)
		, TMP_BL_CUST AS (
								SELECT CUSTOMERNAME, CERTID
									FROM RWA_DEV.BL_CUSTOMER_INFO
								WHERE CERTTYPE = 'Ent01'
									AND ROWID IN (SELECT MAX(ROWID)
											            FROM RWA_DEV.BL_CUSTOMER_INFO
											           WHERE CERTTYPE = 'Ent01'
											           GROUP BY CUSTOMERNAME)
		)
		SELECT
								DATADATE                               --��������
                ,ORGID                             	 	 --����ID
                ,INVACCNO						               	 	 --�����˺�
                ,BELONGORGCODE                         --ҵ����������
                ,CLRDATE															 --������ʼ��
                ,FDATE																 --���׵�����
                ,REPURCHASEVALUE			             	 	 --���׻ع����
                ,REPURCHASETYPE												 --�ع�����
                ,CLIENTNAME					               	 	 --���׶�������
                ,CLIENTCATEGORY												 --���׶��ֿͻ�����
                ,ORGANIZATIONCODE		               	 	 --���׶�����֯��������
                ,COUNTRYCODE					             	 	 --���׶��ֹ��Ҵ���
                ,INDUSTRYID					               	 	 --���׶�����ҵ����
                ,BONDCODE							             	 	 --ծȯ����
                ,BONDISSUEINTENT			             	 	 --ծȯ����Ŀ��
                ,ISSUERNAME					               	 	 --ծȯ����������
                ,ISSUERCATEGORY												 --ծȯ�����˿ͻ�����
                ,ISSUERORGCODE				             	 	 --ծȯ��������֯��������
                ,ISSUERCOUNTRYCODE		             	 	 --ծȯ������ע����Ҵ���
                ,ISSUERINDUSTRYID		               	 	 --ծȯ����������������ҵ
                ,ISSUERSCOPE					       			 	 	 --ծȯ��������ҵ��ģ
                ,ISSUERRATINGORGCODE	             	 	 --ծȯ��������������
                ,ISSUERRATING				  								 --ծȯ����������
                ,ISSUERRATINGDATE											 --ծȯ��������������
                ,ISSUERRATINGORGCODE2	             	 	 --ծȯ��������������2
                ,ISSUERRATING2				  							 --ծȯ����������2
                ,ISSUERRATINGDATE2										 --ծȯ��������������2
                ,BONDRATINGORGCODE		             	 	 --ծȯ��������
                ,BONDRATINGTYPE			               	 	 --ծȯ������������
                ,BONDRATING					               	 	 --ծȯ���еȼ�
                ,BONDRATINGDATE												 --ծȯ��������
                ,BONDRATINGORGCODE2		             	 	 --ծȯ��������2
                ,BONDRATINGTYPE2		               	 	 --ծȯ������������2
                ,BONDRATING2				               	 	 --ծȯ���еȼ�2
                ,BONDRATINGDATE2											 --ծȯ��������2
                ,BONDBEGINDATE												 --ծȯ��������
                ,BONDENDDATE													 --ծȯ��������
                ,BONDCURRENCY				               	 	 --ծȯ����
                ,BONDVALUE						             	 	 --ծȯ��ֵ
                ,BALANCE															 --ҵ�����
		FROM (
    SELECT
                TO_DATE(p_data_dt_str,'YYYYMMDD')            AS DATADATE                 --��������
                ,NVL(T7.ORGID,(SELECT ORGID FROM TMP_SUPPORG WHERE ROWNUM = 1))
                												                     AS ORGID										 --����ID                     		���ղ�¼����������������������г�������ֲ�
                ,RANK() OVER(PARTITION BY T1.INVACCNO ORDER BY LENGTH(NVL(T7.SORTNO,(SELECT SORTNO FROM TMP_SUPPORG WHERE ROWNUM = 1))) DESC)
                																						 AS RECORDNO								 --�������
                ,T1.INVACCNO                             		 AS INVACCNO								 --�����˺�
                ,T2.IACGACBR																 AS BELONGORGCODE            --ҵ����������
                ,TO_CHAR(TO_DATE(T1.CLRDATE,'YYYYMMDD'),'YYYY-MM-DD')
                																						 AS CLRDATE									 --������ʼ��
                ,TO_CHAR(TO_DATE(T1.FDATE,'YYYYMMDD'),'YYYY-MM-DD')
                																						 AS FDATE										 --���׵�����
                ,ABS(T2.IACCURBAL)	             				 		 AS REPURCHASEVALUE		    	 --���׻ع����
                ,CASE WHEN T2.IACITMNO = '11110301' THEN '01'														 --��Ѻʽ
                			WHEN T2.IACITMNO = '11110302' THEN '02'														 --���ʽ
                			ELSE NVL(T4.REPURCHASETYPE,'')
                 END																				 AS REPURCHASETYPE					 --�ع�����
                ,NVL(T4.CLIENTNAME,T1.BNDNAME)             	 AS CLIENTNAME							 --���׶�������
                ,NVL(T4.CLIENTCATEGORY,'0202')             	 AS CLIENTCATEGORY					 --���׶��ֿͻ�����								Ĭ��0202-�й���ҵ����
                ,NVL(T4.ORGANIZATIONCODE,T8.CERTID)      		 AS ORGANIZATIONCODE		  	 --���׶�����֯��������
                ,NVL(T4.COUNTRYCODE,'CHN')                   AS COUNTRYCODE			         --���׶��ֹ��Ҵ���    						Ĭ��CHN-�й�
                ,NVL(T4.INDUSTRYID,'J66')            				 AS INDUSTRYID				       --���׶�����ҵ����    						Ĭ��J66-���ҽ��ڷ���
                ,T4.BONDCODE                          			 AS BONDCODE						     --ծȯ����
                ,T4.BONDISSUEINTENT                          AS BONDISSUEINTENT		    	 --ծȯ����Ŀ��
                ,T4.ISSUERNAME                           		 AS ISSUERNAME					  	 --ծȯ����������
                ,NVL(T4.ISSUERCATEGORY,'0202')             	 AS ISSUERCATEGORY					 --ծȯ�����˿ͻ�����							Ĭ��0202-�й���ҵ����
                ,T4.ISSUERORGCODE    												 AS ISSUERORGCODE			    	 --ծȯ��������֯��������
                ,NVL(T4.ISSUERCOUNTRYCODE,'CHN')             AS ISSUERCOUNTRYCODE	    	 --ծȯ������ע����Ҵ���					Ĭ��CHN-�й�
                ,NVL(T4.ISSUERINDUSTRYID,'J66')              AS ISSUERINDUSTRYID		  	 --ծȯ����������������ҵ					Ĭ��J66-���ҽ��ڷ���
                ,T4.ISSUERSCOPE															 AS ISSUERSCOPE				  		 --ծȯ��������ҵ��ģ
                ,T4.ISSUERRATINGORGCODE											 AS ISSUERRATINGORGCODE 		 --ծȯ��������������
                ,T4.ISSUERRATING														 AS ISSUERRATING						 --ծȯ����������
                ,T4.ISSUERRATINGDATE												 AS ISSUERRATINGDATE				 --ծȯ��������������
                ,T4.ISSUERRATINGORGCODE2										 AS ISSUERRATINGORGCODE2 		 --ծȯ��������������2
                ,T4.ISSUERRATING2														 AS ISSUERRATING2						 --ծȯ����������2
                ,T4.ISSUERRATINGDATE2												 AS ISSUERRATINGDATE2				 --ծȯ��������������2
                ,T4.BONDRATINGORGCODE                        AS BONDRATINGORGCODE	    	 --ծȯ��������
                ,T4.BONDRATINGTYPE                           AS BONDRATINGTYPE			  	 --ծȯ������������
                ,T4.BONDRATING															 AS BONDRATING							 --ծȯ���еȼ�
                ,T4.BONDRATINGDATE													 AS BONDRATINGDATE					 --ծȯ��������
                ,T4.BONDRATINGORGCODE2                       AS BONDRATINGORGCODE2    	 --ծȯ��������2
                ,T4.BONDRATINGTYPE2                          AS BONDRATINGTYPE2			  	 --ծȯ������������2
                ,T4.BONDRATING2															 AS BONDRATING2							 --ծȯ���еȼ�2
                ,T4.BONDRATINGDATE2													 AS BONDRATINGDATE2					 --ծȯ��������2
                ,T4.BONDBEGINDATE                            AS BONDBEGINDATE			    	 --ծȯ��������
                ,T4.BONDENDDATE                              AS BONDENDDATE				    	 --ծȯ��������
                ,NVL(T4.BONDCURRENCY,'CNY')									 AS BONDCURRENCY				  	 --ծȯ����              					Ĭ��CNY-�����
                ,T4.BONDVALUE																 AS BONDVALUE					    	 --ծȯ��ֵ
                ,ABS(T2.IACCURBAL)													 AS BALANCE									 --ҵ�����

    FROM				RWA_DEV.CBS_BND T1	             		 											--ծȯͶ�ʵǼǲ�,��ȡ��Ч��ծȯ��Ϣ
		INNER JOIN 	RWA_DEV.CBS_IAC T2																				--ͨ�÷ֻ���
		ON 					T1.INVACCNO = T2.IACAC_NO
		AND					T2.IACCURBAL <> 0																					--������0
		AND					T2.IACITMNO LIKE '111103%'																--���뷵��(��ع�)ծȯ
		AND					T2.DATANO = p_data_dt_str
		LEFT	JOIN	RWA.ORG_INFO T3																						--������Ϣ��
	  ON					T2.IACGACBR = T3.ORGID
	  LEFT	JOIN	(SELECT INVACCNO
	  									 ,REPURCHASETYPE
	  									 ,CLIENTNAME
	  									 ,CLIENTCATEGORY
	  									 ,ORGANIZATIONCODE
	  									 ,COUNTRYCODE
	  									 ,INDUSTRYID
	  									 ,BONDCODE
	  									 ,BONDISSUEINTENT
	  									 ,ISSUERNAME
	  									 ,ISSUERCATEGORY
	  									 ,ISSUERORGCODE
	  									 ,ISSUERCOUNTRYCODE
	  									 ,ISSUERINDUSTRYID
	  									 ,ISSUERSCOPE
	  									 ,ISSUERRATINGORGCODE
	  									 ,ISSUERRATING
	  									 ,ISSUERRATINGDATE
	  									 ,ISSUERRATINGORGCODE2
	  									 ,ISSUERRATING2
	  									 ,ISSUERRATINGDATE2
	  									 ,BONDRATINGORGCODE
	  									 ,BONDRATINGTYPE
	  									 ,BONDRATING
	  									 ,BONDRATINGDATE
	  									 ,BONDRATINGORGCODE2
	  									 ,BONDRATINGTYPE2
	  									 ,BONDRATING2
	  									 ,BONDRATINGDATE2
	  									 ,BONDBEGINDATE
	  									 ,BONDENDDATE
	  									 ,BONDCURRENCY
	  									 ,BONDVALUE
	  							 FROM RWA.RWA_WS_B_BONDREPURCHASE
	  							WHERE DATADATE = (SELECT MAX(DATADATE) FROM RWA.RWA_WS_B_BONDREPURCHASE WHERE DATADATE < TO_DATE(p_data_dt_str,'YYYYMMDD'))
	  						) T4																											--ȡ���һ�ڲ�¼�����̵�
	  ON					T1.INVACCNO = T4.INVACCNO
	  LEFT	JOIN  TMP_SUPPORG T7
    ON          T3.SORTNO LIKE T7.SORTNO || '%'
    LEFT	JOIN	TMP_BL_CUST T8
    ON					T1.BNDNAME = T8.CUSTOMERNAME
		WHERE				T1.STATUS = '00'																					--�˻�״̬���� 00-������11-������黹
		AND 				T1.TYPE IN ('2','3')																			--ծȯ���ͣ�2-���ع���3-��ع�(����)
		AND					T1.DATANO = p_data_dt_str
		ORDER BY		T1.INVACCNO
		)
		WHERE RECORDNO = 1
		ORDER BY		INVACCNO,BONDCODE
		;

    COMMIT;

    dbms_stats.gather_table_stats(ownname => 'RWA',tabname => 'RWA_WSIB_B_BONDREPURCHASE',cascade => true);

    /*Ŀ�������ͳ��*/
    --ͳ�Ʋ���ļ�¼��
    SELECT COUNT(1) INTO v_count1 FROM RWA.RWA_WSIB_B_BONDREPURCHASE WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD');
    --Dbms_output.Put_line('RWA.RWA_WSIB_B_BONDREPURCHASE��ǰ����ĺ���ϵͳ-���뷵��ծȯ�ع��̵����ݼ�¼Ϊ: ' || v_count || ' ��');


    --2.3 ����ϵͳ-�����ع�ծȯ�ع�ҵ��
    INSERT INTO RWA.RWA_WSIB_S_BONDREPURCHASE(
                DATADATE                               --��������
                ,ORGID                             	 	 --����ID
                ,INVACCNO				                 	 	 	 --�����˺�
                ,CLRDATE															 --������ʼ��
                ,FDATE																 --���׵�����
                ,BELONGORGCODE                         --ҵ����������
                ,BONDCODE						             	 	 	 --ծȯ����
                ,REPURCHASETYPE	  	             	 	 	 --�ع�����
                ,REPURCHASEVALUE	               	 	 	 --���׻ع����
                ,CLIENTNAME			                 	 	 	 --���׶�������
                ,ORGANIZATIONCODE 	             	 	 	 --���׶�����֯��������
                ,COUNTRYCODE			               	 	 	 --���׶��ֹ��Ҵ���
                ,INDUSTRYID			  	             	 	 	 --���׶�����ҵ����
                ,BONDCURRENCY				               	 	 --ծȯ����
                ,BONDVALUE						             	 	 --ծȯ��ֵ
                ,CLIENTCATEGORY			             	 	 	 --���׶��ֿͻ�����
    )
    WITH TMP_SUPPORG AS (
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
								 WHERE T1.SUPPTMPLID = 'M-0065'
							ORDER BY T3.SORTNO
		)
		, TMP_BL_CUST AS (
								SELECT CUSTOMERNAME, CERTID
									FROM RWA_DEV.BL_CUSTOMER_INFO
								WHERE CERTTYPE = 'Ent02'
									AND ROWID IN (SELECT MAX(ROWID)
											            FROM RWA_DEV.BL_CUSTOMER_INFO
											           WHERE CERTTYPE = 'Ent02'
											           GROUP BY CUSTOMERNAME)
		)
		SELECT
								DATADATE                               --��������
                ,ORGID                             	 	 --����ID
                ,INVACCNO				                 	 	 	 --�����˺�
                ,CLRDATE															 --������ʼ��
                ,FDATE																 --���׵�����
                ,BELONGORGCODE                         --ҵ����������
                ,BONDCODE						             	 	 	 --ծȯ����
                ,REPURCHASETYPE	  	             	 	 	 --�ع�����
                ,REPURCHASEVALUE	               	 	 	 --���׻ع����
                ,CLIENTNAME			                 	 	 	 --���׶�������
                ,ORGANIZATIONCODE 	             	 	 	 --���׶�����֯��������
                ,COUNTRYCODE			               	 	 	 --���׶��ֹ��Ҵ���
                ,INDUSTRYID			  	             	 	 	 --���׶�����ҵ����
                ,BONDCURRENCY				               	 	 --ծȯ����
                ,BONDVALUE						             	 	 --ծȯ��ֵ
                ,CLIENTCATEGORY			             	 	 	 --���׶��ֿͻ�����
		FROM (
    SELECT
                TO_DATE(p_data_dt_str,'YYYYMMDD')            AS DATADATE                 --��������
                ,NVL(T7.ORGID,(SELECT ORGID FROM TMP_SUPPORG WHERE ROWNUM = 1))
                												                     AS ORGID										 --����ID                     		���ղ�¼����������������������г�������ֲ�
                ,RANK() OVER(PARTITION BY T1.INVACCNO ORDER BY LENGTH(NVL(T7.SORTNO,(SELECT SORTNO FROM TMP_SUPPORG WHERE ROWNUM = 1))) DESC)
                																						 AS RECORDNO								 --�������
                ,T1.INVACCNO                             		 AS INVACCNO				  		 	 --�����˺�
                ,TO_CHAR(TO_DATE(T1.CLRDATE,'YYYYMMDD'),'YYYY-MM-DD')
                																						 AS CLRDATE									 --������ʼ��
                ,TO_CHAR(TO_DATE(T1.FDATE,'YYYYMMDD'),'YYYY-MM-DD')
                																						 AS FDATE										 --���׵�����
                ,T2.IACGACBR																 AS BELONGORGCODE            --ҵ����������
                ,T4.BONDCODE                           			 AS BONDCODE						     --ծȯ����
                ,T4.REPURCHASETYPE               				 		 AS REPURCHASETYPE	    	 	 --�ع�����
                ,T4.REPURCHASEVALUE                        	 AS REPURCHASEVALUE	  		 	 --���׻ع����
                ,NVL(T4.CLIENTNAME,T1.BNDNAME)             	 AS CLIENTNAME							 --���׶�������
                ,NVL(T4.ORGANIZATIONCODE,T8.CERTID)      		 AS ORGANIZATIONCODE		  	 --���׶�����֯��������
                ,NVL(T4.COUNTRYCODE,'CHN')               		 AS COUNTRYCODE			    	 	 --���׶��ֹ��Ҵ���    						Ĭ��CHN-�й�
                ,NVL(T4.INDUSTRYID,'J66')                    AS INDUSTRYID			    	 	 --���׶�����ҵ����    						Ĭ��J66-���ҽ��ڷ���
                ,NVL(T4.BONDCURRENCY,'CNY')									 AS BONDCURRENCY				     --ծȯ����												Ĭ��CNY-�����
                ,T4.BONDVALUE																 AS BONDVALUE						     --ծȯ��ֵ
                ,NVL(T4.CLIENTCATEGORY,'0202')   						 AS CLIENTCATEGORY	    	 	 --���׶��ֿͻ�����         			Ĭ��0202-�й���ҵ����

    FROM				RWA_DEV.CBS_BND T1	             		 											--ծȯͶ�ʵǼǲ�,��ȡ��Ч��ծȯ��Ϣ
		INNER JOIN 	RWA_DEV.CBS_IAC T2																				--ͨ�÷ֻ���
		ON 					T1.INVACCNO = T2.IACAC_NO
		AND					T2.IACCURBAL <> 0																					--������0
		AND					T2.IACITMNO LIKE '211103%'																--�����ع�(���ع�)ծȯ
		AND					T2.DATANO = p_data_dt_str
		LEFT	JOIN	RWA.ORG_INFO T3																						--������Ϣ��
	  ON					T2.IACGACBR = T3.ORGID
	  LEFT	JOIN	(SELECT INVACCNO
	  									 ,BONDCODE
	  									 ,REPURCHASETYPE
	  									 ,REPURCHASEVALUE
	  									 ,CLIENTNAME
	  									 ,ORGANIZATIONCODE
	  									 ,COUNTRYCODE
	  									 ,INDUSTRYID
	  									 ,BONDCURRENCY
	  									 ,BONDVALUE
	  									 ,CLIENTCATEGORY
	  							 FROM RWA.RWA_WS_S_BONDREPURCHASE
	  							WHERE DATADATE = (SELECT MAX(DATADATE) FROM RWA.RWA_WS_S_BONDREPURCHASE WHERE DATADATE < TO_DATE(p_data_dt_str,'YYYYMMDD'))
	  						) T4																											--ȡ���һ�ڲ�¼�����̵�
	  ON					T1.INVACCNO = T4.INVACCNO
	  LEFT	JOIN  TMP_SUPPORG T7
    ON          T3.SORTNO LIKE T7.SORTNO || '%'
    LEFT	JOIN	TMP_BL_CUST T8
    ON					T1.BNDNAME = T8.CUSTOMERNAME
		WHERE				T1.STATUS = '00'																					--�˻�״̬���� 00-������11-������黹
		AND 				T1.TYPE IN ('2','3')																			--ծȯ���ͣ�2-���ع���3-��ع�(����)
		AND					T1.DATANO = p_data_dt_str
		)
		WHERE RECORDNO = 1
		ORDER BY		INVACCNO,BONDCODE
		;

    COMMIT;

    dbms_stats.gather_table_stats(ownname => 'RWA',tabname => 'RWA_WSIB_S_BONDREPURCHASE',cascade => true);

    /*Ŀ�������ͳ��*/
    --ͳ�Ʋ���ļ�¼��
    SELECT COUNT(1) INTO v_count2 FROM RWA.RWA_WSIB_S_BONDREPURCHASE WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD');
    --Dbms_output.Put_line('RWA.RWA_WSIB_S_BONDREPURCHASE��ǰ����ĺ���ϵͳ-�����ع�ծȯ�ع��̵����ݼ�¼Ϊ: ' || v_count || ' ��');



    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̽��� ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    p_po_rtncode := '1';
	  p_po_rtnmsg  := '�ɹ�' || '-' || (v_count + v_count1 + v_count2);
		--�����쳣
		EXCEPTION
    WHEN OTHERS THEN
				 --Dbms_output.Put_line('������,�������Ϊ:'||sqlcode||';������ϢΪ:'||sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace);
       	 ROLLBACK;
   			 p_po_rtncode := sqlcode;
   			 p_po_rtnmsg  := '�ع�ҵ��¼�����̵�('|| v_pro_name ||')����ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_HG_WSIB;
/

