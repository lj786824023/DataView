CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_TZ_GUARANTEE(
			 											p_data_dt_str	IN	VARCHAR2,		--��������
       											p_po_rtncode	OUT	VARCHAR2,		--���ر�� 1 �ɹ�,0 ʧ��
														p_po_rtnmsg		OUT	VARCHAR2		--��������
				)
  /*
    �洢��������:RWA_DEV.PRO_RWA_TZ_GUARANTEE
    ʵ�ֹ���:����ϵͳ-Ͷ��-��֤(������Դ����ϵͳ��Ӧ�տ�Ͷ�������Ϣȫ������RWAͶ����ӿڱ�֤����)
    ���ݿھ�:ȫ��
    ����Ƶ��:�³�
    ��  ��  :V1.0.0
    ��д��  :YUSJ
    ��дʱ��:2015-05-26
    ��  λ	:�Ϻ���˶��Ϣ�����ɷ����޹�˾
    Դ  ��1 :RWA_DEV.FNS_BND_INFO_B|����ϵͳծȯ��Ϣ��
    Դ  ��2 :RWA_DEV.FNS_BND_BOOK_B|����ϵͳ������
    Դ  ��3 :RWA.RWA_WS_RECEIVABLE|Ӧ�տ�Ͷ�ʲ�¼��
    Դ  ��4 :RWA.RWA_WP_DATASUPPLEMENT|��¼������ϸ��Ϣ��
    Դ  ��5 :RWA_DEV.CBS_BND|ծȯͶ�ʵǼǲ�
    Դ  ��6 :RWA_DEV.CBS_IAC|ͨ�÷ֻ���
    Դ  ��7 :RWA.RWA_WS_B_RECEIVABLE|���뷵�����������ʲ�_Ӧ���˿�Ͷ�ʲ�¼��
    Դ  ��8 :RWA.RWA_WSIB_ABS_ISSUE_EXPOSURE|���л����ʲ�֤ȯ����¶�̵ױ�
    Դ  ��9 :RWA.RWA_WSIB_ABS_INVEST_EXPOSURE|Ͷ�ʻ����ʲ�֤ȯ����¶�̵ױ�
    Ŀ���	 :RWA_DEV.RWA_TZ_GUARANTEE|����ϵͳͶ���ౣ֤��
    �����¼(�޸���|�޸�ʱ��|�޸�����):
  */
  AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_TZ_GUARANTEE';
  --�����쳣����
  v_raise EXCEPTION;
  --�����ж�ֵ����
  v_count INTEGER;

  BEGIN
    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*�����ȫ�����ݼ��������Ŀ���*/
    --1.���Ŀ����е�ԭ�м�¼
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_TZ_GUARANTEE';

    --2.���������������ݴ�Դ����뵽Ŀ�����
    --2.1 ����ϵͳ-Ӧ�տ�Ͷ��-��֤-�ǻ�������-��ͬ
    INSERT INTO RWA_DEV.RWA_TZ_GUARANTEE(
         				 DataDate          												  --��������
								,DataNo                                     --������ˮ��
								,GuaranteeID                                --��֤ID
								,SSysID                                     --ԴϵͳID
								,GuaranteeConID                             --��֤��ͬID
								,GuarantorID                                --��֤��ID
								,CreditRiskDataType                         --���÷�����������
								,GuaranteeWay                            		--������ʽ
								,QualFlagSTD                            		--Ȩ�ط��ϸ��ʶ
								,QualFlagFIRB                               --�����������ϸ��ʶ
								,GuaranteeTypeSTD                           --Ȩ�ط���֤����
								,GuarantorSdvsSTD                           --Ȩ�ط���֤��ϸ��
								,GuaranteeTypeIRB                           --��������֤����
								,GuaranteeAmount                            --��֤�ܶ�
								,Currency                                   --����
								,StartDate                                  --��ʼ����
								,DueDate                                    --��������
								,OriginalMaturity                           --ԭʼ����
								,ResidualM                                  --ʣ������
								,GuarantorIRating                           --��֤���ڲ�����
								,GuarantorPD                                --��֤��ΥԼ����
								,GroupID                                    --������
    )
    WITH TEMP_BND_GUARANTEE AS (
												  SELECT T5.SERIALNO
																,MIN(T1.STARTDATE)							AS STARTDATE
																,MAX(T1.DUEDATE)								AS DUEDATE
																,MIN(T4.QUALIFYFLAG) 						AS QUALIFYFLAG
													  FROM RWA_DEV.RWA_TZ_CONTRACT T1	 															--Ͷ�ʺ�ͬ��
											INNER JOIN (SELECT DISTINCT
																				 SERIALNO
																				,OBJECTNO
																				,QUALIFYFLAG
																		FROM RWA_DEV.NCM_CONTRACT_RELATIVE
																	 WHERE OBJECTTYPE = 'GuarantyContract'
																		 AND DATANO = p_data_dt_str) T4                       --�Ŵ���ͬ������
									    				ON T1.SCONTRACTID = T4.SERIALNO
									    INNER JOIN RWA_DEV.NCM_GUARANTY_CONTRACT T5													--�Ŵ�������ͬ��
									    				ON T4.OBJECTNO = T5.SERIALNO
									    			 AND T5.GUARANTYTYPE = '010'																	--��֤
									    			 AND T5.DATANO = p_data_dt_str
									    INNER JOIN RWA_DEV.NCM_BUSINESS_CONTRACT T8													--�Ŵ���ͬ��
									    				ON T1.SCONTRACTID = T8.SERIALNO
									    			 AND (T8.BUSINESSSUBTYPE NOT LIKE '0010%' OR T8.BUSINESSSUBTYPE IS NULL) 			--�ǻ�������
									    			 AND T8.DATANO = p_data_dt_str
													 WHERE T1.BUSINESSTYPEID IN ('1040105060','1040105061','1040105062')	--Ӧ�տ�Ͷ��ҵ��
												GROUP BY T5.SERIALNO
		)
		,TEMP_CUST_INFO AS (
    								SELECT CUSTOMERID, CUSTOMERNAME, CERTTYPE, CERTID
										  FROM (SELECT CUSTOMERID,
										               CUSTOMERNAME,
										               CERTTYPE,
										               CERTID,
										               ROW_NUMBER() OVER(PARTITION BY CERTTYPE, CERTID ORDER BY CUSTOMERID DESC) AS RN
										          FROM RWA_DEV.NCM_CUSTOMER_INFO
										         WHERE DATANO = P_DATA_DT_STR)
										 WHERE RN = 1
    )
    SELECT
         				 TO_DATE(p_data_dt_str,'YYYYMMDD')								AS  DATADATE          	 --��������
         				,p_data_dt_str														 		    AS	DATANO               --������ˮ��
         				,'BZ' || T1.SERIALNO															AS	GUARANTEEID          --��֤ID
								,'TZ'																						  AS	SSYSID               --ԴϵͳID
								,T1.SERIALNO																			AS	GUARANTEECONID       --��֤��ͬID
								--,DECODE(T1.GUARANTORID,'NCM_','XN-YBGS',T1.GUARANTORID)
								,CASE WHEN REPLACE(T1.GUARANTORID,'NCM_','') IS NULL AND T4.CUSTOMERID IS NULL THEN
													 CASE WHEN T1.CERTTYPE LIKE 'Ind%' THEN 'XN-GRKH'
													 			ELSE 'XN-YBGS'
													 END
											WHEN REPLACE(T1.GUARANTORID,'NCM_','') IS NULL THEN T4.CUSTOMERID
											ELSE T1.GUARANTORID
								 END																							AS	GUARANTORID          --��֤��ID              					�����֤��Ϊ����Ĭ�ϱ�֤��Ϊһ�㹫˾������������0�ĵ�����ͬ���е�����
								,'01'																							AS	CREDITRISKDATATYPE   --���÷�����������      					Ĭ�ϣ�һ�������(01)
								,T1.GUARANTYTYPE																	AS	GUARANTEEWAY       	 --������ʽ
								,''																								AS	QUALFLAGSTD          --Ȩ�ط��ϸ��ʶ
								,CASE WHEN T2.QUALIFYFLAG	= '01' THEN '1'
											WHEN T2.QUALIFYFLAG	= '02' THEN '0'
								 			ELSE ''
								 END																							AS	QUALFLAGFIRB         --�����������ϸ��ʶ
								,''																								AS	GUARANTEETYPESTD     --Ȩ�ط���֤����
								,''																								AS	GUARANTORSDVSSTD     --Ȩ�ط���֤��ϸ��
								,CASE WHEN T2.QUALIFYFLAG = '01' THEN '020201'
								 ELSE ''
								 END																							AS	GUARANTEETYPEIRB     --��������֤����
								,T1.GUARANTYVALUE																	AS	GUARANTEEAMOUNT      --��֤�ܶ�
								,NVL(T1.GUARANTYCURRENCY,'CNY')										AS	CURRENCY             --����
								,T2.STARTDATE																		  AS	STARTDATE            --��ʼ����
								,T2.DUEDATE																				AS	DUEDATE              --��������
								,CASE WHEN (TO_DATE(T2.DUEDATE,'YYYYMMDD')-TO_DATE(T2.STARTDATE,'YYYYMMDD'))/365<0
								      THEN 0
								      ELSE (TO_DATE(T2.DUEDATE,'YYYYMMDD')-TO_DATE(T2.STARTDATE,'YYYYMMDD'))/365
								END																								AS  ORIGINALMATURITY   	 --ԭʼ����
								,CASE WHEN (TO_DATE(T2.DUEDATE,'YYYYMMDD')-TO_DATE(p_data_dt_str,'YYYYMMDD'))/365<0
								      THEN 0
								      ELSE (TO_DATE(T2.DUEDATE,'YYYYMMDD')-TO_DATE(p_data_dt_str,'YYYYMMDD'))/365
								END																								AS	RESIDUALM            --ʣ������
								,T3.PDADJLEVEL																		AS	GUARANTORIRATING     --��֤���ڲ�����
								,T3.PD																						AS	GUARANTORPD          --��֤��ΥԼ����
								,''																								AS	GROUPID              --������
    FROM				RWA_DEV.NCM_GUARANTY_CONTRACT T1 									--������ͬ��Ϣ��
		INNER JOIN  TEMP_BND_GUARANTEE T2				                      --Ӧ�տ�Ͷ�ʱ�֤��ʱ��
    ON          T1.SERIALNO = T2.SERIALNO
    LEFT JOIN 	TEMP_CUST_INFO T4
    ON					T1.CERTTYPE = T4.CERTTYPE
    AND					T1.CERTID = T4.CERTID
    LEFT JOIN		RWA_DEV.RWA_TEMP_PDLEVEL T3
    ON					DECODE(T1.GUARANTORID,'NCM_',T4.CUSTOMERID,NULL,T4.CUSTOMERID,'',T4.CUSTOMERID,T1.GUARANTORID) = T3.CUSTID
		WHERE 			T1.GUARANTYTYPE = '010'														--��֤
		AND					T1.GUARANTYVALUE > 0
		AND 				T1.DATANO = p_data_dt_str
		;

    COMMIT;

    --2.2 ����ϵͳ-Ӧ�տ�Ͷ��-��֤-�ǻ�������-����
    INSERT INTO RWA_DEV.RWA_TZ_GUARANTEE(
         				 DataDate          												  --��������
								,DataNo                                     --������ˮ��
								,GuaranteeID                                --��֤ID
								,SSysID                                     --ԴϵͳID
								,GuaranteeConID                             --��֤��ͬID
								,GuarantorID                                --��֤��ID
								,CreditRiskDataType                         --���÷�����������
								,GuaranteeWay                            		--������ʽ
								,QualFlagSTD                            		--Ȩ�ط��ϸ��ʶ
								,QualFlagFIRB                               --�����������ϸ��ʶ
								,GuaranteeTypeSTD                           --Ȩ�ط���֤����
								,GuarantorSdvsSTD                           --Ȩ�ط���֤��ϸ��
								,GuaranteeTypeIRB                           --��������֤����
								,GuaranteeAmount                            --��֤�ܶ�
								,Currency                                   --����
								,StartDate                                  --��ʼ����
								,DueDate                                    --��������
								,OriginalMaturity                           --ԭʼ����
								,ResidualM                                  --ʣ������
								,GuarantorIRating                           --��֤���ڲ�����
								,GuarantorPD                                --��֤��ΥԼ����
								,GroupID                                    --������
    )
    WITH TEMP_BND_GUARANTEE AS (
												  SELECT T5.SERIALNO
																,MIN(T1.STARTDATE)		AS STARTDATE
																,MAX(T1.DUEDATE)			AS DUEDATE
													  FROM RWA_DEV.RWA_TZ_CONTRACT T1	 															--Ͷ�ʺ�ͬ��
											INNER JOIN RWA_DEV.NCM_BUSINESS_PUTOUT T3														--���˱�
															ON T1.SCONTRACTID = T3.CONTRACTSERIALNO
														 AND T3.DATANO = p_data_dt_str
											INNER JOIN RWA_DEV.NCM_GUARANTY_RELATIVE T4													--ѺƷ������
															ON T3.SERIALNO = T4.OBJECTNO
														 AND T4.OBJECTTYPE = 'PutOutApply'
														 AND T4.DATANO = p_data_dt_str
									    INNER JOIN RWA_DEV.NCM_GUARANTY_CONTRACT T5													--�Ŵ�������ͬ��
									    				ON T4.OBJECTNO = T5.SERIALNO
									    			 AND T5.GUARANTYTYPE = '010'																	--��֤
									    			 AND T5.DATANO = p_data_dt_str
									    INNER JOIN RWA_DEV.NCM_BUSINESS_CONTRACT T8													--�Ŵ���ͬ��
									    				ON T1.SCONTRACTID = T8.SERIALNO
									    			 AND (T8.BUSINESSSUBTYPE NOT LIKE '0010%' OR T8.BUSINESSSUBTYPE IS NULL) 			--�ǻ�������
									    			 AND T8.DATANO = p_data_dt_str
													 WHERE T1.BUSINESSTYPEID IN ('1040105060','1040105061','1040105062')	--Ӧ�տ�Ͷ��ҵ��
												GROUP BY T5.SERIALNO
		)
		,TEMP_CUST_INFO AS (
    								SELECT CUSTOMERID, CUSTOMERNAME, CERTTYPE, CERTID
										  FROM (SELECT CUSTOMERID,
										               CUSTOMERNAME,
										               CERTTYPE,
										               CERTID,
										               ROW_NUMBER() OVER(PARTITION BY CERTTYPE, CERTID ORDER BY CUSTOMERID DESC) AS RN
										          FROM RWA_DEV.NCM_CUSTOMER_INFO
										         WHERE DATANO = P_DATA_DT_STR)
										 WHERE RN = 1
    )
    SELECT
         				 TO_DATE(p_data_dt_str,'YYYYMMDD')								AS  DATADATE          	 --��������
         				,p_data_dt_str														 		    AS	DATANO               --������ˮ��
         				,'BZ' || T1.SERIALNO															AS	GUARANTEEID          --��֤ID
								,'TZ'																						  AS	SSYSID               --ԴϵͳID
								,T1.SERIALNO																			AS	GUARANTEECONID       --��֤��ͬID
								--,DECODE(T1.GUARANTORID,'NCM_','XN-YBGS',T1.GUARANTORID)
								,CASE WHEN REPLACE(T1.GUARANTORID,'NCM_','') IS NULL AND T4.CUSTOMERID IS NULL THEN
													 CASE WHEN T1.CERTTYPE LIKE 'Ind%' THEN 'XN-GRKH'
													 			ELSE 'XN-YBGS'
													 END
											WHEN REPLACE(T1.GUARANTORID,'NCM_','') IS NULL THEN T4.CUSTOMERID
											ELSE T1.GUARANTORID
								 END																							AS	GUARANTORID          --��֤��ID              					�����֤��Ϊ����Ĭ�ϱ�֤��Ϊһ�㹫˾������������0�ĵ�����ͬ���е�����
								,'01'																							AS	CREDITRISKDATATYPE   --���÷�����������      					Ĭ�ϣ�һ�������(01)
								,T1.GUARANTYTYPE																	AS	GUARANTEEWAY       	 --������ʽ
								,''																								AS	QUALFLAGSTD          --Ȩ�ط��ϸ��ʶ
								,''																								AS	QUALFLAGFIRB         --�����������ϸ��ʶ
								,''																								AS	GUARANTEETYPESTD     --Ȩ�ط���֤����
								,''																								AS	GUARANTORSDVSSTD     --Ȩ�ط���֤��ϸ��
								,''																								AS	GUARANTEETYPEIRB     --��������֤����
								,T1.GUARANTYVALUE																	AS	GUARANTEEAMOUNT      --��֤�ܶ�
								,NVL(T1.GUARANTYCURRENCY,'CNY')										AS	CURRENCY             --����
								,T2.STARTDATE																		  AS	STARTDATE            --��ʼ����
								,T2.DUEDATE																				AS	DUEDATE              --��������
								,CASE WHEN (TO_DATE(T2.DUEDATE,'YYYYMMDD')-TO_DATE(T2.STARTDATE,'YYYYMMDD'))/365<0
								      THEN 0
								      ELSE (TO_DATE(T2.DUEDATE,'YYYYMMDD')-TO_DATE(T2.STARTDATE,'YYYYMMDD'))/365
								END																								AS  ORIGINALMATURITY   	 --ԭʼ����
								,CASE WHEN (TO_DATE(T2.DUEDATE,'YYYYMMDD')-TO_DATE(p_data_dt_str,'YYYYMMDD'))/365<0
								      THEN 0
								      ELSE (TO_DATE(T2.DUEDATE,'YYYYMMDD')-TO_DATE(p_data_dt_str,'YYYYMMDD'))/365
								END																								AS	RESIDUALM            --ʣ������
								,T3.PDADJLEVEL																		AS	GUARANTORIRATING     --��֤���ڲ�����
								,T3.PD																						AS	GUARANTORPD          --��֤��ΥԼ����
								,''																								AS	GROUPID              --������
    FROM				RWA_DEV.NCM_GUARANTY_CONTRACT T1 									--������ͬ��Ϣ��
		INNER JOIN  TEMP_BND_GUARANTEE T2				                      --Ӧ�տ�Ͷ�ʱ�֤��ʱ��
    ON          T1.SERIALNO = T2.SERIALNO
    LEFT JOIN 	TEMP_CUST_INFO T4
    ON					T1.CERTTYPE = T4.CERTTYPE
    AND					T1.CERTID = T4.CERTID
    LEFT JOIN		RWA_DEV.RWA_TEMP_PDLEVEL T3
    ON					DECODE(T1.GUARANTORID,'NCM_',T4.CUSTOMERID,NULL,T4.CUSTOMERID,'',T4.CUSTOMERID,T1.GUARANTORID) = T3.CUSTID
		WHERE 			T1.GUARANTYTYPE = '010'														--��֤
		AND					T1.GUARANTYVALUE > 0
		AND 				T1.DATANO = p_data_dt_str
		AND NOT EXISTS (SELECT 1 FROM RWA_DEV.RWA_TZ_GUARANTEE TG WHERE 'BZ' || T1.SERIALNO = TG.GUARANTEEID)
		;

    COMMIT;

    --2.3 ����ϵͳ-Ӧ�տ�Ͷ��-����֤����������֤�������Ա������������Ա���-�ǻ�������-��ͬ
    INSERT INTO RWA_DEV.RWA_TZ_GUARANTEE(
         				 DataDate          												  --��������
								,DataNo                                     --������ˮ��
								,GuaranteeID                                --��֤ID
								,SSysID                                     --ԴϵͳID
								,GuaranteeConID                             --��֤��ͬID
								,GuarantorID                                --��֤��ID
								,CreditRiskDataType                         --���÷�����������
								,GuaranteeWay                            		--������ʽ
								,QualFlagSTD                            		--Ȩ�ط��ϸ��ʶ
								,QualFlagFIRB                               --�����������ϸ��ʶ
								,GuaranteeTypeSTD                           --Ȩ�ط���֤����
								,GuarantorSdvsSTD                           --Ȩ�ط���֤��ϸ��
								,GuaranteeTypeIRB                           --��������֤����
								,GuaranteeAmount                            --��֤�ܶ�
								,Currency                                   --����
								,StartDate                                  --��ʼ����
								,DueDate                                    --��������
								,OriginalMaturity                           --ԭʼ����
								,ResidualM                                  --ʣ������
								,GuarantorIRating                           --��֤���ڲ�����
								,GuarantorPD                                --��֤��ΥԼ����
								,GroupID                                    --������
    )
    WITH TMP_BND_CONTRACT AS (
							SELECT
										 T1.SCONTRACTID 			AS CONTRACTNO
										,MIN(T1.STARTDATE)		AS STARTDATE
										,MAX(T1.DUEDATE)			AS DUEDATE
								FROM RWA_DEV.RWA_TZ_CONTRACT T1	 																--Ͷ�ʺ�ͬ��
					INNER JOIN RWA_DEV.NCM_BUSINESS_CONTRACT TC														--�Ŵ���ͬ��
									ON T1.SCONTRACTID = TC.SERIALNO
								 AND (TC.BUSINESSSUBTYPE NOT LIKE '0010%' OR TC.BUSINESSSUBTYPE IS NULL) 			--�ǻ�������
								 AND TC.DATANO = p_data_dt_str
	    				 WHERE T1.BUSINESSTYPEID IN ('1040105060','1040105061','1040105062')	--Ӧ�տ�Ͷ��ҵ��
	    			GROUP BY T1.SCONTRACTID
		)
		, TEMP_BND_COLLATERAL AS (
													SELECT T5.GUARANTYID
																,MIN(T1.STARTDATE)		AS STARTDATE
																,MAX(T1.DUEDATE)			AS DUEDATE
													  FROM TMP_BND_CONTRACT T1
									    INNER JOIN (SELECT DISTINCT
																				 SERIALNO
																				,OBJECTNO
																		FROM RWA_DEV.NCM_CONTRACT_RELATIVE
						   										 WHERE OBJECTTYPE = 'GuarantyContract'
						     										 AND DATANO = p_data_dt_str) T2
						     							ON T1.CONTRACTNO = T2.SERIALNO
											INNER JOIN RWA_DEV.NCM_GUARANTY_CONTRACT T3													--�Ŵ�������ͬ��
						    							ON T2.OBJECTNO = T3.SERIALNO
						    						 AND T3.DATANO = p_data_dt_str
						    			INNER JOIN (SELECT DISTINCT
						    												 CONTRACTNO
						    												,GUARANTYID
						    										FROM RWA_DEV.NCM_GUARANTY_RELATIVE
						    									 WHERE DATANO = p_data_dt_str
						    									) T4																										--�Ŵ�������ͬ�����ѺƷ������
						    							ON T3.SERIALNO = T4.CONTRACTNO
						    			INNER JOIN RWA_DEV.NCM_GUARANTY_INFO T5															--�Ŵ�����ѺƷ��Ϣ��
						    							ON T4.GUARANTYID = T5.GUARANTYID
						    						 AND T5.GUARANTYTYPEID IN ('004001004001','004001005001','004001006001','004001006002')     					--����֤����������֤�������Ա������������Ա�������Ϊ��֤
						    						 AND T5.CLRSTATUS = '01'																			--ѺƷʵ��״̬������
    						 						 AND T5.CLRGNTSTATUS IN ('03','10')														--ѺƷ��Ѻ״̬��03-��ȷ��ѺȨ��10-�����
						    						 AND T5.DATANO = p_data_dt_str
												GROUP BY T5.GUARANTYID
		)
		, TEMP_CUST_INFO AS (
    								SELECT CUSTOMERID, CERTTYPE, CERTID
                      FROM (SELECT T1.CUSTOMERID,
                                   T1.CERTTYPE,
                                   T1.CERTID,
                                   ROW_NUMBER() OVER(PARTITION BY T1.CERTTYPE, T1.CERTID ORDER BY T1.CUSTOMERID) AS RM
                              FROM RWA_DEV.NCM_CUSTOMER_INFO T1
                             WHERE EXISTS
                             (SELECT 1
                                      FROM RWA_DEV.NCM_GUARANTY_INFO T2
                                     WHERE T1.CERTID = T2.OBLIGEEIDNUMBER
                                       AND T2.DATANO = p_data_dt_str
                                       AND T2.GUARANTYTYPEID IN
                                           ('004001004001',
                                            '004001005001',
                                            '004001006001',
                                            '004001006002',
                                            '001001003001')
                                       AND T2.AFFIRMVALUE0 > 0)
                               AND T1.DATANO = p_data_dt_str)
                     WHERE RM = 1
    )
    , TMP_CUST_IRATING AS (
   								SELECT CUSTID,
									       CUSTNAME,
									       ORGCERTCODE,
									       MODELID,
									       PDLEVEL,
									       PDADJLEVEL,
									       PD,
									       PDVAVLIDDATE
									  FROM RWA_DEV.RWA_TEMP_PDLEVEL
									 WHERE ROWID IN
									       (SELECT MAX(ROWID) FROM RWA_DEV.RWA_TEMP_PDLEVEL GROUP BY ORGCERTCODE)
   	)
    SELECT
         				 TO_DATE(p_data_dt_str,'YYYYMMDD')								AS  DATADATE          	 --��������
         				,p_data_dt_str														 		    AS	DATANO               --������ˮ��
         				,'YP' || T1.GUARANTYID														AS	GUARANTEEID          --��֤ID
								,'TZ'																						  AS	SSYSID               --ԴϵͳID
								,T1.GUARANTYID																		AS	GUARANTEECONID       --��֤��ͬID
								,NVL(T6.CUSTOMERID,'XN-YBGS')											AS	GUARANTORID          --��֤��ID
								,'01'																							AS	CREDITRISKDATATYPE   --���÷�����������      					Ĭ�� һ�������(01)
								,T1.GUARANTYTYPEID																AS	GUARANTEEWAY       	 --������ʽ              					Ĭ�� ��֤(010)
								,''																								AS	QUALFLAGSTD          --Ȩ�ط��ϸ��ʶ
								,CASE WHEN T1.QUALIFYFLAG03	= '01' THEN '1'
											WHEN T1.QUALIFYFLAG03	= '02' THEN '0'
								 ELSE ''
								 END																							AS	QUALFLAGFIRB         --�����������ϸ��ʶ
								,''																								AS	GUARANTEETYPESTD     --Ȩ�ط���֤����
								,''																								AS	GUARANTORSDVSSTD     --Ȩ�ط���֤��ϸ��
								,CASE WHEN T1.QUALIFYFLAG03 = '01' THEN '020201'
								 ELSE ''
								 END																							AS	GUARANTEETYPEIRB     --��������֤����
								,T1.AFFIRMVALUE0																	AS	GUARANTEEAMOUNT      --��֤�ܶ�
								,NVL(T1.AFFIRMCURRENCY,'CNY')											AS	CURRENCY             --����
								,T2.STARTDATE																		  AS	STARTDATE            --��ʼ����
								,T2.DUEDATE																				AS	DUEDATE              --��������
								,CASE WHEN (TO_DATE(T2.DUEDATE,'YYYYMMDD')-TO_DATE(T2.STARTDATE,'YYYYMMDD'))/365<0
								      THEN 0
								      ELSE (TO_DATE(T2.DUEDATE,'YYYYMMDD')-TO_DATE(T2.STARTDATE,'YYYYMMDD'))/365
								END																								AS  ORIGINALMATURITY   	 --ԭʼ����
								,CASE WHEN (TO_DATE(T2.DUEDATE,'YYYYMMDD')-TO_DATE(p_data_dt_str,'YYYYMMDD'))/365<0
								      THEN 0
								      ELSE (TO_DATE(T2.DUEDATE,'YYYYMMDD')-TO_DATE(p_data_dt_str,'YYYYMMDD'))/365
								END																								AS	RESIDUALM            --ʣ������
								,T8.PDADJLEVEL																		AS	GUARANTORIRATING     --��֤���ڲ�����
								,T8.PD																						AS	GUARANTORPD          --��֤��ΥԼ����
								,''																								AS	GROUPID              --������
    FROM				RWA_DEV.NCM_GUARANTY_INFO T1															--�Ŵ�����ѺƷ��Ϣ��
    INNER JOIN	TEMP_BND_COLLATERAL	T2																		--Ӧ�տ�Ͷ�ʵ���ѺƷ��ʱ��
   	ON					T1.GUARANTYID = T2.GUARANTYID
   	LEFT JOIN		TEMP_CUST_INFO T6
    ON					T1.OBLIGEEIDTYPE = T6.CERTTYPE
    AND 				T1.OBLIGEEIDNUMBER = T6.CERTID
    LEFT JOIN		TMP_CUST_IRATING T8																--�ͻ��ڲ�������ʱ��
    ON					REPLACE(T1.OBLIGEEIDNUMBER,'-','') = REPLACE(T8.ORGCERTCODE,'-','')
    AND					T1.OBLIGEEIDTYPE IN ('Ent01','Ent02')
		WHERE 			T1.GUARANTYTYPEID IN ('004001004001','004001005001','004001006001','004001006002')
																																					--����֤����������֤�������Ա������������Ա�������Ϊ��֤
		AND					T1.AFFIRMVALUE0 > 0
		AND 				T1.DATANO = p_data_dt_str
		;

    COMMIT;

    --2.4 ����ϵͳ-Ӧ�տ�Ͷ��-����֤����������֤�������Ա������������Ա���-�ǻ�������-����
    INSERT INTO RWA_DEV.RWA_TZ_GUARANTEE(
         				 DataDate          												  --��������
								,DataNo                                     --������ˮ��
								,GuaranteeID                                --��֤ID
								,SSysID                                     --ԴϵͳID
								,GuaranteeConID                             --��֤��ͬID
								,GuarantorID                                --��֤��ID
								,CreditRiskDataType                         --���÷�����������
								,GuaranteeWay                            		--������ʽ
								,QualFlagSTD                            		--Ȩ�ط��ϸ��ʶ
								,QualFlagFIRB                               --�����������ϸ��ʶ
								,GuaranteeTypeSTD                           --Ȩ�ط���֤����
								,GuarantorSdvsSTD                           --Ȩ�ط���֤��ϸ��
								,GuaranteeTypeIRB                           --��������֤����
								,GuaranteeAmount                            --��֤�ܶ�
								,Currency                                   --����
								,StartDate                                  --��ʼ����
								,DueDate                                    --��������
								,OriginalMaturity                           --ԭʼ����
								,ResidualM                                  --ʣ������
								,GuarantorIRating                           --��֤���ڲ�����
								,GuarantorPD                                --��֤��ΥԼ����
								,GroupID                                    --������
    )
    WITH TMP_BND_CONTRACT AS (
							SELECT
										 T1.SCONTRACTID 			AS CONTRACTNO
										,MIN(T1.STARTDATE)		AS STARTDATE
										,MAX(T1.DUEDATE)			AS DUEDATE
								FROM RWA_DEV.RWA_TZ_CONTRACT T1	 																--Ͷ�ʺ�ͬ��
					INNER JOIN RWA_DEV.NCM_BUSINESS_CONTRACT TC														--�Ŵ���ͬ��
									ON T1.SCONTRACTID = TC.SERIALNO
								 AND (TC.BUSINESSSUBTYPE NOT LIKE '0010%' OR TC.BUSINESSSUBTYPE IS NULL) 			--�ǻ�������
								 AND TC.DATANO = p_data_dt_str
	    				 WHERE T1.BUSINESSTYPEID IN ('1040105060','1040105061','1040105062')	--Ӧ�տ�Ͷ��ҵ��
	    			GROUP BY T1.SCONTRACTID
		)
		, TEMP_BND_COLLATERAL AS (
													SELECT T5.GUARANTYID
																,MIN(T1.STARTDATE)		AS STARTDATE
																,MAX(T1.DUEDATE)			AS DUEDATE
													  FROM TMP_BND_CONTRACT T1
									    INNER JOIN RWA_DEV.NCM_BUSINESS_PUTOUT T2									--���˱�
															ON T1.CONTRACTNO = T2.CONTRACTSERIALNO
												 		 AND T2.DATANO = p_data_dt_str
											INNER JOIN RWA_DEV.NCM_GUARANTY_RELATIVE T3								--ѺƷ������
															ON T2.SERIALNO = T3.OBJECTNO
												 		 AND T3.OBJECTTYPE = 'PutOutApply'
												 		 AND T3.DATANO = p_data_dt_str
						    			INNER JOIN RWA_DEV.NCM_GUARANTY_INFO T5															--�Ŵ�����ѺƷ��Ϣ��
						    							ON T3.GUARANTYID = T5.GUARANTYID
						    						 AND T5.GUARANTYTYPEID IN ('004001004001','004001005001','004001006001','004001006002')     					--����֤����������֤�������Ա������������Ա�������Ϊ��֤
						    						 AND T5.CLRSTATUS = '01'																			--ѺƷʵ��״̬������
    						 						 AND T5.CLRGNTSTATUS IN ('03','10')														--ѺƷ��Ѻ״̬��03-��ȷ��ѺȨ��10-�����
						    						 AND T5.DATANO = p_data_dt_str
												GROUP BY T5.GUARANTYID
		)
		, TEMP_CUST_INFO AS (
    								SELECT CUSTOMERID, CERTTYPE, CERTID
                      FROM (SELECT T1.CUSTOMERID,
                                   T1.CERTTYPE,
                                   T1.CERTID,
                                   ROW_NUMBER() OVER(PARTITION BY T1.CERTTYPE, T1.CERTID ORDER BY T1.CUSTOMERID) AS RM
                              FROM RWA_DEV.NCM_CUSTOMER_INFO T1
                             WHERE EXISTS
                             (SELECT 1
                                      FROM RWA_DEV.NCM_GUARANTY_INFO T2
                                     WHERE T1.CERTID = T2.OBLIGEEIDNUMBER
                                       AND T2.DATANO = p_data_dt_str
                                       AND T2.GUARANTYTYPEID IN
                                           ('004001004001',
                                            '004001005001',
                                            '004001006001',
                                            '004001006002',
                                            '001001003001')
                                       AND T2.AFFIRMVALUE0 > 0)
                               AND T1.DATANO = p_data_dt_str)
                     WHERE RM = 1
    )
    , TMP_CUST_IRATING AS (
   								SELECT CUSTID,
									       CUSTNAME,
									       ORGCERTCODE,
									       MODELID,
									       PDLEVEL,
									       PDADJLEVEL,
									       PD,
									       PDVAVLIDDATE
									  FROM RWA_DEV.RWA_TEMP_PDLEVEL
									 WHERE ROWID IN
									       (SELECT MAX(ROWID) FROM RWA_DEV.RWA_TEMP_PDLEVEL GROUP BY ORGCERTCODE)
   	)
    SELECT
         				 TO_DATE(p_data_dt_str,'YYYYMMDD')								AS  DATADATE          	 --��������
         				,p_data_dt_str														 		    AS	DATANO               --������ˮ��
         				,'YP' || T1.GUARANTYID														AS	GUARANTEEID          --��֤ID
								,'TZ'																						  AS	SSYSID               --ԴϵͳID
								,T1.GUARANTYID																		AS	GUARANTEECONID       --��֤��ͬID
								,NVL(T6.CUSTOMERID,'XN-YBGS')											AS	GUARANTORID          --��֤��ID
								,'01'																							AS	CREDITRISKDATATYPE   --���÷�����������      					Ĭ�� һ�������(01)
								,T1.GUARANTYTYPEID																AS	GUARANTEEWAY       	 --������ʽ              					Ĭ�� ��֤(010)
								,''																								AS	QUALFLAGSTD          --Ȩ�ط��ϸ��ʶ
								,CASE WHEN T1.QUALIFYFLAG03	= '01' THEN '1'
											WHEN T1.QUALIFYFLAG03	= '02' THEN '0'
								 			ELSE ''
								 END																							AS	QUALFLAGFIRB         --�����������ϸ��ʶ
								,''																								AS	GUARANTEETYPESTD     --Ȩ�ط���֤����
								,''																								AS	GUARANTORSDVSSTD     --Ȩ�ط���֤��ϸ��
								,CASE WHEN T1.QUALIFYFLAG03 = '01' THEN '020201'
								 ELSE ''
								 END																							AS	GUARANTEETYPEIRB     --��������֤����
								,T1.AFFIRMVALUE0																	AS	GUARANTEEAMOUNT      --��֤�ܶ�
								,NVL(T1.AFFIRMCURRENCY,'CNY')											AS	CURRENCY             --����
								,T2.STARTDATE																		  AS	STARTDATE            --��ʼ����
								,T2.DUEDATE																				AS	DUEDATE              --��������
								,CASE WHEN (TO_DATE(T2.DUEDATE,'YYYYMMDD')-TO_DATE(T2.STARTDATE,'YYYYMMDD'))/365<0
								      THEN 0
								      ELSE (TO_DATE(T2.DUEDATE,'YYYYMMDD')-TO_DATE(T2.STARTDATE,'YYYYMMDD'))/365
								END																								AS  ORIGINALMATURITY   	 --ԭʼ����
								,CASE WHEN (TO_DATE(T2.DUEDATE,'YYYYMMDD')-TO_DATE(p_data_dt_str,'YYYYMMDD'))/365<0
								      THEN 0
								      ELSE (TO_DATE(T2.DUEDATE,'YYYYMMDD')-TO_DATE(p_data_dt_str,'YYYYMMDD'))/365
								END																								AS	RESIDUALM            --ʣ������
								,T8.PDADJLEVEL																		AS	GUARANTORIRATING     --��֤���ڲ�����
								,T8.PD																						AS	GUARANTORPD          --��֤��ΥԼ����
								,''																								AS	GROUPID              --������
    FROM				RWA_DEV.NCM_GUARANTY_INFO T1															--�Ŵ�����ѺƷ��Ϣ��
    INNER JOIN	TEMP_BND_COLLATERAL	T2																		--Ӧ�տ�Ͷ�ʵ���ѺƷ��ʱ��
   	ON					T1.GUARANTYID = T2.GUARANTYID
   	LEFT JOIN		TEMP_CUST_INFO T6
    ON					T1.OBLIGEEIDTYPE = T6.CERTTYPE
    AND 				T1.OBLIGEEIDNUMBER = T6.CERTID
    LEFT JOIN		TMP_CUST_IRATING T8																--�ͻ��ڲ�������ʱ��
    ON					REPLACE(T1.OBLIGEEIDNUMBER,'-','') = REPLACE(T8.ORGCERTCODE,'-','')
    AND					T1.OBLIGEEIDTYPE IN ('Ent01','Ent02')
		WHERE 			T1.GUARANTYTYPEID IN ('004001004001','004001005001','004001006001','004001006002')
																																					--����֤����������֤�������Ա������������Ա�������Ϊ��֤
		AND					T1.AFFIRMVALUE0 > 0
		AND 				T1.DATANO = p_data_dt_str
		AND NOT EXISTS (SELECT 1 FROM RWA_DEV.RWA_TZ_GUARANTEE TG WHERE 'YP' || T1.GUARANTYID = TG.GUARANTEEID)
		;

    COMMIT;

    --2.5 ����ϵͳ-ծȯͶ��-���-��֤
    INSERT INTO RWA_DEV.RWA_TZ_GUARANTEE(
         				 DataDate          												  --��������
								,DataNo                                     --������ˮ��
								,GuaranteeID                                --��֤ID
								,SSysID                                     --ԴϵͳID
								,GuaranteeConID                             --��֤��ͬID
								,GuarantorID                                --��֤��ID
								,CreditRiskDataType                         --���÷�����������
								,GuaranteeWay                            		--������ʽ
								,QualFlagSTD                            		--Ȩ�ط��ϸ��ʶ
								,QualFlagFIRB                               --�����������ϸ��ʶ
								,GuaranteeTypeSTD                           --Ȩ�ط���֤����
								,GuarantorSdvsSTD                           --Ȩ�ط���֤��ϸ��
								,GuaranteeTypeIRB                           --��������֤����
								,GuaranteeAmount                            --��֤�ܶ�
								,Currency                                   --����
								,StartDate                                  --��ʼ����
								,DueDate                                    --��������
								,OriginalMaturity                           --ԭʼ����
								,ResidualM                                  --ʣ������
								,GuarantorIRating                           --��֤���ڲ�����
								,GuarantorPD                                --��֤��ΥԼ����
								,GroupID                                    --������
    )
    SELECT
         				 TO_DATE(p_data_dt_str,'YYYYMMDD')								AS  DATADATE          	 --��������
         				,p_data_dt_str														 		    AS	DATANO               --������ˮ��
         				,'TZBOND' || T5.SERIALNO													AS	GUARANTEEID          --��֤ID
								,'TZ'																						  AS	SSYSID               --ԴϵͳID
								,T5.SERIALNO																			AS	GUARANTEECONID       --��֤��ͬID
								,T5.THIRDPARTYID1																	AS	GUARANTORID          --��֤��ID
								,'01'																							AS	CREDITRISKDATATYPE   --���÷�����������      					Ĭ�� һ�������(01)
								,'010'																						AS	GUARANTEEWAY       	 --������ʽ              					Ĭ�� ��֤(010)
								,''																								AS	QUALFLAGSTD          --Ȩ�ط��ϸ��ʶ
								,''																								AS	QUALFLAGFIRB         --�����������ϸ��ʶ
								,''																								AS	GUARANTEETYPESTD     --Ȩ�ط���֤����
								,''																								AS	GUARANTORSDVSSTD     --Ȩ�ط���֤��ϸ��
								,''																								AS	GUARANTEETYPEIRB     --��������֤����
								,T1.CONTRACTAMOUNT																AS	GUARANTEEAMOUNT      --��֤�ܶ�
								,T1.SETTLEMENTCURRENCY														AS	CURRENCY             --����
								,T1.STARTDATE																		  AS	STARTDATE            --��ʼ����
								,T1.DUEDATE																				AS	DUEDATE              --��������
								,T1.ORIGINALMATURITY															AS  ORIGINALMATURITY   	 --ԭʼ����
								,T1.RESIDUALM																			AS	RESIDUALM            --ʣ������
								,T6.PDADJLEVEL																		AS	GUARANTORIRATING     --��֤���ڲ�����
								,T6.PD																						AS	GUARANTORPD          --��֤��ΥԼ����
								,''																								AS	GROUPID              --������
    FROM				RWA_DEV.RWA_TZ_CONTRACT T1	 															--Ͷ�ʺ�ͬ��
		INNER JOIN	RWA_DEV.NCM_BOND_INFO T4																	--�Ŵ�ծȯ��Ϣ��
	  ON					T1.SCONTRACTID = T4.OBJECTNO
		AND					T4.OBJECTTYPE = 'BusinessContract'
		AND					T4.DATANO = p_data_dt_str
		INNER JOIN	(SELECT	 SERIALNO
												,THIRDPARTYID1
										FROM RWA_DEV.NCM_BUSINESS_CONTRACT										--�Ŵ���ͬ��
									 WHERE BUSINESSTYPE = '1040202010' 											--���ծȯͶ��
									 	 AND VOUCHTYPE2 = '1'																	--�е�������Ϣ
									 	 AND DATANO = p_data_dt_str) T5
		ON					T1.SCONTRACTID = T5.SERIALNO
		LEFT JOIN		RWA_DEV.RWA_TEMP_PDLEVEL T6
		ON					T5.THIRDPARTYID1 = T6.CUSTID
		LEFT JOIN		RWA_DEV.NCM_CUSTOMER_INFO T7
    ON					T5.THIRDPARTYID1 = T7.CUSTOMERID
    AND					T7.DATANO = p_data_dt_str
	  WHERE 			T1.BUSINESSTYPEID IN ('1040202010','1040202011')					--���ծȯͶ��ҵ��
		AND T1.DATANO=p_data_dt_str
    ;

    COMMIT;

    --2.6 ����ϵͳ-ծȯͶ��-���һ���-��¼
    INSERT INTO RWA_DEV.RWA_TZ_GUARANTEE(
         				 DataDate          												  --��������
								,DataNo                                     --������ˮ��
								,GuaranteeID                                --��֤ID
								,SSysID                                     --ԴϵͳID
								,GuaranteeConID                             --��֤��ͬID
								,GuarantorID                                --��֤��ID
								,CreditRiskDataType                         --���÷�����������
								,GuaranteeWay                            		--������ʽ
								,QualFlagSTD                            		--Ȩ�ط��ϸ��ʶ
								,QualFlagFIRB                               --�����������ϸ��ʶ
								,GuaranteeTypeSTD                           --Ȩ�ط���֤����
								,GuarantorSdvsSTD                           --Ȩ�ط���֤��ϸ��
								,GuaranteeTypeIRB                           --��������֤����
								,GuaranteeAmount                            --��֤�ܶ�
								,Currency                                   --����
								,StartDate                                  --��ʼ����
								,DueDate                                    --��������
								,OriginalMaturity                           --ԭʼ����
								,ResidualM                                  --ʣ������
								,GuarantorIRating                           --��֤���ڲ�����
								,GuarantorPD                                --��֤��ΥԼ����
								,GroupID                                    --������
    )
    SELECT
         				 TO_DATE(p_data_dt_str,'YYYYMMDD')								AS  DATADATE          	 --��������
         				,p_data_dt_str														 		    AS	DATANO               --������ˮ��
         				,T1.MITIGATIONID																	AS	GUARANTEEID          --��֤ID
								,'TZHBJJ'																				  AS	SSYSID               --ԴϵͳID
								,T1.BOND_ID																				AS	GUARANTEECONID       --��֤��ͬID
								,T1.CUSTID1																				AS	GUARANTORID          --��֤��ID
								,'01'																							AS	CREDITRISKDATATYPE   --���÷�����������      					Ĭ�� һ�������(01)
								,T1.GUARANTYTYPE																	AS	GUARANTEEWAY       	 --������ʽ
								,''																								AS	QUALFLAGSTD          --Ȩ�ط��ϸ��ʶ
								,''																								AS	QUALFLAGFIRB         --�����������ϸ��ʶ
								,''																								AS	GUARANTEETYPESTD     --Ȩ�ط���֤����
								,''																								AS	GUARANTORSDVSSTD     --Ȩ�ط���֤��ϸ��
								,''																								AS	GUARANTEETYPEIRB     --��������֤����
								,ROUND(TO_NUMBER(REPLACE(NVL(T1.GUARANTYSUM,'0'),',','')),6)
																																	AS	GUARANTEEAMOUNT      --��֤�ܶ�
								,NVL(T1.GUARANTYCURRENCYCODE,'CNY')								AS	CURRENCY             --����
								,T3.STARTDATE																		  AS	STARTDATE            --��ʼ����
								,T3.DUEDATE																				AS	DUEDATE              --��������
								,T3.ORIGINALMATURITY															AS  ORIGINALMATURITY   	 --ԭʼ����
								,T3.RESIDUALM																			AS	RESIDUALM            --ʣ������
								,''																								AS	GUARANTORIRATING     --��֤���ڲ�����
								,NULL																							AS	GUARANTORPD          --��֤��ΥԼ����
								,''																								AS	GROUPID              --������
    FROM				RWA.RWA_WS_BONDTRADE_MF T1	 																--���һ���ծȯͶ�ʲ�¼��
		INNER JOIN  RWA.RWA_WP_DATASUPPLEMENT T2
    ON          T1.SUPPORGID = T2.ORGID
    AND         T2.DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
    AND         T2.SUPPTMPLID = 'M-0071'
    AND         T2.SUBMITFLAG = '1'
    LEFT JOIN		RWA_DEV.RWA_TZ_EXPOSURE T3
    ON					T1.BOND_ID = T3.DUEID
		WHERE 			T1.GUARANTYTYPE IN ('004001004001','004001005001','004001006001','004001006002','010')
		AND					T1.DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
		;

    COMMIT;

    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_TZ_GUARANTEE',cascade => true);

    /*Ŀ�������ͳ��*/
    --ͳ�Ʋ���ļ�¼
    SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_TZ_GUARANTEE;
    --Dbms_output.Put_line('RWA_DEV.RWA_TZ_GUARANTEE��ǰ����Ĳ���ϵͳ-Ӧ�տ�Ͷ�����ݼ�¼Ϊ:' || v_count || '��');


		--Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̽��� ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    p_po_rtncode := '1';
	  p_po_rtnmsg  := '�ɹ�' || '-' || v_count;
		--�����쳣
		EXCEPTION
    WHEN OTHERS THEN
				 --Dbms_output.Put_line('������,�������Ϊ:'||sqlcode||';������ϢΪ:'||sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace);
       	 ROLLBACK;
   			 p_po_rtncode := sqlcode;
   			 p_po_rtnmsg  := 'Ͷ���ౣ֤('|| v_pro_name ||')ETLת��ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_TZ_GUARANTEE;
/

