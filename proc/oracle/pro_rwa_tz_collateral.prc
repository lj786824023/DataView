CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_TZ_COLLATERAL(
			 											p_data_dt_str	IN	VARCHAR2,		--�������� yyyyMMdd
       											p_po_rtncode	OUT	VARCHAR2,		--���ر�� 1 �ɹ�,0 ʧ��
														p_po_rtnmsg		OUT	VARCHAR2		--��������
				)
  /*
    �洢��������:RWA_DEV.PRO_RWA_TZ_COLLATERAL
    ʵ�ֹ���:����ϵͳ-Ͷ��-����ѺƷ(������Դ����ϵͳ��Ӧ�տ�Ͷ�������Ϣȫ������RWAͶ����ӿڱ����ѺƷ����)
    ���ݿھ�:ȫ��
    ����Ƶ��:�³�����
    ��  ��  :V1.0.0
    ��д��  :LISY
    ��дʱ��:2016-04-21
    ��  λ  :�Ϻ���˶��Ϣ�����ɷ����޹�˾
    Դ  ��1 :RWA_DEV.FNS_BND_INFO_B|����ϵͳծȯ��Ϣ��
    Դ  ��2 :RWA_DEV.FNS_BND_BOOK_B|����ϵͳ������
    Դ  ��3 :RWA.RWA_WS_RECEIVABLE|Ӧ�տ�Ͷ�ʲ�¼��
    Դ  ��4 :RWA.RWA_WP_DATASUPPLEMENT|��¼������ϸ��Ϣ��
    Դ  ��5 :RWA_DEV.CBS_BND|ծȯͶ�ʵǼǲ�
    Դ  ��6 :RWA_DEV.CBS_IAC|ͨ�÷ֻ���
    Դ  ��7 :RWA.RWA_WS_B_RECEIVABLE|���뷵�����������ʲ�_Ӧ���˿�Ͷ�ʲ�¼��
    Դ  ��8 :RWA.RWA_WSIB_ABS_ISSUE_EXPOSURE|���л����ʲ�֤ȯ����¶�̵ױ�
    Դ  ��9 :RWA.RWA_WSIB_ABS_INVEST_EXPOSURE|Ͷ�ʻ����ʲ�֤ȯ����¶�̵ױ�
    Ŀ���1 :RWA_DEV.RWA_TZ_COLLATERAL|����ϵͳͶ�������ѺƷ��
    �����¼(�޸���|�޸�ʱ��|�޸�����):
    */
  AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_TZ_COLLATERAL';
  --�����쳣����
  v_raise EXCEPTION;
  --���嵱ǰ����ļ�¼��
  v_count INTEGER;

  BEGIN
    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*�����ȫ�����ݼ��������Ŀ���*/
    --1.���Ŀ����е�ԭ�м�¼
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_TZ_COLLATERAL';

    --2.���������������ݴ�Դ����뵽Ŀ�����
    --2.1 ����ϵͳ-Ӧ�տ�Ͷ��-����ѺƷ-�ǻ�������-��ͬ
    INSERT INTO RWA_DEV.RWA_TZ_COLLATERAL(
                 DataDate                              --��������
                ,DataNo                                --������ˮ��
                ,CollateralID                       	 --����ѺƷID
                ,SSysID                             	 --ԴϵͳID
                ,SGuarContractID                    	 --Դ������ͬID
                ,SCollateralID                      	 --Դ����ѺƷID
                ,CollateralName                     	 --����ѺƷ����
                ,IssuerID                           	 --������ID
                ,ProviderID                         	 --�ṩ��ID
                ,CreditRiskDataType                 	 --���÷�����������
                ,GuaranteeWay                       	 --������ʽ
                ,SourceColType                      	 --Դ����ѺƷ����
                ,SourceColSubType                   	 --Դ����ѺƷС��
                ,SpecPurpBondFlag                   	 --�Ƿ�Ϊ�չ��������в�����������е�ծȯ
                ,QualFlagSTD                        	 --Ȩ�ط��ϸ��ʶ
                ,QualFlagFIRB                 				 --�����������ϸ��ʶ
                ,CollateralTypeSTD                  	 --Ȩ�ط�����ѺƷ����
                ,CollateralSdvsSTD                  	 --Ȩ�ط�����ѺƷϸ��
                ,CollateralTypeIRB                  	 --����������ѺƷ����
                ,CollateralAmount                   	 --��Ѻ�ܶ�
                ,Currency                           	 --����
                ,StartDate                          	 --��ʼ����
                ,DueDate                            	 --��������
                ,OriginalMaturity                   	 --ԭʼ����
                ,ResidualM                          	 --ʣ������
                ,InteHaircutsFlag                   	 --���й����ۿ�ϵ����ʶ
                ,InternalHc                         	 --�ڲ��ۿ�ϵ��
                ,FCType                             	 --������ѺƷ����
                ,ABSFlag                            	 --�ʲ�֤ȯ����ʶ
                ,RatingDurationType                 	 --������������
                ,FCIssueRating     										 --������ѺƷ���еȼ�
                ,FCIssuerType                          --������ѺƷ���������
                ,FCIssuerState                         --������ѺƷ������ע�����
                ,FCResidualM                           --������ѺƷʣ������
                ,RevaFrequency                         --�ع�Ƶ��
                ,GroupID                               --������
                ,RCERating														 --�����˾���ע����ⲿ����
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
						     										 AND DATANO = p_data_dt_str) T2                       --�Ŵ���ͬ������
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
									    			 AND T5.GUARANTYTYPEID NOT IN ('004001004001','004001005001','004001006001','004001006002','001001003001')   --����֤����������֤�������Ա������������Ա�������Ϊ��֤����֤��ȡ
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
                                       AND T2.GUARANTYTYPEID NOT IN
                                           ('004001004001',
                                            '004001005001',
                                            '004001006001',
                                            '004001006002',
                                            '001001003001')
                                       AND T2.AFFIRMVALUE0 > 0)
                               AND T1.DATANO = p_data_dt_str)
                     WHERE RM = 1
    )
    SELECT
                TO_DATE(p_data_dt_str,'YYYYMMDD')            AS DataDate                 --��������
                ,p_data_dt_str													     AS DataNo                   --������ˮ��
                ,'YP' || T1.GUARANTYID											 AS CollateralID           	 --����ѺƷID
                ,'TZ'                  											 AS SSysID              	 	 --ԴϵͳID
                ,''																					 AS SGuarContractID        	 --Դ������ͬID                             Ĭ�� NULL
                ,T1.GUARANTYID															 AS SCollateralID          	 --Դ����ѺƷID
                ,T7.ITEMNAME		                   			 		 AS CollateralName         	 --����ѺƷ����
                ,T3.OPENBANKNO                               AS IssuerID             	 	 --������ID
                ,T6.CUSTOMERID                           	 	 AS ProviderID             	 --�ṩ��ID
                ,'01'                                  			 AS CreditRiskDataType     	 --���÷�����������                         Ĭ�� һ�������(01)
                ,T1.GUARANTYTYPE                 		 				 AS GuaranteeWay           	 --������ʽ
                ,SUBSTR(T1.GUARANTYTYPEID,1,6)       				 AS SourceColType     	     --Դ����ѺƷ����
                ,T1.GUARANTYTYPEID										       AS SourceColSubType         --Դ����ѺƷС��
                ,CASE WHEN SUBSTR(NVL(T3.BONDPUBLISHPURPOSE,'0020'),2,2) = '01' THEN '1'
                 ELSE '0'
                 END														             AS SpecPurpBondFlag  			 --�Ƿ�Ϊ�չ��������в�����������е�ծȯ
                ,''              									 			 		 AS QualFlagSTD            	 --Ȩ�ط��ϸ��ʶ                           Ĭ�� NULL RWA������
                ,CASE WHEN T1.QUALIFYFLAG03 = '01' THEN '1'
                			WHEN T1.QUALIFYFLAG03 = '02' THEN '0'
                 			ELSE ''
                 END                			 			 						 AS QualFlagFIRB           	 --�����������ϸ��ʶ                       Ĭ�� NULL RWA������
                ,''							              				 			 AS CollateralTypeSTD 			 --Ȩ�ط�����ѺƷ����                    		Ĭ�� NULL RWA������
                ,''			                          					 AS CollateralSdvsSTD 		 	 --Ȩ�ط�����ѺƷϸ��                    		Ĭ�� NULL RWA������
                ,CASE WHEN T1.QUALIFYFLAG03 = '01' THEN T4.SLOWRELEASETYPE
                 ELSE ''
                 END							 		 									 		 AS CollateralTypeIRB      	 --����������ѺƷ����                       Ĭ�� NULL RWA������
                ,T1.AFFIRMVALUE0		 				 								 AS CollateralAmount     	 	 --��Ѻ�ܶ�
                ,NVL(T1.AFFIRMCURRENCY,'CNY')								 AS Currency               	 --����
								,T2.STARTDATE       												 AS StartDate         			 --��ʼ����
                ,T2.DUEDATE													         AS DueDate                	 --��������
                ,CASE WHEN (TO_DATE(T2.DUEDATE,'YYYYMMDD') - TO_DATE(T2.STARTDATE,'YYYYMMDD')) / 365 < 0	THEN 0
                 ELSE (TO_DATE(T2.DUEDATE,'YYYYMMDD') - TO_DATE(T2.STARTDATE,'YYYYMMDD')) / 365
                 END					                             	 AS OriginalMaturity    	 	 --ԭʼ����
                ,CASE WHEN (TO_DATE(T2.DUEDATE,'YYYYMMDD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365 < 0	THEN 0
                 ELSE (TO_DATE(T2.DUEDATE,'YYYYMMDD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365
                 END                                         AS ResidualM           	 	 --ʣ������
                ,'0'                                         AS InteHaircutsFlag    	 	 --���й����ۿ�ϵ����ʶ                     Ĭ�� ��(0)
                ,NULL                                        AS InternalHc          	 	 --�ڲ��ۿ�ϵ��                             Ĭ�� NULL
                ,CASE WHEN T1.QUALIFYFLAG03 = '01' THEN T4.FINANCIALPLEDGETYPE
                 ELSE ''
                 END                                         AS FCType                 	 --������ѺƷ����                           Ĭ�� NULL RWA������
                ,CASE WHEN T3.ABSFLAG = '01' THEN '1'
                 ELSE '0'
                 END			                                   AS ABSFlag             	 	 --�ʲ�֤ȯ����ʶ
                ,T3.TIMELIMIT                                AS RatingDurationType  	 	 --������������                             ��ת��Ϊ���ڡ�����
                ,RWA_DEV.GETSTANDARDRATING1(T3.BONDRATING)   AS FCIssueRating     			 --������ѺƷ���еȼ�                    		��ת��Ϊ����
                ,CASE WHEN T3.OPENBANKTYPE LIKE '01%' OR T3.OPENBANKTYPE LIKE '10%' THEN '01'
                 ELSE '02'
                 END						                             AS FCIssuerType             --������ѺƷ���������                  		���ݷ����˿ͻ������Ƿ�Ϊ��Ȩ���ж�
                ,CASE WHEN NVL(T3.BONDPUBLISHCOUNTRY,'CHN') = 'CHN' THEN '01'
                 ELSE '02'
                 END									                     	 AS FCIssuerState            --������ѺƷ������ע�����
                ,CASE WHEN (TO_DATE(T2.DUEDATE,'YYYYMMDD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365 < 0	THEN 0
                 ELSE (TO_DATE(T2.DUEDATE,'YYYYMMDD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365
                 END                                         AS FCResidualM              --������ѺƷʣ������
                ,1                                           AS RevaFrequency            --�ع�Ƶ��                              		Ĭ�� 1
                ,''                                          AS GroupID                  --������                              		Ĭ�� NULL
                ,T5.RATINGRESULT														 AS RCERating								 --�����˾���ע����ⲿ����

    FROM				RWA_DEV.NCM_GUARANTY_INFO T1															--�Ŵ�����ѺƷ��Ϣ��
    INNER JOIN	TEMP_BND_COLLATERAL	T2
   	ON					T1.GUARANTYID = T2.GUARANTYID
    LEFT JOIN		RWA_DEV.NCM_ASSET_FINANCE T3															--�Ŵ�������ѺƷ��Ϣ��
    ON					T1.GUARANTYID = T3.GUARANTYID
    AND					T3.DATANO = p_data_dt_str
    LEFT JOIN		RWA_DEV.NCM_COL_PARAM T4
    ON					T1.GUARANTYTYPEID = T4.GUARANTYTYPE
    AND					T4.DATANO = p_data_dt_str
    LEFT JOIN		RWA.RWA_WP_COUNTRYRATING T5
    ON					NVL(T3.BONDPUBLISHCOUNTRY,'CHN') = T5.COUNTRYCODE
    AND					T5.ISINUSE = '1'
    LEFT JOIN		TEMP_CUST_INFO T6
    ON					T1.OBLIGEEIDTYPE = T6.CERTTYPE
    AND 				T1.OBLIGEEIDNUMBER = T6.CERTID
    LEFT JOIN		RWA.CODE_LIBRARY T7
    ON					T1.GUARANTYTYPEID = T7.ITEMNO
    AND					T7.CODENO = 'GuarantyList'
		WHERE 			T1.GUARANTYTYPEID NOT IN ('004001004001','004001005001','004001006001','004001006002','001001003001')
																																					--����֤����������֤�������Ա������������Ա�������Ϊ��֤����֤��ȡ
		AND 				T1.AFFIRMVALUE0 > 0
		AND 				T1.DATANO = p_data_dt_str
		;

    COMMIT;

    --2.2 ����ϵͳ-Ӧ�տ�Ͷ��-����ѺƷ-�ǻ�������-����
    INSERT INTO RWA_DEV.RWA_TZ_COLLATERAL(
                 DataDate                              --��������
                ,DataNo                                --������ˮ��
                ,CollateralID                       	 --����ѺƷID
                ,SSysID                             	 --ԴϵͳID
                ,SGuarContractID                    	 --Դ������ͬID
                ,SCollateralID                      	 --Դ����ѺƷID
                ,CollateralName                     	 --����ѺƷ����
                ,IssuerID                           	 --������ID
                ,ProviderID                         	 --�ṩ��ID
                ,CreditRiskDataType                 	 --���÷�����������
                ,GuaranteeWay                       	 --������ʽ
                ,SourceColType                      	 --Դ����ѺƷ����
                ,SourceColSubType                   	 --Դ����ѺƷС��
                ,SpecPurpBondFlag                   	 --�Ƿ�Ϊ�չ��������в�����������е�ծȯ
                ,QualFlagSTD                        	 --Ȩ�ط��ϸ��ʶ
                ,QualFlagFIRB                 				 --�����������ϸ��ʶ
                ,CollateralTypeSTD                  	 --Ȩ�ط�����ѺƷ����
                ,CollateralSdvsSTD                  	 --Ȩ�ط�����ѺƷϸ��
                ,CollateralTypeIRB                  	 --����������ѺƷ����
                ,CollateralAmount                   	 --��Ѻ�ܶ�
                ,Currency                           	 --����
                ,StartDate                          	 --��ʼ����
                ,DueDate                            	 --��������
                ,OriginalMaturity                   	 --ԭʼ����
                ,ResidualM                          	 --ʣ������
                ,InteHaircutsFlag                   	 --���й����ۿ�ϵ����ʶ
                ,InternalHc                         	 --�ڲ��ۿ�ϵ��
                ,FCType                             	 --������ѺƷ����
                ,ABSFlag                            	 --�ʲ�֤ȯ����ʶ
                ,RatingDurationType                 	 --������������
                ,FCIssueRating     										 --������ѺƷ���еȼ�
                ,FCIssuerType                          --������ѺƷ���������
                ,FCIssuerState                         --������ѺƷ������ע�����
                ,FCResidualM                           --������ѺƷʣ������
                ,RevaFrequency                         --�ع�Ƶ��
                ,GroupID                               --������
                ,RCERating														 --�����˾���ע����ⲿ����
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
											INNER JOIN RWA_DEV.NCM_BUSINESS_PUTOUT T2											--���˱�
															ON T1.CONTRACTNO = T2.CONTRACTSERIALNO
														 AND T2.DATANO = p_data_dt_str
											INNER JOIN RWA_DEV.NCM_GUARANTY_RELATIVE T3										--ѺƷ������
															ON T2.SERIALNO = T3.OBJECTNO
														 AND T3.OBJECTTYPE = 'PutOutApply'
														 AND T3.DATANO = p_data_dt_str
									    INNER JOIN RWA_DEV.NCM_GUARANTY_INFO T5															--�Ŵ�����ѺƷ��Ϣ��
									    				ON T3.GUARANTYID = T5.GUARANTYID
									    			 AND T5.GUARANTYTYPEID NOT IN ('004001004001','004001005001','004001006001','004001006002','001001003001')   --����֤����������֤�������Ա������������Ա�������Ϊ��֤����֤��ȡ
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
                                       AND T2.GUARANTYTYPEID NOT IN
                                           ('004001004001',
                                            '004001005001',
                                            '004001006001',
                                            '004001006002',
                                            '001001003001')
                                       AND T2.AFFIRMVALUE0 > 0)
                               AND T1.DATANO = p_data_dt_str)
                     WHERE RM = 1
    )
    SELECT
                TO_DATE(p_data_dt_str,'YYYYMMDD')            AS DataDate                 --��������
                ,p_data_dt_str													     AS DataNo                   --������ˮ��
                ,'YP' || T1.GUARANTYID											 AS CollateralID           	 --����ѺƷID
                ,'TZ'                  											 AS SSysID              	 	 --ԴϵͳID
                ,''																					 AS SGuarContractID        	 --Դ������ͬID                             Ĭ�� NULL
                ,T1.GUARANTYID															 AS SCollateralID          	 --Դ����ѺƷID
                ,T7.ITEMNAME		                   			 		 AS CollateralName         	 --����ѺƷ����
                ,T3.OPENBANKNO                               AS IssuerID             	 	 --������ID
                ,T6.CUSTOMERID                           	 	 AS ProviderID             	 --�ṩ��ID
                ,'01'                                  			 AS CreditRiskDataType     	 --���÷�����������                         Ĭ�� һ�������(01)
                ,T1.GUARANTYTYPE                 		 				 AS GuaranteeWay           	 --������ʽ
                ,SUBSTR(T1.GUARANTYTYPEID,1,6)       				 AS SourceColType     	     --Դ����ѺƷ����
                ,T1.GUARANTYTYPEID										       AS SourceColSubType         --Դ����ѺƷС��
                ,CASE WHEN SUBSTR(NVL(T3.BONDPUBLISHPURPOSE,'0020'),2,2) = '01' THEN '1'
                 ELSE '0'
                 END														             AS SpecPurpBondFlag  			 --�Ƿ�Ϊ�չ��������в�����������е�ծȯ
                ,''              									 			 		 AS QualFlagSTD            	 --Ȩ�ط��ϸ��ʶ                           Ĭ�� NULL RWA������
                ,CASE WHEN T1.QUALIFYFLAG03 = '01' THEN '1'
                			WHEN T1.QUALIFYFLAG03 = '02' THEN '0'
                 			ELSE ''
                 END                			 			 						 AS QualFlagFIRB           	 --�����������ϸ��ʶ                       Ĭ�� NULL RWA������
                ,''							              				 			 AS CollateralTypeSTD 			 --Ȩ�ط�����ѺƷ����                    		Ĭ�� NULL RWA������
                ,''			                          					 AS CollateralSdvsSTD 		 	 --Ȩ�ط�����ѺƷϸ��                    		Ĭ�� NULL RWA������
                ,CASE WHEN T1.QUALIFYFLAG03 = '01' THEN T4.SLOWRELEASETYPE
                 ELSE ''
                 END							 		 									 		 AS CollateralTypeIRB      	 --����������ѺƷ����                       Ĭ�� NULL RWA������
                ,T1.AFFIRMVALUE0		 				 								 AS CollateralAmount     	 	 --��Ѻ�ܶ�
                ,NVL(T1.AFFIRMCURRENCY,'CNY')								 AS Currency               	 --����
								,T2.STARTDATE       												 AS StartDate         			 --��ʼ����
                ,T2.DUEDATE													         AS DueDate                	 --��������
                ,CASE WHEN (TO_DATE(T2.DUEDATE,'YYYYMMDD') - TO_DATE(T2.STARTDATE,'YYYYMMDD')) / 365 < 0	THEN 0
                 ELSE (TO_DATE(T2.DUEDATE,'YYYYMMDD') - TO_DATE(T2.STARTDATE,'YYYYMMDD')) / 365
                 END					                             	 AS OriginalMaturity    	 	 --ԭʼ����
                ,CASE WHEN (TO_DATE(T2.DUEDATE,'YYYYMMDD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365 < 0	THEN 0
                 ELSE (TO_DATE(T2.DUEDATE,'YYYYMMDD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365
                 END                                         AS ResidualM           	 	 --ʣ������
                ,'0'                                         AS InteHaircutsFlag    	 	 --���й����ۿ�ϵ����ʶ                     Ĭ�� ��(0)
                ,NULL                                        AS InternalHc          	 	 --�ڲ��ۿ�ϵ��                             Ĭ�� NULL
                ,CASE WHEN T1.QUALIFYFLAG03 = '01' THEN T4.FINANCIALPLEDGETYPE
                 ELSE ''
                 END                                         AS FCType                 	 --������ѺƷ����                           Ĭ�� NULL RWA������
                ,CASE WHEN T3.ABSFLAG = '01' THEN '1'
                 ELSE '0'
                 END			                                   AS ABSFlag             	 	 --�ʲ�֤ȯ����ʶ
                ,T3.TIMELIMIT                                AS RatingDurationType  	 	 --������������                             ��ת��Ϊ���ڡ�����
                ,RWA_DEV.GETSTANDARDRATING1(T3.BONDRATING)   AS FCIssueRating     			 --������ѺƷ���еȼ�                    		��ת��Ϊ����
                ,CASE WHEN T3.OPENBANKTYPE LIKE '01%' OR T3.OPENBANKTYPE LIKE '10%' THEN '01'
                 ELSE '02'
                 END						                             AS FCIssuerType             --������ѺƷ���������                  		���ݷ����˿ͻ������Ƿ�Ϊ��Ȩ���ж�
                ,CASE WHEN NVL(T3.BONDPUBLISHCOUNTRY,'CHN') = 'CHN' THEN '01'
                 ELSE '02'
                 END									                     	 AS FCIssuerState            --������ѺƷ������ע�����
                ,CASE WHEN (TO_DATE(T2.DUEDATE,'YYYYMMDD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365 < 0	THEN 0
                 ELSE (TO_DATE(T2.DUEDATE,'YYYYMMDD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365
                 END                                         AS FCResidualM              --������ѺƷʣ������
                ,1                                           AS RevaFrequency            --�ع�Ƶ��                              		Ĭ�� 1
                ,''                                          AS GroupID                  --������                              		Ĭ�� NULL
                ,T5.RATINGRESULT														 AS RCERating								 --�����˾���ע����ⲿ����

    FROM				RWA_DEV.NCM_GUARANTY_INFO T1															--�Ŵ�����ѺƷ��Ϣ��
    INNER JOIN	TEMP_BND_COLLATERAL	T2
   	ON					T1.GUARANTYID = T2.GUARANTYID
    LEFT JOIN		RWA_DEV.NCM_ASSET_FINANCE T3															--�Ŵ�������ѺƷ��Ϣ��
    ON					T1.GUARANTYID = T3.GUARANTYID
    AND					T3.DATANO = p_data_dt_str
    LEFT JOIN		RWA_DEV.NCM_COL_PARAM T4
    ON					T1.GUARANTYTYPEID = T4.GUARANTYTYPE
    AND					T4.DATANO = p_data_dt_str
    LEFT JOIN		RWA.RWA_WP_COUNTRYRATING T5
    ON					NVL(T3.BONDPUBLISHCOUNTRY,'CHN') = T5.COUNTRYCODE
    AND					T5.ISINUSE = '1'
    LEFT JOIN		TEMP_CUST_INFO T6
    ON					T1.OBLIGEEIDTYPE = T6.CERTTYPE
    AND 				T1.OBLIGEEIDNUMBER = T6.CERTID
    LEFT JOIN		RWA.CODE_LIBRARY T7
    ON					T1.GUARANTYTYPEID = T7.ITEMNO
    AND					T7.CODENO = 'GuarantyList'
		WHERE 			T1.GUARANTYTYPEID NOT IN ('004001004001','004001005001','004001006001','004001006002','001001003001')
																																					--����֤����������֤�������Ա������������Ա�������Ϊ��֤����֤��ȡ
		AND 				T1.AFFIRMVALUE0 > 0
		AND 				T1.DATANO = p_data_dt_str
		AND NOT EXISTS (SELECT 1 FROM RWA_DEV.RWA_TZ_COLLATERAL TC WHERE 'YP' || T1.GUARANTYID = TC.COLLATERALID)
		;

    COMMIT;

    --2.3 ����ϵͳ-Ӧ�տ�Ͷ��-��֤��
    /*
    INSERT INTO RWA_DEV.RWA_TZ_COLLATERAL(
                 DataDate                              --��������
                ,DataNo                                --������ˮ��
                ,CollateralID                       	 --����ѺƷID
                ,SSysID                             	 --ԴϵͳID
                ,SGuarContractID                    	 --Դ������ͬID
                ,SCollateralID                      	 --Դ����ѺƷID
                ,CollateralName                     	 --����ѺƷ����
                ,IssuerID                           	 --������ID
                ,ProviderID                         	 --�ṩ��ID
                ,CreditRiskDataType                 	 --���÷�����������
                ,GuaranteeWay                       	 --������ʽ
                ,SourceColType                      	 --Դ����ѺƷ����
                ,SourceColSubType                   	 --Դ����ѺƷС��
                ,SpecPurpBondFlag                   	 --�Ƿ�Ϊ�չ��������в�����������е�ծȯ
                ,QualFlagSTD                        	 --Ȩ�ط��ϸ��ʶ
                ,QualFlagFIRB                 				 --�����������ϸ��ʶ
                ,CollateralTypeSTD                  	 --Ȩ�ط�����ѺƷ����
                ,CollateralSdvsSTD                  	 --Ȩ�ط�����ѺƷϸ��
                ,CollateralTypeIRB                  	 --����������ѺƷ����
                ,CollateralAmount                   	 --��Ѻ�ܶ�
                ,Currency                           	 --����
                ,StartDate                          	 --��ʼ����
                ,DueDate                            	 --��������
                ,OriginalMaturity                   	 --ԭʼ����
                ,ResidualM                          	 --ʣ������
                ,InteHaircutsFlag                   	 --���й����ۿ�ϵ����ʶ
                ,InternalHc                         	 --�ڲ��ۿ�ϵ��
                ,FCType                             	 --������ѺƷ����
                ,ABSFlag                            	 --�ʲ�֤ȯ����ʶ
                ,RatingDurationType                 	 --������������
                ,FCIssueRating     										 --������ѺƷ���еȼ�
                ,FCIssuerType                          --������ѺƷ���������
                ,FCIssuerState                         --������ѺƷ������ע�����
                ,FCResidualM                           --������ѺƷʣ������
                ,RevaFrequency                         --�ع�Ƶ��
                ,GroupID                               --������
                ,RCERating														 --�����˾���ע����ⲿ����
    )
    SELECT
                TO_DATE(p_data_dt_str,'YYYYMMDD')            AS DataDate                 --��������
                ,p_data_dt_str													     AS DataNo                   --������ˮ��
                ,T1.SERIALNO																 AS CollateralID           	 --����ѺƷID
                ,'TZ'                  											 AS SSysID              	 	 --ԴϵͳID
                ,''																					 AS SGuarContractID        	 --Դ������ͬID                             Ĭ�� NULL
                ,T1.SERIALNO																 AS SCollateralID          	 --Դ����ѺƷID
                ,'��֤��'	                         			 		 AS CollateralName         	 --����ѺƷ����
                ,''						                               AS IssuerID             	 	 --������ID                                 Ĭ�� NULL
                ,T1.CUSTOMERID                           	 	 AS ProviderID             	 --�ṩ��ID
                ,'01'                                  			 AS CreditRiskDataType     	 --���÷�����������                         Ĭ�� һ�������(01)
                ,'060'                           		 				 AS GuaranteeWay           	 --������ʽ                                 Ĭ�� ��Ѻ(060)
                ,'001001'                            				 AS SourceColType     	     --Դ����ѺƷ����                        		Ĭ�� �ֽ���ȼ���(001001)
                ,'001001003001'												       AS SourceColSubType         --Դ����ѺƷС��                        		Ĭ�� ��֤��(001001003001)
                ,'0'														             AS SpecPurpBondFlag  			 --�Ƿ�Ϊ�չ��������в�����������е�ծȯ		Ĭ�� ��(0)
                ,'1'             									 			 		 AS QualFlagSTD            	 --Ȩ�ط��ϸ��ʶ                           Ĭ�� ��(1)
                ,'1'	                           			 			 AS QualFlagFIRB           	 --�����������ϸ��ʶ                       Ĭ�� ��(1)
                ,'030103'				              				 			 AS CollateralTypeSTD 			 --Ȩ�ط�����ѺƷ����                    		Ĭ�� ��֤��(030103)
                ,'01'		                          					 AS CollateralSdvsSTD 		 	 --Ȩ�ط�����ѺƷϸ��                    		Ĭ�� �ֽ����ʲ�(01)
                ,'030201'					 		 									 		 AS CollateralTypeIRB      	 --����������ѺƷ����                       Ĭ�� ������ѺƷ(030201)
                ,T1.BAILSUM																	 AS CollateralAmount     	 	 --��Ѻ�ܶ�
                ,NVL(T1.BAILCURRENCY,T1.BUSINESSCURRENCY)		 AS Currency               	 --����
								,T3.STARTDATE													       AS StartDate         			 --��ʼ����
                ,T3.DUEDATE													         AS DueDate                	 --��������
                ,CASE WHEN (TO_DATE(T3.DUEDATE,'YYYYMMDD') - TO_DATE(T3.STARTDATE,'YYYYMMDD')) / 365 < 0	THEN 0
                 ELSE (TO_DATE(T3.DUEDATE,'YYYYMMDD') - TO_DATE(T3.STARTDATE,'YYYYMMDD')) / 365
                 END					                             	 AS OriginalMaturity    	 	 --ԭʼ����
                ,CASE WHEN (TO_DATE(T3.DUEDATE,'YYYYMMDD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365 < 0	THEN 0
                 ELSE (TO_DATE(T3.DUEDATE,'YYYYMMDD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365
                 END                                         AS ResidualM           	 	 --ʣ������
                ,'0'                                         AS InteHaircutsFlag    	 	 --���й����ۿ�ϵ����ʶ                     Ĭ�� ��(0)
                ,NULL                                        AS InternalHc          	 	 --�ڲ��ۿ�ϵ��                             Ĭ�� NULL
                ,'01'                                        AS FCType                 	 --������ѺƷ����                           Ĭ�� �ֽ��ֽ�ȼ���(01)
                ,'0'                                         AS ABSFlag             	 	 --�ʲ�֤ȯ����ʶ
                ,''                                          AS RatingDurationType  	 	 --������������                             Ĭ�� NULL
                ,''                                          AS FCIssueRating     			 --������ѺƷ���еȼ�                    		Ĭ�� NULL
                ,''	                                         AS FCIssuerType             --������ѺƷ���������                  		Ĭ�� NULL
                ,''                                       	 AS FCIssuerState            --������ѺƷ������ע�����              		Ĭ�� NULL
                ,CASE WHEN (TO_DATE(T3.DUEDATE,'YYYYMMDD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365 < 0	THEN 0
                 ELSE (TO_DATE(T3.DUEDATE,'YYYYMMDD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365
                 END                                         AS FCResidualM              --������ѺƷʣ������
                ,1                                           AS RevaFrequency            --�ع�Ƶ��                              		Ĭ�� 1
                ,''                                          AS GroupID                  --������                              		Ĭ�� NULL
                ,''																					 AS RCERating								 --�����˾���ע����ⲿ����

    FROM				RWA_DEV.NCM_BUSINESS_CONTRACT T1											--�Ŵ���ͬ��
    INNER JOIN	RWA_DEV.NCM_BUSINESS_DUEBILL T2												--�Ŵ���ݱ�
    ON					T1.SERIALNO = T2.RELATIVESERIALNO2
    AND					T2.DATANO = p_data_dt_str
		INNER JOIN	RWA_DEV.RWA_TZ_EXPOSURE T3	 													--Ͷ�ʱ�¶��
		ON 					T2.SERIALNO = T3.EXPOSUREID
		AND					T3.BUSINESSTYPEID IN ('1040105060','1040105061','1040105062')	--Ӧ�տ�Ͷ��ҵ��
		WHERE 			T1.BUSINESSTYPE = '1040105060'												--Ӧ�տ�Ͷ��ҵ��
		AND					T1.BAILSUM > 0																				--��֤�����0
		AND					T1.DATANO = p_data_dt_str
		;
		*/
		--2.3 ����ϵͳ-Ӧ�տ�Ͷ��-��֤��
		INSERT INTO RWA_DEV.RWA_TZ_COLLATERAL(
                 DataDate                              --��������
                ,DataNo                                --������ˮ��
                ,CollateralID                       	 --����ѺƷID
                ,SSysID                             	 --ԴϵͳID
                ,SGuarContractID                    	 --Դ������ͬID
                ,SCollateralID                      	 --Դ����ѺƷID
                ,CollateralName                     	 --����ѺƷ����
                ,IssuerID                           	 --������ID
                ,ProviderID                         	 --�ṩ��ID
                ,CreditRiskDataType                 	 --���÷�����������
                ,GuaranteeWay                       	 --������ʽ
                ,SourceColType                      	 --Դ����ѺƷ����
                ,SourceColSubType                   	 --Դ����ѺƷС��
                ,SpecPurpBondFlag                   	 --�Ƿ�Ϊ�չ��������в�����������е�ծȯ
                ,QualFlagSTD                        	 --Ȩ�ط��ϸ��ʶ
                ,QualFlagFIRB                 				 --�����������ϸ��ʶ
                ,CollateralTypeSTD                  	 --Ȩ�ط�����ѺƷ����
                ,CollateralSdvsSTD                  	 --Ȩ�ط�����ѺƷϸ��
                ,CollateralTypeIRB                  	 --����������ѺƷ����
                ,CollateralAmount                   	 --��Ѻ�ܶ�
                ,Currency                           	 --����
                ,StartDate                          	 --��ʼ����
                ,DueDate                            	 --��������
                ,OriginalMaturity                   	 --ԭʼ����
                ,ResidualM                          	 --ʣ������
                ,InteHaircutsFlag                   	 --���й����ۿ�ϵ����ʶ
                ,InternalHc                         	 --�ڲ��ۿ�ϵ��
                ,FCType                             	 --������ѺƷ����
                ,ABSFlag                            	 --�ʲ�֤ȯ����ʶ
                ,RatingDurationType                 	 --������������
                ,FCIssueRating     										 --������ѺƷ���еȼ�
                ,FCIssuerType                          --������ѺƷ���������
                ,FCIssuerState                         --������ѺƷ������ע�����
                ,FCResidualM                           --������ѺƷʣ������
                ,RevaFrequency                         --�ع�Ƶ��
                ,GroupID                               --������
                ,RCERating														 --�����˾���ע����ⲿ����
    )
    SELECT
                TO_DATE(p_data_dt_str,'YYYYMMDD')            AS DataDate                 --��������
                ,p_data_dt_str													     AS DataNo                   --������ˮ��
                ,'HT' || T1.CONTRACTNO || T1.BAILCURRENCY		 AS CollateralID           	 --����ѺƷID
                ,'TZ'                  											 AS SSysID              	 	 --ԴϵͳID
                ,''																					 AS SGuarContractID        	 --Դ������ͬID                             Ĭ�� NULL
                ,T1.CONTRACTNO															 AS SCollateralID          	 --Դ����ѺƷID
                ,'��֤��'	                         			 		 AS CollateralName         	 --����ѺƷ����
                ,''						                               AS IssuerID             	 	 --������ID                                 Ĭ�� NULL
                ,T2.CLIENTID                           	 	 	 AS ProviderID             	 --�ṩ��ID
                ,'01'                                  			 AS CreditRiskDataType     	 --���÷�����������                         Ĭ�� һ�������(01)
                ,'060'                           		 				 AS GuaranteeWay           	 --������ʽ                                 Ĭ�� ��Ѻ(060)
                ,'001001'                            				 AS SourceColType     	     --Դ����ѺƷ����                        		Ĭ�� �ֽ���ȼ���(001001)
                ,'001001003001'												       AS SourceColSubType         --Դ����ѺƷС��                        		Ĭ�� ��֤��(001001003001)
                ,'0'														             AS SpecPurpBondFlag  			 --�Ƿ�Ϊ�չ��������в�����������е�ծȯ		Ĭ�� ��(0)
                ,'1'             									 			 		 AS QualFlagSTD            	 --Ȩ�ط��ϸ��ʶ                           Ĭ�� ��(1)
                ,'1'	                           			 			 AS QualFlagFIRB           	 --�����������ϸ��ʶ                       Ĭ�� ��(1)
                ,'030103'				              				 			 AS CollateralTypeSTD 			 --Ȩ�ط�����ѺƷ����                    		Ĭ�� ��֤��(030103)
                ,'01'		                          					 AS CollateralSdvsSTD 		 	 --Ȩ�ط�����ѺƷϸ��                    		Ĭ�� �ֽ����ʲ�(01)
                ,'030201'					 		 									 		 AS CollateralTypeIRB      	 --����������ѺƷ����                       Ĭ�� ������ѺƷ(030201)
                ,T1.BAILBALANCE															 AS CollateralAmount     	 	 --��Ѻ�ܶ�
                ,T1.BAILCURRENCY		 												 AS Currency               	 --����
								,T2.STARTDATE													       AS StartDate         			 --��ʼ����
                ,T2.DUEDATE													         AS DueDate                	 --��������
                ,CASE WHEN (TO_DATE(T2.DUEDATE,'YYYYMMDD') - TO_DATE(T2.STARTDATE,'YYYYMMDD')) / 365 < 0	THEN 0
                 ELSE (TO_DATE(T2.DUEDATE,'YYYYMMDD') - TO_DATE(T2.STARTDATE,'YYYYMMDD')) / 365
                 END					                             	 AS OriginalMaturity    	 	 --ԭʼ����
                ,CASE WHEN (TO_DATE(T2.DUEDATE,'YYYYMMDD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365 < 0	THEN 0
                 ELSE (TO_DATE(T2.DUEDATE,'YYYYMMDD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365
                 END                                         AS ResidualM           	 	 --ʣ������
                ,'0'                                         AS InteHaircutsFlag    	 	 --���й����ۿ�ϵ����ʶ                     Ĭ�� ��(0)
                ,NULL                                        AS InternalHc          	 	 --�ڲ��ۿ�ϵ��                             Ĭ�� NULL
                ,'01'                                        AS FCType                 	 --������ѺƷ����                           Ĭ�� �ֽ��ֽ�ȼ���(01)
                ,'0'                                         AS ABSFlag             	 	 --�ʲ�֤ȯ����ʶ
                ,''                                          AS RatingDurationType  	 	 --������������                             Ĭ�� NULL
                ,''                                          AS FCIssueRating     			 --������ѺƷ���еȼ�                    		Ĭ�� NULL
                ,''	                                         AS FCIssuerType             --������ѺƷ���������                  		Ĭ�� NULL
                ,''                                       	 AS FCIssuerState            --������ѺƷ������ע�����              		Ĭ�� NULL
                ,CASE WHEN (TO_DATE(T2.DUEDATE,'YYYYMMDD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365 < 0	THEN 0
                 ELSE (TO_DATE(T2.DUEDATE,'YYYYMMDD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365
                 END                                         AS FCResidualM              --������ѺƷʣ������
                ,1                                           AS RevaFrequency            --�ع�Ƶ��                              		Ĭ�� 1
                ,''                                          AS GroupID                  --������                              		Ĭ�� NULL
                ,''																					 AS RCERating								 --�����˾���ע����ⲿ����

    FROM				RWA_DEV.RWA_TEMP_BAIL2 T1															--�Ŵ���ͬ��
    INNER JOIN	RWA_DEV.RWA_TZ_CONTRACT T2														--Ͷ�ʺ�ͬ��
    ON					T1.CONTRACTNO = T2.SCONTRACTID
		AND					T2.BUSINESSTYPEID IN ('1040105060','1040105061','1040105062')	--Ӧ�տ�Ͷ��ҵ��
		AND					T2.DATANO = p_data_dt_str
		WHERE 			T1.ISMAX = '1'																				--ȡ��ͬ��ͬ������һ����Ϊ���
		;

    COMMIT;

    --2.4 ����ϵͳ-Ӧ�տ�Ͷ��-Ʊ���ʹ�ҵ��-Ʊ����Ϣ
    INSERT INTO RWA_DEV.RWA_TZ_COLLATERAL(
                 DataDate                              --��������
                ,DataNo                                --������ˮ��
                ,CollateralID                       	 --����ѺƷID
                ,SSysID                             	 --ԴϵͳID
                ,SGuarContractID                    	 --Դ������ͬID
                ,SCollateralID                      	 --Դ����ѺƷID
                ,CollateralName                     	 --����ѺƷ����
                ,IssuerID                           	 --������ID
                ,ProviderID                         	 --�ṩ��ID
                ,CreditRiskDataType                 	 --���÷�����������
                ,GuaranteeWay                       	 --������ʽ
                ,SourceColType                      	 --Դ����ѺƷ����
                ,SourceColSubType                   	 --Դ����ѺƷС��
                ,SpecPurpBondFlag                   	 --�Ƿ�Ϊ�չ��������в�����������е�ծȯ
                ,QualFlagSTD                        	 --Ȩ�ط��ϸ��ʶ
                ,QualFlagFIRB                 				 --�����������ϸ��ʶ
                ,CollateralTypeSTD                  	 --Ȩ�ط�����ѺƷ����
                ,CollateralSdvsSTD                  	 --Ȩ�ط�����ѺƷϸ��
                ,CollateralTypeIRB                  	 --����������ѺƷ����
                ,CollateralAmount                   	 --��Ѻ�ܶ�
                ,Currency                           	 --����
                ,StartDate                          	 --��ʼ����
                ,DueDate                            	 --��������
                ,OriginalMaturity                   	 --ԭʼ����
                ,ResidualM                          	 --ʣ������
                ,InteHaircutsFlag                   	 --���й����ۿ�ϵ����ʶ
                ,InternalHc                         	 --�ڲ��ۿ�ϵ��
                ,FCType                             	 --������ѺƷ����
                ,ABSFlag                            	 --�ʲ�֤ȯ����ʶ
                ,RatingDurationType                 	 --������������
                ,FCIssueRating     										 --������ѺƷ���еȼ�
                ,FCIssuerType                          --������ѺƷ���������
                ,FCIssuerState                         --������ѺƷ������ע�����
                ,FCResidualM                           --������ѺƷʣ������
                ,RevaFrequency                         --�ع�Ƶ��
                ,GroupID                               --������
                ,RCERating														 --�����˾���ע����ⲿ����
    )
    SELECT
                TO_DATE(p_data_dt_str,'YYYYMMDD')            AS DataDate                 --��������
                ,p_data_dt_str													     AS DataNo                   --������ˮ��
                ,'TZBILL' || T1.SERIALNO										 AS CollateralID           	 --����ѺƷID
                ,'TZ'                  											 AS SSysID              	 	 --ԴϵͳID
                ,''																					 AS SGuarContractID        	 --Դ������ͬID                             Ĭ�� NULL
                ,T1.SERIALNO																 AS SCollateralID          	 --Դ����ѺƷID
                ,CASE WHEN T1.BILLTYPE='2' THEN '��ҵ�жһ�Ʊ'
                 ELSE '�й���ҵ���гжһ�Ʊ'
                 END	                         			 		 		 AS CollateralName         	 --����ѺƷ����
                ,T1.ACCEPTORID		                           AS IssuerID             	 	 --������ID                                 Ĭ�� NULL
                ,T1.HOLDERID	                           	 	 AS ProviderID             	 --�ṩ��ID
                ,'01'                                  			 AS CreditRiskDataType     	 --���÷�����������                         Ĭ�� һ�������(01)
                ,'060'                           		 				 AS GuaranteeWay           	 --������ʽ                                 Ĭ�� ��Ѻ(060)
                ,'001004'                            				 AS SourceColType     	     --Դ����ѺƷ����                        		Ĭ�� Ʊ��(001004)
                ,CASE WHEN T1.BILLTYPE='2' THEN '001004004001'													 --��ҵ�жһ�Ʊ
                 ELSE '001004002001'																										 --�й���ҵ���гжһ�Ʊ
                 END																	       AS SourceColSubType         --Դ����ѺƷС��
                ,'0'														             AS SpecPurpBondFlag  			 --�Ƿ�Ϊ�չ��������в�����������е�ծȯ		Ĭ�� ��(0)
                ,''             									 			 		 AS QualFlagSTD            	 --Ȩ�ط��ϸ��ʶ                           Ĭ�� ��(1)
                ,''		                           			 			 AS QualFlagFIRB           	 --�����������ϸ��ʶ                       Ĭ�� ��(1)
                ,''							              				 			 AS CollateralTypeSTD 			 --Ȩ�ط�����ѺƷ����                    		Ĭ�� ��֤��(030103)
                ,''			                          					 AS CollateralSdvsSTD 		 	 --Ȩ�ط�����ѺƷϸ��                    		Ĭ�� �ֽ����ʲ�(01)
                ,''								 		 									 		 AS CollateralTypeIRB      	 --����������ѺƷ����                       Ĭ�� ������ѺƷ(030201)
                ,T1.BILLSUM					 				 								 AS CollateralAmount     	 	 --��Ѻ�ܶ�
                ,T1.LCCURRENCY															 AS Currency               	 --����
								/*
								,T1.ISSUEDATE													       AS StartDate         			 --��ʼ����
                ,T1.MATURITY												         AS DueDate                	 --��������
                ,CASE WHEN (TO_DATE(T1.MATURITY,'YYYYMMDD') - TO_DATE(T1.ISSUEDATE,'YYYYMMDD')) / 365 < 0	THEN 0
                 ELSE (TO_DATE(T1.MATURITY,'YYYYMMDD') - TO_DATE(T1.ISSUEDATE,'YYYYMMDD')) / 365
                 END					                             	 AS OriginalMaturity    	 	 --ԭʼ����
                ,CASE WHEN (TO_DATE(T1.MATURITY,'YYYYMMDD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365 < 0	THEN 0
                 ELSE (TO_DATE(T1.MATURITY,'YYYYMMDD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365
                 END                                         AS ResidualM           	 	 --ʣ������
                 */
                ,T7.STARTDATE													       AS StartDate         			 --��ʼ����
                ,T7.DUEDATE													         AS DueDate                	 --��������
                ,CASE WHEN (TO_DATE(T7.DUEDATE,'YYYYMMDD') - TO_DATE(T7.STARTDATE,'YYYYMMDD')) / 365 < 0	THEN 0
                 ELSE (TO_DATE(T7.DUEDATE,'YYYYMMDD') - TO_DATE(T7.STARTDATE,'YYYYMMDD')) / 365
                 END					                             	 AS OriginalMaturity    	 	 --ԭʼ����
                ,CASE WHEN (TO_DATE(T7.DUEDATE,'YYYYMMDD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365 < 0	THEN 0
                 ELSE (TO_DATE(T7.DUEDATE,'YYYYMMDD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365
                 END                                         AS ResidualM           	 	 --ʣ������
                ,'0'                                         AS InteHaircutsFlag    	 	 --���й����ۿ�ϵ����ʶ                     Ĭ�� ��(0)
                ,NULL                                        AS InternalHc          	 	 --�ڲ��ۿ�ϵ��                             Ĭ�� NULL
                ,''	                                         AS FCType                 	 --������ѺƷ����                           Ĭ�� NULL
                ,'0'                                         AS ABSFlag             	 	 --�ʲ�֤ȯ����ʶ
                ,CASE WHEN NVL(T3.COUNTRYCODE,'CHN') <> 'CHN' THEN '01'
                 ELSE ''
                 END                                         AS RatingDurationType  	 	 --������������                             Ĭ�� NULL
                ,CASE WHEN NVL(T3.COUNTRYCODE,'CHN') <> 'CHN' THEN T5.RATINGRESULT
                 ELSE ''
                 END                                         AS FCIssueRating     			 --������ѺƷ���еȼ�                    		Ĭ�� NULL
                ,CASE WHEN T3.RWACUSTOMERTYPE LIKE '01%' THEN '01'
                			WHEN T3.CUSTOMERID IS NOT NULL THEN '02'
                 ELSE ''
                 END                                         AS FCIssuerType             --������ѺƷ���������                  		Ĭ�� ����(02)
                ,CASE WHEN NVL(T3.COUNTRYCODE,'CHN') = 'CHN' THEN '01'
                 ELSE '02'
                 END					                            	 AS FCIssuerState            --������ѺƷ������ע�����              		Ĭ�� NULL
                /*
                ,CASE WHEN (TO_DATE(T1.MATURITY,'YYYYMMDD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365 < 0	THEN 0
                 ELSE (TO_DATE(T1.MATURITY,'YYYYMMDD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365
                 END                                         AS FCResidualM              --������ѺƷʣ������
                 */
                ,CASE WHEN (TO_DATE(T7.DUEDATE,'YYYYMMDD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365 < 0	THEN 0
                 ELSE (TO_DATE(T7.DUEDATE,'YYYYMMDD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365
                 END                                         AS FCResidualM              --������ѺƷʣ������
                ,1                                           AS RevaFrequency            --�ع�Ƶ��                              		Ĭ�� 1
                ,''                                          AS GroupID                  --������                              		Ĭ�� NULL
                ,T5.RATINGRESULT														 AS RCERating								 --�����˾���ע����ⲿ����

    FROM				RWA_DEV.NCM_BILL_INFO T1															--�Ŵ�Ʊ����Ϣ��
		INNER JOIN	RWA_DEV.NCM_BUSINESS_CONTRACT T6											--
		ON					T1.OBJECTNO = T6.SERIALNO
		AND					T6.BUSINESSSUBTYPE = '003050' 												--����Ͷ�ʹ�����-Ʊ���ʹ�ҵ��
		AND					T6.DATANO = p_data_dt_str
		INNER JOIN	RWA_DEV.RWA_TZ_CONTRACT T7
		ON					T7.SCONTRACTID = T6.SERIALNO
		AND					T7.BUSINESSTYPEID IN ('1040105060','1040105061','1040105062')	--Ӧ�տ�Ͷ��ҵ��
		LEFT JOIN 	RWA_DEV.NCM_CUSTOMER_INFO T3
    ON 					T1.ACCEPTORID = T3.CUSTOMERID
    AND 				T3.DATANO=P_DATA_DT_STR
    LEFT JOIN 	RWA.RWA_WP_COUNTRYRATING T5
    ON 					T3.COUNTRYCODE = T5.COUNTRYCODE
    AND					T5.ISINUSE = '1'
		WHERE 			T1.OBJECTTYPE = 'BusinessContract'										--Ӧ�տ�Ͷ��ҵ��
		AND					T1.DATANO = p_data_dt_str
		;

    COMMIT;

    --2.5 ����ϵͳ-ծȯͶ��-���һ���-��¼
    INSERT INTO RWA_DEV.RWA_TZ_COLLATERAL(
                 DataDate                              --��������
                ,DataNo                                --������ˮ��
                ,CollateralID                       	 --����ѺƷID
                ,SSysID                             	 --ԴϵͳID
                ,SGuarContractID                    	 --Դ������ͬID
                ,SCollateralID                      	 --Դ����ѺƷID
                ,CollateralName                     	 --����ѺƷ����
                ,IssuerID                           	 --������ID
                ,ProviderID                         	 --�ṩ��ID
                ,CreditRiskDataType                 	 --���÷�����������
                ,GuaranteeWay                       	 --������ʽ
                ,SourceColType                      	 --Դ����ѺƷ����
                ,SourceColSubType                   	 --Դ����ѺƷС��
                ,SpecPurpBondFlag                   	 --�Ƿ�Ϊ�չ��������в�����������е�ծȯ
                ,QualFlagSTD                        	 --Ȩ�ط��ϸ��ʶ
                ,QualFlagFIRB                 				 --�����������ϸ��ʶ
                ,CollateralTypeSTD                  	 --Ȩ�ط�����ѺƷ����
                ,CollateralSdvsSTD                  	 --Ȩ�ط�����ѺƷϸ��
                ,CollateralTypeIRB                  	 --����������ѺƷ����
                ,CollateralAmount                   	 --��Ѻ�ܶ�
                ,Currency                           	 --����
                ,StartDate                          	 --��ʼ����
                ,DueDate                            	 --��������
                ,OriginalMaturity                   	 --ԭʼ����
                ,ResidualM                          	 --ʣ������
                ,InteHaircutsFlag                   	 --���й����ۿ�ϵ����ʶ
                ,InternalHc                         	 --�ڲ��ۿ�ϵ��
                ,FCType                             	 --������ѺƷ����
                ,ABSFlag                            	 --�ʲ�֤ȯ����ʶ
                ,RatingDurationType                 	 --������������
                ,FCIssueRating     										 --������ѺƷ���еȼ�
                ,FCIssuerType                          --������ѺƷ���������
                ,FCIssuerState                         --������ѺƷ������ע�����
                ,FCResidualM                           --������ѺƷʣ������
                ,RevaFrequency                         --�ع�Ƶ��
                ,GroupID                               --������
                ,RCERating														 --�����˾���ע����ⲿ����
    )
    SELECT
                TO_DATE(p_data_dt_str,'YYYYMMDD')            AS DataDate                 --��������
                ,p_data_dt_str													     AS DataNo                   --������ˮ��
                ,T1.MITIGATIONID														 AS CollateralID           	 --����ѺƷID
                ,'TZHBJJ'              											 AS SSysID              	 	 --ԴϵͳID
                ,''																					 AS SGuarContractID        	 --Դ������ͬID                             Ĭ�� NULL
                ,T1.BOND_ID																	 AS SCollateralID          	 --Դ����ѺƷID
                ,T4.ITEMNAME                   			 		 		 AS CollateralName         	 --����ѺƷ����
                ,CASE WHEN T1.GUARANTYTYPE LIKE '001003001%' THEN 'ZGZYZF'
                 ELSE T1.CUSTID1
                 END							                           AS IssuerID             	 	 --������ID                                 Ĭ�� NULL
                ,T3.CLIENTID	                           	 	 AS ProviderID             	 --�ṩ��ID
                ,'01'                                  			 AS CreditRiskDataType     	 --���÷�����������                         Ĭ�� һ�������(01)
                ,'060'                           		 				 AS GuaranteeWay           	 --������ʽ                                 Ĭ�� ��Ѻ(060)
                ,SUBSTR(T1.GUARANTYTYPE,1,6)         				 AS SourceColType     	     --Դ����ѺƷ����                        		Ĭ�� Ʊ��(001004)
                ,T1.GUARANTYTYPE											       AS SourceColSubType         --Դ����ѺƷС��
                ,CASE WHEN T1.BONDISSUEINTENT = '01' THEN '1'
                 ELSE '0'
                 END														             AS SpecPurpBondFlag  			 --�Ƿ�Ϊ�չ��������в�����������е�ծȯ		Ĭ�� ��(0)
                ,''             									 			 		 AS QualFlagSTD            	 --Ȩ�ط��ϸ��ʶ                           Ĭ�� ��(1)
                ,''		                           			 			 AS QualFlagFIRB           	 --�����������ϸ��ʶ                       Ĭ�� ��(1)
                ,''							              				 			 AS CollateralTypeSTD 			 --Ȩ�ط�����ѺƷ����                    		Ĭ�� ��֤��(030103)
                ,''			                          					 AS CollateralSdvsSTD 		 	 --Ȩ�ط�����ѺƷϸ��                    		Ĭ�� �ֽ����ʲ�(01)
                ,''								 		 									 		 AS CollateralTypeIRB      	 --����������ѺƷ����                       Ĭ�� ������ѺƷ(030201)
                ,ROUND(TO_NUMBER(REPLACE(NVL(T1.GUARANTYSUM,'0'),',','')),6)
                										 				 								 AS CollateralAmount     	 	 --��Ѻ�ܶ�
                ,NVL(T1.GUARANTYCURRENCYCODE,'CNY')					 AS Currency               	 --����
                ,T3.STARTDATE																 AS	STARTDATE            		 --��ʼ����
								,T3.DUEDATE																	 AS	DUEDATE              		 --��������
								,T3.ORIGINALMATURITY												 AS ORIGINALMATURITY   	 		 --ԭʼ����
								,T3.RESIDUALM																 AS	RESIDUALM            		 --ʣ������
                ,'0'                                         AS InteHaircutsFlag    	 	 --���й����ۿ�ϵ����ʶ                     Ĭ�� ��(0)
                ,NULL                                        AS InternalHc          	 	 --�ڲ��ۿ�ϵ��                             Ĭ�� NULL
                ,''	                                         AS FCType                 	 --������ѺƷ����                           Ĭ�� NULL
                ,'0'                                         AS ABSFlag             	 	 --�ʲ�֤ȯ����ʶ
                ,''                                          AS RatingDurationType  	 	 --������������                             Ĭ�� NULL
                ,''                                          AS FCIssueRating     			 --������ѺƷ���еȼ�                    		Ĭ�� NULL
                ,CASE WHEN T1.GUARANTORCATEGORY LIKE '01%' THEN '01'
                 ELSE '02'
                 END                                         AS FCIssuerType             --������ѺƷ���������                  		Ĭ�� ����(02)
                ,CASE WHEN T1.GUARANTORCOUNTRYCODE = 'CHN' THEN '01'
                 ELSE '02'
                 END					                            	 AS FCIssuerState            --������ѺƷ������ע�����              		Ĭ�� NULL
                ,T3.RESIDUALM                                AS FCResidualM              --������ѺƷʣ������
                ,1                                           AS RevaFrequency            --�ع�Ƶ��                              		Ĭ�� 1
                ,''                                          AS GroupID                  --������                              		Ĭ�� NULL
                ,T5.RATINGRESULT														 AS RCERating								 --�����˾���ע����ⲿ����

    FROM				RWA.RWA_WS_BONDTRADE_MF T1	 																--���һ���ծȯͶ�ʲ�¼��
		INNER JOIN  RWA.RWA_WP_DATASUPPLEMENT T2
    ON          T1.SUPPORGID = T2.ORGID
    AND         T2.DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
    AND         T2.SUPPTMPLID = 'M-0071'
    AND         T2.SUBMITFLAG = '1'
    LEFT JOIN		RWA_DEV.RWA_TZ_EXPOSURE T3
    ON					T1.BOND_ID = T3.DUEID
    LEFT JOIN		RWA.CODE_LIBRARY T4
    ON					T1.GUARANTYTYPE = T4.ITEMNO
    AND					T4.CODENO = 'GuarantyList0071'
    LEFT JOIN		RWA.RWA_WP_COUNTRYRATING T5
    ON 					T1.GUARANTORCOUNTRYCODE = T5.COUNTRYCODE
    AND					T5.ISINUSE = '1'
		WHERE 			T1.GUARANTYTYPE NOT IN ('004001004001','004001005001','004001006001','004001006002','010')
		AND					T1.DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
		;

    COMMIT;

    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_TZ_COLLATERAL',cascade => true);

    /*Ŀ�������ͳ��*/
    --ͳ�Ʋ���ļ�¼��
    SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_TZ_COLLATERAL;
    --Dbms_output.Put_line('RWA_DEV.RWA_TZ_COLLATERAL��ǰ����Ĳ���ϵͳ-Ӧ�տ�Ͷ�����ݼ�¼Ϊ: ' || v_count || ' ��');




    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̽��� ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    p_po_rtncode := '1';
	  p_po_rtnmsg  := '�ɹ�' || '-' || v_count;
		--�����쳣
		EXCEPTION
    WHEN OTHERS THEN
				 --Dbms_output.Put_line('������,�������Ϊ:'||sqlcode||';������ϢΪ:'||sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace);
       	 ROLLBACK;
   			 p_po_rtncode := sqlcode;
   			 p_po_rtnmsg  := 'Ͷ�������ѺƷ('|| v_pro_name ||')ETLת��ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_TZ_COLLATERAL;
/

