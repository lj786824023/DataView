CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_TZ_CMRELEVENCE(
			 											p_data_dt_str	IN	VARCHAR2,		--�������� yyyyMMdd
       											p_po_rtncode	OUT	VARCHAR2,		--���ر�� 1 �ɹ�,0 ʧ��
														p_po_rtnmsg		OUT	VARCHAR2		--��������
				)
  /*
    �洢��������:RWA_DEV.PRO_RWA_TZ_CMRELEVENCE
    ʵ�ֹ���:����ϵͳ-Ͷ��-��ͬ���������(������Դ����ϵͳ��Ӧ�տ�Ͷ�������Ϣȫ������RWAͶ����ӿڱ��ͬ�������������)
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
    Ŀ���1 :RWA_DEV.RWA_TZ_CMRELEVENCE|����ϵͳͶ�����ͬ�����������
    �����¼(�޸���|�޸�ʱ��|�޸�����):
    */
  AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_TZ_CMRELEVENCE';
  --�����쳣����
  v_raise EXCEPTION;
  --���嵱ǰ����ļ�¼��
  v_count INTEGER;

  BEGIN
    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*�����ȫ�����ݼ��������Ŀ���*/
    --1.���Ŀ����е�ԭ�м�¼
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_TZ_CMRELEVENCE';

    --2.���������������ݴ�Դ����뵽Ŀ�����
    --2.1����ϵͳ-Ӧ�տ�Ͷ��-����ѺƷ-�ǻ�������-��ͬ
    INSERT INTO RWA_DEV.RWA_TZ_CMRELEVENCE(
                DATADATE                               --��������
                ,DATANO                                --������ˮ��
                ,CONTRACTID                       	 	 --��ͬID
                ,MITIGATIONID                     	 	 --������ID
                ,MITIGCATEGORY                    	 	 --����������
                ,SGUARCONTRACTID                  	 	 --Դ������ͬID
                ,GROUPID                          	 	 --������
    )
    WITH TMP_BND_CONTRACT AS (
							SELECT --DISTINCT
										 T1.CONTRACTID				AS BOND_ID
										,T1.SCONTRACTID 			AS CONTRACTNO
								FROM RWA_DEV.RWA_TZ_CONTRACT T1	 																--Ͷ�ʺ�ͬ��
					INNER JOIN RWA_DEV.NCM_BUSINESS_CONTRACT TC														--�Ŵ���ͬ��
									ON T1.SCONTRACTID = TC.SERIALNO
								 AND (TC.BUSINESSSUBTYPE NOT LIKE '0010%' OR TC.BUSINESSSUBTYPE IS NULL) 			--�ǻ�������
								 AND TC.DATANO = T1.DATANO
	    				 WHERE T1.BUSINESSTYPEID IN ('1040105060','1040105061','1040105062')	--Ӧ�տ�Ͷ��ҵ��
		)
		, TMP_BND_GUARANTEE AS (
							SELECT DISTINCT
										 T1.BOND_ID					AS CONTRACTNO
										,T5.GUARANTYID			AS GUARANTYID
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
    									) T4													--�Ŵ�������ͬ�����ѺƷ������
    							ON T3.SERIALNO = T4.CONTRACTNO
    			INNER JOIN RWA_DEV.NCM_GUARANTY_INFO T5															--�Ŵ�����ѺƷ��Ϣ��
    							ON T4.GUARANTYID = T5.GUARANTYID
    						 AND T5.GUARANTYTYPEID NOT IN ('004001004001','004001005001','004001006001','004001006002','001001003001')   --����֤����������֤�������Ա������������Ա�������Ϊ��֤����֤��ȡ
    						 AND T5.CLRSTATUS = '01'																			--ѺƷʵ��״̬������
    						 AND T5.CLRGNTSTATUS IN ('03','10')														--ѺƷ��Ѻ״̬��03-��ȷ��ѺȨ��10-�����
    						 AND T5.AFFIRMVALUE0 > 0
    						 AND T5.DATANO = p_data_dt_str
		)
    SELECT      --DISTINCT
                TO_DATE(p_data_dt_str,'YYYYMMDD')            AS DATADATE                 --��������
                ,p_data_dt_str													     AS DATANO                   --������ˮ��
                ,T1.CONTRACTNO						                   AS CONTRACTID               --��ͬID
                ,'YP' || T1.GUARANTYID											 AS MITIGATIONID     	 	 	 	 --������ID
                ,'03'																				 AS MITIGCATEGORY            --����������                 Ĭ�� ����ѺƷ(03)
                ,''																					 AS SGUARCONTRACTID          --Դ������ͬID
                ,''                         				 				 AS GROUPID                	 --������

    FROM				TMP_BND_GUARANTEE T1
		;

    COMMIT;

    --2.2����ϵͳ-Ӧ�տ�Ͷ��-����ѺƷ-�ǻ�������-����
    INSERT INTO RWA_DEV.RWA_TZ_CMRELEVENCE(
                DATADATE                               --��������
                ,DATANO                                --������ˮ��
                ,CONTRACTID                       	 	 --��ͬID
                ,MITIGATIONID                     	 	 --������ID
                ,MITIGCATEGORY                    	 	 --����������
                ,SGUARCONTRACTID                  	 	 --Դ������ͬID
                ,GROUPID                          	 	 --������
    )
    WITH TMP_BND_GUARANTEE AS (
							SELECT DISTINCT
										 T1.CONTRACTID				AS CONTRACTNO
										,GI.GUARANTYID 				AS GUARANTYID
								FROM RWA_DEV.RWA_TZ_CONTRACT T1	 																--Ͷ�ʺ�ͬ��
					INNER JOIN RWA_DEV.NCM_BUSINESS_CONTRACT TC														--�Ŵ���ͬ��
									ON T1.SCONTRACTID = TC.SERIALNO
								 AND (TC.BUSINESSSUBTYPE NOT LIKE '0010%' OR TC.BUSINESSSUBTYPE IS NULL) 			--�ǻ�������
								 AND TC.DATANO = T1.DATANO
					INNER JOIN RWA_DEV.NCM_BUSINESS_PUTOUT TP											--���˱�
									ON TC.SERIALNO = TP.CONTRACTSERIALNO
								 AND TP.DATANO = p_data_dt_str
					INNER JOIN RWA_DEV.NCM_GUARANTY_RELATIVE TG										--ѺƷ������
									ON TP.SERIALNO = TG.OBJECTNO
								 AND TG.OBJECTTYPE = 'PutOutApply'
								 AND TG.DATANO = p_data_dt_str
					INNER JOIN RWA_DEV.NCM_GUARANTY_INFO GI												--ѺƷ��Ϣ��
									ON TG.GUARANTYID = GI.GUARANTYID
								 AND GI.GUARANTYTYPEID NOT IN ('004001004001','004001005001','004001006001','004001006002','001001003001')   --����֤����������֤�������Ա������������Ա�������Ϊ��֤����֤��ȡ
				    		 AND GI.CLRSTATUS = '01'																			--ѺƷʵ��״̬������
				    		 AND GI.CLRGNTSTATUS IN ('03','10')														--ѺƷ��Ѻ״̬��03-��ȷ��ѺȨ��10-�����
				    		 AND GI.AFFIRMVALUE0 > 0
				    		 AND GI.DATANO = p_data_dt_str
	    				 WHERE T1.BUSINESSTYPEID IN ('1040105060','1040105061','1040105062')	--Ӧ�տ�Ͷ��ҵ��
		)
    SELECT      --DISTINCT
                TO_DATE(p_data_dt_str,'YYYYMMDD')            AS DATADATE                 --��������
                ,p_data_dt_str													     AS DATANO                   --������ˮ��
                ,T1.CONTRACTNO						                   AS CONTRACTID               --��ͬID
                ,'YP' || T1.GUARANTYID											 AS MITIGATIONID     	 	 	 	 --������ID
                ,'03'																				 AS MITIGCATEGORY            --����������                 Ĭ�� ����ѺƷ(03)
                ,''																					 AS SGUARCONTRACTID          --Դ������ͬID
                ,''                         				 				 AS GROUPID                	 --������

    FROM				TMP_BND_GUARANTEE T1
    WHERE NOT EXISTS (SELECT 1 FROM RWA_TZ_CMRELEVENCE T2 WHERE T1.CONTRACTNO = T2.CONTRACTID AND 'YP' || T1.GUARANTYID = T2.MITIGATIONID)
		;

    COMMIT;

    --2.3����ϵͳ-Ӧ�տ�Ͷ��-��֤(����֤����������֤�������Ա������������Ա���)-�ǻ�������-��ͬ
    INSERT INTO RWA_DEV.RWA_TZ_CMRELEVENCE(
                DATADATE                               --��������
                ,DATANO                                --������ˮ��
                ,CONTRACTID                       	 	 --��ͬID
                ,MITIGATIONID                     	 	 --������ID
                ,MITIGCATEGORY                    	 	 --����������
                ,SGUARCONTRACTID                  	 	 --Դ������ͬID
                ,GROUPID                          	 	 --������
    )
    WITH TMP_BND_CONTRACT AS (
							SELECT --DISTINCT
										 T1.CONTRACTID				AS BOND_ID
										,T1.SCONTRACTID 			AS CONTRACTNO
								FROM RWA_DEV.RWA_TZ_CONTRACT T1	 																--Ͷ�ʺ�ͬ��
					INNER JOIN RWA_DEV.NCM_BUSINESS_CONTRACT TC														--�Ŵ���ͬ��
									ON T1.SCONTRACTID = TC.SERIALNO
								 AND (TC.BUSINESSSUBTYPE NOT LIKE '0010%' OR TC.BUSINESSSUBTYPE IS NULL) 			--�ǻ�������
								 AND TC.DATANO = T1.DATANO
	    				 WHERE T1.BUSINESSTYPEID IN ('1040105060','1040105061','1040105062')	--Ӧ�տ�Ͷ��ҵ��
		)
		, TMP_BND_GUARANTEE AS (
							SELECT DISTINCT
										 T1.BOND_ID					AS CONTRACTNO
										,T5.GUARANTYID			AS GUARANTYID
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
    									) T4													--�Ŵ�������ͬ�����ѺƷ������
    							ON T3.SERIALNO = T4.CONTRACTNO
    			INNER JOIN RWA_DEV.NCM_GUARANTY_INFO T5															--�Ŵ�����ѺƷ��Ϣ��
    							ON T4.GUARANTYID = T5.GUARANTYID
    						 AND T5.GUARANTYTYPEID IN ('004001004001','004001005001','004001006001','004001006002')     					--����֤����������֤�������Ա������������Ա�������Ϊ��֤
    						 AND T5.CLRSTATUS = '01'																			--ѺƷʵ��״̬������
    						 AND T5.CLRGNTSTATUS IN ('03','10')														--ѺƷ��Ѻ״̬��03-��ȷ��ѺȨ��10-�����
    						 AND T5.AFFIRMVALUE0 > 0
    						 AND T5.DATANO = p_data_dt_str
		)
    SELECT      --DISTINCT
                TO_DATE(p_data_dt_str,'YYYYMMDD')            AS DATADATE                 --��������
                ,p_data_dt_str													     AS DATANO                   --������ˮ��
                ,T1.CONTRACTNO						                   AS CONTRACTID               --��ͬID
                ,'YP' || T1.GUARANTYID											 AS MITIGATIONID     	 	 	 	 --������ID
                ,'02'																				 AS MITIGCATEGORY            --����������                 Ĭ�� ��֤(02)
                ,''																					 AS SGUARCONTRACTID          --Դ������ͬID
                ,''                         				 				 AS GROUPID                	 --������

    FROM				TMP_BND_GUARANTEE T1
		;

    COMMIT;

    --2.4����ϵͳ-Ӧ�տ�Ͷ��-��֤(����֤����������֤�������Ա������������Ա���)-�ǻ�������-����
    INSERT INTO RWA_DEV.RWA_TZ_CMRELEVENCE(
                DATADATE                               --��������
                ,DATANO                                --������ˮ��
                ,CONTRACTID                       	 	 --��ͬID
                ,MITIGATIONID                     	 	 --������ID
                ,MITIGCATEGORY                    	 	 --����������
                ,SGUARCONTRACTID                  	 	 --Դ������ͬID
                ,GROUPID                          	 	 --������
    )
    WITH TMP_BND_GUARANTEE AS (
							SELECT DISTINCT
										 T1.CONTRACTID				AS CONTRACTNO
										,GI.GUARANTYID 				AS GUARANTYID
								FROM RWA_DEV.RWA_TZ_CONTRACT T1	 																--Ͷ�ʺ�ͬ��
					INNER JOIN RWA_DEV.NCM_BUSINESS_CONTRACT TC														--�Ŵ���ͬ��
									ON T1.SCONTRACTID = TC.SERIALNO
								 AND (TC.BUSINESSSUBTYPE NOT LIKE '0010%' OR TC.BUSINESSSUBTYPE IS NULL) 			--�ǻ�������
								 AND TC.DATANO = p_data_dt_str
					INNER JOIN RWA_DEV.NCM_BUSINESS_PUTOUT TP															--���˱�
									ON T1.SCONTRACTID = TP.CONTRACTSERIALNO
								 AND TP.DATANO = p_data_dt_str
					INNER JOIN RWA_DEV.NCM_GUARANTY_RELATIVE TG														--ѺƷ������
									ON TP.SERIALNO = TG.OBJECTNO
								 AND TG.OBJECTTYPE = 'PutOutApply'
								 AND TG.DATANO = p_data_dt_str
					INNER JOIN RWA_DEV.NCM_GUARANTY_INFO GI																--ѺƷ��Ϣ��
									ON TG.GUARANTYID = GI.GUARANTYID
								 AND GI.GUARANTYTYPEID IN ('004001004001','004001005001','004001006001','004001006002')     					--����֤����������֤�������Ա������������Ա�������Ϊ��֤
				    		 AND GI.CLRSTATUS = '01'																				--ѺƷʵ��״̬������
				    		 AND GI.CLRGNTSTATUS IN ('03','10')															--ѺƷ��Ѻ״̬��03-��ȷ��ѺȨ��10-�����
				    		 AND GI.AFFIRMVALUE0 > 0
				    		 AND GI.DATANO = p_data_dt_str
	    				 WHERE T1.BUSINESSTYPEID IN ('1040105060','1040105061','1040105062')	--Ӧ�տ�Ͷ��ҵ��
		)
    SELECT      --DISTINCT
                TO_DATE(p_data_dt_str,'YYYYMMDD')            AS DATADATE                 --��������
                ,p_data_dt_str													     AS DATANO                   --������ˮ��
                ,T1.CONTRACTNO						                   AS CONTRACTID               --��ͬID
                ,'YP' || T1.GUARANTYID											 AS MITIGATIONID     	 	 	 	 --������ID
                ,'02'																				 AS MITIGCATEGORY            --����������                 Ĭ�� ��֤(02)
                ,''																					 AS SGUARCONTRACTID          --Դ������ͬID
                ,''                         				 				 AS GROUPID                	 --������

    FROM				TMP_BND_GUARANTEE T1
    WHERE NOT EXISTS (SELECT 1 FROM RWA_TZ_CMRELEVENCE T2 WHERE T1.CONTRACTNO = T2.CONTRACTID AND 'YP' || T1.GUARANTYID = T2.MITIGATIONID)
		;

    COMMIT;


    --2.5����ϵͳ-Ӧ�տ�Ͷ��-��֤-�ǻ�������-��ͬ
    INSERT INTO RWA_DEV.RWA_TZ_CMRELEVENCE(
                DATADATE                               --��������
                ,DATANO                                --������ˮ��
                ,CONTRACTID                       	 	 --��ͬID
                ,MITIGATIONID                     	 	 --������ID
                ,MITIGCATEGORY                    	 	 --����������
                ,SGUARCONTRACTID                  	 	 --Դ������ͬID
                ,GROUPID                          	 	 --������
    )
    SELECT      DISTINCT
                TO_DATE(p_data_dt_str,'YYYYMMDD')            AS DATADATE                 --��������
                ,p_data_dt_str													     AS DATANO                   --������ˮ��
                ,T1.CONTRACTID						                   AS CONTRACTID               --��ͬID
                ,'BZ' || T5.SERIALNO												 AS MITIGATIONID     	 	 	 	 --������ID
                ,'02'																				 AS MITIGCATEGORY            --����������                 Ĭ�� ��֤(02)
                ,''																					 AS SGUARCONTRACTID          --Դ������ͬID
                ,''                         				 				 AS GROUPID                	 --������

    FROM				RWA_DEV.RWA_TZ_CONTRACT T1	 															--Ͷ�ʺ�ͬ��
		INNER JOIN	RWA_DEV.NCM_BUSINESS_CONTRACT TC													--�Ŵ���ͬ��
		ON 					T1.SCONTRACTID = TC.SERIALNO
		AND 				(TC.BUSINESSSUBTYPE NOT LIKE '0010%' OR TC.BUSINESSSUBTYPE IS NULL) 			--�ǻ�������
		AND 				TC.DATANO = p_data_dt_str
		INNER JOIN  (SELECT  DISTINCT
												 SERIALNO
												,OBJECTNO
										FROM RWA_DEV.NCM_CONTRACT_RELATIVE
									 WHERE OBJECTTYPE = 'GuarantyContract'
										 AND DATANO = p_data_dt_str) T4                       --�Ŵ���ͬ������
    ON          TC.SERIALNO = T4.SERIALNO
    INNER JOIN	RWA_DEV.NCM_GUARANTY_CONTRACT T5													--�Ŵ�������ͬ��
    ON					T4.OBJECTNO = T5.SERIALNO
    AND					T5.GUARANTYTYPE = '010'																		--��֤
    AND					T5.GUARANTYVALUE > 0
    AND					T5.DATANO = p_data_dt_str
		WHERE 			T1.BUSINESSTYPEID IN ('1040105060','1040105061','1040105062')	--Ӧ�տ�Ͷ��ҵ��
		;

    COMMIT;

    --2.6����ϵͳ-Ӧ�տ�Ͷ��-��֤-�ǻ�������-����
    INSERT INTO RWA_DEV.RWA_TZ_CMRELEVENCE(
                DATADATE                               --��������
                ,DATANO                                --������ˮ��
                ,CONTRACTID                       	 	 --��ͬID
                ,MITIGATIONID                     	 	 --������ID
                ,MITIGCATEGORY                    	 	 --����������
                ,SGUARCONTRACTID                  	 	 --Դ������ͬID
                ,GROUPID                          	 	 --������
    )
    SELECT      DISTINCT
                TO_DATE(p_data_dt_str,'YYYYMMDD')            AS DATADATE                 --��������
                ,p_data_dt_str													     AS DATANO                   --������ˮ��
                ,T1.CONTRACTID						                   AS CONTRACTID               --��ͬID
                ,'BZ' || T5.SERIALNO												 AS MITIGATIONID     	 	 	 	 --������ID
                ,'02'																				 AS MITIGCATEGORY            --����������                 Ĭ�� ��֤(02)
                ,''																					 AS SGUARCONTRACTID          --Դ������ͬID
                ,''                         				 				 AS GROUPID                	 --������

    FROM				RWA_DEV.RWA_TZ_CONTRACT T1	 															--Ͷ�ʺ�ͬ��
		INNER JOIN	RWA_DEV.NCM_BUSINESS_CONTRACT TC													--�Ŵ���ͬ��
		ON 					T1.SCONTRACTID = TC.SERIALNO
		AND 				(TC.BUSINESSSUBTYPE NOT LIKE '0010%' OR TC.BUSINESSSUBTYPE IS NULL) 			--�ǻ�������
		AND 				TC.DATANO = p_data_dt_str
		INNER JOIN	RWA_DEV.NCM_BUSINESS_PUTOUT T3														--���˱�
		ON					TC.SERIALNO = T3.CONTRACTSERIALNO
		AND					T3.DATANO = p_data_dt_str
		INNER JOIN	RWA_DEV.NCM_GUARANTY_RELATIVE T4													--ѺƷ������
		ON					T3.SERIALNO = T4.OBJECTNO
		AND					T4.OBJECTTYPE = 'PutOutApply'
		AND					T4.DATANO = p_data_dt_str
    INNER JOIN	RWA_DEV.NCM_GUARANTY_CONTRACT T5													--�Ŵ�������ͬ��
    ON					T4.CONTRACTNO = T5.SERIALNO
    AND					T5.GUARANTYTYPE = '010'																		--��֤
    AND					T5.GUARANTYVALUE > 0
    AND					T5.DATANO = p_data_dt_str
		WHERE 			T1.BUSINESSTYPEID IN ('1040105060','1040105061','1040105062')	--Ӧ�տ�Ͷ��ҵ��
		AND NOT EXISTS (SELECT 1 FROM RWA_TZ_CMRELEVENCE T2 WHERE T1.CONTRACTID = T2.CONTRACTID AND 'BZ' || T5.SERIALNO = T2.MITIGATIONID)
		AND T1.DATANO=p_data_dt_str
    ;

    COMMIT;

    --2.7����ϵͳ-Ӧ�տ�Ͷ��-Ʊ���ʹ�ҵ��-Ʊ����Ϣ
    INSERT INTO RWA_DEV.RWA_TZ_CMRELEVENCE(
                DATADATE                               --��������
                ,DATANO                                --������ˮ��
                ,CONTRACTID                       	 	 --��ͬID
                ,MITIGATIONID                     	 	 --������ID
                ,MITIGCATEGORY                    	 	 --����������
                ,SGUARCONTRACTID                  	 	 --Դ������ͬID
                ,GROUPID                          	 	 --������
    )
    SELECT      DISTINCT
                TO_DATE(p_data_dt_str,'YYYYMMDD')            AS DATADATE                 --��������
                ,p_data_dt_str													     AS DATANO                   --������ˮ��
                ,T1.CONTRACTID						                   AS CONTRACTID               --��ͬID
                ,'TZBILL' || T4.SERIALNO										 AS MITIGATIONID     	 	 	 	 --������ID
                ,'03'																				 AS MITIGCATEGORY            --����������                 Ĭ�� ����ѺƷ(03)
                ,''																					 AS SGUARCONTRACTID          --Դ������ͬID
                ,''                         				 				 AS GROUPID                	 --������

    FROM				RWA_DEV.RWA_TZ_CONTRACT T1	 															--Ͷ�ʺ�ͬ��
		INNER JOIN	RWA_DEV.NCM_BUSINESS_CONTRACT TC													--�Ŵ���ͬ��
		ON 					T1.SCONTRACTID = TC.SERIALNO
		AND 				TC.BUSINESSSUBTYPE = '003050' 														--����Ͷ�ʹ�����-Ʊ���ʹ�ҵ��
		AND 				TC.DATANO = p_data_dt_str
		INNER JOIN  RWA_DEV.NCM_BILL_INFO T4                       						--Ʊ����Ϣ��
    ON          TC.SERIALNO = T4.OBJECTNO
    AND					T4.OBJECTTYPE = 'BusinessContract'
    AND					T4.DATANO = p_data_dt_str
		WHERE 			T1.BUSINESSTYPEID IN ('1040105060','1040105061','1040105062')	--Ӧ�տ�Ͷ��ҵ��
		AND T1.DATANO=p_data_dt_str;

    COMMIT;

    --2.8����ϵͳ-ծȯͶ��-���-��֤
    INSERT INTO RWA_DEV.RWA_TZ_CMRELEVENCE(
                DATADATE                               --��������
                ,DATANO                                --������ˮ��
                ,CONTRACTID                       	 	 --��ͬID
                ,MITIGATIONID                     	 	 --������ID
                ,MITIGCATEGORY                    	 	 --����������
                ,SGUARCONTRACTID                  	 	 --Դ������ͬID
                ,GROUPID                          	 	 --������
    )
    SELECT      --DISTINCT
                TO_DATE(p_data_dt_str,'YYYYMMDD')            AS DATADATE                 --��������
                ,p_data_dt_str													     AS DATANO                   --������ˮ��
                ,T1.CONTRACTID						                   AS CONTRACTID               --��ͬID
                ,'TZBOND' || T5.SERIALNO										 AS MITIGATIONID     	 	 	 	 --������ID
                ,'02'																				 AS MITIGCATEGORY            --����������                 Ĭ�� ��֤(02)
                ,''																					 AS SGUARCONTRACTID          --Դ������ͬID
                ,''                         				 				 AS GROUPID                	 --������

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
	  WHERE 			T1.BUSINESSTYPEID IN ('1040202010','1040202011')					--���ծȯͶ��ҵ��
		AND T1.DATANO=p_data_dt_str
    ;

    COMMIT;

    --2.8����ϵͳ-Ӧ�տ�Ͷ��-����ѺƷ(��֤��)
    /*
    INSERT INTO RWA_DEV.RWA_TZ_CMRELEVENCE(
                DATADATE                               --��������
                ,DATANO                                --������ˮ��
                ,CONTRACTID                       	 	 --��ͬID
                ,MITIGATIONID                     	 	 --������ID
                ,MITIGCATEGORY                    	 	 --����������
                ,SGUARCONTRACTID                  	 	 --Դ������ͬID
                ,GROUPID                          	 	 --������
    )
    SELECT      --DISTINCT
                TO_DATE(p_data_dt_str,'YYYYMMDD')            AS DATADATE                 --��������
                ,p_data_dt_str													     AS DATANO                   --������ˮ��
                ,T1.CONTRACTID						                   AS CONTRACTID               --��ͬID
                ,T1.SCONTRACTID												 			 AS MITIGATIONID     	 	 	 	 --������ID
                ,'03'																				 AS MITIGCATEGORY            --����������                 Ĭ�� ����ѺƷ(03)
                ,''																					 AS SGUARCONTRACTID          --Դ������ͬID
                ,''                         				 				 AS GROUPID                	 --������

    FROM				RWA_DEV.RWA_TZ_CONTRACT T1	 															--Ͷ�ʺ�ͬ��
		INNER JOIN	(SELECT  SERIALNO
										FROM RWA_DEV.NCM_BUSINESS_CONTRACT
									 WHERE BUSINESSTYPE = '1040105060'											--Ӧ�տ�Ͷ��ҵ��
									 	 AND BAILSUM > 0																			--��֤�����0
									 	 AND DATANO = p_data_dt_str) T4
    ON          T1.SCONTRACTID = T4.SERIALNO
		WHERE 			T1.BUSINESSTYPEID IN ('1040105060','1040105061','1040105062')	--Ӧ�տ�Ͷ��ҵ��
		;
		*/
		--2.8����ϵͳ-Ӧ�տ�Ͷ��-����ѺƷ(��֤��)
		INSERT INTO RWA_DEV.RWA_TZ_CMRELEVENCE(
                DATADATE                               --��������
                ,DATANO                                --������ˮ��
                ,CONTRACTID                       	 	 --��ͬID
                ,MITIGATIONID                     	 	 --������ID
                ,MITIGCATEGORY                    	 	 --����������
                ,SGUARCONTRACTID                  	 	 --Դ������ͬID
                ,GROUPID                          	 	 --������
    )
    SELECT      --DISTINCT
                TO_DATE(p_data_dt_str,'YYYYMMDD')            AS DATADATE                 --��������
                ,p_data_dt_str													     AS DATANO                   --������ˮ��
                ,T1.CONTRACTID						                   AS CONTRACTID               --��ͬID
                ,'HT' || T1.SCONTRACTID || T4.BAILCURRENCY
                																						 AS MITIGATIONID     	 	 	 	 --������ID
                ,'03'																				 AS MITIGCATEGORY            --����������                 Ĭ�� ����ѺƷ(03)
                ,''																					 AS SGUARCONTRACTID          --Դ������ͬID
                ,''                         				 				 AS GROUPID                	 --������

    FROM				RWA_DEV.RWA_TZ_CONTRACT T1	 															--Ͷ�ʺ�ͬ��
		INNER JOIN	RWA_DEV.RWA_TEMP_BAIL2 T4
    ON          T1.SCONTRACTID = T4.CONTRACTNO
    AND					T4.ISMAX = '1'																						--ȡ��ͬ��ͬ������һ����Ϊ���
		WHERE 			T1.BUSINESSTYPEID IN ('1040105060','1040105061','1040105062')	--Ӧ�տ�Ͷ��ҵ��
		AND T1.DATANO=p_data_dt_str
    ;

    COMMIT;

    --2.9����ϵͳ-ծȯͶ��-���һ���-��¼
    --2.9.1 ����ѺƷID
    UPDATE RWA.RWA_WS_BONDTRADE_MF T
		   SET T.MITIGATIONID =
		       (WITH TMP_BOND AS (SELECT BOND_ID,SUPPSERIALNO, p_data_dt_str || 'TZHBJJ' || lpad(rownum, 4, '0') AS MITIGATIONID
															  FROM (SELECT BOND_ID,SUPPSERIALNO
															          FROM RWA.RWA_WS_BONDTRADE_MF
															         WHERE DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD')
															         	 AND GUARANTYTYPE IS NOT NULL
															         ORDER BY BOND_ID,SUPPSERIALNO))
		         SELECT T1.MITIGATIONID
		           FROM TMP_BOND T1
		          WHERE T1.BOND_ID = T.BOND_ID
		          	AND T1.SUPPSERIALNO = T.SUPPSERIALNO)
		          WHERE T.DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD')
		;

		COMMIT;

    --2.9.2 ���뻺�͹�ϵ
    INSERT INTO RWA_DEV.RWA_TZ_CMRELEVENCE(
                DATADATE                               --��������
                ,DATANO                                --������ˮ��
                ,CONTRACTID                       	 	 --��ͬID
                ,MITIGATIONID                     	 	 --������ID
                ,MITIGCATEGORY                    	 	 --����������
                ,SGUARCONTRACTID                  	 	 --Դ������ͬID
                ,GROUPID                          	 	 --������
    )
    SELECT      --DISTINCT
                TO_DATE(p_data_dt_str,'YYYYMMDD')            AS DATADATE                 --��������
                ,p_data_dt_str													     AS DATANO                   --������ˮ��
                ,T1.BOND_ID								                   AS CONTRACTID               --��ͬID
                ,T1.MITIGATIONID														 AS MITIGATIONID     	 	 	 	 --������ID
                ,CASE WHEN T1.GUARANTYTYPE IN ('004001004001','004001005001','004001006001','004001006002','010') THEN  '02'
                 ELSE '03'
                 END																				 AS MITIGCATEGORY            --����������                 Ĭ�� ����ѺƷ(03)
                ,''																					 AS SGUARCONTRACTID          --Դ������ͬID
                ,''                         				 				 AS GROUPID                	 --������

    FROM				RWA.RWA_WS_BONDTRADE_MF T1	 																--���һ���ծȯͶ�ʲ�¼��
		INNER JOIN  RWA.RWA_WP_DATASUPPLEMENT T2
    ON          T1.SUPPORGID = T2.ORGID
    AND         T2.DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
    AND         T2.SUPPTMPLID = 'M-0071'
    AND         T2.SUBMITFLAG = '1'
		WHERE 			T1.GUARANTYTYPE IS NOT NULL
		AND					T1.DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
		;

    COMMIT;

    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_TZ_CMRELEVENCE',cascade => true);

    /*Ŀ�������ͳ��*/
    --ͳ�Ʋ���ļ�¼��
    SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_TZ_CMRELEVENCE;
    --Dbms_output.Put_line('RWA_DEV.RWA_TZ_CMRELEVENCE��ǰ����Ĳ���ϵͳ-Ӧ�տ�Ͷ�����ݼ�¼Ϊ: ' || v_count || ' ��');



    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̽��� ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    p_po_rtncode := '1';
	  p_po_rtnmsg  := '�ɹ�' || '-' || v_count;
		--�����쳣
		EXCEPTION
    WHEN OTHERS THEN
				 --Dbms_output.Put_line('������,�������Ϊ:'||sqlcode||';������ϢΪ:'||sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace);
       	 ROLLBACK;
   			 p_po_rtncode := sqlcode;
   			 p_po_rtnmsg  := '��ͬ���������('|| v_pro_name ||')ETLת��ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_TZ_CMRELEVENCE;
/

