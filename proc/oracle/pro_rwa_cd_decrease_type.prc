CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_CD_DECREASE_TYPE(
														p_data_dt_str	IN	VARCHAR2,		--�������� yyyyMMdd
       											p_po_rtncode	OUT	VARCHAR2,		--���ر�� 1 �ɹ�,0 ʧ��
														p_po_rtnmsg		OUT	VARCHAR2		--��������
                                                       )
	/*
    �洢��������:RWA_DEV.PRO_RWA_CD_DECREASE_TYPE
    ʵ�ֹ���:ʵ��(UPDATE)�����ҵ��ļ�ֵ׼������ͷ�̯
    ���ݿھ�:ȫ��
    ����Ƶ��:�³�����
    ��  ��  :V1.0.0
    ��д��  :YUSJ
    ��дʱ��:2016-11-29
    ��  λ  :�Ϻ���˶��Ϣ�����ɷ����޹�˾
    Դ  ��1 :RWA_DEV.RWA_EI_EXPOSURE|���÷��ձ�¶��
    Դ  ��2 :RWA_DEV.RWA_CD_DECREASE_TYPE|׼�������������ñ�
    Դ	 ��3 :RWA_DEV.RWA_TEMP_EXPOSURE_ZBJ|���÷��ձ�¶��ϸ������׼�����Ŀ���ձ�
    Դ  ��4 :RWA_DEV.RWA_TEMP_ZBJ|����׼�����Ŀ����̯���ձ�
    Ŀ���1 :RWA_DEV.RWA_EI_EXPOSURE|���÷��ձ�¶��
    �����¼(�޸���|�޸�ʱ��|�޸�����):
    ��   ע :׼������᷽��
    				 2.�������������弶����������
      				 �弶����Ϊ��1 ������2 ��ע��3 �μ� ��4 ���ɡ�5 ��ʧ
    				 3.�弶������б�����
      				 3.1.������Ϊ1ʱ�������е�Ӧ����Ŀ�Ŀ��Ϊ������ͳ�����м�¼�����ٷֱ���ؼ�¼�����ƽ�����Լ������������ע�������Ϊ��һ��׼���𡱣���
      				 3.2.�������2��3��4��5��ֱ��ͨ���������ֱ���Զ�Ӧ�����ϵ��2%��25%��50%��100%��ע�������Ϊ��ר��׼���𡱣�
    */
	AS
	--����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

	/*��������*/
  --���嵱ǰ���µ�������
  v_count1 				INTEGER;
  --������µ�sql���
  v_update_sql 		VARCHAR2(2000);
  --�ʲ�����
  ASSET_TYPE   		VARCHAR2(500);
  --���շ���
  RISK_TYPE    		VARCHAR2(100);
  --��Ŀ��
  SUBJECT_NO   		VARCHAR2(100);
  --ҵ��Ʒ��
  BUSINESS_TYPE 	VARCHAR2(100);
  --��ҵ����
  INDUSTRY_TYPE 	VARCHAR2(100);
  --�������
  DECREASE_COUNT 	VARCHAR2(100);
  --�����쳣����
  v_raise EXCEPTION;

  BEGIN
  	--1 ׼�����ȡ
  	--1.1 �����Ŵ���ת���ֲ��ֵ�׼���𣬽�����׼�������0�Ĳ���
  	MERGE INTO (SELECT EXPOSUREID, DUEID, ASSETBALANCE, GENERALPROVISION
		              FROM RWA_DEV.RWA_EI_EXPOSURE
		             WHERE DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD')
		               AND SSYSID IN ('XD', 'PJ')) T
		USING (SELECT A1.ACCT_NO     AS ACCT_NO,
		              A1.JEBALANCE   AS JEBALANCE,
		              B.ASSETBALANCE AS ASSETBALANCE
		         FROM (SELECT CASE
		                        WHEN A.ACCT_NO LIKE '%T%' THEN
		                         SUBSTR(A.ACCT_NO, 1, INSTR(A.ACCT_NO, 'T') - 1)
		                        ELSE
		                         A.ACCT_NO
		                      END AS ACCT_NO, --�����˺��ڽ�ݺŵĻ�����׷���ˡ�T2601�����Ƶ��ַ���
		                      CASE
		                        WHEN SUM(NVL(A.ACCOUNT_CR, 0) - NVL(A.ACCOUNT_DR, 0)) < 0 THEN
		                         0
		                        ELSE
		                         SUM(NVL(A.ACCOUNT_CR, 0) - NVL(A.ACCOUNT_DR, 0))
		                      END AS JEBALANCE
		                 FROM RWA_DEV.FNS_XXT_LOAN_JE_B A
		                WHERE A.DATANO = '20170630'
		                  AND A.ACCOUNT_ACCT IN ('13040101', '13040102')
		                  AND A.ACCT_NO <> 'DTB13070800001' --ֱ������
		                  AND A.ACCT_NO NOT LIKE 'CQCBCEDACCT1%' --���ÿ�
		                  AND A.ACCT_NO NOT LIKE 'KID%' --��I��
		                  AND A.DATA_DATE <= '20170630'
		                GROUP BY CASE
		                           WHEN A.ACCT_NO LIKE '%T%' THEN
		                            SUBSTR(A.ACCT_NO, 1, INSTR(A.ACCT_NO, 'T') - 1)
		                           ELSE
		                            A.ACCT_NO
		                         END
		               HAVING SUM(NVL(A.ACCOUNT_CR, 0) - NVL(A.ACCOUNT_DR, 0)) > 0) A1
		        INNER JOIN (SELECT DUEID, SUM(ASSETBALANCE) AS ASSETBALANCE							--Ϊ��ֹ�Ӻ��Ĺ��������������ݳ�ͻ����¶��=YQ+��ݺţ�ծ��ID=��ݺš��˰���ծ��ID�����ʲ����ʲ����ռ�ȷ������������ڱ�¶��׼����
		                     FROM RWA_DEV.RWA_EI_EXPOSURE
		                    where DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD')
		                      AND SSYSID IN ('XD', 'PJ')
		                    group by DUEID) B
		           ON B.DUEID = A1.ACCT_NO) T1
		ON (T.DUEID = T1.ACCT_NO)
		WHEN MATCHED THEN
		  UPDATE
		     SET T.GENERALPROVISION = T1.JEBALANCE * T.ASSETBALANCE / T1.ASSETBALANCE
		;

		COMMIT;

		--1.2 �����Ŵ�����ҵ��ʹ�ú�ͬ����Ϊ��¶ID�����ݵ�׼����(����������֤�����)��ֱ�Ӹ���
		/*
		MERGE INTO (SELECT EXPOSUREID
											,GENERALPROVISION
									FROM RWA_DEV.RWA_EI_EXPOSURE
								 WHERE DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD')
		     					 AND SSYSID = 'XD'
		     					 AND BUSINESSTYPEID IN ('105010', '105150', '105120', '102020', '102050', '106')) T
		USING (SELECT D.RELATIVESERIALNO2 AS ACCT_NO,
		              SUM(NVL(A.ACCOUNT_CR, 0) - NVL(A.ACCOUNT_DR, 0)) AS JEBALANCE
		         FROM RWA_DEV.FNS_XXT_LOAN_JE_B A
		        INNER JOIN RWA_DEV.CMS_BUSINESS_DUEBILL D
		           ON A.ACCT_NO = D.SERIALNO
		          AND D.DATANO = p_data_dt_str
		        WHERE A.DATANO = p_data_dt_str
		          AND A.ACCOUNT_ACCT IN ('13040101', '13040102')
		          AND A.DATA_DATE <= p_data_dt_str
		        GROUP BY D.RELATIVESERIALNO2
		       HAVING SUM(NVL(A.ACCOUNT_CR, 0) - NVL(A.ACCOUNT_DR, 0)) > 0) T1
		ON (T.EXPOSUREID = T1.ACCT_NO)
		WHEN MATCHED THEN
		  UPDATE
		     SET T.GENERALPROVISION = T1.JEBALANCE
		;

		COMMIT;
		*/

		--1.3 ����ֱ�����е��׼���𣬽�����׼�������0�Ĳ���
		UPDATE RWA_DEV.RWA_EI_EXPOSURE T
		   SET T.GENERALPROVISION =
		       (SELECT CASE WHEN SUM(NVL(A.ACCOUNT_CR, 0) - NVL(A.ACCOUNT_DR, 0)) < 0 THEN 0
		       				 ELSE SUM(NVL(A.ACCOUNT_CR, 0) - NVL(A.ACCOUNT_DR, 0))
		       				 END
		          FROM RWA_DEV.FNS_XXT_LOAN_JE_B A
		         WHERE A.DATANO = p_data_dt_str
		           AND A.ACCOUNT_ACCT IN ('13040101', '13040102')
		           AND A.DATA_DATE <= p_data_dt_str
		           AND A.ACCT_NO = 'DTB13070800001')
		 WHERE T.DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD')
		   AND T.SSYSID = 'ZX'
		;

		COMMIT;

		--1.4 �������ÿ�׼����(������δʹ�ö��)��������׼�������0�Ĳ���
		MERGE INTO (SELECT RISKCLASSIFY
											,NORMALPRINCIPAL
											,GENERALPROVISION
									FROM RWA_DEV.RWA_EI_EXPOSURE
								 WHERE DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD')
		     					 AND SSYSID = 'XYK'
		     					 AND BUSINESSTYPEID = '11106010') T
		USING (SELECT T2.RISKCLASSIFY    AS RISKCLASSIFY,
		              T2.NORMALPRINCIPAL AS NORMALPRINCIPAL,
		              CASE WHEN T1.JEBALANCE < 0 THEN 0
		              ELSE T1.JEBALANCE
		              END       				 AS JEBALANCE
		         FROM (SELECT A.ACCT_NO AS ACCT_NO,
		                      SUM(NVL(A.ACCOUNT_CR, 0) - NVL(A.ACCOUNT_DR, 0)) AS JEBALANCE
		                 FROM RWA_DEV.FNS_XXT_LOAN_JE_B A
		                WHERE A.DATANO = p_data_dt_str
		                  AND A.ACCOUNT_ACCT IN ('13040101', '13040102')
		                  AND A.DATA_DATE <= p_data_dt_str
		                  AND A.ACCT_NO LIKE 'CQCBCEDACCT1%'
		                GROUP BY A.ACCT_NO) T1
		        INNER JOIN (SELECT 'CQCBCEDACCT1' || RISKCLASSIFY AS EXPOSUREID,
		                          RISKCLASSIFY,
		                          SUM(NORMALPRINCIPAL) AS NORMALPRINCIPAL
		                     FROM RWA_DEV.RWA_EI_EXPOSURE
		                    WHERE DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD')
		                      AND SSYSID = 'XYK'
		                      AND BUSINESSTYPEID = '11106010'
		                    GROUP BY RISKCLASSIFY) T2
		           ON T1.ACCT_NO = T2.EXPOSUREID) T3
		ON (T.RISKCLASSIFY = T3.RISKCLASSIFY)
		WHEN MATCHED THEN
		  UPDATE
		     SET T.GENERALPROVISION = T3.JEBALANCE * T.NORMALPRINCIPAL / T3.NORMALPRINCIPAL
		;

		COMMIT;

		--1.5 �Ŵ���I��׼���𣬽�����׼�������0�Ĳ���
		UPDATE RWA_DEV.RWA_EI_EXPOSURE T
		   SET T.GENERALPROVISION = T.NORMALPRINCIPAL *
		                            (SELECT CASE WHEN T1.JEBALANCE < 0 THEN 0 ELSE T1.JEBALANCE END / T2.NORMALPRINCIPAL
		                               FROM (SELECT SUM(NVL(A.ACCOUNT_CR, 0) -
		                                                NVL(A.ACCOUNT_DR, 0)) AS JEBALANCE
		                                       FROM RWA_DEV.FNS_XXT_LOAN_JE_B A
		                                      WHERE A.DATANO = p_data_dt_str
		                                        AND A.ACCOUNT_ACCT IN
		                                            ('13040101', '13040102')
		                                        AND A.DATA_DATE <= p_data_dt_str
		                                        AND A.ACCT_NO LIKE 'KID%') T1,
		                                    (SELECT SUM(NORMALPRINCIPAL) AS NORMALPRINCIPAL
		                                       FROM RWA_DEV.RWA_EI_EXPOSURE
		                                      WHERE DATADATE =
		                                            TO_DATE(p_data_dt_str, 'YYYYMMDD')
		                                        AND SSYSID = 'XD'
		                                        AND BUSINESSTYPEID = '11103036') T2)
		 WHERE T.DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD')
		   AND T.SSYSID = 'XD'
		   AND T.BUSINESSTYPEID = '11103036'
		;

		COMMIT;

		--�����Ŵ����ʲ����ڲ���׼��������ʲ����ļ�¼����׼��������ʲ����
		UPDATE RWA_DEV.RWA_EI_EXPOSURE T
		   SET T.GENERALPROVISION = T.ASSETBALANCE
		 WHERE T.GENERALPROVISION > T.ASSETBALANCE
		 	 AND T.SSYSID IN ('XD', 'XYK', 'PJ', 'ZX')
			 AND T.EXPOBELONG = '01'
		;

		COMMIT;

	  --2.����׼����¶
	  --ɾ��Ŀ���������
    DELETE FROM RWA_DEV.RWA_EI_EXPOSURE WHERE DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD') AND SSYSID IN ('ZBJ');
    DELETE FROM RWA_DEV.RWA_EI_CLIENT WHERE DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD') AND SSYSID IN ('ZBJ');
    DELETE FROM RWA_DEV.RWA_EI_CONTRACT WHERE DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD') AND SSYSID IN ('ZBJ');
    COMMIT;

	  --2.1 ���Ⱪ¶Ӧ�տ�Ͷ�ʶ�Ӧ��׼�����ֵ��Ĭ��12312000��Ŀ
    INSERT INTO RWA_DEV.RWA_EI_EXPOSURE(
         				 DataDate           																					--��������
								,DataNo                                 											--������ˮ��
								,ExposureID                             											--���ձ�¶ID
								,DueID                                  											--ծ��ID
								,SSysID                                 											--ԴϵͳID
								,ContractID                             											--��ͬID
								,ClientID                               											--��������ID
								,SOrgID                                 											--Դ����ID
								,SOrgName                               											--Դ��������
								,OrgSortNo                              											--�������������
								,OrgID                                  											--��������ID
								,OrgName                                											--������������
								,AccOrgID                               											--�������ID
								,AccOrgName                             											--�����������
								,IndustryID                             											--������ҵ����
								,IndustryName                           											--������ҵ����
								,BusinessLine                           											--ҵ������
								,AssetType                              											--�ʲ�����
								,AssetSubType                           											--�ʲ�С��
								,BusinessTypeID                         											--ҵ��Ʒ�ִ���
								,BusinessTypeName                       											--ҵ��Ʒ������
								,CreditRiskDataType                     											--���÷�����������
								,AssetTypeOfHaircuts                    											--�ۿ�ϵ����Ӧ�ʲ����
								,BusinessTypeSTD                        											--Ȩ�ط�ҵ������
								,ExpoClassSTD                           											--Ȩ�ط���¶����
								,ExpoSubClassSTD                        											--Ȩ�ط���¶С��
								,ExpoClassIRB                           											--��������¶����
								,ExpoSubClassIRB                        											--��������¶С��
								,ExpoBelong                             											--��¶������ʶ
								,BookType                               											--�˻����
								,ReguTranType                           											--��ܽ�������
								,RepoTranFlag                           											--�ع����ױ�ʶ
								,RevaFrequency                          											--�ع�Ƶ��
								,Currency                               											--����
								,NormalPrincipal                        											--�����������
								,OverdueBalance                         											--�������
								,NonAccrualBalance                      											--��Ӧ�����
								,OnSheetBalance                         											--�������
								,NormalInterest                         											--������Ϣ
								,OnDebitInterest                        											--����ǷϢ
								,OffDebitInterest                       											--����ǷϢ
								,ExpenseReceivable                      											--Ӧ�շ���
								,AssetBalance                           											--�ʲ����
								,AccSubject1                            											--��Ŀһ
								,AccSubject2                            											--��Ŀ��
								,AccSubject3                            											--��Ŀ��
								,StartDate                              											--��ʼ����
								,DueDate                                											--��������
								,OriginalMaturity                       											--ԭʼ����
								,ResidualM                              											--ʣ������
								,RiskClassify                           											--���շ���
								,ExposureStatus                         											--���ձ�¶״̬
								,OverdueDays                            											--��������
								,SpecialProvision                       											--ר��׼����
								,GeneralProvision                       											--һ��׼����
								,EspecialProvision                      											--�ر�׼����
								,WrittenOffAmount                       											--�Ѻ������
								,OffExpoSource                          											--���Ⱪ¶��Դ
								,OffBusinessType                        											--����ҵ������
								,OffBusinessSdvsSTD                     											--Ȩ�ط�����ҵ������ϸ��
								,UncondCancelFlag                       											--�Ƿ����ʱ����������
								,CCFLevel                               											--����ת��ϵ������
								,CCFAIRB                                											--�߼�������ת��ϵ��
								,ClaimsLevel                            											--ծȨ����
								,BondFlag                               											--�Ƿ�Ϊծȯ
								,BondIssueIntent                        											--ծȯ����Ŀ��
								,NSURealPropertyFlag                    											--�Ƿ�����ò�����
								,RepAssetTermType                       											--��ծ�ʲ���������
								,DependOnFPOBFlag                       											--�Ƿ�����������δ��ӯ��
								,IRating                                											--�ڲ�����
								,PD                                     											--ΥԼ����
								,LGDLevel                               											--ΥԼ��ʧ�ʼ���
								,LGDAIRB                                											--�߼���ΥԼ��ʧ��
								,MAIRB                                  											--�߼�����Ч����
								,EADAIRB                                											--�߼���ΥԼ���ձ�¶
								,DefaultFlag                            											--ΥԼ��ʶ
								,BEEL                                   											--��ΥԼ��¶Ԥ����ʧ����
								,DefaultLGD                             											--��ΥԼ��¶ΥԼ��ʧ��
								,EquityExpoFlag                         											--��Ȩ��¶��ʶ
								,EquityInvestType                       											--��ȨͶ�ʶ�������
								,EquityInvestCause          																	--��ȨͶ���γ�ԭ��
								,SLFlag                                 											--רҵ�����ʶ
								,SLType                               												--רҵ��������
								,PFPhase                                											--��Ŀ���ʽ׶�
								,ReguRating                             											--�������
								,CBRCMPRatingFlag                       											--������϶������Ƿ��Ϊ����
								,LargeFlucFlag                          											--�Ƿ񲨶��Խϴ�
								,LiquExpoFlag                           											--�Ƿ���������з��ձ�¶
								,PaymentDealFlag                        											--�Ƿ����Ը�ģʽ
								,DelayTradingDays                       											--�ӳٽ�������
								,SecuritiesFlag                         											--�м�֤ȯ��ʶ
								,SecuIssuerID                           											--֤ȯ������ID
								,RatingDurationType                     											--������������
								,SecuIssueRating                        											--֤ȯ���еȼ�
								,SecuResidualM                          											--֤ȯʣ������
								,SecuRevaFrequency                      											--֤ȯ�ع�Ƶ��
								,CCPTranFlag                            											--�Ƿ����뽻�׶�����ؽ���
								,CCPID                                  											--���뽻�׶���ID
								,QualCCPFlag                         													--�Ƿ�ϸ����뽻�׶���
								,BankRole                               											--���н�ɫ
								,ClearingMethod                        												--���㷽ʽ
								,BankAssetFlag                          											--�Ƿ������ύ�ʲ�
								,MatchConditions    																					--�����������
								,SFTFlag                                											--֤ȯ���ʽ��ױ�ʶ
								,MasterNetAgreeFlag                     											--���������Э���ʶ
								,MasterNetAgreeID                       											--���������Э��ID
								,SFTType                                											--֤ȯ���ʽ�������
								,SecuOwnerTransFlag                     											--֤ȯ����Ȩ�Ƿ�ת��
								,OTCFlag                                 											--�����������߱�ʶ
								,ValidNettingFlag                       											--��Ч�������Э���ʶ
								,ValidNetAgreementID                    											--��Ч�������Э��ID
								,OTCType                                											--����������������
								,DepositRiskPeriod                      											--��֤������ڼ�
								,MTM                                    											--���óɱ�
								,MTMCurrency                            											--���óɱ�����
								,BuyerOrSeller                          											--������
								,QualROFlag                             											--�ϸ�����ʲ���ʶ
								,ROIssuerPerformFlag                    											--�����ʲ��������Ƿ�����Լ
								,BuyerInsolvencyFlag                    											--���ñ������Ƿ��Ʋ�
								,NonpaymentFees                         											--��δ֧������
								,RetailExpoFlag                         											--���۱�¶��ʶ
								,RetailClaimType                        											--����ծȨ����
								,MortgageType                           											--ס����Ѻ��������
								,ExpoNumber                             											--���ձ�¶����
								,LTV                                    											--�����ֵ��
								,Aging                                  											--����
								,NewDefaultDebtFlag                     											--����ΥԼծ���ʶ
								,PDPoolModelID                          											--PD�ֳ�ģ��ID
								,LGDPoolModelID                         											--LGD�ֳ�ģ��ID
								,CCFPoolModelID                         											--CCF�ֳ�ģ��ID
								,PDPoolID           																					--����PD��ID
								,LGDPoolID             																				--����LGD��ID
								,CCFPoolID                                      							--����CCF��ID
								,ABSUAFlag           																					--�ʲ�֤ȯ�������ʲ���ʶ
								,ABSPoolID                                                    --֤ȯ���ʲ���ID
								,GroupID                                                      --������
								,DefaultDate                                                  --ΥԼʱ��
								,ABSPROPORTION																								--�ʲ�֤ȯ������
								,DEBTORNUMBER																									--����˸���
    )
    WITH TMP_FNS_JE AS (
    						SELECT ABS(SUM(T1.BALANCE_D - T1.BALANCE_C)) AS FNS_BALANCE
								  FROM FNS_GL_BALANCE T1
								 WHERE T1.DATANO = p_data_dt_str
								   AND T1.CURRENCY_CODE = 'RMB'
								   AND T1.SUBJECT_NO LIKE '1231%'
    )
    , TMP_EI_JE AS (
    					SELECT SUM(T.GENERALPROVISION) AS EI_BALANCE
							  FROM RWA_EI_EXPOSURE T
							 WHERE T.DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD')
							   AND T.SSYSID IN ('TZ')
    )
    , TMP_JE AS (
    					SELECT A.FNS_BALANCE - B.EI_BALANCE AS ZBJ_BALANCE
    						FROM TMP_FNS_JE A, TMP_EI_JE B
    )
    SELECT
                 TO_DATE(p_data_dt_str,'YYYYMMDD')                                     AS DataDate                --��������
		            ,p_data_dt_str                                                         AS DataNo                  --������ˮ��
		            ,'XN-ZBJ-12312000-10000000-CNY'                                        AS ExposureID              --���ձ�¶ID
		            ,'XN-ZBJ-12312000-10000000-CNY'                                        AS DueID                   --ծ��ID
		            ,'ZBJ'                                                                 AS SSysID                  --ԴϵͳID
		            ,'XN-ZBJ-12312000-10000000-CNY'                                        AS ContractID              --��ͬID
		            ,'XN-ZBJ-10000000-CNY'				                                         AS ClientID                --��������ID
		            ,'9998'	                                                           AS SOrgID                  --Դ����ID
		            ,'�������йɷ����޹�˾'                                                            AS SOrgName                --Դ��������
		            ,'1010'				                                                           AS OrgSortNo               --�������������
		            ,'9998'	                                                           AS OrgID                   --��������ID
		            ,'�������йɷ����޹�˾'	                                                           AS OrgName                 --������������
		            ,'9998'	                                                           AS AccOrgID                --�������ID
		            ,'�������йɷ����޹�˾'			                                                       AS AccOrgName              --�����������
		            ,'J6620'										                                           AS IndustryID              --������ҵ����              			Ĭ�� �������з���(J6620)
		            ,'�������з���'									                                       AS IndustryName            --������ҵ����              			Ĭ�� �������з���
		            ,'0501'	                                                               AS BusinessLine            --ҵ������                  			Ĭ�� ����(0501)
		            ,'132'	                                                               AS AssetType               --�ʲ�����                  			Ĭ�� �����ʲ�(132)
		            ,'13205'                                                               AS AssetSubType            --�ʲ�С��                  			Ĭ�� ���������ʲ�(13205)
		            ,'9010101010'		                                                       AS BusinessTypeID          --ҵ��Ʒ�ִ���              			Ĭ�� ����ҵ��Ʒ��(9010101010)
		            ,'����ҵ��Ʒ��'                                                        AS BusinessTypeName        --ҵ��Ʒ������              			Ĭ�� ����ҵ��Ʒ��(9010101010)
		            ,'01'                                                                  AS CreditRiskDataType      --���÷�����������          			Ĭ�� һ�������(01)
		            ,'01'                                                                  AS AssetTypeOfHaircuts     --�ۿ�ϵ����Ӧ�ʲ����      			Ĭ�� �ֽ��ֽ�ȼ���(01)
		            ,'99'	                                                                 AS BusinessTypeSTD         --Ȩ�ط�ҵ������            			Ĭ�� �����ʲ�(99)
		            ,'0112'                                                                AS ExpoClassSTD            --Ȩ�ط���¶����            			Ĭ�� ����(0112)
		            ,'011216'                                                              AS ExpoSubClassSTD         --Ȩ�ط���¶С��            			Ĭ�� ��������100%����Ȩ�ص��ʲ�(011216)
		            ,'0203'																                                 AS ExpoClassIRB            --��������¶����            			Ĭ�� ��˾���ձ�¶(0203)
		            ,'020301'												                                       AS ExpoSubClassIRB         --��������¶С��            			Ĭ�� һ�㹫˾(020301)
		            ,'01'                                                                  AS ExpoBelong              --��¶������ʶ              			Ĭ�� ����(01)
		            ,'01'                                                                  AS BookType                --�˻����                  			Ĭ�� �����˻�(01)
                ,'02'                                                                  AS ReguTranType            --��ܽ�������              			Ĭ�� �����ʱ��г�(02)
                ,'0'                                                                   AS RepoTranFlag            --�ع����ױ�ʶ              			Ĭ�� ��(0)
                ,1								                                                     AS RevaFrequency           --�ع�Ƶ��                  			Ĭ�� 1
                ,'CNY'										                                             AS Currency                --����
                ,0									                                                   AS NormalPrincipal         --�����������              			Ĭ�� 0
                ,0                                                                     AS OverdueBalance          --�������                  			Ĭ�� 0
                ,0		            								                                     AS NonAccrualBalance       --��Ӧ�����                			Ĭ�� 0
                ,0									                                                   AS OnSheetBalance          --�������                  			Ĭ�� 0
                ,0					                                                           AS NormalInterest          --������Ϣ                  			Ĭ�� 0	��Ϣͳһ�����˱�����
                ,0                                                                     AS OnDebitInterest         --����ǷϢ                  			Ĭ�� 0
                ,0                                                                     AS OffDebitInterest        --����ǷϢ                  			Ĭ�� 0
                ,0																					                           AS ExpenseReceivable       --Ӧ�շ���                  			Ĭ�� 0
                ,0									                                                   AS AssetBalance            --�ʲ����                  			Ĭ�� 0
                ,'12312000'	                                                           AS AccSubject1             --��Ŀһ
                ,NULL                                                                  AS AccSubject2             --��Ŀ��
                ,NULL		                                                               AS AccSubject3             --��Ŀ��
                ,TO_CHAR(TO_DATE(p_data_dt_str,'YYYYMMDD'),'YYYY-MM-DD')               AS StartDate               --��ʼ����
                ,TO_CHAR(ADD_MONTHS(TO_DATE(p_data_dt_str,'YYYYMMDD'),1),'YYYY-MM-DD')
                																                                       AS DueDate             		--��������
                ,(ADD_MONTHS(TO_DATE(p_data_dt_str,'YYYYMMDD'),1) - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365
                																													             AS OriginalMaturity    		--ԭʼ����
                ,(ADD_MONTHS(TO_DATE(p_data_dt_str,'YYYYMMDD'),1) - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365
                		                                                                   AS ResidualM               --ʣ������
                ,'01'																                                   AS RiskClassify            --���շ���												Ĭ�� ����(01)
                ,'01'                                                                  AS ExposureStatus          --���ձ�¶״̬              			Ĭ�� ����(01)
                ,0                                                                     AS OverdueDays             --��������                  			Ĭ�� 0
                ,0						                                                         AS SpecialProvision        --ר��׼����                			Ĭ�� 0	RWA����
                ,T1.ZBJ_BALANCE                                                        AS GeneralProvision        --һ��׼����                			Ĭ�� 0	RWA����
                ,0                                                                     AS EspecialProvision       --�ر�׼����                			Ĭ�� 0	RWA����
                ,0	                                                                   AS WrittenOffAmount        --�Ѻ������                			Ĭ�� 0
                ,''                                                                    AS OffExpoSource           --���Ⱪ¶��Դ              			Ĭ�� NULL
                ,''                                                                    AS OffBusinessType         --����ҵ������              			Ĭ�� NULL
                ,''	                                                                   AS OffBusinessSdvsSTD      --Ȩ�ط�����ҵ������ϸ��    			Ĭ�� NULL
                ,'0'                                                                   AS UncondCancelFlag        --�Ƿ����ʱ����������      			Ĭ�� ��(0)
                ,''                                                                    AS CCFLevel                --����ת��ϵ������          			Ĭ�� NULL
                ,''		                                                                 AS CCFAIRB                 --�߼�������ת��ϵ��        			Ĭ�� NULL
                ,'01'                                                                  AS ClaimsLevel             --ծȨ����                  			Ĭ�� �߼�ծȨ(01)
                ,'0'	                                                                 AS BondFlag                --�Ƿ�Ϊծȯ                			Ĭ�� ��(0)
                ,'02'                                                                  AS BondIssueIntent         --ծȯ����Ŀ��              			Ĭ�� ����(02)
                ,'0'	                                                                 AS NSURealPropertyFlag     --�Ƿ�����ò�����          			Ĭ�� ��(0)
                ,'01'                                                                  AS RepAssetTermType        --��ծ�ʲ���������          			Ĭ�� ���ɹ涨����������(01)
                ,'0'                                                                   AS DependOnFPOBFlag        --�Ƿ�����������δ��ӯ��    			Ĭ�� ��(0)
                ,''															                                       AS IRating                 --�ڲ�����                  			Ĭ�� NULL
                ,NULL									                                                 AS PD                      --ΥԼ����                  			Ĭ�� NULL
                ,NULL                                                                  AS LGDLevel                --ΥԼ��ʧ�ʼ���            			Ĭ�� NULL
                ,NULL                                                                  AS LGDAIRB                 --�߼���ΥԼ��ʧ��          			Ĭ�� NULL
                ,NULL                                                                  AS MAIRB                   --�߼�����Ч����            			Ĭ�� NULL
                ,NULL                                                                  AS EADAIRB                 --�߼���ΥԼ���ձ�¶        			Ĭ�� NULL
                ,'0'																								                   AS DefaultFlag             --ΥԼ��ʶ                  			Ĭ�� ��(0)
                ,0.45                                                                  AS BEEL                    --��ΥԼ��¶Ԥ����ʧ����    			Ĭ�� 0.45
                ,0.45                                                                  AS DefaultLGD              --��ΥԼ��¶ΥԼ��ʧ��      			Ĭ�� 0.45
                ,'0'                                                                   AS EquityExpoFlag          --��Ȩ��¶��ʶ              			Ĭ�� ��(0)
                ,''                                                                    AS EquityInvestType        --��ȨͶ�ʶ�������          			Ĭ�� NULL
                ,''	                                                                   AS EquityInvestCause       --��ȨͶ���γ�ԭ��          			Ĭ�� NULL
                ,'0'                                                                   AS SLFlag                  --רҵ�����ʶ              			Ĭ�� ��(0)
                ,''                                                                    AS SLType                  --רҵ��������              			Ĭ�� NULL
                ,''                                                                    AS PFPhase                 --��Ŀ���ʽ׶�              			Ĭ�� NULL
                ,''	                                                                   AS ReguRating              --�������                  			Ĭ�� NULL
                ,'0'                                                                   AS CBRCMPRatingFlag        --������϶������Ƿ��Ϊ����			Ĭ�� ��(0)
                ,'0'                                                                   AS LargeFlucFlag           --�Ƿ񲨶��Խϴ�            			Ĭ�� ��(0)
                ,'0'                                                                   AS LiquExpoFlag            --�Ƿ���������з��ձ�¶    			Ĭ�� ��(0)
                ,'0'                                                                   AS PaymentDealFlag         --�Ƿ����Ը�ģʽ          			Ĭ�� ��(0)
                ,0	                                                                   AS DelayTradingDays        --�ӳٽ�������              			Ĭ�� 0
                ,'0'                                                                   AS SecuritiesFlag          --�м�֤ȯ��ʶ              			Ĭ�� ��(0)
                ,''                                                                    AS SecuIssuerID            --֤ȯ������ID              			Ĭ�� NULL
                ,''                                                                    AS RatingDurationType      --������������              			Ĭ�� NULL
                ,''	                                                                   AS SecuIssueRating         --֤ȯ���еȼ�              			Ĭ�� NULL
                ,''                                                                    AS SecuResidualM           --֤ȯʣ������              			Ĭ�� NULL
                ,NULL                                                                  AS SecuRevaFrequency       --֤ȯ�ع�Ƶ��              			Ĭ�� NULL
                ,'0'                                                                   AS CCPTranFlag             --�Ƿ����뽻�׶�����ؽ���  			Ĭ�� ��(0)
                ,''	                                                                   AS CCPID                   --���뽻�׶���ID            			Ĭ�� NULL
                ,'0'                                                                   AS QualCCPFlag             --�Ƿ�ϸ����뽻�׶���      			Ĭ�� ��(0)
                ,''                                                                    AS BankRole                --���н�ɫ                  			Ĭ�� NULL
                ,''	                                                                   AS ClearingMethod          --���㷽ʽ                  			Ĭ�� NULL
                ,'0'                                                                   AS BankAssetFlag           --�Ƿ������ύ�ʲ�          			Ĭ�� ��(0)
                ,''	                                                                   AS MatchConditions         --�����������              			Ĭ�� NULL
                ,'0'                                                                   AS SFTFlag                 --֤ȯ���ʽ��ױ�ʶ          			Ĭ�� ��(0)
                ,''		                                                                 AS MasterNetAgreeFlag      --���������Э���ʶ        			Ĭ�� NULL
                ,''                                                                    AS MasterNetAgreeID        --���������Э��ID          			Ĭ�� NULL
                ,''	                                                                   AS SFTType                 --֤ȯ���ʽ�������          			Ĭ�� NULL
                ,'0'                                                                   AS SecuOwnerTransFlag      --֤ȯ����Ȩ�Ƿ�ת��        			Ĭ�� ��(0)
                ,'0'                                                                   AS OTCFlag                 --�����������߱�ʶ          			Ĭ�� ��(0)
                ,'0'                                                                   AS ValidNettingFlag        --��Ч�������Э���ʶ      			Ĭ�� ��(0)
                ,''                                                                    AS ValidNetAgreementID     --��Ч�������Э��ID        			Ĭ�� NULL
                ,''                                                                    AS OTCType                 --����������������          			Ĭ�� NULL
                ,NULL                                                                  AS DepositRiskPeriod       --��֤������ڼ�            			Ĭ�� NULL
                ,0	                                                                   AS MTM                     --���óɱ�                  			Ĭ�� 0
                ,''                                                                    AS MTMCurrency             --���óɱ�����              			Ĭ�� NULL
                ,''	                                                                   AS BuyerOrSeller           --������                  			Ĭ�� NULL
                ,'0'                                                                   AS QualROFlag              --�ϸ�����ʲ���ʶ          			Ĭ�� ��(0)
                ,'0'                                                                   AS ROIssuerPerformFlag     --�����ʲ��������Ƿ�����Լ  			Ĭ�� ��(0)
                ,'0'                                                                   AS BuyerInsolvencyFlag     --���ñ������Ƿ��Ʋ�      			Ĭ�� ��(0)
                ,0	                                                                   AS NonpaymentFees          --��δ֧������              			Ĭ�� 0
                ,'0'                                                                   AS RetailExpoFlag          --���۱�¶��ʶ              			Ĭ�� ��(0)
                ,''                                                                    AS RetailClaimType         --����ծȨ����              			Ĭ�� NULL
                ,''                                                                    AS MortgageType            --ס����Ѻ��������          			Ĭ�� NULL
                ,1                                                                     AS ExpoNumber              --���ձ�¶����              			Ĭ�� 1
                ,0.8                                                                   AS LTV                     --�����ֵ��                			Ĭ�� 0.8
                ,NULL                                                                  AS Aging                   --����                      			Ĭ�� NULL
                ,''								                                                     AS NewDefaultDebtFlag      --����ΥԼծ���ʶ     						Ĭ�� NULL
                ,''                                                                    AS PDPoolModelID           --PD�ֳ�ģ��ID              			Ĭ�� NULL
                ,''                                                                    AS LGDPoolModelID          --LGD�ֳ�ģ��ID             			Ĭ�� NULL
                ,''                                                                    AS CCFPoolModelID          --CCF�ֳ�ģ��ID             			Ĭ�� NULL
                ,''	                                                                   AS PDPoolID                --����PD��ID                			Ĭ�� NULL
                ,''                                                                    AS LGDPoolID               --����LGD��ID               			Ĭ�� NULL
                ,''                                                                    AS CCFPoolID               --����CCF��ID               			Ĭ�� NULL
                ,'0'                                                                   AS ABSUAFlag               --�ʲ�֤ȯ�������ʲ���ʶ    			Ĭ�� ��(0)
                ,''				                                                             AS ABSPoolID               --֤ȯ���ʲ���ID            			Ĭ�� NULL
                ,''                                                                    AS GroupID                 --������                  			Ĭ�� NULL
                ,NULL															                                     AS DefaultDate             --ΥԼʱ��
                ,NULL																																	 AS ABSPROPORTION						--�ʲ�֤ȯ������
								,NULL																																	 AS DEBTORNUMBER						--����˸���


    FROM				TMP_JE T1
    WHERE				T1.ZBJ_BALANCE > 0
    ;

    COMMIT;

    --4.2 ���Ⱪ¶�Ŵ�������ʲ���Ӧ��׼�����ֵ��Ĭ��13040101��Ŀ
    INSERT INTO RWA_DEV.RWA_EI_EXPOSURE(
         				 DataDate           																					--��������
								,DataNo                                 											--������ˮ��
								,ExposureID                             											--���ձ�¶ID
								,DueID                                  											--ծ��ID
								,SSysID                                 											--ԴϵͳID
								,ContractID                             											--��ͬID
								,ClientID                               											--��������ID
								,SOrgID                                 											--Դ����ID
								,SOrgName                               											--Դ��������
								,OrgSortNo                              											--�������������
								,OrgID                                  											--��������ID
								,OrgName                                											--������������
								,AccOrgID                               											--�������ID
								,AccOrgName                             											--�����������
								,IndustryID                             											--������ҵ����
								,IndustryName                           											--������ҵ����
								,BusinessLine                           											--ҵ������
								,AssetType                              											--�ʲ�����
								,AssetSubType                           											--�ʲ�С��
								,BusinessTypeID                         											--ҵ��Ʒ�ִ���
								,BusinessTypeName                       											--ҵ��Ʒ������
								,CreditRiskDataType                     											--���÷�����������
								,AssetTypeOfHaircuts                    											--�ۿ�ϵ����Ӧ�ʲ����
								,BusinessTypeSTD                        											--Ȩ�ط�ҵ������
								,ExpoClassSTD                           											--Ȩ�ط���¶����
								,ExpoSubClassSTD                        											--Ȩ�ط���¶С��
								,ExpoClassIRB                           											--��������¶����
								,ExpoSubClassIRB                        											--��������¶С��
								,ExpoBelong                             											--��¶������ʶ
								,BookType                               											--�˻����
								,ReguTranType                           											--��ܽ�������
								,RepoTranFlag                           											--�ع����ױ�ʶ
								,RevaFrequency                          											--�ع�Ƶ��
								,Currency                               											--����
								,NormalPrincipal                        											--�����������
								,OverdueBalance                         											--�������
								,NonAccrualBalance                      											--��Ӧ�����
								,OnSheetBalance                         											--�������
								,NormalInterest                         											--������Ϣ
								,OnDebitInterest                        											--����ǷϢ
								,OffDebitInterest                       											--����ǷϢ
								,ExpenseReceivable                      											--Ӧ�շ���
								,AssetBalance                           											--�ʲ����
								,AccSubject1                            											--��Ŀһ
								,AccSubject2                            											--��Ŀ��
								,AccSubject3                            											--��Ŀ��
								,StartDate                              											--��ʼ����
								,DueDate                                											--��������
								,OriginalMaturity                       											--ԭʼ����
								,ResidualM                              											--ʣ������
								,RiskClassify                           											--���շ���
								,ExposureStatus                         											--���ձ�¶״̬
								,OverdueDays                            											--��������
								,SpecialProvision                       											--ר��׼����
								,GeneralProvision                       											--һ��׼����
								,EspecialProvision                      											--�ر�׼����
								,WrittenOffAmount                       											--�Ѻ������
								,OffExpoSource                          											--���Ⱪ¶��Դ
								,OffBusinessType                        											--����ҵ������
								,OffBusinessSdvsSTD                     											--Ȩ�ط�����ҵ������ϸ��
								,UncondCancelFlag                       											--�Ƿ����ʱ����������
								,CCFLevel                               											--����ת��ϵ������
								,CCFAIRB                                											--�߼�������ת��ϵ��
								,ClaimsLevel                            											--ծȨ����
								,BondFlag                               											--�Ƿ�Ϊծȯ
								,BondIssueIntent                        											--ծȯ����Ŀ��
								,NSURealPropertyFlag                    											--�Ƿ�����ò�����
								,RepAssetTermType                       											--��ծ�ʲ���������
								,DependOnFPOBFlag                       											--�Ƿ�����������δ��ӯ��
								,IRating                                											--�ڲ�����
								,PD                                     											--ΥԼ����
								,LGDLevel                               											--ΥԼ��ʧ�ʼ���
								,LGDAIRB                                											--�߼���ΥԼ��ʧ��
								,MAIRB                                  											--�߼�����Ч����
								,EADAIRB                                											--�߼���ΥԼ���ձ�¶
								,DefaultFlag                            											--ΥԼ��ʶ
								,BEEL                                   											--��ΥԼ��¶Ԥ����ʧ����
								,DefaultLGD                             											--��ΥԼ��¶ΥԼ��ʧ��
								,EquityExpoFlag                         											--��Ȩ��¶��ʶ
								,EquityInvestType                       											--��ȨͶ�ʶ�������
								,EquityInvestCause          																	--��ȨͶ���γ�ԭ��
								,SLFlag                                 											--רҵ�����ʶ
								,SLType                               												--רҵ��������
								,PFPhase                                											--��Ŀ���ʽ׶�
								,ReguRating                             											--�������
								,CBRCMPRatingFlag                       											--������϶������Ƿ��Ϊ����
								,LargeFlucFlag                          											--�Ƿ񲨶��Խϴ�
								,LiquExpoFlag                           											--�Ƿ���������з��ձ�¶
								,PaymentDealFlag                        											--�Ƿ����Ը�ģʽ
								,DelayTradingDays                       											--�ӳٽ�������
								,SecuritiesFlag                         											--�м�֤ȯ��ʶ
								,SecuIssuerID                           											--֤ȯ������ID
								,RatingDurationType                     											--������������
								,SecuIssueRating                        											--֤ȯ���еȼ�
								,SecuResidualM                          											--֤ȯʣ������
								,SecuRevaFrequency                      											--֤ȯ�ع�Ƶ��
								,CCPTranFlag                            											--�Ƿ����뽻�׶�����ؽ���
								,CCPID                                  											--���뽻�׶���ID
								,QualCCPFlag                         													--�Ƿ�ϸ����뽻�׶���
								,BankRole                               											--���н�ɫ
								,ClearingMethod                        												--���㷽ʽ
								,BankAssetFlag                          											--�Ƿ������ύ�ʲ�
								,MatchConditions    																					--�����������
								,SFTFlag                                											--֤ȯ���ʽ��ױ�ʶ
								,MasterNetAgreeFlag                     											--���������Э���ʶ
								,MasterNetAgreeID                       											--���������Э��ID
								,SFTType                                											--֤ȯ���ʽ�������
								,SecuOwnerTransFlag                     											--֤ȯ����Ȩ�Ƿ�ת��
								,OTCFlag                                 											--�����������߱�ʶ
								,ValidNettingFlag                       											--��Ч�������Э���ʶ
								,ValidNetAgreementID                    											--��Ч�������Э��ID
								,OTCType                                											--����������������
								,DepositRiskPeriod                      											--��֤������ڼ�
								,MTM                                    											--���óɱ�
								,MTMCurrency                            											--���óɱ�����
								,BuyerOrSeller                          											--������
								,QualROFlag                             											--�ϸ�����ʲ���ʶ
								,ROIssuerPerformFlag                    											--�����ʲ��������Ƿ�����Լ
								,BuyerInsolvencyFlag                    											--���ñ������Ƿ��Ʋ�
								,NonpaymentFees                         											--��δ֧������
								,RetailExpoFlag                         											--���۱�¶��ʶ
								,RetailClaimType                        											--����ծȨ����
								,MortgageType                           											--ס����Ѻ��������
								,ExpoNumber                             											--���ձ�¶����
								,LTV                                    											--�����ֵ��
								,Aging                                  											--����
								,NewDefaultDebtFlag                     											--����ΥԼծ���ʶ
								,PDPoolModelID                          											--PD�ֳ�ģ��ID
								,LGDPoolModelID                         											--LGD�ֳ�ģ��ID
								,CCFPoolModelID                         											--CCF�ֳ�ģ��ID
								,PDPoolID           																					--����PD��ID
								,LGDPoolID             																				--����LGD��ID
								,CCFPoolID                                      							--����CCF��ID
								,ABSUAFlag           																					--�ʲ�֤ȯ�������ʲ���ʶ
								,ABSPoolID                                                    --֤ȯ���ʲ���ID
								,GroupID                                                      --������
								,DefaultDate                                                  --ΥԼʱ��
								,ABSPROPORTION																								--�ʲ�֤ȯ������
								,DEBTORNUMBER																									--����˸���
    )
    WITH TMP_FNS_JE AS (
    						SELECT ABS(SUM(T1.BALANCE_D - T1.BALANCE_C)) AS FNS_BALANCE
								  FROM FNS_GL_BALANCE T1
								 WHERE T1.DATANO = p_data_dt_str
								   AND T1.CURRENCY_CODE = 'RMB'
								   AND T1.SUBJECT_NO IN ('13040101',
								                         '13040102')
    )
    , TMP_EI_BAL AS (
    					SELECT T.EXPOCLASSSTD						AS EXPOCLASSSTD
    								,T.EXPOSUBCLASSSTD				AS EXPOSUBCLASSSTD
    								,SUM(T.NORMALPRINCIPAL) 	AS BALANCE
    								,SUM(T.GENERALPROVISION) 	AS EI_BALANCE
							  FROM RWA_EI_EXPOSURE T
							 WHERE T.DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD')
							   AND T.SSYSID IN ('XD', 'XYK', 'PJ', 'ZX')
							   AND T.EXPOBELONG = '01'
						GROUP BY T.EXPOCLASSSTD,T.EXPOSUBCLASSSTD
							HAVING SUM(T.NORMALPRINCIPAL) > 0
    )
    , TMP_EI_JE AS (
    					SELECT SUM(T.NORMALPRINCIPAL) 	AS BALANCE
    								,SUM(T.GENERALPROVISION) 	AS EI_BALANCE
							  FROM RWA_EI_EXPOSURE T
							 WHERE T.DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD')
							   AND T.SSYSID IN ('XD', 'XYK', 'PJ', 'ZX')
							   AND T.EXPOBELONG = '01'
    )
    , TMP_JE AS (
    					SELECT C.EXPOCLASSSTD											AS EXPOCLASSSTD
    								,C.EXPOSUBCLASSSTD									AS EXPOSUBCLASSSTD
    								,C.BALANCE													AS BALANCE
    								,B.BALANCE			 										AS TOTAL_BALANCE
    								,A.FNS_BALANCE - B.EI_BALANCE			 	AS ZBJ_BALANCE
    						FROM TMP_FNS_JE A, TMP_EI_JE B, TMP_EI_BAL C
    )
    SELECT
                 TO_DATE(p_data_dt_str,'YYYYMMDD')                                     AS DataDate                --��������
		            ,p_data_dt_str                                                         AS DataNo                  --������ˮ��
		            ,'XN-ZBJ-' || T1.EXPOSUBCLASSSTD || '-10000000-CNY'                    AS ExposureID              --���ձ�¶ID
		            ,'XN-ZBJ-13040101-10000000-CNY'                                        AS DueID                   --ծ��ID
		            ,'ZBJ'                                                                 AS SSysID                  --ԴϵͳID
		            ,'XN-ZBJ-' || T1.EXPOSUBCLASSSTD || '-10000000-CNY'                    AS ContractID              --��ͬID
		            ,'XN-ZBJ-10000000-CNY'			                                           AS ClientID                --��������ID
		            ,'9998'	                                                           AS SOrgID                  --Դ����ID
		            ,'�������йɷ����޹�˾'                                                            AS SOrgName                --Դ��������
		            ,'1010'				                                                           AS OrgSortNo               --�������������
		            ,'9998'	                                                           AS OrgID                   --��������ID
		            ,'�������йɷ����޹�˾'	                                                           AS OrgName                 --������������
		            ,'9998'	                                                           AS AccOrgID                --�������ID
		            ,'�������йɷ����޹�˾'			                                                       AS AccOrgName              --�����������
		            ,'J6620'										                                           AS IndustryID              --������ҵ����              			Ĭ�� �������з���(J6620)
		            ,'�������з���'									                                       AS IndustryName            --������ҵ����              			Ĭ�� �������з���
		            ,'0501'	                                                               AS BusinessLine            --ҵ������                  			Ĭ�� ����(0501)
		            ,'132'	                                                               AS AssetType               --�ʲ�����                  			Ĭ�� �����ʲ�(132)
		            ,'13205'                                                               AS AssetSubType            --�ʲ�С��                  			Ĭ�� ���������ʲ�(13205)
		            ,'9010101010'		                                                       AS BusinessTypeID          --ҵ��Ʒ�ִ���              			Ĭ�� ����ҵ��Ʒ��(9010101010)
		            ,'����ҵ��Ʒ��'                                                        AS BusinessTypeName        --ҵ��Ʒ������              			Ĭ�� ����ҵ��Ʒ��(9010101010)
		            ,'01'                                                                  AS CreditRiskDataType      --���÷�����������          			Ĭ�� һ�������(01)
		            ,'01'                                                                  AS AssetTypeOfHaircuts     --�ۿ�ϵ����Ӧ�ʲ����      			Ĭ�� �ֽ��ֽ�ȼ���(01)
		            ,'99'	                                                                 AS BusinessTypeSTD         --Ȩ�ط�ҵ������            			Ĭ�� �����ʲ�(99)
		            ,T1.EXPOCLASSSTD                                                       AS ExpoClassSTD            --Ȩ�ط���¶����            			Ĭ�� ����(0112)
		            ,T1.EXPOSUBCLASSSTD                                                    AS ExpoSubClassSTD         --Ȩ�ط���¶С��            			Ĭ�� ��������100%����Ȩ�ص��ʲ�(011216)
		            ,'0203'																                                 AS ExpoClassIRB            --��������¶����            			Ĭ�� ��˾���ձ�¶(0203)
		            ,'020301'												                                       AS ExpoSubClassIRB         --��������¶С��            			Ĭ�� һ�㹫˾(020301)
		            ,'01'                                                                  AS ExpoBelong              --��¶������ʶ              			Ĭ�� ����(01)
		            ,'01'                                                                  AS BookType                --�˻����                  			Ĭ�� �����˻�(01)
                ,'02'                                                                  AS ReguTranType            --��ܽ�������              			Ĭ�� �����ʱ��г�(02)
                ,'0'                                                                   AS RepoTranFlag            --�ع����ױ�ʶ              			Ĭ�� ��(0)
                ,1								                                                     AS RevaFrequency           --�ع�Ƶ��                  			Ĭ�� 1
                ,'CNY'										                                             AS Currency                --����
                ,0									                                                   AS NormalPrincipal         --�����������              			Ĭ�� 0
                ,0                                                                     AS OverdueBalance          --�������                  			Ĭ�� 0
                ,0		            								                                     AS NonAccrualBalance       --��Ӧ�����                			Ĭ�� 0
                ,0									                                                   AS OnSheetBalance          --�������                  			Ĭ�� 0
                ,0					                                                           AS NormalInterest          --������Ϣ                  			Ĭ�� 0	��Ϣͳһ�����˱�����
                ,0                                                                     AS OnDebitInterest         --����ǷϢ                  			Ĭ�� 0
                ,0                                                                     AS OffDebitInterest        --����ǷϢ                  			Ĭ�� 0
                ,0																					                           AS ExpenseReceivable       --Ӧ�շ���                  			Ĭ�� 0
                ,0									                                                   AS AssetBalance            --�ʲ����                  			Ĭ�� 0
                ,'13040101'	                                                           AS AccSubject1             --��Ŀһ
                ,NULL                                                                  AS AccSubject2             --��Ŀ��
                ,NULL		                                                               AS AccSubject3             --��Ŀ��
                ,TO_CHAR(TO_DATE(p_data_dt_str,'YYYYMMDD'),'YYYY-MM-DD')               AS StartDate               --��ʼ����
                ,TO_CHAR(ADD_MONTHS(TO_DATE(p_data_dt_str,'YYYYMMDD'),1),'YYYY-MM-DD')
                																                                       AS DueDate             		--��������
                ,(ADD_MONTHS(TO_DATE(p_data_dt_str,'YYYYMMDD'),1) - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365
                																													             AS OriginalMaturity    		--ԭʼ����
                ,(ADD_MONTHS(TO_DATE(p_data_dt_str,'YYYYMMDD'),1) - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365
                		                                                                   AS ResidualM               --ʣ������
                ,'01'																                                   AS RiskClassify            --���շ���												Ĭ�� ����(01)
                ,'01'                                                                  AS ExposureStatus          --���ձ�¶״̬              			Ĭ�� ����(01)
                ,0                                                                     AS OverdueDays             --��������                  			Ĭ�� 0
                ,0																						                         AS SpecialProvision        --ר��׼����                			Ĭ�� 0	RWA����
                ,T1.ZBJ_BALANCE * T1.BALANCE / T1.TOTAL_BALANCE	                       AS GeneralProvision        --һ��׼����                			Ĭ�� 0	RWA����
                ,0                                                                     AS EspecialProvision       --�ر�׼����                			Ĭ�� 0	RWA����
                ,0	                                                                   AS WrittenOffAmount        --�Ѻ������                			Ĭ�� 0
                ,''                                                                    AS OffExpoSource           --���Ⱪ¶��Դ              			Ĭ�� NULL
                ,''                                                                    AS OffBusinessType         --����ҵ������              			Ĭ�� NULL
                ,''	                                                                   AS OffBusinessSdvsSTD      --Ȩ�ط�����ҵ������ϸ��    			Ĭ�� NULL
                ,'0'                                                                   AS UncondCancelFlag        --�Ƿ����ʱ����������      			Ĭ�� ��(0)
                ,''                                                                    AS CCFLevel                --����ת��ϵ������          			Ĭ�� NULL
                ,''		                                                                 AS CCFAIRB                 --�߼�������ת��ϵ��        			Ĭ�� NULL
                ,'01'                                                                  AS ClaimsLevel             --ծȨ����                  			Ĭ�� �߼�ծȨ(01)
                ,'0'	                                                                 AS BondFlag                --�Ƿ�Ϊծȯ                			Ĭ�� ��(0)
                ,'02'                                                                  AS BondIssueIntent         --ծȯ����Ŀ��              			Ĭ�� ����(02)
                ,'0'	                                                                 AS NSURealPropertyFlag     --�Ƿ�����ò�����          			Ĭ�� ��(0)
                ,'01'                                                                  AS RepAssetTermType        --��ծ�ʲ���������          			Ĭ�� ���ɹ涨����������(01)
                ,'0'                                                                   AS DependOnFPOBFlag        --�Ƿ�����������δ��ӯ��    			Ĭ�� ��(0)
                ,''															                                       AS IRating                 --�ڲ�����                  			Ĭ�� NULL
                ,NULL									                                                 AS PD                      --ΥԼ����                  			Ĭ�� NULL
                ,NULL                                                                  AS LGDLevel                --ΥԼ��ʧ�ʼ���            			Ĭ�� NULL
                ,NULL                                                                  AS LGDAIRB                 --�߼���ΥԼ��ʧ��          			Ĭ�� NULL
                ,NULL                                                                  AS MAIRB                   --�߼�����Ч����            			Ĭ�� NULL
                ,NULL                                                                  AS EADAIRB                 --�߼���ΥԼ���ձ�¶        			Ĭ�� NULL
                ,'0'																								                   AS DefaultFlag             --ΥԼ��ʶ                  			Ĭ�� ��(0)
                ,0.45                                                                  AS BEEL                    --��ΥԼ��¶Ԥ����ʧ����    			Ĭ�� 0.45
                ,0.45                                                                  AS DefaultLGD              --��ΥԼ��¶ΥԼ��ʧ��      			Ĭ�� 0.45
                ,'0'                                                                   AS EquityExpoFlag          --��Ȩ��¶��ʶ              			Ĭ�� ��(0)
                ,''                                                                    AS EquityInvestType        --��ȨͶ�ʶ�������          			Ĭ�� NULL
                ,''	                                                                   AS EquityInvestCause       --��ȨͶ���γ�ԭ��          			Ĭ�� NULL
                ,'0'                                                                   AS SLFlag                  --רҵ�����ʶ              			Ĭ�� ��(0)
                ,''                                                                    AS SLType                  --רҵ��������              			Ĭ�� NULL
                ,''                                                                    AS PFPhase                 --��Ŀ���ʽ׶�              			Ĭ�� NULL
                ,''	                                                                   AS ReguRating              --�������                  			Ĭ�� NULL
                ,'0'                                                                   AS CBRCMPRatingFlag        --������϶������Ƿ��Ϊ����			Ĭ�� ��(0)
                ,'0'                                                                   AS LargeFlucFlag           --�Ƿ񲨶��Խϴ�            			Ĭ�� ��(0)
                ,'0'                                                                   AS LiquExpoFlag            --�Ƿ���������з��ձ�¶    			Ĭ�� ��(0)
                ,'0'                                                                   AS PaymentDealFlag         --�Ƿ����Ը�ģʽ          			Ĭ�� ��(0)
                ,0	                                                                   AS DelayTradingDays        --�ӳٽ�������              			Ĭ�� 0
                ,'0'                                                                   AS SecuritiesFlag          --�м�֤ȯ��ʶ              			Ĭ�� ��(0)
                ,''                                                                    AS SecuIssuerID            --֤ȯ������ID              			Ĭ�� NULL
                ,''                                                                    AS RatingDurationType      --������������              			Ĭ�� NULL
                ,''	                                                                   AS SecuIssueRating         --֤ȯ���еȼ�              			Ĭ�� NULL
                ,''                                                                    AS SecuResidualM           --֤ȯʣ������              			Ĭ�� NULL
                ,NULL                                                                  AS SecuRevaFrequency       --֤ȯ�ع�Ƶ��              			Ĭ�� NULL
                ,'0'                                                                   AS CCPTranFlag             --�Ƿ����뽻�׶�����ؽ���  			Ĭ�� ��(0)
                ,''	                                                                   AS CCPID                   --���뽻�׶���ID            			Ĭ�� NULL
                ,'0'                                                                   AS QualCCPFlag             --�Ƿ�ϸ����뽻�׶���      			Ĭ�� ��(0)
                ,''                                                                    AS BankRole                --���н�ɫ                  			Ĭ�� NULL
                ,''	                                                                   AS ClearingMethod          --���㷽ʽ                  			Ĭ�� NULL
                ,'0'                                                                   AS BankAssetFlag           --�Ƿ������ύ�ʲ�          			Ĭ�� ��(0)
                ,''	                                                                   AS MatchConditions         --�����������              			Ĭ�� NULL
                ,'0'                                                                   AS SFTFlag                 --֤ȯ���ʽ��ױ�ʶ          			Ĭ�� ��(0)
                ,''		                                                                 AS MasterNetAgreeFlag      --���������Э���ʶ        			Ĭ�� NULL
                ,''                                                                    AS MasterNetAgreeID        --���������Э��ID          			Ĭ�� NULL
                ,''	                                                                   AS SFTType                 --֤ȯ���ʽ�������          			Ĭ�� NULL
                ,'0'                                                                   AS SecuOwnerTransFlag      --֤ȯ����Ȩ�Ƿ�ת��        			Ĭ�� ��(0)
                ,'0'                                                                   AS OTCFlag                 --�����������߱�ʶ          			Ĭ�� ��(0)
                ,'0'                                                                   AS ValidNettingFlag        --��Ч�������Э���ʶ      			Ĭ�� ��(0)
                ,''                                                                    AS ValidNetAgreementID     --��Ч�������Э��ID        			Ĭ�� NULL
                ,''                                                                    AS OTCType                 --����������������          			Ĭ�� NULL
                ,NULL                                                                  AS DepositRiskPeriod       --��֤������ڼ�            			Ĭ�� NULL
                ,0	                                                                   AS MTM                     --���óɱ�                  			Ĭ�� 0
                ,''                                                                    AS MTMCurrency             --���óɱ�����              			Ĭ�� NULL
                ,''	                                                                   AS BuyerOrSeller           --������                  			Ĭ�� NULL
                ,'0'                                                                   AS QualROFlag              --�ϸ�����ʲ���ʶ          			Ĭ�� ��(0)
                ,'0'                                                                   AS ROIssuerPerformFlag     --�����ʲ��������Ƿ�����Լ  			Ĭ�� ��(0)
                ,'0'                                                                   AS BuyerInsolvencyFlag     --���ñ������Ƿ��Ʋ�      			Ĭ�� ��(0)
                ,0	                                                                   AS NonpaymentFees          --��δ֧������              			Ĭ�� 0
                ,'0'                                                                   AS RetailExpoFlag          --���۱�¶��ʶ              			Ĭ�� ��(0)
                ,''                                                                    AS RetailClaimType         --����ծȨ����              			Ĭ�� NULL
                ,''                                                                    AS MortgageType            --ס����Ѻ��������          			Ĭ�� NULL
                ,1                                                                     AS ExpoNumber              --���ձ�¶����              			Ĭ�� 1
                ,0.8                                                                   AS LTV                     --�����ֵ��                			Ĭ�� 0.8
                ,NULL                                                                  AS Aging                   --����                      			Ĭ�� NULL
                ,''								                                                     AS NewDefaultDebtFlag      --����ΥԼծ���ʶ     						Ĭ�� NULL
                ,''                                                                    AS PDPoolModelID           --PD�ֳ�ģ��ID              			Ĭ�� NULL
                ,''                                                                    AS LGDPoolModelID          --LGD�ֳ�ģ��ID             			Ĭ�� NULL
                ,''                                                                    AS CCFPoolModelID          --CCF�ֳ�ģ��ID             			Ĭ�� NULL
                ,''	                                                                   AS PDPoolID                --����PD��ID                			Ĭ�� NULL
                ,''                                                                    AS LGDPoolID               --����LGD��ID               			Ĭ�� NULL
                ,''                                                                    AS CCFPoolID               --����CCF��ID               			Ĭ�� NULL
                ,'0'                                                                   AS ABSUAFlag               --�ʲ�֤ȯ�������ʲ���ʶ    			Ĭ�� ��(0)
                ,''				                                                             AS ABSPoolID               --֤ȯ���ʲ���ID            			Ĭ�� NULL
                ,''                                                                    AS GroupID                 --������                  			Ĭ�� NULL
                ,NULL															                                     AS DefaultDate             --ΥԼʱ��
                ,NULL																																	 AS ABSPROPORTION						--�ʲ�֤ȯ������
								,NULL																																	 AS DEBTORNUMBER						--����˸���


    FROM				TMP_JE T1
    WHERE				T1.ZBJ_BALANCE > 0
    ;

    COMMIT;

    --4.3 �����ͬ
    INSERT INTO RWA_DEV.RWA_EI_CONTRACT(
               DataDate                             --��������
              ,DataNo                               --������ˮ��
              ,ContractID                           --��ͬID
              ,SContractID                          --Դ��ͬID
              ,SSysID                               --ԴϵͳID
              ,ClientID                             --��������ID
              ,SOrgID                               --Դ����ID
              ,SOrgName                             --Դ��������
              ,OrgSortNo                            --�������������
              ,OrgID                                --��������ID
              ,OrgName                              --������������
              ,IndustryID                           --������ҵ����
              ,IndustryName                         --������ҵ����
              ,BusinessLine                         --ҵ������
              ,AssetType                            --�ʲ�����
              ,AssetSubType                         --�ʲ�С��
              ,BusinessTypeID                       --ҵ��Ʒ�ִ���
              ,BusinessTypeName                     --ҵ��Ʒ������
              ,CreditRiskDataType                   --���÷�����������
              ,StartDate                            --��ʼ����
              ,DueDate                              --��������
              ,OriginalMaturity                     --ԭʼ����
              ,ResidualM                            --ʣ������
              ,SettlementCurrency                   --�������
              ,ContractAmount                       --��ͬ�ܽ��
              ,NotExtractPart                       --��ͬδ��ȡ����
							,UncondCancelFlag  									  --�Ƿ����ʱ����������
							,ABSUAFlag         									  --�ʲ�֤ȯ�������ʲ���ʶ
							,ABSPoolID         									  --֤ȯ���ʲ���ID
							,GroupID           									  --������
							,GUARANTEETYPE     									  --��Ҫ������ʽ
							,ABSPROPORTION												--�ʲ�֤ȯ������
    )
    SELECT			--DISTINCT
                TO_DATE(p_data_dt_str,'YYYYMMDD')            AS DataDate                             --��������
                ,p_data_dt_str													     AS DataNo                               --������ˮ��
                ,T1.CONTRACTID                   				 		 AS ContractID                           --��ͬID
                ,T1.CONTRACTID                           		 AS SContractID                          --Դ��ͬID
                ,T1.SSYSID                                   AS SSysID                               --ԴϵͳID
                ,T1.CLIENTID                         			 	 AS ClientID                             --��������ID
                ,T1.SORGID                         					 AS SOrgID                               --Դ����ID
                ,T1.SORGNAME                              	 AS SOrgName                             --Դ��������
                ,T1.ORGSORTNO                                AS OrgSortNo                            --�������������
                ,T1.ORGID                              			 AS OrgID                                --��������ID
                ,T1.ORGNAME                                  AS OrgName                              --������������
                ,T1.INDUSTRYID                               AS IndustryID                           --������ҵ����
                ,T1.INDUSTRYNAME                             AS IndustryName                         --������ҵ����
                ,T1.BUSINESSLINE                             AS BusinessLine                         --ҵ������              				Ĭ�� ͬҵ(04)
                ,T1.ASSETTYPE                              	 AS AssetType                            --�ʲ�����
                ,T1.ASSETSUBTYPE              							 AS AssetSubType                         --�ʲ�С��
                ,T1.BUSINESSTYPEID                           AS BusinessTypeID                       --ҵ��Ʒ�ִ���
                ,T1.BUSINESSTYPENAME                         AS BusinessTypeName                     --ҵ��Ʒ������
                ,T1.CREDITRISKDATATYPE                       AS CreditRiskDataType                   --���÷�����������
                ,T1.STARTDATE                                AS StartDate                            --��ʼ����
                ,T1.DUEDATE                                  AS DueDate                              --��������
                ,T1.ORIGINALMATURITY                         AS OriginalMaturity                     --ԭʼ����
                ,T1.RESIDUALM                                AS ResidualM                            --ʣ������
                ,T1.CURRENCY                                 AS SettlementCurrency                   --�������
                ,T1.NORMALPRINCIPAL                          AS ContractAmount                       --��ͬ�ܽ��
                ,0                                           AS NotExtractPart                       --��ͬδ��ȡ����        				Ĭ�� 0
                ,'0'                                         AS UncondCancelFlag  									 --�Ƿ����ʱ����������  				Ĭ�� ��(0)
                ,'0'                                         AS ABSUAFlag         									 --�ʲ�֤ȯ�������ʲ���ʶ				Ĭ�� ��(0)
                ,''                                        	 AS ABSPoolID         									 --֤ȯ���ʲ���ID        				Ĭ�� ��
                ,''                                          AS GroupID           									 --������              				Ĭ�� ��
                ,''																					 AS GUARANTEETYPE     									 --��Ҫ������ʽ
                ,NULL																				 AS ABSPROPORTION												 --�ʲ�֤ȯ������

    FROM				RWA_DEV.RWA_EI_EXPOSURE T1
    WHERE				T1.DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
    AND					T1.SSYSID = 'ZBJ'
    ;

    COMMIT;

    --4.4 �����������
    INSERT INTO RWA_DEV.RWA_EI_CLIENT(
				         DataDate                   --��������
				        ,DataNo                     --������ˮ��
				        ,ClientID                   --��������ID
				        ,SourceClientID             --Դ��������ID
				        ,SSysID                     --ԴϵͳID
				        ,ClientName                 --������������
				        ,SOrgID                     --Դ����ID
				        ,SOrgName                   --Դ��������
				        ,OrgSortNo                  --�������������
				        ,OrgID                      --��������ID
				        ,OrgName                    --������������
				        ,IndustryID                 --������ҵ����
				        ,IndustryName               --������ҵ����
				        ,ClientType                 --�����������
				        ,ClientSubType              --��������С��
				        ,RegistState                --ע����һ����
				        ,RCERating                  --����ע����ⲿ����
				        ,RCERAgency                 --����ע����ⲿ��������
				        ,OrganizationCode           --��֯��������
				        ,ConsolidatedSCFlag         --�Ƿ񲢱��ӹ�˾
				        ,SLClientFlag               --רҵ����ͻ���ʶ
				        ,SLClientType               --רҵ����ͻ�����
				        ,ExpoCategoryIRB            --��������¶���
				        ,ModelID                    --ģ��ID
				        ,ModelIRating               --ģ���ڲ�����
				        ,ModelPD                    --ģ��ΥԼ����
				        ,IRating                    --�ڲ�����
				        ,PD                         --ΥԼ����
				        ,DefaultFlag                --ΥԼ��ʶ
				        ,NewDefaultFlag             --����ΥԼ��ʶ
				        ,DefaultDate                --ΥԼʱ��
				        ,ClientERating              --���������ⲿ����
				        ,CCPFlag                    --���뽻�׶��ֱ�ʶ
				        ,QualCCPFlag                --�Ƿ�ϸ����뽻�׶���
				        ,ClearMemberFlag            --�����Ա��ʶ
				        ,CompanySize                --��ҵ��ģ
				        ,SSMBFlag                   --��׼С΢��ҵ��ʶ
				        ,AnnualSale                 --��˾�ͻ������۶�
				        ,CountryCode                --ע����Ҵ���
				        ,MSMBFlag										--���Ų�΢С��ҵ��ʶ
    )
		SELECT 			--DISTINCT
        				 TO_DATE(p_data_dt_str,'yyyyMMdd')     																	AS DataDate            		--��������
        				,p_data_dt_str                         																	AS DataNo              		--������ˮ��
        				,'XN-ZBJ-10000000-CNY'																									AS ClientID            		--��������ID            Ĭ�� 'XN-ZBJ-10000000-CNY'
        				,'XN-ZBJ-10000000-CNY'																									AS SourceClientID      		--Դ��������ID
        				,'ZBJ'		                             																	AS SSysID              		--ԴϵͳID
        				,'׼����-����ͻ�'																											AS ClientName          		--������������          Ĭ�� ׼����-����ͻ�
        				,'9998'	                                                            AS SOrgID                 --Դ����ID
		            ,'�������йɷ����޹�˾'                                                             AS SOrgName               --Դ��������
		            ,'1010'				                                                            AS OrgSortNo              --�������������
		            ,'9998'	                                                            AS OrgID                  --��������ID
		            ,'�������йɷ����޹�˾'	                                                            AS OrgName                --������������
        				,'J6620'										                                            AS IndustryID             --������ҵ����          Ĭ�� �������з���(J6620)
		            ,'�������з���'									                                        AS IndustryName           --������ҵ����          Ĭ�� �������з���
        				,'03'																																		AS ClientType          		--�����������          Ĭ�� 03-��˾
        				,'0301'																																	AS ClientSubType       		--��������С��          Ĭ�� 0301-һ�㹫˾
        				,'01'	                                 																	AS RegistState         		--ע����һ����        Ĭ�� 01-����
        				,''							                       																	AS RCERating           		--����ע����ⲿ����
        				,'01'                                  																	AS RCERAgency          		--����ע����ⲿ��������
        				,'202869177'											     																	AS OrganizationCode    		--��֯��������
        				,'0'	                                																	AS ConsolidatedSCFlag  		--�Ƿ񲢱��ӹ�˾
        				,'0'                                  																	AS SLClientFlag        		--רҵ����ͻ���ʶ
        				,''	                                  																	AS SLClientType        		--רҵ����ͻ�����
        				,'020301'						                   																	AS ExpoCategoryIRB     		--��������¶���        Ĭ�� 020301-һ�㹫˾
        				,''				                            																	AS ModelID             		--ģ��ID
        				,''				                            																	AS ModelIRating        		--ģ���ڲ�����
        				,NULL                                 																	AS ModelPD             		--ģ��ΥԼ����
        				,''								                    																	AS IRating             		--�ڲ�����
        				,NULL	                                																	AS PD                  		--ΥԼ����
        				,'0'																																		AS DefaultFlag         		--ΥԼ��ʶ
        				,'0'											            																	AS NewDefaultFlag      		--����ΥԼ��ʶ
        				,''																     																	AS DefaultDate         		--ΥԼʱ��
        				,''																     																	AS ClientERating       		--���������ⲿ����
        				,'0'                                  																	AS CCPFlag             		--���뽻�׶��ֱ�ʶ
        				,'0'                                   																	AS QualCCPFlag         		--�Ƿ�ϸ����뽻�׶���
        				,'0'                                   																	AS ClearMemberFlag     		--�����Ա��ʶ
        				,'00'									                																	AS CompanySize         		--��ҵ��ģ
        				,'0'											            																	AS SSMBFlag            		--��׼С΢��ҵ��ʶ
        				,NULL							                     																	AS AnnualSale          		--��˾�ͻ������۶�
        				,'CHN'																																	AS CountryCode            --ע����Ҵ���
        				,''																																			AS MSMBFlag								--���Ų�΢С��ҵ��ʶ

    FROM				DUAL
    ;

    COMMIT;

    --5.����һ��׼���𣬰��弶�������
		UPDATE RWA_DEV.RWA_EI_EXPOSURE SET SPECIALPROVISION = NORMALPRINCIPAL * CASE WHEN RISKCLASSIFY = '01' THEN 0 WHEN RISKCLASSIFY = '02' THEN 0.02 WHEN RISKCLASSIFY = '03' THEN 0.25 WHEN RISKCLASSIFY = '04' THEN 0.5 WHEN RISKCLASSIFY = '05' THEN 1 ELSE 0 END WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD');

		COMMIT;

		--6.���±�¶��BEEL�ֶ�ֵ�����ڷ�̯���׼����/�ʲ����
		UPDATE RWA_DEV.RWA_EI_EXPOSURE SET BEEL = CASE WHEN NVL(GENERALPROVISION,0) / ASSETBALANCE > 1 THEN 1 ELSE  NVL(GENERALPROVISION,0) / ASSETBALANCE END WHERE ASSETBALANCE <> 0 AND DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD');

		COMMIT;

		--7.���±�¶��BEEL�ֶ�ֵ��ʹ�����������õ���beelֵ����
		UPDATE RWA_DEV.RWA_EI_EXPOSURE T SET T.BEEL = (SELECT RH.BEELVALUE FROM RWA_DEV.RWA_TEMP_LGDLEVEL RH WHERE RH.BUSINESSID = T.CONTRACTID)
		WHERE  EXISTS (SELECT 1 FROM RWA_DEV.RWA_TEMP_LGDLEVEL RH WHERE RH.BUSINESSID = T.CONTRACTID AND RH.BEELVALUE IS NOT NULL)
		AND		 T.DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD');

		COMMIT;

  	--8.ͳ�Ʊ�¶�������
  	SELECT COUNT(1) INTO v_count1 FROM RWA_DEV.RWA_EI_EXPOSURE WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD') AND GENERALPROVISION <> 0;

		p_po_rtncode := '1';
		p_po_rtnmsg  := '�ɹ�'||'-'||v_count1;
		--�����쳣
		EXCEPTION
    WHEN OTHERS THEN
				 --Dbms_output.Put_line('������,�������Ϊ:'||sqlcode||';������ϢΪ:'||sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace);
       	 ROLLBACK;
   			 p_po_rtncode := sqlcode;
   			 p_po_rtnmsg  := '׼�������ͷ�̯����ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace ;
         RETURN;

END PRO_RWA_CD_DECREASE_TYPE;
/

