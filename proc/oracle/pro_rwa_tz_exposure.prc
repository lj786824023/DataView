CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_TZ_EXPOSURE(
			 											p_data_dt_str	IN	VARCHAR2,		--�������� yyyyMMdd
       											p_po_rtncode	OUT	VARCHAR2,		--���ر�� 1 �ɹ�,0 ʧ��
														p_po_rtnmsg		OUT	VARCHAR2		--��������
				)
  /*
    �洢��������:RWA_DEV.PRO_RWA_TZ_EXPOSURE
    ʵ�ֹ���:����ϵͳ-Ͷ��-���÷��ձ�¶(������Դ����ϵͳ��ҵ�������Ϣȫ������RWAͶ�ʽӿڱ���ձ�¶����)
    ���ݿھ�:ȫ��
    ����Ƶ��:�³�����
    ��  ��  :V1.0.0
    ��д��  :LISY
    ��дʱ��:2016-04-12
    ��  λ  :�Ϻ���˶��Ϣ�����ɷ����޹�˾
    Դ  ��1 :RWA_DEV.FNS_BND_INFO_B|����ϵͳծȯ��Ϣ��
    Դ  ��2 :RWA_DEV.FNS_BND_BOOK_B|����ϵͳ������
    Դ  ��5 :RWA.CODE_LIBRARY|RWA�����
    Դ	��6 :RWA.ORG_INFO|������Ϣ��
    Դ  ��8 :RWA_DEV.NCM_BUSINESS_DUEBILL|�Ŵ���ݱ�
    Դ  ��9 :RWA_DEV.NCM_CUSTOMER_INFO|�ͻ���Ϣ��
    Դ  ��10:RWA_DEV.IRS_CR_CUSTOMER_RATE|�����ۿͻ�������Ϣ��
    Դ  ��11:RWA_DEV.NCM_BREAKDEFINEDREMARK|�Ŵ�ΥԼ��¼��
    Դ  ��14:RWA.RWA_WSIB_ABS_ISSUE_EXPOSURE|���л����ʲ�֤ȯ����¶�̵ױ�
    Դ  ��15:RWA.RWA_WSIB_ABS_INVEST_EXPOSURE|Ͷ�ʻ����ʲ�֤ȯ����¶�̵ױ�
    Ŀ���  :RWA_DEV.RWA_TZ_EXPOSURE|����ϵͳͶ�������÷��ձ�¶��
    �����¼(�޸���|�޸�ʱ��|�޸�����):
    */
  AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_TZ_EXPOSURE';
  --�����쳣����
  v_raise EXCEPTION;
  --���嵱ǰ����ļ�¼��
  v_count INTEGER;
  --v_count1 INTEGER;
  V_BALANCE NUMBER(24,6);
  S_BALANCE NUMBER(24,6);

  BEGIN
    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*�����ȫ�����ݼ��������Ŀ���*/
    --1.���Ŀ����е�ԭ�м�¼
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_TZ_EXPOSURE';

    --2.���������������ݴ�Դ����뵽Ŀ�����
    --2.1 ����ϵͳ-���ծȯͶ��
    INSERT INTO RWA_DEV.RWA_TZ_EXPOSURE(
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
                ,SBJT2
                ,SBJT_VAL2
                ,SBJT3
                ,SBJT_VAL3
                ,SBJT4
                ,SBJT_VAL4
                ,SBJT5
                ,SBJT_VAL5
    )
    SELECT
                TO_DATE(p_data_dt_str,'YYYYMMDD')            														AS DataDate               --��������
                ,p_data_dt_str													     														AS DataNo                 --������ˮ��
                --,NVL(T3.SERIALNO,T1.BOND_ID)     				 		 														AS ExposureID             --���ձ�¶ID
                ,T1.BOND_ID
                ,T1.BOND_ID                             		 														AS DueID              		--ծ��ID
                ,'TZ'                                        														AS SSysID                 --ԴϵͳID
                ,T1.BOND_ID					                  			 														AS ContractID             --��ͬID
                ,CASE WHEN T1.BOND_TYPE1 in('2004','2000','2020')
                THEN 'XN-ZGSYYH'
                 ELSE NVL(T5.BONDPUBLISHID,T14.PARTICIPANT_CODE)
                 END								                         														AS ClientID               --��������ID                ծȯ������
                ,T1.DEPARTMENT 		                   				 														AS SOrgID                	--Դ����ID
                ,T4.ORGNAME                                                            	AS SOrgName               --Դ��������
		            --,T4.SORTNO	                                                           	AS OrgSortNo              --�������������
                ,NVL(T4.SORTNO,'1010')
                --,T1.DEPARTMENT                           		 														AS OrgID                  --��������ID
                ,decode(substr(T1.DEPARTMENT,1,1),'@','01000000',T1.DEPARTMENT)
                ,nvl(T4.ORGNAME,'����')			                        		 														AS OrgName                --������������
                ,T1.DEPARTMENT                           		 														AS AccOrgID               --�������ID
                ,T4.ORGNAME					 												 														AS AccOrgName             --�����������
                ,CASE WHEN T1.BOND_ID = 'B200801010095' THEN 'J66'																								--ë��ϯ����Ĭ����ҵ
                		 	WHEN T5.ISCOUNTTR = '1' THEN 'S91'																													--��ծʱĬ�Ϸ�������ҵΪS91-���һ���
                 ELSE CASE WHEN NVL(T6.INDUSTRYTYPE,DECODE(SUBSTR(T6.RWACUSTOMERTYPE,1,2),'02','J6620','J66')) = '999999' THEN 'J66'
                 			ELSE NVL(T6.INDUSTRYTYPE,DECODE(SUBSTR(T6.RWACUSTOMERTYPE,1,2),'02','J6620','J66'))
                 			END
                 END						                                                       	AS IndustryID             --������ҵ����
		            ,CASE WHEN T1.BOND_ID = 'B200801010095' THEN '���ҽ��ڷ���'																				--ë��ϯ����Ĭ����ҵ
		            			WHEN T5.ISCOUNTTR = '1' THEN '���һ���'																											--��ծʱĬ�Ϸ�������ҵΪS91-���һ���
                 ELSE CASE WHEN NVL(T7.ITEMNAME,DECODE(SUBSTR(T6.RWACUSTOMERTYPE,1,2),'02','�������з���','���ҽ��ڷ���')) = 'δ֪' THEN '���ҽ��ڷ���'
                 			ELSE NVL(T7.ITEMNAME,DECODE(SUBSTR(T6.RWACUSTOMERTYPE,1,2),'02','�������з���','���ҽ��ڷ���'))
                 			END
                 END				                                                           	AS IndustryName           --������ҵ����
                ,'0201'                                      														AS BusinessLine           --ҵ������                  			Ĭ�� ͬҵ(04)
                ,''                                          														AS AssetType              --�ʲ�����                  			Ĭ�� NULL RWA������
                ,''                             	 																			AS AssetSubType           --�ʲ�С��                  			Ĭ�� NULL RWA������
                ,'1040202010'                        	 													AS BusinessTypeID         --ҵ��Ʒ�ִ���
                ,'���ծȯͶ��'      														AS BusinessTypeName       --ҵ��Ʒ������
                ,'01'                                        														AS CreditRiskDataType     --���÷�����������          			Ĭ�� һ�������(01)
                ,'01'                                   		 														AS AssetTypeOfHaircuts    --�ۿ�ϵ����Ӧ�ʲ����      			Ĭ�� �ֽ��ֽ�ȼ���(01)
                ,'07'                                        														AS BusinessTypeSTD    		--Ȩ�ط�ҵ������            			Ĭ�� һ���ʲ�(07)
                ,CASE WHEN T1.BOND_TYPE1 in('2004','2000','2020')
                      THEN '' --��ҵ����
                      WHEN T1.BOND_TYPE1='2010'
                      THEN '0103'--�ط�����ծȯ
                      WHEN T1.BOND_TYPE1 in('2005','2007','3003','2017')
                      THEN '0106'--��ҵծ
                      WHEN T1.BOND_TYPE1='3002'
                      THEN '0104'--���ڻ���ծ
                      WHEN T1.BOND_TYPE1='2001'
                      THEN '0102'--��ծ
                      WHEN T1.BOND_TYPE1='2003'
                      THEN '0104'--�����Խ���ծ
                      WHEN T1.BOND_TYPE1='2002'
                      THEN '0102'--����ծ
                      WHEN T1.BOND_TYPE1 IN('2008','2009','2013','3001')
                      THEN '0102'--��Ʊ��ʱ������
                       END ---��¶С��                                       														AS ExpoClassSTD		    		--Ȩ�ط���¶����            			Ĭ�� NULL RWA������
                ,CASE WHEN T1.BOND_TYPE1 in('2004','2000','2020')
                      THEN '' --��ҵ����
                      WHEN T1.BOND_TYPE1='2010'
                      THEN '010303'--�ط�����ծȯ
                      WHEN T1.BOND_TYPE1 in('2005','2007','3003','2017')
                      THEN '010601'--��ҵծ
                      WHEN T1.BOND_TYPE1='3002'
                      THEN '010408'--���ڻ���ծ
                      WHEN T1.BOND_TYPE1='2001'
                      THEN '010201'--��ծ
                      WHEN T1.BOND_TYPE1='2003'
                      THEN '010401'--�����Խ���ծ
                      WHEN T1.BOND_TYPE1='2002'
                      THEN '010202'--����ծ
                      WHEN T1.BOND_TYPE1 IN('2008','2009','2013','3001')
                      THEN '010202'--��Ʊ��ʱ������
                       END ---��¶С��
                ,CASE WHEN T1.BOND_ID = 'B200801010095' THEN '0202'																								--���ڻ������ձ�¶
                 ELSE SUBSTR(T13.DITEMNO,1,4)
                 END															                                    	AS ExpoClassIRB           --��������¶����            			Ĭ�� NULL RWA������
		            ,CASE WHEN T1.BOND_ID = 'B200801010095' THEN '020202'																							--����������ڻ���
		             ELSE T13.DITEMNO
		             END											                                             	AS ExpoSubClassIRB        --��������¶С��            			Ĭ�� NULL RWA������
                ,'01'                                        														AS ExpoBelong          		--��¶������ʶ              			Ĭ�� ����(01)
                ,CASE WHEN T1.ASSET_CLASS = '10' THEN '02'
                 ELSE '01'
                 END                                        														AS BookType               --�˻����                  			�ʲ�����Ϊ�����Խ����ʲ�(10)��Ϊ�����˻�(02)������Ϊ�����˻�(01)
                ,'02'                                        														AS ReguTranType         	--��ܽ�������              			Ĭ�� �����ʱ��г�(02)
                ,'0'                                         														AS RepoTranFlag           --�ع����ױ�ʶ              			Ĭ�� ��(0)
                ,1													                 														AS RevaFrequency          --�ع�Ƶ��                  			Ĭ�� 1
                ,NVL(T1.CURRENCY_CODE,'CNY')								 														AS Currency               --����
                ,NVL(T2.POSITION_INITIAL_VALUE,0)	                                      AS NormalPrincipal        --�����������
                ,0                                           														AS OverdueBalance         --�������                  			Ĭ�� 0
                ,0								                           														AS NonAccrualBalance      --��Ӧ�����                			Ĭ�� 0
                ,NVL(T2.POSITION_INITIAL_VALUE,0)										                  	AS OnSheetBalance         --�������
                ,0                                         	 														AS NormalInterest         --������Ϣ                  			Ĭ�� 0 ��Ϣͳһ�����˱�����
                ,0                                         	 														AS OnDebitInterest        --����ǷϢ                  			Ĭ�� 0
                ,0                                         	 														AS OffDebitInterest       --����ǷϢ                  			Ĭ�� 0
                ,0								                           														AS ExpenseReceivable      --Ӧ�շ���                  			Ĭ�� 0
                ,NVL(T2.POSITION_INITIAL_VALUE,0)																			 	AS AssetBalance       		--�ʲ����
                ,T2.SBJT_CD															 		 														AS AccSubject1        		--��Ŀһ
                ,''																			 		 														AS AccSubject2        		--��Ŀ��
                ,''					 																 														AS AccSubject3        		--��Ŀ��
                ,T1.origination_date 					 										 														AS StartDate          	 	--��ʼ����
                ,T1.MATURITY_DATE														 														AS DueDate            		--��������
								,CASE WHEN (TO_DATE(T1.MATURITY_DATE,'YYYYMMDD') - TO_DATE(T1.origination_date,'YYYYMMDD')) / 365 < 0 THEN 0
                      ELSE (TO_DATE(T1.MATURITY_DATE,'YYYYMMDD') - TO_DATE(T1.origination_date,'YYYYMMDD')) / 365
                 END																				 														AS OriginalMaturity   		--ԭʼ����
								,CASE WHEN (TO_DATE(T1.MATURITY_DATE,'YYYYMMDD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365 < 0 THEN 0
								      ELSE (TO_DATE(T1.MATURITY_DATE,'YYYYMMDD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365
								 END																				 														AS ResidualM          		--ʣ������
								,CASE WHEN T10.FINALLYRESULT IN ('A1','A2','A3','A4','A') THEN '01'       												--ʮ��������תΪ�弶����
                			WHEN T10.FINALLYRESULT IN ('B1','B2','B3','B') THEN '02'
                      WHEN T10.FINALLYRESULT IN ('C1','C2','C') THEN '03'
                      WHEN T10.FINALLYRESULT IN ('D1','D2','D') THEN '04'
                      WHEN T10.FINALLYRESULT = 'E' THEN '05'
                      ELSE '01'
                 END																                                   AS RiskClassify            --���շ���												Ĭ�� ����(01)
								,''                                          														AS ExposureStatus         --���ձ�¶״̬              			Ĭ�� NULL
								,0                                           														AS OverdueDays            --��������                  			Ĭ�� 0
								,0                                           														AS SpecialProvision       --ר��׼����                			Ĭ�� 0	RWA����
								,0                                           														AS GeneralProvision       --һ��׼����                			Ĭ�� 0	RWA����
								,0                                           														AS EspecialProvision      --�ر�׼����                			Ĭ�� 0	RWA����
								,0                                          														AS WrittenOffAmount       --�Ѻ������                			Ĭ�� 0
								,''                                          														AS OffExpoSource          --���Ⱪ¶��Դ              			Ĭ�� NULL
								,''                                          														AS OffBusinessType        --����ҵ������              			Ĭ�� NULL
								,''                                          														AS OffBusinessSdvsSTD     --Ȩ�ط�����ҵ������ϸ��    			Ĭ�� NULL
								,''                                          														AS UncondCancelFlag       --�Ƿ����ʱ����������      			Ĭ�� NULL
								,''	                                        														AS CCFLevel               --����ת��ϵ������          			Ĭ�� NULL
								,NULL                                        														AS CCFAIRB                --�߼�������ת��ϵ��        			Ĭ�� NULL
								,CASE WHEN T1.BOND_TYPE2 = '20' THEN '02'
								 ELSE '01'
								 END                                         														AS ClaimsLevel            --ծȨ����
								,'1'                                        														AS BondFlag               --�Ƿ�Ϊծȯ                			Ĭ�� ��(1)
								,SUBSTR(NVL(T5.BONDPUBLISHPURPOSE,'0020'),2,2)													AS BondIssueIntent        --ծȯ����Ŀ��              			Ĭ�� ����(02)
								,'0'                                         														AS NSURealPropertyFlag    --�Ƿ�����ò�����          			Ĭ�� ��(0)
								,''                                         														AS RepAssetTermType       --��ծ�ʲ���������          			Ĭ�� NULL
								,'0'                                         														AS DependOnFPOBFlag       --�Ƿ�����������δ��ӯ��    			Ĭ�� ��(0)
								,T9.PDADJLEVEL										           														AS IRating                --�ڲ�����
								,T9.PD								                      														AS PD                     --ΥԼ����
								,''	                                        														AS LGDLevel               --ΥԼ��ʧ�ʼ���            			Ĭ�� NULL
								,NULL                                        														AS LGDAIRB                --�߼���ΥԼ��ʧ��          			Ĭ�� NULL
								,NULL                                        														AS MAIRB                  --�߼�����Ч����            			Ĭ�� NULL
								,NULL                                        														AS EADAIRB                --�߼���ΥԼ���ձ�¶        			Ĭ�� NULL
								,CASE WHEN T9.PDADJLEVEL = '0116' THEN '1'
								 ELSE '0'
								 END																																		AS DefaultFlag            --ΥԼ��ʶ
								,CASE WHEN T1.BOND_TYPE2 = '20' THEN 0.75
								 ELSE 0.45
								 END                                        														AS BEEL                   --��ΥԼ��¶Ԥ����ʧ����
								,CASE WHEN T1.BOND_TYPE2 = '20' THEN 0.75
								 ELSE 0.45
								 END                                        														AS DefaultLGD             --��ΥԼ��¶ΥԼ��ʧ��      			Ĭ�� NULL
								,'0'                                         														AS EquityExpoFlag         --��Ȩ��¶��ʶ              			Ĭ�� ��(0)
								,''                                          														AS EquityInvestType       --��ȨͶ�ʶ�������          			Ĭ�� NULL
								,''                                         														AS EquityInvestCause      --��ȨͶ���γ�ԭ��          			Ĭ�� NULL
								,'0'                                         														AS SLFlag                 --רҵ�����ʶ              			Ĭ�� ��(0)
								,''                                          														AS SLType             		--רҵ��������              			Ĭ�� NULL
								,''	                                        														AS PFPhase                --��Ŀ���ʽ׶�              			Ĭ�� NULL
								,'01'                                        														AS ReguRating             --�������                  			Ĭ�� ��(01)
								,''                                          														AS CBRCMPRatingFlag       --������϶������Ƿ��Ϊ����			Ĭ�� NULL
								,''                                         														AS LargeFlucFlag          --�Ƿ񲨶��Խϴ�            			Ĭ�� NULL
								,'0'                                         														AS LiquExpoFlag           --�Ƿ���������з��ձ�¶    			Ĭ�� ��(0)
								,'1'                                        														AS PaymentDealFlag        --�Ƿ����Ը�ģʽ          			Ĭ�� ��(1)
								,NULL                                        														AS DelayTradingDays       --�ӳٽ�������              			Ĭ�� NULL
								,'1'				                                 														AS SecuritiesFlag         --�м�֤ȯ��ʶ              			Ĭ�� ��(1)
								,CASE WHEN T1.BOND_ID = 'B200801010095' THEN 'MZXXZ'																														--ë��ϯ����Ĭ�ϲ�������
                			WHEN T3.BUSINESSTYPE = '1040102040' AND (T5.ISCOUNTTR = '1' OR T5.BONDNAME LIKE '%��ծ%') THEN 'ZGZYZF'														--�����ծȯͶ�ʹ�ծʱĬ�Ϸ�����Ϊ�й���������
                			WHEN T3.BUSINESSTYPE = '1040202010' AND T5.BONDFLAG04 = '1' AND T5.MARKETSCATEGORY = '01' THEN T5.BONDPUBLISHCOUNTRY || 'ZYZF'		--���ծȯͶ�ʾ�����������
                			WHEN T3.BUSINESSTYPE = '1040202010' AND T5.BONDFLAG04 = '1' AND T5.MARKETSCATEGORY = '02' THEN T5.BONDPUBLISHCOUNTRY || 'ZYYH'		--���ծȯͶ�ʾ�����������
                			WHEN T3.BUSINESSTYPE = '1040202010' AND T5.BONDFLAG04 = '1' AND T5.MARKETSCATEGORY = '03' THEN T5.BONDPUBLISHCOUNTRY || 'BMST'		--���ծȯͶ�ʾ�����һ����ע��Ĺ�������ʵ��
                			WHEN REPLACE(T5.BONDPUBLISHID,'NCM_','') IS NULL THEN 'XN-YBGS'
                 ELSE T5.BONDPUBLISHID
                 END							                           														AS SecuIssuerID           --֤ȯ������ID
								,T5.TIMELIMIT														   															AS RatingDurationType     --������������
								,CASE WHEN T5.BONDRATING IS NULL THEN ''
                 ELSE RWA_DEV.GETSTANDARDRATING1(T5.BONDRATING)
                 END					                            	 														AS SecuIssueRating        --֤ȯ���еȼ�
								,CASE WHEN (TO_DATE(T5.MATURITYDATE,'YYYYMMDD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365 < 0 THEN 0
								      ELSE (TO_DATE(T5.MATURITYDATE,'YYYYMMDD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365
								 END                                       	 														AS SecuResidualM          --֤ȯʣ������
								,1	                                         														AS SecuRevaFrequency      --֤ȯ�ع�Ƶ��              			Ĭ�� 1
								,'0'                                         														AS CCPTranFlag            --�Ƿ����뽻�׶�����ؽ���  			Ĭ�� ��(0)
								,''                                          														AS CCPID                  --���뽻�׶���ID            			Ĭ�� NULL
								,''                                          														AS QualCCPFlag            --�Ƿ�ϸ����뽻�׶���      			Ĭ�� NULL
								,''                                          														AS BankRole               --���н�ɫ                  			Ĭ�� NULL
								,''                                          														AS ClearingMethod         --���㷽ʽ                  			Ĭ�� NULL
								,''                                          														AS BankAssetFlag          --�Ƿ������ύ�ʲ�          			Ĭ�� NULL
								,''                                         														AS MatchConditions        --�����������              			Ĭ�� NULL
								,'0'                                         														AS SFTFlag                --֤ȯ���ʽ��ױ�ʶ          			Ĭ�� ��(0)
								,'0'                                         														AS MasterNetAgreeFlag     --���������Э���ʶ        			Ĭ�� ��(0)
								,''                                          														AS MasterNetAgreeID       --���������Э��ID          			Ĭ�� NULL
								,''                                          														AS SFTType                --֤ȯ���ʽ�������          			Ĭ�� NULL
								,''                                         														AS SecuOwnerTransFlag     --֤ȯ����Ȩ�Ƿ�ת��        			Ĭ�� NULL
								,'0'                                         														AS OTCFlag                --�����������߱�ʶ          			Ĭ�� ��(0)
								,''                                          														AS ValidNettingFlag       --��Ч�������Э���ʶ      			Ĭ�� NULL
								,''                                          														AS ValidNetAgreementID    --��Ч�������Э��ID        			Ĭ�� NULL
								,''                                          														AS OTCType                --����������������          			Ĭ�� NULL
								,''                                          														AS DepositRiskPeriod      --��֤������ڼ�            			Ĭ�� NULL
								,NULL                                        														AS MTM                    --���óɱ�                  			Ĭ�� NULL
								,''                                          														AS MTMCurrency            --���óɱ�����              			Ĭ�� NULL
								,''                                          														AS BuyerOrSeller          --������                  			Ĭ�� NULL
								,''                                          														AS QualROFlag             --�ϸ�����ʲ���ʶ          			Ĭ�� NULL
								,''                                          														AS ROIssuerPerformFlag    --�����ʲ��������Ƿ�����Լ  			Ĭ�� NULL
								,''                                          														AS BuyerInsolvencyFlag    --���ñ������Ƿ��Ʋ�      			Ĭ�� NULL
								,NULL                                        														AS NonpaymentFees         --��δ֧������              			Ĭ�� NULL
								,'0'                                         														AS RetailExpoFlag         --���۱�¶��ʶ              			Ĭ�� ��(0)
								,''                                          														AS RetailClaimType        --����ծȨ����              			Ĭ�� NULL
								,''                                          														AS MortgageType           --ס����Ѻ��������          			Ĭ�� NULL
								,1                                           														AS ExpoNumber             --���ձ�¶����              			Ĭ�� 1
								,0.8                                         														AS LTV                    --�����ֵ��                			Ĭ�� 0.8
								,NULL                                        														AS Aging                  --����                      			Ĭ�� NULL
								,''                                          														AS NewDefaultDebtFlag     --����ΥԼծ���ʶ          			Ĭ�� NULL
								,''                                          														AS PDPoolModelID          --PD�ֳ�ģ��ID              			Ĭ�� NULL
								,''                                          														AS LGDPoolModelID         --LGD�ֳ�ģ��ID             			Ĭ�� NULL
								,''                                          														AS CCFPoolModelID         --CCF�ֳ�ģ��ID             			Ĭ�� NULL
								,''                                         														AS PDPoolID               --����PD��ID                			Ĭ�� NULL
								,''                                          														AS LGDPoolID              --����LGD��ID               			Ĭ�� NULL
								,''                                          														AS CCFPoolID              --����CCF��ID               			Ĭ�� NULL
								,'0'                                         														AS ABSUAFlag              --�ʲ�֤ȯ�������ʲ���ʶ    			Ĭ�� ��(0)
								,''				                                   														AS ABSPoolID              --֤ȯ���ʲ���ID            			Ĭ�� NULL
								,''                                          														AS GroupID            		--������                  			Ĭ�� NULL
								,CASE WHEN T9.PDADJLEVEL = '0116' THEN TO_DATE(T9.PDVAVLIDDATE,'YYYYMMDD')
								 ELSE NULL
								 END          																													AS DefaultDate            --ΥԼʱ��                  			Ĭ�� NULL
								,NULL																																	  AS ABSPROPORTION					--�ʲ�֤ȯ������
								,NULL																																	  AS DEBTORNUMBER						--����˸���
                ,T2.INT_ADJ_ITEM
                ,T2.INT_ADJ_VAL
                ,DECODE(SUBSTR(T2.ACCRUAL_GLNO, 1, 4), '1132', NULL,'1101',NULL, T2.ACCRUAL_GLNO)
                ,DECODE(SUBSTR(T2.ACCRUAL_GLNO, 1, 4), '1132', NULL,'1101',NULL, T2.ACCRUAL)
                ,T2.FAIR_EXCH_ITEM
                ,T2.FAIR_EXCH_VAL
                ,NULL
                ,NULL
    FROM				RWA_DEV.FNS_BND_INFO_B T1
    INNER JOIN (
     SELECT T.ACCT_NO,
            T.SECURITY_REFERENCE,
            T.ORG_CD,
            T.SBJT_CD,
            T.BELONG_GROUP,
            SUM(NVL(T.POSITION_INITIAL_VALUE,0)) AS POSITION_INITIAL_VALUE,
            T.INT_ADJ_ITEM,
            SUM(NVL(T.INT_ADJ_VAL,0)) AS INT_ADJ_VAL,
            T.ACCRUAL_GLNO,
            SUM(NVL(T.ACCRUAL,0)) AS ACCRUAL,
            T.FAIR_EXCH_ITEM,
            SUM(NVL(T.FAIR_EXCH_VAL,0)) AS FAIR_EXCH_VAL
       FROM BRD_SECURITY_POSI T
      WHERE T.BELONG_GROUP IN ('1', '2') --ȡ����ϵͳFNS
      AND T.DATANO=P_DATA_DT_STR
      GROUP BY T.ACCT_NO,
               T.SECURITY_REFERENCE,
               T.ORG_CD,
               T.SBJT_CD,
               T.BELONG_GROUP,
               T.INT_ADJ_ITEM,
               T.ACCRUAL_GLNO,
               T.FAIR_EXCH_ITEM
    ) t2
    on t1.bond_id=t2.SECURITY_REFERENCE
		LEFT JOIN		RWA_DEV.NCM_BUSINESS_DUEBILL T3														--�Ŵ���ݱ�
		ON					'CW_IMPORTDATA' || T1.BOND_ID = T3.THIRDPARTYACCOUNTS
		--AND					T3.BUSINESSTYPE IN ('1040102040','1040202011')						--1040102040-�����ծȯͶ��;1040202011-���ծȯͶ��
		AND					T3.DATANO = p_data_dt_str
    AND         T1.DATANO=T3.DATANO
		LEFT JOIN		RWA.ORG_INFO T4																            --RWA������
		ON					T1.DEPARTMENT = T4.ORGID
		LEFT JOIN		RWA_DEV.NCM_BOND_INFO T5																	--�Ŵ�ծȯ��Ϣ��
	 	ON					T3.RELATIVESERIALNO2 = T5.OBJECTNO
	  AND					T5.OBJECTTYPE = 'BusinessContract'
		AND					T5.DATANO = p_data_dt_str
	  LEFT JOIN NCM_CUSTOMER_INFO T6 --ͳһ�ͻ���Ϣ��
    ON					NVL(T5.BONDPUBLISHID,T3.CUSTOMERID) = T6.CUSTOMERID
	  AND					T6.DATANO = p_data_dt_str
    LEFT JOIN		RWA.CODE_LIBRARY	T7																			--RWA�����ȡ��ҵ
	  ON					T6.INDUSTRYTYPE = T7.ITEMNO
	  AND					T7.CODENO = 'IndustryType'
	  LEFT JOIN		RWA.CODE_LIBRARY	T8																			--RWA�����ȡҵ��Ʒ��
	  ON					T3.BUSINESSTYPE = T8.ITEMNO
	  AND					T8.CODENO = 'BusinessType'
	  LEFT JOIN		RWA_DEV.RWA_TEMP_PDLEVEL T9																--�ͻ��ڲ�������ʱ��
	  ON					NVL(T5.BONDPUBLISHID,T3.CUSTOMERID) = T9.CUSTID
	  LEFT JOIN		RWA_DEV.NCM_CLASSIFY_RECORD T10														--ʮ����������Ϣ��
	  ON 					T3.RELATIVESERIALNO2 = T10.OBJECTNO
    AND 				T10.OBJECTTYPE = 'TwelveClassify'
    AND 				T10.ISWORK = '1'
    AND 				T10.DATANO = p_data_dt_str
    LEFT JOIN		RWA_DEV.NCM_RWA_RISK_EXPO_RST T11									--��������¶��������
	  ON					T3.SERIALNO = T11.OBJECTNO
	  AND					T11.OBJECTTYPE = 'BusinessDuebill'
	  AND					T11.DATANO = p_data_dt_str
	  LEFT JOIN		RWA_DEV.RWA_CD_CODE_MAPPING T13														--����ӳ���
	  ON					T11.RISKEXPOSURERESULT = T13.SITEMNO
	  AND					T13.SYSID = 'XD'
	  AND					T13.SCODENO = 'RwaResultType'
	  AND					T13.ISINUSE = '1'
    LEFT JOIN (
    SELECT *
  FROM (SELECT T.DATANO,
               T.BOND_ID,
               T.PARTICIPANT_CODE,
               B.PARTICIPANT_NAME,
               ROW_NUMBER() OVER(PARTITION BY T.DATANO, T.BOND_ID, T.PARTICIPANT_CODE ORDER BY T.SORT_SEQ DESC) AS ROW_ID
          FROM FNS_BND_TRANSACTION_B T
          LEFT JOIN FNS_BND_PARTICIPANT_B B
            ON T.DATANO = B.DATANO
            AND T.DATANO=p_data_dt_str
           AND T.PARTICIPANT_CODE = B.PARTICIPANT_CODE
         WHERE T.PARTICIPANT_CODE IS NOT NULL)
 WHERE ROW_ID = 1) T14
     ON T1.DATANO = T14.DATANO
    AND T1.BOND_ID = T14.BOND_ID
	  WHERE 			T1.BOND_TYPE1 <> '060'
		AND					T1.BOND_ID NOT IN
								(SELECT ZQNM FROM RWA.RWA_WSIB_ABS_ISSUE_EXPOSURE WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
								 UNION ALL
								 SELECT ZQNM FROM RWA.RWA_WSIB_ABS_INVEST_EXPOSURE WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
								)
																																		--�����ñ��ų��ʲ�֤ȯ����ծȯ����
		AND 				T1.DATANO = p_data_dt_str														--ծȯ��Ϣ��,��ȡ��Ч��ծȯ��Ϣ
		AND					T1.BOND_CODE IS NOT NULL														--�ų���Ч��ծȯ����
	  ;

    COMMIT;
    
    -- ��
    INSERT INTO RWA_DEV.RWA_TZ_EXPOSURE(
                 DataDate                                                     --��������
                ,DataNo                                                       --������ˮ��
                ,ExposureID                                                   --���ձ�¶ID
                ,DueID                                                        --ծ��ID
                ,SSysID                                                       --ԴϵͳID
                ,ContractID                                                   --��ͬID
                ,ClientID                                                     --��������ID
                ,SOrgID                                                       --Դ����ID
                ,SOrgName                                                     --Դ��������
                ,OrgSortNo                                                    --�������������
                ,OrgID                                                        --��������ID
                ,OrgName                                                      --������������
                ,AccOrgID                                                     --�������ID
                ,AccOrgName                                                   --�����������
                ,IndustryID                                                   --������ҵ����
                ,IndustryName                                                 --������ҵ����
                ,BusinessLine                                                 --ҵ������
                ,AssetType                                                    --�ʲ�����
                ,AssetSubType                                                 --�ʲ�С��
                ,BusinessTypeID                                               --ҵ��Ʒ�ִ���
                ,BusinessTypeName                                             --ҵ��Ʒ������
                ,CreditRiskDataType                                           --���÷�����������
                ,AssetTypeOfHaircuts                                          --�ۿ�ϵ����Ӧ�ʲ����
                ,BusinessTypeSTD                                              --Ȩ�ط�ҵ������
                ,ExpoClassSTD                                                 --Ȩ�ط���¶����
                ,ExpoSubClassSTD                                              --Ȩ�ط���¶С��
                ,ExpoClassIRB                                                 --��������¶����
                ,ExpoSubClassIRB                                              --��������¶С��
                ,ExpoBelong                                                   --��¶������ʶ
                ,BookType                                                     --�˻����
                ,ReguTranType                                                 --��ܽ�������
                ,RepoTranFlag                                                 --�ع����ױ�ʶ
                ,RevaFrequency                                                --�ع�Ƶ��
                ,Currency                                                     --����
                ,NormalPrincipal                                              --�����������
                ,OverdueBalance                                               --�������
                ,NonAccrualBalance                                            --��Ӧ�����
                ,OnSheetBalance                                               --�������
                ,NormalInterest                                               --������Ϣ
                ,OnDebitInterest                                              --����ǷϢ
                ,OffDebitInterest                                             --����ǷϢ
                ,ExpenseReceivable                                            --Ӧ�շ���
                ,AssetBalance                                                 --�ʲ����
                ,AccSubject1                                                  --��Ŀһ
                ,AccSubject2                                                  --��Ŀ��
                ,AccSubject3                                                  --��Ŀ��
                ,StartDate                                                    --��ʼ����
                ,DueDate                                                      --��������
                ,OriginalMaturity                                             --ԭʼ����
                ,ResidualM                                                    --ʣ������
                ,RiskClassify                                                 --���շ���
                ,ExposureStatus                                               --���ձ�¶״̬
                ,OverdueDays                                                  --��������
                ,SpecialProvision                                             --ר��׼����
                ,GeneralProvision                                             --һ��׼����
                ,EspecialProvision                                            --�ر�׼����
                ,WrittenOffAmount                                             --�Ѻ������
                ,OffExpoSource                                                --���Ⱪ¶��Դ
                ,OffBusinessType                                              --����ҵ������
                ,OffBusinessSdvsSTD                                           --Ȩ�ط�����ҵ������ϸ��
                ,UncondCancelFlag                                             --�Ƿ����ʱ����������
                ,CCFLevel                                                     --����ת��ϵ������
                ,CCFAIRB                                                      --�߼�������ת��ϵ��
                ,ClaimsLevel                                                  --ծȨ����
                ,BondFlag                                                     --�Ƿ�Ϊծȯ
                ,BondIssueIntent                                              --ծȯ����Ŀ��
                ,NSURealPropertyFlag                                          --�Ƿ�����ò�����
                ,RepAssetTermType                                             --��ծ�ʲ���������
                ,DependOnFPOBFlag                                             --�Ƿ�����������δ��ӯ��
                ,IRating                                                      --�ڲ�����
                ,PD                                                           --ΥԼ����
                ,LGDLevel                                                     --ΥԼ��ʧ�ʼ���
                ,LGDAIRB                                                      --�߼���ΥԼ��ʧ��
                ,MAIRB                                                        --�߼�����Ч����
                ,EADAIRB                                                      --�߼���ΥԼ���ձ�¶
                ,DefaultFlag                                                  --ΥԼ��ʶ
                ,BEEL                                                         --��ΥԼ��¶Ԥ����ʧ����
                ,DefaultLGD                                                   --��ΥԼ��¶ΥԼ��ʧ��
                ,EquityExpoFlag                                               --��Ȩ��¶��ʶ
                ,EquityInvestType                                             --��ȨͶ�ʶ�������
                ,EquityInvestCause                                            --��ȨͶ���γ�ԭ��
                ,SLFlag                                                       --רҵ�����ʶ
                ,SLType                                                       --רҵ��������
                ,PFPhase                                                      --��Ŀ���ʽ׶�
                ,ReguRating                                                   --�������
                ,CBRCMPRatingFlag                                             --������϶������Ƿ��Ϊ����
                ,LargeFlucFlag                                                --�Ƿ񲨶��Խϴ�
                ,LiquExpoFlag                                                 --�Ƿ���������з��ձ�¶
                ,PaymentDealFlag                                              --�Ƿ����Ը�ģʽ
                ,DelayTradingDays                                             --�ӳٽ�������
                ,SecuritiesFlag                                               --�м�֤ȯ��ʶ
                ,SecuIssuerID                                                 --֤ȯ������ID
                ,RatingDurationType                                           --������������
                ,SecuIssueRating                                              --֤ȯ���еȼ�
                ,SecuResidualM                                                --֤ȯʣ������
                ,SecuRevaFrequency                                            --֤ȯ�ع�Ƶ��
                ,CCPTranFlag                                                  --�Ƿ����뽻�׶�����ؽ���
                ,CCPID                                                        --���뽻�׶���ID
                ,QualCCPFlag                                                  --�Ƿ�ϸ����뽻�׶���
                ,BankRole                                                     --���н�ɫ
                ,ClearingMethod                                               --���㷽ʽ
                ,BankAssetFlag                                                --�Ƿ������ύ�ʲ�
                ,MatchConditions                                              --�����������
                ,SFTFlag                                                      --֤ȯ���ʽ��ױ�ʶ
                ,MasterNetAgreeFlag                                           --���������Э���ʶ
                ,MasterNetAgreeID                                             --���������Э��ID
                ,SFTType                                                      --֤ȯ���ʽ�������
                ,SecuOwnerTransFlag                                           --֤ȯ����Ȩ�Ƿ�ת��
                ,OTCFlag                                                      --�����������߱�ʶ
                ,ValidNettingFlag                                             --��Ч�������Э���ʶ
                ,ValidNetAgreementID                                          --��Ч�������Э��ID
                ,OTCType                                                      --����������������
                ,DepositRiskPeriod                                            --��֤������ڼ�
                ,MTM                                                          --���óɱ�
                ,MTMCurrency                                                  --���óɱ�����
                ,BuyerOrSeller                                                --������
                ,QualROFlag                                                   --�ϸ�����ʲ���ʶ
                ,ROIssuerPerformFlag                                          --�����ʲ��������Ƿ�����Լ
                ,BuyerInsolvencyFlag                                          --���ñ������Ƿ��Ʋ�
                ,NonpaymentFees                                               --��δ֧������
                ,RetailExpoFlag                                               --���۱�¶��ʶ
                ,RetailClaimType                                              --����ծȨ����
                ,MortgageType                                                 --ס����Ѻ��������
                ,ExpoNumber                                                   --���ձ�¶����
                ,LTV                                                          --�����ֵ��
                ,Aging                                                        --����
                ,NewDefaultDebtFlag                                           --����ΥԼծ���ʶ
                ,PDPoolModelID                                                --PD�ֳ�ģ��ID
                ,LGDPoolModelID                                               --LGD�ֳ�ģ��ID
                ,CCFPoolModelID                                               --CCF�ֳ�ģ��ID
                ,PDPoolID                                                     --����PD��ID
                ,LGDPoolID                                                    --����LGD��ID
                ,CCFPoolID                                                    --����CCF��ID
                ,ABSUAFlag                                                    --�ʲ�֤ȯ�������ʲ���ʶ
                ,ABSPoolID                                                    --֤ȯ���ʲ���ID
                ,GroupID                                                      --������
                ,DefaultDate                                                  --ΥԼʱ��
                ,ABSPROPORTION                                                --�ʲ�֤ȯ������
                ,DEBTORNUMBER                                                 --����˸���
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
                           AND NVL(INITIAL_COST, 0) + NVL(MKT_VALUE_CHANGE, 0) + NVL(ACCOUNTABLE_INT, 0) <> 0 --NVL(INT_ADJUST, 0) + ����Ϣ�������⣬��Ϊ���ֹ�����
    )
    SELECT
                TO_DATE(p_data_dt_str,'YYYYMMDD')                                       AS DataDate               --��������
                ,p_data_dt_str                                                          AS DataNo                 --������ˮ��
                ,NVL(T3.SERIALNO,T1.BOND_ID)                                            AS ExposureID             --���ձ�¶ID
                ,T1.BOND_ID                                                             AS DueID                  --ծ��ID
                ,'TZ'                                                                   AS SSysID                 --ԴϵͳID                       Ĭ�� Ͷ��(TZ)
                ,T1.BOND_ID                                                             AS ContractID             --��ͬID
                ,CASE WHEN REPLACE(T3.CUSTOMERID,'NCM_','') IS NULL THEN 'XN-YBGS'
                      ELSE T3.CUSTOMERID
                 END                                                                    AS ClientID               --��������ID
                ,T1.DEPARTMENT                                                          AS SOrgID                 --Դ����ID
                ,T4.ORGNAME                                                             AS SOrgName               --Դ��������
                ,T4.SORTNO                                                              AS OrgSortNo              --�������������
                ,T1.DEPARTMENT                                                          AS OrgID                  --��������ID
                ,T4.ORGNAME                                                             AS OrgName                --������������
                ,T1.DEPARTMENT                                                          AS AccOrgID               --�������ID
                ,T4.ORGNAME                                                             AS AccOrgName             --�����������
                ,CASE WHEN NVL(T5.INDUSTRYTYPE,DECODE(SUBSTR(T5.RWACUSTOMERTYPE,1,2),'02','J6620','J66')) = '999999' THEN 'J66'
                      ELSE NVL(T5.INDUSTRYTYPE,DECODE(SUBSTR(T5.RWACUSTOMERTYPE,1,2),'02','J6620','J66'))
                 END                                                                    AS IndustryID              --������ҵ����
                ,CASE WHEN NVL(T6.ITEMNAME,DECODE(SUBSTR(T5.RWACUSTOMERTYPE,1,2),'02','�������з���','���ҽ��ڷ���')) = 'δ֪' THEN '���ҽ��ڷ���'
                      ELSE NVL(T6.ITEMNAME,DECODE(SUBSTR(T5.RWACUSTOMERTYPE,1,2),'02','�������з���','���ҽ��ڷ���'))
                 END                                                                    AS IndustryName            --������ҵ����
                ,'0401'                                                                 AS BusinessLine           --ҵ������                        Ĭ�� ͬҵ(04)
                ,''                                                                     AS AssetType              --�ʲ�����                        Ĭ�� NULL RWA�������
                ,''                                                                     AS AssetSubType           --�ʲ�С��                        Ĭ�� NULL RWA�������
                ,CASE WHEN T1.BOND_TYPE1 = '081' THEN '1040105061'
                      WHEN T1.BOND_TYPE1 = '100' THEN '1040105062'
                      ELSE '1040105060'
                 END                                                                    AS BusinessTypeID         --ҵ��Ʒ�ִ���
                ,CASE WHEN T1.BOND_TYPE1 = '081' THEN 'Ӧ�տ�����Ͷ��_����'
                      WHEN T1.BOND_TYPE1 = '100' THEN 'Ӧ�տ�����Ͷ��_ͬҵ�浥'
                      ELSE 'Ӧ�տ�����Ͷ��'
                 END                                                                    AS BusinessTypeName       --ҵ��Ʒ������
                ,'01'                                                                   AS CreditRiskDataType     --���÷�����������                Ĭ�� һ�������(01)
                ,'01'                                                                   AS AssetTypeOfHaircuts    --�ۿ�ϵ����Ӧ�ʲ����            Ĭ�� �ֽ��ֽ�ȼ���(01)
                ,'07'                                                                   AS BusinessTypeSTD        --Ȩ�ط�ҵ������                 Ĭ�� һ���ʲ�(07)
                ,CASE WHEN REPLACE(T3.CUSTOMERID,'NCM_','') IS NULL THEN '0112'                                   --�ͻ�Ϊ�գ�Ĭ�� ����(0112)
                      ELSE ''
                 END                                                                    AS ExpoClassSTD           --Ȩ�ط���¶����                 Ĭ�� NULL RWA������
                ,CASE WHEN REPLACE(T3.CUSTOMERID,'NCM_','') IS NULL THEN '011216'                                 --�ͻ�Ϊ�գ�Ĭ�� ��������100%����Ȩ�ص��ʲ�(011216)
                      ELSE ''
                 END                                                                    AS ExpoSubClassSTD        --Ȩ�ط���¶С��                 Ĭ�� NULL RWA������
                ,SUBSTR(T13.DITEMNO,1,4)                                                AS ExpoClassIRB           --��������¶����                 Ĭ�� NULL RWA������
                ,T13.DITEMNO                                                            AS ExpoSubClassIRB        --��������¶С��                 Ĭ�� NULL RWA������
                ,'01'                                                                   AS ExpoBelong             --��¶������ʶ                    Ĭ�ϣ�����(01)
                ,CASE WHEN T1.ASSET_CLASS = '10' THEN '02'
                      ELSE '01'
                 END                                                                    AS BookType               --�˻����                        �ʲ����� �� �������Խ����ʲ�(10)�� , ��Ϊ02-�����˻����ʲ����� �� �������Խ����ʲ���  , ��Ϊ01-�����˻�
                ,'02'                                                                   AS ReguTranType           --��ܽ�������                    Ĭ�� �����ʱ��г�����(02)
                ,'0'                                                                    AS RepoTranFlag           --�ع����ױ�ʶ                    Ĭ�� ��(0)
                ,1                                                                      AS RevaFrequency          --�ع�Ƶ��                        Ĭ��  1
                ,NVL(T1.CURRENCY_CODE,'CNY')                                            AS Currency               --����
                ,NVL(T2.INITIAL_COST,0) + NVL(T2.MKT_VALUE_CHANGE,0) + NVL(T2.ACCOUNTABLE_INT,0)
                                                                                        AS NormalPrincipal        --�����������                    �����������ɱ�����Ϣ����(initial_cost)�����ʼ�ֵ�䶯/���ʼ�ֵ�䶯����(int_adjust)��Ӧ����Ϣ(mkt_value_change)
                ,0                                                                      AS OverdueBalance         --�������                        Ĭ�� 0
                ,0                                                                      AS NonAccrualBalance      --��Ӧ�����                     Ĭ�� 0
                ,NVL(T2.INITIAL_COST,0) + NVL(T2.MKT_VALUE_CHANGE,0) + NVL(T2.ACCOUNTABLE_INT,0)
                                                                                        AS OnSheetBalance         --�������                        �������=�����������+�������+��Ӧ�����
                ,0                                                                      AS NormalInterest         --������Ϣ                        Ĭ�� 0
                ,0                                                                      AS OnDebitInterest        --����ǷϢ                        Ĭ�� 0
                ,0                                                                      AS OffDebitInterest       --����ǷϢ                        Ĭ�� 0
                ,0                                                                      AS ExpenseReceivable      --Ӧ�շ���                        Ĭ�� 0
                ,NVL(T2.INITIAL_COST,0) + NVL(T2.MKT_VALUE_CHANGE,0) + NVL(T2.ACCOUNTABLE_INT,0)
                                                                                        AS AssetBalance           --�ʲ����                        �����ʲ������=�������+Ӧ�շ���+������Ϣ+����ǷϢ
                ,CASE WHEN T1.ASSET_CLASS = '50' THEN '12220400'
                      WHEN T1.ASSET_CLASS = '60' THEN '12220701'
                 END                                                                    AS AccSubject1            --��Ŀһ                         ����ԭϵͳ���ʲ�������ջ�ƿ�Ŀ��ȷ��
                ,''                                                                     AS AccSubject2            --��Ŀ��                         Ĭ�� NULL
                ,''                                                                     AS AccSubject3            --��Ŀ��                         Ĭ�� NULL
                ,T1.EFFECT_DATE                                                         AS StartDate              --��ʼ����
                ,T1.MATURITY_DATE                                                       AS DueDate                --��������
                ,CASE WHEN (TO_DATE(T1.MATURITY_DATE,'YYYYMMDD') - TO_DATE(T1.EFFECT_DATE,'YYYYMMDD')) / 365<0
                      THEN 0
                      ELSE (TO_DATE(T1.MATURITY_DATE,'YYYYMMDD') - TO_DATE(T1.EFFECT_DATE,'YYYYMMDD')) / 365
                 END                                                                    AS OriginalMaturity       --ԭʼ����                        ��λ ��
                ,CASE WHEN (TO_DATE(T1.MATURITY_DATE,'YYYYMMDD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365<0
                      THEN 0
                      ELSE (TO_DATE(T1.MATURITY_DATE,'YYYYMMDD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365
                 END                                                                    AS ResidualM              --ʣ������                        ��λ ��
                ,CASE WHEN T18.FINALLYRESULT IN ('A1','A2','A3','A4','A') THEN '01'                               --ʮ��������תΪ�弶����
                      WHEN T18.FINALLYRESULT IN ('B1','B2','B3','B') THEN '02'
                      WHEN T18.FINALLYRESULT IN ('C1','C2','C') THEN '03'
                      WHEN T18.FINALLYRESULT IN ('D1','D2','D') THEN '04'
                      WHEN T18.FINALLYRESULT = 'E' THEN '05'
                      ELSE '01'
                 END                                                                    AS RiskClassify           --���շ���                        Ĭ�� ����(01)
                ,''                                                                     AS ExposureStatus         --���ձ�¶״̬                    Ĭ�� NULL
                ,0                                                                      AS OverdueDays            --��������                        Ĭ�� 0
                ,0                                                                      AS SpecialProvision       --ר��׼����                     Ĭ�� 0 RWA���� ��Ŀ12220400��ֱ����1%��׼����
                ,0                                                                      AS GeneralProvision       --һ��׼����                     Ĭ�� 0 RWA����
                ,0                                                                      AS EspecialProvision      --�ر�׼����                     Ĭ�� 0 RWA����
                ,0                                                                      AS WrittenOffAmount       --�Ѻ������                     Ĭ�� 0
                ,''                                                                     AS OffExpoSource          --���Ⱪ¶��Դ                    Ĭ�� NULL
                ,''                                                                     AS OffBusinessType        --����ҵ������                    Ĭ�� NULL
                ,''                                                                     AS OffBusinessSdvsSTD     --Ȩ�ط�����ҵ������ϸ��         Ĭ�� NULL
                ,''                                                                     AS UncondCancelFlag       --�Ƿ����ʱ����������            Ĭ�� NULL
                ,''                                                                     AS CCFLevel               --����ת��ϵ������                Ĭ�� NULL
                ,NULL                                                                   AS CCFAIRB                --�߼�������ת��ϵ��             Ĭ�� NULL
                ,CASE WHEN T1.BOND_TYPE2 = '20' THEN '02'
                      ELSE '01'
                 END                                                                    AS ClaimsLevel            --ծȨ����                        ծȯ����2���μ�ծȯ(20)����ծȨ���𣽴μ�ծȨ(02)������Ϊ�߼�ծȨ(01)
                ,'0'                                                                    AS BondFlag               --�Ƿ�Ϊծȯ                     Ĭ�� ��(0)
                ,'02'                                                                   AS BondIssueIntent        --ծȯ����Ŀ��                    Ĭ�� ����(02)
                ,'0'                                                                    AS NSURealPropertyFlag    --�Ƿ�����ò�����                Ĭ�� ��(0)
                ,''                                                                     AS RepAssetTermType       --��ծ�ʲ���������                Ĭ�� NULL
                ,'0'                                                                    AS DependOnFPOBFlag       --�Ƿ�����������δ��ӯ��         Ĭ�� ��(0)
                ,T8.PDADJLEVEL                                                          AS IRating                --�ڲ�����
                ,T8.PD                                                                  AS PD                     --ΥԼ����
                ,''                                                                     AS LGDLevel               --ΥԼ��ʧ�ʼ���                 Ĭ�� NULL
                ,NULL                                                                   AS LGDAIRB                --�߼���ΥԼ��ʧ��                Ĭ�� NULL
                ,NULL                                                                   AS MAIRB                  --�߼�����Ч����                 Ĭ�� NULL
                ,NULL                                                                   AS EADAIRB                --�߼���ΥԼ���ձ�¶             Ĭ�� NULL
                ,CASE WHEN T8.PDADJLEVEL = '0116' THEN '1'
                 ELSE '0'
                 END                                                                    AS DefaultFlag            --ΥԼ��ʶ
                ,CASE WHEN T1.BOND_TYPE2 = '20' THEN 0.75
                 ELSE 0.45
                 END                                                                    AS BEEL                   --��ΥԼ��¶Ԥ����ʧ����         ծȨ���𣽡��߼�ծȨ����BEEL �� 45%��ծȨ���𣽡��μ�ծȨ����BEEL �� 75%
                ,CASE WHEN T1.BOND_TYPE2 = '20' THEN 0.75
                 ELSE 0.45
                 END                                                                    AS DefaultLGD             --��ΥԼ��¶ΥԼ��ʧ��            Ĭ�� NULL
                ,'0'                                                                    AS EquityExpoFlag         --��Ȩ��¶��ʶ                    Ĭ�� ��(0)
                ,''                                                                     AS EquityInvestType       --��ȨͶ�ʶ�������                Ĭ�� NULL
                ,''                                                                     AS EquityInvestCause      --��ȨͶ���γ�ԭ��                Ĭ�� NULL
                ,'0'                                                                    AS SLFlag                 --רҵ�����ʶ                    Ĭ�� ��(0)
                ,''                                                                     AS SLType                 --רҵ��������                    Ĭ�� NULL
                ,''                                                                     AS PFPhase                --��Ŀ���ʽ׶�                    Ĭ�� NULL
                ,'01'                                                                   AS ReguRating             --�������                        Ĭ�� ��(01)
                ,''                                                                     AS CBRCMPRatingFlag       --������϶������Ƿ��Ϊ����       Ĭ�� NULL
                ,''                                                                     AS LargeFlucFlag          --�Ƿ񲨶��Խϴ�                 Ĭ�� NULL
                ,'0'                                                                    AS LiquExpoFlag           --�Ƿ���������з��ձ�¶         Ĭ�� ��(0)
                ,'1'                                                                    AS PaymentDealFlag        --�Ƿ����Ը�ģʽ                Ĭ�� ��(1)
                ,NULL                                                                   AS DelayTradingDays       --�ӳٽ�������                    Ĭ�� NULL
                ,'0'                                                                    AS SecuritiesFlag         --�м�֤ȯ��ʶ                    Ĭ�� ��(0)
                ,''                                                                     AS SecuIssuerID           --֤ȯ������ID                   Ĭ�� NULL
                ,''                                                                     AS RatingDurationType     --������������                    Ĭ�� NULL
                ,''                                                                     AS SecuIssueRating        --֤ȯ���еȼ�                    Ĭ�� NULL
                ,NULL                                                                   AS SecuResidualM          --֤ȯʣ������                    Ĭ�� NULL
                ,1                                                                      AS SecuRevaFrequency      --֤ȯ�ع�Ƶ��                    Ĭ�� 1
                ,'0'                                                                    AS CCPTranFlag            --�Ƿ����뽻�׶�����ؽ���        Ĭ�� ��(0)
                ,''                                                                     AS CCPID                  --���뽻�׶���ID                  Ĭ�� NULL
                ,''                                                                     AS QualCCPFlag            --�Ƿ�ϸ����뽻�׶���            Ĭ�� NULL
                ,''                                                                     AS BankRole               --���н�ɫ                        Ĭ�� NULL
                ,''                                                                     AS ClearingMethod         --���㷽ʽ                        Ĭ�� NULL
                ,''                                                                     AS BankAssetFlag          --�Ƿ������ύ�ʲ�                Ĭ�� NULL
                ,''                                                                     AS MatchConditions        --�����������                    Ĭ�� NULL
                ,'0'                                                                    AS SFTFlag                --֤ȯ���ʽ��ױ�ʶ                Ĭ�� ��(0)
                ,'0'                                                                    AS MasterNetAgreeFlag     --���������Э���ʶ             Ĭ�� ��(0)
                ,''                                                                     AS MasterNetAgreeID       --���������Э��ID               Ĭ�� NULL
                ,''                                                                     AS SFTType                --֤ȯ���ʽ�������                Ĭ�� NULL
                ,''                                                                     AS SecuOwnerTransFlag     --֤ȯ����Ȩ�Ƿ�ת��             Ĭ�� NULL
                ,'0'                                                                    AS OTCFlag                --�����������߱�ʶ                Ĭ�� ��(0)
                ,''                                                                     AS ValidNettingFlag       --��Ч�������Э���ʶ            Ĭ�� NULL
                ,''                                                                     AS ValidNetAgreementID    --��Ч�������Э��ID              Ĭ�� NULL
                ,''                                                                     AS OTCType                --����������������                Ĭ�� NULL
                ,''                                                                     AS DepositRiskPeriod      --��֤������ڼ�                 Ĭ�� NULL
                ,''                                                                     AS MTM                    --���óɱ�                        Ĭ�� NULL
                ,''                                                                     AS MTMCurrency            --���óɱ�����                    Ĭ�� NULL
                ,''                                                                     AS BuyerOrSeller          --������                        Ĭ�� NULL
                ,''                                                                     AS QualROFlag             --�ϸ�����ʲ���ʶ                Ĭ�� NULL
                ,''                                                                     AS ROIssuerPerformFlag    --�����ʲ��������Ƿ�����Լ        Ĭ�� NULL
                ,''                                                                     AS BuyerInsolvencyFlag    --���ñ������Ƿ��Ʋ�            Ĭ�� NULL
                ,''                                                                     AS NonpaymentFees         --��δ֧������                    Ĭ�� NULL
                ,'0'                                                                    AS RetailExpoFlag         --���۱�¶��ʶ                    Ĭ�� ��(0)
                ,''                                                                     AS RetailClaimType        --����ծȨ����                    Ĭ�� NULL
                ,''                                                                     AS MortgageType           --ס����Ѻ��������                Ĭ�� NULL
                ,1                                                                      AS ExpoNumber             --���ձ�¶����                    Ĭ�� 1
                ,0.8                                                                    AS LTV                    --�����ֵ��                     Ĭ�� 0.8
                ,NULL                                                                   AS Aging                  --����                            Ĭ�� NULL
                ,''                                                                     AS NewDefaultDebtFlag     --����ΥԼծ���ʶ                Ĭ�� NULL
                ,''                                                                     AS PDPoolModelID          --PD�ֳ�ģ��ID                    Ĭ�� NULL
                ,''                                                                     AS LGDPoolModelID         --LGD�ֳ�ģ��ID                   Ĭ�� NULL
                ,''                                                                     AS CCFPoolModelID         --CCF�ֳ�ģ��ID                   Ĭ�� NULL
                ,''                                                                     AS PDPoolID               --����PD��ID                     Ĭ�� NULL
                ,''                                                                     AS LGDPoolID              --����LGD��ID                    Ĭ�� NULL
                ,''                                                                     AS CCFPoolID              --����CCF��ID                    Ĭ�� NULL
                ,'0'                                                                    AS ABSUAFlag              --�ʲ�֤ȯ�������ʲ���ʶ         Ĭ�� ��(0)
                ,''                                                                     AS ABSPoolID              --֤ȯ���ʲ���ID                  Ĭ�� NULL
                ,''                                                                     AS GroupID                --������                        Ĭ�� NULL
                ,CASE WHEN T8.PDADJLEVEL = '0116' THEN TO_DATE(T8.PDVAVLIDDATE,'YYYYMMDD')
                 ELSE NULL
                 END                                                                    AS DefaultDate            --ΥԼʱ��
                ,NULL                                                                   AS ABSPROPORTION          --�ʲ�֤ȯ������
                ,NULL                                                                   AS DEBTORNUMBER           --����˸���

    FROM        RWA_DEV.FNS_BND_INFO_B T1
    INNER JOIN  TEMP_BND_BOOK T2
    ON          T1.BOND_ID = T2.BOND_ID
    INNER JOIN  RWA_DEV.NCM_BUSINESS_DUEBILL T3                           --�Ŵ���ݱ�
    ON          'CW_IMPORTDATA' || T1.BOND_ID = T3.THIRDPARTYACCOUNTS
    AND         T3.DATANO = p_data_dt_str
    LEFT JOIN   RWA.ORG_INFO T4                                           --RWA������
    ON          T1.DEPARTMENT = T4.ORGID
    LEFT JOIN NCM_CUSTOMER_INFO T5 --ͳһ�ͻ���Ϣ��
    ON          T3.CUSTOMERID = T5.CUSTOMERID
    AND         T5.DATANO = p_data_dt_str
    LEFT JOIN   RWA.CODE_LIBRARY  T6                                      --RWA�����ȡ��ҵ
    ON          T5.INDUSTRYTYPE = T6.ITEMNO
    AND         T6.CODENO = 'IndustryType'
    LEFT JOIN   RWA.CODE_LIBRARY  T7                                      --RWA�����ȡҵ��Ʒ��
    ON          T3.BUSINESSTYPE = T7.ITEMNO
    AND         T7.CODENO = 'BusinessType'
    LEFT JOIN   RWA_DEV.RWA_TEMP_PDLEVEL T8                               --�ͻ��ڲ�������ʱ��
    ON          T3.CUSTOMERID = T8.CUSTID
    INNER JOIN  RWA_DEV.NCM_BUSINESS_CONTRACT T10
    ON          T3.RELATIVESERIALNO2 = T10.SERIALNO
    AND         T10.BUSINESSSUBTYPE LIKE '0010%'                          --��������
    AND         T10.DATANO = p_data_dt_str
    LEFT JOIN   RWA_DEV.NCM_RWA_RISK_EXPO_RST T12                 --��������¶��������
    ON          T3.SERIALNO = T12.OBJECTNO
    AND         T12.OBJECTTYPE = 'BusinessDuebill'
    AND         T12.DATANO = p_data_dt_str
    LEFT JOIN   RWA_DEV.RWA_CD_CODE_MAPPING T13                           --����ӳ���
    ON          T12.RISKEXPOSURERESULT = T13.SITEMNO
    AND         T13.SYSID = 'XD'
    AND         T13.SCODENO = 'RwaResultType'
    AND         T13.ISINUSE = '1'
    /*
    LEFT JOIN   (SELECT TR.BOND_ID
                       ,TO_NUMBER(REPLACE(TR.RESERVESUM,',','')) AS RESERVESUM
                   FROM RWA.RWA_WS_RESERVE TR
             INNER JOIN RWA.RWA_WP_DATASUPPLEMENT TD                      --���ݲ�¼��
                     ON TR.SUPPORGID = TD.ORGID
                    AND TD.DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
                    AND TD.SUPPTMPLID = 'M-0210'
                    AND TD.SUBMITFLAG = '1'
                  WHERE TR.DATADATE = to_date(p_data_dt_str,'YYYYMMDD')
                ) T17                                                     --Ӧ�տ�Ͷ��׼����¼��
    ON          T1.BOND_ID = T17.BOND_ID
    */
    LEFT JOIN   RWA_DEV.NCM_CLASSIFY_RECORD T18                           --ʮ����������Ϣ��
    ON          T3.RELATIVESERIALNO2 = T18.OBJECTNO
    AND         T18.OBJECTTYPE = 'TwelveClassify'
    AND         T18.ISWORK = '1'
    AND         T18.DATANO = p_data_dt_str
    WHERE       T1.ASSET_CLASS IN ('50','60')                       --ͨ���ʲ�������ȷ��ծȯ����Ӧ�տ�Ͷ�ʡ�
    AND         T1.DATANO = p_data_dt_str                           --ծȯ��Ϣ��,��ȡ��Ч��ծȯ��Ϣ
    AND         T1.BOND_CODE IS NOT NULL                            --�ų���Ч��ծȯ����
    ;

    COMMIT;

    --���Ӧ�տ�Ͷ��ʵ�������˿ͻ���ʱ��
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_TMP_TZCUST';

    --���뵱��Ӧ�տ�Ͷ��ʵ�������˿ͻ����� ��
    INSERT INTO RWA_DEV.RWA_TMP_TZCUST
    SELECT CONTRACTNO,
           CUSTOMERID,
           PDLEVEL,
           PD,
           PDVAVLIDDATE,
           RWACUSTOMERTYPE,
           INDUSTRYTYPE
      FROM (SELECT T1.SERIALNO AS CONTRACTNO,
                   NVL(T2.CUSTOMERID, T3.CUSTOMERID) AS CUSTOMERID,
                   T4.PDADJLEVEL AS PDLEVEL,
                   T4.PD AS PD,
                   T4.PDVAVLIDDATE AS PDVAVLIDDATE,
                   CASE
                     WHEN T2.CUSTOMERID IS NOT NULL THEN
                      T2.RWACUSTOMERTYPE
                     ELSE
                      T3.RWACUSTOMERTYPE
                   END AS RWACUSTOMERTYPE,
                   CASE
                     WHEN T2.CUSTOMERID IS NOT NULL THEN
                      T2.INDUSTRYTYPE
                     ELSE
                      T3.INDUSTRYTYPE
                   END AS INDUSTRYTYPE,
                   ROW_NUMBER() OVER(PARTITION BY T1.SERIALNO ORDER BY T4.PDADJLEVEL DESC,CASE
                     WHEN T2.CUSTOMERID IS NOT NULL THEN
                      T2.RWACUSTOMERTYPE
                     ELSE
                      T3.RWACUSTOMERTYPE
                   END DESC) AS RN
              FROM RWA_DEV.NCM_CONTRACT_RELATIVE T1
             INNER JOIN RWA_DEV.NCM_BUSINESS_DUEBILL T8
                ON T1.SERIALNO = T8.RELATIVESERIALNO2
               AND T8.BUSINESSTYPE = '1040105060'
               AND T8.DATANO = p_data_dt_str
              LEFT JOIN NCM_CUSTOMER_INFO T2 --ͳһ�ͻ���Ϣ��
                ON T1.OBJECTNO = T2.CUSTOMERID
               AND T2.DATANO = p_data_dt_str
              LEFT JOIN NCM_CUSTOMER_INFO T3 --ͳһ�ͻ���Ϣ��
                ON SUBSTR(T1.OBJECTNO, 5) = T3.customerid
               AND T3.DATANO = p_data_dt_str
              LEFT JOIN RWA_DEV.RWA_TEMP_PDLEVEL T4
                ON NVL(T2.CUSTOMERID, T3.CUSTOMERID) = T4.CUSTID
             WHERE T1.OBJECTTYPE = 'Financier'
               AND T1.DATANO = p_data_dt_str)
     WHERE RN = 1
     ;

    COMMIT;

    --����Ӧ�տ�Ͷ��ʵ�������˿ͻ���ʱ��
    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_TMP_TZCUST',cascade => true);


    --2.3 ����ϵͳ-Ӧ�տ�Ͷ��-�ǻ�������-ʵ�������� ��
    INSERT INTO RWA_DEV.RWA_TZ_EXPOSURE(
                 DataDate                                                     --��������
                ,DataNo                                                       --������ˮ��
                ,ExposureID                                                   --���ձ�¶ID
                ,DueID                                                        --ծ��ID
                ,SSysID                                                       --ԴϵͳID
                ,ContractID                                                   --��ͬID
                ,ClientID                                                     --��������ID
                ,SOrgID                                                       --Դ����ID
                ,SOrgName                                                     --Դ��������
                ,OrgSortNo                                                    --�������������
                ,OrgID                                                        --��������ID
                ,OrgName                                                      --������������
                ,AccOrgID                                                     --�������ID
                ,AccOrgName                                                   --�����������
                ,IndustryID                                                   --������ҵ����
                ,IndustryName                                                 --������ҵ����
                ,BusinessLine                                                 --ҵ������
                ,AssetType                                                    --�ʲ�����
                ,AssetSubType                                                 --�ʲ�С��
                ,BusinessTypeID                                               --ҵ��Ʒ�ִ���
                ,BusinessTypeName                                             --ҵ��Ʒ������
                ,CreditRiskDataType                                           --���÷�����������
                ,AssetTypeOfHaircuts                                          --�ۿ�ϵ����Ӧ�ʲ����
                ,BusinessTypeSTD                                              --Ȩ�ط�ҵ������
                ,ExpoClassSTD                                                 --Ȩ�ط���¶����
                ,ExpoSubClassSTD                                              --Ȩ�ط���¶С��
                ,ExpoClassIRB                                                 --��������¶����
                ,ExpoSubClassIRB                                              --��������¶С��
                ,ExpoBelong                                                   --��¶������ʶ
                ,BookType                                                     --�˻����
                ,ReguTranType                                                 --��ܽ�������
                ,RepoTranFlag                                                 --�ع����ױ�ʶ
                ,RevaFrequency                                                --�ع�Ƶ��
                ,Currency                                                     --����
                ,NormalPrincipal                                              --�����������
                ,OverdueBalance                                               --�������
                ,NonAccrualBalance                                            --��Ӧ�����
                ,OnSheetBalance                                               --�������
                ,NormalInterest                                               --������Ϣ
                ,OnDebitInterest                                              --����ǷϢ
                ,OffDebitInterest                                             --����ǷϢ
                ,ExpenseReceivable                                            --Ӧ�շ���
                ,AssetBalance                                                 --�ʲ����
                ,AccSubject1                                                  --��Ŀһ
                ,AccSubject2                                                  --��Ŀ��
                ,AccSubject3                                                  --��Ŀ��
                ,StartDate                                                    --��ʼ����
                ,DueDate                                                      --��������
                ,OriginalMaturity                                             --ԭʼ����
                ,ResidualM                                                    --ʣ������
                ,RiskClassify                                                 --���շ���
                ,ExposureStatus                                               --���ձ�¶״̬
                ,OverdueDays                                                  --��������
                ,SpecialProvision                                             --ר��׼����
                ,GeneralProvision                                             --һ��׼����
                ,EspecialProvision                                            --�ر�׼����
                ,WrittenOffAmount                                             --�Ѻ������
                ,OffExpoSource                                                --���Ⱪ¶��Դ
                ,OffBusinessType                                              --����ҵ������
                ,OffBusinessSdvsSTD                                           --Ȩ�ط�����ҵ������ϸ��
                ,UncondCancelFlag                                             --�Ƿ����ʱ����������
                ,CCFLevel                                                     --����ת��ϵ������
                ,CCFAIRB                                                      --�߼�������ת��ϵ��
                ,ClaimsLevel                                                  --ծȨ����
                ,BondFlag                                                     --�Ƿ�Ϊծȯ
                ,BondIssueIntent                                              --ծȯ����Ŀ��
                ,NSURealPropertyFlag                                          --�Ƿ�����ò�����
                ,RepAssetTermType                                             --��ծ�ʲ���������
                ,DependOnFPOBFlag                                             --�Ƿ�����������δ��ӯ��
                ,IRating                                                      --�ڲ�����
                ,PD                                                           --ΥԼ����
                ,LGDLevel                                                     --ΥԼ��ʧ�ʼ���
                ,LGDAIRB                                                      --�߼���ΥԼ��ʧ��
                ,MAIRB                                                        --�߼�����Ч����
                ,EADAIRB                                                      --�߼���ΥԼ���ձ�¶
                ,DefaultFlag                                                  --ΥԼ��ʶ
                ,BEEL                                                         --��ΥԼ��¶Ԥ����ʧ����
                ,DefaultLGD                                                   --��ΥԼ��¶ΥԼ��ʧ��
                ,EquityExpoFlag                                               --��Ȩ��¶��ʶ
                ,EquityInvestType                                             --��ȨͶ�ʶ�������
                ,EquityInvestCause                                            --��ȨͶ���γ�ԭ��
                ,SLFlag                                                       --רҵ�����ʶ
                ,SLType                                                       --רҵ��������
                ,PFPhase                                                      --��Ŀ���ʽ׶�
                ,ReguRating                                                   --�������
                ,CBRCMPRatingFlag                                             --������϶������Ƿ��Ϊ����
                ,LargeFlucFlag                                                --�Ƿ񲨶��Խϴ�
                ,LiquExpoFlag                                                 --�Ƿ���������з��ձ�¶
                ,PaymentDealFlag                                              --�Ƿ����Ը�ģʽ
                ,DelayTradingDays                                             --�ӳٽ�������
                ,SecuritiesFlag                                               --�м�֤ȯ��ʶ
                ,SecuIssuerID                                                 --֤ȯ������ID
                ,RatingDurationType                                           --������������
                ,SecuIssueRating                                              --֤ȯ���еȼ�
                ,SecuResidualM                                                --֤ȯʣ������
                ,SecuRevaFrequency                                            --֤ȯ�ع�Ƶ��
                ,CCPTranFlag                                                  --�Ƿ����뽻�׶�����ؽ���
                ,CCPID                                                        --���뽻�׶���ID
                ,QualCCPFlag                                                  --�Ƿ�ϸ����뽻�׶���
                ,BankRole                                                     --���н�ɫ
                ,ClearingMethod                                               --���㷽ʽ
                ,BankAssetFlag                                                --�Ƿ������ύ�ʲ�
                ,MatchConditions                                              --�����������
                ,SFTFlag                                                      --֤ȯ���ʽ��ױ�ʶ
                ,MasterNetAgreeFlag                                           --���������Э���ʶ
                ,MasterNetAgreeID                                             --���������Э��ID
                ,SFTType                                                      --֤ȯ���ʽ�������
                ,SecuOwnerTransFlag                                           --֤ȯ����Ȩ�Ƿ�ת��
                ,OTCFlag                                                      --�����������߱�ʶ
                ,ValidNettingFlag                                             --��Ч�������Э���ʶ
                ,ValidNetAgreementID                                          --��Ч�������Э��ID
                ,OTCType                                                      --����������������
                ,DepositRiskPeriod                                            --��֤������ڼ�
                ,MTM                                                          --���óɱ�
                ,MTMCurrency                                                  --���óɱ�����
                ,BuyerOrSeller                                                --������
                ,QualROFlag                                                   --�ϸ�����ʲ���ʶ
                ,ROIssuerPerformFlag                                          --�����ʲ��������Ƿ�����Լ
                ,BuyerInsolvencyFlag                                          --���ñ������Ƿ��Ʋ�
                ,NonpaymentFees                                               --��δ֧������
                ,RetailExpoFlag                                               --���۱�¶��ʶ
                ,RetailClaimType                                              --����ծȨ����
                ,MortgageType                                                 --ס����Ѻ��������
                ,ExpoNumber                                                   --���ձ�¶����
                ,LTV                                                          --�����ֵ��
                ,Aging                                                        --����
                ,NewDefaultDebtFlag                                           --����ΥԼծ���ʶ
                ,PDPoolModelID                                                --PD�ֳ�ģ��ID
                ,LGDPoolModelID                                               --LGD�ֳ�ģ��ID
                ,CCFPoolModelID                                               --CCF�ֳ�ģ��ID
                ,PDPoolID                                                     --����PD��ID
                ,LGDPoolID                                                    --����LGD��ID
                ,CCFPoolID                                                    --����CCF��ID
                ,ABSUAFlag                                                    --�ʲ�֤ȯ�������ʲ���ʶ
                ,ABSPoolID                                                    --֤ȯ���ʲ���ID
                ,GroupID                                                      --������
                ,DefaultDate                                                  --ΥԼʱ��
                ,ABSPROPORTION                                                --�ʲ�֤ȯ������
                ,DEBTORNUMBER                                                 --����˸���
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
                           AND NVL(INITIAL_COST, 0) + NVL(MKT_VALUE_CHANGE, 0) + NVL(ACCOUNTABLE_INT, 0) <> 0 --NVL(INT_ADJUST, 0) + ����Ϣ�������⣬��Ϊ���ֹ�����
    )
    SELECT
                TO_DATE(p_data_dt_str,'YYYYMMDD')                                       AS DataDate               --��������
                ,p_data_dt_str                                                          AS DataNo                 --������ˮ��
                ,NVL(T3.SERIALNO,T1.BOND_ID)                                            AS ExposureID             --���ձ�¶ID
                ,T1.BOND_ID                                                             AS DueID                  --ծ��ID
                ,'TZ'                                                                   AS SSysID                 --ԴϵͳID                       Ĭ�� Ͷ��(TZ)
                ,T1.BOND_ID                                                             AS ContractID             --��ͬID
                ,CASE WHEN REPLACE(T5.CUSTOMERID,'NCM_','') IS NULL THEN 'XN-YBGS'
                      ELSE T5.CUSTOMERID
                 END                                                                    AS ClientID               --��������ID                      ���û��ʵ�������˾�Ĭ��һ��һ�㹫˾
                ,T1.DEPARTMENT                                                          AS SOrgID                 --Դ����ID
                ,T4.ORGNAME                                                             AS SOrgName               --Դ��������
                ,T4.SORTNO                                                              AS OrgSortNo              --�������������
                ,T1.DEPARTMENT                                                          AS OrgID                  --��������ID
                ,T4.ORGNAME                                                             AS OrgName                --������������
                ,T1.DEPARTMENT                                                          AS AccOrgID               --�������ID
                ,T4.ORGNAME                                                             AS AccOrgName             --�����������
                ,CASE WHEN NVL(T5.INDUSTRYTYPE,DECODE(SUBSTR(T5.RWACUSTOMERTYPE,1,2),'02','J6620','J66')) = '999999' THEN 'J66'
                      ELSE NVL(T5.INDUSTRYTYPE,DECODE(SUBSTR(T5.RWACUSTOMERTYPE,1,2),'02','J6620','J66'))
                 END                                                                    AS IndustryID              --������ҵ����
                ,CASE WHEN NVL(T6.ITEMNAME,DECODE(SUBSTR(T5.RWACUSTOMERTYPE,1,2),'02','�������з���','���ҽ��ڷ���')) = 'δ֪' THEN '���ҽ��ڷ���'
                      ELSE NVL(T6.ITEMNAME,DECODE(SUBSTR(T5.RWACUSTOMERTYPE,1,2),'02','�������з���','���ҽ��ڷ���'))
                 END                                                                    AS IndustryName           --������ҵ����
                ,'0401'                                                                 AS BusinessLine           --ҵ������                        Ĭ�� ͬҵ(04)
                ,''                                                                     AS AssetType              --�ʲ�����                        Ĭ�� NULL RWA�������
                ,''                                                                     AS AssetSubType           --�ʲ�С��                        Ĭ�� NULL RWA�������
                ,CASE WHEN T1.BOND_TYPE1 = '081' THEN '1040105061'
                      WHEN T1.BOND_TYPE1 = '100' THEN '1040105062'
                      ELSE '1040105060'
                 END                                                                    AS BusinessTypeID         --ҵ��Ʒ�ִ���
                ,CASE WHEN T1.BOND_TYPE1 = '081' THEN 'Ӧ�տ�����Ͷ��_����'
                      WHEN T1.BOND_TYPE1 = '100' THEN 'Ӧ�տ�����Ͷ��_ͬҵ�浥'
                      ELSE 'Ӧ�տ�����Ͷ��'
                 END                                                                    AS BusinessTypeName       --ҵ��Ʒ������
                ,'01'                                                                   AS CreditRiskDataType     --���÷�����������                Ĭ�� һ�������(01)
                ,'01'                                                                   AS AssetTypeOfHaircuts    --�ۿ�ϵ����Ӧ�ʲ����            Ĭ�� �ֽ��ֽ�ȼ���(01)
                ,'07'                                                                   AS BusinessTypeSTD        --Ȩ�ط�ҵ������                 Ĭ�� һ���ʲ�(07)
                ,CASE WHEN REPLACE(T5.CUSTOMERID,'NCM_','') IS NULL THEN '0112'                                   --��û��ʵ����������Ĭ��Ϊ0112-����
                 ELSE ''
                 END                                                                    AS ExpoClassSTD           --Ȩ�ط���¶����                 Ĭ�� NULL RWA�������
                ,CASE WHEN REPLACE(T5.CUSTOMERID,'NCM_','') IS NULL THEN '011216'                                 --��û��ʵ����������Ĭ��Ϊ011216-��������100%����Ȩ�ص��ʲ�
                 ELSE ''
                 END                                                                    AS ExpoSubClassSTD        --Ȩ�ط���¶С��                 Ĭ�� NULL RWA�������
                ,SUBSTR(T13.DITEMNO,1,4)                                                AS ExpoClassIRB           --��������¶����                 Ĭ�� NULL RWA������
                ,T13.DITEMNO                                                            AS ExpoSubClassIRB        --��������¶С��                 Ĭ�� NULL RWA������
                ,'01'                                                                   AS ExpoBelong             --��¶������ʶ                    Ĭ�ϣ�����(01)
                ,CASE WHEN T1.ASSET_CLASS = '10' THEN '02'
                      ELSE '01'
                 END                                                                    AS BookType               --�˻����                        �ʲ����� �� �������Խ����ʲ�(10)�� , ��Ϊ02-�����˻����ʲ����� �� �������Խ����ʲ���  , ��Ϊ01-�����˻�
                ,'02'                                                                   AS ReguTranType           --��ܽ�������                    Ĭ�� �����ʱ��г�����(02)
                ,'0'                                                                    AS RepoTranFlag           --�ع����ױ�ʶ                    Ĭ�� ��(0)
                ,1                                                                      AS RevaFrequency          --�ع�Ƶ��                        Ĭ��  1
                ,NVL(T1.CURRENCY_CODE,'CNY')                                            AS Currency               --����
                ,NVL(T2.INITIAL_COST,0) + NVL(T2.MKT_VALUE_CHANGE,0) + NVL(T2.ACCOUNTABLE_INT,0)
                                                                                        AS NormalPrincipal        --�����������                    �����������ɱ�����Ϣ����(initial_cost)�����ʼ�ֵ�䶯/���ʼ�ֵ�䶯����(int_adjust)��Ӧ����Ϣ(mkt_value_change)
                ,0                                                                      AS OverdueBalance         --�������                        Ĭ�� 0
                ,0                                                                      AS NonAccrualBalance      --��Ӧ�����                     Ĭ�� 0
                ,NVL(T2.INITIAL_COST,0) + NVL(T2.MKT_VALUE_CHANGE,0) + NVL(T2.ACCOUNTABLE_INT,0)
                                                                                        AS OnSheetBalance         --�������                        �������=�����������+�������+��Ӧ�����
                ,0                                                                      AS NormalInterest         --������Ϣ                        Ĭ�� 0
                ,0                                                                      AS OnDebitInterest        --����ǷϢ                        Ĭ�� 0
                ,0                                                                      AS OffDebitInterest       --����ǷϢ                        Ĭ�� 0
                ,0                                                                      AS ExpenseReceivable      --Ӧ�շ���                        Ĭ�� 0
                ,NVL(T2.INITIAL_COST,0) + NVL(T2.MKT_VALUE_CHANGE,0) + NVL(T2.ACCOUNTABLE_INT,0)
                                                                                        AS AssetBalance           --�ʲ����                        �����ʲ������=�������+Ӧ�շ���+������Ϣ+����ǷϢ
                ,CASE WHEN T1.ASSET_CLASS = '50' THEN '12220400'
                      WHEN T1.ASSET_CLASS = '60' THEN '12220701'
                 END                                                                    AS AccSubject1            --��Ŀһ                         ����ԭϵͳ���ʲ�������ջ�ƿ�Ŀ��ȷ��
                ,''                                                                     AS AccSubject2            --��Ŀ��                         Ĭ�� NULL
                ,''                                                                     AS AccSubject3            --��Ŀ��                         Ĭ�� NULL
                ,T1.EFFECT_DATE                                                         AS StartDate              --��ʼ����
                ,T1.MATURITY_DATE                                                       AS DueDate                --��������
                ,CASE WHEN (TO_DATE(T1.MATURITY_DATE,'YYYYMMDD') - TO_DATE(T1.EFFECT_DATE,'YYYYMMDD')) / 365<0
                      THEN 0
                      ELSE (TO_DATE(T1.MATURITY_DATE,'YYYYMMDD') - TO_DATE(T1.EFFECT_DATE,'YYYYMMDD')) / 365
                 END                                                                    AS OriginalMaturity       --ԭʼ����                        ��λ ��
                ,CASE WHEN (TO_DATE(T1.MATURITY_DATE,'YYYYMMDD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365<0
                      THEN 0
                      ELSE (TO_DATE(T1.MATURITY_DATE,'YYYYMMDD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365
                 END                                                                    AS ResidualM              --ʣ������                        ��λ ��
                ,CASE WHEN T18.FINALLYRESULT IN ('A1','A2','A3','A4','A') THEN '01'                               --ʮ��������תΪ�弶����
                      WHEN T18.FINALLYRESULT IN ('B1','B2','B3','B') THEN '02'
                      WHEN T18.FINALLYRESULT IN ('C1','C2','C') THEN '03'
                      WHEN T18.FINALLYRESULT IN ('D1','D2','D') THEN '04'
                      WHEN T18.FINALLYRESULT = 'E' THEN '05'
                      ELSE '01'
                 END                                                                    AS RiskClassify           --���շ���                        Ĭ�� ����(01)
                ,''                                                                     AS ExposureStatus         --���ձ�¶״̬                    Ĭ�� NULL
                ,0                                                                      AS OverdueDays            --��������                        Ĭ�� 0
                ,0                                                                      AS SpecialProvision       --ר��׼����                     Ĭ�� 0 RWA���� ��Ŀ12220400��ֱ����1%��׼����
                ,0                                                                      AS GeneralProvision       --һ��׼����                     Ĭ�� 0 RWA����
                ,0                                                                      AS EspecialProvision      --�ر�׼����                     Ĭ�� 0 RWA����
                ,0                                                                      AS WrittenOffAmount       --�Ѻ������                     Ĭ�� 0
                ,''                                                                     AS OffExpoSource          --���Ⱪ¶��Դ                    Ĭ�� NULL
                ,''                                                                     AS OffBusinessType        --����ҵ������                    Ĭ�� NULL
                ,''                                                                     AS OffBusinessSdvsSTD     --Ȩ�ط�����ҵ������ϸ��         Ĭ�� NULL
                ,''                                                                     AS UncondCancelFlag       --�Ƿ����ʱ����������            Ĭ�� NULL
                ,''                                                                     AS CCFLevel               --����ת��ϵ������                Ĭ�� NULL
                ,NULL                                                                   AS CCFAIRB                --�߼�������ת��ϵ��             Ĭ�� NULL
                ,CASE WHEN T1.BOND_TYPE2 = '20' THEN '02'
                      ELSE '01'
                 END                                                                    AS ClaimsLevel            --ծȨ����                        ծȯ����2���μ�ծȯ(20)����ծȨ���𣽴μ�ծȨ(02)������Ϊ�߼�ծȨ(01)
                ,'0'                                                                    AS BondFlag               --�Ƿ�Ϊծȯ                     Ĭ�� ��(0)
                ,'02'                                                                   AS BondIssueIntent        --ծȯ����Ŀ��                    Ĭ�� ����(02)
                ,'0'                                                                    AS NSURealPropertyFlag    --�Ƿ�����ò�����                Ĭ�� ��(0)
                ,''                                                                     AS RepAssetTermType       --��ծ�ʲ���������                Ĭ�� NULL
                ,'0'                                                                    AS DependOnFPOBFlag       --�Ƿ�����������δ��ӯ��         Ĭ�� ��(0)
                ,T5.PDLEVEL                                                             AS IRating                --�ڲ�����
                ,T5.PD                                                                  AS PD                     --ΥԼ����
                ,''                                                                     AS LGDLevel               --ΥԼ��ʧ�ʼ���                 Ĭ�� NULL
                ,NULL                                                                   AS LGDAIRB                --�߼���ΥԼ��ʧ��                Ĭ�� NULL
                ,NULL                                                                   AS MAIRB                  --�߼�����Ч����                 Ĭ�� NULL
                ,NULL                                                                   AS EADAIRB                --�߼���ΥԼ���ձ�¶             Ĭ�� NULL
                ,CASE WHEN T5.PDLEVEL = '0116' THEN '1'
                 ELSE '0'
                 END                                                                    AS DefaultFlag            --ΥԼ��ʶ
                ,CASE WHEN T1.BOND_TYPE2 = '20' THEN 0.75
                 ELSE 0.45
                 END                                                                    AS BEEL                   --��ΥԼ��¶Ԥ����ʧ����         ծȨ���𣽡��߼�ծȨ����BEEL �� 45%��ծȨ���𣽡��μ�ծȨ����BEEL �� 75%
                ,CASE WHEN T1.BOND_TYPE2 = '20' THEN 0.75
                 ELSE 0.45
                 END                                                                    AS DefaultLGD             --��ΥԼ��¶ΥԼ��ʧ��            Ĭ�� NULL
                ,'0'                                                                    AS EquityExpoFlag         --��Ȩ��¶��ʶ                    Ĭ�� ��(0)
                ,''                                                                     AS EquityInvestType       --��ȨͶ�ʶ�������                Ĭ�� NULL
                ,''                                                                     AS EquityInvestCause      --��ȨͶ���γ�ԭ��                Ĭ�� NULL
                ,'0'                                                                    AS SLFlag                 --רҵ�����ʶ                    Ĭ�� ��(0)
                ,''                                                                     AS SLType                 --רҵ��������                    Ĭ�� NULL
                ,''                                                                     AS PFPhase                --��Ŀ���ʽ׶�                    Ĭ�� NULL
                ,'01'                                                                   AS ReguRating             --�������                        Ĭ�� ��(01)
                ,''                                                                     AS CBRCMPRatingFlag       --������϶������Ƿ��Ϊ����       Ĭ�� NULL
                ,''                                                                     AS LargeFlucFlag          --�Ƿ񲨶��Խϴ�                 Ĭ�� NULL
                ,'0'                                                                    AS LiquExpoFlag           --�Ƿ���������з��ձ�¶         Ĭ�� ��(0)
                ,'1'                                                                    AS PaymentDealFlag        --�Ƿ����Ը�ģʽ                Ĭ�� ��(1)
                ,NULL                                                                   AS DelayTradingDays       --�ӳٽ�������                    Ĭ�� NULL
                ,'0'                                                                    AS SecuritiesFlag         --�м�֤ȯ��ʶ                    Ĭ�� ��(0)
                ,''                                                                     AS SecuIssuerID           --֤ȯ������ID                   Ĭ�� NULL
                ,''                                                                     AS RatingDurationType     --������������                    Ĭ�� NULL
                ,''                                                                     AS SecuIssueRating        --֤ȯ���еȼ�                    Ĭ�� NULL
                ,NULL                                                                   AS SecuResidualM          --֤ȯʣ������                    Ĭ�� NULL
                ,1                                                                      AS SecuRevaFrequency      --֤ȯ�ع�Ƶ��                    Ĭ�� 1
                ,'0'                                                                    AS CCPTranFlag            --�Ƿ����뽻�׶�����ؽ���        Ĭ�� ��(0)
                ,''                                                                     AS CCPID                  --���뽻�׶���ID                  Ĭ�� NULL
                ,''                                                                     AS QualCCPFlag            --�Ƿ�ϸ����뽻�׶���            Ĭ�� NULL
                ,''                                                                     AS BankRole               --���н�ɫ                        Ĭ�� NULL
                ,''                                                                     AS ClearingMethod         --���㷽ʽ                        Ĭ�� NULL
                ,''                                                                     AS BankAssetFlag          --�Ƿ������ύ�ʲ�                Ĭ�� NULL
                ,''                                                                     AS MatchConditions        --�����������                    Ĭ�� NULL
                ,'0'                                                                    AS SFTFlag                --֤ȯ���ʽ��ױ�ʶ                Ĭ�� ��(0)
                ,'0'                                                                    AS MasterNetAgreeFlag     --���������Э���ʶ             Ĭ�� ��(0)
                ,''                                                                     AS MasterNetAgreeID       --���������Э��ID               Ĭ�� NULL
                ,''                                                                     AS SFTType                --֤ȯ���ʽ�������                Ĭ�� NULL
                ,''                                                                     AS SecuOwnerTransFlag     --֤ȯ����Ȩ�Ƿ�ת��             Ĭ�� NULL
                ,'0'                                                                    AS OTCFlag                --�����������߱�ʶ                Ĭ�� ��(0)
                ,''                                                                     AS ValidNettingFlag       --��Ч�������Э���ʶ            Ĭ�� NULL
                ,''                                                                     AS ValidNetAgreementID    --��Ч�������Э��ID              Ĭ�� NULL
                ,''                                                                     AS OTCType                --����������������                Ĭ�� NULL
                ,''                                                                     AS DepositRiskPeriod      --��֤������ڼ�                 Ĭ�� NULL
                ,''                                                                     AS MTM                    --���óɱ�                        Ĭ�� NULL
                ,''                                                                     AS MTMCurrency            --���óɱ�����                    Ĭ�� NULL
                ,''                                                                     AS BuyerOrSeller          --������                        Ĭ�� NULL
                ,''                                                                     AS QualROFlag             --�ϸ�����ʲ���ʶ                Ĭ�� NULL
                ,''                                                                     AS ROIssuerPerformFlag    --�����ʲ��������Ƿ�����Լ        Ĭ�� NULL
                ,''                                                                     AS BuyerInsolvencyFlag    --���ñ������Ƿ��Ʋ�            Ĭ�� NULL
                ,''                                                                     AS NonpaymentFees         --��δ֧������                    Ĭ�� NULL
                ,'0'                                                                    AS RetailExpoFlag         --���۱�¶��ʶ                    Ĭ�� ��(0)
                ,''                                                                     AS RetailClaimType        --����ծȨ����                    Ĭ�� NULL
                ,''                                                                     AS MortgageType           --ס����Ѻ��������                Ĭ�� NULL
                ,1                                                                      AS ExpoNumber             --���ձ�¶����                    Ĭ�� 1
                ,0.8                                                                    AS LTV                    --�����ֵ��                     Ĭ�� 0.8
                ,NULL                                                                   AS Aging                  --����                            Ĭ�� NULL
                ,''                                                                     AS NewDefaultDebtFlag     --����ΥԼծ���ʶ                Ĭ�� NULL
                ,''                                                                     AS PDPoolModelID          --PD�ֳ�ģ��ID                    Ĭ�� NULL
                ,''                                                                     AS LGDPoolModelID         --LGD�ֳ�ģ��ID                   Ĭ�� NULL
                ,''                                                                     AS CCFPoolModelID         --CCF�ֳ�ģ��ID                   Ĭ�� NULL
                ,''                                                                     AS PDPoolID               --����PD��ID                     Ĭ�� NULL
                ,''                                                                     AS LGDPoolID              --����LGD��ID                    Ĭ�� NULL
                ,''                                                                     AS CCFPoolID              --����CCF��ID                    Ĭ�� NULL
                ,'0'                                                                    AS ABSUAFlag              --�ʲ�֤ȯ�������ʲ���ʶ         Ĭ�� ��(0)
                ,''                                                                     AS ABSPoolID              --֤ȯ���ʲ���ID                  Ĭ�� NULL
                ,''                                                                     AS GroupID                --������                        Ĭ�� NULL
                ,CASE WHEN T5.PDLEVEL = '0116' THEN TO_DATE(T5.PDVAVLIDDATE,'YYYYMMDD')
                 ELSE NULL
                 END                                                                    AS DefaultDate            --ΥԼʱ��
                ,NULL                                                                   AS ABSPROPORTION          --�ʲ�֤ȯ������
                ,NULL                                                                   AS DEBTORNUMBER           --����˸���

    FROM        RWA_DEV.FNS_BND_INFO_B T1
    INNER JOIN  TEMP_BND_BOOK T2
    ON          T1.BOND_ID = T2.BOND_ID
    INNER JOIN  RWA_DEV.NCM_BUSINESS_DUEBILL T3                           --�Ŵ���ݱ�
    ON          'CW_IMPORTDATA' || T1.BOND_ID = T3.THIRDPARTYACCOUNTS
    AND         T3.DATANO = p_data_dt_str
    LEFT JOIN   RWA.ORG_INFO T4                                           --RWA������
    ON          T1.DEPARTMENT = T4.ORGID
    LEFT JOIN   RWA_DEV.RWA_TMP_TZCUST T5                                 --ʵ�������˿ͻ���Ϣ��ʱ��
    ON          T3.RELATIVESERIALNO2 = T5.CONTRACTNO
    LEFT JOIN   RWA.CODE_LIBRARY  T6                                      --RWA�����ȡ��ҵ
    ON          T5.INDUSTRYTYPE = T6.ITEMNO
    AND         T6.CODENO = 'IndustryType'
    LEFT JOIN   RWA.CODE_LIBRARY  T7                                      --RWA�����ȡҵ��Ʒ��
    ON          T3.BUSINESSTYPE = T7.ITEMNO
    AND         T7.CODENO = 'BusinessType'
    INNER JOIN  RWA_DEV.NCM_BUSINESS_CONTRACT T10
    ON          T3.RELATIVESERIALNO2 = T10.SERIALNO
    AND         (T10.BUSINESSSUBTYPE NOT LIKE '0010%' OR T10.BUSINESSSUBTYPE IS NULL)             --�ǻ�������
    AND         T10.DATANO = p_data_dt_str
    LEFT JOIN   RWA_DEV.NCM_RWA_RISK_EXPO_RST T12                 --��������¶��������
    ON          T5.CUSTOMERID = T12.OBJECTNO
    AND         T12.OBJECTTYPE = 'BusinessDuebillCust'                    --�����ж�Ӧʵ��������
    AND         T12.DATANO = p_data_dt_str
    LEFT JOIN   RWA_DEV.RWA_CD_CODE_MAPPING T13                           --����ӳ���
    ON          T12.RISKEXPOSURERESULT = T13.SITEMNO
    AND         T13.SYSID = 'XD'
    AND         T13.SCODENO = 'RwaResultType'
    AND         T13.ISINUSE = '1'
    /*
    LEFT JOIN   (SELECT TR.BOND_ID
                       ,TO_NUMBER(REPLACE(TR.RESERVESUM,',','')) AS RESERVESUM
                   FROM RWA.RWA_WS_RESERVE TR
             INNER JOIN RWA.RWA_WP_DATASUPPLEMENT TD                      --���ݲ�¼��
                     ON TR.SUPPORGID = TD.ORGID
                    AND TD.DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
                    AND TD.SUPPTMPLID = 'M-0210'
                    AND TD.SUBMITFLAG = '1'
                  WHERE TR.DATADATE = to_date(p_data_dt_str,'YYYYMMDD')
                ) T16                                                     --Ӧ�տ�Ͷ��׼����¼��
    ON          T1.BOND_ID = T16.BOND_ID
    */
    LEFT JOIN   RWA_DEV.NCM_CLASSIFY_RECORD T18                           --ʮ����������Ϣ��
    ON          T3.RELATIVESERIALNO2 = T18.OBJECTNO
    AND         T18.OBJECTTYPE = 'TwelveClassify'
    AND         T18.ISWORK = '1'
    AND         T18.DATANO = p_data_dt_str
    WHERE       T1.ASSET_CLASS IN ('50','60')                       --ͨ���ʲ�������ȷ��ծȯ����Ӧ�տ�Ͷ�ʡ�
    AND         T1.DATANO = p_data_dt_str                           --ծȯ��Ϣ��,��ȡ��Ч��ծȯ��Ϣ
    AND         T1.BOND_CODE IS NOT NULL                            --�ų���Ч��ծȯ����
    ;

    COMMIT;
    
    --2.4.4 ����ϵͳ-Ӧ�տ�Ͷ��-���һ���-11010301
    INSERT INTO RWA_DEV.RWA_TZ_EXPOSURE(
                 DataDate                                                     --��������
                ,DataNo                                                       --������ˮ��
                ,ExposureID                                                   --���ձ�¶ID
                ,DueID                                                        --ծ��ID
                ,SSysID                                                       --ԴϵͳID
                ,ContractID                                                   --��ͬID
                ,ClientID                                                     --��������ID
                ,SOrgID                                                       --Դ����ID
                ,SOrgName                                                     --Դ��������
                ,OrgSortNo                                                    --�������������
                ,OrgID                                                        --��������ID
                ,OrgName                                                      --������������
                ,AccOrgID                                                     --�������ID
                ,AccOrgName                                                   --�����������
                ,IndustryID                                                   --������ҵ����
                ,IndustryName                                                 --������ҵ����
                ,BusinessLine                                                 --ҵ������
                ,AssetType                                                    --�ʲ�����
                ,AssetSubType                                                 --�ʲ�С��
                ,BusinessTypeID                                               --ҵ��Ʒ�ִ���
                ,BusinessTypeName                                             --ҵ��Ʒ������
                ,CreditRiskDataType                                           --���÷�����������
                ,AssetTypeOfHaircuts                                          --�ۿ�ϵ����Ӧ�ʲ����
                ,BusinessTypeSTD                                              --Ȩ�ط�ҵ������
                ,ExpoClassSTD                                                 --Ȩ�ط���¶����
                ,ExpoSubClassSTD                                              --Ȩ�ط���¶С��
                ,ExpoClassIRB                                                 --��������¶����
                ,ExpoSubClassIRB                                              --��������¶С��
                ,ExpoBelong                                                   --��¶������ʶ
                ,BookType                                                     --�˻����
                ,ReguTranType                                                 --��ܽ�������
                ,RepoTranFlag                                                 --�ع����ױ�ʶ
                ,RevaFrequency                                                --�ع�Ƶ��
                ,Currency                                                     --����
                ,NormalPrincipal                                              --�����������
                ,OverdueBalance                                               --�������
                ,NonAccrualBalance                                            --��Ӧ�����
                ,OnSheetBalance                                               --�������
                ,NormalInterest                                               --������Ϣ
                ,OnDebitInterest                                              --����ǷϢ
                ,OffDebitInterest                                             --����ǷϢ
                ,ExpenseReceivable                                            --Ӧ�շ���
                ,AssetBalance                                                 --�ʲ����
                ,AccSubject1                                                  --��Ŀһ
                ,AccSubject2                                                  --��Ŀ��
                ,AccSubject3                                                  --��Ŀ��
                ,StartDate                                                    --��ʼ����
                ,DueDate                                                      --��������
                ,OriginalMaturity                                             --ԭʼ����
                ,ResidualM                                                    --ʣ������
                ,RiskClassify                                                 --���շ���
                ,ExposureStatus                                               --���ձ�¶״̬
                ,OverdueDays                                                  --��������
                ,SpecialProvision                                             --ר��׼����
                ,GeneralProvision                                             --һ��׼����
                ,EspecialProvision                                            --�ر�׼����
                ,WrittenOffAmount                                             --�Ѻ������
                ,OffExpoSource                                                --���Ⱪ¶��Դ
                ,OffBusinessType                                              --����ҵ������
                ,OffBusinessSdvsSTD                                           --Ȩ�ط�����ҵ������ϸ��
                ,UncondCancelFlag                                             --�Ƿ����ʱ����������
                ,CCFLevel                                                     --����ת��ϵ������
                ,CCFAIRB                                                      --�߼�������ת��ϵ��
                ,ClaimsLevel                                                  --ծȨ����
                ,BondFlag                                                     --�Ƿ�Ϊծȯ
                ,BondIssueIntent                                              --ծȯ����Ŀ��
                ,NSURealPropertyFlag                                          --�Ƿ�����ò�����
                ,RepAssetTermType                                             --��ծ�ʲ���������
                ,DependOnFPOBFlag                                             --�Ƿ�����������δ��ӯ��
                ,IRating                                                      --�ڲ�����
                ,PD                                                           --ΥԼ����
                ,LGDLevel                                                     --ΥԼ��ʧ�ʼ���
                ,LGDAIRB                                                      --�߼���ΥԼ��ʧ��
                ,MAIRB                                                        --�߼�����Ч����
                ,EADAIRB                                                      --�߼���ΥԼ���ձ�¶
                ,DefaultFlag                                                  --ΥԼ��ʶ
                ,BEEL                                                         --��ΥԼ��¶Ԥ����ʧ����
                ,DefaultLGD                                                   --��ΥԼ��¶ΥԼ��ʧ��
                ,EquityExpoFlag                                               --��Ȩ��¶��ʶ
                ,EquityInvestType                                             --��ȨͶ�ʶ�������
                ,EquityInvestCause                                            --��ȨͶ���γ�ԭ��
                ,SLFlag                                                       --רҵ�����ʶ
                ,SLType                                                       --רҵ��������
                ,PFPhase                                                      --��Ŀ���ʽ׶�
                ,ReguRating                                                   --�������
                ,CBRCMPRatingFlag                                             --������϶������Ƿ��Ϊ����
                ,LargeFlucFlag                                                --�Ƿ񲨶��Խϴ�
                ,LiquExpoFlag                                                 --�Ƿ���������з��ձ�¶
                ,PaymentDealFlag                                              --�Ƿ����Ը�ģʽ
                ,DelayTradingDays                                             --�ӳٽ�������
                ,SecuritiesFlag                                               --�м�֤ȯ��ʶ
                ,SecuIssuerID                                                 --֤ȯ������ID
                ,RatingDurationType                                           --������������
                ,SecuIssueRating                                              --֤ȯ���еȼ�
                ,SecuResidualM                                                --֤ȯʣ������
                ,SecuRevaFrequency                                            --֤ȯ�ع�Ƶ��
                ,CCPTranFlag                                                  --�Ƿ����뽻�׶�����ؽ���
                ,CCPID                                                        --���뽻�׶���ID
                ,QualCCPFlag                                                  --�Ƿ�ϸ����뽻�׶���
                ,BankRole                                                     --���н�ɫ
                ,ClearingMethod                                               --���㷽ʽ
                ,BankAssetFlag                                                --�Ƿ������ύ�ʲ�
                ,MatchConditions                                              --�����������
                ,SFTFlag                                                      --֤ȯ���ʽ��ױ�ʶ
                ,MasterNetAgreeFlag                                           --���������Э���ʶ
                ,MasterNetAgreeID                                             --���������Э��ID
                ,SFTType                                                      --֤ȯ���ʽ�������
                ,SecuOwnerTransFlag                                           --֤ȯ����Ȩ�Ƿ�ת��
                ,OTCFlag                                                      --�����������߱�ʶ
                ,ValidNettingFlag                                             --��Ч�������Э���ʶ
                ,ValidNetAgreementID                                          --��Ч�������Э��ID
                ,OTCType                                                      --����������������
                ,DepositRiskPeriod                                            --��֤������ڼ�
                ,MTM                                                          --���óɱ�
                ,MTMCurrency                                                  --���óɱ�����
                ,BuyerOrSeller                                                --������
                ,QualROFlag                                                   --�ϸ�����ʲ���ʶ
                ,ROIssuerPerformFlag                                          --�����ʲ��������Ƿ�����Լ
                ,BuyerInsolvencyFlag                                          --���ñ������Ƿ��Ʋ�
                ,NonpaymentFees                                               --��δ֧������
                ,RetailExpoFlag                                               --���۱�¶��ʶ
                ,RetailClaimType                                              --����ծȨ����
                ,MortgageType                                                 --ס����Ѻ��������
                ,ExpoNumber                                                   --���ձ�¶����
                ,LTV                                                          --�����ֵ��
                ,Aging                                                        --����
                ,NewDefaultDebtFlag                                           --����ΥԼծ���ʶ
                ,PDPoolModelID                                                --PD�ֳ�ģ��ID
                ,LGDPoolModelID                                               --LGD�ֳ�ģ��ID
                ,CCFPoolModelID                                               --CCF�ֳ�ģ��ID
                ,PDPoolID                                                     --����PD��ID
                ,LGDPoolID                                                    --����LGD��ID
                ,CCFPoolID                                                    --����CCF��ID
                ,ABSUAFlag                                                    --�ʲ�֤ȯ�������ʲ���ʶ
                ,ABSPoolID                                                    --֤ȯ���ʲ���ID
                ,GroupID                                                      --������
                ,DefaultDate                                                  --ΥԼʱ��
                ,ABSPROPORTION                                                --�ʲ�֤ȯ������
                ,DEBTORNUMBER                                                 --����˸���
                ,SBJT2
                ,SBJT_VAL2
                ,SBJT3
                ,SBJT_VAL3
                ,SBJT4
                ,SBJT_VAL4
                ,SBJT5
                ,SBJT_VAL5
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
                           AND NVL(INITIAL_COST, 0) + NVL(MKT_VALUE_CHANGE, 0) + NVL(ACCOUNTABLE_INT, 0) <> 0 --NVL(INT_ADJUST, 0) + ����Ϣ�������⣬��Ϊ���ֹ�����
    )
    SELECT
                TO_DATE(p_data_dt_str,'YYYYMMDD')                                       AS DataDate               --��������
                ,p_data_dt_str                                                          AS DataNo                 --������ˮ��
                ,T1.BOND_ID                                                             AS ExposureID             --���ձ�¶ID
                ,T1.BOND_ID                                                             AS DueID                  --ծ��ID
                ,'TZ'                                                                   AS SSysID                 --ԴϵͳID                       Ĭ�� Ͷ��(TZ)
                ,T1.BOND_ID                                                             AS ContractID             --��ͬID
                ,T3.PARTICIPANT_CODE                                                             AS ClientID               --��������ID                      Ĭ��һ��һ�㹫˾
                ,T1.DEPARTMENT                                                          AS SOrgID                 --Դ����ID
                ,T4.ORGNAME                                                             AS SOrgName               --Դ��������
                --,T4.SORTNO                                                              AS OrgSortNo              --�������������
                ,NVL(T4.SORTNO,'1010')
                --,T1.DEPARTMENT                                                          AS OrgID                  --��������ID
                ,decode(substr(T1.DEPARTMENT,1,1),'@','01000000',T1.DEPARTMENT)
                --,T4.ORGNAME                                                             AS OrgName                --������������
                ,NVL(T4.ORGNAME,'����')
                ,T1.DEPARTMENT                                                          AS AccOrgID               --�������ID
                ,T4.ORGNAME                                                             AS AccOrgName             --�����������
                ,'J66'                                                                  AS IndustryID             --������ҵ����
                ,'���ҽ��ڷ���'                                                         AS IndustryName           --������ҵ����
                ,'0401'                                                                 AS BusinessLine           --ҵ������                        Ĭ�� ͬҵ(04)
                ,''                                                                     AS AssetType              --�ʲ�����                        Ĭ�� NULL RWA�������
                ,''                                                                     AS AssetSubType           --�ʲ�С��                        Ĭ�� NULL RWA�������
                ,CASE WHEN T1.BOND_TYPE1 = '081' THEN '1040105061'
                      WHEN T1.BOND_TYPE1 = '100' THEN '1040105062'
                      ELSE '1040105060'
                 END                                                                    AS BusinessTypeID         --ҵ��Ʒ�ִ���
                ,CASE WHEN T1.BOND_TYPE1 = '081' THEN 'Ӧ�տ�����Ͷ��_����'
                      WHEN T1.BOND_TYPE1 = '100' THEN 'Ӧ�տ�����Ͷ��_ͬҵ�浥'
                      ELSE 'Ӧ�տ�����Ͷ��_���һ���'
                 END                                                                    AS BusinessTypeName       --ҵ��Ʒ������
                ,'01'                                                                   AS CreditRiskDataType     --���÷�����������                Ĭ�� һ�������(01)
                ,'01'                                                                   AS AssetTypeOfHaircuts    --�ۿ�ϵ����Ӧ�ʲ����            Ĭ�� �ֽ��ֽ�ȼ���(01)
                ,'07'                                                                   AS BusinessTypeSTD        --Ȩ�ط�ҵ������                 Ĭ�� һ���ʲ�(07)
                ,'0104'                                                                 AS ExpoClassSTD           --Ȩ�ط���¶����                 Ĭ�� 011216-��������100%����Ȩ�ص��ʲ�
                ,'010408'                                                               AS ExpoSubClassSTD        --Ȩ�ط���¶С��                 Ĭ�� 011216-��������100%����Ȩ�ص��ʲ�
                ,''                                                                     AS ExpoClassIRB           --��������¶����                 Ĭ�� NULL RWA������
                ,''                                                                     AS ExpoSubClassIRB        --��������¶С��                 Ĭ�� NULL RWA������
                ,'01'                                                                   AS ExpoBelong             --��¶������ʶ                    Ĭ�ϣ�����(01)
                ,CASE WHEN T1.ASSET_CLASS = '10' THEN '02'
                      ELSE '01'
                 END                                                                    AS BookType               --�˻����                        �ʲ����� �� �������Խ����ʲ�(10)�� , ��Ϊ02-�����˻����ʲ����� �� �������Խ����ʲ���  , ��Ϊ01-�����˻�
                ,'02'                                                                   AS ReguTranType           --��ܽ�������                    Ĭ�� �����ʱ��г�����(02)
                ,'0'                                                                    AS RepoTranFlag           --�ع����ױ�ʶ                    Ĭ�� ��(0)
                ,1                                                                      AS RevaFrequency          --�ع�Ƶ��                        Ĭ��  1
                ,NVL(T1.CURRENCY_CODE,'CNY')                                            AS Currency               --����
                ,NVL(T2.INITIAL_COST,0)                                                 AS NormalPrincipal        --�����������                    �����������ɱ�����Ϣ����(initial_cost)�����ʼ�ֵ�䶯/���ʼ�ֵ�䶯����(int_adjust)��Ӧ����Ϣ(mkt_value_change)
                ,0                                                                      AS OverdueBalance         --�������                        Ĭ�� 0
                ,0                                                                      AS NonAccrualBalance      --��Ӧ�����                     Ĭ�� 0
                ,NVL(T2.INITIAL_COST,0)                                                 AS OnSheetBalance         --�������                        �������=�����������+�������+��Ӧ�����
                ,0                                                                      AS NormalInterest         --������Ϣ                        Ĭ�� 0
                ,0                                                                      AS OnDebitInterest        --����ǷϢ                        Ĭ�� 0
                ,0                                                                      AS OffDebitInterest       --����ǷϢ                        Ĭ�� 0
                ,0                                                                      AS ExpenseReceivable      --Ӧ�շ���                        Ĭ�� 0
                ,NVL(T2.INITIAL_COST,0)                                                 AS AssetBalance           --�ʲ����                        �����ʲ������=�������+Ӧ�շ���+������Ϣ+����ǷϢ
                ,CASE WHEN T1.ASSET_CLASS = '50' AND T1.BOND_TYPE2 = '50' THEN '11010301'
                 END                                                                    AS AccSubject1            --��Ŀһ                         ����ԭϵͳ���ʲ�������ջ�ƿ�Ŀ��ȷ��
                ,''                                                                     AS AccSubject2            --��Ŀ��                         Ĭ�� NULL
                ,''                                                                     AS AccSubject3            --��Ŀ��                         Ĭ�� NULL
                ,T1.origination_date                                                         AS StartDate              --��ʼ����
                ,T1.MATURITY_DATE                                                       AS DueDate                --��������
                ,CASE WHEN (TO_DATE(T1.MATURITY_DATE,'YYYYMMDD') - TO_DATE(T1.origination_date,'YYYYMMDD')) / 365<0
                      THEN 0
                      ELSE (TO_DATE(T1.MATURITY_DATE,'YYYYMMDD') - TO_DATE(T1.origination_date,'YYYYMMDD')) / 365
                 END                                                                    AS OriginalMaturity       --ԭʼ����                        ��λ ��
                ,CASE WHEN (TO_DATE(T1.MATURITY_DATE,'YYYYMMDD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365<0
                      THEN 0
                      ELSE (TO_DATE(T1.MATURITY_DATE,'YYYYMMDD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365
                 END                                                                    AS ResidualM              --ʣ������                        ��λ ��
                ,'01'                                                                   AS RiskClassify           --���շ���                        Ĭ�� ����(01)
                ,''                                                                     AS ExposureStatus         --���ձ�¶״̬                    Ĭ�� NULL
                ,0                                                                      AS OverdueDays            --��������                        Ĭ�� 0
                ,0                                                                      AS SpecialProvision       --ר��׼����                     Ĭ�� 0 RWA���� ��Ŀ12220400��ֱ����1%��׼����
                ,0                                                                      AS GeneralProvision       --һ��׼����                     Ĭ�� 0 RWA����
                ,0                                                                      AS EspecialProvision      --�ر�׼����                     Ĭ�� 0 RWA����
                ,0                                                                      AS WrittenOffAmount       --�Ѻ������                     Ĭ�� 0
                ,''                                                                     AS OffExpoSource          --���Ⱪ¶��Դ                    Ĭ�� NULL
                ,''                                                                     AS OffBusinessType        --����ҵ������                    Ĭ�� NULL
                ,''                                                                     AS OffBusinessSdvsSTD     --Ȩ�ط�����ҵ������ϸ��         Ĭ�� NULL
                ,''                                                                     AS UncondCancelFlag       --�Ƿ����ʱ����������            Ĭ�� NULL
                ,''                                                                     AS CCFLevel               --����ת��ϵ������                Ĭ�� NULL
                ,NULL                                                                   AS CCFAIRB                --�߼�������ת��ϵ��             Ĭ�� NULL
                ,CASE WHEN T1.BOND_TYPE2 = '20' THEN '02'
                      ELSE '01'
                 END                                                                    AS ClaimsLevel            --ծȨ����                        ծȯ����2���μ�ծȯ(20)����ծȨ���𣽴μ�ծȨ(02)������Ϊ�߼�ծȨ(01)
                ,'0'                                                                    AS BondFlag               --�Ƿ�Ϊծȯ                     Ĭ�� ��(0)
                ,'02'                                                                   AS BondIssueIntent        --ծȯ����Ŀ��                    Ĭ�� ����(02)
                ,'0'                                                                    AS NSURealPropertyFlag    --�Ƿ�����ò�����                Ĭ�� ��(0)
                ,''                                                                     AS RepAssetTermType       --��ծ�ʲ���������                Ĭ�� NULL
                ,'0'                                                                    AS DependOnFPOBFlag       --�Ƿ�����������δ��ӯ��         Ĭ�� ��(0)
                ,''                                                                     AS IRating                --�ڲ�����
                ,NULL                                                                   AS PD                     --ΥԼ����
                ,''                                                                     AS LGDLevel               --ΥԼ��ʧ�ʼ���                 Ĭ�� NULL
                ,NULL                                                                   AS LGDAIRB                --�߼���ΥԼ��ʧ��                Ĭ�� NULL
                ,NULL                                                                   AS MAIRB                  --�߼�����Ч����                 Ĭ�� NULL
                ,NULL                                                                   AS EADAIRB                --�߼���ΥԼ���ձ�¶             Ĭ�� NULL
                ,'0'                                                                    AS DefaultFlag            --ΥԼ��ʶ
                ,CASE WHEN T1.BOND_TYPE2 = '20' THEN 0.75
                 ELSE 0.45
                 END                                                                    AS BEEL                   --��ΥԼ��¶Ԥ����ʧ����         ծȨ���𣽡��߼�ծȨ����BEEL �� 45%��ծȨ���𣽡��μ�ծȨ����BEEL �� 75%
                ,CASE WHEN T1.BOND_TYPE2 = '20' THEN 0.75
                 ELSE 0.45
                 END                                                                    AS DefaultLGD             --��ΥԼ��¶ΥԼ��ʧ��            Ĭ�� NULL
                ,'0'                                                                    AS EquityExpoFlag         --��Ȩ��¶��ʶ                    Ĭ�� ��(0)
                ,''                                                                     AS EquityInvestType       --��ȨͶ�ʶ�������                Ĭ�� NULL
                ,''                                                                     AS EquityInvestCause      --��ȨͶ���γ�ԭ��                Ĭ�� NULL
                ,'0'                                                                    AS SLFlag                 --רҵ�����ʶ                    Ĭ�� ��(0)
                ,''                                                                     AS SLType                 --רҵ��������                    Ĭ�� NULL
                ,''                                                                     AS PFPhase                --��Ŀ���ʽ׶�                    Ĭ�� NULL
                ,'01'                                                                   AS ReguRating             --�������                        Ĭ�� ��(01)
                ,''                                                                     AS CBRCMPRatingFlag       --������϶������Ƿ��Ϊ����       Ĭ�� NULL
                ,''                                                                     AS LargeFlucFlag          --�Ƿ񲨶��Խϴ�                 Ĭ�� NULL
                ,'0'                                                                    AS LiquExpoFlag           --�Ƿ���������з��ձ�¶         Ĭ�� ��(0)
                ,'1'                                                                    AS PaymentDealFlag        --�Ƿ����Ը�ģʽ                Ĭ�� ��(1)
                ,NULL                                                                   AS DelayTradingDays       --�ӳٽ�������                    Ĭ�� NULL
                ,'0'                                                                    AS SecuritiesFlag         --�м�֤ȯ��ʶ                    Ĭ�� ��(0)
                ,''                                                                     AS SecuIssuerID           --֤ȯ������ID                   Ĭ�� NULL
                ,''                                                                     AS RatingDurationType     --������������                    Ĭ�� NULL
                ,''                                                                     AS SecuIssueRating        --֤ȯ���еȼ�                    Ĭ�� NULL
                ,NULL                                                                   AS SecuResidualM          --֤ȯʣ������                    Ĭ�� NULL
                ,1                                                                      AS SecuRevaFrequency      --֤ȯ�ع�Ƶ��                    Ĭ�� 1
                ,'0'                                                                    AS CCPTranFlag            --�Ƿ����뽻�׶�����ؽ���        Ĭ�� ��(0)
                ,''                                                                     AS CCPID                  --���뽻�׶���ID                  Ĭ�� NULL
                ,''                                                                     AS QualCCPFlag            --�Ƿ�ϸ����뽻�׶���            Ĭ�� NULL
                ,''                                                                     AS BankRole               --���н�ɫ                        Ĭ�� NULL
                ,''                                                                     AS ClearingMethod         --���㷽ʽ                        Ĭ�� NULL
                ,''                                                                     AS BankAssetFlag          --�Ƿ������ύ�ʲ�                Ĭ�� NULL
                ,''                                                                     AS MatchConditions        --�����������                    Ĭ�� NULL
                ,'0'                                                                    AS SFTFlag                --֤ȯ���ʽ��ױ�ʶ                Ĭ�� ��(0)
                ,'0'                                                                    AS MasterNetAgreeFlag     --���������Э���ʶ             Ĭ�� ��(0)
                ,''                                                                     AS MasterNetAgreeID       --���������Э��ID               Ĭ�� NULL
                ,''                                                                     AS SFTType                --֤ȯ���ʽ�������                Ĭ�� NULL
                ,''                                                                     AS SecuOwnerTransFlag     --֤ȯ����Ȩ�Ƿ�ת��             Ĭ�� NULL
                ,'0'                                                                    AS OTCFlag                --�����������߱�ʶ                Ĭ�� ��(0)
                ,''                                                                     AS ValidNettingFlag       --��Ч�������Э���ʶ            Ĭ�� NULL
                ,''                                                                     AS ValidNetAgreementID    --��Ч�������Э��ID              Ĭ�� NULL
                ,''                                                                     AS OTCType                --����������������                Ĭ�� NULL
                ,''                                                                     AS DepositRiskPeriod      --��֤������ڼ�                 Ĭ�� NULL
                ,''                                                                     AS MTM                    --���óɱ�                        Ĭ�� NULL
                ,''                                                                     AS MTMCurrency            --���óɱ�����                    Ĭ�� NULL
                ,''                                                                     AS BuyerOrSeller          --������                        Ĭ�� NULL
                ,''                                                                     AS QualROFlag             --�ϸ�����ʲ���ʶ                Ĭ�� NULL
                ,''                                                                     AS ROIssuerPerformFlag    --�����ʲ��������Ƿ�����Լ        Ĭ�� NULL
                ,''                                                                     AS BuyerInsolvencyFlag    --���ñ������Ƿ��Ʋ�            Ĭ�� NULL
                ,''                                                                     AS NonpaymentFees         --��δ֧������                    Ĭ�� NULL
                ,'0'                                                                    AS RetailExpoFlag         --���۱�¶��ʶ                    Ĭ�� ��(0)
                ,''                                                                     AS RetailClaimType        --����ծȨ����                    Ĭ�� NULL
                ,''                                                                     AS MortgageType           --ס����Ѻ��������                Ĭ�� NULL
                ,1                                                                      AS ExpoNumber             --���ձ�¶����                    Ĭ�� 1
                ,0.8                                                                    AS LTV                    --�����ֵ��                     Ĭ�� 0.8
                ,NULL                                                                   AS Aging                  --����                            Ĭ�� NULL
                ,''                                                                     AS NewDefaultDebtFlag     --����ΥԼծ���ʶ                Ĭ�� NULL
                ,''                                                                     AS PDPoolModelID          --PD�ֳ�ģ��ID                    Ĭ�� NULL
                ,''                                                                     AS LGDPoolModelID         --LGD�ֳ�ģ��ID                   Ĭ�� NULL
                ,''                                                                     AS CCFPoolModelID         --CCF�ֳ�ģ��ID                   Ĭ�� NULL
                ,''                                                                     AS PDPoolID               --����PD��ID                     Ĭ�� NULL
                ,''                                                                     AS LGDPoolID              --����LGD��ID                    Ĭ�� NULL
                ,''                                                                     AS CCFPoolID              --����CCF��ID                    Ĭ�� NULL
                ,'0'                                                                    AS ABSUAFlag              --�ʲ�֤ȯ�������ʲ���ʶ         Ĭ�� ��(0)
                ,''                                                                     AS ABSPoolID              --֤ȯ���ʲ���ID                  Ĭ�� NULL
                ,''                                                                     AS GroupID                --������                        Ĭ�� NULL
                ,NULL                                                                   AS DefaultDate            --ΥԼʱ��
                ,NULL                                                                   AS ABSPROPORTION          --�ʲ�֤ȯ������
                ,NULL                                                                   AS DEBTORNUMBER           --����˸���
                ,NULL AS sbjt2
                ,NULL AS sbjt_val2
                ,'11010302' AS sbjt3
                ,t2.ACCOUNTABLE_INT AS sbjt_val3
                ,'11010303' AS sbjt4
                ,t2.MKT_VALUE_CHANGE AS sbjt_val4
                ,NULL
                ,NULL
    FROM        RWA_DEV.FNS_BND_INFO_B T1
    INNER JOIN  TEMP_BND_BOOK T2
    ON          T1.BOND_ID = T2.BOND_ID
    LEFT JOIN (
    SELECT *
  FROM (SELECT T.DATANO,
               T.BOND_ID,
               T.PARTICIPANT_CODE,
               B.PARTICIPANT_NAME,
               ROW_NUMBER() OVER(PARTITION BY T.DATANO, T.BOND_ID, T.PARTICIPANT_CODE ORDER BY T.SORT_SEQ DESC) AS ROW_ID
          FROM FNS_BND_TRANSACTION_B T
          LEFT JOIN FNS_BND_PARTICIPANT_B B
            ON T.DATANO = B.DATANO
           AND T.PARTICIPANT_CODE = B.PARTICIPANT_CODE
         WHERE T.PARTICIPANT_CODE IS NOT NULL)
 WHERE ROW_ID = 1) T3
    ON T1.DATANO=T3.DATANO
    AND T1.BOND_ID = T3.BOND_ID
    LEFT JOIN   RWA.ORG_INFO T4                                           --RWA������
    ON          T1.DEPARTMENT = T4.ORGID
    WHERE       T1.Bond_Type1 = '3002' AND T1.BOND_TYPE2 = '50'  --ͨ���ʲ�������ȷ��ծȯ����Ӧ�տ�Ͷ�ʡ�
    AND         T1.DATANO = p_data_dt_str                           --ծȯ��Ϣ��,��ȡ��Ч��ծȯ��Ϣ
    AND         T1.BOND_CODE IS NOT NULL                            --�ų���Ч��ծȯ����
    --AND         T1.MATURITY_DATE >= p_data_dt_str                   --�ų����ڵ�ծȯ����
    --AND NOT EXISTS (SELECT 1 FROM RWA_DEV.NCM_BUSINESS_DUEBILL CBD INNER JOIN RWA_DEV.NCM_BUSINESS_CONTRACT CBC ON CBD.RELATIVESERIALNO2 = CBC.SERIALNO AND CBD.DATANO = CBC.DATANO WHERE 'CW_IMPORTDATA' || T1.BOND_ID = CBD.THIRDPARTYACCOUNTS AND CBD.DATANO = p_data_dt_str)
    ;

    COMMIT;

    --2.5 �����ʽ�ϵͳ-��ȡ���ҵ�ծȯͶ��-11010101,15010101,15030101
    INSERT INTO RWA_DEV.RWA_TZ_EXPOSURE(
                 DataDate                                                     --��������
                ,DataNo                                                       --������ˮ��
                ,ExposureID                                                   --���ձ�¶ID
                ,DueID                                                        --ծ��ID
                ,SSysID                                                       --ԴϵͳID
                ,ContractID                                                   --��ͬID
                ,ClientID                                                     --��������ID
                ,SOrgID                                                       --Դ����ID
                ,SOrgName                                                     --Դ��������
                ,OrgSortNo                                                    --�������������
                ,OrgID                                                        --��������ID
                ,OrgName                                                      --������������
                ,AccOrgID                                                     --�������ID
                ,AccOrgName                                                   --�����������
                ,IndustryID                                                   --������ҵ����
                ,IndustryName                                                 --������ҵ����
                ,BusinessLine                                                 --ҵ������
                ,AssetType                                                    --�ʲ�����
                ,AssetSubType                                                 --�ʲ�С��
                ,BusinessTypeID                                               --ҵ��Ʒ�ִ���
                ,BusinessTypeName                                             --ҵ��Ʒ������
                ,CreditRiskDataType                                           --���÷�����������
                ,AssetTypeOfHaircuts                                          --�ۿ�ϵ����Ӧ�ʲ����
                ,BusinessTypeSTD                                              --Ȩ�ط�ҵ������
                ,ExpoClassSTD                                                 --Ȩ�ط���¶����
                ,ExpoSubClassSTD                                              --Ȩ�ط���¶С��
                ,ExpoClassIRB                                                 --��������¶����
                ,ExpoSubClassIRB                                              --��������¶С��
                ,ExpoBelong                                                   --��¶������ʶ
                ,BookType                                                     --�˻����
                ,ReguTranType                                                 --��ܽ�������
                ,RepoTranFlag                                                 --�ع����ױ�ʶ
                ,RevaFrequency                                                --�ع�Ƶ��
                ,Currency                                                     --����
                ,NormalPrincipal                                              --�����������
                ,OverdueBalance                                               --�������
                ,NonAccrualBalance                                            --��Ӧ�����
                ,OnSheetBalance                                               --�������
                ,NormalInterest                                               --������Ϣ
                ,OnDebitInterest                                              --����ǷϢ
                ,OffDebitInterest                                             --����ǷϢ
                ,ExpenseReceivable                                            --Ӧ�շ���
                ,AssetBalance                                                 --�ʲ����
                ,AccSubject1                                                  --��Ŀһ
                ,AccSubject2                                                  --��Ŀ��
                ,AccSubject3                                                  --��Ŀ��
                ,StartDate                                                    --��ʼ����
                ,DueDate                                                      --��������
                ,OriginalMaturity                                             --ԭʼ����
                ,ResidualM                                                    --ʣ������
                ,RiskClassify                                                 --���շ���
                ,ExposureStatus                                               --���ձ�¶״̬
                ,OverdueDays                                                  --��������
                ,SpecialProvision                                             --ר��׼����
                ,GeneralProvision                                             --һ��׼����
                ,EspecialProvision                                            --�ر�׼����
                ,WrittenOffAmount                                             --�Ѻ������
                ,OffExpoSource                                                --���Ⱪ¶��Դ
                ,OffBusinessType                                              --����ҵ������
                ,OffBusinessSdvsSTD                                           --Ȩ�ط�����ҵ������ϸ��
                ,UncondCancelFlag                                             --�Ƿ����ʱ����������
                ,CCFLevel                                                     --����ת��ϵ������
                ,CCFAIRB                                                      --�߼�������ת��ϵ��
                ,ClaimsLevel                                                  --ծȨ����
                ,BondFlag                                                     --�Ƿ�Ϊծȯ
                ,BondIssueIntent                                              --ծȯ����Ŀ��
                ,NSURealPropertyFlag                                          --�Ƿ�����ò�����
                ,RepAssetTermType                                             --��ծ�ʲ���������
                ,DependOnFPOBFlag                                             --�Ƿ�����������δ��ӯ��
                ,IRating                                                      --�ڲ�����
                ,PD                                                           --ΥԼ����
                ,LGDLevel                                                     --ΥԼ��ʧ�ʼ���
                ,LGDAIRB                                                      --�߼���ΥԼ��ʧ��
                ,MAIRB                                                        --�߼�����Ч����
                ,EADAIRB                                                      --�߼���ΥԼ���ձ�¶
                ,DefaultFlag                                                  --ΥԼ��ʶ
                ,BEEL                                                         --��ΥԼ��¶Ԥ����ʧ����
                ,DefaultLGD                                                   --��ΥԼ��¶ΥԼ��ʧ��
                ,EquityExpoFlag                                               --��Ȩ��¶��ʶ
                ,EquityInvestType                                             --��ȨͶ�ʶ�������
                ,EquityInvestCause                                            --��ȨͶ���γ�ԭ��
                ,SLFlag                                                       --רҵ�����ʶ
                ,SLType                                                       --רҵ��������
                ,PFPhase                                                      --��Ŀ���ʽ׶�
                ,ReguRating                                                   --�������
                ,CBRCMPRatingFlag                                             --������϶������Ƿ��Ϊ����
                ,LargeFlucFlag                                                --�Ƿ񲨶��Խϴ�
                ,LiquExpoFlag                                                 --�Ƿ���������з��ձ�¶
                ,PaymentDealFlag                                              --�Ƿ����Ը�ģʽ
                ,DelayTradingDays                                             --�ӳٽ�������
                ,SecuritiesFlag                                               --�м�֤ȯ��ʶ
                ,SecuIssuerID                                                 --֤ȯ������ID
                ,RatingDurationType                                           --������������
                ,SecuIssueRating                                              --֤ȯ���еȼ�
                ,SecuResidualM                                                --֤ȯʣ������
                ,SecuRevaFrequency                                            --֤ȯ�ع�Ƶ��
                ,CCPTranFlag                                                  --�Ƿ����뽻�׶�����ؽ���
                ,CCPID                                                        --���뽻�׶���ID
                ,QualCCPFlag                                                  --�Ƿ�ϸ����뽻�׶���
                ,BankRole                                                     --���н�ɫ
                ,ClearingMethod                                               --���㷽ʽ
                ,BankAssetFlag                                                --�Ƿ������ύ�ʲ�
                ,MatchConditions                                              --�����������
                ,SFTFlag                                                      --֤ȯ���ʽ��ױ�ʶ
                ,MasterNetAgreeFlag                                           --���������Э���ʶ
                ,MasterNetAgreeID                                             --���������Э��ID
                ,SFTType                                                      --֤ȯ���ʽ�������
                ,SecuOwnerTransFlag                                           --֤ȯ����Ȩ�Ƿ�ת��
                ,OTCFlag                                                      --�����������߱�ʶ
                ,ValidNettingFlag                                             --��Ч�������Э���ʶ
                ,ValidNetAgreementID                                          --��Ч�������Э��ID
                ,OTCType                                                      --����������������
                ,DepositRiskPeriod                                            --��֤������ڼ�
                ,MTM                                                          --���óɱ�
                ,MTMCurrency                                                  --���óɱ�����
                ,BuyerOrSeller                                                --������
                ,QualROFlag                                                   --�ϸ�����ʲ���ʶ
                ,ROIssuerPerformFlag                                          --�����ʲ��������Ƿ�����Լ
                ,BuyerInsolvencyFlag                                          --���ñ������Ƿ��Ʋ�
                ,NonpaymentFees                                               --��δ֧������
                ,RetailExpoFlag                                               --���۱�¶��ʶ
                ,RetailClaimType                                              --����ծȨ����
                ,MortgageType                                                 --ס����Ѻ��������
                ,ExpoNumber                                                   --���ձ�¶����
                ,LTV                                                          --�����ֵ��
                ,Aging                                                        --����
                ,NewDefaultDebtFlag                                           --����ΥԼծ���ʶ
                ,PDPoolModelID                                                --PD�ֳ�ģ��ID
                ,LGDPoolModelID                                               --LGD�ֳ�ģ��ID
                ,CCFPoolModelID                                               --CCF�ֳ�ģ��ID
                ,PDPoolID                                                     --����PD��ID
                ,LGDPoolID                                                    --����LGD��ID
                ,CCFPoolID                                                    --����CCF��ID
                ,ABSUAFlag                                                    --�ʲ�֤ȯ�������ʲ���ʶ
                ,ABSPoolID                                                    --֤ȯ���ʲ���ID
                ,GroupID                                                      --������
                ,DefaultDate                                                  --ΥԼʱ��
                ,ABSPROPORTION                                                --�ʲ�֤ȯ������
                ,DEBTORNUMBER                                                 --����˸���
                ,SBJT2
                ,SBJT_VAL2
                ,SBJT3
                ,SBJT_VAL3
                ,SBJT4
                ,SBJT_VAL4
                ,SBJT5
                ,SBJT_VAL5
    )
  select TO_DATE(p_data_dt_str, 'YYYYMMDD'),
       p_data_dt_str,
       --T2.ACCT_NO||T2.SECURITY_REFERENCE,
       T2.ACCT_NO,
       --T2.ACCT_NO||T2.SECURITY_REFERENCE,
       T2.ACCT_NO,
       'TZ',
       --T1.BOND_ID,
       T2.ACCT_NO||T2.SECURITY_REFERENCE,
       CASE WHEN T1.BOND_TYPE in('XYB','IBD') THEN 'XN-ZGSYYH' ----XYB��ҵ����ծ
       ELSE NVL(T1.ISSUER_CODE,'XN_JRJG') END            AS CLIENTID,
       T2.ORG_CD,
       T3.ORGNAME,
       --T3.SORTNO,
       NVL(T3.SORTNO,'1010'),
       --T2.ORG_CD,
       DECODE(SUBSTR(T2.ORG_CD,1,1),'@','01000000',T2.ORG_CD),
       --T3.ORGNAME,
       NVL(T3.ORGNAME,'����'),
       T2.ORG_CD,
       T3.ORGNAME,
       NVL(T4.INDUSTRYTYPE,'J6620'),
       T5.ITEMNAME,
       '0401',
       '',
       '',
       '1040102040',
       '�����ծȯͶ��',
       '01',
       '01',
       '07',
       CASE WHEN T1.BOND_TYPE='TB' THEN '0102'--��ծ
            WHEN T1.BOND_TYPE='TBB' THEN '0102'--����ծȯ
            WHEN T1.BOND_TYPE='SSS' THEN '0103'--�ط�����ծ
            WHEN T1.BOND_TYPE='TDZ' THEN '0103'--����ծ
            WHEN T1.BOND_TYPE='PBB' THEN '0104'--����������ծ
            WHEN T1.BOND_TYPE in('XYB','IBD') THEN ''--��Ҳ����ծ/ͬҵ�浥  ���ڻ���
            WHEN T1.BOND_TYPE ='ZQBB'THEN '0106'--����Ʊ��    
            WHEN T1.BOND_TYPE ='CEB'AND T1.BOND_ID='1280175' THEN '0103' --����ծ ��������ʵ�� 
            WHEN T1.BOND_TYPE ='CEB'AND T1.BOND_ID<>'1280175'THEN '0106' --��ҵծ
            WHEN T1.BOND_TYPE ='DOBB'THEN '0106' --��������ȯ
            WHEN T1.BOND_TYPE ='CDQRZQ'THEN '0106' --����������
            WHEN T1.BOND_TYPE ='PPN' THEN '0106'--�ǹ�������ծ�����ʹ���
            WHEN T1.BOND_TYPE ='SEB' THEN '0106'--����Ʊ��
            WHEN T1.BOND_TYPE ='OTHB' THEN '0106'--����ծȯ
            WHEN T1.BOND_TYPE ='CPB' THEN '0106' --��˾ծ
            WHEN T1.BOND_TYPE ='OCB' THEN '0106' --��ҵծȯ 
            WHEN T1.BOND_TYPE ='KFLZ' THEN '0106' --�ɷ���ծ
            WHEN T1.BOND_TYPE ='IPEB' THEN '0106' --���м�˽ļծȯ 
            WHEN T1.BOND_TYPE ='M' THEN '0104'  ---���ڻ���ծ
            WHEN T1.BOND_TYPE ='AMCFB' THEN '0104'  --�ʲ�����˾����ծ
            WHEN T1.BOND_TYPE in ('TTC','OBB','XYBS') THEN '0104' ---�μ�ծ
            WHEN T1.BOND_TYPE ='ABS' THEN ''--ABS ��ABS��ڼ���
            END ,----��¶����
       CASE WHEN T1.BOND_TYPE='TB' THEN '010201'--��ծ
            WHEN T1.BOND_TYPE='TBB' THEN '010202'--����ծȯ
            WHEN T1.BOND_TYPE='SSS' THEN '010303'--�ط�����ծ
            WHEN T1.BOND_TYPE='TDZ' THEN '010302'--����ծ
            WHEN T1.BOND_TYPE='PBB' THEN '010401'--����������ծ
            WHEN T1.BOND_TYPE in('XYB','IBD') THEN ''--��Ҳ����ծ/ͬҵ�浥  ���ڻ���
            WHEN T1.BOND_TYPE ='ZQBB'THEN '010601'--����Ʊ��    
            WHEN T1.BOND_TYPE ='CEB'AND T1.BOND_ID='1280175' THEN '010302' --����ծ ��������ʵ�� 
            WHEN T1.BOND_TYPE ='CEB'AND T1.BOND_ID<>'1280175'THEN '010601' --��ҵծ
            WHEN T1.BOND_TYPE ='DOBB'THEN '010601' --��������ȯ
            WHEN T1.BOND_TYPE ='CDQRZQ'THEN '010601' --����������
            WHEN T1.BOND_TYPE ='PPN' THEN '010601'--�ǹ�������ծ�����ʹ���
            WHEN T1.BOND_TYPE ='SEB' THEN '010601'--����Ʊ��
            WHEN T1.BOND_TYPE ='OTHB' THEN '010601'--����ծȯ
            WHEN T1.BOND_TYPE ='CPB' THEN '010601' --��˾ծ
            WHEN T1.BOND_TYPE ='OCB' THEN '010601' --��ҵծȯ 
            WHEN T1.BOND_TYPE ='KFLZ' THEN '010601' --�ɷ���ծ
            WHEN T1.BOND_TYPE ='IPEB' THEN '010601' --���м�˽ļծȯ 
            WHEN T1.BOND_TYPE ='M' THEN '010408'  ---���ڻ���ծ
            WHEN T1.BOND_TYPE ='AMCFB' THEN '010408'  --�ʲ�����˾����ծ
            WHEN T1.BOND_TYPE in ('TTC','OBB','XYBS') THEN '010407' ---�μ�ծ
            WHEN T1.BOND_TYPE ='ABS' THEN''--ABS ��ABS��ڼ���
            END ,----��¶С��
       '',
       '',
       '01',
       '01',
       '02',
       '0',
       1,
       'CNY',
       NVL(T2.POSITION_INITIAL_VALUE,0),
       0,
       0,
       NVL(T2.POSITION_INITIAL_VALUE,0),
       0,
       0,
       0,
       0,
       NVL(T2.POSITION_INITIAL_VALUE,0),
       T2.SBJT_CD,
       '',
       '',
       NVL(T1.ISSUE_DATE,p_data_dt_str),
       T1.MATU_DT,
      CASE
         WHEN (TO_DATE(T1.MATU_DT, 'YYYY-MM-DD') -
              TO_DATE(NVL(T1.ISSUE_DATE,p_data_dt_str), 'YYYY-MM-DD')) / 365 < 0 THEN
          0
         ELSE
          (TO_DATE(T1.MATU_DT, 'YYYY-MM-DD') -
          TO_DATE(NVL(T1.ISSUE_DATE,p_data_dt_str), 'YYYY-MM-DD')) / 365
       END,
       CASE
         WHEN (TO_DATE(T1.MATU_DT, 'YYYY-MM-DD') -
              TO_DATE(p_data_dt_str, 'YYYYMMDD')) / 365 < 0 THEN
          0
         ELSE
          (TO_DATE(T1.MATU_DT, 'YYYY-MM-DD') -
          TO_DATE(p_data_dt_str, 'YYYYMMDD')) / 365
       END,
       '01',
       '01',
       0,
       0,
       0,
       0,
       0,
       '',
       '',
       '',
       '',
       '',
       '',
       CASE WHEN T1.BOND_TYPE in ('TTC','OBB','XYBS') THEN '02'
         ELSE '01' END ,    ------ծȨ����
       '0',
       NVL(T6.BONDPUBLISHPURPOSE�� '02'),
       '0',
       '',
       '0',
       T7.PDADJLEVEL,
       T7.PD,
       NULL,
       NULL,
       NULL,
       NULL,
       CASE
         WHEN T8.BREAKDATE IS NOT NULL THEN
          '1'
         ELSE
          '0'
       END,
       '',
       '',
       '0',
       '',
       '',
       '0',
       '',
       '',
       '01',
       '',
       '',
       '0',
       '0',
       NULL,
       '0',
       T6.BONDPUBLISHID,
       T6.TIMELIMIT,
       T6.BONDRATING,
       NULL,
       1,
       '0',
       '',
       '',
       '',
       '',
       '0',
       '',
       '0',
       '0',
       '',
       '',
       '0',
       '0',
       '0',
       '',
       '',
       '',
       '',
       '',
       '',
       '0',
       '',
       '',
       '',
       '0',
       '',
       '',
       1,
       0.8,
       NULL,
       '',
       '',
       '',
       '',
       '',
       '',
       '',
       '0',
       '',
       '',
       CASE
         WHEN T7.PDADJLEVEL = '0116' THEN
          TO_DATE(T7.PDVAVLIDDATE, 'YYYYMMDD')
         ELSE
          NULL
       END,
       '',
       '',
       T2.INT_ADJ_ITEM,--��Ϣ����
       T2.INT_ADJ_VAL,--��Ϣ����
       DECODE(SUBSTR(T2.ACCRUAL_GLNO, 1, 4), '1132', NULL,'1101',NULL, T2.ACCRUAL_GLNO),--Ӧ����Ϣ
       DECODE(SUBSTR(T2.ACCRUAL_GLNO, 1, 4), '1132', NULL,'1101',NULL, T2.ACCRUAL),--Ӧ����Ϣ
       T2.FAIR_EXCH_ITEM,--���ʼ�ֵ�䶯 
       T2.FAIR_EXCH_VAL,--���ʼ�ֵ�䶯
       DECODE(T2.SBJT_CD,'11010101','11010104',NULL),--Ӧ����Ϣ 11010101����
       DECODE(T2.SBJT_CD,'11010101',T2.INS_RECEIVABLE,NULL)--Ӧ����Ϣ
  FROM RWA_DEV.BRD_BOND T1
 INNER JOIN (
       SELECT T.ACCT_NO,
       T.SECURITY_REFERENCE,
       T.ORG_CD,
       T.SBJT_CD,
       T.BELONG_GROUP,
       NVL(T.POSITION_INITIAL_VALUE, 0) AS POSITION_INITIAL_VALUE,
       T.INT_ADJ_ITEM,
       NVL(T.INT_ADJ_VAL, 0) AS INT_ADJ_VAL,
       T.ACCRUAL_GLNO,
       NVL(T.ACCRUAL, 0) AS ACCRUAL,
       T.FAIR_EXCH_ITEM,
       NVL(T.FAIR_EXCH_VAL, 0) AS FAIR_EXCH_VAL,
       NVL(T.INS_RECEIVABLE, 0) AS INS_RECEIVABLE
  FROM BRD_SECURITY_POSI T
 WHERE T.BELONG_GROUP = '4'
      --AND SUBSTR(t.sbjt_cd,1,6) = SUBSTR(t.int_adj_item,1,6)
   AND T.DATANO = p_data_dt_str
   AND T.ACCT_NO NOT IN ('SQ0202201905090001',
                         'SQ0202201905090002',
                         'SQ0202201905090003',
                         'SQ0202201905090004')
UNION
SELECT T.ACCT_NO,
       T.SECURITY_REFERENCE,
       T.ORG_CD,
       T.SBJT_CD,
       T.BELONG_GROUP,
       NVL(T.POSITION_INITIAL_VALUE, 0) AS POSITION_INITIAL_VALUE,
       T.INT_ADJ_ITEM,
       NVL(T.INT_ADJ_VAL, 0) AS INT_ADJ_VAL,
       T.ACCRUAL_GLNO,
       NVL(T.ACCRUAL, 0) AS ACCRUAL,
       T.FAIR_EXCH_ITEM,
       NVL(T.FAIR_EXCH_VAL, 0) AS FAIR_EXCH_VAL,
       NVL(T.INS_RECEIVABLE, 0) AS INS_RECEIVABLE
  FROM BRD_SECURITY_POSI T
 WHERE T.BELONG_GROUP = '4'
   AND T.SBJT_CD = '15010101'
   AND T.INT_ADJ_ITEM = '15010102'
   AND T.ACCRUAL_GLNO = '11320701'
   AND T.DATANO = P_DATA_DT_STR
   AND T.ACCT_NO IN ('SQ0202201905090001',
                     'SQ0202201905090002',
                     'SQ0202201905090003',
                     'SQ0202201905090004')
         ) T2
    ON T1.BOND_ID = T2.SECURITY_REFERENCE
   AND T2.BELONG_GROUP = '4' --ծȯ����Ͷ��              
  LEFT JOIN RWA.ORG_INFO T3
    ON T2.ORG_CD = T3.ORGID
   AND T3.STATUS = '1'
  LEFT JOIN NCM_CUSTOMER_INFO T4 --ͳһ�ͻ���Ϣ��
    ON T1.ISSUER_CODE = T4.CUSTOMERID
    AND T1.DATANO=T4.DATANO
   AND T4.CUSTOMERTYPE NOT LIKE '03%' --�Թ��ͻ�             
  LEFT JOIN RWA.CODE_LIBRARY T5
    ON T4.INDUSTRYTYPE = T5.ITEMNO
   AND T5.CODENO = 'IndustryType'
  LEFT JOIN (SELECT *
  FROM (select ROW_NUMBER() OVER(PARTITION BY T.BONDNO ORDER BY T.BONDNO) AS ROW_ID,
               T.*
          from NCM_BOND_INFO t
          WHERE T.DATANO=p_data_dt_str)
 WHERE ROW_ID = 1) T6
    ON T1.BOND_ID = T6.BONDNO
   LEFT JOIN RWA_TEMP_PDLEVEL T7
    ON T1.ISSUER_CODE = T7.CUSTID
  LEFT JOIN (select *
  from (SELECT ROW_NUMBER() OVER(PARTITION BY CUSTOMERID ORDER BY BREAKDATE DESC) as row_id,
               T.*
          FROM NCM_BREAKDEFINEDREMARK T
         where t.breakdate <= p_data_dt_str)
 where row_id = 1) T8
    ON T1.ISSUER_CODE = T8.CUSTOMERID
 WHERE T1.BELONG_GROUP = '4' --ծȯ����Ͷ��
    AND T1.DATANO=p_data_dt_str ;
    commit;
    
    -- �ʹܡ����С����ʼƻ� 11020101��12220101
    INSERT INTO RWA_DEV.RWA_TZ_EXPOSURE(
                 DataDate                                                     --��������
                ,DataNo                                                       --������ˮ��
                ,ExposureID                                                   --���ձ�¶ID
                ,DueID                                                        --ծ��ID
                ,SSysID                                                       --ԴϵͳID
                ,ContractID                                                   --��ͬID
                ,ClientID                                                     --��������ID
                ,SOrgID                                                       --Դ����ID
                ,SOrgName                                                     --Դ��������
                ,OrgSortNo                                                    --�������������
                ,OrgID                                                        --��������ID
                ,OrgName                                                      --������������
                ,AccOrgID                                                     --�������ID
                ,AccOrgName                                                   --�����������
                ,IndustryID                                                   --������ҵ����
                ,IndustryName                                                 --������ҵ����
                ,BusinessLine                                                 --ҵ������
                ,AssetType                                                    --�ʲ�����
                ,AssetSubType                                                 --�ʲ�С��
                ,BusinessTypeID                                               --ҵ��Ʒ�ִ���
                ,BusinessTypeName                                             --ҵ��Ʒ������
                ,CreditRiskDataType                                           --���÷�����������
                ,AssetTypeOfHaircuts                                          --�ۿ�ϵ����Ӧ�ʲ����
                ,BusinessTypeSTD                                              --Ȩ�ط�ҵ������
                ,ExpoClassSTD                                                 --Ȩ�ط���¶����
                ,ExpoSubClassSTD                                              --Ȩ�ط���¶С��
                ,ExpoClassIRB                                                 --��������¶����
                ,ExpoSubClassIRB                                              --��������¶С��
                ,ExpoBelong                                                   --��¶������ʶ
                ,BookType                                                     --�˻����
                ,ReguTranType                                                 --��ܽ�������
                ,RepoTranFlag                                                 --�ع����ױ�ʶ
                ,RevaFrequency                                                --�ع�Ƶ��
                ,Currency                                                     --����
                ,NormalPrincipal                                              --�����������
                ,OverdueBalance                                               --�������
                ,NonAccrualBalance                                            --��Ӧ�����
                ,OnSheetBalance                                               --�������
                ,NormalInterest                                               --������Ϣ
                ,OnDebitInterest                                              --����ǷϢ
                ,OffDebitInterest                                             --����ǷϢ
                ,ExpenseReceivable                                            --Ӧ�շ���
                ,AssetBalance                                                 --�ʲ����
                ,AccSubject1                                                  --��Ŀһ
                ,AccSubject2                                                  --��Ŀ��
                ,AccSubject3                                                  --��Ŀ��
                ,StartDate                                                    --��ʼ����
                ,DueDate                                                      --��������
                ,OriginalMaturity                                             --ԭʼ����
                ,ResidualM                                                    --ʣ������
                ,RiskClassify                                                 --���շ���
                ,ExposureStatus                                               --���ձ�¶״̬
                ,OverdueDays                                                  --��������
                ,SpecialProvision                                             --ר��׼����
                ,GeneralProvision                                             --һ��׼����
                ,EspecialProvision                                            --�ر�׼����
                ,WrittenOffAmount                                             --�Ѻ������
                ,OffExpoSource                                                --���Ⱪ¶��Դ
                ,OffBusinessType                                              --����ҵ������
                ,OffBusinessSdvsSTD                                           --Ȩ�ط�����ҵ������ϸ��
                ,UncondCancelFlag                                             --�Ƿ����ʱ����������
                ,CCFLevel                                                     --����ת��ϵ������
                ,CCFAIRB                                                      --�߼�������ת��ϵ��
                ,ClaimsLevel                                                  --ծȨ����
                ,BondFlag                                                     --�Ƿ�Ϊծȯ
                ,BondIssueIntent                                              --ծȯ����Ŀ��
                ,NSURealPropertyFlag                                          --�Ƿ�����ò�����
                ,RepAssetTermType                                             --��ծ�ʲ���������
                ,DependOnFPOBFlag                                             --�Ƿ�����������δ��ӯ��
                ,IRating                                                      --�ڲ�����
                ,PD                                                           --ΥԼ����
                ,LGDLevel                                                     --ΥԼ��ʧ�ʼ���
                ,LGDAIRB                                                      --�߼���ΥԼ��ʧ��
                ,MAIRB                                                        --�߼�����Ч����
                ,EADAIRB                                                      --�߼���ΥԼ���ձ�¶
                ,DefaultFlag                                                  --ΥԼ��ʶ
                ,BEEL                                                         --��ΥԼ��¶Ԥ����ʧ����
                ,DefaultLGD                                                   --��ΥԼ��¶ΥԼ��ʧ��
                ,EquityExpoFlag                                               --��Ȩ��¶��ʶ
                ,EquityInvestType                                             --��ȨͶ�ʶ�������
                ,EquityInvestCause                                            --��ȨͶ���γ�ԭ��
                ,SLFlag                                                       --רҵ�����ʶ
                ,SLType                                                       --רҵ��������
                ,PFPhase                                                      --��Ŀ���ʽ׶�
                ,ReguRating                                                   --�������
                ,CBRCMPRatingFlag                                             --������϶������Ƿ��Ϊ����
                ,LargeFlucFlag                                                --�Ƿ񲨶��Խϴ�
                ,LiquExpoFlag                                                 --�Ƿ���������з��ձ�¶
                ,PaymentDealFlag                                              --�Ƿ����Ը�ģʽ
                ,DelayTradingDays                                             --�ӳٽ�������
                ,SecuritiesFlag                                               --�м�֤ȯ��ʶ
                ,SecuIssuerID                                                 --֤ȯ������ID
                ,RatingDurationType                                           --������������
                ,SecuIssueRating                                              --֤ȯ���еȼ�
                ,SecuResidualM                                                --֤ȯʣ������
                ,SecuRevaFrequency                                            --֤ȯ�ع�Ƶ��
                ,CCPTranFlag                                                  --�Ƿ����뽻�׶�����ؽ���
                ,CCPID                                                        --���뽻�׶���ID
                ,QualCCPFlag                                                  --�Ƿ�ϸ����뽻�׶���
                ,BankRole                                                     --���н�ɫ
                ,ClearingMethod                                               --���㷽ʽ
                ,BankAssetFlag                                                --�Ƿ������ύ�ʲ�
                ,MatchConditions                                              --�����������
                ,SFTFlag                                                      --֤ȯ���ʽ��ױ�ʶ
                ,MasterNetAgreeFlag                                           --���������Э���ʶ
                ,MasterNetAgreeID                                             --���������Э��ID
                ,SFTType                                                      --֤ȯ���ʽ�������
                ,SecuOwnerTransFlag                                           --֤ȯ����Ȩ�Ƿ�ת��
                ,OTCFlag                                                      --�����������߱�ʶ
                ,ValidNettingFlag                                             --��Ч�������Э���ʶ
                ,ValidNetAgreementID                                          --��Ч�������Э��ID
                ,OTCType                                                      --����������������
                ,DepositRiskPeriod                                            --��֤������ڼ�
                ,MTM                                                          --���óɱ�
                ,MTMCurrency                                                  --���óɱ�����
                ,BuyerOrSeller                                                --������
                ,QualROFlag                                                   --�ϸ�����ʲ���ʶ
                ,ROIssuerPerformFlag                                          --�����ʲ��������Ƿ�����Լ
                ,BuyerInsolvencyFlag                                          --���ñ������Ƿ��Ʋ�
                ,NonpaymentFees                                               --��δ֧������
                ,RetailExpoFlag                                               --���۱�¶��ʶ
                ,RetailClaimType                                              --����ծȨ����
                ,MortgageType                                                 --ס����Ѻ��������
                ,ExpoNumber                                                   --���ձ�¶����
                ,LTV                                                          --�����ֵ��
                ,Aging                                                        --����
                ,NewDefaultDebtFlag                                           --����ΥԼծ���ʶ
                ,PDPoolModelID                                                --PD�ֳ�ģ��ID
                ,LGDPoolModelID                                               --LGD�ֳ�ģ��ID
                ,CCFPoolModelID                                               --CCF�ֳ�ģ��ID
                ,PDPoolID                                                     --����PD��ID
                ,LGDPoolID                                                    --����LGD��ID
                ,CCFPoolID                                                    --����CCF��ID
                ,ABSUAFlag                                                    --�ʲ�֤ȯ�������ʲ���ʶ
                ,ABSPoolID                                                    --֤ȯ���ʲ���ID
                ,GroupID                                                      --������
                ,DefaultDate                                                  --ΥԼʱ��
                ,ABSPROPORTION                                                --�ʲ�֤ȯ������
                ,DEBTORNUMBER                                                 --����˸���
                ,SBJT2
                ,SBJT_VAL2
                ,SBJT3
                ,SBJT_VAL3
                ,SBJT4
                ,SBJT_VAL4
                ,SBJT5
                ,SBJT_VAL5
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
                           AND NVL(INITIAL_COST, 0) + NVL(MKT_VALUE_CHANGE, 0) + NVL(ACCOUNTABLE_INT, 0) <> 0 --NVL(INT_ADJUST, 0) + ����Ϣ�������⣬��Ϊ���ֹ�����
    )
    SELECT
                TO_DATE(p_data_dt_str,'YYYYMMDD')                                       AS DataDate               --��������
                ,p_data_dt_str                                                          AS DataNo                 --������ˮ��
                ,T1.BOND_ID                                                             AS ExposureID             --���ձ�¶ID
                ,T1.BOND_ID                                                             AS DueID                  --ծ��ID
                ,'TZ'                                                                   AS SSysID                 --ԴϵͳID                       Ĭ�� Ͷ��(TZ)
                ,T1.BOND_ID                                                             AS ContractID             --��ͬID
                , case WHEN T1.BOND_NAME LIKE '%����%' AND T1.BOND_NAME NOT LIKE'%����%' 
                  THEN 'XN-ZGSYYH'
                  ELSE NVL(nvl(T3.OBJECTNO,T5.PARTICIPANT_CODE),'XN-YBGS') END          AS ClientID               --��������ID                      Ĭ��һ��һ�㹫˾
                ,T1.DEPARTMENT                                                          AS SOrgID                 --Դ����ID
                ,T4.ORGNAME                                                             AS SOrgName               --Դ��������
                ,NVL(T4.SORTNO,'1010')
                ,decode(substr(T1.DEPARTMENT,1,1),'@','01000000',T1.DEPARTMENT)
                ,NVL(T4.ORGNAME,'����')
                ,T1.DEPARTMENT AS AccOrgID --�������ID
                ,T4.ORGNAME AS AccOrgName --�����������
                ,NVL(T3.INDUSTRYTYPE,'J6620') AS IndustryID --������ҵ����
                ,'' AS IndustryName           --������ҵ����
                ,'0401' AS BusinessLine           --ҵ������                        Ĭ�� ͬҵ(04)
                ,'' AS AssetType              --�ʲ�����                        Ĭ�� NULL RWA�������
                ,'' AS AssetSubType           --�ʲ�С��                        Ĭ�� NULL RWA�������
                ,'1040105060' AS BusinessTypeID         --ҵ��Ʒ�ִ���
                ,CASE WHEN T1.BOND_NAME LIKE '%����%'  THEN 'Ӧ�տ�����Ͷ��_�������'
                      WHEN T1.BOND_NAME NOT LIKE '%����%' AND T1.BOND_NAME LIKE '%���%' THEN 'Ӧ�տ�����Ͷ��_�Ǳ������'
                      WHEN T1.BOND_NAME LIKE '%����%' THEN 'Ӧ�տ�����Ͷ��_���ʼƻ�'
                      WHEN T1.BOND_NAME LIKE '%��%��%' THEN 'Ӧ�տ�����Ͷ��_�ʹܼƻ�'
                      WHEN T1.BOND_NAME LIKE '%��%��%' THEN 'Ӧ�տ�����Ͷ��_���мƻ�'
                      WHEN T1.BOND_TYPE1 ='2008' THEN'ë��ϯ����'
                      ELSE 'Ӧ�տ�����Ͷ��_�ʹ�����'
                 END AS BusinessTypeName       --ҵ��Ʒ������
                ,'01' AS CreditRiskDataType     --���÷�����������                Ĭ�� һ�������(01)
                ,'01' AS AssetTypeOfHaircuts    --�ۿ�ϵ����Ӧ�ʲ����            Ĭ�� �ֽ��ֽ�ȼ���(01)
                ,'07' AS BusinessTypeSTD        --Ȩ�ط�ҵ������                 Ĭ�� һ���ʲ�(07)
                ,CASE WHEN T1.BOND_NAME LIKE '%����%' AND T1.BOND_NAME LIKE'%����%' THEN '0104' ---���������� �������
                      WHEN T1.BOND_NAME LIKE '%����%' AND T1.BOND_NAME NOT LIKE'%����%' THEN '0104' ---��ҵ���� �������
                      WHEN T1.BOND_NAME NOT LIKE '%����%' AND T1.BOND_NAME LIKE '%���%' THEN '0112' -- �Ǳ������
                      WHEN T1.BOND_NAME LIKE '%����%' THEN '0106'
                      WHEN T1.BOND_NAME LIKE '%��%��%' THEN '0104'
                      WHEN T1.BOND_NAME LIKE '%��%��%' THEN '0104'
                      WHEN T1.BOND_TYPE1='2008'  THEN'0112'
                      ELSE '0104'
                        END  AS ExpoClassSTD           --Ȩ�ط���¶����                 Ĭ�� 011216-��������100%����Ȩ�ص��ʲ�
                ,CASE WHEN T1.BOND_NAME LIKE '%����%' AND T1.BOND_NAME LIKE'%����%' THEN '010401' ---���������� �������
                      WHEN T1.BOND_NAME LIKE '%����%' AND T1.BOND_NAME NOT LIKE'%����%' THEN '010406' ---��ҵ���� �������
                      WHEN T1.BOND_NAME NOT LIKE '%����%' AND T1.BOND_NAME LIKE '%���%' THEN '011216' -- �Ǳ������
                      WHEN T1.BOND_NAME LIKE '%����%' THEN '010601'
                      WHEN T1.BOND_NAME LIKE '%��%��%' THEN '010408'
                      WHEN T1.BOND_NAME LIKE '%��%��%' THEN '010408'
                      WHEN T1.BOND_TYPE1='2008'  THEN'011216'
                      ELSE '010408'
                        END    AS ExpoSubClassSTD        --Ȩ�ط���¶С��                 Ĭ�� 011216-��������100%����Ȩ�ص��ʲ�
                ,'' AS ExpoClassIRB           --��������¶����                 Ĭ�� NULL RWA������
                ,'' AS ExpoSubClassIRB        --��������¶С��                 Ĭ�� NULL RWA������
                ,'01' AS ExpoBelong             --��¶������ʶ                    Ĭ�ϣ�����(01)
                ,CASE WHEN T1.ASSET_CLASS = '10' THEN '02'
                      ELSE '01'
                 END AS BookType               --�˻����                        �ʲ����� �� �������Խ����ʲ�(10)�� , ��Ϊ02-�����˻����ʲ����� �� �������Խ����ʲ���  , ��Ϊ01-�����˻�
                ,'02' AS ReguTranType           --��ܽ�������                    Ĭ�� �����ʱ��г�����(02)
                ,'0' AS RepoTranFlag           --�ع����ױ�ʶ                    Ĭ�� ��(0)
                ,1 AS RevaFrequency          --�ع�Ƶ��                        Ĭ��  1
                ,NVL(T1.CURRENCY_CODE,'CNY') AS Currency               --����
                ,NVL(T2.INITIAL_COST,0) AS NormalPrincipal        --�����������                    �����������ɱ�����Ϣ����(initial_cost)�����ʼ�ֵ�䶯/���ʼ�ֵ�䶯����(int_adjust)��Ӧ����Ϣ(mkt_value_change)
                ,0 AS OverdueBalance         --�������                        Ĭ�� 0
                ,0 AS NonAccrualBalance      --��Ӧ�����                     Ĭ�� 0
                ,NVL(T2.INITIAL_COST,0)                                                 AS OnSheetBalance         --�������                        �������=�����������+�������+��Ӧ�����
                ,0                                                                      AS NormalInterest         --������Ϣ                        Ĭ�� 0
                ,0                                                                      AS OnDebitInterest        --����ǷϢ                        Ĭ�� 0
                ,0                                                                      AS OffDebitInterest       --����ǷϢ                        Ĭ�� 0
                ,0                                                                      AS ExpenseReceivable      --Ӧ�շ���                        Ĭ�� 0
                ,NVL(T2.INITIAL_COST,0)                                                 AS AssetBalance           --�ʲ����                        �����ʲ������=�������+Ӧ�շ���+������Ϣ+����ǷϢ
                ,NVL(B.Sbjt_Cd,'12220101')                                                                    AS AccSubject1            --��Ŀһ                         ����ԭϵͳ���ʲ�������ջ�ƿ�Ŀ��ȷ��
                ,''                                                                     AS AccSubject2            --��Ŀ��                         Ĭ�� NULL
                ,''                                                                     AS AccSubject3            --��Ŀ��                         Ĭ�� NULL
                ,T1.origination_date                                                         AS StartDate              --��ʼ����
                ,T1.MATURITY_DATE                                                       AS DueDate                --��������
                ,CASE WHEN (TO_DATE(T1.MATURITY_DATE,'YYYYMMDD') - TO_DATE(T1.origination_date,'YYYYMMDD')) / 365<0
                      THEN 0
                      ELSE (TO_DATE(T1.MATURITY_DATE,'YYYYMMDD') - TO_DATE(T1.origination_date,'YYYYMMDD')) / 365
                 END                                                                    AS OriginalMaturity       --ԭʼ����                        ��λ ��
                ,CASE WHEN (TO_DATE(T1.MATURITY_DATE,'YYYYMMDD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365<0
                      THEN 0
                      ELSE (TO_DATE(T1.MATURITY_DATE,'YYYYMMDD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365
                 END                                                                    AS ResidualM              --ʣ������                        ��λ ��
                ,'01'                                                                   AS RiskClassify           --���շ���                        Ĭ�� ����(01)
                ,''                                                                     AS ExposureStatus         --���ձ�¶״̬                    Ĭ�� NULL
                ,0                                                                      AS OverdueDays            --��������                        Ĭ�� 0
                ,0                                                                      AS SpecialProvision       --ר��׼����                     Ĭ�� 0 RWA���� ��Ŀ12220400��ֱ����1%��׼����
                ,0                                                                      AS GeneralProvision       --һ��׼����                     Ĭ�� 0 RWA����
                ,0                                                                      AS EspecialProvision      --�ر�׼����                     Ĭ�� 0 RWA����
                ,0                                                                      AS WrittenOffAmount       --�Ѻ������                     Ĭ�� 0
                ,''                                                                     AS OffExpoSource          --���Ⱪ¶��Դ                    Ĭ�� NULL
                ,''                                                                     AS OffBusinessType        --����ҵ������                    Ĭ�� NULL
                ,''                                                                     AS OffBusinessSdvsSTD     --Ȩ�ط�����ҵ������ϸ��         Ĭ�� NULL
                ,''                                                                     AS UncondCancelFlag       --�Ƿ����ʱ����������            Ĭ�� NULL
                ,''                                                                     AS CCFLevel               --����ת��ϵ������                Ĭ�� NULL
                ,NULL                                                                   AS CCFAIRB                --�߼�������ת��ϵ��             Ĭ�� NULL
                ,CASE WHEN T1.BOND_TYPE2 = '20' THEN '02'
                      ELSE '01'
                 END                                                                    AS ClaimsLevel            --ծȨ����                        ծȯ����2���μ�ծȯ(20)����ծȨ���𣽴μ�ծȨ(02)������Ϊ�߼�ծȨ(01)
                ,'0'                                                                    AS BondFlag               --�Ƿ�Ϊծȯ                     Ĭ�� ��(0)
                ,'02'                                                                   AS BondIssueIntent        --ծȯ����Ŀ��                    Ĭ�� ����(02)
                ,'0'                                                                    AS NSURealPropertyFlag    --�Ƿ�����ò�����                Ĭ�� ��(0)
                ,''                                                                     AS RepAssetTermType       --��ծ�ʲ���������                Ĭ�� NULL
                ,'0'                                                                    AS DependOnFPOBFlag       --�Ƿ�����������δ��ӯ��         Ĭ�� ��(0)
                ,T6.Pdadjlevel                                                          AS IRating                --�ڲ�����
                ,T6.Pd                                                                  AS PD                     --ΥԼ����
                ,''                                                                     AS LGDLevel               --ΥԼ��ʧ�ʼ���                 Ĭ�� NULL
                ,NULL                                                                   AS LGDAIRB                --�߼���ΥԼ��ʧ��                Ĭ�� NULL
                ,NULL                                                                   AS MAIRB                  --�߼�����Ч����                 Ĭ�� NULL
                ,NULL                                                                   AS EADAIRB                --�߼���ΥԼ���ձ�¶             Ĭ�� NULL
                ,'0'                                                                    AS DefaultFlag            --ΥԼ��ʶ
                ,CASE WHEN T1.BOND_TYPE2 = '20' THEN 0.75
                 ELSE 0.45
                 END                                                                    AS BEEL                   --��ΥԼ��¶Ԥ����ʧ����         ծȨ���𣽡��߼�ծȨ����BEEL �� 45%��ծȨ���𣽡��μ�ծȨ����BEEL �� 75%
                ,CASE WHEN T1.BOND_TYPE2 = '20' THEN 0.75
                 ELSE 0.45
                 END                                                                    AS DefaultLGD             --��ΥԼ��¶ΥԼ��ʧ��            Ĭ�� NULL
                ,'0'                                                                    AS EquityExpoFlag         --��Ȩ��¶��ʶ                    Ĭ�� ��(0)
                ,''                                                                     AS EquityInvestType       --��ȨͶ�ʶ�������                Ĭ�� NULL
                ,''                                                                     AS EquityInvestCause      --��ȨͶ���γ�ԭ��                Ĭ�� NULL
                ,'0'                                                                    AS SLFlag                 --רҵ�����ʶ                    Ĭ�� ��(0)
                ,''                                                                     AS SLType                 --רҵ��������                    Ĭ�� NULL
                ,''                                                                     AS PFPhase                --��Ŀ���ʽ׶�                    Ĭ�� NULL
                ,'01'                                                                   AS ReguRating             --�������                        Ĭ�� ��(01)
                ,''                                                                     AS CBRCMPRatingFlag       --������϶������Ƿ��Ϊ����       Ĭ�� NULL
                ,''                                                                     AS LargeFlucFlag          --�Ƿ񲨶��Խϴ�                 Ĭ�� NULL
                ,'0'                                                                    AS LiquExpoFlag           --�Ƿ���������з��ձ�¶         Ĭ�� ��(0)
                ,'1'                                                                    AS PaymentDealFlag        --�Ƿ����Ը�ģʽ                Ĭ�� ��(1)
                ,NULL                                                                   AS DelayTradingDays       --�ӳٽ�������                    Ĭ�� NULL
                ,'0'                                                                    AS SecuritiesFlag         --�м�֤ȯ��ʶ                    Ĭ�� ��(0)
                ,''                                                                     AS SecuIssuerID           --֤ȯ������ID                   Ĭ�� NULL
                ,''                                                                     AS RatingDurationType     --������������                    Ĭ�� NULL
                ,''                                                                     AS SecuIssueRating        --֤ȯ���еȼ�                    Ĭ�� NULL
                ,NULL                                                                   AS SecuResidualM          --֤ȯʣ������                    Ĭ�� NULL
                ,1                                                                      AS SecuRevaFrequency      --֤ȯ�ع�Ƶ��                    Ĭ�� 1
                ,'0'                                                                    AS CCPTranFlag            --�Ƿ����뽻�׶�����ؽ���        Ĭ�� ��(0)
                ,''                                                                     AS CCPID                  --���뽻�׶���ID                  Ĭ�� NULL
                ,''                                                                     AS QualCCPFlag            --�Ƿ�ϸ����뽻�׶���            Ĭ�� NULL
                ,''                                                                     AS BankRole               --���н�ɫ                        Ĭ�� NULL
                ,''                                                                     AS ClearingMethod         --���㷽ʽ                        Ĭ�� NULL
                ,''                                                                     AS BankAssetFlag          --�Ƿ������ύ�ʲ�                Ĭ�� NULL
                ,''                                                                     AS MatchConditions        --�����������                    Ĭ�� NULL
                ,'0'                                                                    AS SFTFlag                --֤ȯ���ʽ��ױ�ʶ                Ĭ�� ��(0)
                ,'0'                                                                    AS MasterNetAgreeFlag     --���������Э���ʶ             Ĭ�� ��(0)
                ,''                                                                     AS MasterNetAgreeID       --���������Э��ID               Ĭ�� NULL
                ,''                                                                     AS SFTType                --֤ȯ���ʽ�������                Ĭ�� NULL
                ,''                                                                     AS SecuOwnerTransFlag     --֤ȯ����Ȩ�Ƿ�ת��             Ĭ�� NULL
                ,'0'                                                                    AS OTCFlag                --�����������߱�ʶ                Ĭ�� ��(0)
                ,''                                                                     AS ValidNettingFlag       --��Ч�������Э���ʶ            Ĭ�� NULL
                ,''                                                                     AS ValidNetAgreementID    --��Ч�������Э��ID              Ĭ�� NULL
                ,''                                                                     AS OTCType                --����������������                Ĭ�� NULL
                ,''                                                                     AS DepositRiskPeriod      --��֤������ڼ�                 Ĭ�� NULL
                ,''                                                                     AS MTM                    --���óɱ�                        Ĭ�� NULL
                ,''                                                                     AS MTMCurrency            --���óɱ�����                    Ĭ�� NULL
                ,''                                                                     AS BuyerOrSeller          --������                        Ĭ�� NULL
                ,''                                                                     AS QualROFlag             --�ϸ�����ʲ���ʶ                Ĭ�� NULL
                ,''                                                                     AS ROIssuerPerformFlag    --�����ʲ��������Ƿ�����Լ        Ĭ�� NULL
                ,''                                                                     AS BuyerInsolvencyFlag    --���ñ������Ƿ��Ʋ�            Ĭ�� NULL
                ,''                                                                     AS NonpaymentFees         --��δ֧������                    Ĭ�� NULL
                ,'0'                                                                    AS RetailExpoFlag         --���۱�¶��ʶ                    Ĭ�� ��(0)
                ,''                                                                     AS RetailClaimType        --����ծȨ����                    Ĭ�� NULL
                ,''                                                                     AS MortgageType           --ס����Ѻ��������                Ĭ�� NULL
                ,1                                                                      AS ExpoNumber             --���ձ�¶����                    Ĭ�� 1
                ,0.8                                                                    AS LTV                    --�����ֵ��                     Ĭ�� 0.8
                ,NULL                                                                   AS Aging                  --����                            Ĭ�� NULL
                ,''                                                                     AS NewDefaultDebtFlag     --����ΥԼծ���ʶ                Ĭ�� NULL
                ,''                                                                     AS PDPoolModelID          --PD�ֳ�ģ��ID                    Ĭ�� NULL
                ,''                                                                     AS LGDPoolModelID         --LGD�ֳ�ģ��ID                   Ĭ�� NULL
                ,''                                                                     AS CCFPoolModelID         --CCF�ֳ�ģ��ID                   Ĭ�� NULL
                ,''                                                                     AS PDPoolID               --����PD��ID                     Ĭ�� NULL
                ,''                                                                     AS LGDPoolID              --����LGD��ID                    Ĭ�� NULL
                ,''                                                                     AS CCFPoolID              --����CCF��ID                    Ĭ�� NULL
                ,'0'                                                                    AS ABSUAFlag              --�ʲ�֤ȯ�������ʲ���ʶ         Ĭ�� ��(0)
                ,''                                                                     AS ABSPoolID              --֤ȯ���ʲ���ID                  Ĭ�� NULL
                ,''                                                                     AS GROUPID                --������                        Ĭ�� NULL
                ,NULL                                                                   AS DEFAULTDATE            --ΥԼʱ��
                ,NULL                                                                   AS ABSPROPORTION          --�ʲ�֤ȯ������
                ,NULL                                                                   AS DEBTORNUMBER           --����˸���
                ,NULL AS SBJT2
                ,T2.INT_ADJUST AS SBJT_VAL2
                ,NULL AS SBJT3
                ,T2.ACCOUNTABLE_INT AS SBJT_VAL3
                ,NULL AS SBJT4
                ,T2.MKT_VALUE_CHANGE AS SBJT_VAL4
                ,NULL
                ,NULL
    FROM        RWA_DEV.FNS_BND_INFO_B T1
    INNER JOIN  TEMP_BND_BOOK T2
    ON          T1.BOND_ID = T2.BOND_ID
    LEFT JOIN BRD_UN_BOND B
    ON T1.BOND_ID=B.ACCT_NO
    AND T1.DATANO=B.DATANO
    AND B.BELONG_GROUP = '1'
    LEFT JOIN (
         SELECT *
                FROM (SELECT T1.THIRDPARTYACCOUNTS,
                             T2.OBJECTNO,--�ͻ���
                             T3.CUSTOMERNAME,
                             T3.INDUSTRYTYPE,
                             ROW_NUMBER() OVER(PARTITION BY T1.THIRDPARTYACCOUNTS ORDER BY NVL(LENGTH(T3.CUSTOMERNAME), 0) DESC) AS ROW_ID
                        FROM NCM_BUSINESS_DUEBILL T1 --��ݱ�
                  INNER JOIN NCM_CONTRACT_RELATIVE T2 --��ͬ������
                          ON T1.RELATIVESERIALNO2 = T2.SERIALNO
                         AND T1.DATANO = T2.DATANO
                  INNER JOIN NCM_CUSTOMER_INFO T3 --�ͻ���Ϣ��
                          ON T2.OBJECTNO = T3.CUSTOMERID
                         AND T2.DATANO = T3.DATANO
                       WHERE T1.THIRDPARTYACCOUNTS IS NOT NULL
                         AND T1.DATANO = p_data_dt_str) T
                       WHERE ROW_ID = 1) t3
           ON 'CW_IMPORTDATA'||t1.bond_id=t3.THIRDPARTYACCOUNTS
    LEFT JOIN   RWA.ORG_INFO T4                                           --RWA������
           ON   T1.DEPARTMENT = T4.ORGID
    LEFT JOIN (
    SELECT *
  FROM (SELECT T.DATANO,
               T.BOND_ID,
               T.PARTICIPANT_CODE,
               B.PARTICIPANT_NAME,
               ROW_NUMBER() OVER(PARTITION BY T.DATANO, T.BOND_ID, T.PARTICIPANT_CODE ORDER BY T.SORT_SEQ DESC) AS ROW_ID
          FROM FNS_BND_TRANSACTION_B T
          LEFT JOIN FNS_BND_PARTICIPANT_B B
            ON T.DATANO = B.DATANO
           AND T.PARTICIPANT_CODE = B.PARTICIPANT_CODE
         WHERE T.PARTICIPANT_CODE IS NOT NULL)
 WHERE ROW_ID = 1) T5
       ON T1.DATANO = T5.DATANO
       AND T1.BOND_ID = T5. BOND_ID
    LEFT JOIN RWA_TEMP_PDLEVEL T6 --�����������ͻ�������ʱ��
           ON T6.CUSTID = NVL(T3.OBJECTNO,T5.PARTICIPANT_CODE)
        WHERE   T1.BOND_TYPE1 IN ('2018','2019','2008')  --ɸѡ���ʼƻ�  by wzb  2008 �������й�Ʊ
          AND   t1.par_value <> 0
          AND   T1.DATANO = p_data_dt_str                           --ծȯ��Ϣ��,��ȡ��Ч��ծȯ��Ϣ
          AND   T1.CLOSED <> '1' --�ų��ѹرյ�
         -- AND   T1.BOND_ID NOT in('B201803296435','B201712285095')
    ;     -- �ų�ABS��Ʒ BY WZB

    COMMIT;
    
    -- ����͢ 15010101
    INSERT INTO RWA_DEV.RWA_TZ_EXPOSURE(
                 DataDate                                                     --��������
                ,DataNo                                                       --������ˮ��
                ,ExposureID                                                   --���ձ�¶ID
                ,DueID                                                        --ծ��ID
                ,SSysID                                                       --ԴϵͳID
                ,ContractID                                                   --��ͬID
                ,ClientID                                                     --��������ID
                ,SOrgID                                                       --Դ����ID
                ,SOrgName                                                     --Դ��������
                ,OrgSortNo                                                    --�������������
                ,OrgID                                                        --��������ID
                ,OrgName                                                      --������������
                ,AccOrgID                                                     --�������ID
                ,AccOrgName                                                   --�����������
                ,IndustryID                                                   --������ҵ����
                ,IndustryName                                                 --������ҵ����
                ,BusinessLine                                                 --ҵ������
                ,AssetType                                                    --�ʲ�����
                ,AssetSubType                                                 --�ʲ�С��
                ,BusinessTypeID                                               --ҵ��Ʒ�ִ���
                ,BusinessTypeName                                             --ҵ��Ʒ������
                ,CreditRiskDataType                                           --���÷�����������
                ,AssetTypeOfHaircuts                                          --�ۿ�ϵ����Ӧ�ʲ����
                ,BusinessTypeSTD                                              --Ȩ�ط�ҵ������
                ,ExpoClassSTD                                                 --Ȩ�ط���¶����
                ,ExpoSubClassSTD                                              --Ȩ�ط���¶С��
                ,ExpoClassIRB                                                 --��������¶����
                ,ExpoSubClassIRB                                              --��������¶С��
                ,ExpoBelong                                                   --��¶������ʶ
                ,BookType                                                     --�˻����
                ,ReguTranType                                                 --��ܽ�������
                ,RepoTranFlag                                                 --�ع����ױ�ʶ
                ,RevaFrequency                                                --�ع�Ƶ��
                ,Currency                                                     --����
                ,NormalPrincipal                                              --�����������
                ,OverdueBalance                                               --�������
                ,NonAccrualBalance                                            --��Ӧ�����
                ,OnSheetBalance                                               --�������
                ,NormalInterest                                               --������Ϣ
                ,OnDebitInterest                                              --����ǷϢ
                ,OffDebitInterest                                             --����ǷϢ
                ,ExpenseReceivable                                            --Ӧ�շ���
                ,AssetBalance                                                 --�ʲ����
                ,AccSubject1                                                  --��Ŀһ
                ,AccSubject2                                                  --��Ŀ��
                ,AccSubject3                                                  --��Ŀ��
                ,StartDate                                                    --��ʼ����
                ,DueDate                                                      --��������
                ,OriginalMaturity                                             --ԭʼ����
                ,ResidualM                                                    --ʣ������
                ,RiskClassify                                                 --���շ���
                ,ExposureStatus                                               --���ձ�¶״̬
                ,OverdueDays                                                  --��������
                ,SpecialProvision                                             --ר��׼����
                ,GeneralProvision                                             --һ��׼����
                ,EspecialProvision                                            --�ر�׼����
                ,WrittenOffAmount                                             --�Ѻ������
                ,OffExpoSource                                                --���Ⱪ¶��Դ
                ,OffBusinessType                                              --����ҵ������
                ,OffBusinessSdvsSTD                                           --Ȩ�ط�����ҵ������ϸ��
                ,UncondCancelFlag                                             --�Ƿ����ʱ����������
                ,CCFLevel                                                     --����ת��ϵ������
                ,CCFAIRB                                                      --�߼�������ת��ϵ��
                ,ClaimsLevel                                                  --ծȨ����
                ,BondFlag                                                     --�Ƿ�Ϊծȯ
                ,BondIssueIntent                                              --ծȯ����Ŀ��
                ,NSURealPropertyFlag                                          --�Ƿ�����ò�����
                ,RepAssetTermType                                             --��ծ�ʲ���������
                ,DependOnFPOBFlag                                             --�Ƿ�����������δ��ӯ��
                ,IRating                                                      --�ڲ�����
                ,PD                                                           --ΥԼ����
                ,LGDLevel                                                     --ΥԼ��ʧ�ʼ���
                ,LGDAIRB                                                      --�߼���ΥԼ��ʧ��
                ,MAIRB                                                        --�߼�����Ч����
                ,EADAIRB                                                      --�߼���ΥԼ���ձ�¶
                ,DefaultFlag                                                  --ΥԼ��ʶ
                ,BEEL                                                         --��ΥԼ��¶Ԥ����ʧ����
                ,DefaultLGD                                                   --��ΥԼ��¶ΥԼ��ʧ��
                ,EquityExpoFlag                                               --��Ȩ��¶��ʶ
                ,EquityInvestType                                             --��ȨͶ�ʶ�������
                ,EquityInvestCause                                            --��ȨͶ���γ�ԭ��
                ,SLFlag                                                       --רҵ�����ʶ
                ,SLType                                                       --רҵ��������
                ,PFPhase                                                      --��Ŀ���ʽ׶�
                ,ReguRating                                                   --�������
                ,CBRCMPRatingFlag                                             --������϶������Ƿ��Ϊ����
                ,LargeFlucFlag                                                --�Ƿ񲨶��Խϴ�
                ,LiquExpoFlag                                                 --�Ƿ���������з��ձ�¶
                ,PaymentDealFlag                                              --�Ƿ����Ը�ģʽ
                ,DelayTradingDays                                             --�ӳٽ�������
                ,SecuritiesFlag                                               --�м�֤ȯ��ʶ
                ,SecuIssuerID                                                 --֤ȯ������ID
                ,RatingDurationType                                           --������������
                ,SecuIssueRating                                              --֤ȯ���еȼ�
                ,SecuResidualM                                                --֤ȯʣ������
                ,SecuRevaFrequency                                            --֤ȯ�ع�Ƶ��
                ,CCPTranFlag                                                  --�Ƿ����뽻�׶�����ؽ���
                ,CCPID                                                        --���뽻�׶���ID
                ,QualCCPFlag                                                  --�Ƿ�ϸ����뽻�׶���
                ,BankRole                                                     --���н�ɫ
                ,ClearingMethod                                               --���㷽ʽ
                ,BankAssetFlag                                                --�Ƿ������ύ�ʲ�
                ,MatchConditions                                              --�����������
                ,SFTFlag                                                      --֤ȯ���ʽ��ױ�ʶ
                ,MasterNetAgreeFlag                                           --���������Э���ʶ
                ,MasterNetAgreeID                                             --���������Э��ID
                ,SFTType                                                      --֤ȯ���ʽ�������
                ,SecuOwnerTransFlag                                           --֤ȯ����Ȩ�Ƿ�ת��
                ,OTCFlag                                                      --�����������߱�ʶ
                ,ValidNettingFlag                                             --��Ч�������Э���ʶ
                ,ValidNetAgreementID                                          --��Ч�������Э��ID
                ,OTCType                                                      --����������������
                ,DepositRiskPeriod                                            --��֤������ڼ�
                ,MTM                                                          --���óɱ�
                ,MTMCurrency                                                  --���óɱ�����
                ,BuyerOrSeller                                                --������
                ,QualROFlag                                                   --�ϸ�����ʲ���ʶ
                ,ROIssuerPerformFlag                                          --�����ʲ��������Ƿ�����Լ
                ,BuyerInsolvencyFlag                                          --���ñ������Ƿ��Ʋ�
                ,NonpaymentFees                                               --��δ֧������
                ,RetailExpoFlag                                               --���۱�¶��ʶ
                ,RetailClaimType                                              --����ծȨ����
                ,MortgageType                                                 --ס����Ѻ��������
                ,ExpoNumber                                                   --���ձ�¶����
                ,LTV                                                          --�����ֵ��
                ,Aging                                                        --����
                ,NewDefaultDebtFlag                                           --����ΥԼծ���ʶ
                ,PDPoolModelID                                                --PD�ֳ�ģ��ID
                ,LGDPoolModelID                                               --LGD�ֳ�ģ��ID
                ,CCFPoolModelID                                               --CCF�ֳ�ģ��ID
                ,PDPoolID                                                     --����PD��ID
                ,LGDPoolID                                                    --����LGD��ID
                ,CCFPoolID                                                    --����CCF��ID
                ,ABSUAFlag                                                    --�ʲ�֤ȯ�������ʲ���ʶ
                ,ABSPoolID                                                    --֤ȯ���ʲ���ID
                ,GroupID                                                      --������
                ,DefaultDate                                                  --ΥԼʱ��
                ,ABSPROPORTION                                                --�ʲ�֤ȯ������
                ,DEBTORNUMBER                                                 --����˸���
                ,SBJT2
                ,SBJT_VAL2
                ,SBJT3
                ,SBJT_VAL3
                ,SBJT4
                ,SBJT_VAL4
                ,SBJT5
                ,SBJT_VAL5
    )
     SELECT
                TO_DATE(p_data_dt_str,'YYYYMMDD') AS DataDate,  --��������
                p_data_dt_str AS DataNo,--������ˮ��
                T2.Serialno  AS ExposureID,             --���ձ�¶ID
                T2.Serialno  AS DueID,                  --ծ��ID
                'TZ' AS SSysID,     --ԴϵͳID             Ĭ�� Ͷ��(TZ)
                T2.Serialno  AS ContractID,             --��ͬID
                t2.customerid  AS ClientID,    --��������ID                      Ĭ��һ��һ�㹫˾
                t2.mforgid  AS SOrgID, --Դ����ID
                T3.ORGNAME  AS SOrgName,   --Դ��������
                NVL(T3.SORTNO,'1010'),
                decode(substr(T2.mforgid,1,1),'@','01000000',T2.mforgid),
                NVL(T3.Orgname,'����'),
                T2.mforgid   AS AccOrgID,               --�������ID
                T3.ORGNAME    AS AccOrgName,             --�����������
                T5.Industrytype  AS IndustryID,             --������ҵ����
                '' AS IndustryName,           --������ҵ����
                '0401' AS BusinessLine,           --ҵ������                        Ĭ�� ͬҵ(04)
                '' AS AssetType,              --�ʲ�����                        Ĭ�� NULL RWA�������
                ''  AS AssetSubType,           --�ʲ�С��                        Ĭ�� NULL RWA�������
                t2.businesstype  AS BusinessTypeID         --ҵ��Ʒ�ִ���
                ,DECODE(t2.businesstype,'10201080','����͢�����ʣ�','10202091','����͢�����ڣ�','����')  AS BusinessTypeName       --ҵ��Ʒ������
                ,'01'  AS CreditRiskDataType     --���÷�����������                Ĭ�� һ�������(01)
                ,'01' AS AssetTypeOfHaircuts    --�ۿ�ϵ����Ӧ�ʲ����            Ĭ�� �ֽ��ֽ�ȼ���(01)
                ,'07' AS BusinessTypeSTD        --Ȩ�ط�ҵ������                 Ĭ�� һ���ʲ�(07)
                ,'' AS ExpoClassSTD           --Ȩ�ط���¶����                 Ĭ�� 011216-��������100%����Ȩ�ص��ʲ�
                ,''  AS ExpoSubClassSTD        --Ȩ�ط���¶С��                 Ĭ�� 011216-��������100%����Ȩ�ص��ʲ�
                ,''  AS ExpoClassIRB           --��������¶����                 Ĭ�� NULL RWA������
                ,''   AS ExpoSubClassIRB        --��������¶С��                 Ĭ�� NULL RWA������
                ,'01' AS ExpoBelong             --��¶������ʶ                    Ĭ�ϣ�����(01)
                ,'01' --�˻���� �ʲ����� �� �������Խ����ʲ�(10)�� , ��Ϊ02-�����˻����ʲ����� �� �������Խ����ʲ���  , ��Ϊ01-�����˻�
                ,'02' AS ReguTranType           --��ܽ�������                    Ĭ�� �����ʱ��г�����(02)
                ,'0' AS RepoTranFlag           --�ع����ױ�ʶ                    Ĭ�� ��(0)
                ,1 AS RevaFrequency          --�ع�Ƶ��                        Ĭ��  1
                ,T2.Businesscurrency AS Currency               --����
                ,NVL(T2.balance,0)  AS NormalPrincipal        --�����������                    �����������ɱ�����Ϣ����(initial_cost)�����ʼ�ֵ�䶯/���ʼ�ֵ�䶯����(int_adjust)��Ӧ����Ϣ(mkt_value_change)
                ,0   AS OverdueBalance         --�������                        Ĭ�� 0
                ,0  AS NonAccrualBalance      --��Ӧ�����                     Ĭ�� 0
                ,NVL(T2.balance,0) AS OnSheetBalance         --�������                        �������=�����������+�������+��Ӧ�����
                ,0  AS NormalInterest         --������Ϣ                        Ĭ�� 0
                ,0  AS OnDebitInterest        --����ǷϢ                        Ĭ�� 0
                ,0  AS OffDebitInterest       --����ǷϢ                        Ĭ�� 0
                ,0  AS ExpenseReceivable      --Ӧ�շ���                        Ĭ�� 0
                ,NVL(T2.balance,0) AS AssetBalance           --�ʲ����                        �����ʲ������=�������+Ӧ�շ���+������Ϣ+����ǷϢ
                ,'15010101'  AS AccSubject1            --��Ŀһ                         ����ԭϵͳ���ʲ�������ջ�ƿ�Ŀ��ȷ��
                ,'' AS AccSubject2            --��Ŀ��                         Ĭ�� NULL
                ,'' AS AccSubject3            --��Ŀ��                         Ĭ�� NULL
                ,T2.Putoutdate  AS StartDate  --��ʼ����
                ,T4.MATURITY  AS DueDate   --��������
                ,CASE WHEN (TO_DATE(T4.MATURITY,'YYYYMMDD') - TO_DATE(T2.Putoutdate,'YYYYMMDD')) / 365<0
                      THEN 0
                      ELSE (TO_DATE(T4.MATURITY,'YYYYMMDD') - TO_DATE(T2.Putoutdate,'YYYYMMDD')) / 365
                 END AS OriginalMaturity       --ԭʼ����                        ��λ ��
                ,CASE WHEN (TO_DATE(T4.MATURITY,'YYYYMMDD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365<0
                      THEN 0
                      ELSE (TO_DATE(T4.MATURITY,'YYYYMMDD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365
                 END AS ResidualM      --ʣ������                        ��λ ��
                ,'01'  AS RiskClassify        --���շ���                        Ĭ�� ����(01)
                ,''    AS ExposureStatus         --���ձ�¶״̬                    Ĭ�� NULL
                ,0 AS OverdueDays            --��������                        Ĭ�� 0
                ,0    AS SpecialProvision       --ר��׼����                     Ĭ�� 0 RWA���� ��Ŀ12220400��ֱ����1%��׼����
                ,0  AS GeneralProvision       --һ��׼����                     Ĭ�� 0 RWA����
                ,0  AS EspecialProvision      --�ر�׼����                     Ĭ�� 0 RWA����
                ,0   AS WrittenOffAmount       --�Ѻ������                     Ĭ�� 0
                ,'' AS OffExpoSource          --���Ⱪ¶��Դ                    Ĭ�� NULL
                ,'' AS OffBusinessType        --����ҵ������                    Ĭ�� NULL
                ,''   AS OffBusinessSdvsSTD     --Ȩ�ط�����ҵ������ϸ��         Ĭ�� NULL
                ,''   AS UncondCancelFlag       --�Ƿ����ʱ����������            Ĭ�� NULL
                ,''   AS CCFLevel               --����ת��ϵ������                Ĭ�� NULL
                ,NULL  AS CCFAIRB                --�߼�������ת��ϵ��             Ĭ�� NULL
                ,'01'            --ծȨ����                        ծȯ����2���μ�ծȯ(20)����ծȨ���𣽴μ�ծȨ(02)������Ϊ�߼�ծȨ(01)
                ,'0'  AS BondFlag               --�Ƿ�Ϊծȯ                     Ĭ�� ��(0)
                ,'02'   AS BondIssueIntent        --ծȯ����Ŀ��                    Ĭ�� ����(02)
                ,'0'  AS NSURealPropertyFlag    --�Ƿ�����ò�����                Ĭ�� ��(0)
                ,'' AS RepAssetTermType       --��ծ�ʲ���������                Ĭ�� NULL
                ,'0' AS DependOnFPOBFlag       --�Ƿ�����������δ��ӯ��         Ĭ�� ��(0)
                ,'' AS IRating                --�ڲ�����
                ,NULL  AS PD                     --ΥԼ����
                ,''  AS LGDLevel               --ΥԼ��ʧ�ʼ���                 Ĭ�� NULL
                ,NULL  AS LGDAIRB                --�߼���ΥԼ��ʧ��                Ĭ�� NULL
                ,NULL  AS MAIRB                  --�߼�����Ч����                 Ĭ�� NULL
                ,NULL  AS EADAIRB                --�߼���ΥԼ���ձ�¶             Ĭ�� NULL
                ,'0' AS DefaultFlag            --ΥԼ��ʶ
                ,0.45  AS BEEL                   --��ΥԼ��¶Ԥ����ʧ����         ծȨ���𣽡��߼�ծȨ����BEEL �� 45%��ծȨ���𣽡��μ�ծȨ����BEEL �� 75%
                ,0.45 AS DefaultLGD             --��ΥԼ��¶ΥԼ��ʧ��            Ĭ�� NULL
                ,'0'  AS EquityExpoFlag         --��Ȩ��¶��ʶ                    Ĭ�� ��(0)
                ,''  AS EquityInvestType       --��ȨͶ�ʶ�������                Ĭ�� NULL
                ,''   AS EquityInvestCause      --��ȨͶ���γ�ԭ��                Ĭ�� NULL
                ,'0' AS SLFlag                 --רҵ�����ʶ                    Ĭ�� ��(0)
                ,''  AS SLType                 --רҵ��������                    Ĭ�� NULL
                ,''  AS PFPhase                --��Ŀ���ʽ׶�                    Ĭ�� NULL
                ,'01'  AS ReguRating             --�������                        Ĭ�� ��(01)
                ,'' AS CBRCMPRatingFlag       --������϶������Ƿ��Ϊ����       Ĭ�� NULL
                ,'' AS LargeFlucFlag          --�Ƿ񲨶��Խϴ�                 Ĭ�� NULL
                ,'0' AS LiquExpoFlag           --�Ƿ���������з��ձ�¶         Ĭ�� ��(0)
                ,'1'  AS PaymentDealFlag        --�Ƿ����Ը�ģʽ                Ĭ�� ��(1)
                ,NULL AS DelayTradingDays       --�ӳٽ�������                    Ĭ�� NULL
                ,'0' AS SecuritiesFlag         --�м�֤ȯ��ʶ                    Ĭ�� ��(0)
                ,'' AS SecuIssuerID           --֤ȯ������ID                   Ĭ�� NULL
                ,'' AS RatingDurationType     --������������                    Ĭ�� NULL
                ,''  AS SecuIssueRating        --֤ȯ���еȼ�                    Ĭ�� NULL
                ,NULL AS SecuResidualM          --֤ȯʣ������                    Ĭ�� NULL
                ,1 AS SecuRevaFrequency      --֤ȯ�ع�Ƶ��                    Ĭ�� 1
                ,'0' AS CCPTranFlag            --�Ƿ����뽻�׶�����ؽ���        Ĭ�� ��(0)
                ,'' AS CCPID                  --���뽻�׶���ID                  Ĭ�� NULL
                ,'' AS QualCCPFlag            --�Ƿ�ϸ����뽻�׶���            Ĭ�� NULL
                ,'' AS BankRole               --���н�ɫ                        Ĭ�� NULL
                ,'' AS ClearingMethod         --���㷽ʽ                        Ĭ�� NULL
                ,'' AS BankAssetFlag          --�Ƿ������ύ�ʲ�                Ĭ�� NULL
                ,'' AS MatchConditions        --�����������                    Ĭ�� NULL
                ,'0' AS SFTFlag                --֤ȯ���ʽ��ױ�ʶ                Ĭ�� ��(0)
                ,'0' AS MasterNetAgreeFlag     --���������Э���ʶ             Ĭ�� ��(0)
                ,'' AS MasterNetAgreeID       --���������Э��ID               Ĭ�� NULL
                ,'' AS SFTType                --֤ȯ���ʽ�������                Ĭ�� NULL
                ,'' AS SecuOwnerTransFlag     --֤ȯ����Ȩ�Ƿ�ת��             Ĭ�� NULL
                ,'0' AS OTCFlag                --�����������߱�ʶ                Ĭ�� ��(0)
                ,'' AS ValidNettingFlag       --��Ч�������Э���ʶ            Ĭ�� NULL
                ,'' AS ValidNetAgreementID    --��Ч�������Э��ID              Ĭ�� NULL
                ,'' AS OTCType                --����������������                Ĭ�� NULL
                ,'' AS DepositRiskPeriod      --��֤������ڼ�                 Ĭ�� NULL
                ,'' AS MTM                    --���óɱ�                        Ĭ�� NULL
                ,'' AS MTMCurrency            --���óɱ�����                    Ĭ�� NULL
                ,'' AS BuyerOrSeller          --������                        Ĭ�� NULL
                ,'' AS QualROFlag             --�ϸ�����ʲ���ʶ                Ĭ�� NULL
                ,'' AS ROIssuerPerformFlag    --�����ʲ��������Ƿ�����Լ        Ĭ�� NULL
                ,'' AS BuyerInsolvencyFlag    --���ñ������Ƿ��Ʋ�            Ĭ�� NULL
                ,'' AS NonpaymentFees         --��δ֧������                    Ĭ�� NULL
                ,'0' AS RetailExpoFlag         --���۱�¶��ʶ                    Ĭ�� ��(0)
                ,'' AS RetailClaimType        --����ծȨ����                    Ĭ�� NULL
                ,'' AS MortgageType           --ס����Ѻ��������                Ĭ�� NULL
                ,1 AS ExpoNumber             --���ձ�¶����                    Ĭ�� 1
                ,0.8 AS LTV                    --�����ֵ��                     Ĭ�� 0.8
                ,NULL AS Aging                  --����                            Ĭ�� NULL
                ,'' AS NewDefaultDebtFlag     --����ΥԼծ���ʶ                Ĭ�� NULL
                ,'' AS PDPoolModelID          --PD�ֳ�ģ��ID                    Ĭ�� NULL
                ,'' AS LGDPoolModelID         --LGD�ֳ�ģ��ID                   Ĭ�� NULL
                ,'' AS CCFPoolModelID         --CCF�ֳ�ģ��ID                   Ĭ�� NULL
                ,'' AS PDPoolID               --����PD��ID                     Ĭ�� NULL
                ,'' AS LGDPoolID              --����LGD��ID                    Ĭ�� NULL
                ,'' AS CCFPoolID              --����CCF��ID                    Ĭ�� NULL
                ,'0' AS ABSUAFlag              --�ʲ�֤ȯ�������ʲ���ʶ         Ĭ�� ��(0)
                ,'' AS ABSPoolID              --֤ȯ���ʲ���ID                  Ĭ�� NULL
                ,'' AS GroupID                --������                        Ĭ�� NULL
                ,NULL AS DefaultDate            --ΥԼʱ��
                ,NULL AS ABSPROPORTION          --�ʲ�֤ȯ������
                ,NULL AS DEBTORNUMBER           --����˸���
                ,NULL AS sbjt2
                ,NULL AS sbjt_val2
                ,NULL AS sbjt3
                ,NULL AS sbjt_val3
                ,NULL AS sbjt4
                ,NULL AS sbjt_val4
                ,NULL
                ,NULL
         FROM RWA_DEV.NCM_BUSINESS_HISTORY T1
   INNER JOIN RWA_DEV.NCM_BUSINESS_DUEBILL T2
           ON T1.SERIALNO = T2.SERIALNO
          AND T1.DATANO = T2.DATANO
    LEFT JOIN RWA_DEV.NCM_BUSINESS_CONTRACT T4
           ON T2.RELATIVESERIALNO2 = T4.SERIALNO
          AND T2.DATANO=T4.DATANO
    LEFT JOIN RWA.ORG_INFO T3                                           --RWA������
           ON T2.MFORGID = T3.ORGID
    LEFT JOIN RWA_DEV.NCM_CUSTOMER_INFO T5
           ON T2.CUSTOMERID = T5.CUSTOMERID
          AND T2.DATANO = T5.DATANO
    LEFT JOIN RWA_TEMP_PDLEVEL T6 --�����������ͻ�������ʱ��
           ON T6.CUSTID = T2.CUSTOMERID
        WHERE T1.BUSINESSTYPE IN ('10201080', '10202091')
          AND NVL(T2.SUBJECTNO,'1') <> '12220101'
          AND   T1.DATANO = P_DATA_DT_STR
    ;
    COMMIT;
    
   
   /*
   Ӧ�տ���Ͷ�ʹ��ʼ�ֵ�䶯��Ӧ����Ϣ��̯
   by chengang 
   */
   --ȡӦ�տ���Ͷ��-���ʼ�ֵ�䶯��Ӧ����Ϣ���ܽ��
  SELECT /*FGB.SUBJECT_NO,
          FGB.CURRENCY_CODE AS CURRENCY,*/
         CASE  WHEN CL.ATTRIBUTE8 = 'C-D' THEN
             NVL(SUM(FGB.BALANCE_C * NVL(NPQ.MIDDLEPRICE / 100, 1) -
                 FGB.BALANCE_D * NVL(NPQ.MIDDLEPRICE / 100, 1)),0)
            ELSE
             NVL(SUM(FGB.BALANCE_D * NVL(NPQ.MIDDLEPRICE / 100, 1) -
                 FGB.BALANCE_C * NVL(NPQ.MIDDLEPRICE / 100, 1)),0)
          END AS ACCOUNT_BALANCE
          INTO V_BALANCE --���
     FROM RWA_DEV.FNS_GL_BALANCE FGB
     LEFT JOIN RWA_DEV.TMP_CURRENCY_CHANGE NPQ
       ON NPQ.DATANO = FGB.DATANO
      AND NPQ.CURRENCYCODE = FGB.CURRENCY_CODE
    /* LEFT JOIN RWA_DEV.RWA_ARTICULATION_PARAM RAP
       ON RAP.THIRD_SUBJECT_NO = FGB.SUBJECT_NO*/
     LEFT JOIN RWA.CODE_LIBRARY CL
       ON CL.CODENO = 'NewSubject'
      and cl.itemno = fgb.subject_no
    WHERE FGB.DATANO = P_DATA_DT_STR
      AND FGB.CURRENCY_CODE <> 'RMB'
      AND FGB.SUBJECT_NO IN ('11020201', '11020202') --ȡӦ�տ���Ͷ��-���ʼ�ֵ�䶯��Ӧ����Ϣ�Ľ��
    GROUP BY/* FGB.SUBJECT_NO, CURRENCY_CODE ,*/ CL.ATTRIBUTE8;   
    
    
    --Ӧ�տ�Ͷ��������
    select nvl(sum(t.normalprincipal),1) into S_BALANCE from rwa_tz_exposure t 
    where t.accsubject1='11020101'
    and t.datano=P_DATA_DT_STR;
    
    ---��̯
    update rwa_tz_exposure t set t.normalprincipal=t.normalprincipal+V_BALANCE*(T.normalprincipal/S_BALANCE),
    t.assetbalance=t.assetbalance+V_BALANCE*(T.normalprincipal/S_BALANCE)
    where t.accsubject1='11020101'
    and t.datano=P_DATA_DT_STR;
    commit;
    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_TZ_EXPOSURE',cascade => true);


    /*Ŀ�������ͳ��*/
    --ͳ�Ʋ���ļ�¼��
    SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_TZ_EXPOSURE;
    --Dbms_output.Put_line('RWA_DEV.RWA_TZ_EXPOSURE��ǰ����Ĳ���ϵͳ-Ӧ�տ�Ͷ�����ݼ�¼Ϊ: ' || (v_count1 - v_count) || ' ��');



    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̽��� ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    p_po_rtncode := '1';
	  p_po_rtnmsg  := '�ɹ�' || '-' || v_count;
		--�����쳣
		EXCEPTION
    WHEN OTHERS THEN
				 --Dbms_output.Put_line('������,�������Ϊ:'||sqlcode||';������ϢΪ:'||sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace);
       	 ROLLBACK;
   			 p_po_rtncode := sqlcode;
   			 p_po_rtnmsg  := '���÷��ձ�¶('|| v_pro_name ||')ETLת��ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_TZ_EXPOSURE;
/

