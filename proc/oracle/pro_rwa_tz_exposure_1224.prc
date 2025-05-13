CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_TZ_EXPOSURE_1224(
			 											p_data_dt_str	IN	VARCHAR2,		--�������� yyyyMMdd
       											p_po_rtncode	OUT	VARCHAR2,		--���ر�� 1 �ɹ�,0 ʧ��
														p_po_rtnmsg		OUT	VARCHAR2		--��������
				)
  /*
    �洢��������:RWA_DEV.PRO_RWA_TZ_EXPOSURE_1224
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
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_TZ_EXPOSURE_1224';
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
    --2.1 ����ϵͳ-���ծȯͶ�� BY LJZ 21/1/11
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
    SELECT P_DATA_DT_STR, --��������
       P_DATA_DT_STR, --������ˮ��
       T1.SECID, --���ձ�¶ID
       T1.SECID, --ծ��ID
       'tz', --ԴϵͳID
       T1.SECID, --��ͬID
       T3.CNO, --��������ID
       '6001', --Դ����ID
       '�������йɷ����޹�˾����ҵ��', --Դ��������
       '1290', --�������������
       '6001', --��������ID
       '�������йɷ����޹�˾����ҵ��', --������������
       '6001', --�������ID
       '�������йɷ����޹�˾����ҵ��', --�����������
       'J66', --������ҵ����
       '���ҽ��ڷ���', --������ҵ����
       '04', --ҵ������
       NULL, --�ʲ�����
       NULL, --�ʲ�С��
       '1040202010', --ҵ��Ʒ�ִ���
       '���ծȯͶ��', --ҵ��Ʒ������
       '01', --���÷�����������
       '01', --�ۿ�ϵ����Ӧ�ʲ����
       '07', --Ȩ�ط�ҵ������
       NULL, --Ȩ�ط���¶����
       NULL, --Ȩ�ط���¶С��
       NULL, --��������¶����
       NULL, --��������¶С��
       '01', --��¶������ʶ
       NULL, --�˻����
       '02', --��ܽ�������
       '0', --�ع����ױ�ʶ
       1, --�ع�Ƶ��
       T1.CCY, --����
       -T1.PRINAMT, --�����������
       0, --�������
       0, --��Ӧ�����
       -T1.PRINAMT, --�������
       0, --������Ϣ
       0, --����ǷϢ
       0, --����ǷϢ
       0, --Ӧ�շ���
       -T1.PRINAMT, --�ʲ����
       T2.INTGLNO, --��Ŀһ
       NULL, --��Ŀ��
       NULL, --��Ŀ��
       T4.VDATE, --��ʼ����
       T4.MDATE, --��������
       T4.MDATE - T4.VDATE, --ԭʼ����
       T4.MDATE - P_DATA_DT_STR, --ʣ������
       '01', --���շ���
       '01', --���ձ�¶״̬
       0, --��������
       0, --ר��׼����
       0, --һ��׼����
       0, --�ر�׼����
       0, --�Ѻ������
       NULL, --���Ⱪ¶��Դ
       NULL, --����ҵ������
       NULL, --Ȩ�ط�����ҵ������ϸ��
       NULL, --�Ƿ����ʱ����������
       NULL, --����ת��ϵ������
       NULL, --�߼�������ת��ϵ��
       NULL, --ծȨ����
       '1', --�Ƿ�Ϊծȯ
       '02', --ծȯ����Ŀ��
       '0', --�Ƿ�����ò�����
       NULL, --��ծ�ʲ���������
       '0', --�Ƿ�����������δ��ӯ��
       NULL, --�ڲ�����
       NULL, --ΥԼ����
       NULL, --ΥԼ��ʧ�ʼ���
       NULL, --�߼���ΥԼ��ʧ��
       NULL, --�߼�����Ч����
       NULL, --�߼���ΥԼ���ձ�¶
       '0', --ΥԼ��ʶ
       0.45, --��ΥԼ��¶Ԥ����ʧ����
       0.45, --��ΥԼ��¶ΥԼ��ʧ��
       '0', --��Ȩ��¶��ʶ
       NULL, --��ȨͶ�ʶ�������
       NULL, --��ȨͶ���γ�ԭ��
       '0', --רҵ�����ʶ
       NULL, --רҵ��������
       NULL, --��Ŀ���ʽ׶�
       '01', --�������
       NULL, --������϶������Ƿ��Ϊ����
       NULL, --�Ƿ񲨶��Խϴ�
       '0', --�Ƿ���������з��ձ�¶
       '1', --�Ƿ����Ը�ģʽ
       NULL, --�ӳٽ�������
       '1', --�м�֤ȯ��ʶ
       T6.BONDPUBLISHID, --֤ȯ������ID
       T6.TIMELIMIT, --������������
       T6.BONDRATING, --֤ȯ���еȼ�
       (T4.MDATE - P_DATA_DT_STR) / 12, --֤ȯʣ������
       1, --֤ȯ�ع�Ƶ��
       '0', --�Ƿ����뽻�׶�����ؽ���
       NULL, --���뽻�׶���ID
       NULL, --�Ƿ�ϸ����뽻�׶���
       NULL, --���н�ɫ
       NULL, --���㷽ʽ
       NULL, --�Ƿ������ύ�ʲ�
       NULL, --�����������
       '0', --֤ȯ���ʽ��ױ�ʶ
       '0', --���������Э���ʶ
       NULL, --���������Э��ID
       NULL, --֤ȯ���ʽ�������
       NULL, --֤ȯ����Ȩ�Ƿ�ת��
       '0', --�����������߱�ʶ
       NULL, --��Ч�������Э���ʶ
       NULL, --��Ч�������Э��ID
       NULL, --����������������
       NULL, --��֤������ڼ�
       NULL, --���óɱ�
       NULL, --���óɱ�����
       NULL, --������
       NULL, --�ϸ�����ʲ���ʶ
       NULL, --�����ʲ��������Ƿ�����Լ
       NULL, --���ñ������Ƿ��Ʋ�
       NULL, --��δ֧������
       '0', --���۱�¶��ʶ
       NULL, --����ծȨ����
       NULL, --ס����Ѻ��������
       1, --���ձ�¶����
       0.8, --�����ֵ��
       NULL, --����
       NULL, --����ΥԼծ���ʶ
       NULL, --PD�ֳ�ģ��ID
       NULL, --LGD�ֳ�ģ��ID
       NULL, --CCF�ֳ�ģ��ID
       NULL, --����PD��ID
       NULL, --����LGD��ID
       NULL, --����CCF��ID
       '0', --�ʲ�֤ȯ�������ʲ���ʶ
       NULL, --֤ȯ���ʲ���ID
       NULL, --������
       NULL, --ΥԼʱ��
       NULL, --�ʲ�֤ȯ������
       NULL, --����˸���
       T2.INTGLNO, --��Ϣ������Ŀ
       T1.UNAMORTAMT, --��Ϣ�������
       T2.INTGLNO, --��Ϣ��Ŀ
       T1.TDYINTINCEXP, --��Ϣ���
       T2.INTGLNO, --���ʼ�ֵ�䶯��Ŀ
       T1.TDYMTM, --���ʼ�ֵ�䶯���
       NULL, --Ӧ����Ϣ��Ŀ
       NULL --Ӧ����Ϣ���
  FROM OPI_TPOS T1
 INNER JOIN OPI_SL_ACUP T2
    ON T1.DATANO = T2.DATANO
   AND T1.SECID || '|' || T1.PORT || '|' || T1.COST || '|' || T1.INVTYPE =
       SUBSTR(T2.DESCR, 1, INSTR(T2.DESCR, '|', 1, 4) - 1)
 INNER JOIN OPI_SPSH T3
    ON T1.DATANO = T3.DATANO
   AND T1.SECID = T3.SECID
   AND T1.PORT = T3.PORT
   AND T1.INVTYPE = T3.INVTYPE
   AND T1.COST = T3.COST
 INNER JOIN OPI_SECM T4
    ON T1.DATANO = T4.DATANO
   AND T1.SECID = T4.SECID
  LEFT JOIN NCM_BUSINESS_DUEBILL T5
    ON T1.DATANO = T5.DATANO
   AND 'CW_IMPORTDATA' || T1.SECID = T5.THIRDPARTYACCOUNTS
   AND T1.DATANO = T5.DATANO
  LEFT JOIN NCM_BOND_INFO T6
    ON T5.DATANO = T6.DATANO
   AND T5.RELATIVESERIALNO2 = T6.OBJECTNO
   AND T6.OBJECTTYPE = 'BusinessContract'
 WHERE T1.DATANO = P_DATA_DT_STR;

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
    
    --2.4.4 ����ϵͳ-Ӧ�տ�Ͷ��-���һ���-11010301 BY LJZ 21/1/11
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
    SELECT P_DATA_DT_STR, --��������
       P_DATA_DT_STR, --������ˮ��
       T1.FUNDMARKETCODE, --���ձ�¶ID
       T1.FUNDMARKETCODE, --ծ��ID
       'TZ', --ԴϵͳID
       T1.FUNDMARKETCODE, --��ͬID
       NULL, --��������ID
       NVL(T2.ORGID, '6001'), --Դ����ID
       NVL(T3.ORGNAME, '�������йɷ����޹�˾����ҵ��'), --Դ��������
       NVL(T3.SORTNO, '1290'), --�������������
       NVL(T2.ORGID, '6001'), --��������ID
       NVL(T3.ORGNAME, '�������йɷ����޹�˾����ҵ��'), --������������
       NVL(T2.ORGID, '6001'), --�������ID
       NVL(T3.ORGNAME, '�������йɷ����޹�˾����ҵ��'), --�����������
       'J66', --������ҵ����
       '���ҽ��ڷ���', --������ҵ����
       '0401', --ҵ������
       NULL, --�ʲ�����
       NULL, --�ʲ�С��
       '1040105060', --ҵ��Ʒ�ִ���
       'Ӧ�տ�����Ͷ��_���һ���', --ҵ��Ʒ������
       '01', --���÷�����������
       '01', --�ۿ�ϵ����Ӧ�ʲ����
       '07', --Ȩ�ط�ҵ������
       NULL, --Ȩ�ط���¶����
       NULL, --Ȩ�ط���¶С��
       NULL, --��������¶����
       NULL, --��������¶С��
       '01', --��¶������ʶ
       NULL, --�˻����
       '02', --��ܽ�������
       '0', --�ع����ױ�ʶ
       '1', --�ع�Ƶ��
       T1.CURRENCY, --����
       T4.REMAMT, --�����������
       0, --�������
       0, --��Ӧ�����
       T4.REMAMT, --�������
       0, --������Ϣ
       0, --����ǷϢ
       0, --����ǷϢ
       0, --Ӧ�շ���
       T4.REMAMT, --�ʲ����
       T2.SUBJECTFLOW, --��Ŀһ
       NULL, --��Ŀ��
       NULL, --��Ŀ��
       T1.DEFDATE, --��ʼ����
       NULL, --��������
       NULL, --ԭʼ����
       NULL, --ʣ������
       '01', --���շ���
       '01', --���ձ�¶״̬
       0, --��������
       0, --ר��׼����
       0, --һ��׼����
       0, --�ر�׼����
       0, --�Ѻ������
       NULL, --���Ⱪ¶��Դ
       NULL, --����ҵ������
       NULL, --Ȩ�ط�����ҵ������ϸ��
       NULL, --�Ƿ����ʱ����������
       NULL, --����ת��ϵ������
       NULL, --�߼�������ת��ϵ��
       NULL, --ծȨ����
       '1', --�Ƿ�Ϊծȯ
       '02', --ծȯ����Ŀ��
       '0', --�Ƿ�����ò�����
       NULL, --��ծ�ʲ���������
       '0', --�Ƿ�����������δ��ӯ��
       NULL, --�ڲ�����
       NULL, --ΥԼ����
       NULL, --ΥԼ��ʧ�ʼ���
       NULL, --�߼���ΥԼ��ʧ��
       NULL, --�߼�����Ч����
       NULL, --�߼���ΥԼ���ձ�¶
       '0', --ΥԼ��ʶ
       0.45, --��ΥԼ��¶Ԥ����ʧ����
       0.45, --��ΥԼ��¶ΥԼ��ʧ��
       '0', --��Ȩ��¶��ʶ
       NULL, --��ȨͶ�ʶ�������
       NULL, --��ȨͶ���γ�ԭ��
       '0', --רҵ�����ʶ
       NULL, --רҵ��������
       NULL, --��Ŀ���ʽ׶�
       '01', --�������
       NULL, --������϶������Ƿ��Ϊ����
       NULL, --�Ƿ񲨶��Խϴ�
       '0', --�Ƿ���������з��ձ�¶
       '1', --�Ƿ����Ը�ģʽ
       NULL, --�ӳٽ�������
       '1', --�м�֤ȯ��ʶ
       NULL, --֤ȯ������ID
       NULL, --������������
       NULL, --֤ȯ���еȼ�
       NULL, --֤ȯʣ������
       1, --֤ȯ�ع�Ƶ��
       '0', --�Ƿ����뽻�׶�����ؽ���
       NULL, --���뽻�׶���ID
       NULL, --�Ƿ�ϸ����뽻�׶���
       NULL, --���н�ɫ
       NULL, --���㷽ʽ
       NULL, --�Ƿ������ύ�ʲ�
       NULL, --�����������
       '0', --֤ȯ���ʽ��ױ�ʶ
       '0', --���������Э���ʶ
       NULL, --���������Э��ID
       NULL, --֤ȯ���ʽ�������
       NULL, --֤ȯ����Ȩ�Ƿ�ת��
       '0', --�����������߱�ʶ
       NULL, --��Ч�������Э���ʶ
       NULL, --��Ч�������Э��ID
       NULL, --����������������
       NULL, --��֤������ڼ�
       NULL, --���óɱ�
       NULL, --���óɱ�����
       NULL, --������
       NULL, --�ϸ�����ʲ���ʶ
       NULL, --�����ʲ��������Ƿ�����Լ
       NULL, --���ñ������Ƿ��Ʋ�
       NULL, --��δ֧������
       '0', --���۱�¶��ʶ
       NULL, --����ծȨ����
       NULL, --ס����Ѻ��������
       1, --���ձ�¶����
       0.8, --�����ֵ��
       NULL, --����
       NULL, --����ΥԼծ���ʶ
       NULL, --PD�ֳ�ģ��ID
       NULL, --LGD�ֳ�ģ��ID
       NULL, --CCF�ֳ�ģ��ID
       NULL, --����PD��ID
       NULL, --����LGD��ID
       NULL, --����CCF��ID
       '0', --�ʲ�֤ȯ�������ʲ���ʶ
       NULL, --֤ȯ���ʲ���ID
       NULL, --������
       NULL, --ΥԼʱ��
       NULL, --�ʲ�֤ȯ������
       NULL, --����˸���
       NULL, --��Ϣ������Ŀ
       NULL, --��Ϣ�������
       NULL, --��Ϣ��Ŀ
       NULL, --��Ϣ���
       NULL, --���ʼ�ֵ�䶯��Ŀ
       NULL, --���ʼ�ֵ�䶯���
       NULL, --Ӧ����Ϣ��Ŀ
       NULL --Ӧ����Ϣ���
  FROM FDS_FUND_INFO T1
  LEFT JOIN FDS_SYS_BUSI_ACCOUNT T2
    ON T1.DATANO = T2.DATANO
   AND T1.FUNDCODE = T2.BUSINESSID
  LEFT JOIN RWA.ORG_INFO T3
    ON T2.ORGID = T3.ORGID
  LEFT JOIN FDS_SYS_ACCOUNT_HISDALIY T4
    ON T2.DATANO = T4.DATANO
   AND T2.INNERACCOUNT = T4.INNERACCOUNT
   AND T2.ACCOUNT = T4.ACCOUNT
 WHERE T1.DATANO = P_DATA_DT_STR
  AND   T1.effectflag = 'E'  and t4.remamt >0;

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
    
    -- �ʹܡ����С����ʼƻ� 11020101��12220101 BY LJZ 21/1/11
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
    SELECT P_DATA_DT_STR, --��������
       P_DATA_DT_STR, --������ˮ��
       T1.TRADENUM, --���ձ�¶ID
       T1.TRADENUM, --ծ��ID
       'TZ', --ԴϵͳID
       T1.TRADENUM, --��ͬID
       T4.CORPCODE, --��������ID
       T8.CODE, --Դ����ID
       NULL, --Դ��������
       NULL, --�������������
       T8.CODE, --��������ID
       NULL, --������������
       T8.CODE, --�������ID
       NULL, --�����������
       'J6620', --������ҵ����
       NULL, --������ҵ����
       '0401', --ҵ������
       NULL, --�ʲ�����
       NULL, --�ʲ�С��
       '1040105060', --ҵ��Ʒ�ִ���
       CASE
         WHEN T1.ASSETNAME LIKE '%����%' THEN
          'Ӧ�տ�����Ͷ��_�������'
         WHEN T1.ASSETNAME NOT LIKE '%����%' AND T1.ASSETNAME LIKE '%���%' THEN
          'Ӧ�տ�����Ͷ��_�Ǳ������'
         WHEN T1.ASSETNAME LIKE '%��%��%' THEN
          'Ӧ�տ�����Ͷ��_���мƻ�'
         WHEN T1.ASSETNAME LIKE '%����%' THEN
          'Ӧ�տ�����Ͷ��_���ʼƻ�'
         WHEN T1.ASSETNAME LIKE '%��%��%' THEN
          'Ӧ�տ�����Ͷ��_�ʹܼƻ�'
       --  WHEN T1.BOND_TYPE1 ='2008' THEN'ë��ϯ����'
         ELSE
          'Ӧ�տ�����Ͷ��_�ʹ�����'
       END AS BUSINESSTYPENAME, --ҵ��Ʒ������
       '01', --���÷�����������
       '01', --�ۿ�ϵ����Ӧ�ʲ����
       '07', --Ȩ�ط�ҵ������
       NULL, --Ȩ�ط���¶����
       NULL, --Ȩ�ط���¶С��
       NULL, --��������¶����
       NULL, --��������¶С��
       '01', --��¶������ʶ
       DECODE(T1.ZCFL, '10', '02', '01'), --�˻����
       '02', --��ܽ�������
       '0', --�ع����ױ�ʶ
       1, --�ع�Ƶ��
       T1.PK_CURRTYPE, --����
       T2.AMT, --�����������
       0, --�������
       0, --��Ӧ�����
       T2.AMT, --�������
       0, --������Ϣ
       0, --����ǷϢ
       0, --����ǷϢ
       0, --Ӧ�շ���
       T2.AMT, --�ʲ����
       T2.SUBJCODE, --��Ŀһ
       NULL, --��Ŀ��
       NULL, --��Ŀ��
       T1.STARTDATE, --��ʼ����
       T1.ENDDATE, --��������
       (T1.ENDDATE - T1.STARTDATE) / 12, --ԭʼ����
       (T1.ENDDATE - P_DATA_DT_STR) / 12, --ʣ������
       '01', --���շ���
       NULL, --���ձ�¶״̬
       0, --��������
       0, --ר��׼����
       0, --һ��׼����
       0, --�ر�׼����
       0, --�Ѻ������
       NULL, --���Ⱪ¶��Դ
       NULL, --����ҵ������
       NULL, --Ȩ�ط�����ҵ������ϸ��
       NULL, --�Ƿ����ʱ����������
       NULL, --����ת��ϵ������
       NULL, --�߼�������ת��ϵ��
       T1.PK_ASSETDOC, --ծȨ����
       '0', --�Ƿ�Ϊծȯ
       '02', --ծȯ����Ŀ��
       '0', --�Ƿ�����ò�����
       NULL, --��ծ�ʲ���������
       '0', --�Ƿ�����������δ��ӯ��
       NULL, --�ڲ�����
       NULL, --ΥԼ����
       NULL, --ΥԼ��ʧ�ʼ���
       NULL, --�߼���ΥԼ��ʧ��
       NULL, --�߼�����Ч����
       NULL, --�߼���ΥԼ���ձ�¶
       NULL, --ΥԼ��ʶ
       NULL, --��ΥԼ��¶Ԥ����ʧ����
       NULL, --��ΥԼ��¶ΥԼ��ʧ��
       '0', --��Ȩ��¶��ʶ
       NULL, --��ȨͶ�ʶ�������
       NULL, --��ȨͶ���γ�ԭ��
       '0', --רҵ�����ʶ
       NULL, --רҵ��������
       NULL, --��Ŀ���ʽ׶�
       '01', --�������
       NULL, --������϶������Ƿ��Ϊ����
       NULL, --�Ƿ񲨶��Խϴ�
       '0', --�Ƿ���������з��ձ�¶
       '1', --�Ƿ����Ը�ģʽ
       NULL, --�ӳٽ�������
       '0', --�м�֤ȯ��ʶ
       NULL, --֤ȯ������ID
       NULL, --������������
       NULL, --֤ȯ���еȼ�
       NULL, --֤ȯʣ������
       1, --֤ȯ�ع�Ƶ��
       '0', --�Ƿ����뽻�׶�����ؽ���
       NULL, --���뽻�׶���ID
       NULL, --�Ƿ�ϸ����뽻�׶���
       NULL, --���н�ɫ
       NULL, --���㷽ʽ
       NULL, --�Ƿ������ύ�ʲ�
       NULL, --�����������
       '0', --֤ȯ���ʽ��ױ�ʶ
       '0', --���������Э���ʶ
       NULL, --���������Э��ID
       NULL, --֤ȯ���ʽ�������
       NULL, --֤ȯ����Ȩ�Ƿ�ת��
       '0', --�����������߱�ʶ
       NULL, --��Ч�������Э���ʶ
       NULL, --��Ч�������Э��ID
       NULL, --����������������
       NULL, --��֤������ڼ�
       NULL, --���óɱ�
       NULL, --���óɱ�����
       NULL, --������
       NULL, --�ϸ�����ʲ���ʶ
       NULL, --�����ʲ��������Ƿ�����Լ
       NULL, --���ñ������Ƿ��Ʋ�
       NULL, --��δ֧������
       '0', --���۱�¶��ʶ
       NULL, --����ծȨ����
       NULL, --ס����Ѻ��������
       1, --���ձ�¶����
       0.8, --�����ֵ��
       NULL, --����
       NULL, --����ΥԼծ���ʶ
       NULL, --PD�ֳ�ģ��ID
       NULL, --LGD�ֳ�ģ��ID
       NULL, --CCF�ֳ�ģ��ID
       NULL, --����PD��ID
       NULL, --����LGD��ID
       NULL, --����CCF��ID
       '0', --�ʲ�֤ȯ�������ʲ���ʶ
       NULL, --֤ȯ���ʲ���ID
       NULL, --������
       NULL, --ΥԼʱ��
       NULL, --�ʲ�֤ȯ������
       NULL, --����˸���
       NULL,
       NULL,
       NULL,
       NULL,
       NULL,
       NULL,
       NULL,
       NULL
  FROM NFIN_NFT_TRADE_XQMR T1 --ծȯ���뽻��ȷ��
  LEFT JOIN NFIN_FAE_CW_ACCT T2 --��Ŀ���ձ�
    ON T1.DATANO = T2.DATANO
   AND T1.TRADENUM = T2.ACCT_NO
   AND T2.AMT <> 0
  LEFT JOIN NFIN_V_FAE_PRODUCT T3 --��Ʒ������
    ON T1.DATANO = T3.DATANO
   AND T1.PRODUCT = T3.PK_PRODUCT
  LEFT JOIN NFIN_V_NFT_TRADECORP T4 --���׶��ֵ�����
    ON T1.DATANO = T4.DATANO
   AND T1.PK_TRADECORP = T4.PK_TRADECORP
  LEFT JOIN NFIN_V_FAE_CHANNEL T5 --����������
    ON T1.DATANO = T5.DATANO
   AND T1.TRENCH = T5.PK_CHANNEL
  LEFT JOIN NFIN_V_FAE_CUSTMANGER T6 --�ͻ���������
    ON T1.DATANO = T6.DATANO
   AND T1.PK_PSNDOC = T6.PK_DEFDOC
  LEFT JOIN NFIN_V_FAE_BUSINESSLINE T7 --���ߵ�����
    ON T1.DATANO = T7.DATANO
   AND T1.LINES = T7.PK_DEFDOC
  LEFT JOIN NFIN_ORG_ORGS T8 --����������
    ON T1.DATANO = T8.DATANO
   AND T1.PK_ORG = T8.PK_ORG
 WHERE T1.DATANO = P_DATA_DT_STR;

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
END PRO_RWA_TZ_EXPOSURE_1224;
/

