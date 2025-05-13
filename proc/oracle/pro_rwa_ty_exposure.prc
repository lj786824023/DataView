CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_TY_EXPOSURE(
			 											p_data_dt_str	IN	VARCHAR2,		--��������
       											p_po_rtncode	OUT	VARCHAR2,		--���ر��
														p_po_rtnmsg		OUT	VARCHAR2		--��������
)
  /*
    �洢��������:RWA_DEV.PRO_RWA_TY_EXPOSURE
    ʵ�ֹ���:����ϵͳ-ͬҵ�������-���÷��ձ�¶(������Դ����ϵͳ��ҵ�������Ϣȫ������RWAͬҵ������Žӿڱ���ձ�¶����)
    ���ݿھ�:ȫ��
    ����Ƶ��:��ĩ����
    ��  ��  :V1.0.0
    ��д��  :LISHIYONG
    ��дʱ��:2017-04-07
    ��  λ  :�Ϻ���˶��Ϣ�����ɷ����޹�˾
    Դ  ��1 :RWA_DEV.BRD_TERM_MM|�����г�����
    Դ  ��2 :RWA_DEV.BRD_CURR_MM|�����г�����
    Դ  ��3 :RWA.ORG_INFO|������
    Դ  ��5 :RWA.CODE_LIBRARY|RWA�����
    Դ  ��8 :RWA_DEV.IRS_CR_CUSTOMER_RATE|�ͻ�������
    Դ  ��9 :RWA_DEV.NCM_BREAKDEFINEDREMARK|��ʶΥԼ�����
    
    Ŀ���1 :RWA_DEV.RWA_TY_EXPOSURE|ͬҵ��¶��
    ������	:��
    �����¼(�޸���|�޸�ʱ��|�޸�����):
    */
  AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_TY_EXPOSURE';
  --�����쳣����
  v_raise EXCEPTION;
  --���嵱ǰ����ļ�¼��
  v_count1 INTEGER;

  BEGIN
    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*�����ȫ�����ݼ��������Ŀ���*/
    --1.���Ŀ����е�ԭ�м�¼
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_TY_EXPOSURE';           

		--2.���������������ݴ�Դ����뵽Ŀ�����

    --2.1��¶�����ͬҵ-���ڿ���  �޽�������
    INSERT INTO RWA_DEV.RWA_TY_EXPOSURE(
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
    SELECT
      TO_DATE(p_data_dt_str,'YYYYMMDD') , --��������        
      p_data_dt_str , --������ˮ��       
      T1.ACCT_NO  , --���ձ�¶ID        
      T1.ACCT_NO  , --ծ��ID        
      'TY'  , --ԴϵͳID       
      T1.ACCT_NO  , --��ͬID        
   --   NVL(T1.CUST_NO,'XN-ZGSYYH')  , --��������ID     
      case when T1.ACCT_NO in('21611597',
                              '24505651',
                              '24213046',
                              '21248370',
                              '21994327',
                              '24180595',
                              '20421056') then
      'XN-ZGSYYH'
      else NVL(T1.CUST_NO, 'XN-ZGSYYH')
      end, --��������ID     ���⴦��7������Ϊ�������ĵ�����  by chengang 0916
      T1.ORG_CD , --Դ����ID       
      T2.ORGNAME  , --Դ��������       
      --T2.SORTNO , --�������������       
      NVL(T2.SORTNO,'1010'),
      --T1.ORG_CD , --��������ID        
      DECODE(SUBSTR(T1.ORG_CD,1,1),'@','01000000',T1.ORG_CD),
      --T2.ORGNAME  , --������������        
      NVL(T2.ORGNAME,'����'),
      T1.ORG_CD , --�������ID        
      T2.ORGNAME  , --�����������        
      NVL(T3.INDUSTRYTYPE,'J6622') , --������ҵ����        
      T4.ITEMNAME , --������ҵ����        
      CASE WHEN T1.acct_no
      IN ('21964993', '20421075', '21964989', '20524932')
      THEN '0501' --4���������ĵ����߻��ֵ����� by chengang
      WHEN  T1.CCY_CD<>'CNY' THEN '0102'---ͬҵ��ҵĻ��ֵ�ó�� by chengang
      ELSE '0401' END, --ҵ������  Ĭ�ϣ�ͬҵ(0401) 
      ''  , --�ʲ�����        RWAӳ����򣬴�����
      ''  , --�ʲ�С��        RWAӳ����򣬴�����
      CASE WHEN SUBSTR(T1.SBJT_CD, 1, 4) = '1011' THEN '1040101030' --���ͬҵ
           WHEN SUBSTR(T1.SBJT_CD, 1, 4) = '1302' THEN '1040101020' --ͬҵ���
           ELSE 'δ֪' END , --ҵ��Ʒ�ִ���        ������֤������ϵͳ���ʽ�ϵͳ���Ŵ��Ĺ�����ϵ�Ƿ����
      CASE WHEN SUBSTR(T1.SBJT_CD, 1, 4) = '1011' THEN '���ͬҵ' --���ͬҵ
           WHEN SUBSTR(T1.SBJT_CD, 1, 4) = '1302' THEN 'ͬҵ���' --ͬҵ���
           ELSE 'δ֪' END , --ҵ��Ʒ�ִ���        ������֤������ϵͳ���ʽ�ϵͳ���Ŵ��Ĺ�����ϵ�Ƿ����       
      '01'  , --���÷�����������        Ĭ�ϣ�һ�������(01)
      '01'  , --�ۿ�ϵ����Ӧ�ʲ����        Ĭ��: �ֽ��ֽ�ȼ���(01)
      '07'  , --Ȩ�ط�ҵ������       
      ''  , --Ȩ�ط���¶����       RWA����ӳ��
      ''  , --Ȩ�ط���¶С��       RWA����ӳ��
      ''  , --��������¶����       RWA����ӳ��
      ''  , --��������¶С��       RWA����ӳ��
      '01'  , --��¶������ʶ        
      '01'  , --�˻����        Ĭ�ϣ�01-�����˻�
      '02'  , --��ܽ�������        Ĭ�ϣ�
      '0' , --�ع����ױ�ʶ        Ĭ�ϣ���(1)
      1   , --�ع�Ƶ��        Ĭ�ϣ� 1
      T1.CCY_CD , --����        
      T1.CUR_BAL  , --�����������        
      0 , --�������        Ĭ�ϣ�0
      0 , --��Ӧ�����       Ĭ�ϣ�0
      T1.CUR_BAL  , --�������        �������=�����������+�������+��Ӧ�����
      0, --������Ϣ        ������Ϣ=����ǷϢ+����ǷϢ
      0 , --����ǷϢ        Ĭ�ϣ�0
      0 , --����ǷϢ        Ĭ�ϣ�0
      0 , --Ӧ�շ���        Ĭ�ϣ�0
      T1.CUR_BAL  , --�ʲ����        �����ʲ������=�������+Ӧ�շ���+������Ϣ+����ǷϢ, ����֤ȯ���ʽ��ף���Ϊ֤ȯ��ֵ��ع����
      --NVL(T1.CUR_BAL, 0)+NVL(T1.ACCRUALS, 0),
      T1.SBJT_CD  , --��Ŀһ       
      ''  , --��Ŀ��       Ĭ�ϣ� ��
      ''  , --��Ŀ��       Ĭ�ϣ� ��
      T1.START_DT , --��ʼ����        
      '20991231'  , --��������        ����ҵ���޵�����,Ĭ��Ϊ20991231
      '0.1'   , --ԭʼ����              BY WZB 20190910 ����Ĭ������������
      '0.1' , --ʣ������                BY WZB 20190910 ����Ĭ������������
      '01'  , --���շ���        Ĭ�ϣ�����(01)
      '01'  , --���ձ�¶״̬        Ĭ�ϣ�����(01)
      ''  , --��������        Ĭ�ϣ���
      0 , --ר��׼����       RWAӳ����򣬴�����
      0 , --һ��׼����       RWAӳ����򣬴����䣬��I9��ȡ
      0 , --�ر�׼����       RWAӳ����򣬴�����
      0 , --�Ѻ������       
      ''  , --���Ⱪ¶��Դ        
      ''  , --����ҵ������        
      ''  , --Ȩ�ط�����ҵ������ϸ��       
      ''  , --�Ƿ����ʱ����������        
      ''  , --����ת��ϵ������        
      NULL  , --�߼�������ת��ϵ��       
      '01'  , --ծȨ����        
      '0' , --�Ƿ�Ϊծȯ       
      '02'  , --ծȯ����Ŀ��        
      '0' , --�Ƿ�����ò�����        
      ''  , --��ծ�ʲ���������        
      '0' , --�Ƿ�����������δ��ӯ��       
      T5.PDADJLEVEL , --�ڲ�����        ���׶��ֵ��ڲ�����-�ͻ�����
      T5.PD , --ΥԼ����        ���׶��ֵ��ڲ�����-�ͻ�����
      NULL  , --ΥԼ��ʧ�ʼ���       
      NULL  , --�߼���ΥԼ��ʧ��        
      NULL  , --�߼�����Ч����       
      NULL  , --�߼���ΥԼ���ձ�¶       
      CASE WHEN T6.BREAKDATE IS NOT NULL THEN '1' ELSE '0' END  , --ΥԼ��ʶ        ���׶��ֵ��ڲ�����-�ͻ�������������֤�ܷ������ϵͳ��ȡ
      0.45  , --��ΥԼ��¶Ԥ����ʧ����       
      0.45  , --��ΥԼ��¶ΥԼ��ʧ��        �߼�ծȨ0.45���μ�ծȯ0.75
      '0' , --��Ȩ��¶��ʶ        
      ''  , --��ȨͶ�ʶ�������        
      ''  , --��ȨͶ���γ�ԭ��        
      '0' , --רҵ�����ʶ        
      ''  , --רҵ��������        
      ''  , --��Ŀ���ʽ׶�        
      ''  , --�������        
      '0' , --������϶������Ƿ��Ϊ����       
      '0' , --�Ƿ񲨶��Խϴ�       
      '0' , --�Ƿ���������з��ձ�¶       
      '0' , --�Ƿ����Ը�ģʽ        
      NULL  , --�ӳٽ�������        
      '0' , --�м�֤ȯ��ʶ        
      ''  , --֤ȯ������ID       
      ''  , --������������        
      ''  , --֤ȯ���еȼ�        
      NULL  , --֤ȯʣ������        
      1 , --֤ȯ�ع�Ƶ��        
      '0' , --�Ƿ����뽻�׶�����ؽ���        
      ''  , --���뽻�׶���ID        
      ''  , --�Ƿ�ϸ����뽻�׶���        
      ''  , --���н�ɫ        
      ''  , --���㷽ʽ        
      '0' , --�Ƿ������ύ�ʲ�        
      ''  , --�����������        
      '0' , --֤ȯ���ʽ��ױ�ʶ        
      '0' , --���������Э���ʶ       
      ''  , --���������Э��ID       
      ''  , --֤ȯ���ʽ�������        Ĭ�ϣ����ع�(01) ��ع�(02)
      '0' , --֤ȯ����Ȩ�Ƿ�ת��       
      '0' , --�����������߱�ʶ        
      '0' , --��Ч�������Э���ʶ        
      ''  , --��Ч�������Э��ID        
      ''  , --����������������        
      ''  , --��֤������ڼ�       
      ''  , --���óɱ�        
      ''  , --���óɱ�����        
      ''  , --������        
      '0' , --�ϸ�����ʲ���ʶ        
      ''  , --�����ʲ��������Ƿ�����Լ        
      ''  , --���ñ������Ƿ��Ʋ�        
      ''  , --��δ֧������        
      '0' , --���۱�¶��ʶ        
      ''  , --����ծȨ����        
      ''  , --ס����Ѻ��������        
      1 , --���ձ�¶����        
      0.8 , --�����ֵ��       
      NULL  , --����        
      ''  , --����ΥԼծ���ʶ        
      ''  , --PD�ֳ�ģ��ID        
      ''  , --LGD�ֳ�ģ��ID       
      ''  , --CCF�ֳ�ģ��ID       
      ''  , --����PD��ID       
      ''  , --����LGD��ID        
      ''  , --����CCF��ID        
      '0' , --�ʲ�֤ȯ�������ʲ���ʶ       
      ''  , --֤ȯ���ʲ���ID        
      ''  , --������        
      CASE WHEN T5.PDADJLEVEL = '0116' THEN TO_DATE(T5.PDVAVLIDDATE,'YYYYMMDD')
               ELSE NULL
               END , --ΥԼʱ��        ���׶��ֵ��ڲ�����-�ͻ�����
      ''  , --�ʲ�֤ȯ������       
      ''   --����˸���           
  FROM  BRD_CURR_MM T1 --�����г�����     
  LEFT JOIN RWA.ORG_INFO T2
         ON T1.ORG_CD = T2.ORGID
        AND T2.STATUS = '1'     
  LEFT JOIN RWA_DEV.NCM_CUSTOMER_INFO T3
         ON T1.CUST_NO = T3.CUSTOMERID
         AND T1.DATANO=T3.DATANO
        AND T3.CUSTOMERTYPE NOT LIKE '03%' --�Թ��ͻ�   
  LEFT JOIN RWA.CODE_LIBRARY T4
         ON T3.INDUSTRYTYPE = T4.ITEMNO
        AND T4.CODENO = 'IndustryType'   
  /*LEFT JOIN RWA_DEV.IRS_CR_CUSTOMER_RATE T5
         ON T1.CUST_NO = T5.T_IT_CUSTOMER_ID   */
  LEFT JOIN RWA_TEMP_PDLEVEL T5
         ON  T5.CUSTID = T1.CUST_NO
  LEFT JOIN RWA_DEV.NCM_BREAKDEFINEDREMARK T6
         ON T1.CUST_NO = T6.CUSTOMERID     
         AND T1.DATANO=T6.DATANO
  WHERE T1.CUR_BAL <> 0 AND SUBSTR(T1.SBJT_CD, 1, 1) <> 2 --�޳���ծҵ��
        AND SUBSTR(T1.SBJT_CD, 1, 6) IN (
                  '101101', --���ͬҵ-��ž���ͬҵ�����������
                  '101107', --���ͬҵ-��ž���ͬҵ�����������
                  '101108',	--���ͬҵ-��ž���ͬҵ����һ�����
                  '101104'	--���ͬҵ-��ž���ͬҵ����һ�����      
        )
        AND T1.DATANO=p_data_dt_str; 

    COMMIT;

    --2.2��¶�����ͬҵ-���ڿ�����ͬҵ
    INSERT INTO RWA_DEV.RWA_TY_EXPOSURE(
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
    SELECT
      TO_DATE(p_data_dt_str,'YYYYMMDD') , --��������
      p_data_dt_str , --������ˮ��
      T1.ACCT_NO  , --���ձ�¶ID
      T1.ACCT_NO  , --ծ��ID
      'TY'  , --ԴϵͳID
      T1.ACCT_NO  , --��ͬID
      --NVL(T1.CUST_NO,'XN-ZGSYYH')  , --��������ID  TZ_CLIENTĬ����ҵ����
      case when T1.ACCT_NO in('21611597',
                              '24505651',
                              '24213046',
                              '21248370',
                              '21994327',
                              '24180595',
                              '20421056') THEN 'XN-ZGSYYH'
           WHEN T1.Import_Source='T_OPI_DLDT' AND substr(t1.cust_no,1,4)='1000'
           THEN 'OPI'||t1.cust_no
             ELSE t1.cust_no --by ljz
      end,
      T1.ORG_CD , --Դ����ID
      T2.ORGNAME  , --Դ��������
      --T2.SORTNO , --�������������
      NVL(T2.SORTNO,'1010'),
      --T1.ORG_CD , --��������ID
      DECODE(SUBSTR(T1.ORG_CD,1,1),'@','01000000',T1.ORG_CD),
      --T2.ORGNAME  , --������������
      NVL(T2.ORGNAME,'����'),
      T1.ORG_CD , --�������ID
      T2.ORGNAME  , --�����������
      NVL(T3.INDUSTRYTYPE,'J6622') , --������ҵ����
      T4.ITEMNAME , --������ҵ����
      CASE WHEN  T1.CCY_CD<>'CNY' THEN '0102' ---ͬҵ��ҵĻ��ֵ�ó�� bychengang
      ELSE  '0401' END , --ҵ������
      ''  , --�ʲ�����
      ''  , --�ʲ�С��
      CASE WHEN SUBSTR(T1.SBJT_CD, 1, 4) = '1011' THEN '1040101030' --���ͬҵ
           WHEN SUBSTR(T1.SBJT_CD, 1, 4) = '1302' THEN '1040101020' --ͬҵ���
           ELSE 'δ֪' END , --ҵ��Ʒ�ִ���        ������֤������ϵͳ���ʽ�ϵͳ���Ŵ��Ĺ�����ϵ�Ƿ����
      CASE WHEN SUBSTR(T1.SBJT_CD, 1, 4) = '1011' THEN '���ͬҵ' --���ͬҵ
           WHEN SUBSTR(T1.SBJT_CD, 1, 4) = '1302' THEN 'ͬҵ���' --ͬҵ���
           ELSE 'δ֪' END , --ҵ��Ʒ�ִ���        ������֤������ϵͳ���ʽ�ϵͳ���Ŵ��Ĺ�����ϵ�Ƿ����
      '01'  , --���÷�����������
      '01'  , --�ۿ�ϵ����Ӧ�ʲ����
      '07'  , --Ȩ�ط�ҵ������
      ''  , --Ȩ�ط���¶����
      ''  , --Ȩ�ط���¶С��
      ''  , --��������¶����
      ''  , --��������¶С��
      '01'  , --��¶������ʶ
      '01'  , --�˻����
      '02'  , --��ܽ�������
      '0' , --�ع����ױ�ʶ
      1   , --�ع�Ƶ��
      T1.CCY_CD , --����
      T1.CUR_BAL  , --�����������
      0 , --�������
      0 , --��Ӧ�����
      T1.CUR_BAL  , --�������
      0, --������Ϣ
      0 , --����ǷϢ
      0 , --����ǷϢ
      0 , --Ӧ�շ���
      T1.CUR_BAL  , --�ʲ����
      --NVL(T1.CUR_BAL, 0)+NVL(T1.ACCRUALS, 0),
      T1.SBJT_CD  , --��Ŀһ
      ''  , --��Ŀ��
      ''  , --��Ŀ��
      T1.START_DT , --��ʼ����
      T1.MATU_DT  , --��������
      CASE WHEN (TO_DATE(T1.MATU_DT,'YYYY-MM-DD') - TO_DATE(T1.START_DT,'YYYY-MM-DD')) / 365<0
                                  THEN 0
                                  ELSE (TO_DATE(T1.MATU_DT,'YYYY-MM-DD') - TO_DATE(T1.START_DT,'YYYY-MM-DD')) / 365
                            END  , --ԭʼ����
      CASE WHEN (TO_DATE(T1.MATU_DT,'YYYY-MM-DD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365<0
                    THEN 0
                    ELSE (TO_DATE(T1.MATU_DT,'YYYY-MM-DD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365
              END  , --ʣ������
      '01'  , --���շ���
      '01'  , --���ձ�¶״̬
      ''  , --��������
      0 , --ר��׼����
      0 , --һ��׼����
      0 , --�ر�׼����
      0 , --�Ѻ������
      ''  , --���Ⱪ¶��Դ
      ''  , --����ҵ������
      ''  , --Ȩ�ط�����ҵ������ϸ��
      ''  , --�Ƿ����ʱ����������
      ''  , --����ת��ϵ������
      NULL  , --�߼�������ת��ϵ��
      '01'  , --ծȨ����
      '0' , --�Ƿ�Ϊծȯ
      '02'  , --ծȯ����Ŀ��
      '0' , --�Ƿ�����ò�����
      ''  , --��ծ�ʲ���������
      '0' , --�Ƿ�����������δ��ӯ��
      T5.PDADJLEVEL , --�ڲ�����
      T5.PD , --ΥԼ����
      NULL  , --ΥԼ��ʧ�ʼ���
      NULL  , --�߼���ΥԼ��ʧ��
      NULL  , --�߼�����Ч����
      NULL  , --�߼���ΥԼ���ձ�¶
      CASE WHEN T6.BREAKDATE IS NOT NULL THEN '1' ELSE '0' END  , --ΥԼ��ʶ
      0.45  , --��ΥԼ��¶Ԥ����ʧ����
      0.45  , --��ΥԼ��¶ΥԼ��ʧ��
      '0' , --��Ȩ��¶��ʶ
      ''  , --��ȨͶ�ʶ�������
      ''  , --��ȨͶ���γ�ԭ��
      '0' , --רҵ�����ʶ
      ''  , --רҵ��������
      ''  , --��Ŀ���ʽ׶�
      ''  , --�������
      '0' , --������϶������Ƿ��Ϊ����
      '0' , --�Ƿ񲨶��Խϴ�
      '0' , --�Ƿ���������з��ձ�¶
      '0' , --�Ƿ����Ը�ģʽ
      NULL  , --�ӳٽ�������
      '0' , --�м�֤ȯ��ʶ
      ''  , --֤ȯ������ID
      ''  , --������������
      ''  , --֤ȯ���еȼ�
      NULL  , --֤ȯʣ������
      1 , --֤ȯ�ع�Ƶ��
      '0' , --�Ƿ����뽻�׶�����ؽ���
      ''  , --���뽻�׶���ID
      ''  , --�Ƿ�ϸ����뽻�׶���
      ''  , --���н�ɫ
      ''  , --���㷽ʽ
      '0' , --�Ƿ������ύ�ʲ�
      ''  , --�����������
      '0' , --֤ȯ���ʽ��ױ�ʶ
      '0' , --���������Э���ʶ
      ''  , --���������Э��ID
      ''  , --֤ȯ���ʽ�������
      '0' , --֤ȯ����Ȩ�Ƿ�ת��
      '0' , --�����������߱�ʶ
      '0' , --��Ч�������Э���ʶ
      ''  , --��Ч�������Э��ID
      ''  , --����������������
      ''  , --��֤������ڼ�
      ''  , --���óɱ�
      ''  , --���óɱ�����
      ''  , --������
      '0' , --�ϸ�����ʲ���ʶ
      ''  , --�����ʲ��������Ƿ�����Լ
      ''  , --���ñ������Ƿ��Ʋ�
      ''  , --��δ֧������
      '0' , --���۱�¶��ʶ��Ϣ��Ϣ
      ''  , --����ծȨ����
      ''  , --ס����Ѻ��������
      1 , --���ձ�¶����
      0.8 , --�����ֵ��
      NULL  , --����
      ''  , --����ΥԼծ���ʶ
      ''  , --PD�ֳ�ģ��ID
      ''  , --LGD�ֳ�ģ��ID
      ''  , --CCF�ֳ�ģ��ID
      ''  , --����PD��ID
      ''  , --����LGD��ID
      ''  , --����CCF��ID
      '0' , --�ʲ�֤ȯ�������ʲ���ʶ
      ''  , --֤ȯ���ʲ���ID
      ''  , --������
      CASE WHEN T5.PDADJLEVEL = '0116' THEN TO_DATE(T5.PDVAVLIDDATE,'YYYYMMDD')
               ELSE NULL
               END , --ΥԼʱ��
      ''  , --�ʲ�֤ȯ������
      ''    --����˸���
  FROM  BRD_TERM_MM T1  --�����г�����
  LEFT JOIN RWA.ORG_INFO T2
         ON T1.ORG_CD = T2.ORGID AND T2.STATUS = '1'
  LEFT JOIN RWA_DEV.NCM_CUSTOMER_INFO T3
         ON T1.CUST_NO = T3.CUSTOMERID
         AND T1.DATANO=T3.DATANO
        AND T3.CUSTOMERTYPE NOT LIKE '03%' --�Թ��ͻ�
  LEFT JOIN RWA.CODE_LIBRARY T4
         ON T3.INDUSTRYTYPE = T4.ITEMNO
        AND T4.CODENO = 'IndustryType'
  /*LEFT JOIN RWA_DEV.IRS_CR_CUSTOMER_RATE T5
         ON T1.CUST_NO = T5.T_IT_CUSTOMER_ID*/
  LEFT JOIN RWA_TEMP_PDLEVEL T5
         ON T5.CUSTID = T1.CUST_NO
  LEFT JOIN RWA_DEV.NCM_BREAKDEFINEDREMARK T6
         ON T1.CUST_NO = T6.CUSTOMERID
         AND T1.DATANO=T6.DATANO
      WHERE T1.CUR_BAL <> 0
        AND SUBSTR(T1.SBJT_CD, 1, 1) <> 2 --�޳���ծҵ��
        AND SUBSTR(T1.SBJT_CD, 1, 6) IN (
            '101102', --���ͬҵ-��ž���ͬҵ����һ�����
            '101103',	--���ͬҵ-��ž���ͬҵ����һ�����
            '101105',	--���ͬҵ-��ž���ͬҵ�����������
            '101106',	--���ͬҵ-��ž���ͬҵ�����������
            '130201',	--����ʽ�-��ž���ͬҵ����
            '130202'	--����ʽ�-��ž���ͬҵ����
       ) AND T1.DATANO=p_data_dt_str
       ;
    
    COMMIT;
    
    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_TY_EXPOSURE',cascade => true);

    /*Ŀ�������ͳ��*/
    --ͳ�Ʋ���ļ�¼
    SELECT COUNT(1) INTO v_count1 FROM RWA_DEV.RWA_TY_EXPOSURE;
    --Dbms_output.Put_line('RWA_DEV.RWA_TY_EXPOSURE��ǰ����ĺ���ϵͳ-���ͬҵ���ݼ�¼Ϊ: ' || (v_count2-v_count1) || '��');
    
    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̽��� ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

		p_po_rtncode := '1';
	  p_po_rtnmsg  := '�ɹ�' || '-' || v_count1;
		--�����쳣
		EXCEPTION
    WHEN OTHERS THEN
				 --Dbms_output.Put_line('������,�������Ϊ:'||sqlcode||';������ϢΪ:'||sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace);
       	 ROLLBACK;
   			 p_po_rtncode := sqlcode;
   			 p_po_rtnmsg  := '���÷��ձ�¶('|| v_pro_name ||')ETLת��ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_TY_EXPOSURE;
/

