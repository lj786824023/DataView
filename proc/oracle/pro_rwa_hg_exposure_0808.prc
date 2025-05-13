CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_HG_EXPOSURE_0808(
			 											p_data_dt_str	IN	VARCHAR2,		--�������� yyyyMMdd
       											p_po_rtncode	OUT	VARCHAR2,		--���ر�� 1 �ɹ�,0 ʧ��
														p_po_rtnmsg		OUT	VARCHAR2		--��������
				)
  /*
    �洢��������:RWA_DEV.PRO_RWA_HG_EXPOSURE_0808
    ʵ�ֹ���:����ϵͳ-�ع�-���÷��ձ�¶(������Դ����ϵͳ��ҵ�������Ϣȫ������RWA�ع��ӿڱ���ձ�¶����)
    ���ݿھ�:ȫ��
    ����Ƶ��:�³�����
    ��  ��  :V1.0.0
    ��д��  :LISHIYONG
    ��дʱ��:2017-04-07
    ��  λ  :�Ϻ���˶��Ϣ�����ɷ����޹�˾
    
    Դ  ��1 :RWA.CODE_LIBRARY|RWA�����
    Դ	 ��2 :RWA.ORG_INFO|������Ϣ��
    Դ  ��3 :RWA_DEV.NCM_CUSTOMER_INFO|�ͻ���Ϣ��
    Դ  ��4 :RWA_DEV.IRS_CR_CUSTOMER_RATE|�����ۿͻ�������Ϣ��
    Դ  ��5 :RWA_DEV.NCM_BREAKDEFINEDREMARK|�Ŵ�ΥԼ��¼��
    Դ  ��6 :RWA_DEV.BRD_BILL_REPO|Ʊ�ݻع�
    Դ  ��7 :RWA_DEV.BRD_REPO|ծȯ�ع�
    
    Ŀ���  :RWA_DEV.RWA_HG_EXPOSURE|����ϵͳ�ع������÷��ձ�¶��
    �����¼(�޸���|�޸�ʱ��|�޸�����):
    pxl 2019/04/15 ȥ���Ϻ���ϵͳ��ر�ȥ��ծȯ��Ʊ�ݲ�¼����
    chengang 2019/04/23 ����RWA_DEV.BRD_BILL_REPO��RWA_DEV.BRD_REPO�Ļ�����
    */
  AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_HG_EXPOSURE_0808';
  --�����쳣����
  v_raise EXCEPTION;
  --���嵱ǰ����ļ�¼��
  v_count INTEGER;
  


  BEGIN
    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*�����ȫ�����ݼ��������Ŀ���*/
    --1.���Ŀ����е�ԭ�м�¼
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_HG_EXPOSURE';
     

    --2.���������������ݴ�Դ����뵽Ŀ�����
    --2.1 ծȯҵ��-���뷵��ծȯ�ع�-��Ѻʽ�����뷵��ծȯ�ع�-���ʽ�������ع�ծȯ�ع�-���ʽ
    INSERT INTO RWA_DEV.RWA_HG_EXPOSURE(
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
        'HG'  , --ԴϵͳID
        'MRFSZQ' || T1.ACCT_NO  , --��ͬID
        T1.CUST_NO  , --��������ID
        T1.ORG_CD , --Դ����ID
        T2.ORGNAME  , --Դ��������
        T2.SORTNO , --�������������
        T1.ORG_CD , --��������ID
        T2.ORGNAME  , --������������
        T1.ORG_CD , --�������ID
        T2.ORGNAME  , --�����������
        T3.INDUSTRYTYPE , --������ҵ����
        T4.ITEMNAME , --������ҵ����
        '0401'  , --ҵ������
        ''  , --�ʲ�����
        ''  , --�ʲ�С��
        CASE
           WHEN T1.REPO_TYPE IN ('4', 'RB') THEN
            '1040102010' --��ع� ծȯ���뷵����Ѻʽ
           WHEN T1.REPO_TYPE IN ('2', 'RS') THEN
            '1040102010' --���ع� ծȯ�����ع���Ѻʽ 
           ELSE
            'δ֪'
         END , --ҵ��Ʒ�ִ��� 
        /*  �������� ����Դϵͳ  T1.CLIENT_PROPRIETARY �Ƿ��������Ѻ�ֶ�ȫ��Ϊ��  ��������������ʽҵ��  ��ҵ���߼�
        
        CASE
           WHEN (T1.REPO_TYPE = '4' OR T1.PROD_TYPE = 'RB') AND T1.CLIENT_PROPRIETARY = 'T' THEN
            '1040102020' --��ع� ծȯ���뷵�����ʽ
           WHEN (T1.REPO_TYPE = '4' OR T1.PROD_TYPE = 'RB') AND T1.CLIENT_PROPRIETARY = 'F' THEN
            '1040102010' --��ع� ծȯ���뷵����Ѻʽ
           WHEN (T1.REPO_TYPE = '2' OR T1.PROD_TYPE = 'RS') AND T1.CLIENT_PROPRIETARY = 'T' THEN
            '1040102020' --���ع� ծȯ�����ع����ʽ
           WHEN (T1.REPO_TYPE = '2' OR T1.PROD_TYPE = 'RS') AND T1.CLIENT_PROPRIETARY = 'F' THEN
            '1040102010' --���ع� ծȯ�����ع���Ѻʽ 
           ELSE
            'δ֪'
         END , --ҵ��Ʒ�ִ���  */
        
        CASE
           WHEN T1.REPO_TYPE IN ('4', 'RB') THEN
            'ծȯ��Ѻʽ�ع�' --��ع� ծȯ���뷵����Ѻʽ
           WHEN T1.REPO_TYPE IN ('2', 'RS') THEN
            'ծȯ��Ѻʽ�ع�' --���ع� ծȯ�����ع���Ѻʽ
           ELSE
            'δ֪'
         END, --ҵ��Ʒ������
         
        /* ͬ��
        CASE
           WHEN (T1.REPO_TYPE = '4' OR T1.PROD_TYPE = 'RB') AND T1.CLIENT_PROPRIETARY = 'T' THEN
            'ծȯ���ʽ�ع�' --��ع� ծȯ���뷵�����ʽ
           WHEN (T1.REPO_TYPE = '4' OR T1.PROD_TYPE = 'RB') AND T1.CLIENT_PROPRIETARY = 'F' THEN
            'ծȯ��Ѻʽ�ع�' --��ع� ծȯ���뷵����Ѻʽ
           WHEN (T1.REPO_TYPE = '2' OR T1.PROD_TYPE = 'RS') AND T1.CLIENT_PROPRIETARY = 'T' THEN
            'ծȯ���ʽ�ع�' --���ع� ծȯ�����ع����ʽ
           WHEN (T1.REPO_TYPE = '2' OR T1.PROD_TYPE = 'RS') AND T1.CLIENT_PROPRIETARY = 'F' THEN
            'ծȯ��Ѻʽ�ع�' --���ع� ծȯ�����ع���Ѻʽ
           ELSE
            'δ֪'
         END , --ҵ��Ʒ������*/
        
        CASE
           WHEN T1.REPO_TYPE IN ('4', 'RB') THEN
            '01' --��ع� ծȯ���뷵����Ѻʽ
           WHEN T1.REPO_TYPE IN ('2', 'RS') THEN
            '01' --���ع� ծȯ�����ع���Ѻʽ
           ELSE
            'δ֪'
         END ,  --���÷�����������      
        /* ͬ��        
        CASE
           WHEN (T1.REPO_TYPE = '4' OR T1.PROD_TYPE = 'RB') AND T1.CLIENT_PROPRIETARY = 'T' THEN
            '06' --��ع� ծȯ���뷵�����ʽ
           WHEN (T1.REPO_TYPE = '4' OR T1.PROD_TYPE = 'RB') AND T1.CLIENT_PROPRIETARY = 'F' THEN
            '01' --��ع� ծȯ���뷵����Ѻʽ
           WHEN (T1.REPO_TYPE = '2' OR T1.PROD_TYPE = 'RS') AND T1.CLIENT_PROPRIETARY = 'T' THEN
            '06' --���ع� ծȯ�����ع����ʽ
           WHEN (T1.REPO_TYPE = '2' OR T1.PROD_TYPE = 'RS') AND T1.CLIENT_PROPRIETARY = 'F' THEN
            '01' --���ع� ծȯ�����ع���Ѻʽ
           ELSE
            'δ֪'
         END , --���÷����������� */
        '01'  , --�ۿ�ϵ����Ӧ�ʲ����
        ''  , --Ȩ�ط�ҵ������
        ''  , --Ȩ�ط���¶����
        ''  , --Ȩ�ط���¶С��
        ''  , --��������¶����
        ''  , --��������¶С��
        CASE WHEN T1.CLIENT_PROPRIETARY = 'T' THEN  '03' ELSE '01' END    , --��¶������ʶ
        '01'  , --�˻����
        '01'  , --��ܽ�������
        '1' , --�ع����ױ�ʶ
        1   , --�ع�Ƶ��
        T1.CASH_CCY_CD  , --����
        NVL(T1.CASH_NOMINAL, 0) , --�����������
        0 , --�������
        0 , --��Ӧ�����
        NVL(T1.CASH_NOMINAL, 0) , --�������
        NVL(T1.ACCRUAL, 0)  , --������Ϣ
        0 , --����ǷϢ
        0 , --����ǷϢ
        0 , --Ӧ�շ���
        NVL(T1.CASH_NOMINAL, 0) , --�ʲ����
        T1.PRINCIPAL_GLNO , --��Ŀһ
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
        T1.ODUE_DT  , --��������
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
        '01'  , --�������
        ''  , --������϶������Ƿ��Ϊ����
        ''  , --�Ƿ񲨶��Խϴ�
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
        CASE WHEN T1.CLIENT_PROPRIETARY = 'T' THEN  '1' ELSE '0' END  , --֤ȯ���ʽ��ױ�ʶ
        '0' , --���������Э���ʶ
        ''  , --���������Э��ID
        CASE WHEN  T1.REPO_TYPE = '4' THEN '02' ELSE '01' END , --֤ȯ���ʽ�������
        CASE WHEN T1.CLIENT_PROPRIETARY = 'T' THEN  '1' ELSE '0'  END , --֤ȯ����Ȩ�Ƿ�ת��
        '0' , --�����������߱�ʶ
        '0' , --��Ч�������Э���ʶ
        ''  , --��Ч�������Э��ID
        ''  , --����������������
        ''  , --��֤������ڼ�
        NULL  , --���óɱ�
        NULL  , --���óɱ�����
        ''  , --������
        '0' , --�ϸ�����ʲ���ʶ
        ''  , --�����ʲ��������Ƿ�����Լ
        ''  , --���ñ������Ƿ��Ʋ�
        NULL  , --��δ֧������
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
                 END , --ΥԼʱ��  ��Ҫ����rwa.CODE_LIBARY
        NULL  , --�ʲ�֤ȯ������
        NULL   --����˸���

    FROM BRD_REPO      T1      
    LEFT JOIN RWA.ORG_INFO      T2  ON T1.ORG_CD = T2.ORGID AND T2.STATUS = '1'     
    LEFT JOIN RWA_DEV.NCM_CUSTOMER_INFO     T3  ON T1.CUST_NO = T3.CUSTOMERID AND T3.CUSTOMERTYPE NOT LIKE '03%' --�Թ��ͻ�   
    LEFT JOIN RWA.CODE_LIBRARY      T4  ON T3.INDUSTRYTYPE = T4.ITEMNO AND T4.CODENO = 'IndustryType'   
    LEFT JOIN RWA_DEV.IRS_CR_CUSTOMER_RATE      T5  ON T1.CUST_NO = T5.T_IT_CUSTOMER_ID   
    LEFT JOIN RWA_DEV.NCM_BREAKDEFINEDREMARK      T6  ON T1.CUST_NO = T6.CUSTOMERID   
    WHERE T1.CASH_NOMINAL<> 0 
      AND T1.PRINCIPAL_GLNO IS NOT NULL
      ;

    COMMIT;

    --2.2 Ʊ��ҵ��-���뷵��Ʊ�ݻع�
    INSERT INTO RWA_DEV.RWA_HG_EXPOSURE(
                DataDate           																						--��������
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
            TO_DATE(p_data_dt_str,'YYYYMMDD')   , --��������
            p_data_dt_str   , --������ˮ��
            T1.ACCT_NO    , --���ձ�¶ID
            T1.ACCT_NO    , --ծ��ID
            'HG'    , --ԴϵͳID
            'MRFSPJ' || T1.ACCT_NO    , --��ͬID
            T1.CUST_NO    , --��������ID
            T1.ORG_CD   , --Դ����ID
            T2.ORGNAME    , --Դ��������
            T2.SORTNO   , --�������������
            T1.ORG_CD   , --��������ID
            T2.ORGNAME    , --������������
            T1.ORG_CD   , --�������ID
            T2.ORGNAME    , --�����������
            T3.INDUSTRYTYPE   , --������ҵ����
            T4.ITEMNAME   , --������ҵ����
            '0401'    , --ҵ������
            ''    , --�ʲ�����
            ''    , --�ʲ�С��
            '10303010'    , --ҵ��Ʒ�ִ���
            '��Ʊת����ҵ��'   , --ҵ��Ʒ������
            '01'    , --���÷�����������
            '01'    , --�ۿ�ϵ����Ӧ�ʲ����
            ''    , --Ȩ�ط�ҵ������
            ''    , --Ȩ�ط���¶����
            ''    , --Ȩ�ط���¶С��
            ''    , --��������¶����
            ''    , --��������¶С��
            '01'    , --��¶������ʶ
            '01'    , --�˻����
            '01'    , --��ܽ�������
            '1'   , --�ع����ױ�ʶ
            1     , --�ع�Ƶ��
            T1.CASH_CCY_CD    , --����
            T1.CASH_NOMINAL   , --�����������
            0   , --�������
            0   , --��Ӧ�����
            T1.CASH_NOMINAL   , --�������
            T1.ACCRUAL    , --������Ϣ
            0   , --����ǷϢ
            0   , --����ǷϢ
            0   , --Ӧ�շ���
            T1.CASH_NOMINAL   , --�ʲ����
            T1.PRINCIPAL_GLNO   , --��Ŀһ
            ''    , --��Ŀ��
            ''    , --��Ŀ��
            T1.START_DT   , --��ʼ����
            T1.MATU_DT    , --��������
            CASE WHEN (TO_DATE(T1.MATU_DT,'YYYY-MM-DD') - TO_DATE(T1.START_DT,'YYYY-MM-DD')) / 365<0
                                  THEN 0
                                  ELSE (TO_DATE(T1.MATU_DT,'YYYY-MM-DD') - TO_DATE(T1.START_DT,'YYYY-MM-DD')) / 365
                            END    , --ԭʼ����
            CASE WHEN (TO_DATE(T1.MATU_DT,'YYYY-MM-DD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365<0
                          THEN 0
                          ELSE (TO_DATE(T1.MATU_DT,'YYYY-MM-DD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365
                    END    , --ʣ������
            '01'    , --���շ���
            '01'    , --���ձ�¶״̬
            T1.ODUE_DT    , --��������
            0   , --ר��׼����
            0   , --һ��׼����
            0   , --�ر�׼����
            0   , --�Ѻ������
            ''   , --���Ⱪ¶��Դ
            ''   , --����ҵ������
            ''   , --Ȩ�ط�����ҵ������ϸ��
            ''   , --�Ƿ����ʱ����������
            ''   , --����ת��ϵ������
            NULL    , --�߼�������ת��ϵ��
            '01'    , --ծȨ����
            '0'   , --�Ƿ�Ϊծȯ
            '02'    , --ծȯ����Ŀ��
            '0'   , --�Ƿ�����ò�����
            ''    , --��ծ�ʲ���������
            '0'   , --�Ƿ�����������δ��ӯ��
            T5.PDADJLEVEL  , --�ڲ�����
            T5.PD   , --ΥԼ����
            NULL    , --ΥԼ��ʧ�ʼ���
            NULL    , --�߼���ΥԼ��ʧ��
            NULL    , --�߼�����Ч����
            NULL    , --�߼���ΥԼ���ձ�¶
            CASE WHEN T6.BREAKDATE IS NOT NULL THEN '1' ELSE '0' END    , --ΥԼ��ʶ
            0.45    , --��ΥԼ��¶Ԥ����ʧ����
            0.45    , --��ΥԼ��¶ΥԼ��ʧ��
            '0'   , --��Ȩ��¶��ʶ
            ''    , --��ȨͶ�ʶ�������
            ''    , --��ȨͶ���γ�ԭ��
            '0'   , --רҵ�����ʶ
            ''    , --רҵ��������
            ''    , --��Ŀ���ʽ׶�
            '01'    , --�������
            ''    , --������϶������Ƿ��Ϊ����
            ''    , --�Ƿ񲨶��Խϴ�
            '0'   , --�Ƿ���������з��ձ�¶
            '0'   , --�Ƿ����Ը�ģʽ
            NULL    , --�ӳٽ�������
            '0'   , --�м�֤ȯ��ʶ
            ''    , --֤ȯ������ID
            ''    , --������������
            ''    , --֤ȯ���еȼ�
            NULL    , --֤ȯʣ������
            1   , --֤ȯ�ع�Ƶ��
            '0'   , --�Ƿ����뽻�׶�����ؽ���
            ''    , --���뽻�׶���ID
            ''    , --�Ƿ�ϸ����뽻�׶���
            ''    , --���н�ɫ
            ''    , --���㷽ʽ
            '0'   , --�Ƿ������ύ�ʲ�
            ''    , --�����������
            '0'   , --֤ȯ���ʽ��ױ�ʶ
            '0'   , --���������Э���ʶ
            ''    , --���������Э��ID
            ''    , --֤ȯ���ʽ�������
            '0'   , --֤ȯ����Ȩ�Ƿ�ת��
            '0'   , --�����������߱�ʶ
            '0'   , --��Ч�������Э���ʶ
            ''    , --��Ч�������Э��ID
            ''    , --����������������
            ''    , --��֤������ڼ�
            ''    , --���óɱ�
            ''    , --���óɱ�����
            ''    , --������
            '0'   , --�ϸ�����ʲ���ʶ
            ''    , --�����ʲ��������Ƿ�����Լ
            ''    , --���ñ������Ƿ��Ʋ�
            ''    , --��δ֧������
            '0'   , --���۱�¶��ʶ
            ''    , --����ծȨ����
            ''    , --ס����Ѻ��������
            1   , --���ձ�¶����
            0.8   , --�����ֵ��
            NULL    , --����
            ''    , --����ΥԼծ���ʶ
            ''    , --PD�ֳ�ģ��ID
            ''    , --LGD�ֳ�ģ��ID
            ''    , --CCF�ֳ�ģ��ID
            ''    , --����PD��ID
            ''    , --����LGD��ID
            ''    , --����CCF��ID
            '0'   , --�ʲ�֤ȯ�������ʲ���ʶ
            ''    , --֤ȯ���ʲ���ID
            ''    , --������
            CASE WHEN T5.PDADJLEVEL = '0116' THEN TO_DATE(T5.PDVAVLIDDATE,'YYYYMMDD')
                     ELSE NULL
                     END   , --ΥԼʱ��
            ''    , --�ʲ�֤ȯ������
            ''      --����˸���
    FROM  RWA_DEV.BRD_BILL_REPO     T1  --Ʊ�ݻع�    
    LEFT JOIN RWA.ORG_INFO      T2  ON T1.ORG_CD = T2.ORGID AND T2.STATUS = '1'           
    LEFT JOIN RWA_DEV.NCM_CUSTOMER_INFO     T3  ON T1.CUST_NO = T3.CUSTOMERID AND T3.CUSTOMERTYPE NOT LIKE '01%' --�Թ��ͻ�         
    LEFT JOIN RWA.CODE_LIBRARY      T4  ON T3.INDUSTRYTYPE = T4.ITEMNO AND T4.CODENO = 'IndustryType'         
    LEFT JOIN RWA_DEV.IRS_CR_CUSTOMER_RATE      T5  ON T1.CUST_NO = T5.T_IT_CUSTOMER_ID         
    LEFT JOIN RWA_DEV.NCM_BREAKDEFINEDREMARK      T6  ON T1.CUST_NO = T6.CUSTOMERID         
     WHERE T1.CASH_NOMINAL <> 0 --������Ч����
       AND T1.PRINCIPAL_GLNO IS NOT NULL --��ALM���з���  ��ĿΪ�յ����ݲ�����Ϊ��ʷ����
       AND SUBSTR(T1.PRINCIPAL_GLNO, 1, 6) = '111102' --���뷵�۽����ʲ�-���뷵��Ʊ��
       AND (T1.CLIENT_PROPRIETARY <> 'N' OR T1.CLIENT_PROPRIETARY IS NULL )--�Ƿ��������Ѻ NΪ���ʽ  ��N��Ѻʽ
		;

    COMMIT;

    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_HG_EXPOSURE',cascade => true);

    /*Ŀ�������ͳ��*/
    --ͳ�Ʋ���ļ�¼��
    SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_HG_EXPOSURE;
    --Dbms_output.Put_line('RWA_DEV.RWA_HG_EXPOSURE��ǰ����ĺ���ϵͳ-���ʽ�ع����ݼ�¼Ϊ: ' || (v_count1 - v_count) || ' ��');



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
END PRO_RWA_HG_EXPOSURE_0808;
/

