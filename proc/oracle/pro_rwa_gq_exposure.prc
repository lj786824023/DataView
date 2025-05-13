CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_GQ_EXPOSURE(
                                                P_DATA_DT_STR  IN   VARCHAR2,    --��������
                                                P_PO_RTNCODE   OUT  VARCHAR2,    --���ر��
                                                P_PO_RTNMSG    OUT  VARCHAR2     --��������
                                               )
  /*
    �洢��������:RWA_DEV.PRO_RWA_GQ_EXPOSURE
    ʵ�ֹ���:��ȨͶ��-���÷��ձ�¶
    ���ݿھ�:ȫ��
    ����Ƶ��:��ĩ
    ��  ��  :V1.0.0
    ��д��  :qpzhong
    ��дʱ��:2016-08-23
    ��  λ  :�Ϻ���˶��Ϣ�����ɷ����޹�˾
    Դ  ��1 :RWA_DEV.rwa_ei_unconsfiinvest        |���ڹ�ȨͶ�ʲ�¼��
    Դ  ��2 :RWA.ORG_INFO             |������
    Ŀ���  :RWA_DEV.rwa_gq_exposure                  |��ȨͶ��-���÷��ձ�¶��
    �����¼(�޸���|�޸�ʱ��|�޸�����):
    */
  AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  V_PRO_NAME VARCHAR2(200) := 'RWA_DEV.PRO_RWA_GQ_EXPOSURE';
  V_COUNT INTEGER;
  --�����쳣����
  V_RAISE EXCEPTION;

  BEGIN
    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    --1.���Ŀ����е�ԭ�м�¼
    /*�����ȫ�����ݼ��������Ŀ���*/
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_GQ_EXPOSURE';

    --2.���������������ݴ�Դ����뵽Ŀ�����
    /*����Ŀ���*/
    INSERT INTO RWA_DEV.RWA_GQ_EXPOSURE (
                 DataDate                                                                           --��������
                ,DataNo                                                                             --������ˮ��
                ,ExposureID                                                                         --���ձ�¶ID
                ,DueID                                                                              --ծ��ID
                ,SSysID                                                                             --ԴϵͳID
                ,ContractID                                                                         --��ͬID
                ,ClientID                                                                           --��������ID
                ,SOrgID                                                                             --Դ����ID
                ,SOrgName                                                                           --Դ��������
                ,OrgSortNo                                                                          --�������������
                ,OrgID                                                                              --��������ID
                ,OrgName                                                                            --������������
                ,AccOrgID                                                                           --�������ID
                ,AccOrgName                                                                         --�����������
                ,IndustryID                                                                         --������ҵ����
                ,IndustryName                                                                       --������ҵ����
                ,BusinessLine                                                                       --ҵ������
                ,AssetType                                                                          --�ʲ�����
                ,AssetSubType                                                                       --�ʲ�С��
                ,BusinessTypeID                                                                     --ҵ��Ʒ�ִ���
                ,BusinessTypeName                                                                   --ҵ��Ʒ������
                ,CreditRiskDataType                                                                 --���÷�����������
                ,AssetTypeOfHaircuts                                                                --�ۿ�ϵ����Ӧ�ʲ����
                ,BusinessTypeSTD                                                                    --Ȩ�ط�ҵ������
                ,ExpoClassSTD                                                                       --Ȩ�ط���¶����
                ,ExpoSubClassSTD                                                                    --Ȩ�ط���¶С��
                ,ExpoClassIRB                                                                       --��������¶����
                ,ExpoSubClassIRB                                                                    --��������¶С��
                ,ExpoBelong                                                                         --��¶������ʶ
                ,BookType                                                                           --�˻����
                ,ReguTranType                                                                       --��ܽ�������
                ,RepoTranFlag                                                                       --�ع����ױ�ʶ
                ,RevaFrequency                                                                      --�ع�Ƶ��
                ,Currency                                                                           --����
                ,NormalPrincipal                                                                    --�����������
                ,OverdueBalance                                                                     --�������
                ,NonAccrualBalance                                                                  --��Ӧ�����
                ,OnSheetBalance                                                                     --�������
                ,NormalInterest                                                                     --������Ϣ
                ,OnDebitInterest                                                                    --����ǷϢ
                ,OffDebitInterest                                                                   --����ǷϢ
                ,ExpenseReceivable                                                                  --Ӧ�շ���
                ,AssetBalance                                                                       --�ʲ����
                ,AccSubject1                                                                        --��Ŀһ
                ,AccSubject2                                                                        --��Ŀ��
                ,AccSubject3                                                                        --��Ŀ��
                ,StartDate                                                                          --��ʼ����
                ,DueDate                                                                            --��������
                ,OriginalMaturity                                                                   --ԭʼ����
                ,ResidualM                                                                          --ʣ������
                ,RiskClassify                                                                       --���շ���
                ,ExposureStatus                                                                     --���ձ�¶״̬
                ,OverdueDays                                                                        --��������
                ,SpecialProvision                                                                   --ר��׼����
                ,GeneralProvision                                                                   --һ��׼����
                ,EspecialProvision                                                                  --�ر�׼����
                ,WrittenOffAmount                                                                   --�Ѻ������
                ,OffExpoSource                                                                      --���Ⱪ¶��Դ
                ,OffBusinessType                                                                    --����ҵ������
                ,OffBusinessSdvsSTD                                                                 --Ȩ�ط�����ҵ������ϸ��
                ,UncondCancelFlag                                                                   --�Ƿ����ʱ����������
                ,CCFLevel                                                                           --����ת��ϵ������
                ,CCFAIRB                                                                            --�߼�������ת��ϵ��
                ,ClaimsLevel                                                                        --ծȨ����
                ,BondFlag                                                                           --�Ƿ�Ϊծȯ
                ,BondIssueIntent                                                                    --ծȯ����Ŀ��
                ,NSURealPropertyFlag                                                                --�Ƿ�����ò�����
                ,RepAssetTermType                                                                   --��ծ�ʲ���������
                ,DependOnFPOBFlag                                                                   --�Ƿ�����������δ��ӯ��
                ,IRating                                                                            --�ڲ�����
                ,PD                                                                                 --ΥԼ����
                ,LGDLevel                                                                           --ΥԼ��ʧ�ʼ���
                ,LGDAIRB                                                                            --�߼���ΥԼ��ʧ��
                ,MAIRB                                                                              --�߼�����Ч����
                ,EADAIRB                                                                            --�߼���ΥԼ���ձ�¶
                ,DefaultFlag                                                                        --ΥԼ��ʶ
                ,BEEL                                                                               --��ΥԼ��¶Ԥ����ʧ����
                ,DefaultLGD                                                                         --��ΥԼ��¶ΥԼ��ʧ��
                ,EquityExpoFlag                                                                     --��Ȩ��¶��ʶ
                ,EquityInvestType                                                                   --��ȨͶ�ʶ�������
                ,EquityInvestCause                                                                  --��ȨͶ���γ�ԭ��
                ,SLFlag                                                                             --רҵ�����ʶ
                ,SLType                                                                             --רҵ��������
                ,PFPhase                                                                            --��Ŀ���ʽ׶�
                ,ReguRating                                                                         --�������
                ,CBRCMPRatingFlag                                                                   --������϶������Ƿ��Ϊ����
                ,LargeFlucFlag                                                                      --�Ƿ񲨶��Խϴ�
                ,LiquExpoFlag                                                                       --�Ƿ���������з��ձ�¶
                ,PaymentDealFlag                                                                    --�Ƿ����Ը�ģʽ
                ,DelayTradingDays                                                                   --�ӳٽ�������
                ,SecuritiesFlag                                                                     --�м�֤ȯ��ʶ
                ,SecuIssuerID                                                                       --֤ȯ������ID
                ,RatingDurationType                                                                 --������������
                ,SecuIssueRating                                                                    --֤ȯ���еȼ�
                ,SecuResidualM                                                                      --֤ȯʣ������
                ,SecuRevaFrequency                                                                  --֤ȯ�ع�Ƶ��
                ,CCPTranFlag                                                                        --�Ƿ����뽻�׶�����ؽ���
                ,CCPID                                                                              --���뽻�׶���ID
                ,QualCCPFlag                                                                        --�Ƿ�ϸ����뽻�׶���
                ,BankRole                                                                           --���н�ɫ
                ,ClearingMethod                                                                     --���㷽ʽ
                ,BankAssetFlag                                                                      --�Ƿ������ύ�ʲ�
                ,MatchConditions                                                                    --�����������
                ,SFTFlag                                                                            --֤ȯ���ʽ��ױ�ʶ
                ,MasterNetAgreeFlag                                                                 --���������Э���ʶ
                ,MasterNetAgreeID                                                                   --���������Э��ID
                ,SFTType                                                                            --֤ȯ���ʽ�������
                ,SecuOwnerTransFlag                                                                 --֤ȯ����Ȩ�Ƿ�ת��
                ,OTCFlag                                                                            --�����������߱�ʶ
                ,ValidNettingFlag                                                                   --��Ч�������Э���ʶ
                ,ValidNetAgreementID                                                                --��Ч�������Э��ID
                ,OTCType                                                                            --����������������
                ,DepositRiskPeriod                                                                  --��֤������ڼ�
                ,MTM                                                                                --���óɱ�
                ,MTMCurrency                                                                        --���óɱ�����
                ,BuyerOrSeller                                                                      --������
                ,QualROFlag                                                                         --�ϸ�����ʲ���ʶ
                ,ROIssuerPerformFlag                                                                --�����ʲ��������Ƿ�����Լ
                ,BuyerInsolvencyFlag                                                                --���ñ������Ƿ��Ʋ�
                ,NonpaymentFees                                                                     --��δ֧������
                ,RetailExpoFlag                                                                     --���۱�¶��ʶ
                ,RetailClaimType                                                                    --����ծȨ����
                ,MortgageType                                                                       --ס����Ѻ��������
                ,ExpoNumber                                                                         --���ձ�¶����
                ,LTV                                                                                --�����ֵ��
                ,Aging                                                                              --����
                ,NewDefaultDebtFlag                                                                 --����ΥԼծ���ʶ
                ,PDPoolModelID                                                                      --PD�ֳ�ģ��ID
                ,LGDPoolModelID                                                                     --LGD�ֳ�ģ��ID
                ,CCFPoolModelID                                                                     --CCF�ֳ�ģ��ID
                ,PDPoolID                                                                           --����PD��ID
                ,LGDPoolID                                                                          --����LGD��ID
                ,CCFPoolID                                                                          --����CCF��ID
                ,ABSUAFlag                                                                          --�ʲ�֤ȯ�������ʲ���ʶ
                ,ABSPoolID                                                                          --֤ȯ���ʲ���ID
                ,GroupID                                                                            --������
                ,DefaultDate                                                                        --ΥԼʱ��
                ,ABSPROPORTION                                                                      --�ʲ�֤ȯ������
                ,DEBTORNUMBER                                                                       --����˸���
    )
    SELECT
                TO_DATE(p_data_dt_str,'YYYYMMDD')                               AS datadate                    --��������
                ,p_data_dt_str                                                  AS datano                      --������ˮ��
                ,'GQ'||t1.serialno                                              AS exposureid                  --���ձ�¶ID
                ,''                                                             AS dueid                       --ծ��ID
                ,'GQ'                                                           AS ssysid                      --ԴϵͳID                  (Ĭ��Ϊ'GC')
                ,'GQ'||t1.serialno                                              AS contractid                  --��ͬID
                ,t1.custid1                                                     AS clientid                    --��������ID
                ,t1.orgid                                                       AS sorgid                      --Դ����ID
                ,t2.orgname                                                     AS sorgname                    --Դ��������
                ,T2.SORTNO                                                      AS ORGSORTNO                   --�������������
                ,t1.orgid                                                       AS orgid                       --��������ID
                ,t2.orgname                                                     AS orgname                     --������������
                ,t1.orgid                                                       AS accorgid                    --�������ID                (Ĭ��ֵ0000-��������,)
                ,t2.orgname                                                     AS accorgname                  --�����������              (Ĭ��ֵ��������,)
                ,'999999'                                                       AS industryid                  --������ҵ����
                ,'δ֪'                                                         AS industryname                --������ҵ����
                ,t1.businessline                                                AS businessline                --����                      (Ĭ��Ϊ'6����',1��˾,2����,3С��ҵ,4�ʽ�,5�ʹ�,6����)
                ,'121'                                                          AS assettype                   --�ʲ�����
                ,'12103'                                                        AS assetsubtype                --�ʲ�С��
                ,'109060'                                                       AS businesstypeid              --ҵ��Ʒ�ִ���              (Ĭ��ֵ'GQ')
                ,'��ȨͶ��'                                                     AS businesstypename            --ҵ��Ʒ������              (Ĭ��ֵ'��ȨͶ��')
                ,'01'                                                           AS creditriskdatatype          --���÷�����������          (Ĭ��Ϊ��Ȩ,01:һ�������;02:һ������;03���׶���)
                ,'02'                                                           AS assettypeofhaircuts         --�ۿ�ϵ����Ӧ�ʲ����      (Ĭ��ֵ��'02'�����ֽ��ֵ�����ٱ��յ���������Ʋ�Ʒ)
                ,'05'                                                           AS businesstypestd             --Ȩ�ط�ҵ������            (Ĭ��Ϊ05��Ȩ)
                ,'0110'                                                         AS expoclassstd                --Ȩ�ط���¶����            (Ĭ��Ϊ0110��Ȩ)
                ,CASE WHEN substr(t1.equityinvesttype,1,2)='02' THEN '011001'
                      WHEN t1.equityinvestcause = '01'  THEN '011002'           --��Ȩ�ʲ��γ�ԭ��(equityinvestcause) 01 �������� 02 ������ԭ�� 03 ����
                      WHEN t1.equityinvestcause = '02' THEN '011003'
                      ELSE '011004' END                                         AS exposubclassstd             --Ȩ�ط���¶С��
                ,''                                                             AS expoclassirb                --��������¶����
                ,''                                                             AS exposubclassirb             --��������¶С��            (�Խ��ڻ����Ĺ�Ȩ  020501  ����ҵ�Ĺ�Ȩ  020502)
                ,'01'                                                           AS expobelong                  --��¶������ʶ              (Ĭ��ֵΪ'����',01����;02һ�����;03���׶���)
                ,'01'                                                           AS booktype                    --�˻����                  (Ĭ��Ϊ�����˻���'01'�����˻�)
                ,'02'                                                           AS regutrantype                --��ܽ�������              (Ĭ��ֵΪ�����ʱ��г�����,'02'�����ʱ��г�����)
                ,'0'                                                            AS repotranflag                --�ع����ױ�ʶ              (Ĭ��ֵ'��',1��0��)
                ,1                                                              AS revafrequency               --�ع�Ƶ��                  (Ĭ��Ϊ'1')
                ,t1.currency                                                    AS currency                    --����                      (Ĭ��ֵ'�����')
                ,CASE WHEN substr(t1.equityinvesttype,1,2)='02' THEN t1.equityinvestamount
                      else ctocinvestamount end                                 AS normalprincipal             --�����������             (��ȨͶ�ʽ��)
                ,0                                                              AS overduebalance              --�������                  (Ĭ��Ϊ��)
                ,0                                                              AS nonaccrualbalance           --��Ӧ�����                (Ĭ��Ϊ��)
                ,CASE WHEN substr(t1.equityinvesttype,1,2)='02' THEN t1.equityinvestamount
                      ELSE ctocinvestamount END                                 AS onsheetbalance              --�������                  (����������� + ������� + ��Ӧ����)
                ,0                                                              AS normalinterest              --������Ϣ                  (Ĭ��Ϊ��)
                ,0                                                              AS ondebitinterest             --����ǷϢ                  (Ĭ��Ϊ0)
                ,0                                                              AS offdebitinterest            --����ǷϢ                  (Ĭ��Ϊ0)
                ,0                                                              AS expensereceivable           --Ӧ�շ���                  (Ĭ��Ϊ0)
                ,CASE WHEN substr(t1.equityinvesttype,1,2)='02' THEN t1.equityinvestamount
                      ELSE ctocinvestamount END                                 AS assetbalance                --�ʲ����                  (�ʲ����=�������+������Ϣ+����ǷϢ+Ӧ�շ���)
                ,t1.Subject                                                     AS accsubject1                 --��Ŀһ                    (��ƿ�Ŀ)
                ,''                                                             AS accsubject2                 --��Ŀ��                    (Ĭ��Ϊ��)
                ,''                                                             AS accsubject3                 --��Ŀ��                    (Ĭ��Ϊ��)
                ,TO_CHAR(TO_DATE(p_data_dt_str,'YYYY-MM-DD'),'YYYY-MM-DD')      AS startdate                   --��ʼ����
                ,TO_CHAR(ADD_MONTHS(TO_DATE(p_data_dt_str,'YYYY-MM-DD'),1),'YYYY-MM-DD')
                                                                                AS duedate                     --��������
                ,(ADD_MONTHS(TO_DATE(p_data_dt_str,'YYYYMMDD'),1) - TO_DATE(p_data_dt_str,'YYYYMMDD'))/365
                                                                                AS originalmaturity            --ԭʼ����
                ,(ADD_MONTHS(TO_DATE(p_data_dt_str,'YYYYMMDD'),1) - TO_DATE(p_data_dt_str,'YYYYMMDD'))/365
                                                                                AS residualm                   --ʣ������
                ,NVL(t1.riskclassify,'01')                                      AS riskclassify                --���շ���                  (Ĭ��Ϊ01-����)
                ,'01'                                                           AS exposurestatus              --���ձ�¶״̬              (Ĭ��Ϊ01-����)
                ,0                                                              AS overduedays                 --��������                  (Ĭ��Ϊ0)
                ,0                                                              AS specialprovision            --ר��׼����                (Ĭ��Ϊ0)
                ,0                                                              AS generalprovision            --һ��׼����                (Ĭ��Ϊ0)
                ,0                                                              AS especialprovision           --�ر�׼����                (Ĭ��Ϊ0)
                ,0                                                              AS writtenoffamount            --�Ѻ������                (Ĭ��Ϊ0)
                ,''                                                             AS offexposource               --���Ⱪ¶��Դ              (Ĭ��Ϊ��)
                ,''                                                             AS offbusinesstype             --����ҵ������              (Ĭ��Ϊ��)
                ,''                                                             AS offbusinesssdvsstd          --Ȩ�ط�����ҵ������ϸ��    (Ĭ��Ϊ��)
                ,'0'                                                            AS uncondcancelflag            --�Ƿ����ʱ����������      (Ĭ��ֵ'��',1��0��)
                ,''                                                             AS ccflevel                    --����ת��ϵ������          (Ĭ��ֵ��)
                ,NULL                                                           AS ccfairb                     --�߼�������ת��ϵ��        (Ĭ��Ϊ��)
                ,'01'                                                           AS claimslevel                 --ծȨ����                  (Ĭ��ֵΪ'�߼�ծȨ',01�߼�ծȨ;02�ͼ�ծȨ)
                ,'0'                                                            AS bondflag                    --�Ƿ�Ϊծȯ                (Ĭ��ֵ'��',1��0��)
                ,''                                                             AS bondissueintent             --ծȯ����Ŀ��
                ,'0'                                                            AS nsurealpropertyflag         --�Ƿ�����ò�����          (Ĭ��ֵ'��',1��0��)
                ,''                                                             AS repassettermtype            --��ծ�ʲ���������          (Ĭ��Ϊ��)
                ,'0'                                                            AS securevafrequency           --�Ƿ�����������δ��ӯ��    (Ĭ��ֵ'��',1��0��)
                ,''                                                             AS irating                     --�ڲ�����                  (Ĭ��Ϊ��)
                ,NULL                                                           AS pd                          --ΥԼ����
                ,''                                                             AS lgdlevel                    --ΥԼ��ʧ�ʼ���            (Ĭ��Ϊ��ֵ)
                ,NULL                                                           AS lgdairb                     --�߼���ΥԼ��ʧ��
                ,NULL                                                           AS mairb                       --�߼�����Ч����            (Ĭ��Ϊ��)
                ,''                                                             AS eadairb                     --�߼���ΥԼ���ձ�¶
                ,'0'                                                            AS defaultflag                 --ΥԼ��ʶ                  (Ĭ��ֵ'��',1��0��)
                ,0.45                                                           AS beel                        --��ΥԼ��¶Ԥ����ʧ����    (Ĭ��Ϊ0.45)
                ,0.45                                                           AS defaultlgd                  --��ΥԼ��¶ΥԼ��ʧ��      (Ĭ��Ϊ0.45)
                ,'1'                                                            AS equityexpoflag              --��Ȩ��¶��ʶ              (Ĭ��ֵ'��',1��0��)
                ,case when substr(t1.EQUITYINVESTTYPE,1,2)= '02' then '01' else '02' end                                                   AS equityinvesttype            --��ȨͶ�ʶ�������
                ,t1.equityinvestcause                                           AS equityinvestcause           --��ȨͶ���γ�ԭ��
                ,'0'                                                            AS slflag                      --רҵ�����ʶ              (Ĭ��ֵ'��',1��0��)
                ,''                                                             AS sltype                      --רҵ��������
                ,''                                                             AS pfphase                     --��Ŀ���ʽ׶�
                ,''                                                             AS regurating                  --�������
                ,''                                                             AS cbrcmpratingflag            --������϶������Ƿ��Ϊ����
                ,'0'                                                            AS largeflucflag               --�Ƿ񲨶��Խϴ�            (Ĭ��ֵ'��',1��0��)
                ,'0'                                                            AS liquexpoflag                --�Ƿ���������з��ձ�¶    (Ĭ��ֵ'��',1��0��)
                ,'0'                                                            AS paymentdealflag             --�Ƿ����Ը�ģʽ          (Ĭ��ֵ'��',1��0��)
                ,0                                                              AS delaytradingdays            --�ӳٽ�������
                ,'0'                                                            AS securitiesflag              --�м�֤ȯ��ʶ              (Ĭ��ֵ'��',1��0��)
                ,''                                                             AS secuissuerid                --֤ȯ������ID
                ,''                                                             AS ratingdurationtype          --������������
                ,''                                                             AS secuissuerating             --֤ȯ���еȼ�
                ,0                                                              AS securesidualm               --֤ȯʣ������
                ,1                                                              AS dependonfpobflag            --֤ȯ�ع�Ƶ��              (Ĭ��Ϊ1)
                ,'0'                                                            AS ccptranflag                 --�Ƿ����뽻�׶�����ؽ���  (Ĭ��ֵ'��',1��0��)
                ,''                                                             AS ccpid                       --���뽻�׶���ID
                ,'0'                                                            AS qualccpflag                 --�Ƿ�ϸ����뽻�׶���      (Ĭ��ֵ'��',1��0��)
                ,''                                                             AS bankrole                    --���н�ɫ
                ,''                                                             AS clearingmethod              --���㷽ʽ
                ,''                                                             AS bankassetflag               --�Ƿ������ύ�ʲ�
                ,''                                                             AS matchconditions             --�����������
                ,'0'                                                            AS sftflag                     --֤ȯ���ʽ��ױ�ʶ          (Ĭ��ֵ'��',1��0��)
                ,'0'                                                            AS masternetagreeflag          --���������Э���ʶ        (Ĭ��ֵ'��',1��0��)
                ,''                                                             AS masternetagreeid            --���������Э��ID
                ,''                                                             AS sfttype                     --֤ȯ���ʽ�������
                ,'0'                                                            AS secuownertransflag          --֤ȯ����Ȩ�Ƿ�ת��        (Ĭ��ֵ'��',1��0��)
                ,'0'                                                            AS otcflag                     --�����������߱�ʶ          (Ĭ��ֵ'��',1��0��)
                ,'0'                                                            AS validnettingflag            --��Ч�������Э���ʶ      (Ĭ��ֵ'��',1��0��)
                ,''                                                             AS validnetagreementid         --��Ч�������Э��ID
                ,''                                                             AS otctype                     --����������������
                ,0                                                              AS depositriskperiod           --��֤������ڼ�
                ,0                                                              AS mtm                         --���óɱ�
                ,''                                                             AS mtmcurrency                 --���óɱ�����
                ,''                                                             AS buyerorseller               --������
                ,'0'                                                            AS qualroflag                  --�ϸ�����ʲ���ʶ          (Ĭ��ֵ'��',1��0��)
                ,'0'                                                            AS roissuerperformflag         --�����ʲ��������Ƿ�����Լ  (Ĭ��ֵ'��',1��0��)
                ,''                                                             AS buyerinsolvencyflag         --���ñ������Ƿ��Ʋ�
                ,0                                                              AS nonpaymentfees              --��δ֧������
                ,'0'                                                            AS retailexpoflag              --���۱�¶��ʶ              (Ĭ��ֵ'��',1��0��)
                ,''                                                             AS retailclaimtype             --����ծȨ����              (Ĭ��Ϊ��)
                ,''                                                             AS mortgagetype                --ס����Ѻ��������          (Ĭ��Ϊ��)
                ,1                                                              AS exponumber                  --���ձ�¶����              (Ĭ��Ϊ1)
                ,0.8                                                            AS LTV                         --�����ֵ��                    Ĭ�� 0.8
                ,NULL                                                           AS Aging                       --����                          Ĭ�� NULL
                ,''                                                             AS NewDefaultDebtFlag          --����ΥԼծ���ʶ                 Ĭ�� NULL
                ,''                                                             AS PDPoolModelID               --PD�ֳ�ģ��ID                  Ĭ�� NULL
                ,''                                                             AS LGDPoolModelID              --LGD�ֳ�ģ��ID                 Ĭ�� NULL
                ,''                                                             AS CCFPoolModelID              --CCF�ֳ�ģ��ID                 Ĭ�� NULL
                ,''                                                             AS PDPoolID                    --����PD��ID                    Ĭ�� NULL
                ,''                                                             AS LGDPoolID                   --����LGD��ID                   Ĭ�� NULL
                ,''                                                             AS CCFPoolID                   --����CCF��ID                   Ĭ�� NULL
                ,'0'                                                            AS ABSUAFlag                   --�ʲ�֤ȯ�������ʲ���ʶ        Ĭ�� ��(0)
                ,''                                                             AS ABSPoolID                   --֤ȯ���ʲ���ID                Ĭ�� NULL
                ,''                                                             AS GroupID                     --������                      Ĭ�� NULL
                ,NULL                                                           AS DefaultDate                 --ΥԼʱ��
                ,NULL                                                           AS ABSPROPORTION               --�ʲ�֤ȯ������
                ,NULL                                                           AS DEBTORNUMBER                --����˸���
    FROM        RWA_DEV.rwa_ei_unconsfiinvest t1                  --���ڹ�ȨͶ�ʲ�¼��
    LEFT JOIN   RWA.ORG_INFO T2
    ON          T2.ORGID = T1.ORGID
    WHERE       T1.DATADATE = TO_DATE(p_data_dt_str,'yyyy-mm-dd')
    AND         T1.CONSOLIDATEFLAG = '0'
    AND         t1.EQUITYINVESTTYPE LIKE '03%'
    ;
    COMMIT;

    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_GQ_EXPOSURE',cascade => true);

    /*Ŀ�������ͳ��*/
    --ͳ�Ʋ���ļ�¼
    SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_GQ_EXPOSURE;
    Dbms_output.Put_line('RWA_DEV.rwa_gq_exposure��ǰ��������ݼ�¼Ϊ:' || v_count || '��');
    Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̽��� ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    p_po_rtncode := '1';
    p_po_rtnmsg  := '�ɹ�-'||v_count;
    --�����쳣
    EXCEPTION
    WHEN OTHERS THEN
         Dbms_output.Put_line('������,�������Ϊ:'||sqlcode||';������ϢΪ:'||sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace);
          ROLLBACK;
          p_po_rtncode := sqlcode;
          p_po_rtnmsg  := '��ȨͶ��-���÷��ձ�¶(RWA_DEV.pro_rwa_gq_exposure)ETLת��ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;
    RETURN;

END pro_rwa_gq_exposure;
/

