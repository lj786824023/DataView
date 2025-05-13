CREATE OR REPLACE PROCEDURE RWA_DEV.pro_rwa_xn_exposure(
                                                p_data_dt_str  IN   VARCHAR2,    --��������
                                                p_po_rtncode   OUT  VARCHAR2,    --���ر��
                                                p_po_rtnmsg    OUT  VARCHAR2     --��������
                                               )
  /*
    �洢��������:RWA_DEV.pro_rwa_xn_exposure
    ʵ�ֹ���:��������-���÷��ձ�¶ETLת��
    ���ݿھ�:ȫ��
    ����Ƶ��:��ĩ����
    ��  ��  :V1.0.0
    ��д��  :qpzhong
    ��дʱ��:2016-9-20
    ��  λ  :�Ϻ���˶��Ϣ�����ɷ����޹�˾
    Դ  ��1 :RWA_DEV.FNS_GL_BALANCE                   |���˱�
    Դ  ��2 :RWA_DEV.NSS_PA_QUOTEPRICE                |����ת����
    Դ  ��3 :RWA_DEV.RWA_EI_EXPOSURE                  |����-���÷��ձ�¶��
    Դ  ��4 :RWA_DEV.RWA_ARTICULATION_PARAM           |���˹���������
    Դ  ��5 :RWA.CODE_LIBRARY                         |�����
    Դ  ��6 :RWA.ORG_INFO OI                          |������
    Դ  ��7 :RWA.RWA_WS_XD_UNPUTOUT                   |����δ��ҵ��¼��
    Դ  ��8 :RWA_DEV.RWA_TMP_DERIVATION_SUBJECT       |���˹���������Ŀ��ʱ��
    Դ  ��9 :RWA_DEV.RWA_TMP_GLBALANCE                |���˹�����Ŀ�����ʱ��
    Ŀ���1 :RWA_DEV.RWA_EI_EXPOSURE                  |����-���÷��ձ�¶��
    Ŀ���2 :RWA_DEV.RWA_EI_CONTRACT                  |����-���÷��ձ�¶��
    Ŀ���3 :RWA_DEV.RWA_EI_CLIENT                    |����-���÷��ձ�¶��
    Ŀ���4 :RWA_DEV.RWA_TMP_DERIVATION_SUBJECT       |���˹���������Ŀ��ʱ��
    Ŀ���5 :RWA_DEV.RWA_TMP_GLBALANCE                |���˹�����Ŀ�����ʱ��
    �����¼(�޸���|�޸�ʱ��|�޸�����):
  */
  AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  v_pro_name VARCHAR2(200) := 'RWA_DEV.pro_rwa_xn_exposure';
  v_datadate date := to_date(p_data_dt_str,'yyyy/mm/dd');       --��������
  v_datano VARCHAR2(8) := to_char(v_datadate, 'yyyymmdd');      --������ˮ��
  v_startdate VARCHAR2(10) := to_char(v_datadate,'yyyy-mm-dd'); --��ʼ����
  V_ILDDEBT NUMBER(24,6) := 0;
  --�������ļ�¼��
  v_count INTEGER;


  CURSOR C_SERIALNO IS
  select T.SERIALNO,T.THIRD_SUBJECT_NO AS SUBJECT_NO
    from RWA_DEV.RWA_ARTICULATION_PARAM T
   where T.ARTICULATERELATION IS NOT NULL
     AND ARTICULATETYPE IN ('01', '04')
     AND ISCALCULATE = '1' --�Ƿ�RWA���� 0�� 1��
     AND ISINUSE = '1' --����״̬ 1���� 0ͣ��
     ;

   V_SERIALNO C_SERIALNO%ROWTYPE;

  BEGIN
    DBMS_OUTPUT.PUT_LINE('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    --1.���Ŀ����е�ԭ�м�¼
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_TMP_DERIVATION_SUBJECT';
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_TMP_GLBALANCE';

    --ɾ��Ŀ���������
    DELETE FROM RWA_DEV.RWA_EI_EXPOSURE WHERE DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD') AND SSYSID IN ('XN');
    DELETE FROM RWA_DEV.RWA_EI_CLIENT WHERE DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD') AND SSYSID IN ('XN');
    DELETE FROM RWA_DEV.RWA_EI_CONTRACT WHERE DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD') AND SSYSID IN ('XN');
    COMMIT;

    --��ȡ�����ʲ��ۼ���
    SELECT COUNT(1) INTO V_ILDDEBT FROM RWA_dEV.Rwa_Ei_Profitdist T WHERE T.DATADATE = V_DATADATE;

    IF V_ILDDEBT > 0 THEN
      SELECT T.ILDDEBT INTO V_ILDDEBT FROM RWA_dEV.Rwa_Ei_Profitdist T WHERE T.DATADATE = V_DATADATE;
    END IF;

    --��ȡ���ڹ�����ϵ�Ŀ�Ŀ

    OPEN C_SERIALNO ;
    LOOP FETCH C_SERIALNO INTO V_SERIALNO;
    EXIT WHEN C_SERIALNO%NOTFOUND;

    INSERT INTO RWA_DEV.RWA_TMP_DERIVATION_SUBJECT
      WITH TEMP_SUbJECT1 AS  --��ȡ�򵥼Ӽ��Ŀ�Ŀ
       (
       SELECT DISTINCT DS.ARTICULATERELATION --ԭʼ��Ŀ��
                       ,REGEXP_SUBSTR(DS.ARTICULATERELATION,'[^-+]+', 1,LEVEL, 'i') AS REL_SUBJECT_NO --������Ŀ
          FROM (
              SELECT * FROM RWA_DEV.RWA_ARTICULATION_PARAM    --���˹���������
              WHERE UPPER(ARTICULATERELATION) NOT LIKE 'MAX%'
               AND SERIALNO = V_SERIALNO.SERIALNO
           ) DS
           CONNECT BY LEVEL <= LENGTH(REGEXP_REPLACE(DS.ARTICULATERELATION, '[^-+]+', '')) + 1
        )
        ,
        TEMP_SUbJECT2 AS    --��ȡ���ӿ�Ŀ
       (SELECT DISTINCT ARTICULATERELATION       --ԭʼ��Ŀ��
                       ,REGEXP_SUBSTR(ARTICULATERELATION2,'[^-+]+', 1,LEVEL, 'i') AS REL_SUBJECT_NO --������Ŀ
          FROM(
             SELECT ARTICULATERELATION,
                    CASE WHEN INSTR(ARTICULATERELATION,',')>6 THEN SUBSTR(ARTICULATERELATION,5,INSTR(ARTICULATERELATION,',')-5)
                         ELSE SUBSTR(ARTICULATERELATION,7,LENGTH(ARTICULATERELATION)-7) END AS ARTICULATERELATION2
              FROM RWA_DEV.RWA_ARTICULATION_PARAM  --���˹���������
             WHERE UPPER(ARTICULATERELATION) LIKE 'MAX%'
               AND SERIALNO = V_SERIALNO.SERIALNO
           )
        CONNECT BY LEVEL <= LENGTH(REGEXP_REPLACE(ARTICULATERELATION2, '[^-+]+', '')) + 1)
      SELECT RAP.THIRD_SUBJECT_NO, TS.REL_SUBJECT_NO
        FROM RWA_DEV.RWA_ARTICULATION_PARAM RAP
       INNER JOIN (SELECT ARTICULATERELATION,REL_SUBJECT_NO FROM TEMP_SUBJECT1
                   UNION
                   SELECT ARTICULATERELATION,REL_SUBJECT_NO FROM TEMP_SUBJECT2
                  ) TS
          ON TS.ARTICULATERELATION = RAP.ARTICULATERELATION
       ORDER BY RAP.THIRD_SUBJECT_NO, TS.REL_SUBJECT_NO ASC;
    COMMIT;

     --��ʼ�������Ŀ��Ϣ
    INSERT INTO RWA_DEV.RWA_TMP_GLBALANCE(
           SUBJECT_NO,ORGID,CURRENCY,ACCOUNT_BALANCE
    )
    WITH TEMP_GL AS (
            SELECT FGB.SUBJECT_NO,
                   FGB.ORG_ID AS ORGID,
                   FGB.CURRENCY_CODE AS CURRENCY,
                   CASE WHEN CL.ATTRIBUTE8 = 'C-D'
                        THEN SUM(FGB.BALANCE_C * NVL(NPQ.MIDDLEPRICE/100, 1) - FGB.BALANCE_D * NVL(NPQ.MIDDLEPRICE/100, 1))
                        ELSE SUM(FGB.BALANCE_D * NVL(NPQ.MIDDLEPRICE/100, 1) - FGB.BALANCE_C * NVL(NPQ.MIDDLEPRICE/100, 1))
                    END AS ACCOUNT_BALANCE --��Ŀ���
              FROM RWA_DEV.FNS_GL_BALANCE FGB           --���˱�
              LEFT JOIN RWA_DEV.TMP_CURRENCY_CHANGE NPQ   --����ת����
                ON NPQ.DATANO = FGB.DATANO
               AND NPQ.CURRENCYCODE = FGB.CURRENCY_CODE
              LEFT JOIN RWA.CODE_LIBRARY CL     --�����
                ON CL.CODENO = 'NewSubject'
               AND CL.ITEMNO = FGB.SUBJECT_NO
             WHERE FGB.DATANO = V_DATANO
               AND FGB.CURRENCY_CODE <> 'RMB'
               AND FGB.SUBJECT_NO  in (select articulaterelation from rwa_dev.rwa_tmp_derivation_subject where subject_no = v_serialno.subject_no)
             GROUP BY FGB.SUBJECT_NO, FGB.ORG_ID, CURRENCY_CODE,CL.ATTRIBUTE8
             ORDER BY FGB.SUBJECT_NO, FGB.ORG_ID, FGB.CURRENCY_CODE ASC
             )
      SELECT SUBJECT_NO,
             ORGID,
             CURRENCY,
             SUBSTR(COMPLEX_LOGIC_FUNCTION,INSTR(COMPLEX_LOGIC_FUNCTION,'@',1,1)+1) AS ACCOUNT_BALANCE
      FROM (SELECT SUBJECT_NO,
                   ORGID,
                   CURRENCY,
                   FUN_DERIVATION_SUBJECT(REL_SUBJECT_NO || '@' || LOGIC_FUNCTION || '@' || ACCOUNT_BALANCE) AS COMPLEX_LOGIC_FUNCTION
             FROM (
                  SELECT TAP2.SUBJECT_NO as SUBJECT_NO,
                         TAP2.ORGID as ORGID,
                         TAP2.CURRENCY,
                         REPLACE(UPPER(RAP.ARTICULATERELATION),'MAX','GREATEST') AS LOGIC_FUNCTION,
                         TAP2.ARTICULATERELATION AS REL_SUBJECT_NO,
                         NVL(GL.ACCOUNT_BALANCE, 0) AS ACCOUNT_BALANCE
                    FROM (
                          SELECT DISTINCT TAP.SUBJECT_NO,
                                 GL2.ORGID,
                                 GL2.CURRENCY,
                                 TAP.ARTICULATERELATION
                            FROM RWA_DEV.RWA_TMP_DERIVATION_SUBJECT TAP,TEMP_GL GL2
                    ) TAP2
                    LEFT JOIN TEMP_GL GL
                      ON TAP2.ARTICULATERELATION = GL.SUBJECT_NO
                     AND TAP2.CURRENCY = GL.CURRENCY
                     AND TAP2.ORGID = GL.ORGID
                     AND EXISTS (SELECT 1 FROM TEMP_GL GL2
                                  WHERE GL2.ORGID = GL.ORGID
                                    AND GL2.CURRENCY = GL.CURRENCY
                                    AND GL2.SUBJECT_NO = TAP2.SUBJECT_NO)
                   INNER JOIN RWA_DEV.RWA_ARTICULATION_PARAM RAP --���˹���������
                      ON RAP.THIRD_SUBJECT_NO = TAP2.SUBJECT_NO
                     AND RAP.ISCALCULATE = '1' --�Ƿ����1�� 0��
                     AND RAP.ISINUSE = '1' --����״̬ 1���� 0ͣ��
                     AND RAP.ISGATHER = '0' --�Ƿ���ܵ�����
                     AND RAP.ARTICULATETYPE IN ('01','04')
                     AND RAP.SERIALNO = V_SERIALNO.SERIALNO
                   UNION ALL
                   SELECT SUBJECT_NO,
                         '9998' as orgid,
                         'CNY' AS CURRENCY,
                         REPLACE(UPPER(ARTICULATERELATION), 'MAX', 'GREATEST') AS LOGIC_FUNCTION,
                         REL_SUBJECT_NO,
                         SUM(NVL(ACCOUNT_BALANCE, 0)) AS ACCOUNT_BALANCE
                    FROM (SELECT DISTINCT TAP2.SUBJECT_NO,
                                          TAP2.ARTICULATERELATION AS REL_SUBJECT_NO,
                                          GL.ACCOUNT_BALANCE,
                                          RAP.ARTICULATERELATION  AS ARTICULATERELATION
                            FROM (SELECT DISTINCT TAP.SUBJECT_NO,
                                                  TAP.ARTICULATERELATION
                                    FROM RWA_DEV.RWA_TMP_DERIVATION_SUBJECT TAP,
                                         TEMP_GL                            GL2
                                   WHERE GL2.ACCOUNT_BALANCE <> 0) TAP2
                            LEFT JOIN (SELECT SUBJECT_NO,
                                             SUM(ACCOUNT_BALANCE) AS ACCOUNT_BALANCE
                                        FROM TEMP_GL
                                       GROUP BY SUBJECT_NO) GL
                              ON TAP2.ARTICULATERELATION = GL.SUBJECT_NO
                             AND EXISTS (SELECT 1 FROM TEMP_GL GL2 WHERE GL2.SUBJECT_NO = TAP2.SUBJECT_NO)
                           INNER JOIN RWA_DEV.RWA_ARTICULATION_PARAM RAP --���˹���������
                              ON RAP.THIRD_SUBJECT_NO = TAP2.SUBJECT_NO
                             AND RAP.ISCALCULATE = '1' --�Ƿ����1�� 0��
                             AND RAP.ISINUSE = '1' --����״̬ 1���� 0ͣ��
                             AND RAP.ISGATHER = '1' --�Ƿ���ܵ�����
                             AND RAP.ARTICULATETYPE IN ('01', '04')
                             AND RAP.SERIALNO = V_SERIALNO.SERIALNO
                             )
                   GROUP BY SUBJECT_NO, REL_SUBJECT_NO, ARTICULATERELATION
                   )
              GROUP BY SUBJECT_NO, ORGID, CURRENCY
       );
       COMMIT;

END LOOP;
CLOSE C_SERIALNO;

dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_TMP_DERIVATION_SUBJECT',cascade => true);
dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_TMP_GLBALANCE',cascade => true);

    --2.�����������ݲ���Ŀ��
    INSERT INTO RWA_DEV.RWA_EI_EXPOSURE(
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
                DISTINCT
                V_DATADATE                                                        AS DATADATE                   --��������
                ,V_DATANO                                                         AS DATANO                     --������ˮ��
                ,'XN-' || GL.SUBJECT_NO || '-' || GL.ORGID || '-' || GL.CURRENCY  AS EXPOSUREID                 --���ձ�¶����              (ZZ- || ��Ŀ || ������� || ���� )
                ,'XN-' || GL.SUBJECT_NO || '-' || GL.ORGID || '-' || GL.CURRENCY  AS DUEID                      --ծ�����
                ,'XN'                                                             AS SSYSID                     --Դϵͳ����
                ,'XN-' || GL.SUBJECT_NO || '-' || GL.ORGID || '-' || GL.CURRENCY  AS CONTRACTID                 --��ͬ����
                ,'XN-' || GL.SUBJECT_NO || '-' || GL.ORGID || '-' || GL.CURRENCY  AS CLIENTID                   --�����������
                ,GL.ORGID                                                         AS SORGID                     --Դ��������
                ,GL.ORGNAME                                                       AS SORGNAME                   --Դ��������
                ,'1010'                                                              AS ORGSORTNO                  --���������
                ,GL.ORGID                                                         AS ORGID                      --������������
                ,GL.ORGNAME                                                       AS ORGNAME                    --������������
                ,GL.ORGID                                                         AS ACCORGID                   --�����������
                ,GL.ORGNAME                                                       AS ACCORGNAME                 --�����������
                ,'999999'                                                         AS INDUSTRYID                 --������ҵ����
                ,'δ֪'                                                           AS industryname               --������ҵ����              (ϵͳת��)
                ,'0501'                                                           AS BUSINESSLINE               --����
                ,''                                                               AS ASSETTYPE                  --�ʲ�����                  (���ݿ�Ŀת��)
                ,''                                                               AS ASSETSUBTYPE               --�ʲ�С��                  (���ݿ�Ŀת��)
                ,'9010101010'                                                     AS BUSINESSTYPEID             --ҵ��Ʒ�ִ���
                ,'����ҵ��Ʒ��'                                                   AS BUSINESSTYPENAME           --ҵ��Ʒ������
                ,CASE WHEN RETAILFLAG='1' THEN '02' ELSE '01' END                 AS CREDITRISKDATATYPE         --���÷�����������          (Ĭ��'һ�������',01һ�������,02һ������)
                ,'01'                                                             AS ASSETTYPEOFHAIRCUTS        --�ۿ�ϵ����Ӧ�ʲ����      (Ĭ��'�ֽ��ֽ�ȼ���',01�ֽ��ֽ�ȼ���)
                ,''                                                               AS BUSINESSTYPESTD            --Ȩ�ط�ҵ������            (ϵͳת��)
                ,''                                                               AS EXPOCLASSSTD               --Ȩ�ط���¶����            (ϵͳת��)
                ,''                                                               AS EXPOSUBCLASSSTD            --Ȩ�ط���¶С��            (ϵͳת��)
                ,''                                                               AS EXPOCLASSIRB               --��������¶����            (ϵͳת��)
                ,''                                                               AS EXPOSUBCLASSIRB            --��������¶С��            (ϵͳת��)
                ,CASE WHEN SUBSTR(GL.SUBJECT_NO,1,1) = '7' THEN '02' ELSE '01' END AS EXPOBELONG                 --��¶������ʶ
                , '01'                                                            AS BOOKTYPE                   --�˻����
                ,'03'                                                             AS REGUTRANTYPE               --��ܽ�������              (Ĭ��'��Ѻ����',01�ع�����,02�����ʱ��г�����,03��Ѻ����)
                ,'0'                                                              AS REPOTRANFLAG               --�ع����ױ�ʶ              (Ĭ��Ϊ��,1��0��)
                ,1                                                                AS REVAFREQUENCY              --�ع�Ƶ��
                ,GL.CURRENCY                                                      AS CURRENCY                   --����
                ,CASE WHEN GL.SUBJECT_NO = '17010000' THEN GL.ACCOUNT_BALANCE - NVL(V_ILDDEBT,0)
                ELSE GL.ACCOUNT_BALANCE END                                       AS NORMALPRINCIPAL            --�����������              (������ֵ���м���)
                ,0                                                                AS OVERDUEBALANCE             --�������
                ,0                                                                AS NONACCRUALBALANCE          --��Ӧ�����
                ,CASE WHEN GL.SUBJECT_NO = '17010000' THEN GL.ACCOUNT_BALANCE - NVL(V_ILDDEBT,0)
                ELSE GL.ACCOUNT_BALANCE END + 0 + 0                               AS ONSHEETBALANCE             --�������
                ,0                                                                AS NORMALINTEREST             --������Ϣ
                ,0                                                                AS ONDEBITINTEREST            --����ǷϢ
                ,0                                                                AS OFFDEBITINTEREST           --����ǷϢ
                ,0                                                                AS EXPENSERECEIVABLE          --Ӧ�շ���
                ,(CASE WHEN GL.SUBJECT_NO = '17010000' THEN GL.ACCOUNT_BALANCE - NVL(V_ILDDEBT,0)
                ELSE GL.ACCOUNT_BALANCE END+ 0 + 0) + (0 + 0 + 0) + 0             AS ASSETBALANCE               --�ʲ����
                ,GL.SUBJECT_NO                                                    AS ACCSUBJECT1                --��Ŀһ
                ,''                                                               AS ACCSUBJECT2                --��Ŀ��
                ,''                                                               AS ACCSUBJECT3                --��Ŀ��
                ,V_STARTDATE                                                      AS STARTDATE                  --��ʼ����
                ,TO_CHAR(ADD_MONTHS(V_DATADATE,1),'YYYY-MM-DD')                   AS DUEDATE                    --��������                  (�������� + 1����)
                ,(ADD_MONTHS(V_DATADATE,1) - V_DATADATE)/365                      AS ORIGINALMATURITY           --ԭʼ����                  (��λ��)
                ,(ADD_MONTHS(V_DATADATE,1) - V_DATADATE)/365                      AS RESIDUALM                  --ʣ������                  (��λ��)
                ,'01'                                                             AS RISKCLASSIFY               --���շ���
                ,''                                                               AS EXPOSURESTATUS             --���ձ�¶״̬               (Ĭ��Ϊ��)
                ,0                                                                AS OVERDUEDAYS                --��������
                ,0                                                                AS SPECIALPROVISION           --ר��׼����
                ,0                                                                AS GENERALPROVISION           --һ��׼����
                ,0                                                                AS ESPECIALPROVISION          --�ر�׼����
                ,0                                                                AS WRITTENOFFAMOUNT           --�Ѻ������
                ,CASE WHEN SUBSTR(GL.SUBJECT_NO,1,1) = '7' THEN '03' ELSE '' END  AS OFFEXPOSOURCE              --���Ⱪ¶��Դ
                ,''                                                               AS OFFBUSINESSTYPE            --����ҵ������              (ϵͳת��)
                ,''                                                               AS OFFBUSINESSSDVSSTD         --Ȩ�ط�����ҵ������ϸ��    (ϵͳת��)
                ,'0'                                                              AS UNCONDCANCELFLAG           --�Ƿ����ʱ����������      (Ĭ��Ϊ��,1��0��)
                ,''                                                               AS CCFLEVEL                   --����ת��ϵ������
                ,0                                                                AS CCFAIRB                    --�߼���������ת��ϵ��
                ,'01'                                                             AS CLAIMSLEVEL                --ծȨ����                  (Ĭ��Ϊ�߼�ծȨ;01�߼�ծȨ,02�μ�ծȨ)
                ,'0'                                                              AS BONDFLAG                   --�Ƿ�Ϊծȯ                (Ĭ��Ϊ��,1��0��)
                ,''                                                               AS BONDISSUEINTENT            --ծȯ����Ŀ��
                ,'0'                                                              AS NSUREALPROPERTYFLAG        --�Ƿ�����ò�����          (Ĭ��Ϊ��,1��0��)
                ,''                                                               AS REPASSETTERMTYPE           --��ծ�ʲ���������
                ,'0'                                                              AS DEPENDONFPOBFLAG           --�Ƿ�����������δ��ӯ��    (Ĭ��Ϊ��,1��0��)
                ,''                                                               AS IRATING                    --�ڲ�����
                ,NULL                                                             AS PD                         --ΥԼ����
                ,''                                                               AS LGDLEVEL                   --ΥԼ��ʧ�ʼ���
                ,NULL                                                             AS LGDAIRB                    --�߼�����ΥԼ��ʧ��
                ,NULL                                                             AS MAIRB                      --�߼�������Ч����
                ,NULL                                                             AS EADAIRB                    --�߼�����ΥԼ���ձ�¶
                ,'0'                                                              AS DEFAULTFLAG                --ΥԼ��ʶ                  (Ĭ��Ϊ��,1��0��)
                ,NULL                                                             AS BEEL                       --��ΥԼ��¶Ԥ����ʧ����
                ,NULL                                                             AS DEFAULTLGD                 --��ΥԼ��¶ΥԼ��ʧ��
                ,'0'                                                              AS EQUITYEXPOFLAG             --��Ȩ��¶��ʶ              (Ĭ��Ϊ��,1��0��)
                ,''                                                               AS EQUITYINVESTTYPE           --��ȨͶ�ʶ�������
                ,''                                                               AS EQUITYINVESTCAUSE          --��ȨͶ���γ�ԭ��
                ,'0'                                                              AS SLFLAG                     --רҵ�����ʶ              (Ĭ��Ϊ��,1��0��)
                ,''                                                               AS SLTYPE                     --רҵ��������
                ,''                                                               AS PFPHASE                    --��Ŀ���ʽ׶�
                ,''                                                               AS REGURATING                 --�������
                ,'0'                                                              AS CBRCMPRATINGFLAG           --������϶������Ƿ��Ϊ����(Ĭ��Ϊ��,1��0��)
                ,'0'                                                              AS LARGEFLUCFLAG              --�Ƿ񲨶��Խϴ�            (Ĭ��Ϊ��,1��0��)
                ,'0'                                                              AS LIQUEXPOFLAG               --�Ƿ���������з��ձ�¶    (Ĭ��Ϊ��,1��0��)
                ,'0'                                                              AS PAYMENTDEALFLAG            --�Ƿ����Ը�ģʽ          (Ĭ��Ϊ��,1��0��)
                ,0                                                                AS DELAYTRADINGDAYS           --�ӳٽ�������
                ,'0'                                                              AS SECURITIESFLAG             --�м�֤ȯ��ʶ              (Ĭ��Ϊ��,1��0��)
                ,''                                                               AS SECUISSUERID               --֤ȯ�����˴���
                ,''                                                               AS RATINGDURATIONTYPE         --������������
                ,''                                                               AS SECUISSUERATING            --֤ȯ���еȼ�
                ,NULL                                                             AS SECURESIDUALM              --֤ȯʣ������
                ,''                                                               AS SECUREVAFREQUENCY          --֤ȯ�ع�Ƶ��
                ,'0'                                                              AS CCPTRANFLAG                --�Ƿ����뽻�׶�����ؽ���  (Ĭ��Ϊ��,1��0��)
                ,''                                                               AS CCPID                      --���뽻�׶���ID
                ,'0'                                                              AS QUALCCPFLAG                --�Ƿ�ϸ����뽻�׶���      (Ĭ��Ϊ��,1��0��)
                ,''                                                               AS BANKROLE                   --���н�ɫ
                ,''                                                               AS CLEARINGMETHOD             --���㷽ʽ
                ,'0'                                                              AS BANKASSETFLAG              --�Ƿ������ύ�ʲ�          (Ĭ��Ϊ��,1��0��)
                ,''                                                               AS MATCHCONDITIONS            --�����������
                ,'0'                                                              AS SFTFLAG                    --֤ȯ���ʽ��ױ�ʶ          (Ĭ��Ϊ��,1��0��)
                ,'0'                                                              AS MASTERNETAGREEFLAG         --���������Э���ʶ        (Ĭ��Ϊ��,1��0��)
                ,''                                                               AS MASTERNETAGREEID           --���������Э�����
                ,''                                                               AS SFTTYPE                    --֤ȯ���ʽ�������
                ,'0'                                                              AS SECUOWNERTRANSFLAG         --֤ȯ����Ȩ�Ƿ�ת��        (Ĭ��Ϊ��,1��0��)
                ,'0'                                                              AS OTCFLAG                    --�����������߱�ʶ          (Ĭ��Ϊ��,1��0��)
                ,'0'                                                              AS VALIDNETTINGFLAG           --��Ч�������Э���ʶ      (Ĭ��Ϊ��,1��0��)
                ,''                                                               AS VALIDNETAGREEMENTID        --��Ч�������Э�����
                ,''                                                               AS OTCTYPE                    --����������������
                ,0                                                                AS DEPOSITRISKPERIOD          --��֤������ڼ�
                ,0                                                                AS MTM                        --���óɱ�
                ,''                                                               AS MTMCURRENCY                --���óɱ�����
                ,''                                                               AS BUYERORSELLER              --������
                ,'0'                                                              AS QUALROFLAG                 --�ϸ�����ʲ���ʶ          (Ĭ��Ϊ��,1��0��)
                ,'0'                                                              AS ROISSUERPERFORMFLAG        --�����ʲ��������Ƿ�����Լ  (Ĭ��Ϊ��,1��0��)
                ,'0'                                                              AS BUYERINSOLVENCYFLAG        --���ñ������Ƿ��Ʋ�      (Ĭ��Ϊ��,1��0��)
                ,''                                                               AS NONPAYMENTFEES             --��δ֧������
                ,RETAILFLAG                                                       AS RETAILEXPOFLAG             --���۱�¶��ʶ              (Ĭ��Ϊ��,1��0��)
                ,''                                                               AS RETAILCLAIMTYPE            --����ծȨ����
                ,''                                                               AS MORTGAGETYPE               --ס����Ѻ��������
                ,1                                                                AS ExpoNumber                 --���ձ�¶����                Ĭ�� 1
                ,0.8                                                              AS LTV                        --�����ֵ��                 Ĭ�� 0.8
                ,NULL                                                             AS Aging                      --����                        Ĭ�� NULL
                ,''                                                               AS NewDefaultDebtFlag         --����ΥԼծ���ʶ            Ĭ�� NULL
                ,''                                                               AS PDPoolModelID              --PD�ֳ�ģ��ID                Ĭ�� NULL
                ,''                                                               AS LGDPoolModelID             --LGD�ֳ�ģ��ID               Ĭ�� NULL
                ,''                                                               AS CCFPoolModelID             --CCF�ֳ�ģ��ID               Ĭ�� NULL
                ,''                                                               AS PDPoolID                   --����PD��ID                 Ĭ�� NULL
                ,''                                                               AS LGDPoolID                  --����LGD��ID                Ĭ�� NULL
                ,''                                                               AS CCFPoolID                  --����CCF��ID                Ĭ�� NULL
                ,'0'                                                              AS ABSUAFlag                  --�ʲ�֤ȯ�������ʲ���ʶ     Ĭ�� ��(0)
                ,''                                                               AS ABSPoolID                  --֤ȯ���ʲ���ID              Ĭ�� NULL
                ,''                                                               AS GroupID                    --������                    Ĭ�� NULL
                ,NULL                                                             AS DefaultDate                --ΥԼʱ��
                ,NULL                                                             AS ABSPROPORTION              --�ʲ�֤ȯ������
                ,NULL                                                             AS DEBTORNUMBER               --����˸���
     FROM (
            SELECT FGB.SUBJECT_NO,
                   FGB.ORG_ID AS ORGID,
                   OI.ORGNAME ,
                   FGB.CURRENCY_CODE AS CURRENCY,
                   RAP.RETAILFLAG,
                   CASE WHEN CL.ATTRIBUTE8 = 'C-D'
                        THEN SUM(FGB.BALANCE_C * NVL(NPQ.MIDDLEPRICE/100, 1) - FGB.BALANCE_D * NVL(NPQ.MIDDLEPRICE/100, 1))
                        ELSE SUM(FGB.BALANCE_D * NVL(NPQ.MIDDLEPRICE/100, 1) - FGB.BALANCE_C * NVL(NPQ.MIDDLEPRICE/100, 1))
                        END AS ACCOUNT_BALANCE --��Ŀ���
              FROM RWA_DEV.FNS_GL_BALANCE FGB
              LEFT JOIN RWA_DEV.TMP_CURRENCY_CHANGE NPQ
                ON NPQ.DATANO = FGB.DATANO
               AND NPQ.CURRENCYCODE = FGB.CURRENCY_CODE
               LEFT JOIN RWA.ORG_INFO OI
                ON OI.ORGID = FGB.ORG_ID
             INNER JOIN RWA_DEV.RWA_ARTICULATION_PARAM RAP
                ON RAP.THIRD_SUBJECT_NO = FGB.SUBJECT_NO
              LEFT JOIN RWA.CODE_LIBRARY CL
                ON CL.CODENO = 'NewSubject'
               AND cl.itemno = fgb.subject_no
             WHERE FGB.DATANO = V_DATANO
               AND FGB.CURRENCY_CODE <> 'RMB'
               AND RAP.ISGATHER = '0'
               AND RAP.ISCALCULATE = '1'
               AND RAP.ARTICULATETYPE IN ('01','04')
               AND RAP.ARTICULATERELATION IS NULL
             GROUP BY FGB.SUBJECT_NO, FGB.ORG_ID,OI.ORGNAME, CURRENCY_CODE, RAP.RETAILFLAG, CL.ATTRIBUTE8
             UNION ALL
             SELECT FGB.SUBJECT_NO,
                   '9998' AS ORGID,
                   '��������' AS ORGNAME ,
                   FGB.CURRENCY_CODE AS CURRENCY,
                   RAP.RETAILFLAG,
                  CASE WHEN CL.ATTRIBUTE8 = 'C-D'
                   THEN  SUM(FGB.BALANCE_C * NVL(NPQ.MIDDLEPRICE/100, 1) - FGB.BALANCE_D * NVL(NPQ.MIDDLEPRICE/100, 1))
                     ELSE SUM(FGB.BALANCE_D * NVL(NPQ.MIDDLEPRICE/100, 1) - FGB.BALANCE_C * NVL(NPQ.MIDDLEPRICE/100, 1))  END AS ACCOUNT_BALANCE --��Ŀ���
              FROM RWA_DEV.FNS_GL_BALANCE FGB
              LEFT JOIN RWA_DEV.TMP_CURRENCY_CHANGE NPQ
                ON NPQ.DATANO = FGB.DATANO
               AND NPQ.CURRENCYCODE = FGB.CURRENCY_CODE
              LEFT JOIN RWA_DEV.RWA_ARTICULATION_PARAM RAP
                ON RAP.THIRD_SUBJECT_NO = FGB.SUBJECT_NO
              LEFT JOIN RWA.CODE_LIBRARY CL
                ON CL.CODENO = 'NewSubject'
               and cl.itemno = fgb.subject_no
             WHERE FGB.DATANO = V_DATANO
               AND FGB.CURRENCY_CODE <> 'RMB'
               AND RAP.ISGATHER = '1'
               AND RAP.ISCALCULATE = '1'
               AND RAP.ARTICULATETYPE IN ('01','04')
               AND RAP.ARTICULATERELATION IS NULL
             GROUP BY FGB.SUBJECT_NO, CURRENCY_CODE,RAP.RETAILFLAG,CL.ATTRIBUTE8
    ) GL
     WHERE GL.ACCOUNT_BALANCE<>0
       AND GL.SUBJECT_NO NOT IN ( SELECT ACCSUBJECT1
                                    FROM RWA_DEV.RWA_EI_EXPOSURE
                                   WHERE DATADATE = V_DATADATE
                                     AND ACCSUBJECT1 IS NOT NULL
                                     AND SSYSID <> 'GC' )
    ;
    COMMIT;

    /*�����Ŀ����*/
    INSERT INTO RWA_DEV.RWA_EI_EXPOSURE(
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
                V_DATADATE                                                         AS DATADATE                   --��������
                ,V_DATANO                                                          AS DATANO                     --������ˮ��
                ,'XN-' || GL.SUBJECT_NO || '-' || GL.ORGID || '-' || GL.CURRENCY   AS EXPOSUREID                 --���ձ�¶����
                ,'XN-' || GL.SUBJECT_NO || '-' || GL.ORGID || '-' || GL.CURRENCY   AS DUEID                      --ծ�����
                ,'XN'                                                              AS SSYSID                     --Դϵͳ����
                ,'XN-' || GL.SUBJECT_NO || '-' || GL.ORGID || '-' || GL.CURRENCY   AS CONTRACTID                 --��ͬ����
                ,'XN-' || GL.SUBJECT_NO || '-' || GL.ORGID || '-' || GL.CURRENCY   AS CLIENTID                   --�����������
                ,GL.ORGID                                                          AS SORGID                     --Դ��������
                ,OI.ORGNAME                                                        AS SORGNAME                   --Դ��������
                ,OI.SORTNO                                                         AS ORGSORTNO                  --���������
                ,GL.ORGID                                                          AS ORGID                      --������������
                ,OI.ORGNAME                                                        AS ORGNAME                    --������������
                ,GL.ORGID                                                          AS ACCORGID                   --�����������
                ,OI.ORGNAME                                                        AS ACCORGNAME                 --�����������
                ,'999999'                                                          AS INDUSTRYID                 --������ҵ����
                ,'δ֪'                                                            AS INDUSTRYNAME               --������ҵ����
                ,'0501'                                                            AS BUSINESSLINE               --����
                ,''                                                                AS ASSETTYPE                  --�ʲ�����                  (���������ʲ�)
                ,''                                                                AS ASSETSUBTYPE               --�ʲ�С��
                ,'9010101010'                                                      AS BUSINESSTYPEID             --ҵ��Ʒ�ִ���
                ,'����ҵ��Ʒ��'                                                    AS BUSINESSTYPENAME           --ҵ��Ʒ������
                ,CASE WHEN RETAILFLAG='1' THEN '02' ELSE '01' END                  AS CREDITRISKDATATYPE         --���÷�����������          (Ĭ��'һ�������',01һ�������,02һ������)
                ,'01'                                                              AS ASSETTYPEOFHAIRCUTS        --�ۿ�ϵ����Ӧ�ʲ����      (Ĭ��'�ֽ��ֽ�ȼ���',01�ֽ��ֽ�ȼ���)
                ,'07'                                                              AS BUSINESSTYPESTD            --Ȩ�ط�ҵ������            (Ĭ��Ϊ07һ���ʲ�)
                ,''                                                                AS EXPOCLASSSTD               --Ȩ�ط���¶����            (Ĭ��Ϊ0112����)
                ,''                                                                AS EXPOSUBCLASSSTD            --Ȩ�ط���¶С��            (Ĭ��Ϊ011216��������100%����Ȩ�ص��ʲ�)
                ,''                                                                AS EXPOCLASSIRB               --��������¶����            (Ĭ��Ϊ��)
                ,''                                                                AS EXPOSUBCLASSIRB            --��������¶С��            (Ĭ��Ϊ��)
                ,'01'                                                              AS EXPOBELONG                 --��¶������ʶ              (01����;02һ�����)
                ,'01'                                                              AS BOOKTYPE                   --�˻����                  (01�����˻�;02�����˻�)
                ,'03'                                                              AS REGUTRANTYPE               --��ܽ�������              (Ĭ��'��Ѻ����',01�ع�����,02�����ʱ��г�����,03��Ѻ����)
                ,'0'                                                               AS REPOTRANFLAG               --�ع����ױ�ʶ              (Ĭ��Ϊ��,1��0��)
                ,1                                                                 AS REVAFREQUENCY              --�ع�Ƶ��
                ,GL.CURRENCY                                                       AS CURRENCY                   --����                      (Ĭ��Ϊ01�����)
                ,CASE WHEN GL.SUBJECT_NO = '17010000' THEN GL.ACCOUNT_BALANCE - NVL(V_ILDDEBT,0)
                ELSE GL.ACCOUNT_BALANCE END                                        AS NORMALPRINCIPAL            --�����������              (������ֵ���м���)
                ,0                                                                 AS OVERDUEBALANCE             --�������
                ,0                                                                 AS NONACCRUALBALANCE          --��Ӧ�����
                ,CASE WHEN GL.SUBJECT_NO = '17010000' THEN GL.ACCOUNT_BALANCE - NVL(V_ILDDEBT,0)
                ELSE GL.ACCOUNT_BALANCE END                                        AS ONSHEETBALANCE             --�������
                ,0                                                                 AS NORMALINTEREST             --������Ϣ
                ,0                                                                 AS ONDEBITINTEREST            --����ǷϢ
                ,0                                                                 AS OFFDEBITINTEREST           --����ǷϢ
                ,0                                                                 AS EXPENSERECEIVABLE          --Ӧ�շ���
                ,CASE WHEN GL.SUBJECT_NO = '17010000' THEN GL.ACCOUNT_BALANCE - NVL(V_ILDDEBT,0)
                ELSE GL.ACCOUNT_BALANCE END                                        AS ASSETBALANCE               --�ʲ����
                ,GL.SUBJECT_NO                                                     AS ACCSUBJECT1                --��Ŀһ
                ,''                                                                AS ACCSUBJECT2                --��Ŀ��
                ,''                                                                AS ACCSUBJECT3                --��Ŀ��
                ,V_STARTDATE                                                       AS STARTDATE                  --��ʼ����
                ,TO_CHAR(ADD_MONTHS(V_DATADATE,1),'YYYY-MM-DD')                    AS DUEDATE                    --��������                  (�������� + 1����)
                ,(ADD_MONTHS(V_DATADATE,1) - V_DATADATE)/365                       AS ORIGINALMATURITY           --ԭʼ����                  (��λ��)
                ,(ADD_MONTHS(V_DATADATE,1) - V_DATADATE)/365                       AS RESIDUALM                  --ʣ������
                ,'01'                                                              AS RISKCLASSIFY               --���շ���
                ,''                                                                AS EXPOSURESTATUS             --���ձ�¶״̬               (Ĭ��Ϊ��)
                ,0                                                                 AS OVERDUEDAYS                --��������
                ,0                                                                 AS SPECIALPROVISION           --ר��׼����
                ,0                                                                 AS GENERALPROVISION           --һ��׼����
                ,0                                                                 AS ESPECIALPROVISION          --�ر�׼����
                ,0                                                                 AS WRITTENOFFAMOUNT           --�Ѻ������
                ,''                                                                AS OFFEXPOSOURCE              --���Ⱪ¶��Դ
                ,''                                                                AS OFFBUSINESSTYPE            --����ҵ������
                ,''                                                                AS OFFBUSINESSSDVSSTD         --Ȩ�ط�����ҵ������ϸ��
                ,'1'                                                               AS UNCONDCANCELFLAG           --�Ƿ����ʱ����������      (Ĭ��Ϊ��,1��0��)
                ,''                                                                AS CCFLEVEL                   --����ת��ϵ������
                ,NULL                                                              AS CCFAIRB                    --�߼���������ת��ϵ��
                ,'01'                                                              AS CLAIMSLEVEL                --ծȨ����                  (Ĭ��Ϊ�߼�ծȨ;01�߼�ծȨ,02�μ�ծȨ)
                ,'0'                                                               AS BONDFLAG                   --�Ƿ�Ϊծȯ                (Ĭ��Ϊ��,1��0��)
                ,''                                                                AS BONDISSUEINTENT            --ծȯ����Ŀ��
                ,'0'                                                               AS NSUREALPROPERTYFLAG        --�Ƿ�����ò�����          (Ĭ��Ϊ��,1��0��)
                ,''                                                                AS REPASSETTERMTYPE           --��ծ�ʲ���������
                ,'0'                                                               AS DEPENDONFPOBFLAG           --�Ƿ�����������δ��ӯ��    (Ĭ��Ϊ��,1��0��)
                ,''                                                                AS IRATING                    --�ڲ�����
                ,NULL                                                              AS PD                         --ΥԼ����
                ,''                                                                AS LGDLEVEL                   --ΥԼ��ʧ�ʼ���
                ,NULL                                                              AS LGDAIRB                    --�߼�����ΥԼ��ʧ��
                ,NULL                                                              AS MAIRB                      --�߼�������Ч����
                ,NULL                                                              AS EADAIRB                    --�߼�����ΥԼ���ձ�¶
                ,'0'                                                               AS DEFAULTFLAG                --ΥԼ��ʶ                  (Ĭ��Ϊ��,1��0��)
                ,NULL                                                              AS BEEL                       --��ΥԼ��¶Ԥ����ʧ����
                ,NULL                                                              AS DEFAULTLGD                 --��ΥԼ��¶ΥԼ��ʧ��
                ,'0'                                                               AS EQUITYEXPOFLAG             --��Ȩ��¶��ʶ              (Ĭ��Ϊ��,1��0��)
                ,''                                                                AS EQUITYINVESTTYPE           --��ȨͶ�ʶ�������
                ,''                                                                AS EQUITYINVESTCAUSE          --��ȨͶ���γ�ԭ��
                ,'0'                                                               AS SLFLAG                     --רҵ�����ʶ              (Ĭ��Ϊ��,1��0��)
                ,''                                                                AS SLTYPE                     --רҵ��������
                ,''                                                                AS PFPHASE                    --��Ŀ���ʽ׶�
                ,''                                                                AS REGURATING                 --�������
                ,'0'                                                               AS CBRCMPRATINGFLAG           --������϶������Ƿ��Ϊ����(Ĭ��Ϊ��,1��0��)
                ,'0'                                                               AS LARGEFLUCFLAG              --�Ƿ񲨶��Խϴ�            (Ĭ��Ϊ��,1��0��)
                ,'0'                                                               AS LIQUEXPOFLAG               --�Ƿ���������з��ձ�¶    (Ĭ��Ϊ��,1��0��)
                ,'0'                                                               AS PAYMENTDEALFLAG            --�Ƿ����Ը�ģʽ          (Ĭ��Ϊ��,1��0��)
                ,0                                                                 AS DELAYTRADINGDAYS           --�ӳٽ�������
                ,'0'                                                               AS SECURITIESFLAG             --�м�֤ȯ��ʶ              (Ĭ��Ϊ��,1��0��)
                ,''                                                                AS SECUISSUERID               --֤ȯ�����˴���
                ,''                                                                AS RATINGDURATIONTYPE         --������������
                ,''                                                                AS SECUISSUERATING            --֤ȯ���еȼ�
                ,NULL                                                              AS SECURESIDUALM              --֤ȯʣ������
                ,''                                                                AS SECUREVAFREQUENCY          --֤ȯ�ع�Ƶ��
                ,'0'                                                               AS CCPTRANFLAG                --�Ƿ����뽻�׶�����ؽ���  (Ĭ��Ϊ��,1��0��)
                ,''                                                                AS CCPID                      --���뽻�׶���ID
                ,'0'                                                               AS QUALCCPFLAG                --�Ƿ�ϸ����뽻�׶���      (Ĭ��Ϊ��,1��0��)
                ,''                                                                AS BANKROLE                   --���н�ɫ
                ,''                                                                AS CLEARINGMETHOD             --���㷽ʽ
                ,'0'                                                               AS BANKASSETFLAG              --�Ƿ������ύ�ʲ�          (Ĭ��Ϊ��,1��0��)
                ,''                                                                AS MATCHCONDITIONS            --�����������
                ,'0'                                                               AS SFTFLAG                    --֤ȯ���ʽ��ױ�ʶ          (Ĭ��Ϊ��,1��0��)
                ,'0'                                                               AS MASTERNETAGREEFLAG         --���������Э���ʶ        (Ĭ��Ϊ��,1��0��)
                ,''                                                                AS MASTERNETAGREEID           --���������Э�����
                ,''                                                                AS SFTTYPE                    --֤ȯ���ʽ�������
                ,'0'                                                               AS SECUOWNERTRANSFLAG         --֤ȯ����Ȩ�Ƿ�ת��        (Ĭ��Ϊ��,1��0��)
                ,'0'                                                               AS OTCFLAG                    --�����������߱�ʶ          (Ĭ��Ϊ��,1��0��)
                ,'0'                                                               AS VALIDNETTINGFLAG           --��Ч�������Э���ʶ      (Ĭ��Ϊ��,1��0��)
                ,''                                                                AS VALIDNETAGREEMENTID        --��Ч�������Э�����
                ,''                                                                AS OTCTYPE                    --����������������
                ,0                                                                 AS DEPOSITRISKPERIOD          --��֤������ڼ�
                ,0                                                                 AS MTM                        --���óɱ�
                ,''                                                                AS MTMCURRENCY                --���óɱ�����
                ,''                                                                AS BUYERORSELLER              --������
                ,'0'                                                               AS QUALROFLAG                 --�ϸ�����ʲ���ʶ          (Ĭ��Ϊ��,1��0��)
                ,'0'                                                               AS ROISSUERPERFORMFLAG        --�����ʲ��������Ƿ�����Լ  (Ĭ��Ϊ��,1��0��)
                ,'0'                                                               AS BUYERINSOLVENCYFLAG        --���ñ������Ƿ��Ʋ�      (Ĭ��Ϊ��,1��0��)
                ,''                                                                AS NONPAYMENTFEES             --��δ֧������
                ,RAP.RETAILFLAG                                                    AS RETAILEXPOFLAG             --���۱�¶��ʶ              (Ĭ��Ϊ��,1��0��)
                ,''                                                                AS RETAILCLAIMTYPE            --����ծȨ����
                ,''                                                                AS MORTGAGETYPE               --ס����Ѻ��������
                ,1                                                                 AS ExpoNumber                 --���ձ�¶����               Ĭ�� 1
                ,0.8                                                               AS LTV                        --�����ֵ��                  Ĭ�� 0.8
                ,NULL                                                              AS Aging                      --����                       Ĭ�� NULL
                ,''                                                                AS NewDefaultDebtFlag         --����ΥԼծ���ʶ           Ĭ�� NULL
                ,''                                                                AS PDPoolModelID              --PD�ֳ�ģ��ID               Ĭ�� NULL
                ,''                                                                AS LGDPoolModelID             --LGD�ֳ�ģ��ID              Ĭ�� NULL
                ,''                                                                AS CCFPoolModelID             --CCF�ֳ�ģ��ID              Ĭ�� NULL
                ,''                                                                AS PDPoolID                   --����PD��ID                  Ĭ�� NULL
                ,''                                                                AS LGDPoolID                  --����LGD��ID                 Ĭ�� NULL
                ,''                                                                AS CCFPoolID                  --����CCF��ID                 Ĭ�� NULL
                ,'0'                                                               AS ABSUAFlag                  --�ʲ�֤ȯ�������ʲ���ʶ      Ĭ�� ��(0)
                ,''                                                                AS ABSPoolID                  --֤ȯ���ʲ���ID             Ĭ�� NULL
                ,''                                                                AS GroupID                    --������                   Ĭ�� NULL
                ,NULL                                                              AS DefaultDate                --ΥԼʱ��
                ,NULL                                                              AS ABSPROPORTION              --�ʲ�֤ȯ������
                ,NULL                                                              AS DEBTORNUMBER               --����˸���
    FROM RWA_DEV.RWA_TMP_GLBALANCE GL
    LEFT JOIN RWA_DEV.RWA_ARTICULATION_PARAM RAP
      ON RAP.THIRD_SUBJECT_NO = GL.SUBJECT_NO
    LEFT JOIN RWA.ORG_INFO OI
      ON OI.ORGID = GL.ORGID
   WHERE GL.ACCOUNT_BALANCE <> 0
     AND GL.SUBJECT_NO NOT IN
         (SELECT DISTINCT SUBJECT_NO
            FROM RWA_DEV.RWA_TMP_DERIVATION_SUBJECT RTDS
            LEFT JOIN RWA_DEV.RWA_EI_EXPOSURE REE
              ON REE.DATADATE = V_DATADATE
             AND REE.ACCSUBJECT1 = RTDS.ARTICULATERELATION
           WHERE REE.ACCSUBJECT1 IS NOT NULL
             AND REE.SSYSID <> 'GC');
    COMMIT;

    /*�����Ŀ����*/
    INSERT INTO RWA_DEV.RWA_EI_EXPOSURE(
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
                V_DATADATE                                                         AS DATADATE                   --��������
                ,V_DATANO                                                          AS DATANO                     --������ˮ��
                ,'YPWF-' || V_DATANO                                               AS EXPOSUREID                 --���ձ�¶����
                ,'YPWF-' || V_DATANO                                               AS DUEID                      --ծ�����
                ,'XN'                                                              AS SSYSID                     --Դϵͳ����
                ,'YPWF-' || V_DATANO                                               AS CONTRACTID                 --��ͬ����
                ,'YPWF-' || V_DATANO                                               AS CLIENTID                   --�����������
                ,'9998'                                                        AS SORGID                     --Դ��������
                ,'��������'                                                        AS SORGNAME                   --Դ��������
                ,'1'                                                               AS ORGSORTNO                  --���������
                ,'9998'                                                        AS ORGID                      --������������
                 ,'��������'                                                       AS ORGNAME                    --������������
                ,'9998'                                                        AS ACCORGID                   --�����������
                 ,'��������'                                                       AS ACCORGNAME                 --�����������
                ,'999999'                                                          AS INDUSTRYID                 --������ҵ����
                ,'δ֪'                                                            AS INDUSTRYNAME               --������ҵ����
                ,'0501'                                                            AS BUSINESSLINE               --����
                ,'132'                                                             AS ASSETTYPE                  --�ʲ�����                  (���������ʲ�)
                ,'13205'                                                           AS ASSETSUBTYPE               --�ʲ�С��
                ,'109080'                                                          AS BUSINESSTYPEID             --ҵ��Ʒ�ִ���
                ,'�Ŵ�����δ��'                                                    AS BUSINESSTYPENAME           --ҵ��Ʒ������
                ,'01'                                                              AS CREDITRISKDATATYPE         --���÷�����������          (Ĭ��'һ�������',01һ�������,02һ������)
                ,'01'                                                              AS ASSETTYPEOFHAIRCUTS        --�ۿ�ϵ����Ӧ�ʲ����      (Ĭ��'�ֽ��ֽ�ȼ���',01�ֽ��ֽ�ȼ���)
                ,'07'                                                              AS BUSINESSTYPESTD            --Ȩ�ط�ҵ������            (Ĭ��Ϊ07һ���ʲ�)
                ,'0112'                                                            AS EXPOCLASSSTD               --Ȩ�ط���¶����            (Ĭ��Ϊ0112����)
                ,'011216'                                                          AS EXPOSUBCLASSSTD            --Ȩ�ط���¶С��            (Ĭ��Ϊ011216��������100%����Ȩ�ص��ʲ�)
                ,''                                                                AS EXPOCLASSIRB               --��������¶����            (Ĭ��Ϊ��)
                ,''                                                                AS EXPOSUBCLASSIRB            --��������¶С��            (Ĭ��Ϊ��)
                ,'02'                                                              AS EXPOBELONG                 --��¶������ʶ              (01����;02һ�����)
                ,'01'                                                              AS BOOKTYPE                   --�˻����                  (01�����˻�;02�����˻�)
                ,'03'                                                              AS REGUTRANTYPE               --��ܽ�������             (Ĭ��'��Ѻ����',01�ع�����,02�����ʱ��г�����,03��Ѻ����)
                ,'0'                                                               AS REPOTRANFLAG               --�ع����ױ�ʶ              (Ĭ��Ϊ��,1��0��)
                ,1                                                                 AS REVAFREQUENCY              --�ع�Ƶ��
                ,'CNY'                                                             AS CURRENCY                   --����                      (Ĭ��Ϊ01�����)
                ,T.BALANCE                                                         AS NORMALPRINCIPAL            --�����������
                ,0                                                                 AS OVERDUEBALANCE             --�������
                ,0                                                                 AS NONACCRUALBALANCE          --��Ӧ�����
                ,T.BALANCE                                                         AS ONSHEETBALANCE             --�������
                ,0                                                                 AS NORMALINTEREST             --������Ϣ
                ,0                                                                 AS ONDEBITINTEREST            --����ǷϢ
                ,0                                                                 AS OFFDEBITINTEREST           --����ǷϢ
                ,0                                                                 AS EXPENSERECEIVABLE          --Ӧ�շ���
                ,T.BALANCE                                                         AS ASSETBALANCE               --�ʲ����
                ,''                                                                AS ACCSUBJECT1                --��Ŀһ
                ,''                                                                AS ACCSUBJECT2                --��Ŀ��
                ,''                                                                AS ACCSUBJECT3                --��Ŀ��
                ,V_STARTDATE                                                       AS STARTDATE                  --��ʼ����
                ,TO_CHAR(ADD_MONTHS(V_DATADATE,1),'YYYY-MM-DD')                    AS DUEDATE                    --��������                  (�������� + 1����)
                ,(ADD_MONTHS(V_DATADATE,1) - V_DATADATE)/365                       AS ORIGINALMATURITY           --ԭʼ����                  (��λ��)
                ,(ADD_MONTHS(V_DATADATE,1) - V_DATADATE)/365                       AS RESIDUALM                  --ʣ������
                ,'01'                                                              AS RISKCLASSIFY               --���շ���
                ,''                                                                AS EXPOSURESTATUS             --���ձ�¶״̬               (Ĭ��Ϊ��)
                ,0                                                                 AS OVERDUEDAYS                --��������
                ,0                                                                 AS SPECIALPROVISION           --ר��׼����
                ,0                                                                 AS GENERALPROVISION           --һ��׼����
                ,0                                                                 AS ESPECIALPROVISION          --�ر�׼����
                ,0                                                                 AS WRITTENOFFAMOUNT           --�Ѻ������
                ,'01'                                                              AS OFFEXPOSOURCE              --���Ⱪ¶��Դ
                ,'02'                                                              AS OFFBUSINESSTYPE            --����ҵ������
                ,'0201'                                                            AS OFFBUSINESSSDVSSTD         --Ȩ�ط�����ҵ������ϸ��
                ,'1'                                                               AS UNCONDCANCELFLAG           --�Ƿ����ʱ����������      (Ĭ��Ϊ��,1��0��)
                ,''                                                                AS CCFLEVEL                   --����ת��ϵ������
                ,NULL                                                              AS CCFAIRB                    --�߼���������ת��ϵ��
                ,'01'                                                              AS CLAIMSLEVEL                --ծȨ����                  (Ĭ��Ϊ�߼�ծȨ;01�߼�ծȨ,02�μ�ծȨ)
                ,'0'                                                               AS BONDFLAG                   --�Ƿ�Ϊծȯ                (Ĭ��Ϊ��,1��0��)
                ,''                                                                AS BONDISSUEINTENT            --ծȯ����Ŀ��
                ,'0'                                                               AS NSUREALPROPERTYFLAG        --�Ƿ�����ò�����          (Ĭ��Ϊ��,1��0��)
                ,''                                                                AS REPASSETTERMTYPE           --��ծ�ʲ���������
                ,'0'                                                               AS DEPENDONFPOBFLAG           --�Ƿ�����������δ��ӯ��    (Ĭ��Ϊ��,1��0��)
                ,''                                                                AS IRATING                    --�ڲ�����
                ,NULL                                                              AS PD                         --ΥԼ����
                ,''                                                                AS LGDLEVEL                   --ΥԼ��ʧ�ʼ���
                ,NULL                                                              AS LGDAIRB                    --�߼�����ΥԼ��ʧ��
                ,NULL                                                              AS MAIRB                      --�߼�������Ч����
                ,NULL                                                              AS EADAIRB                    --�߼�����ΥԼ���ձ�¶
                ,'0'                                                               AS DEFAULTFLAG                --ΥԼ��ʶ                  (Ĭ��Ϊ��,1��0��)
                ,NULL                                                              AS BEEL                       --��ΥԼ��¶Ԥ����ʧ����
                ,NULL                                                              AS DEFAULTLGD                 --��ΥԼ��¶ΥԼ��ʧ��
                ,'0'                                                               AS EQUITYEXPOFLAG             --��Ȩ��¶��ʶ              (Ĭ��Ϊ��,1��0��)
                ,''                                                                AS EQUITYINVESTTYPE           --��ȨͶ�ʶ�������
                ,''                                                                AS EQUITYINVESTCAUSE          --��ȨͶ���γ�ԭ��
                ,'0'                                                               AS SLFLAG                     --רҵ�����ʶ              (Ĭ��Ϊ��,1��0��)
                ,''                                                                AS SLTYPE                     --רҵ��������
                ,''                                                                AS PFPHASE                    --��Ŀ���ʽ׶�
                ,''                                                                AS REGURATING                 --�������
                ,'0'                                                               AS CBRCMPRATINGFLAG           --������϶������Ƿ��Ϊ����(Ĭ��Ϊ��,1��0��)
                ,'0'                                                               AS LARGEFLUCFLAG              --�Ƿ񲨶��Խϴ�            (Ĭ��Ϊ��,1��0��)
                ,'0'                                                               AS LIQUEXPOFLAG               --�Ƿ���������з��ձ�¶    (Ĭ��Ϊ��,1��0��)
                ,'0'                                                               AS PAYMENTDEALFLAG            --�Ƿ����Ը�ģʽ          (Ĭ��Ϊ��,1��0��)
                ,0                                                                 AS DELAYTRADINGDAYS           --�ӳٽ�������
                ,'0'                                                               AS SECURITIESFLAG             --�м�֤ȯ��ʶ              (Ĭ��Ϊ��,1��0��)
                ,''                                                                AS SECUISSUERID               --֤ȯ�����˴���
                ,''                                                                AS RATINGDURATIONTYPE         --������������
                ,''                                                                AS SECUISSUERATING            --֤ȯ���еȼ�
                ,NULL                                                              AS SECURESIDUALM              --֤ȯʣ������
                ,''                                                                AS SECUREVAFREQUENCY          --֤ȯ�ع�Ƶ��
                ,'0'                                                               AS CCPTRANFLAG                --�Ƿ����뽻�׶�����ؽ���  (Ĭ��Ϊ��,1��0��)
                ,''                                                                AS CCPID                      --���뽻�׶���ID
                ,'0'                                                               AS QUALCCPFLAG                --�Ƿ�ϸ����뽻�׶���      (Ĭ��Ϊ��,1��0��)
                ,''                                                                AS BANKROLE                   --���н�ɫ
                ,''                                                                AS CLEARINGMETHOD             --���㷽ʽ
                ,'0'                                                               AS BANKASSETFLAG              --�Ƿ������ύ�ʲ�          (Ĭ��Ϊ��,1��0��)
                ,''                                                                AS MATCHCONDITIONS            --�����������
                ,'0'                                                               AS SFTFLAG                    --֤ȯ���ʽ��ױ�ʶ          (Ĭ��Ϊ��,1��0��)
                ,'0'                                                               AS MASTERNETAGREEFLAG         --���������Э���ʶ        (Ĭ��Ϊ��,1��0��)
                ,''                                                                AS MASTERNETAGREEID           --���������Э�����
                ,''                                                                AS SFTTYPE                    --֤ȯ���ʽ�������
                ,'0'                                                               AS SECUOWNERTRANSFLAG         --֤ȯ����Ȩ�Ƿ�ת��        (Ĭ��Ϊ��,1��0��)
                ,'0'                                                               AS OTCFLAG                    --�����������߱�ʶ          (Ĭ��Ϊ��,1��0��)
                ,'0'                                                               AS VALIDNETTINGFLAG           --��Ч�������Э���ʶ      (Ĭ��Ϊ��,1��0��)
                ,''                                                                AS VALIDNETAGREEMENTID        --��Ч�������Э�����
                ,''                                                                AS OTCTYPE                    --����������������
                ,0                                                                 AS DEPOSITRISKPERIOD          --��֤������ڼ�
                ,0                                                                 AS MTM                        --���óɱ�
                ,''                                                                AS MTMCURRENCY                --���óɱ�����
                ,''                                                                AS BUYERORSELLER              --������
                ,'0'                                                               AS QUALROFLAG                 --�ϸ�����ʲ���ʶ          (Ĭ��Ϊ��,1��0��)
                ,'0'                                                               AS ROISSUERPERFORMFLAG        --�����ʲ��������Ƿ�����Լ  (Ĭ��Ϊ��,1��0��)
                ,'0'                                                               AS BUYERINSOLVENCYFLAG        --���ñ������Ƿ��Ʋ�      (Ĭ��Ϊ��,1��0��)
                ,''                                                                AS NONPAYMENTFEES             --��δ֧������
                ,'0'                                                               AS RETAILEXPOFLAG             --���۱�¶��ʶ              (Ĭ��Ϊ��,1��0��)
                ,''                                                                AS RETAILCLAIMTYPE            --����ծȨ����
                ,''                                                                AS MORTGAGETYPE               --ס����Ѻ��������
                ,1                                                                 AS ExpoNumber                 --���ձ�¶����               Ĭ�� 1
                ,0.8                                                               AS LTV                        --�����ֵ��                  Ĭ�� 0.8
                ,NULL                                                              AS Aging                      --����                       Ĭ�� NULL
                ,''                                                                AS NewDefaultDebtFlag         --����ΥԼծ���ʶ           Ĭ�� NULL
                ,''                                                                AS PDPoolModelID              --PD�ֳ�ģ��ID               Ĭ�� NULL
                ,''                                                                AS LGDPoolModelID             --LGD�ֳ�ģ��ID              Ĭ�� NULL
                ,''                                                                AS CCFPoolModelID             --CCF�ֳ�ģ��ID              Ĭ�� NULL
                ,''                                                                AS PDPoolID                   --����PD��ID                  Ĭ�� NULL
                ,''                                                                AS LGDPoolID                  --����LGD��ID                 Ĭ�� NULL
                ,''                                                                AS CCFPoolID                  --����CCF��ID                 Ĭ�� NULL
                ,'0'                                                               AS ABSUAFlag                  --�ʲ�֤ȯ�������ʲ���ʶ      Ĭ�� ��(0)
                ,''                                                                AS ABSPoolID                  --֤ȯ���ʲ���ID             Ĭ�� NULL
                ,''                                                                AS GroupID                    --������                   Ĭ�� NULL
                ,NULL                                                              AS DefaultDate                --ΥԼʱ��
                ,NULL                                                              AS ABSPROPORTION              --�ʲ�֤ȯ������
                ,NULL                                                              AS DEBTORNUMBER               --����˸���
    FROM RWA.RWA_WS_XD_UNPUTOUT T
   WHERE T.DATADATE = V_DATADATE
   ;
    COMMIT;

   /*�����Ŀ����*/
    INSERT INTO RWA_DEV.RWA_EI_EXPOSURE(
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
                V_DATADATE                                                         AS DATADATE                   --��������
                ,V_DATANO                                                          AS DATANO                     --������ˮ��
                ,CASE WHEN T.WSTYPE = '01' THEN 'MDYP-' || V_DATANO
                      ELSE 'MDSP-'||V_DATANO END                                   AS EXPOSUREID                 --���ձ�¶����
                ,CASE WHEN T.WSTYPE = '01' THEN 'MDYP-' || V_DATANO
                      ELSE 'MDSP-'||V_DATANO END                                   AS DUEID                      --ծ�����
                ,'XN'                                                              AS SSYSID                     --Դϵͳ����
                ,CASE WHEN T.WSTYPE = '01' THEN 'MDYP-' || V_DATANO
                      ELSE 'MDSP-'||V_DATANO END                                   AS CONTRACTID                 --��ͬ����
                ,CASE WHEN T.WSTYPE = '01' THEN 'MDYP-' || V_DATANO
                      ELSE 'MDSP-'||V_DATANO END                                   AS CLIENTID                   --�����������
                ,'9998'                                                        AS SORGID                     --Դ��������
                ,'��������'                                                        AS SORGNAME                   --Դ��������
                ,'1'                                                               AS ORGSORTNO                  --���������
                ,'9998'                                                        AS ORGID                      --������������
                 ,'��������'                                                       AS ORGNAME                    --������������
                ,'9998'                                                        AS ACCORGID                   --�����������
                 ,'��������'                                                       AS ACCORGNAME                 --�����������
                ,'999999'                                                          AS INDUSTRYID                 --������ҵ����
                ,'δ֪'                                                            AS INDUSTRYNAME               --������ҵ����
                ,'0501'                                                            AS BUSINESSLINE               --����
                ,'210'                                                             AS ASSETTYPE                  --�ʲ�����                  (���������ʲ�)
                ,'21001'                                                           AS ASSETSUBTYPE               --�ʲ�С��
                ,'9010101010'                                                      AS BUSINESSTYPEID             --ҵ��Ʒ�ִ���
                ,'����ҵ��Ʒ��'                                                    AS BUSINESSTYPENAME           --ҵ��Ʒ������
                ,'01'                                                              AS CREDITRISKDATATYPE         --���÷�����������          (Ĭ��'һ�������',01һ�������,02һ������)
                ,'01'                                                              AS ASSETTYPEOFHAIRCUTS        --�ۿ�ϵ����Ӧ�ʲ����      (Ĭ��'�ֽ��ֽ�ȼ���',01�ֽ��ֽ�ȼ���)
                ,'07'                                                              AS BUSINESSTYPESTD            --Ȩ�ط�ҵ������            (Ĭ��Ϊ07һ���ʲ�)
                ,case when t.wstype = '01' then '0104' else '0106' end             AS EXPOCLASSSTD               --Ȩ�ط���¶����            (Ĭ��Ϊ0112����)
                ,case when t.wstype = '01' then '010406' else '010601' end         AS EXPOSUBCLASSSTD            --Ȩ�ط���¶С��            (Ĭ��Ϊ011216��������100%����Ȩ�ص��ʲ�)
                ,''                                                                AS EXPOCLASSIRB               --��������¶����            (Ĭ��Ϊ��)
                ,''                                                                AS EXPOSUBCLASSIRB            --��������¶С��            (Ĭ��Ϊ��)
                ,'02'                                                              AS EXPOBELONG                 --��¶������ʶ              (01����;02һ�����)
                ,'01'                                                              AS BOOKTYPE                   --�˻����                  (01�����˻�;02�����˻�)
                ,'03'                                                              AS REGUTRANTYPE               --��ܽ�������             (Ĭ��'��Ѻ����',01�ع�����,02�����ʱ��г�����,03��Ѻ����)
                ,'0'                                                               AS REPOTRANFLAG               --�ع����ױ�ʶ              (Ĭ��Ϊ��,1��0��)
                ,1                                                                 AS REVAFREQUENCY              --�ع�Ƶ��
                ,'CNY'                                                             AS CURRENCY                   --����                      (Ĭ��Ϊ01�����)
                ,T.BALANCE                                                         AS NORMALPRINCIPAL            --�����������
                ,0                                                                 AS OVERDUEBALANCE             --�������
                ,0                                                                 AS NONACCRUALBALANCE          --��Ӧ�����
                ,T.BALANCE                                                         AS ONSHEETBALANCE             --�������
                ,0                                                                 AS NORMALINTEREST             --������Ϣ
                ,0                                                                 AS ONDEBITINTEREST            --����ǷϢ
                ,0                                                                 AS OFFDEBITINTEREST           --����ǷϢ
                ,0                                                                 AS EXPENSERECEIVABLE          --Ӧ�շ���
                ,T.BALANCE                                                         AS ASSETBALANCE               --�ʲ����
                ,''                                                                AS ACCSUBJECT1                --��Ŀһ
                ,''                                                                AS ACCSUBJECT2                --��Ŀ��
                ,''                                                                AS ACCSUBJECT3                --��Ŀ��
                ,V_STARTDATE                                                       AS STARTDATE                  --��ʼ����
                ,TO_CHAR(ADD_MONTHS(V_DATADATE,4),'YYYY-MM-DD')                    AS DUEDATE                    --��������                  (�������� + 1����)
                ,(ADD_MONTHS(V_DATADATE,4) - V_DATADATE)/365                       AS ORIGINALMATURITY           --ԭʼ����                  (��λ��)
                ,(ADD_MONTHS(V_DATADATE,4) - V_DATADATE)/365                       AS RESIDUALM                  --ʣ������
                ,'01'                                                              AS RISKCLASSIFY               --���շ���
                ,''                                                                AS EXPOSURESTATUS             --���ձ�¶״̬               (Ĭ��Ϊ��)
                ,0                                                                 AS OVERDUEDAYS                --��������
                ,0                                                                 AS SPECIALPROVISION           --ר��׼����
                ,0                                                                 AS GENERALPROVISION           --һ��׼����
                ,0                                                                 AS ESPECIALPROVISION          --�ر�׼����
                ,0                                                                 AS WRITTENOFFAMOUNT           --�Ѻ������
                ,'01'                                                              AS OFFEXPOSOURCE              --���Ⱪ¶��Դ
                ,'10'                                                              AS OFFBUSINESSTYPE            --����ҵ������
                ,'1002'                                                            AS OFFBUSINESSSDVSSTD         --Ȩ�ط�����ҵ������ϸ��
                ,'1'                                                               AS UNCONDCANCELFLAG           --�Ƿ����ʱ����������      (Ĭ��Ϊ��,1��0��)
                ,''                                                                AS CCFLEVEL                   --����ת��ϵ������
                ,NULL                                                              AS CCFAIRB                    --�߼���������ת��ϵ��
                ,'01'                                                              AS CLAIMSLEVEL                --ծȨ����                  (Ĭ��Ϊ�߼�ծȨ;01�߼�ծȨ,02�μ�ծȨ)
                ,'0'                                                               AS BONDFLAG                   --�Ƿ�Ϊծȯ                (Ĭ��Ϊ��,1��0��)
                ,''                                                                AS BONDISSUEINTENT            --ծȯ����Ŀ��
                ,'0'                                                               AS NSUREALPROPERTYFLAG        --�Ƿ�����ò�����          (Ĭ��Ϊ��,1��0��)
                ,''                                                                AS REPASSETTERMTYPE           --��ծ�ʲ���������
                ,'0'                                                               AS DEPENDONFPOBFLAG           --�Ƿ�����������δ��ӯ��    (Ĭ��Ϊ��,1��0��)
                ,''                                                                AS IRATING                    --�ڲ�����
                ,NULL                                                              AS PD                         --ΥԼ����
                ,''                                                                AS LGDLEVEL                   --ΥԼ��ʧ�ʼ���
                ,NULL                                                              AS LGDAIRB                    --�߼�����ΥԼ��ʧ��
                ,NULL                                                              AS MAIRB                      --�߼�������Ч����
                ,NULL                                                              AS EADAIRB                    --�߼�����ΥԼ���ձ�¶
                ,'0'                                                               AS DEFAULTFLAG                --ΥԼ��ʶ                  (Ĭ��Ϊ��,1��0��)
                ,NULL                                                              AS BEEL                       --��ΥԼ��¶Ԥ����ʧ����
                ,NULL                                                              AS DEFAULTLGD                 --��ΥԼ��¶ΥԼ��ʧ��
                ,'0'                                                               AS EQUITYEXPOFLAG             --��Ȩ��¶��ʶ              (Ĭ��Ϊ��,1��0��)
                ,''                                                                AS EQUITYINVESTTYPE           --��ȨͶ�ʶ�������
                ,''                                                                AS EQUITYINVESTCAUSE          --��ȨͶ���γ�ԭ��
                ,'0'                                                               AS SLFLAG                     --רҵ�����ʶ              (Ĭ��Ϊ��,1��0��)
                ,''                                                                AS SLTYPE                     --רҵ��������
                ,''                                                                AS PFPHASE                    --��Ŀ���ʽ׶�
                ,''                                                                AS REGURATING                 --�������
                ,'0'                                                               AS CBRCMPRATINGFLAG           --������϶������Ƿ��Ϊ����(Ĭ��Ϊ��,1��0��)
                ,'0'                                                               AS LARGEFLUCFLAG              --�Ƿ񲨶��Խϴ�            (Ĭ��Ϊ��,1��0��)
                ,'0'                                                               AS LIQUEXPOFLAG               --�Ƿ���������з��ձ�¶    (Ĭ��Ϊ��,1��0��)
                ,'0'                                                               AS PAYMENTDEALFLAG            --�Ƿ����Ը�ģʽ          (Ĭ��Ϊ��,1��0��)
                ,0                                                                 AS DELAYTRADINGDAYS           --�ӳٽ�������
                ,'0'                                                               AS SECURITIESFLAG             --�м�֤ȯ��ʶ              (Ĭ��Ϊ��,1��0��)
                ,''                                                                AS SECUISSUERID               --֤ȯ�����˴���
                ,''                                                                AS RATINGDURATIONTYPE         --������������
                ,''                                                                AS SECUISSUERATING            --֤ȯ���еȼ�
                ,NULL                                                              AS SECURESIDUALM              --֤ȯʣ������
                ,''                                                                AS SECUREVAFREQUENCY          --֤ȯ�ع�Ƶ��
                ,'0'                                                               AS CCPTRANFLAG                --�Ƿ����뽻�׶�����ؽ���  (Ĭ��Ϊ��,1��0��)
                ,''                                                                AS CCPID                      --���뽻�׶���ID
                ,'0'                                                               AS QUALCCPFLAG                --�Ƿ�ϸ����뽻�׶���      (Ĭ��Ϊ��,1��0��)
                ,''                                                                AS BANKROLE                   --���н�ɫ
                ,''                                                                AS CLEARINGMETHOD             --���㷽ʽ
                ,'0'                                                               AS BANKASSETFLAG              --�Ƿ������ύ�ʲ�          (Ĭ��Ϊ��,1��0��)
                ,''                                                                AS MATCHCONDITIONS            --�����������
                ,'0'                                                               AS SFTFLAG                    --֤ȯ���ʽ��ױ�ʶ          (Ĭ��Ϊ��,1��0��)
                ,'0'                                                               AS MASTERNETAGREEFLAG         --���������Э���ʶ        (Ĭ��Ϊ��,1��0��)
                ,''                                                                AS MASTERNETAGREEID           --���������Э�����
                ,''                                                                AS SFTTYPE                    --֤ȯ���ʽ�������
                ,'0'                                                               AS SECUOWNERTRANSFLAG         --֤ȯ����Ȩ�Ƿ�ת��        (Ĭ��Ϊ��,1��0��)
                ,'0'                                                               AS OTCFLAG                    --�����������߱�ʶ          (Ĭ��Ϊ��,1��0��)
                ,'0'                                                               AS VALIDNETTINGFLAG           --��Ч�������Э���ʶ      (Ĭ��Ϊ��,1��0��)
                ,''                                                                AS VALIDNETAGREEMENTID        --��Ч�������Э�����
                ,''                                                                AS OTCTYPE                    --����������������
                ,0                                                                 AS DEPOSITRISKPERIOD          --��֤������ڼ�
                ,0                                                                 AS MTM                        --���óɱ�
                ,''                                                                AS MTMCURRENCY                --���óɱ�����
                ,''                                                                AS BUYERORSELLER              --������
                ,'0'                                                               AS QUALROFLAG                 --�ϸ�����ʲ���ʶ          (Ĭ��Ϊ��,1��0��)
                ,'0'                                                               AS ROISSUERPERFORMFLAG        --�����ʲ��������Ƿ�����Լ  (Ĭ��Ϊ��,1��0��)
                ,'0'                                                               AS BUYERINSOLVENCYFLAG        --���ñ������Ƿ��Ʋ�      (Ĭ��Ϊ��,1��0��)
                ,''                                                                AS NONPAYMENTFEES             --��δ֧������
                ,'0'                                                               AS RETAILEXPOFLAG             --���۱�¶��ʶ              (Ĭ��Ϊ��,1��0��)
                ,''                                                                AS RETAILCLAIMTYPE            --����ծȨ����
                ,''                                                                AS MORTGAGETYPE               --ס����Ѻ��������
                ,1                                                                 AS ExpoNumber                 --���ձ�¶����               Ĭ�� 1
                ,0.8                                                               AS LTV                        --�����ֵ��                  Ĭ�� 0.8
                ,NULL                                                              AS Aging                      --����                       Ĭ�� NULL
                ,''                                                                AS NewDefaultDebtFlag         --����ΥԼծ���ʶ           Ĭ�� NULL
                ,''                                                                AS PDPoolModelID              --PD�ֳ�ģ��ID               Ĭ�� NULL
                ,''                                                                AS LGDPoolModelID             --LGD�ֳ�ģ��ID              Ĭ�� NULL
                ,''                                                                AS CCFPoolModelID             --CCF�ֳ�ģ��ID              Ĭ�� NULL
                ,''                                                                AS PDPoolID                   --����PD��ID                  Ĭ�� NULL
                ,''                                                                AS LGDPoolID                  --����LGD��ID                 Ĭ�� NULL
                ,''                                                                AS CCFPoolID                  --����CCF��ID                 Ĭ�� NULL
                ,'0'                                                               AS ABSUAFlag                  --�ʲ�֤ȯ�������ʲ���ʶ      Ĭ�� ��(0)
                ,''                                                                AS ABSPoolID                  --֤ȯ���ʲ���ID             Ĭ�� NULL
                ,''                                                                AS GroupID                    --������                   Ĭ�� NULL
                ,NULL                                                              AS DefaultDate                --ΥԼʱ��
                ,NULL                                                              AS ABSPROPORTION              --�ʲ�֤ȯ������
                ,NULL                                                              AS DEBTORNUMBER               --����˸���
    FROM RWA.RWA_WS_SUPPLY T
   WHERE T.DATADATE = V_DATADATE
     AND T.BALANCE<>0
   ;
    COMMIT;

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
                ,MSMBFlag                   --���Ų�΢С��ҵ��ʶ
      )
      SELECT DISTINCT
                      REE.DATADATE                                 AS DATADATE,
                      REE.DATANO                                   AS DATANO,
                      REE.CLIENTID                                 AS CLIENTID,
                      ''                                           AS SOURCECLIENTID,
                      REE.SSYSID                                   AS SSYSID,
                      REE.CLIENTID || '-����ͻ�'                  AS CLIENTNAME,
                      '9998'                                   AS SORGID,
                      '��������'                                   AS SORGNAME,
                      '1'                                AS ORGSORTNO,
                      '9998'                                   AS ORGID,
                      '��������'                                   AS ORGNAME,
                      REE.INDUSTRYID                               AS INDUSTRYID,
                      REE.INDUSTRYNAME                             AS INDUSTRYNAME,
                      CASE WHEN REE.ACCSUBJECT1 LIKE '1003%' THEN '01'
                           WHEN REE.RETAILEXPOFLAG = '1' THEN '04'
                           WHEN REE.EXPOCLASSSTD = '0104' THEN '02'
                        ELSE  '03' END                             AS CLIENTTYPE,
                      CASE WHEN REE.ACCSUBJECT1 LIKE '1003%' THEN '0103'
                           WHEN REE.RETAILEXPOFLAG = '1' THEN '0401'
                           WHEN REE.EXPOSUBCLASSSTD = '010406' THEN '0202'
                        ELSE '0301' END                            AS CLIENTSUBTYPE,
                      '01'                                         AS REGISTSTATE,
                      '0124'                                       AS RCERATING,
                      ''                                           AS RCERAGENCY,
                      'XN' || REE.ACCSUBJECT1                      AS ORGANIZATIONCODE,
                      '0'                                          AS CONSOLIDATEDSCFLAG,
                      '0'                                          AS SLCLIENTFLAG,
                      NULL                                         AS SLCLIENTTYPE,
                      NULL                                         AS EXPOCATEGORYIRB,
                      NULL                                         AS MODELID,
                      NULL                                         AS MODELIRATING,
                      NULL                                         AS MODELPD,
                      NULL                                         AS IRATING,
                      NULL                                         AS PD,
                      '0'                                          AS DEFAULTFLAG,
                      '0'                                          AS NEWDEFAULTFLAG,
                      NULL                                         AS DEFAULTDATE,
                      NULL                                         AS CLIENTERATING,
                      '0'                                          AS CCPFLAG,
                      '0'                                          AS QUALCCPFLAG,
                      '0'                                          AS CLEARMEMBERFLAG,
                      NULL                                         AS COMPANYSIZE,
                      '0'                                          AS SSMBFLAG,
                      NULL                                         AS ANNUALSALE,
                      'CHN'                                        AS COUNTRYCODE,
                      ''                                           AS MSMBFLAG
        FROM RWA_DEV.RWA_EI_EXPOSURE REE
       WHERE REE.DATADATE = V_DATADATE
         AND REE.SSYSID = 'XN'
     ;
     COMMIT;

     INSERT INTO RWA_DEV.RWA_EI_CONTRACT (
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
                ,UncondCancelFlag                     --�Ƿ����ʱ����������
                ,ABSUAFlag                            --�ʲ�֤ȯ�������ʲ���ʶ
                ,ABSPoolID                            --֤ȯ���ʲ���ID
                ,GroupID                              --������
                ,GUARANTEETYPE                        --��Ҫ������ʽ
                ,ABSPROPORTION                        --�ʲ�֤ȯ������
                 )
        SELECT
                 REE.DATADATE                                                    AS DATADATE,
                 REE.DATANO                                                      AS DATANO,
                 REE.CONTRACTID                                                  AS CONTRACTID,
                 NULL                                                            AS SCONTRACTID,
                 REE.SSYSID                                                      AS SSYSID,
                 REE.CLIENTID                                                    AS CLIENTID,
                 REE.ORGID                                                       AS SORGID,
                 REE.ORGNAME                                                     AS SORGNAME,
                 REE.ORGSORTNO                                                   AS ORGSORTNO,
                 REE.ORGID                                                       AS ORGID,
                 REE.ORGNAME                                                     AS ORGNAME,
                 REE.INDUSTRYID                                                  AS INDUSTRYID,
                 REE.INDUSTRYNAME                                                AS INDUSTRYNAME,
                 REE.BUSINESSLINE                                                AS BUSINESSLINE,
                 REE.ASSETTYPE                                                   AS ASSETTYPE,
                 REE.ASSETSUBTYPE                                                AS ASSETSUBTYPE,
                 REE.BUSINESSTYPEID                                              AS BUSINESSTYPEID,
                 REE.BUSINESSTYPENAME                                            AS BUSINESSTYPENAME,
                 REE.CREDITRISKDATATYPE                                          AS CREDITRISKDATATYPE,
                 REE.STARTDATE                                                   AS STARTDATE,
                 REE.DUEDATE                                                     AS DUEDATE,
                 REE.ORIGINALMATURITY                                            AS ORIGINALMATURITY,
                 REE.RESIDUALM                                                   AS RESIDUALM,
                 REE.CURRENCY                                                    AS SETTLEMENTCURRENCY,
                 REE.NORMALPRINCIPAL                                             AS CONTRACTAMOUNT,
                 0                                                               AS NOTEXTRACTPART,
                 '0'                                                             AS UNCONDCANCELFLAG,
                 '0'                                                             AS ABSUAFLAG,
                 NULL                                                            AS ABSPOOLID,
                 NULL                                                            AS GROUPID,
                 NULL                                                            AS GUARANTEETYPE,
                 NULL                                                            AS ABSPROPORTION
           FROM RWA_DEV.RWA_EI_EXPOSURE REE
          WHERE REE.DATADATE = v_datadate
            AND REE.SSYSID = 'XN'
    ;
    COMMIT;

  ----------ֱ�����м���G4A-1A���� ����ID���弶�������  BY WZB

    UPDATE RWA_EI_EXPOSURE SET SSYSID='ZX'  WHERE DATANO=p_data_dt_str AND ACCSUBJECT1='13070800';
    COMMIT;


    /*Ŀ�������ͳ��*/
    --ͳ�Ʋ���ļ�¼
    SELECT COUNT(1) INTO V_COUNT
    FROM RWA_DEV.RWA_EI_EXPOSURE T1
    WHERE DATADATE = V_DATADATE
    AND SSYSID = 'XN' ;

    DBMS_OUTPUT.PUT_LINE('RWA_DEV.rwa_ei_exposure�����������������ݼ�¼Ϊ:' || V_COUNT || '��');
    DBMS_OUTPUT.PUT_LINE('��ִ�� ' || V_PRO_NAME || ' �洢���̽��� ��:' || TO_CHAR(SYSDATE,'YYYY-MM-DD HH24:MI:SS'));

    P_PO_RTNCODE := '1';
    P_PO_RTNMSG  := '�ɹ�-'||V_COUNT;
    --�����쳣
    EXCEPTION
    WHEN OTHERS THEN
          --DBMS_OUTPUT.PUT_LINE('������,�������Ϊ:'||SQLCODE||';������ϢΪ:'||SQLERRM||';��������Ϊ:'||DBMS_UTILITY.FORMAT_ERROR_BACKTRACE);
          ROLLBACK;
          P_PO_RTNCODE := SQLCODE;
          P_PO_RTNMSG  := '��������-���÷��ձ�¶(PRO_RWA_XN_EXPOSURE)ETLת��ʧ�ܣ�'|| SQLERRM||';��������Ϊ:'|| DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
         RETURN;

END PRO_RWA_XN_EXPOSURE;
/

