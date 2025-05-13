CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_XF_EXPOSURE(
                            p_data_dt_str  IN   VARCHAR2, --��������
                            p_po_rtncode   OUT  VARCHAR2, --���ر��
                            p_po_rtnmsg    OUT  VARCHAR2  --��������
)
  /*
    �洢��������:RWA_DEV.PRO_RWA_XF_EXPOSURE
    ʵ�ֹ���:���ѽ���-���ѽ���-���÷��ձ�¶(������Դ���ѽ���ϵͳ��ҵ�������Ϣȫ������RWA����ӿڱ���ձ�¶����)
    ���ݿھ�:ȫ��
    ����Ƶ��:�³�����
    ��  ��  :V1.0.0
    ��д��  :xlpang
    ��дʱ��:2019-05-28
    ��  λ  :�Ϻ���˶��Ϣ�����ɷ����޹�˾
    Դ  ��1 :RWA_DEV.RMPS_CQ_LOAN|�����Ϣ
    Դ  ��2 :RWA_DEV.RWA_CD_PAYTODW_ORG|����ͳһ��Ϣ
    Դ  ��3 :RWA_DEV.RMPS_CQ_CONTRACT|��ͬ��Ϣ
    Դ  ��4 :
    Դ  ��5 :

    Ŀ���1 :RWA_DEV.RWA_XF_EXPOSURE|RWA���÷��ձ�¶��Ϣ��
    ������  :��
    �����¼(�޸���|�޸�ʱ��|�޸�����):
    */

  AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_XF_EXPOSURE';
  --�����쳣����
  v_raise EXCEPTION;
  --���嵱ǰ����ļ�¼��
  v_count1 INTEGER;
  --v_count2 INTEGER;

  BEGIN

    --DBMS_OUTPUT.PUT_LINE('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    --���Ŀ����е�ԭ�м�¼
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_XF_EXPOSURE';

    
    INSERT INTO RWA_DEV.RWA_XF_EXPOSURE(
                DATADATE                        --��������
               ,DATANO                          --������ˮ��
               ,EXPOSUREID                      --���ձ�¶ID
               ,DUEID                           --ծ��ID
               ,SSYSID                          --ԴϵͳID
               ,CONTRACTID                      --��ͬID
               ,CLIENTID                        --��������ID
               ,SORGID                          --Դ����ID
               ,SORGNAME                        --Դ��������
               ,ORGSORTNO                       --�������������
               ,ORGID                           --��������ID
               ,ORGNAME                         --������������
               ,ACCORGID                        --�������ID
               ,ACCORGNAME                      --�����������
               ,INDUSTRYID                      --������ҵ����
               ,INDUSTRYNAME                    --������ҵ����
               ,BUSINESSLINE                    --ҵ������
               ,ASSETTYPE                       --�ʲ�����
               ,ASSETSUBTYPE                    --�ʲ�С��
               ,BUSINESSTYPEID                  --ҵ��Ʒ�ִ���
               ,BUSINESSTYPENAME                --ҵ��Ʒ������
               ,CREDITRISKDATATYPE              --���÷�����������
               ,ASSETTYPEOFHAIRCUTS             --�ۿ�ϵ����Ӧ�ʲ����
               ,BUSINESSTYPESTD                 --Ȩ�ط�ҵ������
               ,EXPOCLASSSTD                    --Ȩ�ط���¶����
               ,EXPOSUBCLASSSTD                 --Ȩ�ط���¶С��
               ,EXPOCLASSIRB                    --��������¶����
               ,EXPOSUBCLASSIRB                 --��������¶С��
               ,EXPOBELONG                      --��¶������ʶ
               ,BOOKTYPE                        --�˻����
               ,REGUTRANTYPE                    --��ܽ�������
               ,REPOTRANFLAG                    --�ع����ױ�ʶ
               ,REVAFREQUENCY                   --�ع�Ƶ��
               ,CURRENCY                        --����
               ,NORMALPRINCIPAL                 --�����������
               ,OVERDUEBALANCE                  --�������
               ,NONACCRUALBALANCE               --��Ӧ�����
               ,ONSHEETBALANCE                  --�������
               ,NORMALINTEREST                  --������Ϣ
               ,ONDEBITINTEREST                 --����ǷϢ
               ,OFFDEBITINTEREST                --����ǷϢ
               ,EXPENSERECEIVABLE               --Ӧ�շ���
               ,ASSETBALANCE                    --�ʲ����
               ,ACCSUBJECT1                     --��Ŀһ
               ,ACCSUBJECT2                     --��Ŀ��
               ,ACCSUBJECT3                     --��Ŀ��
               ,STARTDATE                       --��ʼ����
               ,DUEDATE                         --��������
               ,ORIGINALMATURITY                --ԭʼ����
               ,RESIDUALM                       --ʣ������
               ,RISKCLASSIFY                    --���շ���
               ,EXPOSURESTATUS                  --���ձ�¶״̬
               ,OVERDUEDAYS                     --��������
               ,SPECIALPROVISION                --ר��׼����
               ,GENERALPROVISION                --һ��׼����
               ,ESPECIALPROVISION               --�ر�׼����
               ,WRITTENOFFAMOUNT                --�Ѻ������
               ,OFFEXPOSOURCE                   --���Ⱪ¶��Դ
               ,OFFBUSINESSTYPE                 --����ҵ������
               ,OFFBUSINESSSDVSSTD              --Ȩ�ط�����ҵ������ϸ��
               ,UNCONDCANCELFLAG                --�Ƿ����ʱ����������
               ,CCFLEVEL                        --����ת��ϵ������
               ,CCFAIRB                         --�߼�������ת��ϵ��
               ,CLAIMSLEVEL                     --ծȨ����
               ,BONDFLAG                        --�Ƿ�Ϊծȯ
               ,BONDISSUEINTENT                 --ծȯ����Ŀ��
               ,NSUREALPROPERTYFLAG             --�Ƿ�����ò�����
               ,REPASSETTERMTYPE                --��ծ�ʲ���������
               ,DEPENDONFPOBFLAG                --�Ƿ�����������δ��ӯ��
               ,IRATING                         --�ڲ�����
               ,PD                              --ΥԼ����
               ,LGDLEVEL                        --ΥԼ��ʧ�ʼ���
               ,LGDAIRB                         --�߼���ΥԼ��ʧ��
               ,MAIRB                           --�߼�����Ч����
               ,EADAIRB                         --�߼���ΥԼ���ձ�¶
               ,DEFAULTFLAG                     --ΥԼ��ʶ
               ,BEEL                            --��ΥԼ��¶Ԥ����ʧ����
               ,DEFAULTLGD                      --��ΥԼ��¶ΥԼ��ʧ��
               ,EQUITYEXPOFLAG                  --��Ȩ��¶��ʶ
               ,EQUITYINVESTTYPE                --��ȨͶ�ʶ�������
               ,EQUITYINVESTCAUSE               --��ȨͶ���γ�ԭ��
               ,SLFLAG                          --רҵ�����ʶ
               ,SLTYPE                          --רҵ��������
               ,PFPHASE                         --��Ŀ���ʽ׶�
               ,REGURATING                      --�������
               ,CBRCMPRATINGFLAG                --������϶������Ƿ��Ϊ����
               ,LARGEFLUCFLAG                   --�Ƿ񲨶��Խϴ�
               ,LIQUEXPOFLAG                    --�Ƿ���������з��ձ�¶
               ,PAYMENTDEALFLAG                 --�Ƿ����Ը�ģʽ
               ,DELAYTRADINGDAYS                --�ӳٽ�������
               ,SECURITIESFLAG                  --�м�֤ȯ��ʶ
               ,SECUISSUERID                    --֤ȯ������ID
               ,RATINGDURATIONTYPE              --������������
               ,SECUISSUERATING                 --֤ȯ���еȼ�
               ,SECURESIDUALM                   --֤ȯʣ������
               ,SECUREVAFREQUENCY               --֤ȯ�ع�Ƶ��
               ,CCPTRANFLAG                     --�Ƿ����뽻�׶�����ؽ���
               ,CCPID                           --���뽻�׶���ID
               ,QUALCCPFLAG                     --�Ƿ�ϸ����뽻�׶���
               ,BANKROLE                        --���н�ɫ
               ,CLEARINGMETHOD                  --���㷽ʽ
               ,BANKASSETFLAG                   --�Ƿ������ύ�ʲ�
               ,MATCHCONDITIONS                 --�����������
               ,SFTFLAG                         --֤ȯ���ʽ��ױ�ʶ
               ,MASTERNETAGREEFLAG              --���������Э���ʶ
               ,MASTERNETAGREEID                --���������Э��ID
               ,SFTTYPE                         --֤ȯ���ʽ�������
               ,SECUOWNERTRANSFLAG              --֤ȯ����Ȩ�Ƿ�ת��
               ,OTCFLAG                         --�����������߱�ʶ
               ,VALIDNETTINGFLAG                --��Ч�������Э���ʶ
               ,VALIDNETAGREEMENTID             --��Ч�������Э��ID
               ,OTCTYPE                         --����������������
               ,DEPOSITRISKPERIOD               --��֤������ڼ�
               ,MTM                             --���óɱ�
               ,MTMCURRENCY                     --���óɱ�����
               ,BUYERORSELLER                   --������
               ,QUALROFLAG                      --�ϸ�����ʲ���ʶ
               ,ROISSUERPERFORMFLAG             --�����ʲ��������Ƿ�����Լ
               ,BUYERINSOLVENCYFLAG             --���ñ������Ƿ��Ʋ�
               ,NONPAYMENTFEES                  --��δ֧������
               ,RETAILEXPOFLAG                  --���۱�¶��ʶ
               ,RETAILCLAIMTYPE                 --����ծȨ����
               ,MORTGAGETYPE                    --ס����Ѻ��������
               ,EXPONUMBER                      --���ձ�¶����
               ,LTV                             --�����ֵ��
               ,AGING                           --����
               ,NEWDEFAULTDEBTFLAG              --����ΥԼծ���ʶ
               ,PDPOOLMODELID                   --PD�ֳ�ģ��ID
               ,LGDPOOLMODELID                  --LGD�ֳ�ģ��ID
               ,CCFPOOLMODELID                  --CCF�ֳ�ģ��ID
               ,PDPOOLID                        --����PD��ID
               ,LGDPOOLID                       --����LGD��ID
               ,CCFPOOLID                       --����CCF��ID
               ,ABSUAFLAG                       --�ʲ�֤ȯ�������ʲ���ʶ
               ,ABSPOOLID                       --֤ȯ���ʲ���ID
               ,GROUPID                         --������
    )
    SELECT TO_DATE(T1.DATANO, 'YYYYMMDD'), --  ��������
           T1.DATANO, --  ������ˮ��
           T1.LOAN_ID, --  ���ձ�¶ID
           T1.LOAN_ID, --  ծ��ID
           'XJ', --  ԴϵͳID
           T1.CONTRACT_NBR, --  ��ͬID
           T1.CUST_ID, --  ��������ID
           T1.CORE_ACCT_ORG, --  Դ����ID
           T2.ORGNAME, --  Դ��������
           T2.SORTNO, --  �������������
           T1.CORE_ACCT_ORG, --  ��������ID
           T2.ORGNAME, --  ������������
           T1.CORE_ACCT_ORG, --  �������ID
           T2.ORGNAME, --  �����������
           NULL, --  ������ҵ����
           NULL, --  ������ҵ����
           '0301', --  ҵ������ Ĭ�� ����
           NULL, --  �ʲ�����
           NULL, --  �ʲ�С��
           '11103038', --  ҵ��Ʒ�ִ���
           '��e��', --  ҵ��Ʒ������
           '02', --  ���÷�����������
           '01', --  �ۿ�ϵ����Ӧ�ʲ����
           NULL, --  Ȩ�ط�ҵ������
           NULL, --  Ȩ�ط���¶����
           NULL, --  Ȩ�ط���¶С��
           NULL, --  ��������¶����
           NULL, --  ��������¶С��
           '01', --  ��¶������ʶ
           '01', --  �˻����
           '03', --  ��ܽ�������
           '0', --  �ع����ױ�ʶ
           1, --  �ع�Ƶ��
           T1.CURRENCY, --  ����
           T1.PRIN_BAL, --  �����������
           0, --  �������
           0, --  ��Ӧ�����
           T1.PRIN_BAL, --  �������
           0, --  ������Ϣ
           T1.IN_INTEREST_BAL, --  ����ǷϢ
           T1.OUT_INTEREST_BAL, --  ����ǷϢ
           0, --  Ӧ�շ���
           T1.PRIN_BAL, --  �ʲ����
           T1.AccSubject, --  ��Ŀһ
           NULL, --  ��Ŀ��
           NULL, --  ��Ŀ��
           T1.PAY_FINISH_DATE, --  ��ʼ����
           T1.DD_EXPIR_DAY, --  ��������
           CASE
             WHEN (TO_DATE(T1.DD_EXPIR_DAY, 'YYYYMMDD') -
                  TO_DATE(T1.PAY_FINISH_DATE, 'YYYYMMDD')) / 365 < 0 THEN
              0
             ELSE
              (TO_DATE(T1.DD_EXPIR_DAY, 'YYYYMMDD') -
              TO_DATE(T1.PAY_FINISH_DATE, 'YYYYMMDD')) / 365
           END AS ORIGINALMATURITY, -- ԭʼ����
           CASE
             WHEN (TO_DATE(T1.DD_EXPIR_DAY, 'YYYYMMDD') -
                  TO_DATE(T1.DATANO, 'YYYYMMDD')) / 365 < 0 THEN
              0
             ELSE
              (TO_DATE(T1.DD_EXPIR_DAY, 'YYYYMMDD') -
              TO_DATE(T1.DATANO, 'YYYYMMDD')) / 365
           END AS RESIDUALM, -- ʣ������
           CASE
             WHEN T1.FIVE_CLASS = 'A' THEN
              '01' --����
             WHEN T1.FIVE_CLASS = 'B' THEN
              '02' --��ע
             WHEN T1.FIVE_CLASS = 'C' THEN
              '03' --�μ�
             WHEN T1.FIVE_CLASS = 'D' THEN
              '04' --����
             WHEN T1.FIVE_CLASS = 'E' THEN
              '05' --��ʧ��
             ELSE
              '@' || T1.FIVE_CLASS
           END, --  ���շ���
           '01', --  ���ձ�¶״̬ Ĭ������
           T1.OVERDUE_DAYS, --  ��������
           0, --  ר��׼����
           0, --  һ��׼����
           0, --  �ر�׼����
           0, --  �Ѻ������
           NULL, --  ���Ⱪ¶��Դ
           NULL, --  ����ҵ������
           NULL, --  Ȩ�ط�����ҵ������ϸ��
           '1', --  �Ƿ����ʱ����������
           NULL, --  ����ת��ϵ������
           0, --  �߼�������ת��ϵ��
           '01', --  ծȨ���� Ĭ�� �߼�ծ
           '0', --  �Ƿ�Ϊծȯ
           NULL, --  ծȯ����Ŀ��
           '0', --  �Ƿ�����ò�����
           NULL, --  ��ծ�ʲ���������
           '0', --  �Ƿ�����������δ��ӯ��
           NULL, --  �ڲ�����
           NULL, --  ΥԼ����
           NULL, --  ΥԼ��ʧ�ʼ���
           NULL, --  �߼���ΥԼ��ʧ��
           NULL, --  �߼�����Ч����
           T1.PRIN_BAL, --  �߼���ΥԼ���ձ�¶
           NULL, --  ΥԼ��ʶ
           0.45, --  ��ΥԼ��¶Ԥ����ʧ����
           0.45, --  ��ΥԼ��¶ΥԼ��ʧ��
           '0', --  ��Ȩ��¶��ʶ
           NULL, --  ��ȨͶ�ʶ�������
           NULL, --  ��ȨͶ���γ�ԭ��
           '0', --  רҵ�����ʶ
           NULL, --  רҵ��������
           NULL, --  ��Ŀ���ʽ׶�
           NULL, --  �������
           '0', --  ������϶������Ƿ��Ϊ����
           '0', --  �Ƿ񲨶��Խϴ�
           '0', --  �Ƿ���������з��ձ�¶
           '0', --  �Ƿ����Ը�ģʽ
           NULL, --  �ӳٽ�������
           '0', --  �м�֤ȯ��ʶ
           NULL, --  ֤ȯ������ID
           NULL, --  ������������
           NULL, --  ֤ȯ���еȼ�
           NULL, --  ֤ȯʣ������
           NULL, --  ֤ȯ�ع�Ƶ��
           '0', --  �Ƿ����뽻�׶�����ؽ���
           NULL, --  ���뽻�׶���ID
           NULL, --  �Ƿ�ϸ����뽻�׶���
           NULL, --  ���н�ɫ
           NULL, --  ���㷽ʽ
           NULL, --  �Ƿ������ύ�ʲ�
           NULL, --  �����������
           '0', --  ֤ȯ���ʽ��ױ�ʶ
           NULL, --  ���������Э���ʶ
           NULL, --  ���������Э��ID
           NULL, --  ֤ȯ���ʽ�������
           '0', --  ֤ȯ����Ȩ�Ƿ�ת��
           NULL, --  �����������߱�ʶ
           NULL, --  ��Ч�������Э���ʶ
           NULL, --  ��Ч�������Э��ID
           NULL, --  ����������������
           NULL, --  ��֤������ڼ�
           NULL, --  ���óɱ�
           NULL, --  ���óɱ�����
           NULL, --  ������
           '0', --  �ϸ�����ʲ���ʶ
           '0', --  �����ʲ��������Ƿ�����Լ
           '0', --  ���ñ������Ƿ��Ʋ�
           NULL, --  ��δ֧������
           '1', --  ���۱�¶��ʶ
           '020403', --  ����ծȨ����
           NULL, --  ס����Ѻ��������
           1, --  ���ձ�¶����
           NULL, --  �����ֵ��
           NULL, --  ����
           NULL, --  ����ΥԼծ���ʶ
           NULL, --  PD�ֳ�ģ��ID
           NULL, --  LGD�ֳ�ģ��ID
           NULL, --  CCF�ֳ�ģ��ID
           NULL, --  ����PD��ID
           NULL, --  ����LGD��ID
           NULL, --  ����CCF��ID
           NULL, --  �ʲ�֤ȯ�������ʲ���ʶ
           NULL, --  ֤ȯ���ʲ���ID
           NULL --  ������
      FROM RWA_DEV.RMPS_CQ_LOAN T1 --�����Ϣ
      LEFT JOIN RWA.ORG_INFO T2
        ON T1.CORE_ACCT_ORG = T2.ORGID
     WHERE T1.DATANO = p_data_dt_str --�ս����ڲ�Ϊ��
       AND T1.PRIN_BAL <> 0 --������������
       AND T1.TERMIN_DATE IS NOT NULL; --����ս����ڲ�Ϊ��

     COMMIT;
    
    

    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_XF_EXPOSURE',cascade => true);


    /*Ŀ�������ͳ��*/
    --ͳ�Ʋ���ļ�¼
    SELECT COUNT(1) INTO v_count1 FROM RWA_DEV.RWA_XF_EXPOSURE;

    --DBMS_OUTPUT.PUT_LINE('��ִ�� ' || v_pro_name || ' �洢���̽��� ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));
    p_po_rtncode := '1';
    p_po_rtnmsg  := '�ɹ�' || '-' || v_count1;

    --�����쳣
    EXCEPTION WHEN OTHERS THEN
        --DBMS_OUTPUT.PUT_LINE('������,�������Ϊ:'||sqlcode||';������ϢΪ:'||sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace);
        ROLLBACK;
        p_po_rtncode := sqlcode;
        p_po_rtnmsg  := '���ѽ���ϵͳ���÷��ձ�¶('|| v_pro_name ||')ETLת��ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;

      RETURN;
END PRO_RWA_XF_EXPOSURE;
/

