CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_LC_EXPOSURE(P_DATA_DT_STR IN VARCHAR2, --��������
                                                P_PO_RTNCODE  OUT VARCHAR2, --���ر��
                                                P_PO_RTNMSG   OUT VARCHAR2 --��������
                                                )
/*
  �洢��������:RWA_DEV.PRO_RWA_LC_EXPOSURE
  ʵ�ֹ���:���ϵͳ-���Ͷ��-���÷��ձ�¶(������Դ���ϵͳ��ҵ�������Ϣȫ������RWA���Ͷ�ʽӿڱ���ձ�¶����)
  ���ݿھ�:ȫ��
  ����Ƶ��:�³�����
  ��  ��  :V1.0.0
  ��д��  :QHJIANG
  ��дʱ��:2016-04-14
  ��  λ  :�Ϻ���˶��Ϣ�����ɷ����޹�˾
  Դ  ��1 :RWA_DEV.ZGS_INVESTASSETDETAIL|�ʲ������
  Դ  ��2 :RWA_DEV.ZGS_FINANCING_INFO|��Ʒ��Ϣ��
  Դ  ��3 :RWA_DEV.ZGS_ATBOND|ծȯ��Ϣ��
  Դ  ��4 :RWA_DEV.ZGS_ATINTRUST_PLAN|�ʲ�����ƻ���
  Դ  ��5 :RWA.CODE_LIBRARY|RWA�����
  --Դ ��6 :RWA.RWA_WS_FCII_BOND|ծȯ���Ͷ�ʲ�¼�� ����
  --Դ ��7 :RWA.RWA_WS_FCII_PLAN|�ʹܼƻ����Ͷ�ʲ�¼�� ����
  --Դ  ��8 :RWA.RWA_WP_DATASUPPLEMENT|��¼������ϸ��Ϣ�� ����
  Ŀ���1 :RWA_DEV.RWA_LC_EXPOSURE|RWA���÷��ձ�¶��Ϣ��
  ������  :��
  �����¼(�޸���|�޸�ʱ��|�޸�����):
  */

 AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  V_PRO_NAME VARCHAR2(200) := 'RWA_DEV.PRO_RWA_LC_EXPOSURE';
  --�����쳣����
  V_RAISE EXCEPTION;
  --���嵱ǰ����ļ�¼��
  V_COUNT1 INTEGER;
  --v_count2 INTEGER;

BEGIN

  --DBMS_OUTPUT.PUT_LINE('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

  --���Ŀ����е�ԭ�м�¼
  EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_LC_EXPOSURE';

  --15030201 ֱ�Ӵ�����ȡ
  INSERT INTO RWA_DEV.RWA_LC_EXPOSURE
    (DATADATE, --��������
     DATANO, --������ˮ��
     EXPOSUREID, --���ձ�¶ID
     DUEID, --ծ��ID
     SSYSID, --ԴϵͳID
     CONTRACTID, --��ͬID
     CLIENTID, --��������ID
     SORGID, --Դ����ID
     SORGNAME, --Դ��������
     ORGSORTNO, --�������������
     ORGID, --��������ID
     ORGNAME, --������������
     ACCORGID, --�������ID
     ACCORGNAME, --�����������
     INDUSTRYID, --������ҵ����
     INDUSTRYNAME, --������ҵ����
     BUSINESSLINE, --ҵ������
     ASSETTYPE, --�ʲ�����
     ASSETSUBTYPE, --�ʲ�С��
     BUSINESSTYPEID, --ҵ��Ʒ�ִ���
     BUSINESSTYPENAME, --ҵ��Ʒ������
     CREDITRISKDATATYPE, --���÷�����������
     ASSETTYPEOFHAIRCUTS, --�ۿ�ϵ����Ӧ�ʲ����
     BUSINESSTYPESTD, --Ȩ�ط�ҵ������
     EXPOCLASSSTD, --Ȩ�ط���¶����
     EXPOSUBCLASSSTD, --Ȩ�ط���¶С��
     EXPOCLASSIRB, --��������¶����
     EXPOSUBCLASSIRB, --��������¶С��
     EXPOBELONG, --��¶������ʶ
     BOOKTYPE, --�˻����
     REGUTRANTYPE, --��ܽ�������
     REPOTRANFLAG, --�ع����ױ�ʶ
     REVAFREQUENCY, --�ع�Ƶ��
     CURRENCY, --����
     NORMALPRINCIPAL, --�����������
     OVERDUEBALANCE, --�������
     NONACCRUALBALANCE, --��Ӧ�����
     ONSHEETBALANCE, --�������
     NORMALINTEREST, --������Ϣ
     ONDEBITINTEREST, --����ǷϢ
     OFFDEBITINTEREST, --����ǷϢ
     EXPENSERECEIVABLE, --Ӧ�շ���
     ASSETBALANCE, --�ʲ����
     ACCSUBJECT1, --��Ŀһ
     ACCSUBJECT2, --��Ŀ��
     ACCSUBJECT3, --��Ŀ��
     STARTDATE, --��ʼ����
     DUEDATE, --��������
     ORIGINALMATURITY, --ԭʼ����
     RESIDUALM, --ʣ������
     RISKCLASSIFY, --���շ���
     EXPOSURESTATUS, --���ձ�¶״̬
     OVERDUEDAYS, --��������
     SPECIALPROVISION, --ר��׼����
     GENERALPROVISION, --һ��׼����
     ESPECIALPROVISION, --�ر�׼����
     WRITTENOFFAMOUNT, --�Ѻ������
     OFFEXPOSOURCE, --���Ⱪ¶��Դ
     OFFBUSINESSTYPE, --����ҵ������
     OFFBUSINESSSDVSSTD, --Ȩ�ط�����ҵ������ϸ��
     UNCONDCANCELFLAG, --�Ƿ����ʱ����������
     CCFLEVEL, --����ת��ϵ������
     CCFAIRB, --�߼�������ת��ϵ��
     CLAIMSLEVEL, --ծȨ����
     BONDFLAG, --�Ƿ�Ϊծȯ
     BONDISSUEINTENT, --ծȯ����Ŀ��
     NSUREALPROPERTYFLAG, --�Ƿ�����ò�����
     REPASSETTERMTYPE, --��ծ�ʲ���������
     DEPENDONFPOBFLAG, --�Ƿ�����������δ��ӯ��
     IRATING, --�ڲ�����
     PD, --ΥԼ����
     LGDLEVEL, --ΥԼ��ʧ�ʼ���
     LGDAIRB, --�߼���ΥԼ��ʧ��
     MAIRB, --�߼�����Ч����
     EADAIRB, --�߼���ΥԼ���ձ�¶
     DEFAULTFLAG, --ΥԼ��ʶ
     BEEL, --��ΥԼ��¶Ԥ����ʧ����
     DEFAULTLGD, --��ΥԼ��¶ΥԼ��ʧ��
     EQUITYEXPOFLAG, --��Ȩ��¶��ʶ
     EQUITYINVESTTYPE, --��ȨͶ�ʶ�������
     EQUITYINVESTCAUSE, --��ȨͶ���γ�ԭ��
     SLFLAG, --רҵ�����ʶ
     SLTYPE, --רҵ��������
     PFPHASE, --��Ŀ���ʽ׶�
     REGURATING, --�������
     CBRCMPRATINGFLAG, --������϶������Ƿ��Ϊ����
     LARGEFLUCFLAG, --�Ƿ񲨶��Խϴ�
     LIQUEXPOFLAG, --�Ƿ���������з��ձ�¶
     PAYMENTDEALFLAG, --�Ƿ����Ը�ģʽ
     DELAYTRADINGDAYS, --�ӳٽ�������
     SECURITIESFLAG, --�м�֤ȯ��ʶ
     SECUISSUERID, --֤ȯ������ID
     RATINGDURATIONTYPE, --������������
     SECUISSUERATING, --֤ȯ���еȼ�
     SECURESIDUALM, --֤ȯʣ������
     SECUREVAFREQUENCY, --֤ȯ�ع�Ƶ��
     CCPTRANFLAG, --�Ƿ����뽻�׶�����ؽ���
     CCPID, --���뽻�׶���ID
     QUALCCPFLAG, --�Ƿ�ϸ����뽻�׶���
     BANKROLE, --���н�ɫ
     CLEARINGMETHOD, --���㷽ʽ
     BANKASSETFLAG, --�Ƿ������ύ�ʲ�
     MATCHCONDITIONS, --�����������
     SFTFLAG, --֤ȯ���ʽ��ױ�ʶ
     MASTERNETAGREEFLAG, --���������Э���ʶ
     MASTERNETAGREEID, --���������Э��ID
     SFTTYPE, --֤ȯ���ʽ�������
     SECUOWNERTRANSFLAG, --֤ȯ����Ȩ�Ƿ�ת��
     OTCFLAG, --�����������߱�ʶ
     VALIDNETTINGFLAG, --��Ч�������Э���ʶ
     VALIDNETAGREEMENTID, --��Ч�������Э��ID
     OTCTYPE, --����������������
     DEPOSITRISKPERIOD, --��֤������ڼ�
     MTM, --���óɱ�
     MTMCURRENCY, --���óɱ�����
     BUYERORSELLER, --������
     QUALROFLAG, --�ϸ�����ʲ���ʶ
     ROISSUERPERFORMFLAG, --�����ʲ��������Ƿ�����Լ
     BUYERINSOLVENCYFLAG, --���ñ������Ƿ��Ʋ�
     NONPAYMENTFEES, --��δ֧������
     RETAILEXPOFLAG, --���۱�¶��ʶ
     RETAILCLAIMTYPE, --����ծȨ����
     MORTGAGETYPE, --ס����Ѻ��������
     EXPONUMBER, --���ձ�¶����
     LTV, --�����ֵ��
     AGING, --����
     NEWDEFAULTDEBTFLAG, --����ΥԼծ���ʶ
     PDPOOLMODELID, --PD�ֳ�ģ��ID
     LGDPOOLMODELID, --LGD�ֳ�ģ��ID
     CCFPOOLMODELID, --CCF�ֳ�ģ��ID
     PDPOOLID, --����PD��ID
     LGDPOOLID, --����LGD��ID
     CCFPOOLID, --����CCF��ID
     ABSUAFLAG, --�ʲ�֤ȯ�������ʲ���ʶ
     ABSPOOLID, --֤ȯ���ʲ���ID
     GROUPID, --������
     DEFAULTDATE, --ΥԼʱ��
     ABSPROPORTION, --�ʲ�֤ȯ������
     DEBTORNUMBER, --����˸���
     SBJT4,
     SBJT_VAL4)
  
    SELECT TO_DATE(P_DATA_DT_STR, 'YYYYMMDD') AS DATADATE, --��������
           P_DATA_DT_STR AS DATANO, --������ˮ��
           'XN-15030201' AS EXPOSUREID, --���ձ�¶ID
           'XN-15030201' AS DUEID, --ծ��ID
           'LC' AS SSYSID, --ԴϵͳID
           'XN-15030201' AS CONTRACTID, --��ͬID
           'XN-ZGSYYH' AS CLIENTID, --��������ID
           '9998' AS SORGID, --Դ����ID  Ĭ�� �����ʲ�����(01160000)
           '��������' AS SORGNAME, --Դ��������  Ĭ�� �����ʲ�����
           '1' AS ORGSORTNO, --�������������
           '9998' AS ORGID, --��������ID  Ĭ�� �����ʲ�����(01160000)
           '��������' AS ORGNAME, --������������  Ĭ�� �����ʲ�����
           '9998' AS ACCORGID, --�������ID  Ĭ�� �����ʲ�����(01160000)
           '��������' AS ACCORGNAME, --�����������  Ĭ�� �����ʲ�����
           'C41' AS INDUSTRYID, --������ҵ����
           '��������ҵ' AS INDUSTRYNAME, --������ҵ����
           '0402' AS BUSINESSLINE, --ҵ������  Ĭ�� ͬҵ(04)
           '132' AS ASSETTYPE, --�ʲ�����  Ĭ�� NULL RWA������
           '13205' AS ASSETSUBTYPE, --�ʲ�С��  Ĭ�� NULL RWA������
           '109020' AS BUSINESSTYPEID, --ҵ��Ʒ�ִ���  �ر�ҵ��Ʒ�ִ���
           '���-ծȯͶ��' AS BUSINESSTYPENAME, --ҵ��Ʒ������  �ر�ҵ��Ʒ�ִ���
           '01' AS CREDITRISKDATATYPE, --���÷�����������  Ĭ�� һ�������(01)
           '01' AS ASSETTYPEOFHAIRCUTS, --�ۿ�ϵ����Ӧ�ʲ����  Ĭ�� �ֽ��ֽ�ȼ���(01)
           '07' AS BUSINESSTYPESTD, --Ȩ�ط�ҵ������  Ĭ�� һ���ʲ�(07)
           '0112' AS EXPOCLASSSTD, --Ȩ�ط���¶����  Ĭ�� NULL RWA������
           '011216' AS EXPOSUBCLASSSTD, --Ȩ�ط���¶С��  Ĭ�� NULL RWA������
           '0203' AS EXPOCLASSIRB, --��������¶����  Ĭ�� NULL RWA������
           '020301' AS EXPOSUBCLASSIRB, --��������¶С��  Ĭ�� NULL RWA������
           '01' AS EXPOBELONG, --��¶������ʶ  Ĭ�� ����(01)
           '01' AS BOOKTYPE, --�˻����  �ʲ�����Ϊ�����Խ����ʲ�(10)��Ϊ�����˻�(02)������Ϊ�����˻�(01)
           '03' AS REGUTRANTYPE, --��ܽ�������  Ĭ�� ��Ѻ����(03)
           '0' AS REPOTRANFLAG, --�ع����ױ�ʶ  Ĭ�� ��(0)
           '1' AS REVAFREQUENCY, --�ع�Ƶ��  Ĭ�� 1
           T1.CURRENCY AS CURRENCY, --����
           T1.CUR_BAL AS NORMALPRINCIPAL, --�����������
           0 AS OVERDUEBALANCE, --�������  Ĭ�� 0
           0 AS NONACCRUALBALANCE, --��Ӧ�����  Ĭ�� 0
           T1.CUR_BAL AS ONSHEETBALANCE, --�������
           0 AS NORMALINTEREST, --������Ϣ  Ĭ�� 0 ��Ϣͳһ�����˱�����
           0 AS ONDEBITINTEREST, --����ǷϢ  Ĭ�� 0
           0 AS OFFDEBITINTEREST, --����ǷϢ  Ĭ�� 0
           0 AS EXPENSERECEIVABLE, --Ӧ�շ���  Ĭ�� 0
           T1.CUR_BAL AS ASSETBALANCE, --�ʲ����  ���ҵ������У�����
           T1.SUBJECT_NO AS ACCSUBJECT1, --��Ŀһ
           '' AS ACCSUBJECT2, --��Ŀ��
           '' AS ACCSUBJECT3, --��Ŀ��
           P_DATA_DT_STR AS STARTDATE, --��ʼ����
           P_DATA_DT_STR AS DUEDATE, --��������
           0 AS ORIGINALMATURITY, --ԭʼ����
           0 AS RESIDUALM, --ʣ������
           '01' AS RISKCLASSIFY, --���շ���                        ����ʹ���Ŵ���12������ת��
           '01' AS EXPOSURESTATUS, --���ձ�¶״̬                    Ĭ�� ����(01)
           0 AS OVERDUEDAYS, --��������                        Ĭ�� 0
           0 AS SPECIALPROVISION, --ר��׼����                     Ĭ�� 0  RWA����
           0 AS GENERALPROVISION, --һ��׼����                     Ĭ�� 0  RWA����
           0 AS ESPECIALPROVISION, --�ر�׼����                     Ĭ�� 0  RWA����
           0 AS WRITTENOFFAMOUNT, --�Ѻ������                     Ĭ�� 0
           '' AS OFFEXPOSOURCE, --���Ⱪ¶��Դ                    Ĭ�� NULL
           '' AS OFFBUSINESSTYPE, --����ҵ������                    Ĭ�� NULL
           '' AS OFFBUSINESSSDVSSTD, --Ȩ�ط�����ҵ������ϸ��         Ĭ�� NULL
           '0' AS UNCONDCANCELFLAG, --�Ƿ����ʱ����������            Ĭ�� NULL
           '' AS CCFLEVEL, --����ת��ϵ������                Ĭ�� NULL
           NULL AS CCFAIRB, --�߼�������ת��ϵ��             Ĭ�� NULL
           '01' AS CLAIMSLEVEL, --ծȨ����
           '1' AS BONDFLAG, --�Ƿ�Ϊծȯ                     Ĭ�� ��(1)
           '02' AS BONDISSUEINTENT, --ծȯ����Ŀ��
           '0' AS NSUREALPROPERTYFLAG, --�Ƿ�����ò�����                Ĭ�� ��(0)
           '' AS REPASSETTERMTYPE, --��ծ�ʲ���������                Ĭ�� NULL
           '0' AS DEPENDONFPOBFLAG, --�Ƿ�����������δ��ӯ��         Ĭ�� ��(0)
           NULL AS IRATING, --�ڲ�����
           NULL AS PD, --ΥԼ����
           '' AS LGDLEVEL, --ΥԼ��ʧ�ʼ���                 Ĭ�� NULL
           NULL AS LGDAIRB, --�߼���ΥԼ��ʧ��                Ĭ�� NULL
           NULL AS MAIRB, --�߼�����Ч����                 Ĭ�� NULL
           NULL AS EADAIRB, --�߼���ΥԼ���ձ�¶             Ĭ�� NULL
           '0' AS DEFAULTFLAG, --ΥԼ��ʶ
           0.45 AS BEEL, --��ΥԼ��¶Ԥ����ʧ����          ծȨ����=�μ�ծ(02)����Ϊ0.75������Ϊ0.45
           0.45 AS DEFAULTLGD, --��ΥԼ��¶ΥԼ��ʧ��            Ĭ�� NULL
           '0' AS EQUITYEXPOFLAG, --��Ȩ��¶��ʶ                    Ĭ�� ��(0)
           '' AS EQUITYINVESTTYPE, --��ȨͶ�ʶ�������                Ĭ�� NULL
           '' AS EQUITYINVESTCAUSE, --��ȨͶ���γ�ԭ��                Ĭ�� NULL
           '0' AS SLFLAG, --רҵ�����ʶ                    Ĭ�� ��(0)
           '' AS SLTYPE, --רҵ��������                    Ĭ�� NULL
           '' AS PFPHASE, --��Ŀ���ʽ׶�                    Ĭ�� NULL
           '01' AS REGURATING, --�������                        Ĭ�� ��(01)
           '' AS CBRCMPRATINGFLAG, --������϶������Ƿ��Ϊ����     Ĭ�� NULL
           '' AS LARGEFLUCFLAG, --�Ƿ񲨶��Խϴ�                 Ĭ�� NULL
           '0' AS LIQUEXPOFLAG, --�Ƿ���������з��ձ�¶         Ĭ�� ��(0)
           '1' AS PAYMENTDEALFLAG, --�Ƿ����Ը�ģʽ                Ĭ�� ��(1)
           '0' AS DELAYTRADINGDAYS, --�ӳٽ�������                    Ĭ�� NULL
           '1' AS SECURITIESFLAG, --�м�֤ȯ��ʶ                    Ĭ�� ��(1)
           '' AS SECUISSUERID, --֤ȯ������ID
           '' AS RATINGDURATIONTYPE, --������������
           '' AS SECUISSUERATING, --֤ȯ���еȼ�
           NULL AS SECURESIDUALM, --֤ȯʣ������
           1 AS SECUREVAFREQUENCY, --֤ȯ�ع�Ƶ��                    Ĭ�� 1
           '0' AS CCPTRANFLAG, --�Ƿ����뽻�׶�����ؽ���        Ĭ�� ��(0)
           '' AS CCPID, --���뽻�׶���ID                  Ĭ�� NULL
           '' AS QUALCCPFLAG, --�Ƿ�ϸ����뽻�׶���            Ĭ�� NULL
           '' AS BANKROLE, --���н�ɫ                        Ĭ�� NULL
           '' AS CLEARINGMETHOD, --���㷽ʽ                        Ĭ�� NULL
           '' AS BANKASSETFLAG, --�Ƿ������ύ�ʲ�                Ĭ�� NULL
           '' AS MATCHCONDITIONS, --�����������                    Ĭ�� NULL
           '0' AS SFTFLAG, --֤ȯ���ʽ��ױ�ʶ                Ĭ�� ��(0)
           '0' AS MASTERNETAGREEFLAG, --���������Э���ʶ             Ĭ�� ��(0)
           '' AS MASTERNETAGREEID, --���������Э��ID               Ĭ�� NULL
           '' AS SFTTYPE, --֤ȯ���ʽ�������                Ĭ�� NULL
           '' AS SECUOWNERTRANSFLAG, --֤ȯ����Ȩ�Ƿ�ת��             Ĭ�� NULL
           '0' AS OTCFLAG, --�����������߱�ʶ                Ĭ�� ��(0)
           '' AS VALIDNETTINGFLAG, --��Ч�������Э���ʶ            Ĭ�� NULL
           '' AS VALIDNETAGREEMENTID, --��Ч�������Э��ID              Ĭ�� NULL
           '' AS OTCTYPE, --����������������                Ĭ�� NULL
           '' AS DEPOSITRISKPERIOD, --��֤������ڼ�                 Ĭ�� NULL
           NULL AS MTM, --���óɱ�                        Ĭ�� NULL
           '' AS MTMCURRENCY, --���óɱ�����                    Ĭ�� NULL
           '' AS BUYERORSELLER, --������                        Ĭ�� NULL
           '' AS QUALROFLAG, --�ϸ�����ʲ���ʶ                Ĭ�� NULL
           '' AS ROISSUERPERFORMFLAG, --�����ʲ��������Ƿ�����Լ        Ĭ�� NULL
           '' AS BUYERINSOLVENCYFLAG, --���ñ������Ƿ��Ʋ�            Ĭ�� NULL
           NULL AS NONPAYMENTFEES, --��δ֧������                    Ĭ�� NULL
           '0' AS RETAILEXPOFLAG, --���۱�¶��ʶ                    Ĭ�� ��(0)
           '' AS RETAILCLAIMTYPE, --����ծȨ����                    Ĭ�� NULL
           '' AS MORTGAGETYPE, --ס����Ѻ��������                Ĭ�� NULL
           1 AS EXPONUMBER, --���ձ�¶����                    Ĭ�� 1
           0.8 AS LTV, --�����ֵ��                     Ĭ�� 0.8
           NULL AS AGING, --����                            Ĭ�� NULL
           '' AS NEWDEFAULTDEBTFLAG, --����ΥԼծ���ʶ                Ĭ�� NULL
           '' AS PDPOOLMODELID, --PD�ֳ�ģ��ID                    Ĭ�� NULL
           '' AS LGDPOOLMODELID, --LGD�ֳ�ģ��ID                   Ĭ�� NULL
           '' AS CCFPOOLMODELID, --CCF�ֳ�ģ��ID                   Ĭ�� NULL
           '' AS PDPOOLID, --����PD��ID                     Ĭ�� NULL
           '' AS LGDPOOLID, --����LGD��ID                    Ĭ�� NULL
           '' AS CCFPOOLID, --����CCF��ID                    Ĭ�� NULL
           '0' AS ABSUAFLAG, --�ʲ�֤ȯ�������ʲ���ʶ         Ĭ�� ��(0)
           '' AS ABSPOOLID, --֤ȯ���ʲ���ID                  Ĭ�� NULL
           '' AS GROUPID, --������                        Ĭ�� NULL
           NULL AS DEFAULTDATE, --ΥԼʱ��                        Ĭ�� NULL
           NULL AS ABSPROPORTION, --�ʲ�֤ȯ������
           NULL AS DEBTORNUMBER, --����˸���
           NULL,
           NULL
      FROM (SELECT T1.DATANO,
                   '12220102' AS SUBJECT_NO,
                   'CNY' AS CURRENCY,
                   SUM(T1.BALANCE_D * T2.JZRAT / 100 -
                       T1.BALANCE_C * T2.JZRAT / 100) AS CUR_BAL -- �跽-����
              FROM FNS_GL_BALANCE T1
              LEFT JOIN NNS_JT_EXRATE T2 -- ����
                ON T1.DATANO = T2.DATANO
               AND T1.CURRENCY_CODE = T2.CCY
             WHERE T1.CURRENCY_CODE <> 'RMB'
               AND T1.DATANO = P_DATA_DT_STR
               AND (T1.SUBJECT_NO LIKE '150302%' OR
                   T1.SUBJECT_NO IN ('12220102', '12220103', '12220104'))
             GROUP BY T1.DATANO) T1; -- 12220102=15030201+15030202+15030203+15030204+12220102+12220103+12220104

  COMMIT;

  DBMS_STATS.GATHER_TABLE_STATS(OWNNAME => 'RWA_DEV',
                                TABNAME => 'RWA_LC_EXPOSURE',
                                CASCADE => TRUE);

  --DBMS_OUTPUT.PUT_LINE('����������2�������롾���÷��ձ�¶-�ʹܡ�' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

  /*Ŀ�������ͳ��*/
  --ͳ�Ʋ���ļ�¼
  SELECT COUNT(1) INTO V_COUNT1 FROM RWA_DEV.RWA_LC_EXPOSURE;
  --DBMS_OUTPUT.PUT_LINE('RWA_DEV.RWA_LC_EXPOSURE��ǰ��������ϵͳ-�ʹܼƻ�Ͷ�����ݼ�¼Ϊ��' || (v_count2-v_count1) || '��');

  --DBMS_OUTPUT.PUT_LINE('��ִ�� ' || v_pro_name || ' �洢���̽��� ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));
  P_PO_RTNCODE := '1';
  P_PO_RTNMSG  := '�ɹ�' || '-' || V_COUNT1;

  --�����쳣
EXCEPTION
  WHEN OTHERS THEN
    --DBMS_OUTPUT.PUT_LINE('������,�������Ϊ:'||sqlcode||';������ϢΪ:'||sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace);
    ROLLBACK;
    P_PO_RTNCODE := SQLCODE;
    P_PO_RTNMSG  := '���÷��ձ�¶(' || V_PRO_NAME || ')ETLת��ʧ�ܣ�' || SQLERRM ||
                    ';��������Ϊ:' || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
  
    RETURN;
END PRO_RWA_LC_EXPOSURE;
/

