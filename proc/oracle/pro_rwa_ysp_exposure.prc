CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_YSP_EXPOSURE(P_DATA_DT_STR IN VARCHAR2, --�������� yyyyMMdd
                                                 P_PO_RTNCODE  OUT VARCHAR2, --���ر�� 1 �ɹ�,0 ʧ��
                                                 P_PO_RTNMSG   OUT VARCHAR2 --��������
                                                 )
/*
  �洢��������:RWA_DEV.PRO_RWA_YSP_EXPOSURE
  ʵ�ֹ���:����ϵͳ-����Ʒҵ��-���÷��ձ�
  ���ݿھ�:ȫ��
  ����Ƶ��:�³�����
  ��  ��  :V1.0.0
  ��д��  :
  ��дʱ��:2019-04-17
  ��  λ  :�Ϻ���˶��Ϣ�����ɷ����޹�˾
  Դ  ��1 :RWA_DEV.BRD_SWAP|������
  Դ  ��2 :RWA.ORG_INFO|������Ϣ��
  Դ  ��3 :RWA_DEV.NCM_CUSTOMER_INFO|�ͻ���Ϣ��
  Դ  ��4 :RWA.CODE_LIBRARY|������
  Դ  ��5 :RWA_DEV.IRS_CR_CUSTOMER_RATE|�ͻ�������
  Դ  ��6 :RWA_DEV.NCM_BREAKDEFINEDREMARK|��ʶΥԼ�����
  Դ  ��6 :RWA_DEV.OPI_SWDT\Դϵͳ�Ļ�����
  �����¼(�޸���|�޸�ʱ��|�޸�����):
  
  */
 AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  V_PRO_NAME VARCHAR2(200) := 'RWA_DEV.PRO_RWA_YSP_EXPOSURE';
  --�����쳣����
  V_RAISE EXCEPTION;
  --���嵱ǰ����ļ�¼��
  V_COUNT INTEGER;

BEGIN
  --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

  /*�����ȫ�����ݼ��������Ŀ���*/
  --1.���Ŀ����е�ԭ�м�¼
  EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_YSP_EXPOSURE';

  --2.���������������ݴ�Դ����뵽Ŀ�����
  
  --���ʻ���  ���һ���ҵ��

  INSERT INTO RWA_DEV.RWA_YSP_EXPOSURE
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
     DEBTORNUMBER --����˸���
     )
  
        SELECT TO_DATE(p_data_dt_str, 'YYYYMMDD'), --01��������
           p_data_dt_str, --02������ˮ��
           T1.DEALNO || T1.SEQ || T1.PAYRECIND, --���ձ�¶ID
           T1.DEALNO || T1.SEQ || T1.PAYRECIND, --ծ��ID
           'YSP', --ԴϵͳID
           T1.DEALNO || T1.SEQ || T1.PAYRECIND, --��ͬID
           'OPI' || TRIM(T2.CNO), --��������ID
           '6001', --Դ����ID
           '�������йɷ����޹�˾����ҵ��', --Դ��������
           '1290', --�������������
           '6001', --��������ID
           '�������йɷ����޹�˾����ҵ��', --������������
           '6001', --�������ID
           '�������йɷ����޹�˾����ҵ��', --�����������
           'J6621', --������ҵ����
           '��ҵ���з���', --������ҵ����
           '0102',  --ҵ������
           '223',   --�ʲ�����
           '22301', --�ʲ�С��
           CASE
             WHEN substr(T2.Cost,6,1) = '1' THEN
              '404000001'
             WHEN substr(T2.Cost,6,1) in( '2','3') THEN
              '404000002'
           END, --ҵ��Ʒ�ִ���
           CASE
             WHEN substr(T2.Cost,6,1) = '1' THEN
              '���ʻ���'
             WHEN substr(T2.Cost,6,1) in( '2','3') THEN
              '���һ���'
           END, --ҵ��Ʒ������
           '05', --���÷�����������
           '01', --�ۿ�ϵ����Ӧ�ʲ����
           '01', --Ȩ�ط�ҵ������
           '', --Ȩ�ط���¶����
           '', --Ȩ�ط���¶С��
           '', --��������¶����
           '', --��������¶С��
           '03', --��¶������ʶ
           CASE
             WHEN SUBSTR(T2.COST,4,1) = '3' THEN
              '01'
             ELSE
              '02'
           END, --�˻����
           '02', --��ܽ�������
           '0', --�ع����ױ�ʶ
           '1', --�ع�Ƶ��
           T1.NOTCCY, --����
           ABS(T1.NOTCCYAMT), --�����������
           '0', --�������
           '0', --��Ӧ�����
           ABS(T1.NOTCCYAMT), --�������
           '0', --������Ϣ
           '0', --����ǷϢ
           '0', --����ǷϢ
           '0', --Ӧ�շ���
           ABS(T1.NOTCCYAMT), --�ʲ����
           '70120000', --��Ŀһ
           '', --��Ŀ��
           '', --��Ŀ��
           T2.STARTDATE, --��ʼ����
            CASE
               --��������ʱ
               WHEN T1.FIXFLOATIND = 'L' AND T3.RATEREVDTE IS NOT NULL THEN TO_CHAR(T3.RATEREVDTE,'YYYYMMDD')
               ELSE T2.MATDATE
            END, --��������
           CASE
               --��������ʱ
               WHEN T1.FIXFLOATIND = 'L' AND T3.RATEREVDTE IS NOT NULL THEN 
                     CASE
                         WHEN (T3.RATEREVDTE - TO_DATE(T2.STARTDATE, 'YYYY/MM/DD')) / 365 < 0 THEN
                          0
                         ELSE
                          (T3.RATEREVDTE - TO_DATE(T2.STARTDATE, 'YYYY/MM/DD')) / 365
                     END
               --�������
               ELSE
                     CASE
                         WHEN (TO_DATE(T2.MATDATE, 'YYYY/MM/DD') - TO_DATE(T2.STARTDATE, 'YYYY/MM/DD')) / 365 < 0 THEN
                          0
                         ELSE
                              (TO_DATE(T2.MATDATE, 'YYYY/MM/DD') - TO_DATE(T2.STARTDATE, 'YYYY/MM/DD')) / 365
                     END
           END, --ԭʼ����   
           CASE
         --��������ʱ
         WHEN T1.FIXFLOATIND = 'L' AND T3.RATEREVDTE IS NOT NULL THEN 
               CASE
                   WHEN (T3.RATEREVDTE - TO_DATE(p_data_dt_str, 'YYYY/MM/DD')) / 365 < 0 THEN
                    0
                   ELSE
                    (T3.RATEREVDTE - TO_DATE(p_data_dt_str, 'YYYY/MM/DD')) / 365
               END         
          --�������
          ELSE
               CASE
                   WHEN (TO_DATE(T2.MATDATE, 'YYYY/MM/DD') - TO_DATE(p_data_dt_str, 'YYYY/MM/DD')) / 365 < 0 THEN
                    0
                   ELSE
                        (TO_DATE(T2.MATDATE, 'YYYY/MM/DD') - TO_DATE(p_data_dt_str, 'YYYY/MM/DD')) / 365
               END      
           END, --ʣ������  
           '01', --���շ���
           '01', --���ձ�¶״̬
           '0', --��������
           '0', --ר��׼����
           '0', --һ��׼����
           '0', --�ر�׼����
           '0', --�Ѻ������
           '', --���Ⱪ¶��Դ
           '', --����ҵ������
           '', --Ȩ�ط�����ҵ������ϸ��
           '0', --�Ƿ����ʱ����������
           '', --����ת��ϵ������
           '', --�߼�������ת��ϵ��
           '01', --ծȨ����
           '0', --�Ƿ�Ϊծȯ
           '', --ծȯ����Ŀ��
           '0', --�Ƿ�����ò�����
           '', --��ծ�ʲ���������
           '0', --�Ƿ�����������δ��ӯ��
           '', --�ڲ�����
           '', --ΥԼ����
           '', --ΥԼ��ʧ�ʼ���
           '', --�߼���ΥԼ��ʧ��
           '', --�߼�����Ч����
           '', --�߼���ΥԼ���ձ�¶
           '', --ΥԼ��ʶ
           '0.45', --��ΥԼ��¶Ԥ����ʧ����
           '0.45', --��ΥԼ��¶ΥԼ��ʧ��
           '0', --��Ȩ��¶��ʶ
           '', --��ȨͶ�ʶ�������
           '', --��ȨͶ���γ�ԭ��
           '0', --רҵ�����ʶ
           '', --רҵ��������
           '', --��Ŀ���ʽ׶�
           '', --�������
           '0', --������϶������Ƿ��Ϊ����
           '0', --�Ƿ񲨶��Խϴ�
           '0', --�Ƿ���������з��ձ�¶
           '0', --�Ƿ����Ը�ģʽ
           '0', --�ӳٽ�������
           '0', --�м�֤ȯ��ʶ
           '', --֤ȯ������ID
           '', --������������
           '', --֤ȯ���еȼ�
           '', --֤ȯʣ������
           '', --֤ȯ�ع�Ƶ��
           '0', --�Ƿ����뽻�׶�����ؽ���
           '', --���뽻�׶���ID0
           '0', --�Ƿ�ϸ����뽻�׶���
           '', --���н�ɫ
           '', --���㷽ʽ
           '0', --�Ƿ������ύ�ʲ�
           '', --�����������
           '0', --֤ȯ���ʽ��ױ�ʶ
           '0', --���������Э���ʶ
           '', --���������Э��ID
           '', --֤ȯ���ʽ�������
           '0', --֤ȯ����Ȩ�Ƿ�ת��
           '1', --�����������߱�ʶ
           '0', --��Ч�������Э���ʶ
           '', --��Ч�������Э��ID
           '01', --����������������
           '', --��֤������ڼ�
           ABS(T2.NPVBAMT), --���óɱ������м�ֵ��
           T1.NOTCCY, --���óɱ�����
           '', --������
           '0', --�ϸ�����ʲ���ʶ
           '0', --�����ʲ��������Ƿ�����Լ
           '0', --���ñ������Ƿ��Ʋ�
           '0', --��δ֧������
           '0', --���۱�¶��ʶ
           '', --����ծȨ����
           '', --ס����Ѻ��������
           '1', --���ձ�¶����
           '0.8', --�����ֵ��
           '', --����
           '', --����ΥԼծ���ʶ
           '', --PD�ֳ�ģ��ID
           '', --LGD�ֳ�ģ��ID
           '', --CCF�ֳ�ģ��ID
           '', --����PD��ID
           '', --����LGD��ID
           '', --����CCF��ID
           '0', --�ʲ�֤ȯ�������ʲ���ʶ
           '', --֤ȯ���ʲ���ID
           '', --������
           '', -- BREAKDATE--ΥԼʱ�� 
           '', --�ʲ�֤ȯ������
           '' --����˸���
      FROM OPI_SWDT T1--��������
      INNER JOIN OPI_SWDH T2--������ͷ 
      ON T1.DEALNO = T2.DEALNO
      AND T2.DATANO = p_data_dt_str
      AND T2.PORT<>'SWDK'    --�ų��ṹ�Դ��ҵ��
      LEFT JOIN (
            SELECT SWDS.DEALNO, SWDS.SEQ,PAYRECIND, TO_DATE(MAX(SWDS.RATEREVDTE)) AS   RATEREVDTE 
              FROM OPI_SWDS SWDS  --��Ϣ��Ϣ��   ��ȡ�ض�������              
            WHERE SWDS.DATANO = p_data_dt_str
              AND SWDS.RATEREVDTE IS NOT NULL 
            GROUP BY SWDS.DEALNO, SWDS.SEQ,PAYRECIND
      )T3
        ON T3.DEALNO = T1.DEALNO 
       AND T3.SEQ = T1.SEQ
       AND T1.PAYRECIND=T3.PAYRECIND
      LEFT JOIN OPI_CUST T4
             ON T2.CNO = T4.CNO         
            AND T4.DATANO = p_data_dt_str
    WHERE T1.DATANO = p_data_dt_str
      AND T1.PAYRECIND = 'R' --���׶���ֻ���Ƕ��ַ���     
      AND SUBSTR(T2.COST, 1, 1) = '3' --��һλ=3  --����Ϊ����/���ҵ���ҵ��
      AND SUBSTR(T2.COST, 6, 1) IN ('1', '2','3') --����λ=1\2  --���ʵ���\���ҵ���,Զ��
      AND T2.VERIND = 1 
      AND TRIM(T2.REVDATE) IS NULL
      AND TO_DATE(T1.MATDATE,'YYYYMMDD')>=TO_DATE(p_data_dt_str,'YYYYMMDD')  --�����ղ���С�ڵ�ǰ����       
    ;
    
  COMMIT;
 
  --�����ڡ�Զ�� ������
  
  INSERT INTO RWA_DEV.RWA_YSP_EXPOSURE
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
     DEBTORNUMBER --����˸���
     )
    SELECT TO_DATE(p_data_dt_str, 'YYYYMMDD'), --01��������
       p_data_dt_str, --02������ˮ��
       DEALNO || T1.SEQ || T1.PS, --���ձ�¶ID
       DEALNO || T1.SEQ || T1.PS, --ծ��ID
       'YSP', --ԴϵͳID
       DEALNO || T1.SEQ || T1.PS, --��ͬID
       'OPI'|| TRIM(T1.CUST), --��������ID
       '6001', --Դ����ID
       '�������йɷ����޹�˾����ҵ��', --Դ��������
       '1290', --�������������
       '6001', --��������ID
       '�������йɷ����޹�˾����ҵ��', --������������
       '6001', --�������ID
       '�������йɷ����޹�˾����ҵ��', --�����������
       'J6621', --������ҵ����
       '��ҵ���з���', --������ҵ����
       '0102', --ҵ������
       '223', --�ʲ�����
       '22301', --�ʲ�С��
        CASE  WHEN  SUBSTR(T1.COST,6,1)=1
         THEN '404000003'
           WHEN  SUBSTR(T1.COST,6,1)=2
         THEN '404000004'
           WHEN  SUBSTR(T1.COST,6,1)=3
         THEN '404000005'
        END, --ҵ��Ʒ�ִ���
       CASE  WHEN  SUBSTR(T1.COST,6,1)=1
         THEN '��㼴��'
           WHEN  SUBSTR(T1.COST,6,1)=2
         THEN '���Զ��'
           WHEN  SUBSTR(T1.COST,6,1)=3
         THEN '������'
        END, --ҵ��Ʒ������
       '01', --���÷�����������
       '01', --�ۿ�ϵ����Ӧ�ʲ����
       '01', --Ȩ�ط�ҵ������
       '', --Ȩ�ط���¶����
       '', --Ȩ�ط���¶С��
       '', --��������¶����
       '', --��������¶С��
       '02', --��¶������ʶ
       CASE
         WHEN SUBSTR(COST, 1�� 4) = 'E' THEN
          '01'
         ELSE
          '02'
       END, --�˻����
       '02', --��ܽ�������
       '0', --�ع����ױ�ʶ
       '1', --�ع�Ƶ��
       CASE
         WHEN T1.PS = 'P' THEN T1.CTRCCY  --��֧����ʽΪPʱ  ����������  ���������������˷���
         WHEN T1.PS = 'S' THEN T1.CCY     --��֧����ʽΪSʱ  ����������  ��������������˷���
         ELSE T1.CCY
       END , --����
       CASE
         WHEN T1.PS = 'P' THEN ABS(T1.CTRAMT)  --��֧����ʽΪPʱ  ����������  ���������������˷���
         WHEN T1.PS = 'S' THEN ABS(T1.CCYAMT)     --��֧����ʽΪSʱ  ����������  ��������������˷���
         ELSE ABS(T1.CCYAMT)
       END, --�����������
       '0', --�������
       '0', --��Ӧ�����
       CASE
         WHEN T1.PS = 'P' THEN ABS(T1.CTRAMT)  --��֧����ʽΪPʱ  ����������  ���������������˷���
         WHEN T1.PS = 'S' THEN ABS(T1.CCYAMT)     --��֧����ʽΪSʱ  ����������  ��������������˷���
         ELSE ABS(T1.CCYAMT)
       END, --�������
       '0', --������Ϣ
       '0', --����ǷϢ
       '0', --����ǷϢ
       '0', --Ӧ�շ���
       CASE
         WHEN T1.PS = 'P' THEN ABS(T1.CTRAMT)  --��֧����ʽΪPʱ  ����������  ���������������˷���
         WHEN T1.PS = 'S' THEN ABS(T1.CCYAMT)     --��֧����ʽΪSʱ  ����������  ��������������˷���
         ELSE ABS(T1.CCYAMT)
       END, --�ʲ���� 
       '70270400', --��Ŀһ Ӧ�յ��ڽ��ۻ�-�ױ�Ӧ�յ��ڽ��ۻ�
       '', --��Ŀ��
       '', --��Ŀ��
       T1.DEALDATE, --��ʼ����
       T1.VDATE, --��������
       CASE
         WHEN (TO_DATE(T1.VDATE, 'YYYY-MM-DD') -
              TO_DATE(T1.DEALDATE, 'YYYY-MM-DD')) / 365 < 0 THEN
          0
         ELSE
          (TO_DATE(T1.VDATE, 'YYYY-MM-DD') -
          TO_DATE(T1.DEALDATE, 'YYYY-MM-DD')) / 365
       END, --ԭʼ���� 
       CASE
         WHEN (TO_DATE(T1.VDATE, 'YYYY-MM-DD') -
              TO_DATE(p_data_dt_str, 'YYYYMMDD')) / 365 < 0 THEN
          0
         ELSE
          (TO_DATE(T1.VDATE, 'YYYY-MM-DD') -
          TO_DATE(p_data_dt_str, 'YYYYMMDD')) / 365
       END, --ʣ������
       '01', --���շ���
       '01', --���ձ�¶״̬
       '0', --��������
       '0', --ר��׼����
       '0', --һ��׼����
       '0', --�ر�׼����
       '0', --�Ѻ������
       '', --���Ⱪ¶��Դ
       '', --����ҵ������
       '', --Ȩ�ط�����ҵ������ϸ��
       '0', --�Ƿ����ʱ����������
       '', --����ת��ϵ������
       '', --�߼�������ת��ϵ��
       '01', --ծȨ����
       '0', --�Ƿ�Ϊծȯ
       '', --ծȯ����Ŀ��
       '0', --�Ƿ�����ò�����
       '', --��ծ�ʲ���������
       '0', --�Ƿ�����������δ��ӯ��
       '', --�ڲ�����
       '', --ΥԼ����
       '', --ΥԼ��ʧ�ʼ���
       '', --�߼���ΥԼ��ʧ��
       '', --�߼�����Ч����
       '', --�߼���ΥԼ���ձ�¶
       '', --ΥԼ��ʶ
       '0.45', --��ΥԼ��¶Ԥ����ʧ����
       '0.45', --��ΥԼ��¶ΥԼ��ʧ��
       '0', --��Ȩ��¶��ʶ
       '', --��ȨͶ�ʶ�������
       '', --��ȨͶ���γ�ԭ��
       '0', --רҵ�����ʶ
       '', --רҵ��������
       '', --��Ŀ���ʽ׶�
       '', --�������
       '0', --������϶������Ƿ��Ϊ����
       '0', --�Ƿ񲨶��Խϴ�
       '0', --�Ƿ���������з��ձ�¶
       '0', --�Ƿ����Ը�ģʽ
       '0', --�ӳٽ�������
       '0', --�м�֤ȯ��ʶ
       '', --֤ȯ������ID
       '', --������������
       '', --֤ȯ���еȼ�
       '', --֤ȯʣ������
       '', --֤ȯ�ع�Ƶ��
       '0', --�Ƿ����뽻�׶�����ؽ���
       '', --���뽻�׶���ID
       '0', --�Ƿ�ϸ����뽻�׶���
       '', --���н�ɫ
       '', --���㷽ʽ
       '0', --�Ƿ������ύ�ʲ�
       '', --�����������
       '0', --֤ȯ���ʽ��ױ�ʶ
       '0', --���������Э���ʶ
       '', --���������Э��ID
       '', --֤ȯ���ʽ�������
       '0', --֤ȯ����Ȩ�Ƿ�ת��
       '1', --�����������߱�ʶ
       '0', --��Ч�������Э���ʶ
       '', --��Ч�������Э��ID
       '02', --����������������
       '', --��֤������ڼ�
       T1.CCYNPVAMT + T1.CTRNPVAMT, --���óɱ�= MAX(���м�ֵ, 0)
       CASE
         WHEN T1.PS = 'P' THEN T1.CTRCCY  --��֧����ʽΪPʱ  ����������  ���������������˷���
         WHEN T1.PS = 'S' THEN T1.CCY     --��֧����ʽΪSʱ  ����������  ��������������˷���
         ELSE T1.CCY
       END, --���óɱ�����
       '', --������
       '0', --�ϸ�����ʲ���ʶ
       '0', --�����ʲ��������Ƿ�����Լ
       '0', --���ñ������Ƿ��Ʋ�
       '0', --��δ֧������
       '0', --���۱�¶��ʶ
       '', --����ծȨ����
       '', --ס����Ѻ��������
       '1', --���ձ�¶����
       '0.8', --�����ֵ��
       '', --����
       '', --����ΥԼծ���ʶ
       '', --PD�ֳ�ģ��ID
       '', --LGD�ֳ�ģ��ID
       '', --CCF�ֳ�ģ��ID
       '', --����PD��ID
       '', --����LGD��ID
       '', --����CCF��ID
       '0', --�ʲ�֤ȯ�������ʲ���ʶ
       '', --֤ȯ���ʲ���ID
       '', --������
       '', -- BREAKDATE, --ΥԼʱ�� 
       '', --�ʲ�֤ȯ������
       '' --����˸���
  FROM OPI_FXDH T1
  LEFT JOIN OPI_CUST T2
    ON T1.DATANO = T2.DATANO
   AND T1.CUST = T2.CNO
   AND T1.DATANO = p_data_dt_str              
  WHERE T1.DATANO = p_data_dt_str
   AND T1.VDATE >= p_data_dt_str     
   AND SUBSTR(T1.COST, 1, 1) = '2' --��һλ=2  --����Ϊ���
   AND SUBSTR(T1.COST, 6, 1)='2' --����λ=2  --Զ��
   AND T1.VERIND = 1
   AND TRIM(T1.REVDATE) IS NULL
   ;
   
   COMMIT;
   
  

  DBMS_STATS.GATHER_TABLE_STATS(OWNNAME => 'RWA_DEV',
                                TABNAME => 'RWA_YSP_EXPOSURE',
                                CASCADE => TRUE);

  /*Ŀ�������ͳ��*/
  --ͳ�Ʋ���ļ�¼��
  SELECT COUNT(1) INTO V_COUNT FROM RWA_DEV.RWA_YSP_EXPOSURE;

  --Dbms_output.Put_line('RWA_DEV.RWA_TZ_CONTRACT��ǰ����Ĳ���ϵͳ-Ӧ�տ�Ͷ�����ݼ�¼Ϊ: ' || (v_count1 - v_count) || ' ��');
  --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̽��� ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

  P_PO_RTNCODE := '1';
  P_PO_RTNMSG  := '�ɹ�' || '-' || V_COUNT;
  --�����쳣
EXCEPTION
  WHEN OTHERS THEN
    --Dbms_output.Put_line('������,�������Ϊ:'||sqlcode||';������ϢΪ:'||sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace);
    ROLLBACK;
    P_PO_RTNCODE := SQLCODE;
    P_PO_RTNMSG  := '��ͬ��Ϣ(' || V_PRO_NAME || ')ETLת��ʧ�ܣ�' || SQLERRM ||
                    ';��������Ϊ:' || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
    RETURN;
END PRO_RWA_YSP_EXPOSURE;
/

