CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_EI_EXPOSURE(p_data_dt_str IN VARCHAR2, --��������
                                                p_po_rtncode  OUT VARCHAR2, --���ر�� 1 �ɹ�,0 ʧ��
                                                p_po_rtnmsg   OUT VARCHAR2 --��������
                                                )
/*
  �洢��������:RWA_DEV.PRO_RWA_EI_EXPOSURE
  ʵ�ֹ���:���ܷ��ձ�¶��,�������з��ձ�¶����Ϣ
  ���ݿھ�:ȫ��
  ����Ƶ��:�³�
  ��  ��  :V1.0.0
  ��д��  :SHUXD
  ��дʱ��:2016-06-01
  ��  λ  :�Ϻ���˶��Ϣ�����ɷ����޹�˾
  Դ  ��1 :RWA_DEV.RWA_XD_EXPOSURE|�Ŵ����ձ�¶��
  Դ  ��2 :RWA_DEV.RWA_HG_EXPOSURE|�ع����ձ�¶��
  Դ  ��3 :RWA_DEV.RWA_LC_EXPOSURE|��Ʒ��ձ�¶��
  Դ  ��4 :RWA_DEV.RWA_PJ_EXPOSURE|Ʊ�ݷ��ձ�¶��
  Դ  ��5 :RWA_DEV.RWA_TY_EXPOSURE|ͬҵ���ձ�¶��
  Դ  ��6 :RWA_DEV.RWA_TZ_EXPOSURE|Ͷ�ʷ��ձ�¶��
  Դ  ��7 :RWA_DEV.RWA_XYK_EXPOSURE|���ÿ����ձ�¶��
  Դ  ��8 :RWA_DEV.RWA_GQ_EXPOSURE|��Ȩ���ձ�¶��
  Դ  ��9 :RWA_DEV.RWA_ABS_ISSURE_EXPOSURE|�ʲ�֤ȯ�����л������ձ�¶��
  Դ  ��10:RWA_DEV.RWA_DZ_EXPOSURE|��ծ�ʲ����ձ�¶��
  Դ  ��11:RWA_DEV.RWA_ZX_EXPOSURE|ֱ�����з��ձ�¶��
  Դ  ��12:RWA_DEV.RWA_YSP_EXPOSURE|����Ʒҵ�����÷��ձ�¶��
  
  Ŀ���  :RWA_DEV.RWA_EI_EXPOSURE|���ձ�¶���ܱ�
  ������  :��
  �����¼(�޸���|�޸�ʱ��|�޸�����)��
  
  pxl  2019/05/08 �����ʽ�ϵͳ����Ʒҵ��¶��Ϣ����������ӿ�
  pxl  2019/05/29 �������ѽ��ڸ���ҵ��¶��Ϣ����������ӿ�
  
  */
 AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_EI_EXPOSURE';
  --�����ж�ֵ����
  v_count INTEGER;
  --�����쳣����
  v_raise EXCEPTION;

BEGIN
  --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

  BEGIN
    --ɾ�������ڵķ���������˴洢�����ǵ�һ�����׳��쳣�����Ժ���
    EXECUTE IMMEDIATE 'ALTER TABLE RWA_DEV.RWA_EI_EXPOSURE DROP PARTITION EXPOSURE' ||
                      p_data_dt_str;
  
    COMMIT;
  
  EXCEPTION
    WHEN OTHERS THEN
      IF (SQLCODE <> '-2149') THEN
        --�״η���truncate�����2149�쳣
        p_po_rtncode := sqlcode;
        p_po_rtnmsg  := '�������÷��ձ�¶��(' || v_pro_name || ')ETLת��ʧ�ܣ�' ||
                        sqlerrm || ';��������Ϊ:' ||
                        dbms_utility.format_error_backtrace;
        RETURN;
      END IF;
  END;

  --����һ����ǰ�����µķ���
  EXECUTE IMMEDIATE 'ALTER TABLE RWA_DEV.RWA_EI_EXPOSURE ADD PARTITION EXPOSURE' ||
                    p_data_dt_str || ' VALUES(TO_DATE(' || p_data_dt_str ||
                    ',''YYYYMMDD''))';

  COMMIT;

  /*�����Ŵ��ķ��ձ�¶��Ϣ*/
  INSERT /*+ APPEND */
  INTO RWA_DEV.RWA_EI_EXPOSURE
    (DATADATE -- ��������
    ,
     DATANO -- ������ˮ��
    ,
     EXPOSUREID -- ���ձ�¶ID
    ,
     DUEID -- ծ��ID
    ,
     SSYSID -- ԴϵͳID
    ,
     CONTRACTID -- ��ͬID
    ,
     CLIENTID -- ��������ID
    ,
     SORGID -- Դ����ID
    ,
     SORGNAME -- Դ��������
    ,
     ORGID -- ��������ID
    ,
     ORGNAME -- ������������
    ,
     ACCORGID -- �������ID
    ,
     ACCORGNAME -- �����������
    ,
     INDUSTRYID -- ������ҵ����
    ,
     INDUSTRYNAME -- ������ҵ����
    ,
     BUSINESSLINE -- ����
    ,
     ASSETTYPE -- �ʲ�����
    ,
     ASSETSUBTYPE -- �ʲ�С��
    ,
     BUSINESSTYPEID -- ҵ��Ʒ�ִ���
    ,
     BUSINESSTYPENAME -- ҵ��Ʒ������
    ,
     CREDITRISKDATATYPE -- ���÷�����������
    ,
     ASSETTYPEOFHAIRCUTS -- �ۿ�ϵ����Ӧ�ʲ����
    ,
     BUSINESSTYPESTD -- Ȩ�ط�ҵ������
    ,
     EXPOCLASSSTD -- Ȩ�ط���¶����
    ,
     EXPOSUBCLASSSTD -- Ȩ�ط���¶С��
    ,
     EXPOCLASSIRB -- ��������¶����
    ,
     EXPOSUBCLASSIRB -- ��������¶С��
    ,
     EXPOBELONG -- ��¶������ʶ
    ,
     BOOKTYPE -- �˻����
    ,
     REGUTRANTYPE -- ��ܽ�������
    ,
     REPOTRANFLAG -- �ع����ױ�ʶ
    ,
     REVAFREQUENCY -- �ع�Ƶ��
    ,
     CURRENCY -- ����
    ,
     NORMALPRINCIPAL -- �����������
    ,
     OVERDUEBALANCE -- �������
    ,
     NONACCRUALBALANCE -- ��Ӧ�����
    ,
     ONSHEETBALANCE -- �������
    ,
     NORMALINTEREST -- ������Ϣ
    ,
     ONDEBITINTEREST -- ����ǷϢ
    ,
     OFFDEBITINTEREST -- ����ǷϢ
    ,
     EXPENSERECEIVABLE -- Ӧ�շ���
    ,
     ASSETBALANCE -- �ʲ����
    ,
     ACCSUBJECT1 -- ��Ŀһ
    ,
     ACCSUBJECT2 -- ��Ŀ��
    ,
     ACCSUBJECT3 -- ��Ŀ��
    ,
     STARTDATE -- ��ʼ����
    ,
     DUEDATE -- ��������
    ,
     ORIGINALMATURITY -- ԭʼ����
    ,
     RESIDUALM -- ʣ������
    ,
     RISKCLASSIFY -- ���շ���
    ,
     EXPOSURESTATUS -- ���ձ�¶״̬
    ,
     OVERDUEDAYS -- ��������
    ,
     SPECIALPROVISION -- ר��׼����
    ,
     GENERALPROVISION -- һ��׼����
    ,
     ESPECIALPROVISION -- �ر�׼����
    ,
     WRITTENOFFAMOUNT -- �Ѻ������
    ,
     OFFEXPOSOURCE -- ���Ⱪ¶��Դ
    ,
     OFFBUSINESSTYPE -- ����ҵ������
    ,
     OFFBUSINESSSDVSSTD -- Ȩ�ط�����ҵ������ϸ��
    ,
     UNCONDCANCELFLAG -- �Ƿ����ʱ����������
    ,
     CCFLEVEL -- ����ת��ϵ������
    ,
     CCFAIRB -- �߼�������ת��ϵ��
    ,
     CLAIMSLEVEL -- ծȨ����
    ,
     BONDFLAG -- �Ƿ�Ϊծȯ
    ,
     BONDISSUEINTENT -- ծȯ����Ŀ��
    ,
     NSUREALPROPERTYFLAG -- �Ƿ�����ò�����
    ,
     REPASSETTERMTYPE -- ��ծ�ʲ���������
    ,
     DEPENDONFPOBFLAG -- �Ƿ�����������δ��ӯ��
    ,
     IRATING -- �ڲ�����
    ,
     PD -- ΥԼ����
    ,
     LGDLEVEL -- ΥԼ��ʧ�ʼ���
    ,
     LGDAIRB -- �߼���ΥԼ��ʧ��
    ,
     MAIRB -- �߼�����Ч����
    ,
     EADAIRB -- �߼���ΥԼ���ձ�¶
    ,
     DEFAULTFLAG -- ΥԼ��ʶ
    ,
     BEEL -- ��ΥԼ��¶Ԥ����ʧ����
    ,
     DEFAULTLGD -- ��ΥԼ��¶ΥԼ��ʧ��
    ,
     EQUITYEXPOFLAG -- ��Ȩ��¶��ʶ
    ,
     EQUITYINVESTTYPE -- ��ȨͶ�ʶ�������
    ,
     EQUITYINVESTCAUSE -- ��ȨͶ���γ�ԭ��
    ,
     SLFLAG -- רҵ�����ʶ
    ,
     SLTYPE -- רҵ��������
    ,
     PFPHASE -- ��Ŀ���ʽ׶�
    ,
     REGURATING -- �������
    ,
     CBRCMPRATINGFLAG -- ������϶������Ƿ��Ϊ����
    ,
     LARGEFLUCFLAG -- �Ƿ񲨶��Խϴ�
    ,
     LIQUEXPOFLAG -- �Ƿ���������з��ձ�¶
    ,
     PAYMENTDEALFLAG -- �Ƿ����Ը�ģʽ
    ,
     DELAYTRADINGDAYS -- �ӳٽ�������
    ,
     SECURITIESFLAG -- �м�֤ȯ��ʶ
    ,
     SECUISSUERID -- ֤ȯ������ID
    ,
     RATINGDURATIONTYPE -- ������������
    ,
     SECUISSUERATING -- ֤ȯ���еȼ�
    ,
     SECURESIDUALM -- ֤ȯʣ������
    ,
     SECUREVAFREQUENCY -- ֤ȯ�ع�Ƶ��
    ,
     CCPTRANFLAG -- �Ƿ����뽻�׶�����ؽ���
    ,
     CCPID -- ���뽻�׶���ID
    ,
     QUALCCPFLAG -- �Ƿ�ϸ����뽻�׶���
    ,
     BANKROLE -- ���н�ɫ
    ,
     CLEARINGMETHOD -- ���㷽ʽ
    ,
     BANKASSETFLAG -- �Ƿ������ύ�ʲ�
    ,
     MATCHCONDITIONS -- �����������
    ,
     SFTFLAG -- ֤ȯ���ʽ��ױ�ʶ
    ,
     MASTERNETAGREEFLAG -- ���������Э���ʶ
    ,
     MASTERNETAGREEID -- ���������Э��ID
    ,
     SFTTYPE -- ֤ȯ���ʽ�������
    ,
     SECUOWNERTRANSFLAG -- ֤ȯ����Ȩ�Ƿ�ת��
    ,
     OTCFLAG -- �����������߱�ʶ
    ,
     VALIDNETTINGFLAG -- ��Ч�������Э���ʶ
    ,
     VALIDNETAGREEMENTID -- ��Ч�������Э��ID
    ,
     OTCTYPE -- ����������������
    ,
     DEPOSITRISKPERIOD -- ��֤������ڼ�
    ,
     MTM -- ���óɱ�
    ,
     MTMCURRENCY -- ���óɱ�����
    ,
     BUYERORSELLER -- ������
    ,
     QUALROFLAG -- �ϸ�����ʲ���ʶ
    ,
     ROISSUERPERFORMFLAG -- �����ʲ��������Ƿ�����Լ
    ,
     BUYERINSOLVENCYFLAG -- ���ñ������Ƿ��Ʋ�
    ,
     NONPAYMENTFEES -- ��δ֧������
    ,
     RETAILEXPOFLAG -- ���۱�¶��ʶ
    ,
     RETAILCLAIMTYPE -- ����ծȨ����
    ,
     MORTGAGETYPE -- ס����Ѻ��������
    ,
     DEBTORNUMBER -- ����˸���
    ,
     EXPONUMBER -- ���ձ�¶����
    ,
     PDPOOLMODELID -- PD�ֳ�ģ��ID
    ,
     LGDPOOLMODELID -- LGD�ֳ�ģ��ID
    ,
     CCFPOOLMODELID -- CCF�ֳ�ģ��ID
    ,
     PDPOOLID -- ����PD��ID
    ,
     LGDPOOLID -- ����LGD��ID
    ,
     CCFPOOLID -- ����CCF��ID
    ,
     ABSUAFLAG -- �ʲ�֤ȯ�������ʲ���ʶ
    ,
     ABSPOOLID -- ֤ȯ���ʲ���ID
    ,
     ABSPROPORTION -- �ʲ�֤ȯ������
    ,
     GROUPID -- ������
    ,
     ORGSORTNO --�������������
    ,
     LTV --�����ֵ��
    ,
     AGING --����
    ,
     NEWDEFAULTDEBTFLAG --����ΥԼծ���ʶ
    ,
     DEFAULTDATE --ΥԼʱ��
     )
    SELECT DATADATE AS DATADATE -- ��������
          ,
           DATANO AS DATANO -- ������ˮ��
          ,
           EXPOSUREID AS EXPOSUREID -- ���ձ�¶ID
          ,
           DUEID AS DUEID -- ծ��ID
          ,
           SSYSID AS SSYSID -- ԴϵͳID
          ,
           CONTRACTID AS CONTRACTID -- ��ͬID
          ,
           CLIENTID AS CLIENTID -- ��������ID
          ,
           SORGID AS SORGID -- Դ����ID
          ,
           SORGNAME AS SORGNAME -- Դ��������
          ,
           ORGID AS ORGID -- ��������ID
          ,
           ORGNAME AS ORGNAME -- ������������
          ,
           ACCORGID AS ACCORGID -- �������ID
          ,
           ACCORGNAME AS ACCORGNAME -- �����������
          ,
           CASE
             WHEN CREDITRISKDATATYPE <> '02' AND INDUSTRYID IS NULL THEN
              'J66'
             ELSE
              INDUSTRYID
           END AS INDUSTRYID --������ҵ����
          ,
           CASE
             WHEN CREDITRISKDATATYPE <> '02' AND INDUSTRYNAME IS NULL THEN
              '���ҽ��ڷ���'
             ELSE
              NVL(INDUSTRYNAME,'δ֪' �� END AS INDUSTRYNAME --������ҵ����
          , BUSINESSLINE AS BUSINESSLINE -- ����
          , ASSETTYPE AS ASSETTYPE -- �ʲ�����
          , ASSETSUBTYPE AS ASSETSUBTYPE -- �ʲ�С��
          , BUSINESSTYPEID AS BUSINESSTYPEID -- ҵ��Ʒ�ִ���
          , BUSINESSTYPENAME AS BUSINESSTYPENAME -- ҵ��Ʒ������
          , CREDITRISKDATATYPE AS CREDITRISKDATATYPE -- ���÷�����������
          , ASSETTYPEOFHAIRCUTS AS ASSETTYPEOFHAIRCUTS -- �ۿ�ϵ����Ӧ�ʲ����
          , BUSINESSTYPESTD AS BUSINESSTYPESTD -- Ȩ�ط�ҵ������
          , EXPOCLASSSTD AS EXPOCLASSSTD -- Ȩ�ط���¶����
          , EXPOSUBCLASSSTD AS EXPOSUBCLASSSTD -- Ȩ�ط���¶С��
          , EXPOCLASSIRB AS EXPOCLASSIRB -- ��������¶����
          , EXPOSUBCLASSIRB AS EXPOSUBCLASSIRB -- ��������¶С��
          , EXPOBELONG AS EXPOBELONG -- ��¶������ʶ
          , BOOKTYPE AS BOOKTYPE -- �˻����
          , REGUTRANTYPE AS REGUTRANTYPE -- ��ܽ�������
          , REPOTRANFLAG AS REPOTRANFLAG -- �ع����ױ�ʶ
          , REVAFREQUENCY AS REVAFREQUENCY -- �ع�Ƶ��
          , CURRENCY AS CURRENCY -- ����
          , NORMALPRINCIPAL AS NORMALPRINCIPAL -- �����������
          , OVERDUEBALANCE AS OVERDUEBALANCE -- �������
          , NONACCRUALBALANCE AS NONACCRUALBALANCE -- ��Ӧ�����
          , ONSHEETBALANCE AS ONSHEETBALANCE -- �������
          , NORMALINTEREST AS NORMALINTEREST -- ������Ϣ
          , ONDEBITINTEREST AS ONDEBITINTEREST -- ����ǷϢ
          , OFFDEBITINTEREST AS OFFDEBITINTEREST -- ����ǷϢ
          , EXPENSERECEIVABLE AS EXPENSERECEIVABLE -- Ӧ�շ���
          , ASSETBALANCE AS ASSETBALANCE -- �ʲ����
          , ACCSUBJECT1 AS ACCSUBJECT1 -- ��Ŀһ
          , ACCSUBJECT2 AS ACCSUBJECT2 -- ��Ŀ��
          , ACCSUBJECT3 AS ACCSUBJECT3 -- ��Ŀ��
          , TO_CHAR(TO_DATE(STARTDATE, 'YYYY-MM-DD'), 'YYYY-MM-DD') AS STARTDATE -- ��ʼ����
          , TO_CHAR(TO_DATE(DUEDATE, 'YYYY-MM-DD'), 'YYYY-MM-DD') AS DUEDATE -- ��������
          , ORIGINALMATURITY AS ORIGINALMATURITY -- ԭʼ����
          , RESIDUALM AS RESIDUALM -- ʣ������
          , RISKCLASSIFY AS RISKCLASSIFY -- ���շ���
          , EXPOSURESTATUS AS EXPOSURESTATUS -- ���ձ�¶״̬
          , OVERDUEDAYS AS OVERDUEDAYS -- ��������
          , SPECIALPROVISION AS SPECIALPROVISION -- ר��׼����
          , GENERALPROVISION AS GENERALPROVISION -- һ��׼����
          , ESPECIALPROVISION AS ESPECIALPROVISION -- �ر�׼����
          , WRITTENOFFAMOUNT AS WRITTENOFFAMOUNT -- �Ѻ������
          , OFFEXPOSOURCE AS OFFEXPOSOURCE -- ���Ⱪ¶��Դ
          , OFFBUSINESSTYPE AS OFFBUSINESSTYPE -- ����ҵ������
          , OFFBUSINESSSDVSSTD AS OFFBUSINESSSDVSSTD -- Ȩ�ط�����ҵ������ϸ��
          , UNCONDCANCELFLAG AS UNCONDCANCELFLAG -- �Ƿ����ʱ����������
          , CCFLEVEL AS CCFLEVEL -- ����ת��ϵ������
          , CCFAIRB AS CCFAIRB -- �߼�������ת��ϵ��
          , CLAIMSLEVEL AS CLAIMSLEVEL -- ծȨ����
          , BONDFLAG AS BONDFLAG -- �Ƿ�Ϊծȯ
          , BONDISSUEINTENT AS BONDISSUEINTENT -- ծȯ����Ŀ��
          , NSUREALPROPERTYFLAG AS NSUREALPROPERTYFLAG -- �Ƿ�����ò�����
          , REPASSETTERMTYPE AS REPASSETTERMTYPE -- ��ծ�ʲ���������
          , DEPENDONFPOBFLAG AS DEPENDONFPOBFLAG -- �Ƿ�����������δ��ӯ��
          , IRATING AS IRATING -- �ڲ�����
          , PD AS PD -- ΥԼ����
          , LGDLEVEL AS LGDLEVEL -- ΥԼ��ʧ�ʼ���
          , LGDAIRB AS LGDAIRB -- �߼���ΥԼ��ʧ��
          , MAIRB AS MAIRB -- �߼�����Ч����
          , EADAIRB AS EADAIRB -- �߼���ΥԼ���ձ�¶
          , DEFAULTFLAG AS DEFAULTFLAG -- ΥԼ��ʶ
          , BEEL AS BEEL -- ��ΥԼ��¶Ԥ����ʧ����
          , DEFAULTLGD AS DEFAULTLGD -- ��ΥԼ��¶ΥԼ��ʧ��
          , EQUITYEXPOFLAG AS EQUITYEXPOFLAG -- ��Ȩ��¶��ʶ
          , EQUITYINVESTTYPE AS EQUITYINVESTTYPE -- ��ȨͶ�ʶ�������
          , EQUITYINVESTCAUSE AS EQUITYINVESTCAUSE -- ��ȨͶ���γ�ԭ��
          , SLFLAG AS SLFLAG -- רҵ�����ʶ
          , SLTYPE AS SLTYPE -- רҵ��������
          , PFPHASE AS PFPHASE -- ��Ŀ���ʽ׶�
          , REGURATING AS REGURATING -- �������
          , CBRCMPRATINGFLAG AS CBRCMPRATINGFLAG -- ������϶������Ƿ��Ϊ����
          , LARGEFLUCFLAG AS LARGEFLUCFLAG -- �Ƿ񲨶��Խϴ�
          , LIQUEXPOFLAG AS LIQUEXPOFLAG -- �Ƿ���������з��ձ�¶
          , PAYMENTDEALFLAG AS PAYMENTDEALFLAG -- �Ƿ����Ը�ģʽ
          , DELAYTRADINGDAYS AS DELAYTRADINGDAYS -- �ӳٽ�������
          , SECURITIESFLAG AS SECURITIESFLAG -- �м�֤ȯ��ʶ
          , SECUISSUERID AS SECUISSUERID -- ֤ȯ������ID
          , RATINGDURATIONTYPE AS RATINGDURATIONTYPE -- ������������
          , SECUISSUERATING AS SECUISSUERATING -- ֤ȯ���еȼ�
          , SECURESIDUALM AS SECURESIDUALM -- ֤ȯʣ������
          , SECUREVAFREQUENCY AS SECUREVAFREQUENCY -- ֤ȯ�ع�Ƶ��
          , CCPTRANFLAG AS CCPTRANFLAG -- �Ƿ����뽻�׶�����ؽ���
          , CCPID AS CCPID -- ���뽻�׶���ID
          , QUALCCPFLAG AS QUALCCPFLAG -- �Ƿ�ϸ����뽻�׶���
          , BANKROLE AS BANKROLE -- ���н�ɫ
          , CLEARINGMETHOD AS CLEARINGMETHOD -- ���㷽ʽ
          , BANKASSETFLAG AS BANKASSETFLAG -- �Ƿ������ύ�ʲ�
          , MATCHCONDITIONS AS MATCHCONDITIONS -- �����������
          , SFTFLAG AS SFTFLAG -- ֤ȯ���ʽ��ױ�ʶ
          , MASTERNETAGREEFLAG AS MASTERNETAGREEFLAG -- ���������Э���ʶ
          , MASTERNETAGREEID AS MASTERNETAGREEID -- ���������Э��ID
          , SFTTYPE AS SFTTYPE -- ֤ȯ���ʽ�������
          , SECUOWNERTRANSFLAG AS SECUOWNERTRANSFLAG -- ֤ȯ����Ȩ�Ƿ�ת��
          , OTCFLAG AS OTCFLAG -- �����������߱�ʶ
          , VALIDNETTINGFLAG AS VALIDNETTINGFLAG -- ��Ч�������Э���ʶ
          , VALIDNETAGREEMENTID AS VALIDNETAGREEMENTID -- ��Ч�������Э��ID
          , OTCTYPE AS OTCTYPE -- ����������������
          , DEPOSITRISKPERIOD AS DEPOSITRISKPERIOD -- ��֤������ڼ�
          , MTM AS MTM -- ���óɱ�
          , MTMCURRENCY AS MTMCURRENCY -- ���óɱ�����
          , BUYERORSELLER AS BUYERORSELLER -- ������
          , QUALROFLAG AS QUALROFLAG -- �ϸ�����ʲ���ʶ
          , ROISSUERPERFORMFLAG AS ROISSUERPERFORMFLAG -- �����ʲ��������Ƿ�����Լ
          , BUYERINSOLVENCYFLAG AS BUYERINSOLVENCYFLAG -- ���ñ������Ƿ��Ʋ�
          , NONPAYMENTFEES AS NONPAYMENTFEES -- ��δ֧������
          , RETAILEXPOFLAG AS RETAILEXPOFLAG -- ���۱�¶��ʶ
          , RETAILCLAIMTYPE AS RETAILCLAIMTYPE -- ����ծȨ����
          , MORTGAGETYPE AS MORTGAGETYPE -- ס����Ѻ��������
          , DEBTORNUMBER AS DEBTORNUMBER -- ����˸���
          , EXPONUMBER AS EXPONUMBER -- ���ձ�¶����
          , PDPOOLMODELID AS PDPOOLMODELID -- PD�ֳ�ģ��ID
          , LGDPOOLMODELID AS LGDPOOLMODELID -- LGD�ֳ�ģ��ID
          , CCFPOOLMODELID AS CCFPOOLMODELID -- CCF�ֳ�ģ��ID
          , PDPOOLID AS PDPOOLID -- ����PD��ID
          , LGDPOOLID AS LGDPOOLID -- ����LGD��ID
          , CCFPOOLID AS CCFPOOLID -- ����CCF��ID
          , ABSUAFLAG AS ABSUAFLAG -- �ʲ�֤ȯ�������ʲ���ʶ
          , ABSPOOLID AS ABSPOOLID -- ֤ȯ���ʲ���ID
          , ABSPROPORTION AS ABSPROPORTION -- �ʲ�֤ȯ������
          , GROUPID AS GROUPID -- ������
          , ORGSORTNO AS ORGSORTNO --�������������
          , LTV AS LTV --�����ֵ��
          , AGING AS AGING --����
          , NEWDEFAULTDEBTFLAG AS NEWDEFAULTDEBTFLAG --����ΥԼծ���ʶ
          , DEFAULTDATE AS DEFAULTDATE --ΥԼʱ��
           FROM RWA_DEV.RWA_XD_EXPOSURE WHERE DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD') and accsubject1 <> '70230101' ---20191014 by wzb
           AND ACCSUBJECT1 NOT LIKE '@%' --20191123 BY YSJ
           AND ASSETBALANCE > 0 ----20191024 BY WZB
           ;
  COMMIT;

  /*����ع��ķ��ձ�¶��Ϣ*/
  INSERT /*+ APPEND */
  INTO RWA_DEV.RWA_EI_EXPOSURE
    (DATADATE -- ��������
    ,
     DATANO -- ������ˮ��
    ,
     EXPOSUREID -- ���ձ�¶ID
    ,
     DUEID -- ծ��ID
    ,
     SSYSID -- ԴϵͳID
    ,
     CONTRACTID -- ��ͬID
    ,
     CLIENTID -- ��������ID
    ,
     SORGID -- Դ����ID
    ,
     SORGNAME -- Դ��������
    ,
     ORGID -- ��������ID
    ,
     ORGNAME -- ������������
    ,
     ACCORGID -- �������ID
    ,
     ACCORGNAME -- �����������
    ,
     INDUSTRYID -- ������ҵ����
    ,
     INDUSTRYNAME -- ������ҵ����
    ,
     BUSINESSLINE -- ����
    ,
     ASSETTYPE -- �ʲ�����
    ,
     ASSETSUBTYPE -- �ʲ�С��
    ,
     BUSINESSTYPEID -- ҵ��Ʒ�ִ���
    ,
     BUSINESSTYPENAME -- ҵ��Ʒ������
    ,
     CREDITRISKDATATYPE -- ���÷�����������
    ,
     ASSETTYPEOFHAIRCUTS -- �ۿ�ϵ����Ӧ�ʲ����
    ,
     BUSINESSTYPESTD -- Ȩ�ط�ҵ������
    ,
     EXPOCLASSSTD -- Ȩ�ط���¶����
    ,
     EXPOSUBCLASSSTD -- Ȩ�ط���¶С��
    ,
     EXPOCLASSIRB -- ��������¶����
    ,
     EXPOSUBCLASSIRB -- ��������¶С��
    ,
     EXPOBELONG -- ��¶������ʶ
    ,
     BOOKTYPE -- �˻����
    ,
     REGUTRANTYPE -- ��ܽ�������
    ,
     REPOTRANFLAG -- �ع����ױ�ʶ
    ,
     REVAFREQUENCY -- �ع�Ƶ��
    ,
     CURRENCY -- ����
    ,
     NORMALPRINCIPAL -- �����������
    ,
     OVERDUEBALANCE -- �������
    ,
     NONACCRUALBALANCE -- ��Ӧ�����
    ,
     ONSHEETBALANCE -- �������
    ,
     NORMALINTEREST -- ������Ϣ
    ,
     ONDEBITINTEREST -- ����ǷϢ
    ,
     OFFDEBITINTEREST -- ����ǷϢ
    ,
     EXPENSERECEIVABLE -- Ӧ�շ���
    ,
     ASSETBALANCE -- �ʲ����
    ,
     ACCSUBJECT1 -- ��Ŀһ
    ,
     ACCSUBJECT2 -- ��Ŀ��
    ,
     ACCSUBJECT3 -- ��Ŀ��
    ,
     STARTDATE -- ��ʼ����
    ,
     DUEDATE -- ��������
    ,
     ORIGINALMATURITY -- ԭʼ����
    ,
     RESIDUALM -- ʣ������
    ,
     RISKCLASSIFY -- ���շ���
    ,
     EXPOSURESTATUS -- ���ձ�¶״̬
    ,
     OVERDUEDAYS -- ��������
    ,
     SPECIALPROVISION -- ר��׼����
    ,
     GENERALPROVISION -- һ��׼����
    ,
     ESPECIALPROVISION -- �ر�׼����
    ,
     WRITTENOFFAMOUNT -- �Ѻ������
    ,
     OFFEXPOSOURCE -- ���Ⱪ¶��Դ
    ,
     OFFBUSINESSTYPE -- ����ҵ������
    ,
     OFFBUSINESSSDVSSTD -- Ȩ�ط�����ҵ������ϸ��
    ,
     UNCONDCANCELFLAG -- �Ƿ����ʱ����������
    ,
     CCFLEVEL -- ����ת��ϵ������
    ,
     CCFAIRB -- �߼�������ת��ϵ��
    ,
     CLAIMSLEVEL -- ծȨ����
    ,
     BONDFLAG -- �Ƿ�Ϊծȯ
    ,
     BONDISSUEINTENT -- ծȯ����Ŀ��
    ,
     NSUREALPROPERTYFLAG -- �Ƿ�����ò�����
    ,
     REPASSETTERMTYPE -- ��ծ�ʲ���������
    ,
     DEPENDONFPOBFLAG -- �Ƿ�����������δ��ӯ��
    ,
     IRATING -- �ڲ�����
    ,
     PD -- ΥԼ����
    ,
     LGDLEVEL -- ΥԼ��ʧ�ʼ���
    ,
     LGDAIRB -- �߼���ΥԼ��ʧ��
    ,
     MAIRB -- �߼�����Ч����
    ,
     EADAIRB -- �߼���ΥԼ���ձ�¶
    ,
     DEFAULTFLAG -- ΥԼ��ʶ
    ,
     BEEL -- ��ΥԼ��¶Ԥ����ʧ����
    ,
     DEFAULTLGD -- ��ΥԼ��¶ΥԼ��ʧ��
    ,
     EQUITYEXPOFLAG -- ��Ȩ��¶��ʶ
    ,
     EQUITYINVESTTYPE -- ��ȨͶ�ʶ�������
    ,
     EQUITYINVESTCAUSE -- ��ȨͶ���γ�ԭ��
    ,
     SLFLAG -- רҵ�����ʶ
    ,
     SLTYPE -- רҵ��������
    ,
     PFPHASE -- ��Ŀ���ʽ׶�
    ,
     REGURATING -- �������
    ,
     CBRCMPRATINGFLAG -- ������϶������Ƿ��Ϊ����
    ,
     LARGEFLUCFLAG -- �Ƿ񲨶��Խϴ�
    ,
     LIQUEXPOFLAG -- �Ƿ���������з��ձ�¶
    ,
     PAYMENTDEALFLAG -- �Ƿ����Ը�ģʽ
    ,
     DELAYTRADINGDAYS -- �ӳٽ�������
    ,
     SECURITIESFLAG -- �м�֤ȯ��ʶ
    ,
     SECUISSUERID -- ֤ȯ������ID
    ,
     RATINGDURATIONTYPE -- ������������
    ,
     SECUISSUERATING -- ֤ȯ���еȼ�
    ,
     SECURESIDUALM -- ֤ȯʣ������
    ,
     SECUREVAFREQUENCY -- ֤ȯ�ع�Ƶ��
    ,
     CCPTRANFLAG -- �Ƿ����뽻�׶�����ؽ���
    ,
     CCPID -- ���뽻�׶���ID
    ,
     QUALCCPFLAG -- �Ƿ�ϸ����뽻�׶���
    ,
     BANKROLE -- ���н�ɫ
    ,
     CLEARINGMETHOD -- ���㷽ʽ
    ,
     BANKASSETFLAG -- �Ƿ������ύ�ʲ�
    ,
     MATCHCONDITIONS -- �����������
    ,
     SFTFLAG -- ֤ȯ���ʽ��ױ�ʶ
    ,
     MASTERNETAGREEFLAG -- ���������Э���ʶ
    ,
     MASTERNETAGREEID -- ���������Э��ID
    ,
     SFTTYPE -- ֤ȯ���ʽ�������
    ,
     SECUOWNERTRANSFLAG -- ֤ȯ����Ȩ�Ƿ�ת��
    ,
     OTCFLAG -- �����������߱�ʶ
    ,
     VALIDNETTINGFLAG -- ��Ч�������Э���ʶ
    ,
     VALIDNETAGREEMENTID -- ��Ч�������Э��ID
    ,
     OTCTYPE -- ����������������
    ,
     DEPOSITRISKPERIOD -- ��֤������ڼ�
    ,
     MTM -- ���óɱ�
    ,
     MTMCURRENCY -- ���óɱ�����
    ,
     BUYERORSELLER -- ������
    ,
     QUALROFLAG -- �ϸ�����ʲ���ʶ
    ,
     ROISSUERPERFORMFLAG -- �����ʲ��������Ƿ�����Լ
    ,
     BUYERINSOLVENCYFLAG -- ���ñ������Ƿ��Ʋ�
    ,
     NONPAYMENTFEES -- ��δ֧������
    ,
     RETAILEXPOFLAG -- ���۱�¶��ʶ
    ,
     RETAILCLAIMTYPE -- ����ծȨ����
    ,
     MORTGAGETYPE -- ס����Ѻ��������
    ,
     DEBTORNUMBER -- ����˸���
    ,
     EXPONUMBER -- ���ձ�¶����
    ,
     PDPOOLMODELID -- PD�ֳ�ģ��ID
    ,
     LGDPOOLMODELID -- LGD�ֳ�ģ��ID
    ,
     CCFPOOLMODELID -- CCF�ֳ�ģ��ID
    ,
     PDPOOLID -- ����PD��ID
    ,
     LGDPOOLID -- ����LGD��ID
    ,
     CCFPOOLID -- ����CCF��ID
    ,
     ABSUAFLAG -- �ʲ�֤ȯ�������ʲ���ʶ
    ,
     ABSPOOLID -- ֤ȯ���ʲ���ID
    ,
     ABSPROPORTION -- �ʲ�֤ȯ������
    ,
     GROUPID -- ������
    ,
     ORGSORTNO --�������������
    ,
     LTV --�����ֵ��
    ,
     AGING --����
    ,
     NEWDEFAULTDEBTFLAG --����ΥԼծ���ʶ
    ,
     DEFAULTDATE --ΥԼʱ��
     )
    SELECT DATADATE   AS DATADATE -- ��������
          ,
           DATANO     AS DATANO -- ������ˮ��
          ,
           EXPOSUREID AS EXPOSUREID -- ���ձ�¶ID
          ,
           DUEID      AS DUEID -- ծ��ID
          ,
           SSYSID     AS SSYSID -- ԴϵͳID
          ,
           CONTRACTID AS CONTRACTID -- ��ͬID
          ,
           CLIENTID   AS CLIENTID -- ��������ID
          ,
           SORGID     AS SORGID -- Դ����ID
          ,
           SORGNAME   AS SORGNAME -- Դ��������
          ,
           ORGID      AS ORGID -- ��������ID
          ,
           ORGNAME    AS ORGNAME -- ������������
          ,
           ACCORGID   AS ACCORGID -- �������ID
          ,
           ACCORGNAME AS ACCORGNAME -- �����������
          ,
           INDUSTRYID AS INDUSTRYID --������ҵ����
          ,
           NVL       (INDUSTRYNAME, 'δ֪'        �� AS INDUSTRYNAME --������ҵ����
                  , BUSINESSLINE AS BUSINESSLINE -- ����
                  , ASSETTYPE AS ASSETTYPE -- �ʲ�����
                  , ASSETSUBTYPE AS ASSETSUBTYPE -- �ʲ�С��
                  , BUSINESSTYPEID AS BUSINESSTYPEID -- ҵ��Ʒ�ִ���
                  , BUSINESSTYPENAME AS BUSINESSTYPENAME -- ҵ��Ʒ������
                  , CREDITRISKDATATYPE AS CREDITRISKDATATYPE -- ���÷�����������
                  , ASSETTYPEOFHAIRCUTS AS ASSETTYPEOFHAIRCUTS -- �ۿ�ϵ����Ӧ�ʲ����
                  , BUSINESSTYPESTD AS BUSINESSTYPESTD -- Ȩ�ط�ҵ������
                  , EXPOCLASSSTD AS EXPOCLASSSTD -- Ȩ�ط���¶����
                  , EXPOSUBCLASSSTD AS EXPOSUBCLASSSTD -- Ȩ�ط���¶С��
                  , EXPOCLASSIRB AS EXPOCLASSIRB -- ��������¶����
                  , EXPOSUBCLASSIRB AS EXPOSUBCLASSIRB -- ��������¶С��
                  , EXPOBELONG AS EXPOBELONG -- ��¶������ʶ
                  , BOOKTYPE AS BOOKTYPE -- �˻����
                  , REGUTRANTYPE AS REGUTRANTYPE -- ��ܽ�������
                  , REPOTRANFLAG AS REPOTRANFLAG -- �ع����ױ�ʶ
                  , REVAFREQUENCY AS REVAFREQUENCY -- �ع�Ƶ��
                  , CURRENCY AS CURRENCY -- ����
                  , NORMALPRINCIPAL AS NORMALPRINCIPAL -- �����������
                  , OVERDUEBALANCE AS OVERDUEBALANCE -- �������
                  , NONACCRUALBALANCE AS NONACCRUALBALANCE -- ��Ӧ�����
                  , ONSHEETBALANCE AS ONSHEETBALANCE -- �������
                  , NORMALINTEREST AS NORMALINTEREST -- ������Ϣ
                  , ONDEBITINTEREST AS ONDEBITINTEREST -- ����ǷϢ
                  , OFFDEBITINTEREST AS OFFDEBITINTEREST -- ����ǷϢ
                  , EXPENSERECEIVABLE AS EXPENSERECEIVABLE -- Ӧ�շ���
                  , ASSETBALANCE AS ASSETBALANCE -- �ʲ����
                  , ACCSUBJECT1 AS ACCSUBJECT1 -- ��Ŀһ
                  , ACCSUBJECT2 AS ACCSUBJECT2 -- ��Ŀ��
                  , ACCSUBJECT3 AS ACCSUBJECT3 -- ��Ŀ��
                  , TO_CHAR(TO_DATE(STARTDATE, 'YYYY-MM-DD'       ), 'YYYY-MM-DD'       ) AS STARTDATE -- ��ʼ����
                  , TO_CHAR(TO_DATE(DUEDATE, 'YYYY-MM-DD'       ), 'YYYY-MM-DD'       ) AS DUEDATE -- ��������
                  , ORIGINALMATURITY AS ORIGINALMATURITY -- ԭʼ����
                  , RESIDUALM AS RESIDUALM -- ʣ������
                  , RISKCLASSIFY AS RISKCLASSIFY -- ���շ���
                  , EXPOSURESTATUS AS EXPOSURESTATUS -- ���ձ�¶״̬
                  , OVERDUEDAYS AS OVERDUEDAYS -- ��������
                  , SPECIALPROVISION AS SPECIALPROVISION -- ר��׼����
                  , GENERALPROVISION AS GENERALPROVISION -- һ��׼����
                  , ESPECIALPROVISION AS ESPECIALPROVISION -- �ر�׼����
                  , WRITTENOFFAMOUNT AS WRITTENOFFAMOUNT -- �Ѻ������
                  , OFFEXPOSOURCE AS OFFEXPOSOURCE -- ���Ⱪ¶��Դ
                  , OFFBUSINESSTYPE AS OFFBUSINESSTYPE -- ����ҵ������
                  , OFFBUSINESSSDVSSTD AS OFFBUSINESSSDVSSTD -- Ȩ�ط�����ҵ������ϸ��
                  , UNCONDCANCELFLAG AS UNCONDCANCELFLAG -- �Ƿ����ʱ����������
                  , CCFLEVEL AS CCFLEVEL -- ����ת��ϵ������
                  , CCFAIRB AS CCFAIRB -- �߼�������ת��ϵ��
                  , CLAIMSLEVEL AS CLAIMSLEVEL -- ծȨ����
                  , BONDFLAG AS BONDFLAG -- �Ƿ�Ϊծȯ
                  , BONDISSUEINTENT AS BONDISSUEINTENT -- ծȯ����Ŀ��
                  , NSUREALPROPERTYFLAG AS NSUREALPROPERTYFLAG -- �Ƿ�����ò�����
                  , REPASSETTERMTYPE AS REPASSETTERMTYPE -- ��ծ�ʲ���������
                  , DEPENDONFPOBFLAG AS DEPENDONFPOBFLAG -- �Ƿ�����������δ��ӯ��
                  , IRATING AS IRATING -- �ڲ�����
                  , PD AS PD -- ΥԼ����
                  , LGDLEVEL AS LGDLEVEL -- ΥԼ��ʧ�ʼ���
                  , LGDAIRB AS LGDAIRB -- �߼���ΥԼ��ʧ��
                  , MAIRB AS MAIRB -- �߼�����Ч����
                  , EADAIRB AS EADAIRB -- �߼���ΥԼ���ձ�¶
                  , DEFAULTFLAG AS DEFAULTFLAG -- ΥԼ��ʶ
                  , BEEL AS BEEL -- ��ΥԼ��¶Ԥ����ʧ����
                  , DEFAULTLGD AS DEFAULTLGD -- ��ΥԼ��¶ΥԼ��ʧ��
                  , EQUITYEXPOFLAG AS EQUITYEXPOFLAG -- ��Ȩ��¶��ʶ
                  , EQUITYINVESTTYPE AS EQUITYINVESTTYPE -- ��ȨͶ�ʶ�������
                  , EQUITYINVESTCAUSE AS EQUITYINVESTCAUSE -- ��ȨͶ���γ�ԭ��
                  , SLFLAG AS SLFLAG -- רҵ�����ʶ
                  , SLTYPE AS SLTYPE -- רҵ��������
                  , PFPHASE AS PFPHASE -- ��Ŀ���ʽ׶�
                  , REGURATING AS REGURATING -- �������
                  , CBRCMPRATINGFLAG AS CBRCMPRATINGFLAG -- ������϶������Ƿ��Ϊ����
                  , LARGEFLUCFLAG AS LARGEFLUCFLAG -- �Ƿ񲨶��Խϴ�
                  , LIQUEXPOFLAG AS LIQUEXPOFLAG -- �Ƿ���������з��ձ�¶
                  , PAYMENTDEALFLAG AS PAYMENTDEALFLAG -- �Ƿ����Ը�ģʽ
                  , DELAYTRADINGDAYS AS DELAYTRADINGDAYS -- �ӳٽ�������
                  , SECURITIESFLAG AS SECURITIESFLAG -- �м�֤ȯ��ʶ
                  , SECUISSUERID AS SECUISSUERID -- ֤ȯ������ID
                  , RATINGDURATIONTYPE AS RATINGDURATIONTYPE -- ������������
                  , SECUISSUERATING AS SECUISSUERATING -- ֤ȯ���еȼ�
                  , SECURESIDUALM AS SECURESIDUALM -- ֤ȯʣ������
                  , SECUREVAFREQUENCY AS SECUREVAFREQUENCY -- ֤ȯ�ع�Ƶ��
                  , CCPTRANFLAG AS CCPTRANFLAG -- �Ƿ����뽻�׶�����ؽ���
                  , CCPID AS CCPID -- ���뽻�׶���ID
                  , QUALCCPFLAG AS QUALCCPFLAG -- �Ƿ�ϸ����뽻�׶���
                  , BANKROLE AS BANKROLE -- ���н�ɫ
                  , CLEARINGMETHOD AS CLEARINGMETHOD -- ���㷽ʽ
                  , BANKASSETFLAG AS BANKASSETFLAG -- �Ƿ������ύ�ʲ�
                  , MATCHCONDITIONS AS MATCHCONDITIONS -- �����������
                  , SFTFLAG AS SFTFLAG -- ֤ȯ���ʽ��ױ�ʶ
                  , MASTERNETAGREEFLAG AS MASTERNETAGREEFLAG -- ���������Э���ʶ
                  , MASTERNETAGREEID AS MASTERNETAGREEID -- ���������Э��ID
                  , SFTTYPE AS SFTTYPE -- ֤ȯ���ʽ�������
                  , SECUOWNERTRANSFLAG AS SECUOWNERTRANSFLAG -- ֤ȯ����Ȩ�Ƿ�ת��
                  , OTCFLAG AS OTCFLAG -- �����������߱�ʶ
                  , VALIDNETTINGFLAG AS VALIDNETTINGFLAG -- ��Ч�������Э���ʶ
                  , VALIDNETAGREEMENTID AS VALIDNETAGREEMENTID -- ��Ч�������Э��ID
                  , OTCTYPE AS OTCTYPE -- ����������������
                  , DEPOSITRISKPERIOD AS DEPOSITRISKPERIOD -- ��֤������ڼ�
                  , MTM AS MTM -- ���óɱ�
                  , MTMCURRENCY AS MTMCURRENCY -- ���óɱ�����
                  , BUYERORSELLER AS BUYERORSELLER -- ������
                  , QUALROFLAG AS QUALROFLAG -- �ϸ�����ʲ���ʶ
                  , ROISSUERPERFORMFLAG AS ROISSUERPERFORMFLAG -- �����ʲ��������Ƿ�����Լ
                  , BUYERINSOLVENCYFLAG AS BUYERINSOLVENCYFLAG -- ���ñ������Ƿ��Ʋ�
                  , NONPAYMENTFEES AS NONPAYMENTFEES -- ��δ֧������
                  , RETAILEXPOFLAG AS RETAILEXPOFLAG -- ���۱�¶��ʶ
                  , RETAILCLAIMTYPE AS RETAILCLAIMTYPE -- ����ծȨ����
                  , MORTGAGETYPE AS MORTGAGETYPE -- ס����Ѻ��������
                  , DEBTORNUMBER AS DEBTORNUMBER -- ����˸���
                  , EXPONUMBER AS EXPONUMBER -- ���ձ�¶����
                  , PDPOOLMODELID AS PDPOOLMODELID -- PD�ֳ�ģ��ID
                  , LGDPOOLMODELID AS LGDPOOLMODELID -- LGD�ֳ�ģ��ID
                  , CCFPOOLMODELID AS CCFPOOLMODELID -- CCF�ֳ�ģ��ID
                  , PDPOOLID AS PDPOOLID -- ����PD��ID
                  , LGDPOOLID AS LGDPOOLID -- ����LGD��ID
                  , CCFPOOLID AS CCFPOOLID -- ����CCF��ID
                  , ABSUAFLAG AS ABSUAFLAG -- �ʲ�֤ȯ�������ʲ���ʶ
                  , ABSPOOLID AS ABSPOOLID -- ֤ȯ���ʲ���ID
                  , ABSPROPORTION AS ABSPROPORTION -- �ʲ�֤ȯ������
                  , GROUPID AS GROUPID -- ������
                  , ORGSORTNO AS ORGSORTNO --�������������
                  , LTV AS LTV --�����ֵ��
                  , AGING AS AGING --����
                  , NEWDEFAULTDEBTFLAG AS NEWDEFAULTDEBTFLAG --����ΥԼծ���ʶ
                  , DEFAULTDATE AS DEFAULTDATE --ΥԼʱ��
                   FROM RWA_DEV.RWA_HG_EXPOSURE WHERE DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD'       );
  COMMIT;

  /*������Ƶķ��ձ�¶��Ϣ*/
  INSERT /*+ APPEND */
  INTO RWA_DEV.RWA_EI_EXPOSURE
    (DATADATE -- ��������
    ,
     DATANO -- ������ˮ��
    ,
     EXPOSUREID -- ���ձ�¶ID
    ,
     DUEID -- ծ��ID
    ,
     SSYSID -- ԴϵͳID
    ,
     CONTRACTID -- ��ͬID
    ,
     CLIENTID -- ��������ID
    ,
     SORGID -- Դ����ID
    ,
     SORGNAME -- Դ��������
    ,
     ORGID -- ��������ID
    ,
     ORGNAME -- ������������
    ,
     ACCORGID -- �������ID
    ,
     ACCORGNAME -- �����������
    ,
     INDUSTRYID -- ������ҵ����
    ,
     INDUSTRYNAME -- ������ҵ����
    ,
     BUSINESSLINE -- ����
    ,
     ASSETTYPE -- �ʲ�����
    ,
     ASSETSUBTYPE -- �ʲ�С��
    ,
     BUSINESSTYPEID -- ҵ��Ʒ�ִ���
    ,
     BUSINESSTYPENAME -- ҵ��Ʒ������
    ,
     CREDITRISKDATATYPE -- ���÷�����������
    ,
     ASSETTYPEOFHAIRCUTS -- �ۿ�ϵ����Ӧ�ʲ����
    ,
     BUSINESSTYPESTD -- Ȩ�ط�ҵ������
    ,
     EXPOCLASSSTD -- Ȩ�ط���¶����
    ,
     EXPOSUBCLASSSTD -- Ȩ�ط���¶С��
    ,
     EXPOCLASSIRB -- ��������¶����
    ,
     EXPOSUBCLASSIRB -- ��������¶С��
    ,
     EXPOBELONG -- ��¶������ʶ
    ,
     BOOKTYPE -- �˻����
    ,
     REGUTRANTYPE -- ��ܽ�������
    ,
     REPOTRANFLAG -- �ع����ױ�ʶ
    ,
     REVAFREQUENCY -- �ع�Ƶ��
    ,
     CURRENCY -- ����
    ,
     NORMALPRINCIPAL -- �����������
    ,
     OVERDUEBALANCE -- �������
    ,
     NONACCRUALBALANCE -- ��Ӧ�����
    ,
     ONSHEETBALANCE -- �������
    ,
     NORMALINTEREST -- ������Ϣ
    ,
     ONDEBITINTEREST -- ����ǷϢ
    ,
     OFFDEBITINTEREST -- ����ǷϢ
    ,
     EXPENSERECEIVABLE -- Ӧ�շ���
    ,
     ASSETBALANCE -- �ʲ����
    ,
     ACCSUBJECT1 -- ��Ŀһ
    ,
     ACCSUBJECT2 -- ��Ŀ��
    ,
     ACCSUBJECT3 -- ��Ŀ��
    ,
     STARTDATE -- ��ʼ����
    ,
     DUEDATE -- ��������
    ,
     ORIGINALMATURITY -- ԭʼ����
    ,
     RESIDUALM -- ʣ������
    ,
     RISKCLASSIFY -- ���շ���
    ,
     EXPOSURESTATUS -- ���ձ�¶״̬
    ,
     OVERDUEDAYS -- ��������
    ,
     SPECIALPROVISION -- ר��׼����
    ,
     GENERALPROVISION -- һ��׼����
    ,
     ESPECIALPROVISION -- �ر�׼����
    ,
     WRITTENOFFAMOUNT -- �Ѻ������
    ,
     OFFEXPOSOURCE -- ���Ⱪ¶��Դ
    ,
     OFFBUSINESSTYPE -- ����ҵ������
    ,
     OFFBUSINESSSDVSSTD -- Ȩ�ط�����ҵ������ϸ��
    ,
     UNCONDCANCELFLAG -- �Ƿ����ʱ����������
    ,
     CCFLEVEL -- ����ת��ϵ������
    ,
     CCFAIRB -- �߼�������ת��ϵ��
    ,
     CLAIMSLEVEL -- ծȨ����
    ,
     BONDFLAG -- �Ƿ�Ϊծȯ
    ,
     BONDISSUEINTENT -- ծȯ����Ŀ��
    ,
     NSUREALPROPERTYFLAG -- �Ƿ�����ò�����
    ,
     REPASSETTERMTYPE -- ��ծ�ʲ���������
    ,
     DEPENDONFPOBFLAG -- �Ƿ�����������δ��ӯ��
    ,
     IRATING -- �ڲ�����
    ,
     PD -- ΥԼ����
    ,
     LGDLEVEL -- ΥԼ��ʧ�ʼ���
    ,
     LGDAIRB -- �߼���ΥԼ��ʧ��
    ,
     MAIRB -- �߼�����Ч����
    ,
     EADAIRB -- �߼���ΥԼ���ձ�¶
    ,
     DEFAULTFLAG -- ΥԼ��ʶ
    ,
     BEEL -- ��ΥԼ��¶Ԥ����ʧ����
    ,
     DEFAULTLGD -- ��ΥԼ��¶ΥԼ��ʧ��
    ,
     EQUITYEXPOFLAG -- ��Ȩ��¶��ʶ
    ,
     EQUITYINVESTTYPE -- ��ȨͶ�ʶ�������
    ,
     EQUITYINVESTCAUSE -- ��ȨͶ���γ�ԭ��
    ,
     SLFLAG -- רҵ�����ʶ
    ,
     SLTYPE -- רҵ��������
    ,
     PFPHASE -- ��Ŀ���ʽ׶�
    ,
     REGURATING -- �������
    ,
     CBRCMPRATINGFLAG -- ������϶������Ƿ��Ϊ����
    ,
     LARGEFLUCFLAG -- �Ƿ񲨶��Խϴ�
    ,
     LIQUEXPOFLAG -- �Ƿ���������з��ձ�¶
    ,
     PAYMENTDEALFLAG -- �Ƿ����Ը�ģʽ
    ,
     DELAYTRADINGDAYS -- �ӳٽ�������
    ,
     SECURITIESFLAG -- �м�֤ȯ��ʶ
    ,
     SECUISSUERID -- ֤ȯ������ID
    ,
     RATINGDURATIONTYPE -- ������������
    ,
     SECUISSUERATING -- ֤ȯ���еȼ�
    ,
     SECURESIDUALM -- ֤ȯʣ������
    ,
     SECUREVAFREQUENCY -- ֤ȯ�ع�Ƶ��
    ,
     CCPTRANFLAG -- �Ƿ����뽻�׶�����ؽ���
    ,
     CCPID -- ���뽻�׶���ID
    ,
     QUALCCPFLAG -- �Ƿ�ϸ����뽻�׶���
    ,
     BANKROLE -- ���н�ɫ
    ,
     CLEARINGMETHOD -- ���㷽ʽ
    ,
     BANKASSETFLAG -- �Ƿ������ύ�ʲ�
    ,
     MATCHCONDITIONS -- �����������
    ,
     SFTFLAG -- ֤ȯ���ʽ��ױ�ʶ
    ,
     MASTERNETAGREEFLAG -- ���������Э���ʶ
    ,
     MASTERNETAGREEID -- ���������Э��ID
    ,
     SFTTYPE -- ֤ȯ���ʽ�������
    ,
     SECUOWNERTRANSFLAG -- ֤ȯ����Ȩ�Ƿ�ת��
    ,
     OTCFLAG -- �����������߱�ʶ
    ,
     VALIDNETTINGFLAG -- ��Ч�������Э���ʶ
    ,
     VALIDNETAGREEMENTID -- ��Ч�������Э��ID
    ,
     OTCTYPE -- ����������������
    ,
     DEPOSITRISKPERIOD -- ��֤������ڼ�
    ,
     MTM -- ���óɱ�
    ,
     MTMCURRENCY -- ���óɱ�����
    ,
     BUYERORSELLER -- ������
    ,
     QUALROFLAG -- �ϸ�����ʲ���ʶ
    ,
     ROISSUERPERFORMFLAG -- �����ʲ��������Ƿ�����Լ
    ,
     BUYERINSOLVENCYFLAG -- ���ñ������Ƿ��Ʋ�
    ,
     NONPAYMENTFEES -- ��δ֧������
    ,
     RETAILEXPOFLAG -- ���۱�¶��ʶ
    ,
     RETAILCLAIMTYPE -- ����ծȨ����
    ,
     MORTGAGETYPE -- ס����Ѻ��������
    ,
     DEBTORNUMBER -- ����˸���
    ,
     EXPONUMBER -- ���ձ�¶����
    ,
     PDPOOLMODELID -- PD�ֳ�ģ��ID
    ,
     LGDPOOLMODELID -- LGD�ֳ�ģ��ID
    ,
     CCFPOOLMODELID -- CCF�ֳ�ģ��ID
    ,
     PDPOOLID -- ����PD��ID
    ,
     LGDPOOLID -- ����LGD��ID
    ,
     CCFPOOLID -- ����CCF��ID
    ,
     ABSUAFLAG -- �ʲ�֤ȯ�������ʲ���ʶ
    ,
     ABSPOOLID -- ֤ȯ���ʲ���ID
    ,
     ABSPROPORTION -- �ʲ�֤ȯ������
    ,
     GROUPID -- ������
    ,
     ORGSORTNO --�������������
    ,
     LTV --�����ֵ��
    ,
     AGING --����
    ,
     NEWDEFAULTDEBTFLAG --����ΥԼծ���ʶ
    ,
     DEFAULTDATE --ΥԼʱ��
     )
    SELECT DATADATE   AS DATADATE -- ��������
          ,
           DATANO     AS DATANO -- ������ˮ��
          ,
           EXPOSUREID AS EXPOSUREID -- ���ձ�¶ID
          ,
           DUEID      AS DUEID -- ծ��ID
          ,
           SSYSID     AS SSYSID -- ԴϵͳID
          ,
           CONTRACTID AS CONTRACTID -- ��ͬID
          ,
           CLIENTID   AS CLIENTID -- ��������ID
          ,
           SORGID     AS SORGID -- Դ����ID
          ,
           SORGNAME   AS SORGNAME -- Դ��������
          ,
           ORGID      AS ORGID -- ��������ID
          ,
           ORGNAME    AS ORGNAME -- ������������
          ,
           ACCORGID   AS ACCORGID -- �������ID
          ,
           ACCORGNAME AS ACCORGNAME -- �����������
          ,
           INDUSTRYID AS INDUSTRYID --������ҵ����
          ,
           NVL       (INDUSTRYNAME, 'δ֪'        �� AS INDUSTRYNAME --������ҵ����
                  , BUSINESSLINE AS BUSINESSLINE -- ����
                  , ASSETTYPE AS ASSETTYPE -- �ʲ�����
                  , ASSETSUBTYPE AS ASSETSUBTYPE -- �ʲ�С��
                  , BUSINESSTYPEID AS BUSINESSTYPEID -- ҵ��Ʒ�ִ���
                  , BUSINESSTYPENAME AS BUSINESSTYPENAME -- ҵ��Ʒ������
                  , CREDITRISKDATATYPE AS CREDITRISKDATATYPE -- ���÷�����������
                  , ASSETTYPEOFHAIRCUTS AS ASSETTYPEOFHAIRCUTS -- �ۿ�ϵ����Ӧ�ʲ����
                  , BUSINESSTYPESTD AS BUSINESSTYPESTD -- Ȩ�ط�ҵ������
                  , EXPOCLASSSTD AS EXPOCLASSSTD -- Ȩ�ط���¶����
                  , EXPOSUBCLASSSTD AS EXPOSUBCLASSSTD -- Ȩ�ط���¶С��
                  , EXPOCLASSIRB AS EXPOCLASSIRB -- ��������¶����
                  , EXPOSUBCLASSIRB AS EXPOSUBCLASSIRB -- ��������¶С��
                  , EXPOBELONG AS EXPOBELONG -- ��¶������ʶ
                  , BOOKTYPE AS BOOKTYPE -- �˻����
                  , REGUTRANTYPE AS REGUTRANTYPE -- ��ܽ�������
                  , REPOTRANFLAG AS REPOTRANFLAG -- �ع����ױ�ʶ
                  , REVAFREQUENCY AS REVAFREQUENCY -- �ع�Ƶ��
                  , CURRENCY AS CURRENCY -- ����
           --,decode(accsubject1,'15030201',nvl(SBJT_VAL4,'0')+NORMALPRINCIPAL,NORMALPRINCIPAL)           AS NORMALPRINCIPAL         -- �����������
                  , NORMALPRINCIPAL, OVERDUEBALANCE AS OVERDUEBALANCE -- �������
                  , NONACCRUALBALANCE AS NONACCRUALBALANCE -- ��Ӧ�����
                  , ONSHEETBALANCE AS ONSHEETBALANCE -- �������
                  , NORMALINTEREST AS NORMALINTEREST -- ������Ϣ
                  , ONDEBITINTEREST AS ONDEBITINTEREST -- ����ǷϢ
                  , OFFDEBITINTEREST AS OFFDEBITINTEREST -- ����ǷϢ
                  , EXPENSERECEIVABLE AS EXPENSERECEIVABLE -- Ӧ�շ���
           --,decode(accsubject1,'15030201',nvl(SBJT_VAL4,'0')+ASSETBALANCE,ASSETBALANCE)                                             AS ASSETBALANCE            -- �ʲ����
                  , NORMALPRINCIPAL, ACCSUBJECT1 AS ACCSUBJECT1 -- ��Ŀһ
                  , ACCSUBJECT2 AS ACCSUBJECT2 -- ��Ŀ��
                  , ACCSUBJECT3 AS ACCSUBJECT3 -- ��Ŀ��
                  , TO_CHAR(TO_DATE(STARTDATE, 'YYYY-MM-DD'       ), 'YYYY-MM-DD'       ) AS STARTDATE -- ��ʼ����
                  , TO_CHAR(TO_DATE(DUEDATE, 'YYYY-MM-DD'       ), 'YYYY-MM-DD'       ) AS DUEDATE -- ��������
                  , ORIGINALMATURITY AS ORIGINALMATURITY -- ԭʼ����
                  , RESIDUALM AS RESIDUALM -- ʣ������
                  , RISKCLASSIFY AS RISKCLASSIFY -- ���շ���
                  , EXPOSURESTATUS AS EXPOSURESTATUS -- ���ձ�¶״̬
                  , OVERDUEDAYS AS OVERDUEDAYS -- ��������
                  , SPECIALPROVISION AS SPECIALPROVISION -- ר��׼����
                  , GENERALPROVISION AS GENERALPROVISION -- һ��׼����
                  , ESPECIALPROVISION AS ESPECIALPROVISION -- �ر�׼����
                  , WRITTENOFFAMOUNT AS WRITTENOFFAMOUNT -- �Ѻ������
                  , OFFEXPOSOURCE AS OFFEXPOSOURCE -- ���Ⱪ¶��Դ
                  , OFFBUSINESSTYPE AS OFFBUSINESSTYPE -- ����ҵ������
                  , OFFBUSINESSSDVSSTD AS OFFBUSINESSSDVSSTD -- Ȩ�ط�����ҵ������ϸ��
                  , UNCONDCANCELFLAG AS UNCONDCANCELFLAG -- �Ƿ����ʱ����������
                  , CCFLEVEL AS CCFLEVEL -- ����ת��ϵ������
                  , CCFAIRB AS CCFAIRB -- �߼�������ת��ϵ��
                  , CLAIMSLEVEL AS CLAIMSLEVEL -- ծȨ����
                  , BONDFLAG AS BONDFLAG -- �Ƿ�Ϊծȯ
                  , BONDISSUEINTENT AS BONDISSUEINTENT -- ծȯ����Ŀ��
                  , NSUREALPROPERTYFLAG AS NSUREALPROPERTYFLAG -- �Ƿ�����ò�����
                  , REPASSETTERMTYPE AS REPASSETTERMTYPE -- ��ծ�ʲ���������
                  , DEPENDONFPOBFLAG AS DEPENDONFPOBFLAG -- �Ƿ�����������δ��ӯ��
                  , IRATING AS IRATING -- �ڲ�����
                  , PD AS PD -- ΥԼ����
                  , LGDLEVEL AS LGDLEVEL -- ΥԼ��ʧ�ʼ���
                  , LGDAIRB AS LGDAIRB -- �߼���ΥԼ��ʧ��
                  , MAIRB AS MAIRB -- �߼�����Ч����
                  , EADAIRB AS EADAIRB -- �߼���ΥԼ���ձ�¶
                  , DEFAULTFLAG AS DEFAULTFLAG -- ΥԼ��ʶ
                  , BEEL AS BEEL -- ��ΥԼ��¶Ԥ����ʧ����
                  , DEFAULTLGD AS DEFAULTLGD -- ��ΥԼ��¶ΥԼ��ʧ��
                  , EQUITYEXPOFLAG AS EQUITYEXPOFLAG -- ��Ȩ��¶��ʶ
                  , EQUITYINVESTTYPE AS EQUITYINVESTTYPE -- ��ȨͶ�ʶ�������
                  , EQUITYINVESTCAUSE AS EQUITYINVESTCAUSE -- ��ȨͶ���γ�ԭ��
                  , SLFLAG AS SLFLAG -- רҵ�����ʶ
                  , SLTYPE AS SLTYPE -- רҵ��������
                  , PFPHASE AS PFPHASE -- ��Ŀ���ʽ׶�
                  , REGURATING AS REGURATING -- �������
                  , CBRCMPRATINGFLAG AS CBRCMPRATINGFLAG -- ������϶������Ƿ��Ϊ����
                  , LARGEFLUCFLAG AS LARGEFLUCFLAG -- �Ƿ񲨶��Խϴ�
                  , LIQUEXPOFLAG AS LIQUEXPOFLAG -- �Ƿ���������з��ձ�¶
                  , PAYMENTDEALFLAG AS PAYMENTDEALFLAG -- �Ƿ����Ը�ģʽ
                  , DELAYTRADINGDAYS AS DELAYTRADINGDAYS -- �ӳٽ�������
                  , SECURITIESFLAG AS SECURITIESFLAG -- �м�֤ȯ��ʶ
                  , SECUISSUERID AS SECUISSUERID -- ֤ȯ������ID
                  , RATINGDURATIONTYPE AS RATINGDURATIONTYPE -- ������������
                  , SECUISSUERATING AS SECUISSUERATING -- ֤ȯ���еȼ�
                  , SECURESIDUALM AS SECURESIDUALM -- ֤ȯʣ������
                  , SECUREVAFREQUENCY AS SECUREVAFREQUENCY -- ֤ȯ�ع�Ƶ��
                  , CCPTRANFLAG AS CCPTRANFLAG -- �Ƿ����뽻�׶�����ؽ���
                  , CCPID AS CCPID -- ���뽻�׶���ID
                  , QUALCCPFLAG AS QUALCCPFLAG -- �Ƿ�ϸ����뽻�׶���
                  , BANKROLE AS BANKROLE -- ���н�ɫ
                  , CLEARINGMETHOD AS CLEARINGMETHOD -- ���㷽ʽ
                  , BANKASSETFLAG AS BANKASSETFLAG -- �Ƿ������ύ�ʲ�
                  , MATCHCONDITIONS AS MATCHCONDITIONS -- �����������
                  , SFTFLAG AS SFTFLAG -- ֤ȯ���ʽ��ױ�ʶ
                  , MASTERNETAGREEFLAG AS MASTERNETAGREEFLAG -- ���������Э���ʶ
                  , MASTERNETAGREEID AS MASTERNETAGREEID -- ���������Э��ID
                  , SFTTYPE AS SFTTYPE -- ֤ȯ���ʽ�������
                  , SECUOWNERTRANSFLAG AS SECUOWNERTRANSFLAG -- ֤ȯ����Ȩ�Ƿ�ת��
                  , OTCFLAG AS OTCFLAG -- �����������߱�ʶ
                  , VALIDNETTINGFLAG AS VALIDNETTINGFLAG -- ��Ч�������Э���ʶ
                  , VALIDNETAGREEMENTID AS VALIDNETAGREEMENTID -- ��Ч�������Э��ID
                  , OTCTYPE AS OTCTYPE -- ����������������
                  , DEPOSITRISKPERIOD AS DEPOSITRISKPERIOD -- ��֤������ڼ�
                  , MTM AS MTM -- ���óɱ�
                  , MTMCURRENCY AS MTMCURRENCY -- ���óɱ�����
                  , BUYERORSELLER AS BUYERORSELLER -- ������
                  , QUALROFLAG AS QUALROFLAG -- �ϸ�����ʲ���ʶ
                  , ROISSUERPERFORMFLAG AS ROISSUERPERFORMFLAG -- �����ʲ��������Ƿ�����Լ
                  , BUYERINSOLVENCYFLAG AS BUYERINSOLVENCYFLAG -- ���ñ������Ƿ��Ʋ�
                  , NONPAYMENTFEES AS NONPAYMENTFEES -- ��δ֧������
                  , RETAILEXPOFLAG AS RETAILEXPOFLAG -- ���۱�¶��ʶ
                  , RETAILCLAIMTYPE AS RETAILCLAIMTYPE -- ����ծȨ����
                  , MORTGAGETYPE AS MORTGAGETYPE -- ס����Ѻ��������
                  , DEBTORNUMBER AS DEBTORNUMBER -- ����˸���
                  , EXPONUMBER AS EXPONUMBER -- ���ձ�¶����
                  , PDPOOLMODELID AS PDPOOLMODELID -- PD�ֳ�ģ��ID
                  , LGDPOOLMODELID AS LGDPOOLMODELID -- LGD�ֳ�ģ��ID
                  , CCFPOOLMODELID AS CCFPOOLMODELID -- CCF�ֳ�ģ��ID
                  , PDPOOLID AS PDPOOLID -- ����PD��ID
                  , LGDPOOLID AS LGDPOOLID -- ����LGD��ID
                  , CCFPOOLID AS CCFPOOLID -- ����CCF��ID
                  , ABSUAFLAG AS ABSUAFLAG -- �ʲ�֤ȯ�������ʲ���ʶ
                  , ABSPOOLID AS ABSPOOLID -- ֤ȯ���ʲ���ID
                  , ABSPROPORTION AS ABSPROPORTION -- �ʲ�֤ȯ������
                  , GROUPID AS GROUPID -- ������
                  , ORGSORTNO AS ORGSORTNO --�������������
                  , LTV AS LTV --�����ֵ��
                  , AGING AS AGING --����
                  , NEWDEFAULTDEBTFLAG AS NEWDEFAULTDEBTFLAG --����ΥԼծ���ʶ
                  , DEFAULTDATE AS DEFAULTDATE --ΥԼʱ��
                   FROM RWA_DEV.RWA_LC_EXPOSURE WHERE DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD'       );
  COMMIT;

  /*����Ʊ�ݵķ��ձ�¶��Ϣ*/
  INSERT /*+ APPEND */
  INTO RWA_DEV.RWA_EI_EXPOSURE
    (DATADATE -- ��������
    ,
     DATANO -- ������ˮ��
    ,
     EXPOSUREID -- ���ձ�¶ID
    ,
     DUEID -- ծ��ID
    ,
     SSYSID -- ԴϵͳID
    ,
     CONTRACTID -- ��ͬID
    ,
     CLIENTID -- ��������ID
    ,
     SORGID -- Դ����ID
    ,
     SORGNAME -- Դ��������
    ,
     ORGID -- ��������ID
    ,
     ORGNAME -- ������������
    ,
     ACCORGID -- �������ID
    ,
     ACCORGNAME -- �����������
    ,
     INDUSTRYID -- ������ҵ����
    ,
     INDUSTRYNAME -- ������ҵ����
    ,
     BUSINESSLINE -- ����
    ,
     ASSETTYPE -- �ʲ�����
    ,
     ASSETSUBTYPE -- �ʲ�С��
    ,
     BUSINESSTYPEID -- ҵ��Ʒ�ִ���
    ,
     BUSINESSTYPENAME -- ҵ��Ʒ������
    ,
     CREDITRISKDATATYPE -- ���÷�����������
    ,
     ASSETTYPEOFHAIRCUTS -- �ۿ�ϵ����Ӧ�ʲ����
    ,
     BUSINESSTYPESTD -- Ȩ�ط�ҵ������
    ,
     EXPOCLASSSTD -- Ȩ�ط���¶����
    ,
     EXPOSUBCLASSSTD -- Ȩ�ط���¶С��
    ,
     EXPOCLASSIRB -- ��������¶����
    ,
     EXPOSUBCLASSIRB -- ��������¶С��
    ,
     EXPOBELONG -- ��¶������ʶ
    ,
     BOOKTYPE -- �˻����
    ,
     REGUTRANTYPE -- ��ܽ�������
    ,
     REPOTRANFLAG -- �ع����ױ�ʶ
    ,
     REVAFREQUENCY -- �ع�Ƶ��
    ,
     CURRENCY -- ����
    ,
     NORMALPRINCIPAL -- �����������
    ,
     OVERDUEBALANCE -- �������
    ,
     NONACCRUALBALANCE -- ��Ӧ�����
    ,
     ONSHEETBALANCE -- �������
    ,
     NORMALINTEREST -- ������Ϣ
    ,
     ONDEBITINTEREST -- ����ǷϢ
    ,
     OFFDEBITINTEREST -- ����ǷϢ
    ,
     EXPENSERECEIVABLE -- Ӧ�շ���
    ,
     ASSETBALANCE -- �ʲ����
    ,
     ACCSUBJECT1 -- ��Ŀһ
    ,
     ACCSUBJECT2 -- ��Ŀ��
    ,
     ACCSUBJECT3 -- ��Ŀ��
    ,
     STARTDATE -- ��ʼ����
    ,
     DUEDATE -- ��������
    ,
     ORIGINALMATURITY -- ԭʼ����
    ,
     RESIDUALM -- ʣ������
    ,
     RISKCLASSIFY -- ���շ���
    ,
     EXPOSURESTATUS -- ���ձ�¶״̬
    ,
     OVERDUEDAYS -- ��������
    ,
     SPECIALPROVISION -- ר��׼����
    ,
     GENERALPROVISION -- һ��׼����
    ,
     ESPECIALPROVISION -- �ر�׼����
    ,
     WRITTENOFFAMOUNT -- �Ѻ������
    ,
     OFFEXPOSOURCE -- ���Ⱪ¶��Դ
    ,
     OFFBUSINESSTYPE -- ����ҵ������
    ,
     OFFBUSINESSSDVSSTD -- Ȩ�ط�����ҵ������ϸ��
    ,
     UNCONDCANCELFLAG -- �Ƿ����ʱ����������
    ,
     CCFLEVEL -- ����ת��ϵ������
    ,
     CCFAIRB -- �߼�������ת��ϵ��
    ,
     CLAIMSLEVEL -- ծȨ����
    ,
     BONDFLAG -- �Ƿ�Ϊծȯ
    ,
     BONDISSUEINTENT -- ծȯ����Ŀ��
    ,
     NSUREALPROPERTYFLAG -- �Ƿ�����ò�����
    ,
     REPASSETTERMTYPE -- ��ծ�ʲ���������
    ,
     DEPENDONFPOBFLAG -- �Ƿ�����������δ��ӯ��
    ,
     IRATING -- �ڲ�����
    ,
     PD -- ΥԼ����
    ,
     LGDLEVEL -- ΥԼ��ʧ�ʼ���
    ,
     LGDAIRB -- �߼���ΥԼ��ʧ��
    ,
     MAIRB -- �߼�����Ч����
    ,
     EADAIRB -- �߼���ΥԼ���ձ�¶
    ,
     DEFAULTFLAG -- ΥԼ��ʶ
    ,
     BEEL -- ��ΥԼ��¶Ԥ����ʧ����
    ,
     DEFAULTLGD -- ��ΥԼ��¶ΥԼ��ʧ��
    ,
     EQUITYEXPOFLAG -- ��Ȩ��¶��ʶ
    ,
     EQUITYINVESTTYPE -- ��ȨͶ�ʶ�������
    ,
     EQUITYINVESTCAUSE -- ��ȨͶ���γ�ԭ��
    ,
     SLFLAG -- רҵ�����ʶ
    ,
     SLTYPE -- רҵ��������
    ,
     PFPHASE -- ��Ŀ���ʽ׶�
    ,
     REGURATING -- �������
    ,
     CBRCMPRATINGFLAG -- ������϶������Ƿ��Ϊ����
    ,
     LARGEFLUCFLAG -- �Ƿ񲨶��Խϴ�
    ,
     LIQUEXPOFLAG -- �Ƿ���������з��ձ�¶
    ,
     PAYMENTDEALFLAG -- �Ƿ����Ը�ģʽ
    ,
     DELAYTRADINGDAYS -- �ӳٽ�������
    ,
     SECURITIESFLAG -- �м�֤ȯ��ʶ
    ,
     SECUISSUERID -- ֤ȯ������ID
    ,
     RATINGDURATIONTYPE -- ������������
    ,
     SECUISSUERATING -- ֤ȯ���еȼ�
    ,
     SECURESIDUALM -- ֤ȯʣ������
    ,
     SECUREVAFREQUENCY -- ֤ȯ�ع�Ƶ��
    ,
     CCPTRANFLAG -- �Ƿ����뽻�׶�����ؽ���
    ,
     CCPID -- ���뽻�׶���ID
    ,
     QUALCCPFLAG -- �Ƿ�ϸ����뽻�׶���
    ,
     BANKROLE -- ���н�ɫ
    ,
     CLEARINGMETHOD -- ���㷽ʽ
    ,
     BANKASSETFLAG -- �Ƿ������ύ�ʲ�
    ,
     MATCHCONDITIONS -- �����������
    ,
     SFTFLAG -- ֤ȯ���ʽ��ױ�ʶ
    ,
     MASTERNETAGREEFLAG -- ���������Э���ʶ
    ,
     MASTERNETAGREEID -- ���������Э��ID
    ,
     SFTTYPE -- ֤ȯ���ʽ�������
    ,
     SECUOWNERTRANSFLAG -- ֤ȯ����Ȩ�Ƿ�ת��
    ,
     OTCFLAG -- �����������߱�ʶ
    ,
     VALIDNETTINGFLAG -- ��Ч�������Э���ʶ
    ,
     VALIDNETAGREEMENTID -- ��Ч�������Э��ID
    ,
     OTCTYPE -- ����������������
    ,
     DEPOSITRISKPERIOD -- ��֤������ڼ�
    ,
     MTM -- ���óɱ�
    ,
     MTMCURRENCY -- ���óɱ�����
    ,
     BUYERORSELLER -- ������
    ,
     QUALROFLAG -- �ϸ�����ʲ���ʶ
    ,
     ROISSUERPERFORMFLAG -- �����ʲ��������Ƿ�����Լ
    ,
     BUYERINSOLVENCYFLAG -- ���ñ������Ƿ��Ʋ�
    ,
     NONPAYMENTFEES -- ��δ֧������
    ,
     RETAILEXPOFLAG -- ���۱�¶��ʶ
    ,
     RETAILCLAIMTYPE -- ����ծȨ����
    ,
     MORTGAGETYPE -- ס����Ѻ��������
    ,
     DEBTORNUMBER -- ����˸���
    ,
     EXPONUMBER -- ���ձ�¶����
    ,
     PDPOOLMODELID -- PD�ֳ�ģ��ID
    ,
     LGDPOOLMODELID -- LGD�ֳ�ģ��ID
    ,
     CCFPOOLMODELID -- CCF�ֳ�ģ��ID
    ,
     PDPOOLID -- ����PD��ID
    ,
     LGDPOOLID -- ����LGD��ID
    ,
     CCFPOOLID -- ����CCF��ID
    ,
     ABSUAFLAG -- �ʲ�֤ȯ�������ʲ���ʶ
    ,
     ABSPOOLID -- ֤ȯ���ʲ���ID
    ,
     ABSPROPORTION -- �ʲ�֤ȯ������
    ,
     GROUPID -- ������
    ,
     ORGSORTNO --�������������
    ,
     LTV --�����ֵ��
    ,
     AGING --����
    ,
     NEWDEFAULTDEBTFLAG --����ΥԼծ���ʶ
    ,
     DEFAULTDATE --ΥԼʱ��
     )
    SELECT DATADATE   AS DATADATE -- ��������
          ,
           DATANO     AS DATANO -- ������ˮ��
          ,
           EXPOSUREID AS EXPOSUREID -- ���ձ�¶ID
          ,
           DUEID      AS DUEID -- ծ��ID
          ,
           SSYSID     AS SSYSID -- ԴϵͳID
          ,
           CONTRACTID AS CONTRACTID -- ��ͬID
          ,
           CLIENTID   AS CLIENTID -- ��������ID
          ,
           SORGID     AS SORGID -- Դ����ID
          ,
           SORGNAME   AS SORGNAME -- Դ��������
          ,
           ORGID      AS ORGID -- ��������ID
          ,
           ORGNAME    AS ORGNAME -- ������������
          ,
           ACCORGID   AS ACCORGID -- �������ID
          ,
           ACCORGNAME AS ACCORGNAME -- �����������
          ,
           INDUSTRYID AS INDUSTRYID --������ҵ����
          ,
           NVL       (INDUSTRYNAME, 'δ֪'        �� AS INDUSTRYNAME --������ҵ����
                  , BUSINESSLINE AS BUSINESSLINE -- ����
                  , ASSETTYPE AS ASSETTYPE -- �ʲ�����
                  , ASSETSUBTYPE AS ASSETSUBTYPE -- �ʲ�С��
                  , BUSINESSTYPEID AS BUSINESSTYPEID -- ҵ��Ʒ�ִ���
                  , BUSINESSTYPENAME AS BUSINESSTYPENAME -- ҵ��Ʒ������
                  , CREDITRISKDATATYPE AS CREDITRISKDATATYPE -- ���÷�����������
                  , ASSETTYPEOFHAIRCUTS AS ASSETTYPEOFHAIRCUTS -- �ۿ�ϵ����Ӧ�ʲ����
                  , BUSINESSTYPESTD AS BUSINESSTYPESTD -- Ȩ�ط�ҵ������
                  , EXPOCLASSSTD AS EXPOCLASSSTD -- Ȩ�ط���¶����
                  , EXPOSUBCLASSSTD AS EXPOSUBCLASSSTD -- Ȩ�ط���¶С��
                  , EXPOCLASSIRB AS EXPOCLASSIRB -- ��������¶����
                  , EXPOSUBCLASSIRB AS EXPOSUBCLASSIRB -- ��������¶С��
                  , EXPOBELONG AS EXPOBELONG -- ��¶������ʶ
                  , BOOKTYPE AS BOOKTYPE -- �˻����
                  , REGUTRANTYPE AS REGUTRANTYPE -- ��ܽ�������
                  , REPOTRANFLAG AS REPOTRANFLAG -- �ع����ױ�ʶ
                  , REVAFREQUENCY AS REVAFREQUENCY -- �ع�Ƶ��
                  , CURRENCY AS CURRENCY -- ����
                  , NORMALPRINCIPAL AS NORMALPRINCIPAL -- �����������
                  , OVERDUEBALANCE AS OVERDUEBALANCE -- �������
                  , NONACCRUALBALANCE AS NONACCRUALBALANCE -- ��Ӧ�����
                  , ONSHEETBALANCE AS ONSHEETBALANCE -- �������
                  , NORMALINTEREST AS NORMALINTEREST -- ������Ϣ
                  , ONDEBITINTEREST AS ONDEBITINTEREST -- ����ǷϢ
                  , OFFDEBITINTEREST AS OFFDEBITINTEREST -- ����ǷϢ
                  , EXPENSERECEIVABLE AS EXPENSERECEIVABLE -- Ӧ�շ���
                  , ASSETBALANCE AS ASSETBALANCE -- �ʲ����
                  , ACCSUBJECT1 AS ACCSUBJECT1 -- ��Ŀһ
                  , ACCSUBJECT2 AS ACCSUBJECT2 -- ��Ŀ��
                  , ACCSUBJECT3 AS ACCSUBJECT3 -- ��Ŀ��
                  , TO_CHAR(TO_DATE(STARTDATE, 'YYYY-MM-DD'       ), 'YYYY-MM-DD'       ) AS STARTDATE -- ��ʼ����
                  , TO_CHAR(TO_DATE(DUEDATE, 'YYYY-MM-DD'       ), 'YYYY-MM-DD'       ) AS DUEDATE -- ��������
                  , ORIGINALMATURITY AS ORIGINALMATURITY -- ԭʼ����
                  , RESIDUALM AS RESIDUALM -- ʣ������
                  , RISKCLASSIFY AS RISKCLASSIFY -- ���շ���
                  , EXPOSURESTATUS AS EXPOSURESTATUS -- ���ձ�¶״̬
                  , OVERDUEDAYS AS OVERDUEDAYS -- ��������
                  , SPECIALPROVISION AS SPECIALPROVISION -- ר��׼����
                  , GENERALPROVISION AS GENERALPROVISION -- һ��׼����
                  , ESPECIALPROVISION AS ESPECIALPROVISION -- �ر�׼����
                  , WRITTENOFFAMOUNT AS WRITTENOFFAMOUNT -- �Ѻ������
                  , OFFEXPOSOURCE AS OFFEXPOSOURCE -- ���Ⱪ¶��Դ
                  , OFFBUSINESSTYPE AS OFFBUSINESSTYPE -- ����ҵ������
                  , OFFBUSINESSSDVSSTD AS OFFBUSINESSSDVSSTD -- Ȩ�ط�����ҵ������ϸ��
                  , UNCONDCANCELFLAG AS UNCONDCANCELFLAG -- �Ƿ����ʱ����������
                  , CCFLEVEL AS CCFLEVEL -- ����ת��ϵ������
                  , CCFAIRB AS CCFAIRB -- �߼�������ת��ϵ��
                  , CLAIMSLEVEL AS CLAIMSLEVEL -- ծȨ����
                  , BONDFLAG AS BONDFLAG -- �Ƿ�Ϊծȯ
                  , BONDISSUEINTENT AS BONDISSUEINTENT -- ծȯ����Ŀ��
                  , NSUREALPROPERTYFLAG AS NSUREALPROPERTYFLAG -- �Ƿ�����ò�����
                  , REPASSETTERMTYPE AS REPASSETTERMTYPE -- ��ծ�ʲ���������
                  , DEPENDONFPOBFLAG AS DEPENDONFPOBFLAG -- �Ƿ�����������δ��ӯ��
                  , IRATING AS IRATING -- �ڲ�����
                  , PD AS PD -- ΥԼ����
                  , LGDLEVEL AS LGDLEVEL -- ΥԼ��ʧ�ʼ���
                  , LGDAIRB AS LGDAIRB -- �߼���ΥԼ��ʧ��
                  , MAIRB AS MAIRB -- �߼�����Ч����
                  , EADAIRB AS EADAIRB -- �߼���ΥԼ���ձ�¶
                  , DEFAULTFLAG AS DEFAULTFLAG -- ΥԼ��ʶ
                  , BEEL AS BEEL -- ��ΥԼ��¶Ԥ����ʧ����
                  , DEFAULTLGD AS DEFAULTLGD -- ��ΥԼ��¶ΥԼ��ʧ��
                  , EQUITYEXPOFLAG AS EQUITYEXPOFLAG -- ��Ȩ��¶��ʶ
                  , EQUITYINVESTTYPE AS EQUITYINVESTTYPE -- ��ȨͶ�ʶ�������
                  , EQUITYINVESTCAUSE AS EQUITYINVESTCAUSE -- ��ȨͶ���γ�ԭ��
                  , SLFLAG AS SLFLAG -- רҵ�����ʶ
                  , SLTYPE AS SLTYPE -- רҵ��������
                  , PFPHASE AS PFPHASE -- ��Ŀ���ʽ׶�
                  , REGURATING AS REGURATING -- �������
                  , CBRCMPRATINGFLAG AS CBRCMPRATINGFLAG -- ������϶������Ƿ��Ϊ����
                  , LARGEFLUCFLAG AS LARGEFLUCFLAG -- �Ƿ񲨶��Խϴ�
                  , LIQUEXPOFLAG AS LIQUEXPOFLAG -- �Ƿ���������з��ձ�¶
                  , PAYMENTDEALFLAG AS PAYMENTDEALFLAG -- �Ƿ����Ը�ģʽ
                  , DELAYTRADINGDAYS AS DELAYTRADINGDAYS -- �ӳٽ�������
                  , SECURITIESFLAG AS SECURITIESFLAG -- �м�֤ȯ��ʶ
                  , SECUISSUERID AS SECUISSUERID -- ֤ȯ������ID
                  , RATINGDURATIONTYPE AS RATINGDURATIONTYPE -- ������������
                  , SECUISSUERATING AS SECUISSUERATING -- ֤ȯ���еȼ�
                  , SECURESIDUALM AS SECURESIDUALM -- ֤ȯʣ������
                  , SECUREVAFREQUENCY AS SECUREVAFREQUENCY -- ֤ȯ�ع�Ƶ��
                  , CCPTRANFLAG AS CCPTRANFLAG -- �Ƿ����뽻�׶�����ؽ���
                  , CCPID AS CCPID -- ���뽻�׶���ID
                  , QUALCCPFLAG AS QUALCCPFLAG -- �Ƿ�ϸ����뽻�׶���
                  , BANKROLE AS BANKROLE -- ���н�ɫ
                  , CLEARINGMETHOD AS CLEARINGMETHOD -- ���㷽ʽ
                  , BANKASSETFLAG AS BANKASSETFLAG -- �Ƿ������ύ�ʲ�
                  , MATCHCONDITIONS AS MATCHCONDITIONS -- �����������
                  , SFTFLAG AS SFTFLAG -- ֤ȯ���ʽ��ױ�ʶ
                  , MASTERNETAGREEFLAG AS MASTERNETAGREEFLAG -- ���������Э���ʶ
                  , MASTERNETAGREEID AS MASTERNETAGREEID -- ���������Э��ID
                  , SFTTYPE AS SFTTYPE -- ֤ȯ���ʽ�������
                  , SECUOWNERTRANSFLAG AS SECUOWNERTRANSFLAG -- ֤ȯ����Ȩ�Ƿ�ת��
                  , OTCFLAG AS OTCFLAG -- �����������߱�ʶ
                  , VALIDNETTINGFLAG AS VALIDNETTINGFLAG -- ��Ч�������Э���ʶ
                  , VALIDNETAGREEMENTID AS VALIDNETAGREEMENTID -- ��Ч�������Э��ID
                  , OTCTYPE AS OTCTYPE -- ����������������
                  , DEPOSITRISKPERIOD AS DEPOSITRISKPERIOD -- ��֤������ڼ�
                  , MTM AS MTM -- ���óɱ�
                  , MTMCURRENCY AS MTMCURRENCY -- ���óɱ�����
                  , BUYERORSELLER AS BUYERORSELLER -- ������
                  , QUALROFLAG AS QUALROFLAG -- �ϸ�����ʲ���ʶ
                  , ROISSUERPERFORMFLAG AS ROISSUERPERFORMFLAG -- �����ʲ��������Ƿ�����Լ
                  , BUYERINSOLVENCYFLAG AS BUYERINSOLVENCYFLAG -- ���ñ������Ƿ��Ʋ�
                  , NONPAYMENTFEES AS NONPAYMENTFEES -- ��δ֧������
                  , RETAILEXPOFLAG AS RETAILEXPOFLAG -- ���۱�¶��ʶ
                  , RETAILCLAIMTYPE AS RETAILCLAIMTYPE -- ����ծȨ����
                  , MORTGAGETYPE AS MORTGAGETYPE -- ס����Ѻ��������
                  , DEBTORNUMBER AS DEBTORNUMBER -- ����˸���
                  , EXPONUMBER AS EXPONUMBER -- ���ձ�¶����
                  , PDPOOLMODELID AS PDPOOLMODELID -- PD�ֳ�ģ��ID
                  , LGDPOOLMODELID AS LGDPOOLMODELID -- LGD�ֳ�ģ��ID
                  , CCFPOOLMODELID AS CCFPOOLMODELID -- CCF�ֳ�ģ��ID
                  , PDPOOLID AS PDPOOLID -- ����PD��ID
                  , LGDPOOLID AS LGDPOOLID -- ����LGD��ID
                  , CCFPOOLID AS CCFPOOLID -- ����CCF��ID
                  , ABSUAFLAG AS ABSUAFLAG -- �ʲ�֤ȯ�������ʲ���ʶ
                  , ABSPOOLID AS ABSPOOLID -- ֤ȯ���ʲ���ID
                  , ABSPROPORTION AS ABSPROPORTION -- �ʲ�֤ȯ������
                  , GROUPID AS GROUPID -- ������
                  , ORGSORTNO AS ORGSORTNO --�������������
                  , LTV AS LTV --�����ֵ��
                  , AGING AS AGING --����
                  , NEWDEFAULTDEBTFLAG AS NEWDEFAULTDEBTFLAG --����ΥԼծ���ʶ
                  , DEFAULTDATE AS DEFAULTDATE --ΥԼʱ��
                   FROM RWA_DEV.RWA_PJ_EXPOSURE WHERE DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD'       );
  COMMIT;

  /*����ͬҵ�ķ��ձ�¶��Ϣ*/
  INSERT /*+ APPEND */
  INTO RWA_DEV.RWA_EI_EXPOSURE
    (DATADATE -- ��������
    ,
     DATANO -- ������ˮ��
    ,
     EXPOSUREID -- ���ձ�¶ID
    ,
     DUEID -- ծ��ID
    ,
     SSYSID -- ԴϵͳID
    ,
     CONTRACTID -- ��ͬID
    ,
     CLIENTID -- ��������ID
    ,
     SORGID -- Դ����ID
    ,
     SORGNAME -- Դ��������
    ,
     ORGID -- ��������ID
    ,
     ORGNAME -- ������������
    ,
     ACCORGID -- �������ID
    ,
     ACCORGNAME -- �����������
    ,
     INDUSTRYID -- ������ҵ����
    ,
     INDUSTRYNAME -- ������ҵ����
    ,
     BUSINESSLINE -- ����
    ,
     ASSETTYPE -- �ʲ�����
    ,
     ASSETSUBTYPE -- �ʲ�С��
    ,
     BUSINESSTYPEID -- ҵ��Ʒ�ִ���
    ,
     BUSINESSTYPENAME -- ҵ��Ʒ������
    ,
     CREDITRISKDATATYPE -- ���÷�����������
    ,
     ASSETTYPEOFHAIRCUTS -- �ۿ�ϵ����Ӧ�ʲ����
    ,
     BUSINESSTYPESTD -- Ȩ�ط�ҵ������
    ,
     EXPOCLASSSTD -- Ȩ�ط���¶����
    ,
     EXPOSUBCLASSSTD -- Ȩ�ط���¶С��
    ,
     EXPOCLASSIRB -- ��������¶����
    ,
     EXPOSUBCLASSIRB -- ��������¶С��
    ,
     EXPOBELONG -- ��¶������ʶ
    ,
     BOOKTYPE -- �˻����
    ,
     REGUTRANTYPE -- ��ܽ�������
    ,
     REPOTRANFLAG -- �ع����ױ�ʶ
    ,
     REVAFREQUENCY -- �ع�Ƶ��
    ,
     CURRENCY -- ����
    ,
     NORMALPRINCIPAL -- �����������
    ,
     OVERDUEBALANCE -- �������
    ,
     NONACCRUALBALANCE -- ��Ӧ�����
    ,
     ONSHEETBALANCE -- �������
    ,
     NORMALINTEREST -- ������Ϣ
    ,
     ONDEBITINTEREST -- ����ǷϢ
    ,
     OFFDEBITINTEREST -- ����ǷϢ
    ,
     EXPENSERECEIVABLE -- Ӧ�շ���
    ,
     ASSETBALANCE -- �ʲ����
    ,
     ACCSUBJECT1 -- ��Ŀһ
    ,
     ACCSUBJECT2 -- ��Ŀ��
    ,
     ACCSUBJECT3 -- ��Ŀ��
    ,
     STARTDATE -- ��ʼ����
    ,
     DUEDATE -- ��������
    ,
     ORIGINALMATURITY -- ԭʼ����
    ,
     RESIDUALM -- ʣ������
    ,
     RISKCLASSIFY -- ���շ���
    ,
     EXPOSURESTATUS -- ���ձ�¶״̬
    ,
     OVERDUEDAYS -- ��������
    ,
     SPECIALPROVISION -- ר��׼����
    ,
     GENERALPROVISION -- һ��׼����
    ,
     ESPECIALPROVISION -- �ر�׼����
    ,
     WRITTENOFFAMOUNT -- �Ѻ������
    ,
     OFFEXPOSOURCE -- ���Ⱪ¶��Դ
    ,
     OFFBUSINESSTYPE -- ����ҵ������
    ,
     OFFBUSINESSSDVSSTD -- Ȩ�ط�����ҵ������ϸ��
    ,
     UNCONDCANCELFLAG -- �Ƿ����ʱ����������
    ,
     CCFLEVEL -- ����ת��ϵ������
    ,
     CCFAIRB -- �߼�������ת��ϵ��
    ,
     CLAIMSLEVEL -- ծȨ����
    ,
     BONDFLAG -- �Ƿ�Ϊծȯ
    ,
     BONDISSUEINTENT -- ծȯ����Ŀ��
    ,
     NSUREALPROPERTYFLAG -- �Ƿ�����ò�����
    ,
     REPASSETTERMTYPE -- ��ծ�ʲ���������
    ,
     DEPENDONFPOBFLAG -- �Ƿ�����������δ��ӯ��
    ,
     IRATING -- �ڲ�����
    ,
     PD -- ΥԼ����
    ,
     LGDLEVEL -- ΥԼ��ʧ�ʼ���
    ,
     LGDAIRB -- �߼���ΥԼ��ʧ��
    ,
     MAIRB -- �߼�����Ч����
    ,
     EADAIRB -- �߼���ΥԼ���ձ�¶
    ,
     DEFAULTFLAG -- ΥԼ��ʶ
    ,
     BEEL -- ��ΥԼ��¶Ԥ����ʧ����
    ,
     DEFAULTLGD -- ��ΥԼ��¶ΥԼ��ʧ��
    ,
     EQUITYEXPOFLAG -- ��Ȩ��¶��ʶ
    ,
     EQUITYINVESTTYPE -- ��ȨͶ�ʶ�������
    ,
     EQUITYINVESTCAUSE -- ��ȨͶ���γ�ԭ��
    ,
     SLFLAG -- רҵ�����ʶ
    ,
     SLTYPE -- רҵ��������
    ,
     PFPHASE -- ��Ŀ���ʽ׶�
    ,
     REGURATING -- �������
    ,
     CBRCMPRATINGFLAG -- ������϶������Ƿ��Ϊ����
    ,
     LARGEFLUCFLAG -- �Ƿ񲨶��Խϴ�
    ,
     LIQUEXPOFLAG -- �Ƿ���������з��ձ�¶
    ,
     PAYMENTDEALFLAG -- �Ƿ����Ը�ģʽ
    ,
     DELAYTRADINGDAYS -- �ӳٽ�������
    ,
     SECURITIESFLAG -- �м�֤ȯ��ʶ
    ,
     SECUISSUERID -- ֤ȯ������ID
    ,
     RATINGDURATIONTYPE -- ������������
    ,
     SECUISSUERATING -- ֤ȯ���еȼ�
    ,
     SECURESIDUALM -- ֤ȯʣ������
    ,
     SECUREVAFREQUENCY -- ֤ȯ�ع�Ƶ��
    ,
     CCPTRANFLAG -- �Ƿ����뽻�׶�����ؽ���
    ,
     CCPID -- ���뽻�׶���ID
    ,
     QUALCCPFLAG -- �Ƿ�ϸ����뽻�׶���
    ,
     BANKROLE -- ���н�ɫ
    ,
     CLEARINGMETHOD -- ���㷽ʽ
    ,
     BANKASSETFLAG -- �Ƿ������ύ�ʲ�
    ,
     MATCHCONDITIONS -- �����������
    ,
     SFTFLAG -- ֤ȯ���ʽ��ױ�ʶ
    ,
     MASTERNETAGREEFLAG -- ���������Э���ʶ
    ,
     MASTERNETAGREEID -- ���������Э��ID
    ,
     SFTTYPE -- ֤ȯ���ʽ�������
    ,
     SECUOWNERTRANSFLAG -- ֤ȯ����Ȩ�Ƿ�ת��
    ,
     OTCFLAG -- �����������߱�ʶ
    ,
     VALIDNETTINGFLAG -- ��Ч�������Э���ʶ
    ,
     VALIDNETAGREEMENTID -- ��Ч�������Э��ID
    ,
     OTCTYPE -- ����������������
    ,
     DEPOSITRISKPERIOD -- ��֤������ڼ�
    ,
     MTM -- ���óɱ�
    ,
     MTMCURRENCY -- ���óɱ�����
    ,
     BUYERORSELLER -- ������
    ,
     QUALROFLAG -- �ϸ�����ʲ���ʶ
    ,
     ROISSUERPERFORMFLAG -- �����ʲ��������Ƿ�����Լ
    ,
     BUYERINSOLVENCYFLAG -- ���ñ������Ƿ��Ʋ�
    ,
     NONPAYMENTFEES -- ��δ֧������
    ,
     RETAILEXPOFLAG -- ���۱�¶��ʶ
    ,
     RETAILCLAIMTYPE -- ����ծȨ����
    ,
     MORTGAGETYPE -- ס����Ѻ��������
    ,
     DEBTORNUMBER -- ����˸���
    ,
     EXPONUMBER -- ���ձ�¶����
    ,
     PDPOOLMODELID -- PD�ֳ�ģ��ID
    ,
     LGDPOOLMODELID -- LGD�ֳ�ģ��ID
    ,
     CCFPOOLMODELID -- CCF�ֳ�ģ��ID
    ,
     PDPOOLID -- ����PD��ID
    ,
     LGDPOOLID -- ����LGD��ID
    ,
     CCFPOOLID -- ����CCF��ID
    ,
     ABSUAFLAG -- �ʲ�֤ȯ�������ʲ���ʶ
    ,
     ABSPOOLID -- ֤ȯ���ʲ���ID
    ,
     ABSPROPORTION -- �ʲ�֤ȯ������
    ,
     GROUPID -- ������
    ,
     ORGSORTNO --�������������
    ,
     LTV --�����ֵ��
    ,
     AGING --����
    ,
     NEWDEFAULTDEBTFLAG --����ΥԼծ���ʶ
    ,
     DEFAULTDATE --ΥԼʱ��
     )
    SELECT DATADATE   AS DATADATE -- ��������
          ,
           DATANO     AS DATANO -- ������ˮ��
          ,
           EXPOSUREID AS EXPOSUREID -- ���ձ�¶ID
          ,
           DUEID      AS DUEID -- ծ��ID
          ,
           SSYSID     AS SSYSID -- ԴϵͳID
          ,
           CONTRACTID AS CONTRACTID -- ��ͬID
          ,
           CLIENTID   AS CLIENTID -- ��������ID
          ,
           SORGID     AS SORGID -- Դ����ID
          ,
           SORGNAME   AS SORGNAME -- Դ��������
          ,
           ORGID      AS ORGID -- ��������ID
          ,
           ORGNAME    AS ORGNAME -- ������������
          ,
           ACCORGID   AS ACCORGID -- �������ID
          ,
           ACCORGNAME AS ACCORGNAME -- �����������
          ,
           INDUSTRYID AS INDUSTRYID --������ҵ����
          ,
           NVL       (INDUSTRYNAME, 'δ֪'        �� AS INDUSTRYNAME --������ҵ����
                  , BUSINESSLINE AS BUSINESSLINE -- ����
                  , ASSETTYPE AS ASSETTYPE -- �ʲ�����
                  , ASSETSUBTYPE AS ASSETSUBTYPE -- �ʲ�С��
                  , BUSINESSTYPEID AS BUSINESSTYPEID -- ҵ��Ʒ�ִ���
                  , BUSINESSTYPENAME AS BUSINESSTYPENAME -- ҵ��Ʒ������
                  , CREDITRISKDATATYPE AS CREDITRISKDATATYPE -- ���÷�����������
                  , ASSETTYPEOFHAIRCUTS AS ASSETTYPEOFHAIRCUTS -- �ۿ�ϵ����Ӧ�ʲ����
                  , BUSINESSTYPESTD AS BUSINESSTYPESTD -- Ȩ�ط�ҵ������
                  , EXPOCLASSSTD AS EXPOCLASSSTD -- Ȩ�ط���¶����
                  , EXPOSUBCLASSSTD AS EXPOSUBCLASSSTD -- Ȩ�ط���¶С��
                  , EXPOCLASSIRB AS EXPOCLASSIRB -- ��������¶����
                  , EXPOSUBCLASSIRB AS EXPOSUBCLASSIRB -- ��������¶С��
                  , EXPOBELONG AS EXPOBELONG -- ��¶������ʶ
                  , BOOKTYPE AS BOOKTYPE -- �˻����
                  , REGUTRANTYPE AS REGUTRANTYPE -- ��ܽ�������
                  , REPOTRANFLAG AS REPOTRANFLAG -- �ع����ױ�ʶ
                  , REVAFREQUENCY AS REVAFREQUENCY -- �ع�Ƶ��
                  , CURRENCY AS CURRENCY -- ����
                  , NORMALPRINCIPAL AS NORMALPRINCIPAL -- �����������
                  , OVERDUEBALANCE AS OVERDUEBALANCE -- �������
                  , NONACCRUALBALANCE AS NONACCRUALBALANCE -- ��Ӧ�����
                  , ONSHEETBALANCE AS ONSHEETBALANCE -- �������
                  , NORMALINTEREST AS NORMALINTEREST -- ������Ϣ
                  , ONDEBITINTEREST AS ONDEBITINTEREST -- ����ǷϢ
                  , OFFDEBITINTEREST AS OFFDEBITINTEREST -- ����ǷϢ
                  , EXPENSERECEIVABLE AS EXPENSERECEIVABLE -- Ӧ�շ���
                  , ASSETBALANCE AS ASSETBALANCE -- �ʲ����
                  , ACCSUBJECT1 AS ACCSUBJECT1 -- ��Ŀһ
                  , ACCSUBJECT2 AS ACCSUBJECT2 -- ��Ŀ��
                  , ACCSUBJECT3 AS ACCSUBJECT3 -- ��Ŀ��
                  , TO_CHAR(TO_DATE(STARTDATE, 'YYYY-MM-DD'       ), 'YYYY-MM-DD'       ) AS STARTDATE -- ��ʼ����
                  , TO_CHAR(TO_DATE(DUEDATE, 'YYYY-MM-DD'       ), 'YYYY-MM-DD'       ) AS DUEDATE -- ��������
                  , ORIGINALMATURITY AS ORIGINALMATURITY -- ԭʼ����
                  , RESIDUALM AS RESIDUALM -- ʣ������
                  , RISKCLASSIFY AS RISKCLASSIFY -- ���շ���
                  , EXPOSURESTATUS AS EXPOSURESTATUS -- ���ձ�¶״̬
                  , OVERDUEDAYS AS OVERDUEDAYS -- ��������
                  , SPECIALPROVISION AS SPECIALPROVISION -- ר��׼����
                  , GENERALPROVISION AS GENERALPROVISION -- һ��׼����
                  , ESPECIALPROVISION AS ESPECIALPROVISION -- �ر�׼����
                  , WRITTENOFFAMOUNT AS WRITTENOFFAMOUNT -- �Ѻ������
                  , OFFEXPOSOURCE AS OFFEXPOSOURCE -- ���Ⱪ¶��Դ
                  , OFFBUSINESSTYPE AS OFFBUSINESSTYPE -- ����ҵ������
                  , OFFBUSINESSSDVSSTD AS OFFBUSINESSSDVSSTD -- Ȩ�ط�����ҵ������ϸ��
                  , UNCONDCANCELFLAG AS UNCONDCANCELFLAG -- �Ƿ����ʱ����������
                  , CCFLEVEL AS CCFLEVEL -- ����ת��ϵ������
                  , CCFAIRB AS CCFAIRB -- �߼�������ת��ϵ��
                  , CLAIMSLEVEL AS CLAIMSLEVEL -- ծȨ����
                  , BONDFLAG AS BONDFLAG -- �Ƿ�Ϊծȯ
                  , BONDISSUEINTENT AS BONDISSUEINTENT -- ծȯ����Ŀ��
                  , NSUREALPROPERTYFLAG AS NSUREALPROPERTYFLAG -- �Ƿ�����ò�����
                  , REPASSETTERMTYPE AS REPASSETTERMTYPE -- ��ծ�ʲ���������
                  , DEPENDONFPOBFLAG AS DEPENDONFPOBFLAG -- �Ƿ�����������δ��ӯ��
                  , IRATING AS IRATING -- �ڲ�����
                  , PD AS PD -- ΥԼ����
                  , LGDLEVEL AS LGDLEVEL -- ΥԼ��ʧ�ʼ���
                  , LGDAIRB AS LGDAIRB -- �߼���ΥԼ��ʧ��
                  , MAIRB AS MAIRB -- �߼�����Ч����
                  , EADAIRB AS EADAIRB -- �߼���ΥԼ���ձ�¶
                  , DEFAULTFLAG AS DEFAULTFLAG -- ΥԼ��ʶ
                  , BEEL AS BEEL -- ��ΥԼ��¶Ԥ����ʧ����
                  , DEFAULTLGD AS DEFAULTLGD -- ��ΥԼ��¶ΥԼ��ʧ��
                  , EQUITYEXPOFLAG AS EQUITYEXPOFLAG -- ��Ȩ��¶��ʶ
                  , EQUITYINVESTTYPE AS EQUITYINVESTTYPE -- ��ȨͶ�ʶ�������
                  , EQUITYINVESTCAUSE AS EQUITYINVESTCAUSE -- ��ȨͶ���γ�ԭ��
                  , SLFLAG AS SLFLAG -- רҵ�����ʶ
                  , SLTYPE AS SLTYPE -- רҵ��������
                  , PFPHASE AS PFPHASE -- ��Ŀ���ʽ׶�
                  , REGURATING AS REGURATING -- �������
                  , CBRCMPRATINGFLAG AS CBRCMPRATINGFLAG -- ������϶������Ƿ��Ϊ����
                  , LARGEFLUCFLAG AS LARGEFLUCFLAG -- �Ƿ񲨶��Խϴ�
                  , LIQUEXPOFLAG AS LIQUEXPOFLAG -- �Ƿ���������з��ձ�¶
                  , PAYMENTDEALFLAG AS PAYMENTDEALFLAG -- �Ƿ����Ը�ģʽ
                  , DELAYTRADINGDAYS AS DELAYTRADINGDAYS -- �ӳٽ�������
                  , SECURITIESFLAG AS SECURITIESFLAG -- �м�֤ȯ��ʶ
                  , SECUISSUERID AS SECUISSUERID -- ֤ȯ������ID
                  , RATINGDURATIONTYPE AS RATINGDURATIONTYPE -- ������������
                  , SECUISSUERATING AS SECUISSUERATING -- ֤ȯ���еȼ�
                  , SECURESIDUALM AS SECURESIDUALM -- ֤ȯʣ������
                  , SECUREVAFREQUENCY AS SECUREVAFREQUENCY -- ֤ȯ�ع�Ƶ��
                  , CCPTRANFLAG AS CCPTRANFLAG -- �Ƿ����뽻�׶�����ؽ���
                  , CCPID AS CCPID -- ���뽻�׶���ID
                  , QUALCCPFLAG AS QUALCCPFLAG -- �Ƿ�ϸ����뽻�׶���
                  , BANKROLE AS BANKROLE -- ���н�ɫ
                  , CLEARINGMETHOD AS CLEARINGMETHOD -- ���㷽ʽ
                  , BANKASSETFLAG AS BANKASSETFLAG -- �Ƿ������ύ�ʲ�
                  , MATCHCONDITIONS AS MATCHCONDITIONS -- �����������
                  , SFTFLAG AS SFTFLAG -- ֤ȯ���ʽ��ױ�ʶ
                  , MASTERNETAGREEFLAG AS MASTERNETAGREEFLAG -- ���������Э���ʶ
                  , MASTERNETAGREEID AS MASTERNETAGREEID -- ���������Э��ID
                  , SFTTYPE AS SFTTYPE -- ֤ȯ���ʽ�������
                  , SECUOWNERTRANSFLAG AS SECUOWNERTRANSFLAG -- ֤ȯ����Ȩ�Ƿ�ת��
                  , OTCFLAG AS OTCFLAG -- �����������߱�ʶ
                  , VALIDNETTINGFLAG AS VALIDNETTINGFLAG -- ��Ч�������Э���ʶ
                  , VALIDNETAGREEMENTID AS VALIDNETAGREEMENTID -- ��Ч�������Э��ID
                  , OTCTYPE AS OTCTYPE -- ����������������
                  , DEPOSITRISKPERIOD AS DEPOSITRISKPERIOD -- ��֤������ڼ�
                  , MTM AS MTM -- ���óɱ�
                  , MTMCURRENCY AS MTMCURRENCY -- ���óɱ�����
                  , BUYERORSELLER AS BUYERORSELLER -- ������
                  , QUALROFLAG AS QUALROFLAG -- �ϸ�����ʲ���ʶ
                  , ROISSUERPERFORMFLAG AS ROISSUERPERFORMFLAG -- �����ʲ��������Ƿ�����Լ
                  , BUYERINSOLVENCYFLAG AS BUYERINSOLVENCYFLAG -- ���ñ������Ƿ��Ʋ�
                  , NONPAYMENTFEES AS NONPAYMENTFEES -- ��δ֧������
                  , RETAILEXPOFLAG AS RETAILEXPOFLAG -- ���۱�¶��ʶ
                  , RETAILCLAIMTYPE AS RETAILCLAIMTYPE -- ����ծȨ����
                  , MORTGAGETYPE AS MORTGAGETYPE -- ס����Ѻ��������
                  , DEBTORNUMBER AS DEBTORNUMBER -- ����˸���
                  , EXPONUMBER AS EXPONUMBER -- ���ձ�¶����
                  , PDPOOLMODELID AS PDPOOLMODELID -- PD�ֳ�ģ��ID
                  , LGDPOOLMODELID AS LGDPOOLMODELID -- LGD�ֳ�ģ��ID
                  , CCFPOOLMODELID AS CCFPOOLMODELID -- CCF�ֳ�ģ��ID
                  , PDPOOLID AS PDPOOLID -- ����PD��ID
                  , LGDPOOLID AS LGDPOOLID -- ����LGD��ID
                  , CCFPOOLID AS CCFPOOLID -- ����CCF��ID
                  , ABSUAFLAG AS ABSUAFLAG -- �ʲ�֤ȯ�������ʲ���ʶ
                  , ABSPOOLID AS ABSPOOLID -- ֤ȯ���ʲ���ID
                  , ABSPROPORTION AS ABSPROPORTION -- �ʲ�֤ȯ������
                  , GROUPID AS GROUPID -- ������
                  , ORGSORTNO AS ORGSORTNO --�������������
                  , LTV AS LTV --�����ֵ��
                  , AGING AS AGING --����
                  , NEWDEFAULTDEBTFLAG AS NEWDEFAULTDEBTFLAG --����ΥԼծ���ʶ
                  , DEFAULTDATE AS DEFAULTDATE --ΥԼʱ��
                   FROM RWA_DEV.RWA_TY_EXPOSURE WHERE DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD'       );
  COMMIT;

  /*�������ÿ��ķ��ձ�¶��Ϣ*/
  INSERT /*+ APPEND */
  INTO RWA_DEV.RWA_EI_EXPOSURE
    (DATADATE -- ��������
    ,
     DATANO -- ������ˮ��
    ,
     EXPOSUREID -- ���ձ�¶ID
    ,
     DUEID -- ծ��ID
    ,
     SSYSID -- ԴϵͳID
    ,
     CONTRACTID -- ��ͬID
    ,
     CLIENTID -- ��������ID
    ,
     SORGID -- Դ����ID
    ,
     SORGNAME -- Դ��������
    ,
     ORGID -- ��������ID
    ,
     ORGNAME -- ������������
    ,
     ACCORGID -- �������ID
    ,
     ACCORGNAME -- �����������
    ,
     INDUSTRYID -- ������ҵ����
    ,
     INDUSTRYNAME -- ������ҵ����
    ,
     BUSINESSLINE -- ����
    ,
     ASSETTYPE -- �ʲ�����
    ,
     ASSETSUBTYPE -- �ʲ�С��
    ,
     BUSINESSTYPEID -- ҵ��Ʒ�ִ���
    ,
     BUSINESSTYPENAME -- ҵ��Ʒ������
    ,
     CREDITRISKDATATYPE -- ���÷�����������
    ,
     ASSETTYPEOFHAIRCUTS -- �ۿ�ϵ����Ӧ�ʲ����
    ,
     BUSINESSTYPESTD -- Ȩ�ط�ҵ������
    ,
     EXPOCLASSSTD -- Ȩ�ط���¶����
    ,
     EXPOSUBCLASSSTD -- Ȩ�ط���¶С��
    ,
     EXPOCLASSIRB -- ��������¶����
    ,
     EXPOSUBCLASSIRB -- ��������¶С��
    ,
     EXPOBELONG -- ��¶������ʶ
    ,
     BOOKTYPE -- �˻����
    ,
     REGUTRANTYPE -- ��ܽ�������
    ,
     REPOTRANFLAG -- �ع����ױ�ʶ
    ,
     REVAFREQUENCY -- �ع�Ƶ��
    ,
     CURRENCY -- ����
    ,
     NORMALPRINCIPAL -- �����������
    ,
     OVERDUEBALANCE -- �������
    ,
     NONACCRUALBALANCE -- ��Ӧ�����
    ,
     ONSHEETBALANCE -- �������
    ,
     NORMALINTEREST -- ������Ϣ
    ,
     ONDEBITINTEREST -- ����ǷϢ
    ,
     OFFDEBITINTEREST -- ����ǷϢ
    ,
     EXPENSERECEIVABLE -- Ӧ�շ���
    ,
     ASSETBALANCE -- �ʲ����
    ,
     ACCSUBJECT1 -- ��Ŀһ
    ,
     ACCSUBJECT2 -- ��Ŀ��
    ,
     ACCSUBJECT3 -- ��Ŀ��
    ,
     STARTDATE -- ��ʼ����
    ,
     DUEDATE -- ��������
    ,
     ORIGINALMATURITY -- ԭʼ����
    ,
     RESIDUALM -- ʣ������
    ,
     RISKCLASSIFY -- ���շ���
    ,
     EXPOSURESTATUS -- ���ձ�¶״̬
    ,
     OVERDUEDAYS -- ��������
    ,
     SPECIALPROVISION -- ר��׼����
    ,
     GENERALPROVISION -- һ��׼����
    ,
     ESPECIALPROVISION -- �ر�׼����
    ,
     WRITTENOFFAMOUNT -- �Ѻ������
    ,
     OFFEXPOSOURCE -- ���Ⱪ¶��Դ
    ,
     OFFBUSINESSTYPE -- ����ҵ������
    ,
     OFFBUSINESSSDVSSTD -- Ȩ�ط�����ҵ������ϸ��
    ,
     UNCONDCANCELFLAG -- �Ƿ����ʱ����������
    ,
     CCFLEVEL -- ����ת��ϵ������
    ,
     CCFAIRB -- �߼�������ת��ϵ��
    ,
     CLAIMSLEVEL -- ծȨ����
    ,
     BONDFLAG -- �Ƿ�Ϊծȯ
    ,
     BONDISSUEINTENT -- ծȯ����Ŀ��
    ,
     NSUREALPROPERTYFLAG -- �Ƿ�����ò�����
    ,
     REPASSETTERMTYPE -- ��ծ�ʲ���������
    ,
     DEPENDONFPOBFLAG -- �Ƿ�����������δ��ӯ��
    ,
     IRATING -- �ڲ�����
    ,
     PD -- ΥԼ����
    ,
     LGDLEVEL -- ΥԼ��ʧ�ʼ���
    ,
     LGDAIRB -- �߼���ΥԼ��ʧ��
    ,
     MAIRB -- �߼�����Ч����
    ,
     EADAIRB -- �߼���ΥԼ���ձ�¶
    ,
     DEFAULTFLAG -- ΥԼ��ʶ
    ,
     BEEL -- ��ΥԼ��¶Ԥ����ʧ����
    ,
     DEFAULTLGD -- ��ΥԼ��¶ΥԼ��ʧ��
    ,
     EQUITYEXPOFLAG -- ��Ȩ��¶��ʶ
    ,
     EQUITYINVESTTYPE -- ��ȨͶ�ʶ�������
    ,
     EQUITYINVESTCAUSE -- ��ȨͶ���γ�ԭ��
    ,
     SLFLAG -- רҵ�����ʶ
    ,
     SLTYPE -- רҵ��������
    ,
     PFPHASE -- ��Ŀ���ʽ׶�
    ,
     REGURATING -- �������
    ,
     CBRCMPRATINGFLAG -- ������϶������Ƿ��Ϊ����
    ,
     LARGEFLUCFLAG -- �Ƿ񲨶��Խϴ�
    ,
     LIQUEXPOFLAG -- �Ƿ���������з��ձ�¶
    ,
     PAYMENTDEALFLAG -- �Ƿ����Ը�ģʽ
    ,
     DELAYTRADINGDAYS -- �ӳٽ�������
    ,
     SECURITIESFLAG -- �м�֤ȯ��ʶ
    ,
     SECUISSUERID -- ֤ȯ������ID
    ,
     RATINGDURATIONTYPE -- ������������
    ,
     SECUISSUERATING -- ֤ȯ���еȼ�
    ,
     SECURESIDUALM -- ֤ȯʣ������
    ,
     SECUREVAFREQUENCY -- ֤ȯ�ع�Ƶ��
    ,
     CCPTRANFLAG -- �Ƿ����뽻�׶�����ؽ���
    ,
     CCPID -- ���뽻�׶���ID
    ,
     QUALCCPFLAG -- �Ƿ�ϸ����뽻�׶���
    ,
     BANKROLE -- ���н�ɫ
    ,
     CLEARINGMETHOD -- ���㷽ʽ
    ,
     BANKASSETFLAG -- �Ƿ������ύ�ʲ�
    ,
     MATCHCONDITIONS -- �����������
    ,
     SFTFLAG -- ֤ȯ���ʽ��ױ�ʶ
    ,
     MASTERNETAGREEFLAG -- ���������Э���ʶ
    ,
     MASTERNETAGREEID -- ���������Э��ID
    ,
     SFTTYPE -- ֤ȯ���ʽ�������
    ,
     SECUOWNERTRANSFLAG -- ֤ȯ����Ȩ�Ƿ�ת��
    ,
     OTCFLAG -- �����������߱�ʶ
    ,
     VALIDNETTINGFLAG -- ��Ч�������Э���ʶ
    ,
     VALIDNETAGREEMENTID -- ��Ч�������Э��ID
    ,
     OTCTYPE -- ����������������
    ,
     DEPOSITRISKPERIOD -- ��֤������ڼ�
    ,
     MTM -- ���óɱ�
    ,
     MTMCURRENCY -- ���óɱ�����
    ,
     BUYERORSELLER -- ������
    ,
     QUALROFLAG -- �ϸ�����ʲ���ʶ
    ,
     ROISSUERPERFORMFLAG -- �����ʲ��������Ƿ�����Լ
    ,
     BUYERINSOLVENCYFLAG -- ���ñ������Ƿ��Ʋ�
    ,
     NONPAYMENTFEES -- ��δ֧������
    ,
     RETAILEXPOFLAG -- ���۱�¶��ʶ
    ,
     RETAILCLAIMTYPE -- ����ծȨ����
    ,
     MORTGAGETYPE -- ס����Ѻ��������
    ,
     DEBTORNUMBER -- ����˸���
    ,
     EXPONUMBER -- ���ձ�¶����
    ,
     PDPOOLMODELID -- PD�ֳ�ģ��ID
    ,
     LGDPOOLMODELID -- LGD�ֳ�ģ��ID
    ,
     CCFPOOLMODELID -- CCF�ֳ�ģ��ID
    ,
     PDPOOLID -- ����PD��ID
    ,
     LGDPOOLID -- ����LGD��ID
    ,
     CCFPOOLID -- ����CCF��ID
    ,
     ABSUAFLAG -- �ʲ�֤ȯ�������ʲ���ʶ
    ,
     ABSPOOLID -- ֤ȯ���ʲ���ID
    ,
     ABSPROPORTION -- �ʲ�֤ȯ������
    ,
     GROUPID -- ������
    ,
     ORGSORTNO --�������������
    ,
     LTV --�����ֵ��
    ,
     AGING --����
    ,
     NEWDEFAULTDEBTFLAG --����ΥԼծ���ʶ
    ,
     DEFAULTDATE --ΥԼʱ��
     )
    SELECT DATADATE   AS DATADATE -- ��������
          ,
           DATANO     AS DATANO -- ������ˮ��
          ,
           EXPOSUREID AS EXPOSUREID -- ���ձ�¶ID
          ,
           DUEID      AS DUEID -- ծ��ID
          ,
           SSYSID     AS SSYSID -- ԴϵͳID
          ,
           CONTRACTID AS CONTRACTID -- ��ͬID
          ,
           CLIENTID   AS CLIENTID -- ��������ID
          ,
           SORGID     AS SORGID -- Դ����ID
          ,
           SORGNAME   AS SORGNAME -- Դ��������
          ,
           ORGID      AS ORGID -- ��������ID
          ,
           ORGNAME    AS ORGNAME -- ������������
          ,
           ACCORGID   AS ACCORGID -- �������ID
          ,
           ACCORGNAME AS ACCORGNAME -- �����������
          ,
           INDUSTRYID AS INDUSTRYID --������ҵ����
          ,
           NVL       (INDUSTRYNAME, 'δ֪'        �� AS INDUSTRYNAME --������ҵ����
                  , BUSINESSLINE AS BUSINESSLINE -- ����
                  , ASSETTYPE AS ASSETTYPE -- �ʲ�����
                  , ASSETSUBTYPE AS ASSETSUBTYPE -- �ʲ�С��
                  , BUSINESSTYPEID AS BUSINESSTYPEID -- ҵ��Ʒ�ִ���
                  , BUSINESSTYPENAME AS BUSINESSTYPENAME -- ҵ��Ʒ������
                  , CREDITRISKDATATYPE AS CREDITRISKDATATYPE -- ���÷�����������
                  , ASSETTYPEOFHAIRCUTS AS ASSETTYPEOFHAIRCUTS -- �ۿ�ϵ����Ӧ�ʲ����
                  , BUSINESSTYPESTD AS BUSINESSTYPESTD -- Ȩ�ط�ҵ������
                  , EXPOCLASSSTD AS EXPOCLASSSTD -- Ȩ�ط���¶����
                  , EXPOSUBCLASSSTD AS EXPOSUBCLASSSTD -- Ȩ�ط���¶С��
                  , EXPOCLASSIRB AS EXPOCLASSIRB -- ��������¶����
                  , EXPOSUBCLASSIRB AS EXPOSUBCLASSIRB -- ��������¶С��
                  , EXPOBELONG AS EXPOBELONG -- ��¶������ʶ
                  , BOOKTYPE AS BOOKTYPE -- �˻����
                  , REGUTRANTYPE AS REGUTRANTYPE -- ��ܽ�������
                  , REPOTRANFLAG AS REPOTRANFLAG -- �ع����ױ�ʶ
                  , REVAFREQUENCY AS REVAFREQUENCY -- �ع�Ƶ��
                  , CURRENCY AS CURRENCY -- ����
                  , NORMALPRINCIPAL AS NORMALPRINCIPAL -- �����������
                  , OVERDUEBALANCE AS OVERDUEBALANCE -- �������
                  , NONACCRUALBALANCE AS NONACCRUALBALANCE -- ��Ӧ�����
                  , ONSHEETBALANCE AS ONSHEETBALANCE -- �������
                  , NORMALINTEREST AS NORMALINTEREST -- ������Ϣ
                  , ONDEBITINTEREST AS ONDEBITINTEREST -- ����ǷϢ
                  , OFFDEBITINTEREST AS OFFDEBITINTEREST -- ����ǷϢ
                  , EXPENSERECEIVABLE AS EXPENSERECEIVABLE -- Ӧ�շ���
                  , ASSETBALANCE AS ASSETBALANCE -- �ʲ����
                  , ACCSUBJECT1 AS ACCSUBJECT1 -- ��Ŀһ
                  , ACCSUBJECT2 AS ACCSUBJECT2 -- ��Ŀ��
                  , ACCSUBJECT3 AS ACCSUBJECT3 -- ��Ŀ��
                  , TO_CHAR(TO_DATE(STARTDATE, 'YYYY-MM-DD'       ), 'YYYY-MM-DD'       ) AS STARTDATE -- ��ʼ����
                  , TO_CHAR(TO_DATE(DUEDATE, 'YYYY-MM-DD'       ), 'YYYY-MM-DD'       ) AS DUEDATE -- ��������
                  , ORIGINALMATURITY AS ORIGINALMATURITY -- ԭʼ����
                  , RESIDUALM AS RESIDUALM -- ʣ������
                  , RISKCLASSIFY AS RISKCLASSIFY -- ���շ���
                  , EXPOSURESTATUS AS EXPOSURESTATUS -- ���ձ�¶״̬
                  , OVERDUEDAYS AS OVERDUEDAYS -- ��������
                  , SPECIALPROVISION AS SPECIALPROVISION -- ר��׼����
                  , GENERALPROVISION AS GENERALPROVISION -- һ��׼����
                  , ESPECIALPROVISION AS ESPECIALPROVISION -- �ر�׼����
                  , WRITTENOFFAMOUNT AS WRITTENOFFAMOUNT -- �Ѻ������
                  , OFFEXPOSOURCE AS OFFEXPOSOURCE -- ���Ⱪ¶��Դ
                  , OFFBUSINESSTYPE AS OFFBUSINESSTYPE -- ����ҵ������
                  , OFFBUSINESSSDVSSTD AS OFFBUSINESSSDVSSTD -- Ȩ�ط�����ҵ������ϸ��
                  , UNCONDCANCELFLAG AS UNCONDCANCELFLAG -- �Ƿ����ʱ����������
                  , CCFLEVEL AS CCFLEVEL -- ����ת��ϵ������
                  , CCFAIRB AS CCFAIRB -- �߼�������ת��ϵ��
                  , CLAIMSLEVEL AS CLAIMSLEVEL -- ծȨ����
                  , BONDFLAG AS BONDFLAG -- �Ƿ�Ϊծȯ
                  , BONDISSUEINTENT AS BONDISSUEINTENT -- ծȯ����Ŀ��
                  , NSUREALPROPERTYFLAG AS NSUREALPROPERTYFLAG -- �Ƿ�����ò�����
                  , REPASSETTERMTYPE AS REPASSETTERMTYPE -- ��ծ�ʲ���������
                  , DEPENDONFPOBFLAG AS DEPENDONFPOBFLAG -- �Ƿ�����������δ��ӯ��
                  , IRATING AS IRATING -- �ڲ�����
                  , PD AS PD -- ΥԼ����
                  , LGDLEVEL AS LGDLEVEL -- ΥԼ��ʧ�ʼ���
                  , LGDAIRB AS LGDAIRB -- �߼���ΥԼ��ʧ��
                  , MAIRB AS MAIRB -- �߼�����Ч����
                  , EADAIRB AS EADAIRB -- �߼���ΥԼ���ձ�¶
                  , DEFAULTFLAG AS DEFAULTFLAG -- ΥԼ��ʶ
                  , BEEL AS BEEL -- ��ΥԼ��¶Ԥ����ʧ����
                  , DEFAULTLGD AS DEFAULTLGD -- ��ΥԼ��¶ΥԼ��ʧ��
                  , EQUITYEXPOFLAG AS EQUITYEXPOFLAG -- ��Ȩ��¶��ʶ
                  , EQUITYINVESTTYPE AS EQUITYINVESTTYPE -- ��ȨͶ�ʶ�������
                  , EQUITYINVESTCAUSE AS EQUITYINVESTCAUSE -- ��ȨͶ���γ�ԭ��
                  , SLFLAG AS SLFLAG -- רҵ�����ʶ
                  , SLTYPE AS SLTYPE -- רҵ��������
                  , PFPHASE AS PFPHASE -- ��Ŀ���ʽ׶�
                  , REGURATING AS REGURATING -- �������
                  , CBRCMPRATINGFLAG AS CBRCMPRATINGFLAG -- ������϶������Ƿ��Ϊ����
                  , LARGEFLUCFLAG AS LARGEFLUCFLAG -- �Ƿ񲨶��Խϴ�
                  , LIQUEXPOFLAG AS LIQUEXPOFLAG -- �Ƿ���������з��ձ�¶
                  , PAYMENTDEALFLAG AS PAYMENTDEALFLAG -- �Ƿ����Ը�ģʽ
                  , DELAYTRADINGDAYS AS DELAYTRADINGDAYS -- �ӳٽ�������
                  , SECURITIESFLAG AS SECURITIESFLAG -- �м�֤ȯ��ʶ
                  , SECUISSUERID AS SECUISSUERID -- ֤ȯ������ID
                  , RATINGDURATIONTYPE AS RATINGDURATIONTYPE -- ������������
                  , SECUISSUERATING AS SECUISSUERATING -- ֤ȯ���еȼ�
                  , SECURESIDUALM AS SECURESIDUALM -- ֤ȯʣ������
                  , SECUREVAFREQUENCY AS SECUREVAFREQUENCY -- ֤ȯ�ع�Ƶ��
                  , CCPTRANFLAG AS CCPTRANFLAG -- �Ƿ����뽻�׶�����ؽ���
                  , CCPID AS CCPID -- ���뽻�׶���ID
                  , QUALCCPFLAG AS QUALCCPFLAG -- �Ƿ�ϸ����뽻�׶���
                  , BANKROLE AS BANKROLE -- ���н�ɫ
                  , CLEARINGMETHOD AS CLEARINGMETHOD -- ���㷽ʽ
                  , BANKASSETFLAG AS BANKASSETFLAG -- �Ƿ������ύ�ʲ�
                  , MATCHCONDITIONS AS MATCHCONDITIONS -- �����������
                  , SFTFLAG AS SFTFLAG -- ֤ȯ���ʽ��ױ�ʶ
                  , MASTERNETAGREEFLAG AS MASTERNETAGREEFLAG -- ���������Э���ʶ
                  , MASTERNETAGREEID AS MASTERNETAGREEID -- ���������Э��ID
                  , SFTTYPE AS SFTTYPE -- ֤ȯ���ʽ�������
                  , SECUOWNERTRANSFLAG AS SECUOWNERTRANSFLAG -- ֤ȯ����Ȩ�Ƿ�ת��
                  , OTCFLAG AS OTCFLAG -- �����������߱�ʶ
                  , VALIDNETTINGFLAG AS VALIDNETTINGFLAG -- ��Ч�������Э���ʶ
                  , VALIDNETAGREEMENTID AS VALIDNETAGREEMENTID -- ��Ч�������Э��ID
                  , OTCTYPE AS OTCTYPE -- ����������������
                  , DEPOSITRISKPERIOD AS DEPOSITRISKPERIOD -- ��֤������ڼ�
                  , MTM AS MTM -- ���óɱ�
                  , MTMCURRENCY AS MTMCURRENCY -- ���óɱ�����
                  , BUYERORSELLER AS BUYERORSELLER -- ������
                  , QUALROFLAG AS QUALROFLAG -- �ϸ�����ʲ���ʶ
                  , ROISSUERPERFORMFLAG AS ROISSUERPERFORMFLAG -- �����ʲ��������Ƿ�����Լ
                  , BUYERINSOLVENCYFLAG AS BUYERINSOLVENCYFLAG -- ���ñ������Ƿ��Ʋ�
                  , NONPAYMENTFEES AS NONPAYMENTFEES -- ��δ֧������
                  , RETAILEXPOFLAG AS RETAILEXPOFLAG -- ���۱�¶��ʶ
                  , RETAILCLAIMTYPE AS RETAILCLAIMTYPE -- ����ծȨ����
                  , MORTGAGETYPE AS MORTGAGETYPE -- ס����Ѻ��������
                  , DEBTORNUMBER AS DEBTORNUMBER -- ����˸���
                  , EXPONUMBER AS EXPONUMBER -- ���ձ�¶����
                  , PDPOOLMODELID AS PDPOOLMODELID -- PD�ֳ�ģ��ID
                  , LGDPOOLMODELID AS LGDPOOLMODELID -- LGD�ֳ�ģ��ID
                  , CCFPOOLMODELID AS CCFPOOLMODELID -- CCF�ֳ�ģ��ID
                  , PDPOOLID AS PDPOOLID -- ����PD��ID
                  , LGDPOOLID AS LGDPOOLID -- ����LGD��ID
                  , CCFPOOLID AS CCFPOOLID -- ����CCF��ID
                  , ABSUAFLAG AS ABSUAFLAG -- �ʲ�֤ȯ�������ʲ���ʶ
                  , ABSPOOLID AS ABSPOOLID -- ֤ȯ���ʲ���ID
                  , ABSPROPORTION AS ABSPROPORTION -- �ʲ�֤ȯ������
                  , GROUPID AS GROUPID -- ������
                  , ORGSORTNO AS ORGSORTNO --�������������
                  , LTV AS LTV --�����ֵ��
                  , AGING AS AGING --����
                  , NEWDEFAULTDEBTFLAG AS NEWDEFAULTDEBTFLAG --����ΥԼծ���ʶ
                  , DEFAULTDATE AS DEFAULTDATE --ΥԼʱ��
                   FROM RWA_DEV.RWA_XYK_EXPOSURE WHERE DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD'       );
  COMMIT;

  /*����Ͷ�ʵķ��ձ�¶��Ϣ*/
  INSERT /*+ APPEND */
  INTO RWA_DEV.RWA_EI_EXPOSURE
    (DATADATE -- ��������
    ,
     DATANO -- ������ˮ��
    ,
     EXPOSUREID -- ���ձ�¶ID
    ,
     DUEID -- ծ��ID
    ,
     SSYSID -- ԴϵͳID
    ,
     CONTRACTID -- ��ͬID
    ,
     CLIENTID -- ��������ID
    ,
     SORGID -- Դ����ID
    ,
     SORGNAME -- Դ��������
    ,
     ORGID -- ��������ID
    ,
     ORGNAME -- ������������
    ,
     ACCORGID -- �������ID
    ,
     ACCORGNAME -- �����������
    ,
     INDUSTRYID -- ������ҵ����
    ,
     INDUSTRYNAME -- ������ҵ����
    ,
     BUSINESSLINE -- ����
    ,
     ASSETTYPE -- �ʲ�����
    ,
     ASSETSUBTYPE -- �ʲ�С��
    ,
     BUSINESSTYPEID -- ҵ��Ʒ�ִ���
    ,
     BUSINESSTYPENAME -- ҵ��Ʒ������
    ,
     CREDITRISKDATATYPE -- ���÷�����������
    ,
     ASSETTYPEOFHAIRCUTS -- �ۿ�ϵ����Ӧ�ʲ����
    ,
     BUSINESSTYPESTD -- Ȩ�ط�ҵ������
    ,
     EXPOCLASSSTD -- Ȩ�ط���¶����
    ,
     EXPOSUBCLASSSTD -- Ȩ�ط���¶С��
    ,
     EXPOCLASSIRB -- ��������¶����
    ,
     EXPOSUBCLASSIRB -- ��������¶С��
    ,
     EXPOBELONG -- ��¶������ʶ
    ,
     BOOKTYPE -- �˻����
    ,
     REGUTRANTYPE -- ��ܽ�������
    ,
     REPOTRANFLAG -- �ع����ױ�ʶ
    ,
     REVAFREQUENCY -- �ع�Ƶ��
    ,
     CURRENCY -- ����
    ,
     NORMALPRINCIPAL -- �����������
    ,
     OVERDUEBALANCE -- �������
    ,
     NONACCRUALBALANCE -- ��Ӧ�����
    ,
     ONSHEETBALANCE -- �������
    ,
     NORMALINTEREST -- ������Ϣ
    ,
     ONDEBITINTEREST -- ����ǷϢ
    ,
     OFFDEBITINTEREST -- ����ǷϢ
    ,
     EXPENSERECEIVABLE -- Ӧ�շ���
    ,
     ASSETBALANCE -- �ʲ����
    ,
     ACCSUBJECT1 -- ��Ŀһ
    ,
     ACCSUBJECT2 -- ��Ŀ��
    ,
     ACCSUBJECT3 -- ��Ŀ��
    ,
     STARTDATE -- ��ʼ����
    ,
     DUEDATE -- ��������
    ,
     ORIGINALMATURITY -- ԭʼ����
    ,
     RESIDUALM -- ʣ������
    ,
     RISKCLASSIFY -- ���շ���
    ,
     EXPOSURESTATUS -- ���ձ�¶״̬
    ,
     OVERDUEDAYS -- ��������
    ,
     SPECIALPROVISION -- ר��׼����
    ,
     GENERALPROVISION -- һ��׼����
    ,
     ESPECIALPROVISION -- �ر�׼����
    ,
     WRITTENOFFAMOUNT -- �Ѻ������
    ,
     OFFEXPOSOURCE -- ���Ⱪ¶��Դ
    ,
     OFFBUSINESSTYPE -- ����ҵ������
    ,
     OFFBUSINESSSDVSSTD -- Ȩ�ط�����ҵ������ϸ��
    ,
     UNCONDCANCELFLAG -- �Ƿ����ʱ����������
    ,
     CCFLEVEL -- ����ת��ϵ������
    ,
     CCFAIRB -- �߼�������ת��ϵ��
    ,
     CLAIMSLEVEL -- ծȨ����
    ,
     BONDFLAG -- �Ƿ�Ϊծȯ
    ,
     BONDISSUEINTENT -- ծȯ����Ŀ��
    ,
     NSUREALPROPERTYFLAG -- �Ƿ�����ò�����
    ,
     REPASSETTERMTYPE -- ��ծ�ʲ���������
    ,
     DEPENDONFPOBFLAG -- �Ƿ�����������δ��ӯ��
    ,
     IRATING -- �ڲ�����
    ,
     PD -- ΥԼ����
    ,
     LGDLEVEL -- ΥԼ��ʧ�ʼ���
    ,
     LGDAIRB -- �߼���ΥԼ��ʧ��
    ,
     MAIRB -- �߼�����Ч����
    ,
     EADAIRB -- �߼���ΥԼ���ձ�¶
    ,
     DEFAULTFLAG -- ΥԼ��ʶ
    ,
     BEEL -- ��ΥԼ��¶Ԥ����ʧ����
    ,
     DEFAULTLGD -- ��ΥԼ��¶ΥԼ��ʧ��
    ,
     EQUITYEXPOFLAG -- ��Ȩ��¶��ʶ
    ,
     EQUITYINVESTTYPE -- ��ȨͶ�ʶ�������
    ,
     EQUITYINVESTCAUSE -- ��ȨͶ���γ�ԭ��
    ,
     SLFLAG -- רҵ�����ʶ
    ,
     SLTYPE -- רҵ��������
    ,
     PFPHASE -- ��Ŀ���ʽ׶�
    ,
     REGURATING -- �������
    ,
     CBRCMPRATINGFLAG -- ������϶������Ƿ��Ϊ����
    ,
     LARGEFLUCFLAG -- �Ƿ񲨶��Խϴ�
    ,
     LIQUEXPOFLAG -- �Ƿ���������з��ձ�¶
    ,
     PAYMENTDEALFLAG -- �Ƿ����Ը�ģʽ
    ,
     DELAYTRADINGDAYS -- �ӳٽ�������
    ,
     SECURITIESFLAG -- �м�֤ȯ��ʶ
    ,
     SECUISSUERID -- ֤ȯ������ID
    ,
     RATINGDURATIONTYPE -- ������������
    ,
     SECUISSUERATING -- ֤ȯ���еȼ�
    ,
     SECURESIDUALM -- ֤ȯʣ������
    ,
     SECUREVAFREQUENCY -- ֤ȯ�ع�Ƶ��
    ,
     CCPTRANFLAG -- �Ƿ����뽻�׶�����ؽ���
    ,
     CCPID -- ���뽻�׶���ID
    ,
     QUALCCPFLAG -- �Ƿ�ϸ����뽻�׶���
    ,
     BANKROLE -- ���н�ɫ
    ,
     CLEARINGMETHOD -- ���㷽ʽ
    ,
     BANKASSETFLAG -- �Ƿ������ύ�ʲ�
    ,
     MATCHCONDITIONS -- �����������
    ,
     SFTFLAG -- ֤ȯ���ʽ��ױ�ʶ
    ,
     MASTERNETAGREEFLAG -- ���������Э���ʶ
    ,
     MASTERNETAGREEID -- ���������Э��ID
    ,
     SFTTYPE -- ֤ȯ���ʽ�������
    ,
     SECUOWNERTRANSFLAG -- ֤ȯ����Ȩ�Ƿ�ת��
    ,
     OTCFLAG -- �����������߱�ʶ
    ,
     VALIDNETTINGFLAG -- ��Ч�������Э���ʶ
    ,
     VALIDNETAGREEMENTID -- ��Ч�������Э��ID
    ,
     OTCTYPE -- ����������������
    ,
     DEPOSITRISKPERIOD -- ��֤������ڼ�
    ,
     MTM -- ���óɱ�
    ,
     MTMCURRENCY -- ���óɱ�����
    ,
     BUYERORSELLER -- ������
    ,
     QUALROFLAG -- �ϸ�����ʲ���ʶ
    ,
     ROISSUERPERFORMFLAG -- �����ʲ��������Ƿ�����Լ
    ,
     BUYERINSOLVENCYFLAG -- ���ñ������Ƿ��Ʋ�
    ,
     NONPAYMENTFEES -- ��δ֧������
    ,
     RETAILEXPOFLAG -- ���۱�¶��ʶ
    ,
     RETAILCLAIMTYPE -- ����ծȨ����
    ,
     MORTGAGETYPE -- ס����Ѻ��������
    ,
     DEBTORNUMBER -- ����˸���
    ,
     EXPONUMBER -- ���ձ�¶����
    ,
     PDPOOLMODELID -- PD�ֳ�ģ��ID
    ,
     LGDPOOLMODELID -- LGD�ֳ�ģ��ID
    ,
     CCFPOOLMODELID -- CCF�ֳ�ģ��ID
    ,
     PDPOOLID -- ����PD��ID
    ,
     LGDPOOLID -- ����LGD��ID
    ,
     CCFPOOLID -- ����CCF��ID
    ,
     ABSUAFLAG -- �ʲ�֤ȯ�������ʲ���ʶ
    ,
     ABSPOOLID -- ֤ȯ���ʲ���ID
    ,
     ABSPROPORTION -- �ʲ�֤ȯ������
    ,
     GROUPID -- ������
    ,
     ORGSORTNO --�������������
    ,
     LTV --�����ֵ��
    ,
     AGING --����
    ,
     NEWDEFAULTDEBTFLAG --����ΥԼծ���ʶ
    ,
     DEFAULTDATE --ΥԼʱ��
     )
    SELECT DATADATE   AS DATADATE -- ��������
          ,
           DATANO     AS DATANO -- ������ˮ��
          ,
           EXPOSUREID AS EXPOSUREID -- ���ձ�¶ID
          ,
           DUEID      AS DUEID -- ծ��ID
          ,
           SSYSID     AS SSYSID -- ԴϵͳID
          ,
           CONTRACTID AS CONTRACTID -- ��ͬID
          ,
           CLIENTID   AS CLIENTID -- ��������ID
          ,
           SORGID     AS SORGID -- Դ����ID
          ,
           SORGNAME   AS SORGNAME -- Դ��������
          ,
           ORGID      AS ORGID -- ��������ID
          ,
           ORGNAME    AS ORGNAME -- ������������
          ,
           ACCORGID   AS ACCORGID -- �������ID
          ,
           ACCORGNAME AS ACCORGNAME -- �����������
          ,
           INDUSTRYID AS INDUSTRYID --������ҵ����
          ,
           NVL       (INDUSTRYNAME, 'δ֪'        �� AS INDUSTRYNAME --������ҵ����
                  , BUSINESSLINE AS BUSINESSLINE -- ����
                  , ASSETTYPE AS ASSETTYPE -- �ʲ�����
                  , ASSETSUBTYPE AS ASSETSUBTYPE -- �ʲ�С��
                  , BUSINESSTYPEID AS BUSINESSTYPEID -- ҵ��Ʒ�ִ���
                  , BUSINESSTYPENAME AS BUSINESSTYPENAME -- ҵ��Ʒ������
                  , CREDITRISKDATATYPE AS CREDITRISKDATATYPE -- ���÷�����������
                  , ASSETTYPEOFHAIRCUTS AS ASSETTYPEOFHAIRCUTS -- �ۿ�ϵ����Ӧ�ʲ����
                  , BUSINESSTYPESTD AS BUSINESSTYPESTD -- Ȩ�ط�ҵ������
                  , EXPOCLASSSTD AS EXPOCLASSSTD -- Ȩ�ط���¶����
                  , EXPOSUBCLASSSTD AS EXPOSUBCLASSSTD -- Ȩ�ط���¶С��
                  , EXPOCLASSIRB AS EXPOCLASSIRB -- ��������¶����
                  , EXPOSUBCLASSIRB AS EXPOSUBCLASSIRB -- ��������¶С��
                  , EXPOBELONG AS EXPOBELONG -- ��¶������ʶ
                  , BOOKTYPE AS BOOKTYPE -- �˻����
                  , REGUTRANTYPE AS REGUTRANTYPE -- ��ܽ�������
                  , REPOTRANFLAG AS REPOTRANFLAG -- �ع����ױ�ʶ
                  , REVAFREQUENCY AS REVAFREQUENCY -- �ع�Ƶ��
                  , CURRENCY AS CURRENCY -- ����
                  ,CASE ACCSUBJECT1
             WHEN '11010101' THEN
              NORMALPRINCIPAL +
              DECODE(SBJT3, '11010103', NVL(SBJT_VAL3, 0), 0) +
              NVL(SBJT_VAL4, 0) + NVL(SBJT_VAL5, 0) --����+���ʼ�ֵ�䶯+Ӧ����Ϣ+Ӧ����Ϣ
             WHEN '11010301' THEN
              NORMALPRINCIPAL +
              DECODE(SBJT3, '11010302', NVL(SBJT_VAL3, 0), 0) +
              NVL(SBJT_VAL4, 0) --����+Ӧ����Ϣ+���ʼ�ֵ�䶯
             WHEN '15010101' THEN
              NORMALPRINCIPAL + NVL(SBJT_VAL2, 0) +
              DECODE(SBJT3, '15010103', NVL(SBJT_VAL3, 0), 0) --����+��Ϣ����+Ӧ����Ϣ
             WHEN '15030101' THEN
              NORMALPRINCIPAL + NVL(SBJT_VAL2, 0) +
              DECODE(SBJT3, '15030103', NVL(SBJT_VAL3, 0), 0) +
              NVL(SBJT_VAL4, 0) --����+��Ϣ����+Ӧ����Ϣ+���ʼ�ֵ�䶯
             ELSE
              NORMALPRINCIPAL
           END        AS NORMALPRINCIPAL -- �����������
                  , OVERDUEBALANCE AS OVERDUEBALANCE -- �������
                  , NONACCRUALBALANCE AS NONACCRUALBALANCE -- ��Ӧ�����
                  ,CASE ACCSUBJECT1
             WHEN '11010101' THEN
              NORMALPRINCIPAL +
              DECODE(SBJT3, '11010103', NVL(SBJT_VAL3, 0), 0) +
              NVL(SBJT_VAL4, 0) + NVL(SBJT_VAL5, 0) --����+���ʼ�ֵ�䶯+Ӧ����Ϣ+Ӧ����Ϣ
             WHEN '11010301' THEN
              NORMALPRINCIPAL +
              DECODE(SBJT3, '11010302', NVL(SBJT_VAL3, 0), 0) +
              NVL(SBJT_VAL4, 0) --����+Ӧ����Ϣ+���ʼ�ֵ�䶯
             WHEN '15010101' THEN
              NORMALPRINCIPAL + NVL(SBJT_VAL2, 0) +
              DECODE(SBJT3, '15010103', NVL(SBJT_VAL3, 0), 0) --����+��Ϣ����+Ӧ����Ϣ
             WHEN '15030101' THEN
              NORMALPRINCIPAL + NVL(SBJT_VAL2, 0) +
              DECODE(SBJT3, '15030103', NVL(SBJT_VAL3, 0), 0) +
              NVL(SBJT_VAL4, 0) --����+��Ϣ����+Ӧ����Ϣ+���ʼ�ֵ�䶯
             ELSE
              NORMALPRINCIPAL
           END    AS ONSHEETBALANCE -- �������
                  , NORMALINTEREST AS NORMALINTEREST -- ������Ϣ
                  , ONDEBITINTEREST AS ONDEBITINTEREST -- ����ǷϢ
                  , OFFDEBITINTEREST AS OFFDEBITINTEREST -- ����ǷϢ
                  , EXPENSERECEIVABLE AS EXPENSERECEIVABLE -- Ӧ�շ���
                  ,CASE ACCSUBJECT1
             WHEN '11010101' THEN
              NORMALPRINCIPAL +
              DECODE(SBJT3, '11010103', NVL(SBJT_VAL3, 0), 0) +
              NVL(SBJT_VAL4, 0) + NVL(SBJT_VAL5, 0) --����+���ʼ�ֵ�䶯+Ӧ����Ϣ+Ӧ����Ϣ
             WHEN '11010301' THEN
              NORMALPRINCIPAL +
              DECODE(SBJT3, '11010302', NVL(SBJT_VAL3, 0), 0) +
              NVL(SBJT_VAL4, 0) --����+Ӧ����Ϣ+���ʼ�ֵ�䶯
             WHEN '15010101' THEN
              NORMALPRINCIPAL + NVL(SBJT_VAL2, 0) +
              DECODE(SBJT3, '15010103', NVL(SBJT_VAL3, 0), 0) --����+��Ϣ����+Ӧ����Ϣ
             WHEN '15030101' THEN
              NORMALPRINCIPAL + NVL(SBJT_VAL2, 0) +
              DECODE(SBJT3, '15030103', NVL(SBJT_VAL3, 0), 0) +
              NVL(SBJT_VAL4, 0) --����+��Ϣ����+Ӧ����Ϣ+���ʼ�ֵ�䶯
             ELSE
              ASSETBALANCE
           END        AS ASSETBALANCE -- �ʲ���� BY WZB 20190910 ���Ϲ��ʼ�ֵ�䶯 ��Ϣ������Ӧ��Ӧ��
                  , ACCSUBJECT1 AS ACCSUBJECT1 -- ��Ŀһ
                  , ACCSUBJECT2 AS ACCSUBJECT2 -- ��Ŀ��
                  , ACCSUBJECT3 AS ACCSUBJECT3 -- ��Ŀ��
                  , TO_CHAR(TO_DATE(STARTDATE, 'YYYY-MM-DD'       ), 'YYYY-MM-DD'       ) AS STARTDATE -- ��ʼ����
                  , TO_CHAR(TO_DATE(DUEDATE, 'YYYY-MM-DD'       ), 'YYYY-MM-DD'       ) AS DUEDATE -- ��������
                  , ORIGINALMATURITY AS ORIGINALMATURITY -- ԭʼ����
                  , RESIDUALM AS RESIDUALM -- ʣ������
                  , RISKCLASSIFY AS RISKCLASSIFY -- ���շ���
                  , EXPOSURESTATUS AS EXPOSURESTATUS -- ���ձ�¶״̬
                  , OVERDUEDAYS AS OVERDUEDAYS -- ��������
                  , SPECIALPROVISION AS SPECIALPROVISION -- ר��׼����
                  , GENERALPROVISION AS GENERALPROVISION -- һ��׼����
                  , ESPECIALPROVISION AS ESPECIALPROVISION -- �ر�׼����
                  , WRITTENOFFAMOUNT AS WRITTENOFFAMOUNT -- �Ѻ������
                  , OFFEXPOSOURCE AS OFFEXPOSOURCE -- ���Ⱪ¶��Դ
                  , OFFBUSINESSTYPE AS OFFBUSINESSTYPE -- ����ҵ������
                  , OFFBUSINESSSDVSSTD AS OFFBUSINESSSDVSSTD -- Ȩ�ط�����ҵ������ϸ��
                  , UNCONDCANCELFLAG AS UNCONDCANCELFLAG -- �Ƿ����ʱ����������
                  , CCFLEVEL AS CCFLEVEL -- ����ת��ϵ������
                  , CCFAIRB AS CCFAIRB -- �߼�������ת��ϵ��
                  , CLAIMSLEVEL AS CLAIMSLEVEL -- ծȨ����
                  , BONDFLAG AS BONDFLAG -- �Ƿ�Ϊծȯ
                  , BONDISSUEINTENT AS BONDISSUEINTENT -- ծȯ����Ŀ��
                  , NSUREALPROPERTYFLAG AS NSUREALPROPERTYFLAG -- �Ƿ�����ò�����
                  , REPASSETTERMTYPE AS REPASSETTERMTYPE -- ��ծ�ʲ���������
                  , DEPENDONFPOBFLAG AS DEPENDONFPOBFLAG -- �Ƿ�����������δ��ӯ��
                  , IRATING AS IRATING -- �ڲ�����
                  , PD AS PD -- ΥԼ����
                  , LGDLEVEL AS LGDLEVEL -- ΥԼ��ʧ�ʼ���
                  , LGDAIRB AS LGDAIRB -- �߼���ΥԼ��ʧ��
                  , MAIRB AS MAIRB -- �߼�����Ч����
                  , EADAIRB AS EADAIRB -- �߼���ΥԼ���ձ�¶
                  , DEFAULTFLAG AS DEFAULTFLAG -- ΥԼ��ʶ
                  , BEEL AS BEEL -- ��ΥԼ��¶Ԥ����ʧ����
                  , DEFAULTLGD AS DEFAULTLGD -- ��ΥԼ��¶ΥԼ��ʧ��
                  , EQUITYEXPOFLAG AS EQUITYEXPOFLAG -- ��Ȩ��¶��ʶ
                  , EQUITYINVESTTYPE AS EQUITYINVESTTYPE -- ��ȨͶ�ʶ�������
                  , EQUITYINVESTCAUSE AS EQUITYINVESTCAUSE -- ��ȨͶ���γ�ԭ��
                  , SLFLAG AS SLFLAG -- רҵ�����ʶ
                  , SLTYPE AS SLTYPE -- רҵ��������
                  , PFPHASE AS PFPHASE -- ��Ŀ���ʽ׶�
                  , REGURATING AS REGURATING -- �������
                  , CBRCMPRATINGFLAG AS CBRCMPRATINGFLAG -- ������϶������Ƿ��Ϊ����
                  , LARGEFLUCFLAG AS LARGEFLUCFLAG -- �Ƿ񲨶��Խϴ�
                  , LIQUEXPOFLAG AS LIQUEXPOFLAG -- �Ƿ���������з��ձ�¶
                  , PAYMENTDEALFLAG AS PAYMENTDEALFLAG -- �Ƿ����Ը�ģʽ
                  , DELAYTRADINGDAYS AS DELAYTRADINGDAYS -- �ӳٽ�������
                  , SECURITIESFLAG AS SECURITIESFLAG -- �м�֤ȯ��ʶ
                  , SECUISSUERID AS SECUISSUERID -- ֤ȯ������ID
                  , RATINGDURATIONTYPE AS RATINGDURATIONTYPE -- ������������
                  , SECUISSUERATING AS SECUISSUERATING -- ֤ȯ���еȼ�
                  , SECURESIDUALM AS SECURESIDUALM -- ֤ȯʣ������
                  , SECUREVAFREQUENCY AS SECUREVAFREQUENCY -- ֤ȯ�ع�Ƶ��
                  , CCPTRANFLAG AS CCPTRANFLAG -- �Ƿ����뽻�׶�����ؽ���
                  , CCPID AS CCPID -- ���뽻�׶���ID
                  , QUALCCPFLAG AS QUALCCPFLAG -- �Ƿ�ϸ����뽻�׶���
                  , BANKROLE AS BANKROLE -- ���н�ɫ
                  , CLEARINGMETHOD AS CLEARINGMETHOD -- ���㷽ʽ
                  , BANKASSETFLAG AS BANKASSETFLAG -- �Ƿ������ύ�ʲ�
                  , MATCHCONDITIONS AS MATCHCONDITIONS -- �����������
                  , SFTFLAG AS SFTFLAG -- ֤ȯ���ʽ��ױ�ʶ
                  , MASTERNETAGREEFLAG AS MASTERNETAGREEFLAG -- ���������Э���ʶ
                  , MASTERNETAGREEID AS MASTERNETAGREEID -- ���������Э��ID
                  , SFTTYPE AS SFTTYPE -- ֤ȯ���ʽ�������
                  , SECUOWNERTRANSFLAG AS SECUOWNERTRANSFLAG -- ֤ȯ����Ȩ�Ƿ�ת��
                  , OTCFLAG AS OTCFLAG -- �����������߱�ʶ
                  , VALIDNETTINGFLAG AS VALIDNETTINGFLAG -- ��Ч�������Э���ʶ
                  , VALIDNETAGREEMENTID AS VALIDNETAGREEMENTID -- ��Ч�������Э��ID
                  , OTCTYPE AS OTCTYPE -- ����������������
                  , DEPOSITRISKPERIOD AS DEPOSITRISKPERIOD -- ��֤������ڼ�
                  , MTM AS MTM -- ���óɱ�
                  , MTMCURRENCY AS MTMCURRENCY -- ���óɱ�����
                  , BUYERORSELLER AS BUYERORSELLER -- ������
                  , QUALROFLAG AS QUALROFLAG -- �ϸ�����ʲ���ʶ
                  , ROISSUERPERFORMFLAG AS ROISSUERPERFORMFLAG -- �����ʲ��������Ƿ�����Լ
                  , BUYERINSOLVENCYFLAG AS BUYERINSOLVENCYFLAG -- ���ñ������Ƿ��Ʋ�
                  , NONPAYMENTFEES AS NONPAYMENTFEES -- ��δ֧������
                  , RETAILEXPOFLAG AS RETAILEXPOFLAG -- ���۱�¶��ʶ
                  , RETAILCLAIMTYPE AS RETAILCLAIMTYPE -- ����ծȨ����
                  , MORTGAGETYPE AS MORTGAGETYPE -- ס����Ѻ��������
                  , DEBTORNUMBER AS DEBTORNUMBER -- ����˸���
                  , EXPONUMBER AS EXPONUMBER -- ���ձ�¶����
                  , PDPOOLMODELID AS PDPOOLMODELID -- PD�ֳ�ģ��ID
                  , LGDPOOLMODELID AS LGDPOOLMODELID -- LGD�ֳ�ģ��ID
                  , CCFPOOLMODELID AS CCFPOOLMODELID -- CCF�ֳ�ģ��ID
                  , PDPOOLID AS PDPOOLID -- ����PD��ID
                  , LGDPOOLID AS LGDPOOLID -- ����LGD��ID
                  , CCFPOOLID AS CCFPOOLID -- ����CCF��ID
                  , ABSUAFLAG AS ABSUAFLAG -- �ʲ�֤ȯ�������ʲ���ʶ
                  , ABSPOOLID AS ABSPOOLID -- ֤ȯ���ʲ���ID
                  , ABSPROPORTION AS ABSPROPORTION -- �ʲ�֤ȯ������
                  , GROUPID AS GROUPID -- ������
                  , ORGSORTNO AS ORGSORTNO --�������������
                  , LTV AS LTV --�����ֵ��
                  , AGING AS AGING --����
                  , NEWDEFAULTDEBTFLAG AS NEWDEFAULTDEBTFLAG --����ΥԼծ���ʶ
                  , DEFAULTDATE AS DEFAULTDATE --ΥԼʱ��
                   FROM RWA_DEV.RWA_TZ_EXPOSURE WHERE DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD'       ) AND (NVL(ACCSUBJECT1, '00000000'       ) <> '11010101'        or EXPOSUBCLASSSTD = '010407'       ) and exposureid not in('B201803296435',
                                                                                                                                                                                                                                     'B201712285095')       ; ---20190910 by wzb �����ծȯ11010101��Ŀ�ǽ����Խ����ʲ���ֻ���г����գ�
  COMMIT;

  /*�����Ȩ�ķ��ձ�¶��Ϣ*/
  INSERT /*+ APPEND */
  INTO RWA_DEV.RWA_EI_EXPOSURE
    (DATADATE -- ��������
    ,
     DATANO -- ������ˮ��
    ,
     EXPOSUREID -- ���ձ�¶ID
    ,
     DUEID -- ծ��ID
    ,
     SSYSID -- ԴϵͳID
    ,
     CONTRACTID -- ��ͬID
    ,
     CLIENTID -- ��������ID
    ,
     SORGID -- Դ����ID
    ,
     SORGNAME -- Դ��������
    ,
     ORGID -- ��������ID
    ,
     ORGNAME -- ������������
    ,
     ACCORGID -- �������ID
    ,
     ACCORGNAME -- �����������
    ,
     INDUSTRYID -- ������ҵ����
    ,
     INDUSTRYNAME -- ������ҵ����
    ,
     BUSINESSLINE -- ����
    ,
     ASSETTYPE -- �ʲ�����
    ,
     ASSETSUBTYPE -- �ʲ�С��
    ,
     BUSINESSTYPEID -- ҵ��Ʒ�ִ���
    ,
     BUSINESSTYPENAME -- ҵ��Ʒ������
    ,
     CREDITRISKDATATYPE -- ���÷�����������
    ,
     ASSETTYPEOFHAIRCUTS -- �ۿ�ϵ����Ӧ�ʲ����
    ,
     BUSINESSTYPESTD -- Ȩ�ط�ҵ������
    ,
     EXPOCLASSSTD -- Ȩ�ط���¶����
    ,
     EXPOSUBCLASSSTD -- Ȩ�ط���¶С��
    ,
     EXPOCLASSIRB -- ��������¶����
    ,
     EXPOSUBCLASSIRB -- ��������¶С��
    ,
     EXPOBELONG -- ��¶������ʶ
    ,
     BOOKTYPE -- �˻����
    ,
     REGUTRANTYPE -- ��ܽ�������
    ,
     REPOTRANFLAG -- �ع����ױ�ʶ
    ,
     REVAFREQUENCY -- �ع�Ƶ��
    ,
     CURRENCY -- ����
    ,
     NORMALPRINCIPAL -- �����������
    ,
     OVERDUEBALANCE -- �������
    ,
     NONACCRUALBALANCE -- ��Ӧ�����
    ,
     ONSHEETBALANCE -- �������
    ,
     NORMALINTEREST -- ������Ϣ
    ,
     ONDEBITINTEREST -- ����ǷϢ
    ,
     OFFDEBITINTEREST -- ����ǷϢ
    ,
     EXPENSERECEIVABLE -- Ӧ�շ���
    ,
     ASSETBALANCE -- �ʲ����
    ,
     ACCSUBJECT1 -- ��Ŀһ
    ,
     ACCSUBJECT2 -- ��Ŀ��
    ,
     ACCSUBJECT3 -- ��Ŀ��
    ,
     STARTDATE -- ��ʼ����
    ,
     DUEDATE -- ��������
    ,
     ORIGINALMATURITY -- ԭʼ����
    ,
     RESIDUALM -- ʣ������
    ,
     RISKCLASSIFY -- ���շ���
    ,
     EXPOSURESTATUS -- ���ձ�¶״̬
    ,
     OVERDUEDAYS -- ��������
    ,
     SPECIALPROVISION -- ר��׼����
    ,
     GENERALPROVISION -- һ��׼����
    ,
     ESPECIALPROVISION -- �ر�׼����
    ,
     WRITTENOFFAMOUNT -- �Ѻ������
    ,
     OFFEXPOSOURCE -- ���Ⱪ¶��Դ
    ,
     OFFBUSINESSTYPE -- ����ҵ������
    ,
     OFFBUSINESSSDVSSTD -- Ȩ�ط�����ҵ������ϸ��
    ,
     UNCONDCANCELFLAG -- �Ƿ����ʱ����������
    ,
     CCFLEVEL -- ����ת��ϵ������
    ,
     CCFAIRB -- �߼�������ת��ϵ��
    ,
     CLAIMSLEVEL -- ծȨ����
    ,
     BONDFLAG -- �Ƿ�Ϊծȯ
    ,
     BONDISSUEINTENT -- ծȯ����Ŀ��
    ,
     NSUREALPROPERTYFLAG -- �Ƿ�����ò�����
    ,
     REPASSETTERMTYPE -- ��ծ�ʲ���������
    ,
     DEPENDONFPOBFLAG -- �Ƿ�����������δ��ӯ��
    ,
     IRATING -- �ڲ�����
    ,
     PD -- ΥԼ����
    ,
     LGDLEVEL -- ΥԼ��ʧ�ʼ���
    ,
     LGDAIRB -- �߼���ΥԼ��ʧ��
    ,
     MAIRB -- �߼�����Ч����
    ,
     EADAIRB -- �߼���ΥԼ���ձ�¶
    ,
     DEFAULTFLAG -- ΥԼ��ʶ
    ,
     BEEL -- ��ΥԼ��¶Ԥ����ʧ����
    ,
     DEFAULTLGD -- ��ΥԼ��¶ΥԼ��ʧ��
    ,
     EQUITYEXPOFLAG -- ��Ȩ��¶��ʶ
    ,
     EQUITYINVESTTYPE -- ��ȨͶ�ʶ�������
    ,
     EQUITYINVESTCAUSE -- ��ȨͶ���γ�ԭ��
    ,
     SLFLAG -- רҵ�����ʶ
    ,
     SLTYPE -- רҵ��������
    ,
     PFPHASE -- ��Ŀ���ʽ׶�
    ,
     REGURATING -- �������
    ,
     CBRCMPRATINGFLAG -- ������϶������Ƿ��Ϊ����
    ,
     LARGEFLUCFLAG -- �Ƿ񲨶��Խϴ�
    ,
     LIQUEXPOFLAG -- �Ƿ���������з��ձ�¶
    ,
     PAYMENTDEALFLAG -- �Ƿ����Ը�ģʽ
    ,
     DELAYTRADINGDAYS -- �ӳٽ�������
    ,
     SECURITIESFLAG -- �м�֤ȯ��ʶ
    ,
     SECUISSUERID -- ֤ȯ������ID
    ,
     RATINGDURATIONTYPE -- ������������
    ,
     SECUISSUERATING -- ֤ȯ���еȼ�
    ,
     SECURESIDUALM -- ֤ȯʣ������
    ,
     SECUREVAFREQUENCY -- ֤ȯ�ع�Ƶ��
    ,
     CCPTRANFLAG -- �Ƿ����뽻�׶�����ؽ���
    ,
     CCPID -- ���뽻�׶���ID
    ,
     QUALCCPFLAG -- �Ƿ�ϸ����뽻�׶���
    ,
     BANKROLE -- ���н�ɫ
    ,
     CLEARINGMETHOD -- ���㷽ʽ
    ,
     BANKASSETFLAG -- �Ƿ������ύ�ʲ�
    ,
     MATCHCONDITIONS -- �����������
    ,
     SFTFLAG -- ֤ȯ���ʽ��ױ�ʶ
    ,
     MASTERNETAGREEFLAG -- ���������Э���ʶ
    ,
     MASTERNETAGREEID -- ���������Э��ID
    ,
     SFTTYPE -- ֤ȯ���ʽ�������
    ,
     SECUOWNERTRANSFLAG -- ֤ȯ����Ȩ�Ƿ�ת��
    ,
     OTCFLAG -- �����������߱�ʶ
    ,
     VALIDNETTINGFLAG -- ��Ч�������Э���ʶ
    ,
     VALIDNETAGREEMENTID -- ��Ч�������Э��ID
    ,
     OTCTYPE -- ����������������
    ,
     DEPOSITRISKPERIOD -- ��֤������ڼ�
    ,
     MTM -- ���óɱ�
    ,
     MTMCURRENCY -- ���óɱ�����
    ,
     BUYERORSELLER -- ������
    ,
     QUALROFLAG -- �ϸ�����ʲ���ʶ
    ,
     ROISSUERPERFORMFLAG -- �����ʲ��������Ƿ�����Լ
    ,
     BUYERINSOLVENCYFLAG -- ���ñ������Ƿ��Ʋ�
    ,
     NONPAYMENTFEES -- ��δ֧������
    ,
     RETAILEXPOFLAG -- ���۱�¶��ʶ
    ,
     RETAILCLAIMTYPE -- ����ծȨ����
    ,
     MORTGAGETYPE -- ס����Ѻ��������
    ,
     DEBTORNUMBER -- ����˸���
    ,
     EXPONUMBER -- ���ձ�¶����
    ,
     PDPOOLMODELID -- PD�ֳ�ģ��ID
    ,
     LGDPOOLMODELID -- LGD�ֳ�ģ��ID
    ,
     CCFPOOLMODELID -- CCF�ֳ�ģ��ID
    ,
     PDPOOLID -- ����PD��ID
    ,
     LGDPOOLID -- ����LGD��ID
    ,
     CCFPOOLID -- ����CCF��ID
    ,
     ABSUAFLAG -- �ʲ�֤ȯ�������ʲ���ʶ
    ,
     ABSPOOLID -- ֤ȯ���ʲ���ID
    ,
     ABSPROPORTION -- �ʲ�֤ȯ������
    ,
     GROUPID -- ������
    ,
     ORGSORTNO --�������������
    ,
     LTV --�����ֵ��
    ,
     AGING --����
    ,
     NEWDEFAULTDEBTFLAG --����ΥԼծ���ʶ
    ,
     DEFAULTDATE --ΥԼʱ��
     )
    SELECT DATADATE   AS DATADATE -- ��������
          ,
           DATANO     AS DATANO -- ������ˮ��
          ,
           EXPOSUREID AS EXPOSUREID -- ���ձ�¶ID
          ,
           DUEID      AS DUEID -- ծ��ID
          ,
           SSYSID     AS SSYSID -- ԴϵͳID
          ,
           CONTRACTID AS CONTRACTID -- ��ͬID
          ,
           CLIENTID   AS CLIENTID -- ��������ID
          ,
           SORGID     AS SORGID -- Դ����ID
          ,
           SORGNAME   AS SORGNAME -- Դ��������
          ,
           ORGID      AS ORGID -- ��������ID
          ,
           ORGNAME    AS ORGNAME -- ������������
          ,
           ACCORGID   AS ACCORGID -- �������ID
          ,
           ACCORGNAME AS ACCORGNAME -- �����������
          ,
           INDUSTRYID AS INDUSTRYID --������ҵ����
          ,
           NVL       (INDUSTRYNAME, 'δ֪'        �� AS INDUSTRYNAME --������ҵ����
                  , BUSINESSLINE AS BUSINESSLINE -- ����
                  , ASSETTYPE AS ASSETTYPE -- �ʲ�����
                  , ASSETSUBTYPE AS ASSETSUBTYPE -- �ʲ�С��
                  , BUSINESSTYPEID AS BUSINESSTYPEID -- ҵ��Ʒ�ִ���
                  , BUSINESSTYPENAME AS BUSINESSTYPENAME -- ҵ��Ʒ������
                  , CREDITRISKDATATYPE AS CREDITRISKDATATYPE -- ���÷�����������
                  , ASSETTYPEOFHAIRCUTS AS ASSETTYPEOFHAIRCUTS -- �ۿ�ϵ����Ӧ�ʲ����
                  , BUSINESSTYPESTD AS BUSINESSTYPESTD -- Ȩ�ط�ҵ������
                  , EXPOCLASSSTD AS EXPOCLASSSTD -- Ȩ�ط���¶����
                  , EXPOSUBCLASSSTD AS EXPOSUBCLASSSTD -- Ȩ�ط���¶С��
                  , EXPOCLASSIRB AS EXPOCLASSIRB -- ��������¶����
                  , EXPOSUBCLASSIRB AS EXPOSUBCLASSIRB -- ��������¶С��
                  , EXPOBELONG AS EXPOBELONG -- ��¶������ʶ
                  , BOOKTYPE AS BOOKTYPE -- �˻����
                  , REGUTRANTYPE AS REGUTRANTYPE -- ��ܽ�������
                  , REPOTRANFLAG AS REPOTRANFLAG -- �ع����ױ�ʶ
                  , REVAFREQUENCY AS REVAFREQUENCY -- �ع�Ƶ��
                  , CURRENCY AS CURRENCY -- ����
                  , NORMALPRINCIPAL AS NORMALPRINCIPAL -- �����������
                  , OVERDUEBALANCE AS OVERDUEBALANCE -- �������
                  , NONACCRUALBALANCE AS NONACCRUALBALANCE -- ��Ӧ�����
                  , ONSHEETBALANCE AS ONSHEETBALANCE -- �������
                  , NORMALINTEREST AS NORMALINTEREST -- ������Ϣ
                  , ONDEBITINTEREST AS ONDEBITINTEREST -- ����ǷϢ
                  , OFFDEBITINTEREST AS OFFDEBITINTEREST -- ����ǷϢ
                  , EXPENSERECEIVABLE AS EXPENSERECEIVABLE -- Ӧ�շ���
                  , ASSETBALANCE AS ASSETBALANCE -- �ʲ����
                  , ACCSUBJECT1 AS ACCSUBJECT1 -- ��Ŀһ
                  , ACCSUBJECT2 AS ACCSUBJECT2 -- ��Ŀ��
                  , ACCSUBJECT3 AS ACCSUBJECT3 -- ��Ŀ��
                  , TO_CHAR(TO_DATE(STARTDATE, 'YYYY-MM-DD'       ), 'YYYY-MM-DD'       ) AS STARTDATE -- ��ʼ����
                  , TO_CHAR(TO_DATE(DUEDATE, 'YYYY-MM-DD'       ), 'YYYY-MM-DD'       ) AS DUEDATE -- ��������
                  , ORIGINALMATURITY AS ORIGINALMATURITY -- ԭʼ����
                  , RESIDUALM AS RESIDUALM -- ʣ������
                  , RISKCLASSIFY AS RISKCLASSIFY -- ���շ���
                  , EXPOSURESTATUS AS EXPOSURESTATUS -- ���ձ�¶״̬
                  , OVERDUEDAYS AS OVERDUEDAYS -- ��������
                  , SPECIALPROVISION AS SPECIALPROVISION -- ר��׼����
                  , GENERALPROVISION AS GENERALPROVISION -- һ��׼����
                  , ESPECIALPROVISION AS ESPECIALPROVISION -- �ر�׼����
                  , WRITTENOFFAMOUNT AS WRITTENOFFAMOUNT -- �Ѻ������
                  , OFFEXPOSOURCE AS OFFEXPOSOURCE -- ���Ⱪ¶��Դ
                  , OFFBUSINESSTYPE AS OFFBUSINESSTYPE -- ����ҵ������
                  , OFFBUSINESSSDVSSTD AS OFFBUSINESSSDVSSTD -- Ȩ�ط�����ҵ������ϸ��
                  , UNCONDCANCELFLAG AS UNCONDCANCELFLAG -- �Ƿ����ʱ����������
                  , CCFLEVEL AS CCFLEVEL -- ����ת��ϵ������
                  , CCFAIRB AS CCFAIRB -- �߼�������ת��ϵ��
                  , CLAIMSLEVEL AS CLAIMSLEVEL -- ծȨ����
                  , BONDFLAG AS BONDFLAG -- �Ƿ�Ϊծȯ
                  , BONDISSUEINTENT AS BONDISSUEINTENT -- ծȯ����Ŀ��
                  , NSUREALPROPERTYFLAG AS NSUREALPROPERTYFLAG -- �Ƿ�����ò�����
                  , REPASSETTERMTYPE AS REPASSETTERMTYPE -- ��ծ�ʲ���������
                  , DEPENDONFPOBFLAG AS DEPENDONFPOBFLAG -- �Ƿ�����������δ��ӯ��
                  , IRATING AS IRATING -- �ڲ�����
                  , PD AS PD -- ΥԼ����
                  , LGDLEVEL AS LGDLEVEL -- ΥԼ��ʧ�ʼ���
                  , LGDAIRB AS LGDAIRB -- �߼���ΥԼ��ʧ��
                  , MAIRB AS MAIRB -- �߼�����Ч����
                  , EADAIRB AS EADAIRB -- �߼���ΥԼ���ձ�¶
                  , DEFAULTFLAG AS DEFAULTFLAG -- ΥԼ��ʶ
                  , BEEL AS BEEL -- ��ΥԼ��¶Ԥ����ʧ����
                  , DEFAULTLGD AS DEFAULTLGD -- ��ΥԼ��¶ΥԼ��ʧ��
                  , EQUITYEXPOFLAG AS EQUITYEXPOFLAG -- ��Ȩ��¶��ʶ
                  , EQUITYINVESTTYPE AS EQUITYINVESTTYPE -- ��ȨͶ�ʶ�������
                  , EQUITYINVESTCAUSE AS EQUITYINVESTCAUSE -- ��ȨͶ���γ�ԭ��
                  , SLFLAG AS SLFLAG -- רҵ�����ʶ
                  , SLTYPE AS SLTYPE -- רҵ��������
                  , PFPHASE AS PFPHASE -- ��Ŀ���ʽ׶�
                  , REGURATING AS REGURATING -- �������
                  , CBRCMPRATINGFLAG AS CBRCMPRATINGFLAG -- ������϶������Ƿ��Ϊ����
                  , LARGEFLUCFLAG AS LARGEFLUCFLAG -- �Ƿ񲨶��Խϴ�
                  , LIQUEXPOFLAG AS LIQUEXPOFLAG -- �Ƿ���������з��ձ�¶
                  , PAYMENTDEALFLAG AS PAYMENTDEALFLAG -- �Ƿ����Ը�ģʽ
                  , DELAYTRADINGDAYS AS DELAYTRADINGDAYS -- �ӳٽ�������
                  , SECURITIESFLAG AS SECURITIESFLAG -- �м�֤ȯ��ʶ
                  , SECUISSUERID AS SECUISSUERID -- ֤ȯ������ID
                  , RATINGDURATIONTYPE AS RATINGDURATIONTYPE -- ������������
                  , SECUISSUERATING AS SECUISSUERATING -- ֤ȯ���еȼ�
                  , SECURESIDUALM AS SECURESIDUALM -- ֤ȯʣ������
                  , SECUREVAFREQUENCY AS SECUREVAFREQUENCY -- ֤ȯ�ع�Ƶ��
                  , CCPTRANFLAG AS CCPTRANFLAG -- �Ƿ����뽻�׶�����ؽ���
                  , CCPID AS CCPID -- ���뽻�׶���ID
                  , QUALCCPFLAG AS QUALCCPFLAG -- �Ƿ�ϸ����뽻�׶���
                  , BANKROLE AS BANKROLE -- ���н�ɫ
                  , CLEARINGMETHOD AS CLEARINGMETHOD -- ���㷽ʽ
                  , BANKASSETFLAG AS BANKASSETFLAG -- �Ƿ������ύ�ʲ�
                  , MATCHCONDITIONS AS MATCHCONDITIONS -- �����������
                  , SFTFLAG AS SFTFLAG -- ֤ȯ���ʽ��ױ�ʶ
                  , MASTERNETAGREEFLAG AS MASTERNETAGREEFLAG -- ���������Э���ʶ
                  , MASTERNETAGREEID AS MASTERNETAGREEID -- ���������Э��ID
                  , SFTTYPE AS SFTTYPE -- ֤ȯ���ʽ�������
                  , SECUOWNERTRANSFLAG AS SECUOWNERTRANSFLAG -- ֤ȯ����Ȩ�Ƿ�ת��
                  , OTCFLAG AS OTCFLAG -- �����������߱�ʶ
                  , VALIDNETTINGFLAG AS VALIDNETTINGFLAG -- ��Ч�������Э���ʶ
                  , VALIDNETAGREEMENTID AS VALIDNETAGREEMENTID -- ��Ч�������Э��ID
                  , OTCTYPE AS OTCTYPE -- ����������������
                  , DEPOSITRISKPERIOD AS DEPOSITRISKPERIOD -- ��֤������ڼ�
                  , MTM AS MTM -- ���óɱ�
                  , MTMCURRENCY AS MTMCURRENCY -- ���óɱ�����
                  , BUYERORSELLER AS BUYERORSELLER -- ������
                  , QUALROFLAG AS QUALROFLAG -- �ϸ�����ʲ���ʶ
                  , ROISSUERPERFORMFLAG AS ROISSUERPERFORMFLAG -- �����ʲ��������Ƿ�����Լ
                  , BUYERINSOLVENCYFLAG AS BUYERINSOLVENCYFLAG -- ���ñ������Ƿ��Ʋ�
                  , NONPAYMENTFEES AS NONPAYMENTFEES -- ��δ֧������
                  , RETAILEXPOFLAG AS RETAILEXPOFLAG -- ���۱�¶��ʶ
                  , RETAILCLAIMTYPE AS RETAILCLAIMTYPE -- ����ծȨ����
                  , MORTGAGETYPE AS MORTGAGETYPE -- ס����Ѻ��������
                  , DEBTORNUMBER AS DEBTORNUMBER -- ����˸���
                  , EXPONUMBER AS EXPONUMBER -- ���ձ�¶����
                  , PDPOOLMODELID AS PDPOOLMODELID -- PD�ֳ�ģ��ID
                  , LGDPOOLMODELID AS LGDPOOLMODELID -- LGD�ֳ�ģ��ID
                  , CCFPOOLMODELID AS CCFPOOLMODELID -- CCF�ֳ�ģ��ID
                  , PDPOOLID AS PDPOOLID -- ����PD��ID
                  , LGDPOOLID AS LGDPOOLID -- ����LGD��ID
                  , CCFPOOLID AS CCFPOOLID -- ����CCF��ID
                  , ABSUAFLAG AS ABSUAFLAG -- �ʲ�֤ȯ�������ʲ���ʶ
                  , ABSPOOLID AS ABSPOOLID -- ֤ȯ���ʲ���ID
                  , ABSPROPORTION AS ABSPROPORTION -- �ʲ�֤ȯ������
                  , GROUPID AS GROUPID -- ������
                  , ORGSORTNO AS ORGSORTNO --�������������
                  , LTV AS LTV --�����ֵ��
                  , AGING AS AGING --����
                  , NEWDEFAULTDEBTFLAG AS NEWDEFAULTDEBTFLAG --����ΥԼծ���ʶ
                  , DEFAULTDATE AS DEFAULTDATE --ΥԼʱ��
                   FROM RWA_DEV.RWA_GQ_EXPOSURE WHERE DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD'       );
  COMMIT;

  /*�����ծ�ʲ��ķ��ձ�¶��Ϣ*/
  INSERT /*+ APPEND */
  INTO RWA_DEV.RWA_EI_EXPOSURE
    (DATADATE -- ��������
    ,
     DATANO -- ������ˮ��
    ,
     EXPOSUREID -- ���ձ�¶ID
    ,
     DUEID -- ծ��ID
    ,
     SSYSID -- ԴϵͳID
    ,
     CONTRACTID -- ��ͬID
    ,
     CLIENTID -- ��������ID
    ,
     SORGID -- Դ����ID
    ,
     SORGNAME -- Դ��������
    ,
     ORGID -- ��������ID
    ,
     ORGNAME -- ������������
    ,
     ACCORGID -- �������ID
    ,
     ACCORGNAME -- �����������
    ,
     INDUSTRYID -- ������ҵ����
    ,
     INDUSTRYNAME -- ������ҵ����
    ,
     BUSINESSLINE -- ����
    ,
     ASSETTYPE -- �ʲ�����
    ,
     ASSETSUBTYPE -- �ʲ�С��
    ,
     BUSINESSTYPEID -- ҵ��Ʒ�ִ���
    ,
     BUSINESSTYPENAME -- ҵ��Ʒ������
    ,
     CREDITRISKDATATYPE -- ���÷�����������
    ,
     ASSETTYPEOFHAIRCUTS -- �ۿ�ϵ����Ӧ�ʲ����
    ,
     BUSINESSTYPESTD -- Ȩ�ط�ҵ������
    ,
     EXPOCLASSSTD -- Ȩ�ط���¶����
    ,
     EXPOSUBCLASSSTD -- Ȩ�ط���¶С��
    ,
     EXPOCLASSIRB -- ��������¶����
    ,
     EXPOSUBCLASSIRB -- ��������¶С��
    ,
     EXPOBELONG -- ��¶������ʶ
    ,
     BOOKTYPE -- �˻����
    ,
     REGUTRANTYPE -- ��ܽ�������
    ,
     REPOTRANFLAG -- �ع����ױ�ʶ
    ,
     REVAFREQUENCY -- �ع�Ƶ��
    ,
     CURRENCY -- ����
    ,
     NORMALPRINCIPAL -- �����������
    ,
     OVERDUEBALANCE -- �������
    ,
     NONACCRUALBALANCE -- ��Ӧ�����
    ,
     ONSHEETBALANCE -- �������
    ,
     NORMALINTEREST -- ������Ϣ
    ,
     ONDEBITINTEREST -- ����ǷϢ
    ,
     OFFDEBITINTEREST -- ����ǷϢ
    ,
     EXPENSERECEIVABLE -- Ӧ�շ���
    ,
     ASSETBALANCE -- �ʲ����
    ,
     ACCSUBJECT1 -- ��Ŀһ
    ,
     ACCSUBJECT2 -- ��Ŀ��
    ,
     ACCSUBJECT3 -- ��Ŀ��
    ,
     STARTDATE -- ��ʼ����
    ,
     DUEDATE -- ��������
    ,
     ORIGINALMATURITY -- ԭʼ����
    ,
     RESIDUALM -- ʣ������
    ,
     RISKCLASSIFY -- ���շ���
    ,
     EXPOSURESTATUS -- ���ձ�¶״̬
    ,
     OVERDUEDAYS -- ��������
    ,
     SPECIALPROVISION -- ר��׼����
    ,
     GENERALPROVISION -- һ��׼����
    ,
     ESPECIALPROVISION -- �ر�׼����
    ,
     WRITTENOFFAMOUNT -- �Ѻ������
    ,
     OFFEXPOSOURCE -- ���Ⱪ¶��Դ
    ,
     OFFBUSINESSTYPE -- ����ҵ������
    ,
     OFFBUSINESSSDVSSTD -- Ȩ�ط�����ҵ������ϸ��
    ,
     UNCONDCANCELFLAG -- �Ƿ����ʱ����������
    ,
     CCFLEVEL -- ����ת��ϵ������
    ,
     CCFAIRB -- �߼�������ת��ϵ��
    ,
     CLAIMSLEVEL -- ծȨ����
    ,
     BONDFLAG -- �Ƿ�Ϊծȯ
    ,
     BONDISSUEINTENT -- ծȯ����Ŀ��
    ,
     NSUREALPROPERTYFLAG -- �Ƿ�����ò�����
    ,
     REPASSETTERMTYPE -- ��ծ�ʲ���������
    ,
     DEPENDONFPOBFLAG -- �Ƿ�����������δ��ӯ��
    ,
     IRATING -- �ڲ�����
    ,
     PD -- ΥԼ����
    ,
     LGDLEVEL -- ΥԼ��ʧ�ʼ���
    ,
     LGDAIRB -- �߼���ΥԼ��ʧ��
    ,
     MAIRB -- �߼�����Ч����
    ,
     EADAIRB -- �߼���ΥԼ���ձ�¶
    ,
     DEFAULTFLAG -- ΥԼ��ʶ
    ,
     BEEL -- ��ΥԼ��¶Ԥ����ʧ����
    ,
     DEFAULTLGD -- ��ΥԼ��¶ΥԼ��ʧ��
    ,
     EQUITYEXPOFLAG -- ��Ȩ��¶��ʶ
    ,
     EQUITYINVESTTYPE -- ��ȨͶ�ʶ�������
    ,
     EQUITYINVESTCAUSE -- ��ȨͶ���γ�ԭ��
    ,
     SLFLAG -- רҵ�����ʶ
    ,
     SLTYPE -- רҵ��������
    ,
     PFPHASE -- ��Ŀ���ʽ׶�
    ,
     REGURATING -- �������
    ,
     CBRCMPRATINGFLAG -- ������϶������Ƿ��Ϊ����
    ,
     LARGEFLUCFLAG -- �Ƿ񲨶��Խϴ�
    ,
     LIQUEXPOFLAG -- �Ƿ���������з��ձ�¶
    ,
     PAYMENTDEALFLAG -- �Ƿ����Ը�ģʽ
    ,
     DELAYTRADINGDAYS -- �ӳٽ�������
    ,
     SECURITIESFLAG -- �м�֤ȯ��ʶ
    ,
     SECUISSUERID -- ֤ȯ������ID
    ,
     RATINGDURATIONTYPE -- ������������
    ,
     SECUISSUERATING -- ֤ȯ���еȼ�
    ,
     SECURESIDUALM -- ֤ȯʣ������
    ,
     SECUREVAFREQUENCY -- ֤ȯ�ع�Ƶ��
    ,
     CCPTRANFLAG -- �Ƿ����뽻�׶�����ؽ���
    ,
     CCPID -- ���뽻�׶���ID
    ,
     QUALCCPFLAG -- �Ƿ�ϸ����뽻�׶���
    ,
     BANKROLE -- ���н�ɫ
    ,
     CLEARINGMETHOD -- ���㷽ʽ
    ,
     BANKASSETFLAG -- �Ƿ������ύ�ʲ�
    ,
     MATCHCONDITIONS -- �����������
    ,
     SFTFLAG -- ֤ȯ���ʽ��ױ�ʶ
    ,
     MASTERNETAGREEFLAG -- ���������Э���ʶ
    ,
     MASTERNETAGREEID -- ���������Э��ID
    ,
     SFTTYPE -- ֤ȯ���ʽ�������
    ,
     SECUOWNERTRANSFLAG -- ֤ȯ����Ȩ�Ƿ�ת��
    ,
     OTCFLAG -- �����������߱�ʶ
    ,
     VALIDNETTINGFLAG -- ��Ч�������Э���ʶ
    ,
     VALIDNETAGREEMENTID -- ��Ч�������Э��ID
    ,
     OTCTYPE -- ����������������
    ,
     DEPOSITRISKPERIOD -- ��֤������ڼ�
    ,
     MTM -- ���óɱ�
    ,
     MTMCURRENCY -- ���óɱ�����
    ,
     BUYERORSELLER -- ������
    ,
     QUALROFLAG -- �ϸ�����ʲ���ʶ
    ,
     ROISSUERPERFORMFLAG -- �����ʲ��������Ƿ�����Լ
    ,
     BUYERINSOLVENCYFLAG -- ���ñ������Ƿ��Ʋ�
    ,
     NONPAYMENTFEES -- ��δ֧������
    ,
     RETAILEXPOFLAG -- ���۱�¶��ʶ
    ,
     RETAILCLAIMTYPE -- ����ծȨ����
    ,
     MORTGAGETYPE -- ס����Ѻ��������
    ,
     DEBTORNUMBER -- ����˸���
    ,
     EXPONUMBER -- ���ձ�¶����
    ,
     PDPOOLMODELID -- PD�ֳ�ģ��ID
    ,
     LGDPOOLMODELID -- LGD�ֳ�ģ��ID
    ,
     CCFPOOLMODELID -- CCF�ֳ�ģ��ID
    ,
     PDPOOLID -- ����PD��ID
    ,
     LGDPOOLID -- ����LGD��ID
    ,
     CCFPOOLID -- ����CCF��ID
    ,
     ABSUAFLAG -- �ʲ�֤ȯ�������ʲ���ʶ
    ,
     ABSPOOLID -- ֤ȯ���ʲ���ID
    ,
     ABSPROPORTION -- �ʲ�֤ȯ������
    ,
     GROUPID -- ������
    ,
     ORGSORTNO --�������������
    ,
     LTV --�����ֵ��
    ,
     AGING --����
    ,
     NEWDEFAULTDEBTFLAG --����ΥԼծ���ʶ
    ,
     DEFAULTDATE --ΥԼʱ��
     )
    SELECT DATADATE   AS DATADATE -- ��������
          ,
           DATANO     AS DATANO -- ������ˮ��
          ,
           EXPOSUREID AS EXPOSUREID -- ���ձ�¶ID
          ,
           DUEID      AS DUEID -- ծ��ID
          ,
           SSYSID     AS SSYSID -- ԴϵͳID
          ,
           CONTRACTID AS CONTRACTID -- ��ͬID
          ,
           CLIENTID   AS CLIENTID -- ��������ID
          ,
           SORGID     AS SORGID -- Դ����ID
          ,
           SORGNAME   AS SORGNAME -- Դ��������
          ,
           ORGID      AS ORGID -- ��������ID
          ,
           ORGNAME    AS ORGNAME -- ������������
          ,
           ACCORGID   AS ACCORGID -- �������ID
          ,
           ACCORGNAME AS ACCORGNAME -- �����������
          ,
           INDUSTRYID AS INDUSTRYID --������ҵ����
          ,
           NVL       (INDUSTRYNAME, 'δ֪'        �� AS INDUSTRYNAME --������ҵ����
                  , BUSINESSLINE AS BUSINESSLINE -- ����
                  , ASSETTYPE AS ASSETTYPE -- �ʲ�����
                  , ASSETSUBTYPE AS ASSETSUBTYPE -- �ʲ�С��
                  , BUSINESSTYPEID AS BUSINESSTYPEID -- ҵ��Ʒ�ִ���
                  , BUSINESSTYPENAME AS BUSINESSTYPENAME -- ҵ��Ʒ������
                  , CREDITRISKDATATYPE AS CREDITRISKDATATYPE -- ���÷�����������
                  , ASSETTYPEOFHAIRCUTS AS ASSETTYPEOFHAIRCUTS -- �ۿ�ϵ����Ӧ�ʲ����
                  , BUSINESSTYPESTD AS BUSINESSTYPESTD -- Ȩ�ط�ҵ������
                  , EXPOCLASSSTD AS EXPOCLASSSTD -- Ȩ�ط���¶����
                  , EXPOSUBCLASSSTD AS EXPOSUBCLASSSTD -- Ȩ�ط���¶С��
                  , EXPOCLASSIRB AS EXPOCLASSIRB -- ��������¶����
                  , EXPOSUBCLASSIRB AS EXPOSUBCLASSIRB -- ��������¶С��
                  , EXPOBELONG AS EXPOBELONG -- ��¶������ʶ
                  , BOOKTYPE AS BOOKTYPE -- �˻����
                  , REGUTRANTYPE AS REGUTRANTYPE -- ��ܽ�������
                  , REPOTRANFLAG AS REPOTRANFLAG -- �ع����ױ�ʶ
                  , REVAFREQUENCY AS REVAFREQUENCY -- �ع�Ƶ��
                  , CURRENCY AS CURRENCY -- ����
                  , NORMALPRINCIPAL AS NORMALPRINCIPAL -- �����������
                  , OVERDUEBALANCE AS OVERDUEBALANCE -- �������
                  , NONACCRUALBALANCE AS NONACCRUALBALANCE -- ��Ӧ�����
                  , ONSHEETBALANCE AS ONSHEETBALANCE -- �������
                  , NORMALINTEREST AS NORMALINTEREST -- ������Ϣ
                  , ONDEBITINTEREST AS ONDEBITINTEREST -- ����ǷϢ
                  , OFFDEBITINTEREST AS OFFDEBITINTEREST -- ����ǷϢ
                  , EXPENSERECEIVABLE AS EXPENSERECEIVABLE -- Ӧ�շ���
                  , ASSETBALANCE AS ASSETBALANCE -- �ʲ����
                  , ACCSUBJECT1 AS ACCSUBJECT1 -- ��Ŀһ
                  , ACCSUBJECT2 AS ACCSUBJECT2 -- ��Ŀ��
                  , ACCSUBJECT3 AS ACCSUBJECT3 -- ��Ŀ��
                  , TO_CHAR(TO_DATE(STARTDATE, 'YYYY-MM-DD'       ), 'YYYY-MM-DD'       ) AS STARTDATE -- ��ʼ����
                  , TO_CHAR(TO_DATE(DUEDATE, 'YYYY-MM-DD'       ), 'YYYY-MM-DD'       ) AS DUEDATE -- ��������
                  , ORIGINALMATURITY AS ORIGINALMATURITY -- ԭʼ����
                  , RESIDUALM AS RESIDUALM -- ʣ������
                  , RISKCLASSIFY AS RISKCLASSIFY -- ���շ���
                  , EXPOSURESTATUS AS EXPOSURESTATUS -- ���ձ�¶״̬
                  , OVERDUEDAYS AS OVERDUEDAYS -- ��������
                  , SPECIALPROVISION AS SPECIALPROVISION -- ר��׼����
                  , GENERALPROVISION AS GENERALPROVISION -- һ��׼����
                  , ESPECIALPROVISION AS ESPECIALPROVISION -- �ر�׼����
                  , WRITTENOFFAMOUNT AS WRITTENOFFAMOUNT -- �Ѻ������
                  , OFFEXPOSOURCE AS OFFEXPOSOURCE -- ���Ⱪ¶��Դ
                  , OFFBUSINESSTYPE AS OFFBUSINESSTYPE -- ����ҵ������
                  , OFFBUSINESSSDVSSTD AS OFFBUSINESSSDVSSTD -- Ȩ�ط�����ҵ������ϸ��
                  , UNCONDCANCELFLAG AS UNCONDCANCELFLAG -- �Ƿ����ʱ����������
                  , CCFLEVEL AS CCFLEVEL -- ����ת��ϵ������
                  , CCFAIRB AS CCFAIRB -- �߼�������ת��ϵ��
                  , CLAIMSLEVEL AS CLAIMSLEVEL -- ծȨ����
                  , BONDFLAG AS BONDFLAG -- �Ƿ�Ϊծȯ
                  , BONDISSUEINTENT AS BONDISSUEINTENT -- ծȯ����Ŀ��
                  , NSUREALPROPERTYFLAG AS NSUREALPROPERTYFLAG -- �Ƿ�����ò�����
                  , REPASSETTERMTYPE AS REPASSETTERMTYPE -- ��ծ�ʲ���������
                  , DEPENDONFPOBFLAG AS DEPENDONFPOBFLAG -- �Ƿ�����������δ��ӯ��
                  , IRATING AS IRATING -- �ڲ�����
                  , PD AS PD -- ΥԼ����
                  , LGDLEVEL AS LGDLEVEL -- ΥԼ��ʧ�ʼ���
                  , LGDAIRB AS LGDAIRB -- �߼���ΥԼ��ʧ��
                  , MAIRB AS MAIRB -- �߼�����Ч����
                  , EADAIRB AS EADAIRB -- �߼���ΥԼ���ձ�¶
                  , DEFAULTFLAG AS DEFAULTFLAG -- ΥԼ��ʶ
                  , BEEL AS BEEL -- ��ΥԼ��¶Ԥ����ʧ����
                  , DEFAULTLGD AS DEFAULTLGD -- ��ΥԼ��¶ΥԼ��ʧ��
                  , EQUITYEXPOFLAG AS EQUITYEXPOFLAG -- ��Ȩ��¶��ʶ
                  , EQUITYINVESTTYPE AS EQUITYINVESTTYPE -- ��ȨͶ�ʶ�������
                  , EQUITYINVESTCAUSE AS EQUITYINVESTCAUSE -- ��ȨͶ���γ�ԭ��
                  , SLFLAG AS SLFLAG -- רҵ�����ʶ
                  , SLTYPE AS SLTYPE -- רҵ��������
                  , PFPHASE AS PFPHASE -- ��Ŀ���ʽ׶�
                  , REGURATING AS REGURATING -- �������
                  , CBRCMPRATINGFLAG AS CBRCMPRATINGFLAG -- ������϶������Ƿ��Ϊ����
                  , LARGEFLUCFLAG AS LARGEFLUCFLAG -- �Ƿ񲨶��Խϴ�
                  , LIQUEXPOFLAG AS LIQUEXPOFLAG -- �Ƿ���������з��ձ�¶
                  , PAYMENTDEALFLAG AS PAYMENTDEALFLAG -- �Ƿ����Ը�ģʽ
                  , DELAYTRADINGDAYS AS DELAYTRADINGDAYS -- �ӳٽ�������
                  , SECURITIESFLAG AS SECURITIESFLAG -- �м�֤ȯ��ʶ
                  , SECUISSUERID AS SECUISSUERID -- ֤ȯ������ID
                  , RATINGDURATIONTYPE AS RATINGDURATIONTYPE -- ������������
                  , SECUISSUERATING AS SECUISSUERATING -- ֤ȯ���еȼ�
                  , SECURESIDUALM AS SECURESIDUALM -- ֤ȯʣ������
                  , SECUREVAFREQUENCY AS SECUREVAFREQUENCY -- ֤ȯ�ع�Ƶ��
                  , CCPTRANFLAG AS CCPTRANFLAG -- �Ƿ����뽻�׶�����ؽ���
                  , CCPID AS CCPID -- ���뽻�׶���ID
                  , QUALCCPFLAG AS QUALCCPFLAG -- �Ƿ�ϸ����뽻�׶���
                  , BANKROLE AS BANKROLE -- ���н�ɫ
                  , CLEARINGMETHOD AS CLEARINGMETHOD -- ���㷽ʽ
                  , BANKASSETFLAG AS BANKASSETFLAG -- �Ƿ������ύ�ʲ�
                  , MATCHCONDITIONS AS MATCHCONDITIONS -- �����������
                  , SFTFLAG AS SFTFLAG -- ֤ȯ���ʽ��ױ�ʶ
                  , MASTERNETAGREEFLAG AS MASTERNETAGREEFLAG -- ���������Э���ʶ
                  , MASTERNETAGREEID AS MASTERNETAGREEID -- ���������Э��ID
                  , SFTTYPE AS SFTTYPE -- ֤ȯ���ʽ�������
                  , SECUOWNERTRANSFLAG AS SECUOWNERTRANSFLAG -- ֤ȯ����Ȩ�Ƿ�ת��
                  , OTCFLAG AS OTCFLAG -- �����������߱�ʶ
                  , VALIDNETTINGFLAG AS VALIDNETTINGFLAG -- ��Ч�������Э���ʶ
                  , VALIDNETAGREEMENTID AS VALIDNETAGREEMENTID -- ��Ч�������Э��ID
                  , OTCTYPE AS OTCTYPE -- ����������������
                  , DEPOSITRISKPERIOD AS DEPOSITRISKPERIOD -- ��֤������ڼ�
                  , MTM AS MTM -- ���óɱ�
                  , MTMCURRENCY AS MTMCURRENCY -- ���óɱ�����
                  , BUYERORSELLER AS BUYERORSELLER -- ������
                  , QUALROFLAG AS QUALROFLAG -- �ϸ�����ʲ���ʶ
                  , ROISSUERPERFORMFLAG AS ROISSUERPERFORMFLAG -- �����ʲ��������Ƿ�����Լ
                  , BUYERINSOLVENCYFLAG AS BUYERINSOLVENCYFLAG -- ���ñ������Ƿ��Ʋ�
                  , NONPAYMENTFEES AS NONPAYMENTFEES -- ��δ֧������
                  , RETAILEXPOFLAG AS RETAILEXPOFLAG -- ���۱�¶��ʶ
                  , RETAILCLAIMTYPE AS RETAILCLAIMTYPE -- ����ծȨ����
                  , MORTGAGETYPE AS MORTGAGETYPE -- ס����Ѻ��������
                  , DEBTORNUMBER AS DEBTORNUMBER -- ����˸���
                  , EXPONUMBER AS EXPONUMBER -- ���ձ�¶����
                  , PDPOOLMODELID AS PDPOOLMODELID -- PD�ֳ�ģ��ID
                  , LGDPOOLMODELID AS LGDPOOLMODELID -- LGD�ֳ�ģ��ID
                  , CCFPOOLMODELID AS CCFPOOLMODELID -- CCF�ֳ�ģ��ID
                  , PDPOOLID AS PDPOOLID -- ����PD��ID
                  , LGDPOOLID AS LGDPOOLID -- ����LGD��ID
                  , CCFPOOLID AS CCFPOOLID -- ����CCF��ID
                  , ABSUAFLAG AS ABSUAFLAG -- �ʲ�֤ȯ�������ʲ���ʶ
                  , ABSPOOLID AS ABSPOOLID -- ֤ȯ���ʲ���ID
                  , ABSPROPORTION AS ABSPROPORTION -- �ʲ�֤ȯ������
                  , GROUPID AS GROUPID -- ������
                  , ORGSORTNO AS ORGSORTNO --�������������
                  , LTV AS LTV --�����ֵ��
                  , AGING AS AGING --����
                  , NEWDEFAULTDEBTFLAG AS NEWDEFAULTDEBTFLAG --����ΥԼծ���ʶ
                  , DEFAULTDATE AS DEFAULTDATE --ΥԼʱ��
                   FROM RWA_DEV.RWA_DZ_EXPOSURE WHERE DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD'       );
  COMMIT;

  /*����ֱ�����еķ��ձ�¶��Ϣ*/
  INSERT /*+ APPEND */
  INTO RWA_DEV.RWA_EI_EXPOSURE
    (DATADATE -- ��������
    ,
     DATANO -- ������ˮ��
    ,
     EXPOSUREID -- ���ձ�¶ID
    ,
     DUEID -- ծ��ID
    ,
     SSYSID -- ԴϵͳID
    ,
     CONTRACTID -- ��ͬID
    ,
     CLIENTID -- ��������ID
    ,
     SORGID -- Դ����ID
    ,
     SORGNAME -- Դ��������
    ,
     ORGID -- ��������ID
    ,
     ORGNAME -- ������������
    ,
     ACCORGID -- �������ID
    ,
     ACCORGNAME -- �����������
    ,
     INDUSTRYID -- ������ҵ����
    ,
     INDUSTRYNAME -- ������ҵ����
    ,
     BUSINESSLINE -- ����
    ,
     ASSETTYPE -- �ʲ�����
    ,
     ASSETSUBTYPE -- �ʲ�С��
    ,
     BUSINESSTYPEID -- ҵ��Ʒ�ִ���
    ,
     BUSINESSTYPENAME -- ҵ��Ʒ������
    ,
     CREDITRISKDATATYPE -- ���÷�����������
    ,
     ASSETTYPEOFHAIRCUTS -- �ۿ�ϵ����Ӧ�ʲ����
    ,
     BUSINESSTYPESTD -- Ȩ�ط�ҵ������
    ,
     EXPOCLASSSTD -- Ȩ�ط���¶����
    ,
     EXPOSUBCLASSSTD -- Ȩ�ط���¶С��
    ,
     EXPOCLASSIRB -- ��������¶����
    ,
     EXPOSUBCLASSIRB -- ��������¶С��
    ,
     EXPOBELONG -- ��¶������ʶ
    ,
     BOOKTYPE -- �˻����
    ,
     REGUTRANTYPE -- ��ܽ�������
    ,
     REPOTRANFLAG -- �ع����ױ�ʶ
    ,
     REVAFREQUENCY -- �ع�Ƶ��
    ,
     CURRENCY -- ����
    ,
     NORMALPRINCIPAL -- �����������
    ,
     OVERDUEBALANCE -- �������
    ,
     NONACCRUALBALANCE -- ��Ӧ�����
    ,
     ONSHEETBALANCE -- �������
    ,
     NORMALINTEREST -- ������Ϣ
    ,
     ONDEBITINTEREST -- ����ǷϢ
    ,
     OFFDEBITINTEREST -- ����ǷϢ
    ,
     EXPENSERECEIVABLE -- Ӧ�շ���
    ,
     ASSETBALANCE -- �ʲ����
    ,
     ACCSUBJECT1 -- ��Ŀһ
    ,
     ACCSUBJECT2 -- ��Ŀ��
    ,
     ACCSUBJECT3 -- ��Ŀ��
    ,
     STARTDATE -- ��ʼ����
    ,
     DUEDATE -- ��������
    ,
     ORIGINALMATURITY -- ԭʼ����
    ,
     RESIDUALM -- ʣ������
    ,
     RISKCLASSIFY -- ���շ���
    ,
     EXPOSURESTATUS -- ���ձ�¶״̬
    ,
     OVERDUEDAYS -- ��������
    ,
     SPECIALPROVISION -- ר��׼����
    ,
     GENERALPROVISION -- һ��׼����
    ,
     ESPECIALPROVISION -- �ر�׼����
    ,
     WRITTENOFFAMOUNT -- �Ѻ������
    ,
     OFFEXPOSOURCE -- ���Ⱪ¶��Դ
    ,
     OFFBUSINESSTYPE -- ����ҵ������
    ,
     OFFBUSINESSSDVSSTD -- Ȩ�ط�����ҵ������ϸ��
    ,
     UNCONDCANCELFLAG -- �Ƿ����ʱ����������
    ,
     CCFLEVEL -- ����ת��ϵ������
    ,
     CCFAIRB -- �߼�������ת��ϵ��
    ,
     CLAIMSLEVEL -- ծȨ����
    ,
     BONDFLAG -- �Ƿ�Ϊծȯ
    ,
     BONDISSUEINTENT -- ծȯ����Ŀ��
    ,
     NSUREALPROPERTYFLAG -- �Ƿ�����ò�����
    ,
     REPASSETTERMTYPE -- ��ծ�ʲ���������
    ,
     DEPENDONFPOBFLAG -- �Ƿ�����������δ��ӯ��
    ,
     IRATING -- �ڲ�����
    ,
     PD -- ΥԼ����
    ,
     LGDLEVEL -- ΥԼ��ʧ�ʼ���
    ,
     LGDAIRB -- �߼���ΥԼ��ʧ��
    ,
     MAIRB -- �߼�����Ч����
    ,
     EADAIRB -- �߼���ΥԼ���ձ�¶
    ,
     DEFAULTFLAG -- ΥԼ��ʶ
    ,
     BEEL -- ��ΥԼ��¶Ԥ����ʧ����
    ,
     DEFAULTLGD -- ��ΥԼ��¶ΥԼ��ʧ��
    ,
     EQUITYEXPOFLAG -- ��Ȩ��¶��ʶ
    ,
     EQUITYINVESTTYPE -- ��ȨͶ�ʶ�������
    ,
     EQUITYINVESTCAUSE -- ��ȨͶ���γ�ԭ��
    ,
     SLFLAG -- רҵ�����ʶ
    ,
     SLTYPE -- רҵ��������
    ,
     PFPHASE -- ��Ŀ���ʽ׶�
    ,
     REGURATING -- �������
    ,
     CBRCMPRATINGFLAG -- ������϶������Ƿ��Ϊ����
    ,
     LARGEFLUCFLAG -- �Ƿ񲨶��Խϴ�
    ,
     LIQUEXPOFLAG -- �Ƿ���������з��ձ�¶
    ,
     PAYMENTDEALFLAG -- �Ƿ����Ը�ģʽ
    ,
     DELAYTRADINGDAYS -- �ӳٽ�������
    ,
     SECURITIESFLAG -- �м�֤ȯ��ʶ
    ,
     SECUISSUERID -- ֤ȯ������ID
    ,
     RATINGDURATIONTYPE -- ������������
    ,
     SECUISSUERATING -- ֤ȯ���еȼ�
    ,
     SECURESIDUALM -- ֤ȯʣ������
    ,
     SECUREVAFREQUENCY -- ֤ȯ�ع�Ƶ��
    ,
     CCPTRANFLAG -- �Ƿ����뽻�׶�����ؽ���
    ,
     CCPID -- ���뽻�׶���ID
    ,
     QUALCCPFLAG -- �Ƿ�ϸ����뽻�׶���
    ,
     BANKROLE -- ���н�ɫ
    ,
     CLEARINGMETHOD -- ���㷽ʽ
    ,
     BANKASSETFLAG -- �Ƿ������ύ�ʲ�
    ,
     MATCHCONDITIONS -- �����������
    ,
     SFTFLAG -- ֤ȯ���ʽ��ױ�ʶ
    ,
     MASTERNETAGREEFLAG -- ���������Э���ʶ
    ,
     MASTERNETAGREEID -- ���������Э��ID
    ,
     SFTTYPE -- ֤ȯ���ʽ�������
    ,
     SECUOWNERTRANSFLAG -- ֤ȯ����Ȩ�Ƿ�ת��
    ,
     OTCFLAG -- �����������߱�ʶ
    ,
     VALIDNETTINGFLAG -- ��Ч�������Э���ʶ
    ,
     VALIDNETAGREEMENTID -- ��Ч�������Э��ID
    ,
     OTCTYPE -- ����������������
    ,
     DEPOSITRISKPERIOD -- ��֤������ڼ�
    ,
     MTM -- ���óɱ�
    ,
     MTMCURRENCY -- ���óɱ�����
    ,
     BUYERORSELLER -- ������
    ,
     QUALROFLAG -- �ϸ�����ʲ���ʶ
    ,
     ROISSUERPERFORMFLAG -- �����ʲ��������Ƿ�����Լ
    ,
     BUYERINSOLVENCYFLAG -- ���ñ������Ƿ��Ʋ�
    ,
     NONPAYMENTFEES -- ��δ֧������
    ,
     RETAILEXPOFLAG -- ���۱�¶��ʶ
    ,
     RETAILCLAIMTYPE -- ����ծȨ����
    ,
     MORTGAGETYPE -- ס����Ѻ��������
    ,
     DEBTORNUMBER -- ����˸���
    ,
     EXPONUMBER -- ���ձ�¶����
    ,
     PDPOOLMODELID -- PD�ֳ�ģ��ID
    ,
     LGDPOOLMODELID -- LGD�ֳ�ģ��ID
    ,
     CCFPOOLMODELID -- CCF�ֳ�ģ��ID
    ,
     PDPOOLID -- ����PD��ID
    ,
     LGDPOOLID -- ����LGD��ID
    ,
     CCFPOOLID -- ����CCF��ID
    ,
     ABSUAFLAG -- �ʲ�֤ȯ�������ʲ���ʶ
    ,
     ABSPOOLID -- ֤ȯ���ʲ���ID
    ,
     ABSPROPORTION -- �ʲ�֤ȯ������
    ,
     GROUPID -- ������
    ,
     ORGSORTNO --�������������
    ,
     LTV --�����ֵ��
    ,
     AGING --����
    ,
     NEWDEFAULTDEBTFLAG --����ΥԼծ���ʶ
    ,
     DEFAULTDATE --ΥԼʱ��
     )
    SELECT DATADATE   AS DATADATE -- ��������
          ,
           DATANO     AS DATANO -- ������ˮ��
          ,
           EXPOSUREID AS EXPOSUREID -- ���ձ�¶ID
          ,
           DUEID      AS DUEID -- ծ��ID
          ,
           SSYSID     AS SSYSID -- ԴϵͳID
          ,
           CONTRACTID AS CONTRACTID -- ��ͬID
          ,
           CLIENTID   AS CLIENTID -- ��������ID
          ,
           SORGID     AS SORGID -- Դ����ID
          ,
           SORGNAME   AS SORGNAME -- Դ��������
          ,
           ORGID      AS ORGID -- ��������ID
          ,
           ORGNAME    AS ORGNAME -- ������������
          ,
           ACCORGID   AS ACCORGID -- �������ID
          ,
           ACCORGNAME AS ACCORGNAME -- �����������
          ,
           INDUSTRYID AS INDUSTRYID --������ҵ����
          ,
           NVL       (INDUSTRYNAME, 'δ֪'        �� AS INDUSTRYNAME --������ҵ����
                  , BUSINESSLINE AS BUSINESSLINE -- ����
                  , ASSETTYPE AS ASSETTYPE -- �ʲ�����
                  , ASSETSUBTYPE AS ASSETSUBTYPE -- �ʲ�С��
                  , BUSINESSTYPEID AS BUSINESSTYPEID -- ҵ��Ʒ�ִ���
                  , BUSINESSTYPENAME AS BUSINESSTYPENAME -- ҵ��Ʒ������
                  , CREDITRISKDATATYPE AS CREDITRISKDATATYPE -- ���÷�����������
                  , ASSETTYPEOFHAIRCUTS AS ASSETTYPEOFHAIRCUTS -- �ۿ�ϵ����Ӧ�ʲ����
                  , BUSINESSTYPESTD AS BUSINESSTYPESTD -- Ȩ�ط�ҵ������
                  , EXPOCLASSSTD AS EXPOCLASSSTD -- Ȩ�ط���¶����
                  , EXPOSUBCLASSSTD AS EXPOSUBCLASSSTD -- Ȩ�ط���¶С��
                  , EXPOCLASSIRB AS EXPOCLASSIRB -- ��������¶����
                  , EXPOSUBCLASSIRB AS EXPOSUBCLASSIRB -- ��������¶С��
                  , EXPOBELONG AS EXPOBELONG -- ��¶������ʶ
                  , BOOKTYPE AS BOOKTYPE -- �˻����
                  , REGUTRANTYPE AS REGUTRANTYPE -- ��ܽ�������
                  , REPOTRANFLAG AS REPOTRANFLAG -- �ع����ױ�ʶ
                  , REVAFREQUENCY AS REVAFREQUENCY -- �ع�Ƶ��
                  , CURRENCY AS CURRENCY -- ����
                  , NORMALPRINCIPAL AS NORMALPRINCIPAL -- �����������
                  , OVERDUEBALANCE AS OVERDUEBALANCE -- �������
                  , NONACCRUALBALANCE AS NONACCRUALBALANCE -- ��Ӧ�����
                  , ONSHEETBALANCE AS ONSHEETBALANCE -- �������
                  , NORMALINTEREST AS NORMALINTEREST -- ������Ϣ
                  , ONDEBITINTEREST AS ONDEBITINTEREST -- ����ǷϢ
                  , OFFDEBITINTEREST AS OFFDEBITINTEREST -- ����ǷϢ
                  , EXPENSERECEIVABLE AS EXPENSERECEIVABLE -- Ӧ�շ���
                  , ASSETBALANCE AS ASSETBALANCE -- �ʲ����
                  , ACCSUBJECT1 AS ACCSUBJECT1 -- ��Ŀһ
                  , ACCSUBJECT2 AS ACCSUBJECT2 -- ��Ŀ��
                  , ACCSUBJECT3 AS ACCSUBJECT3 -- ��Ŀ��
                  , TO_CHAR(TO_DATE(STARTDATE, 'YYYY-MM-DD'       ), 'YYYY-MM-DD'       ) AS STARTDATE -- ��ʼ����
                  , TO_CHAR(TO_DATE(DUEDATE, 'YYYY-MM-DD'       ), 'YYYY-MM-DD'       ) AS DUEDATE -- ��������
                  , ORIGINALMATURITY AS ORIGINALMATURITY -- ԭʼ����
                  , RESIDUALM AS RESIDUALM -- ʣ������
                  , RISKCLASSIFY AS RISKCLASSIFY -- ���շ���
                  , EXPOSURESTATUS AS EXPOSURESTATUS -- ���ձ�¶״̬
                  , OVERDUEDAYS AS OVERDUEDAYS -- ��������
                  , SPECIALPROVISION AS SPECIALPROVISION -- ר��׼����
                  , GENERALPROVISION AS GENERALPROVISION -- һ��׼����
                  , ESPECIALPROVISION AS ESPECIALPROVISION -- �ر�׼����
                  , WRITTENOFFAMOUNT AS WRITTENOFFAMOUNT -- �Ѻ������
                  , OFFEXPOSOURCE AS OFFEXPOSOURCE -- ���Ⱪ¶��Դ
                  , OFFBUSINESSTYPE AS OFFBUSINESSTYPE -- ����ҵ������
                  , OFFBUSINESSSDVSSTD AS OFFBUSINESSSDVSSTD -- Ȩ�ط�����ҵ������ϸ��
                  , UNCONDCANCELFLAG AS UNCONDCANCELFLAG -- �Ƿ����ʱ����������
                  , CCFLEVEL AS CCFLEVEL -- ����ת��ϵ������
                  , CCFAIRB AS CCFAIRB -- �߼�������ת��ϵ��
                  , CLAIMSLEVEL AS CLAIMSLEVEL -- ծȨ����
                  , BONDFLAG AS BONDFLAG -- �Ƿ�Ϊծȯ
                  , BONDISSUEINTENT AS BONDISSUEINTENT -- ծȯ����Ŀ��
                  , NSUREALPROPERTYFLAG AS NSUREALPROPERTYFLAG -- �Ƿ�����ò�����
                  , REPASSETTERMTYPE AS REPASSETTERMTYPE -- ��ծ�ʲ���������
                  , DEPENDONFPOBFLAG AS DEPENDONFPOBFLAG -- �Ƿ�����������δ��ӯ��
                  , IRATING AS IRATING -- �ڲ�����
                  , PD AS PD -- ΥԼ����
                  , LGDLEVEL AS LGDLEVEL -- ΥԼ��ʧ�ʼ���
                  , LGDAIRB AS LGDAIRB -- �߼���ΥԼ��ʧ��
                  , MAIRB AS MAIRB -- �߼�����Ч����
                  , EADAIRB AS EADAIRB -- �߼���ΥԼ���ձ�¶
                  , DEFAULTFLAG AS DEFAULTFLAG -- ΥԼ��ʶ
                  , BEEL AS BEEL -- ��ΥԼ��¶Ԥ����ʧ����
                  , DEFAULTLGD AS DEFAULTLGD -- ��ΥԼ��¶ΥԼ��ʧ��
                  , EQUITYEXPOFLAG AS EQUITYEXPOFLAG -- ��Ȩ��¶��ʶ
                  , EQUITYINVESTTYPE AS EQUITYINVESTTYPE -- ��ȨͶ�ʶ�������
                  , EQUITYINVESTCAUSE AS EQUITYINVESTCAUSE -- ��ȨͶ���γ�ԭ��
                  , SLFLAG AS SLFLAG -- רҵ�����ʶ
                  , SLTYPE AS SLTYPE -- רҵ��������
                  , PFPHASE AS PFPHASE -- ��Ŀ���ʽ׶�
                  , REGURATING AS REGURATING -- �������
                  , CBRCMPRATINGFLAG AS CBRCMPRATINGFLAG -- ������϶������Ƿ��Ϊ����
                  , LARGEFLUCFLAG AS LARGEFLUCFLAG -- �Ƿ񲨶��Խϴ�
                  , LIQUEXPOFLAG AS LIQUEXPOFLAG -- �Ƿ���������з��ձ�¶
                  , PAYMENTDEALFLAG AS PAYMENTDEALFLAG -- �Ƿ����Ը�ģʽ
                  , DELAYTRADINGDAYS AS DELAYTRADINGDAYS -- �ӳٽ�������
                  , SECURITIESFLAG AS SECURITIESFLAG -- �м�֤ȯ��ʶ
                  , SECUISSUERID AS SECUISSUERID -- ֤ȯ������ID
                  , RATINGDURATIONTYPE AS RATINGDURATIONTYPE -- ������������
                  , SECUISSUERATING AS SECUISSUERATING -- ֤ȯ���еȼ�
                  , SECURESIDUALM AS SECURESIDUALM -- ֤ȯʣ������
                  , SECUREVAFREQUENCY AS SECUREVAFREQUENCY -- ֤ȯ�ع�Ƶ��
                  , CCPTRANFLAG AS CCPTRANFLAG -- �Ƿ����뽻�׶�����ؽ���
                  , CCPID AS CCPID -- ���뽻�׶���ID
                  , QUALCCPFLAG AS QUALCCPFLAG -- �Ƿ�ϸ����뽻�׶���
                  , BANKROLE AS BANKROLE -- ���н�ɫ
                  , CLEARINGMETHOD AS CLEARINGMETHOD -- ���㷽ʽ
                  , BANKASSETFLAG AS BANKASSETFLAG -- �Ƿ������ύ�ʲ�
                  , MATCHCONDITIONS AS MATCHCONDITIONS -- �����������
                  , SFTFLAG AS SFTFLAG -- ֤ȯ���ʽ��ױ�ʶ
                  , MASTERNETAGREEFLAG AS MASTERNETAGREEFLAG -- ���������Э���ʶ
                  , MASTERNETAGREEID AS MASTERNETAGREEID -- ���������Э��ID
                  , SFTTYPE AS SFTTYPE -- ֤ȯ���ʽ�������
                  , SECUOWNERTRANSFLAG AS SECUOWNERTRANSFLAG -- ֤ȯ����Ȩ�Ƿ�ת��
                  , OTCFLAG AS OTCFLAG -- �����������߱�ʶ
                  , VALIDNETTINGFLAG AS VALIDNETTINGFLAG -- ��Ч�������Э���ʶ
                  , VALIDNETAGREEMENTID AS VALIDNETAGREEMENTID -- ��Ч�������Э��ID
                  , OTCTYPE AS OTCTYPE -- ����������������
                  , DEPOSITRISKPERIOD AS DEPOSITRISKPERIOD -- ��֤������ڼ�
                  , MTM AS MTM -- ���óɱ�
                  , MTMCURRENCY AS MTMCURRENCY -- ���óɱ�����
                  , BUYERORSELLER AS BUYERORSELLER -- ������
                  , QUALROFLAG AS QUALROFLAG -- �ϸ�����ʲ���ʶ
                  , ROISSUERPERFORMFLAG AS ROISSUERPERFORMFLAG -- �����ʲ��������Ƿ�����Լ
                  , BUYERINSOLVENCYFLAG AS BUYERINSOLVENCYFLAG -- ���ñ������Ƿ��Ʋ�
                  , NONPAYMENTFEES AS NONPAYMENTFEES -- ��δ֧������
                  , RETAILEXPOFLAG AS RETAILEXPOFLAG -- ���۱�¶��ʶ
                  , RETAILCLAIMTYPE AS RETAILCLAIMTYPE -- ����ծȨ����
                  , MORTGAGETYPE AS MORTGAGETYPE -- ס����Ѻ��������
                  , DEBTORNUMBER AS DEBTORNUMBER -- ����˸���
                  , EXPONUMBER AS EXPONUMBER -- ���ձ�¶����
                  , PDPOOLMODELID AS PDPOOLMODELID -- PD�ֳ�ģ��ID
                  , LGDPOOLMODELID AS LGDPOOLMODELID -- LGD�ֳ�ģ��ID
                  , CCFPOOLMODELID AS CCFPOOLMODELID -- CCF�ֳ�ģ��ID
                  , PDPOOLID AS PDPOOLID -- ����PD��ID
                  , LGDPOOLID AS LGDPOOLID -- ����LGD��ID
                  , CCFPOOLID AS CCFPOOLID -- ����CCF��ID
                  , ABSUAFLAG AS ABSUAFLAG -- �ʲ�֤ȯ�������ʲ���ʶ
                  , ABSPOOLID AS ABSPOOLID -- ֤ȯ���ʲ���ID
                  , ABSPROPORTION AS ABSPROPORTION -- �ʲ�֤ȯ������
                  , GROUPID AS GROUPID -- ������
                  , ORGSORTNO AS ORGSORTNO --�������������
                  , LTV AS LTV --�����ֵ��
                  , AGING AS AGING --����
                  , NEWDEFAULTDEBTFLAG AS NEWDEFAULTDEBTFLAG --����ΥԼծ���ʶ
                  , DEFAULTDATE AS DEFAULTDATE --ΥԼʱ��
                   FROM RWA_DEV.RWA_ZX_EXPOSURE WHERE DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD'       );
  COMMIT;

  /*�����ʲ�֤ȯ�����л����ķ��ձ�¶��Ϣ*/
  INSERT /*+ APPEND */
  INTO RWA_DEV.RWA_EI_EXPOSURE
    (DATADATE -- ��������
    ,
     DATANO -- ������ˮ��
    ,
     EXPOSUREID -- ���ձ�¶ID
    ,
     DUEID -- ծ��ID
    ,
     SSYSID -- ԴϵͳID
    ,
     CONTRACTID -- ��ͬID
    ,
     CLIENTID -- ��������ID
    ,
     SORGID -- Դ����ID
    ,
     SORGNAME -- Դ��������
    ,
     ORGID -- ��������ID
    ,
     ORGNAME -- ������������
    ,
     ACCORGID -- �������ID
    ,
     ACCORGNAME -- �����������
    ,
     INDUSTRYID -- ������ҵ����
    ,
     INDUSTRYNAME -- ������ҵ����
    ,
     BUSINESSLINE -- ����
    ,
     ASSETTYPE -- �ʲ�����
    ,
     ASSETSUBTYPE -- �ʲ�С��
    ,
     BUSINESSTYPEID -- ҵ��Ʒ�ִ���
    ,
     BUSINESSTYPENAME -- ҵ��Ʒ������
    ,
     CREDITRISKDATATYPE -- ���÷�����������
    ,
     ASSETTYPEOFHAIRCUTS -- �ۿ�ϵ����Ӧ�ʲ����
    ,
     BUSINESSTYPESTD -- Ȩ�ط�ҵ������
    ,
     EXPOCLASSSTD -- Ȩ�ط���¶����
    ,
     EXPOSUBCLASSSTD -- Ȩ�ط���¶С��
    ,
     EXPOCLASSIRB -- ��������¶����
    ,
     EXPOSUBCLASSIRB -- ��������¶С��
    ,
     EXPOBELONG -- ��¶������ʶ
    ,
     BOOKTYPE -- �˻����
    ,
     REGUTRANTYPE -- ��ܽ�������
    ,
     REPOTRANFLAG -- �ع����ױ�ʶ
    ,
     REVAFREQUENCY -- �ع�Ƶ��
    ,
     CURRENCY -- ����
    ,
     NORMALPRINCIPAL -- �����������
    ,
     OVERDUEBALANCE -- �������
    ,
     NONACCRUALBALANCE -- ��Ӧ�����
    ,
     ONSHEETBALANCE -- �������
    ,
     NORMALINTEREST -- ������Ϣ
    ,
     ONDEBITINTEREST -- ����ǷϢ
    ,
     OFFDEBITINTEREST -- ����ǷϢ
    ,
     EXPENSERECEIVABLE -- Ӧ�շ���
    ,
     ASSETBALANCE -- �ʲ����
    ,
     ACCSUBJECT1 -- ��Ŀһ
    ,
     ACCSUBJECT2 -- ��Ŀ��
    ,
     ACCSUBJECT3 -- ��Ŀ��
    ,
     STARTDATE -- ��ʼ����
    ,
     DUEDATE -- ��������
    ,
     ORIGINALMATURITY -- ԭʼ����
    ,
     RESIDUALM -- ʣ������
    ,
     RISKCLASSIFY -- ���շ���
    ,
     EXPOSURESTATUS -- ���ձ�¶״̬
    ,
     OVERDUEDAYS -- ��������
    ,
     SPECIALPROVISION -- ר��׼����
    ,
     GENERALPROVISION -- һ��׼����
    ,
     ESPECIALPROVISION -- �ر�׼����
    ,
     WRITTENOFFAMOUNT -- �Ѻ������
    ,
     OFFEXPOSOURCE -- ���Ⱪ¶��Դ
    ,
     OFFBUSINESSTYPE -- ����ҵ������
    ,
     OFFBUSINESSSDVSSTD -- Ȩ�ط�����ҵ������ϸ��
    ,
     UNCONDCANCELFLAG -- �Ƿ����ʱ����������
    ,
     CCFLEVEL -- ����ת��ϵ������
    ,
     CCFAIRB -- �߼�������ת��ϵ��
    ,
     CLAIMSLEVEL -- ծȨ����
    ,
     BONDFLAG -- �Ƿ�Ϊծȯ
    ,
     BONDISSUEINTENT -- ծȯ����Ŀ��
    ,
     NSUREALPROPERTYFLAG -- �Ƿ�����ò�����
    ,
     REPASSETTERMTYPE -- ��ծ�ʲ���������
    ,
     DEPENDONFPOBFLAG -- �Ƿ�����������δ��ӯ��
    ,
     IRATING -- �ڲ�����
    ,
     PD -- ΥԼ����
    ,
     LGDLEVEL -- ΥԼ��ʧ�ʼ���
    ,
     LGDAIRB -- �߼���ΥԼ��ʧ��
    ,
     MAIRB -- �߼�����Ч����
    ,
     EADAIRB -- �߼���ΥԼ���ձ�¶
    ,
     DEFAULTFLAG -- ΥԼ��ʶ
    ,
     BEEL -- ��ΥԼ��¶Ԥ����ʧ����
    ,
     DEFAULTLGD -- ��ΥԼ��¶ΥԼ��ʧ��
    ,
     EQUITYEXPOFLAG -- ��Ȩ��¶��ʶ
    ,
     EQUITYINVESTTYPE -- ��ȨͶ�ʶ�������
    ,
     EQUITYINVESTCAUSE -- ��ȨͶ���γ�ԭ��
    ,
     SLFLAG -- רҵ�����ʶ
    ,
     SLTYPE -- רҵ��������
    ,
     PFPHASE -- ��Ŀ���ʽ׶�
    ,
     REGURATING -- �������
    ,
     CBRCMPRATINGFLAG -- ������϶������Ƿ��Ϊ����
    ,
     LARGEFLUCFLAG -- �Ƿ񲨶��Խϴ�
    ,
     LIQUEXPOFLAG -- �Ƿ���������з��ձ�¶
    ,
     PAYMENTDEALFLAG -- �Ƿ����Ը�ģʽ
    ,
     DELAYTRADINGDAYS -- �ӳٽ�������
    ,
     SECURITIESFLAG -- �м�֤ȯ��ʶ
    ,
     SECUISSUERID -- ֤ȯ������ID
    ,
     RATINGDURATIONTYPE -- ������������
    ,
     SECUISSUERATING -- ֤ȯ���еȼ�
    ,
     SECURESIDUALM -- ֤ȯʣ������
    ,
     SECUREVAFREQUENCY -- ֤ȯ�ع�Ƶ��
    ,
     CCPTRANFLAG -- �Ƿ����뽻�׶�����ؽ���
    ,
     CCPID -- ���뽻�׶���ID
    ,
     QUALCCPFLAG -- �Ƿ�ϸ����뽻�׶���
    ,
     BANKROLE -- ���н�ɫ
    ,
     CLEARINGMETHOD -- ���㷽ʽ
    ,
     BANKASSETFLAG -- �Ƿ������ύ�ʲ�
    ,
     MATCHCONDITIONS -- �����������
    ,
     SFTFLAG -- ֤ȯ���ʽ��ױ�ʶ
    ,
     MASTERNETAGREEFLAG -- ���������Э���ʶ
    ,
     MASTERNETAGREEID -- ���������Э��ID
    ,
     SFTTYPE -- ֤ȯ���ʽ�������
    ,
     SECUOWNERTRANSFLAG -- ֤ȯ����Ȩ�Ƿ�ת��
    ,
     OTCFLAG -- �����������߱�ʶ
    ,
     VALIDNETTINGFLAG -- ��Ч�������Э���ʶ
    ,
     VALIDNETAGREEMENTID -- ��Ч�������Э��ID
    ,
     OTCTYPE -- ����������������
    ,
     DEPOSITRISKPERIOD -- ��֤������ڼ�
    ,
     MTM -- ���óɱ�
    ,
     MTMCURRENCY -- ���óɱ�����
    ,
     BUYERORSELLER -- ������
    ,
     QUALROFLAG -- �ϸ�����ʲ���ʶ
    ,
     ROISSUERPERFORMFLAG -- �����ʲ��������Ƿ�����Լ
    ,
     BUYERINSOLVENCYFLAG -- ���ñ������Ƿ��Ʋ�
    ,
     NONPAYMENTFEES -- ��δ֧������
    ,
     RETAILEXPOFLAG -- ���۱�¶��ʶ
    ,
     RETAILCLAIMTYPE -- ����ծȨ����
    ,
     MORTGAGETYPE -- ס����Ѻ��������
    ,
     DEBTORNUMBER -- ����˸���
    ,
     EXPONUMBER -- ���ձ�¶����
    ,
     PDPOOLMODELID -- PD�ֳ�ģ��ID
    ,
     LGDPOOLMODELID -- LGD�ֳ�ģ��ID
    ,
     CCFPOOLMODELID -- CCF�ֳ�ģ��ID
    ,
     PDPOOLID -- ����PD��ID
    ,
     LGDPOOLID -- ����LGD��ID
    ,
     CCFPOOLID -- ����CCF��ID
    ,
     ABSUAFLAG -- �ʲ�֤ȯ�������ʲ���ʶ
    ,
     ABSPOOLID -- ֤ȯ���ʲ���ID
    ,
     ABSPROPORTION -- �ʲ�֤ȯ������
    ,
     GROUPID -- ������
    ,
     ORGSORTNO --�������������
    ,
     LTV --�����ֵ��
    ,
     AGING --����
    ,
     NEWDEFAULTDEBTFLAG --����ΥԼծ���ʶ
    ,
     DEFAULTDATE --ΥԼʱ��
     )
    SELECT DATADATE   AS DATADATE -- ��������
          ,
           DATANO     AS DATANO -- ������ˮ��
          ,
           EXPOSUREID AS EXPOSUREID -- ���ձ�¶ID
          ,
           DUEID      AS DUEID -- ծ��ID
          ,
           SSYSID     AS SSYSID -- ԴϵͳID
          ,
           CONTRACTID AS CONTRACTID -- ��ͬID
          ,
           CLIENTID   AS CLIENTID -- ��������ID
          ,
           SORGID     AS SORGID -- Դ����ID
          ,
           SORGNAME   AS SORGNAME -- Դ��������
          ,
           ORGID      AS ORGID -- ��������ID
          ,
           ORGNAME    AS ORGNAME -- ������������
          ,
           ACCORGID   AS ACCORGID -- �������ID
          ,
           ACCORGNAME AS ACCORGNAME -- �����������
          ,
           INDUSTRYID AS INDUSTRYID --������ҵ����
          ,
           NVL       (INDUSTRYNAME, 'δ֪'        �� AS INDUSTRYNAME --������ҵ����
                  , BUSINESSLINE AS BUSINESSLINE -- ����
                  , ASSETTYPE AS ASSETTYPE -- �ʲ�����
                  , ASSETSUBTYPE AS ASSETSUBTYPE -- �ʲ�С��
                  , BUSINESSTYPEID AS BUSINESSTYPEID -- ҵ��Ʒ�ִ���
                  , BUSINESSTYPENAME AS BUSINESSTYPENAME -- ҵ��Ʒ������
                  , CREDITRISKDATATYPE AS CREDITRISKDATATYPE -- ���÷�����������
                  , ASSETTYPEOFHAIRCUTS AS ASSETTYPEOFHAIRCUTS -- �ۿ�ϵ����Ӧ�ʲ����
                  , BUSINESSTYPESTD AS BUSINESSTYPESTD -- Ȩ�ط�ҵ������
                  , EXPOCLASSSTD AS EXPOCLASSSTD -- Ȩ�ط���¶����
                  , EXPOSUBCLASSSTD AS EXPOSUBCLASSSTD -- Ȩ�ط���¶С��
                  , EXPOCLASSIRB AS EXPOCLASSIRB -- ��������¶����
                  , EXPOSUBCLASSIRB AS EXPOSUBCLASSIRB -- ��������¶С��
                  , EXPOBELONG AS EXPOBELONG -- ��¶������ʶ
                  , BOOKTYPE AS BOOKTYPE -- �˻����
                  , REGUTRANTYPE AS REGUTRANTYPE -- ��ܽ�������
                  , REPOTRANFLAG AS REPOTRANFLAG -- �ع����ױ�ʶ
                  , REVAFREQUENCY AS REVAFREQUENCY -- �ع�Ƶ��
                  , CURRENCY AS CURRENCY -- ����
                  , NORMALPRINCIPAL AS NORMALPRINCIPAL -- �����������
                  , OVERDUEBALANCE AS OVERDUEBALANCE -- �������
                  , NONACCRUALBALANCE AS NONACCRUALBALANCE -- ��Ӧ�����
                  , ONSHEETBALANCE AS ONSHEETBALANCE -- �������
                  , NORMALINTEREST AS NORMALINTEREST -- ������Ϣ
                  , ONDEBITINTEREST AS ONDEBITINTEREST -- ����ǷϢ
                  , OFFDEBITINTEREST AS OFFDEBITINTEREST -- ����ǷϢ
                  , EXPENSERECEIVABLE AS EXPENSERECEIVABLE -- Ӧ�շ���
                  , ASSETBALANCE AS ASSETBALANCE -- �ʲ����
                  , ACCSUBJECT1 AS ACCSUBJECT1 -- ��Ŀһ
                  , ACCSUBJECT2 AS ACCSUBJECT2 -- ��Ŀ��
                  , ACCSUBJECT3 AS ACCSUBJECT3 -- ��Ŀ��
                  , TO_CHAR(TO_DATE(STARTDATE, 'YYYY-MM-DD'       ), 'YYYY-MM-DD'       ) AS STARTDATE -- ��ʼ����
                  , TO_CHAR(TO_DATE(DUEDATE, 'YYYY-MM-DD'       ), 'YYYY-MM-DD'       ) AS DUEDATE -- ��������
                  , ORIGINALMATURITY AS ORIGINALMATURITY -- ԭʼ����
                  , RESIDUALM AS RESIDUALM -- ʣ������
                  , RISKCLASSIFY AS RISKCLASSIFY -- ���շ���
                  , EXPOSURESTATUS AS EXPOSURESTATUS -- ���ձ�¶״̬
                  , OVERDUEDAYS AS OVERDUEDAYS -- ��������
                  , SPECIALPROVISION AS SPECIALPROVISION -- ר��׼����
                  , GENERALPROVISION AS GENERALPROVISION -- һ��׼����
                  , ESPECIALPROVISION AS ESPECIALPROVISION -- �ر�׼����
                  , WRITTENOFFAMOUNT AS WRITTENOFFAMOUNT -- �Ѻ������
                  , OFFEXPOSOURCE AS OFFEXPOSOURCE -- ���Ⱪ¶��Դ
                  , OFFBUSINESSTYPE AS OFFBUSINESSTYPE -- ����ҵ������
                  , OFFBUSINESSSDVSSTD AS OFFBUSINESSSDVSSTD -- Ȩ�ط�����ҵ������ϸ��
                  , UNCONDCANCELFLAG AS UNCONDCANCELFLAG -- �Ƿ����ʱ����������
                  , CCFLEVEL AS CCFLEVEL -- ����ת��ϵ������
                  , CCFAIRB AS CCFAIRB -- �߼�������ת��ϵ��
                  , CLAIMSLEVEL AS CLAIMSLEVEL -- ծȨ����
                  , BONDFLAG AS BONDFLAG -- �Ƿ�Ϊծȯ
                  , BONDISSUEINTENT AS BONDISSUEINTENT -- ծȯ����Ŀ��
                  , NSUREALPROPERTYFLAG AS NSUREALPROPERTYFLAG -- �Ƿ�����ò�����
                  , REPASSETTERMTYPE AS REPASSETTERMTYPE -- ��ծ�ʲ���������
                  , DEPENDONFPOBFLAG AS DEPENDONFPOBFLAG -- �Ƿ�����������δ��ӯ��
                  , IRATING AS IRATING -- �ڲ�����
                  , PD AS PD -- ΥԼ����
                  , LGDLEVEL AS LGDLEVEL -- ΥԼ��ʧ�ʼ���
                  , LGDAIRB AS LGDAIRB -- �߼���ΥԼ��ʧ��
                  , MAIRB AS MAIRB -- �߼�����Ч����
                  , EADAIRB AS EADAIRB -- �߼���ΥԼ���ձ�¶
                  , DEFAULTFLAG AS DEFAULTFLAG -- ΥԼ��ʶ
                  , BEEL AS BEEL -- ��ΥԼ��¶Ԥ����ʧ����
                  , DEFAULTLGD AS DEFAULTLGD -- ��ΥԼ��¶ΥԼ��ʧ��
                  , EQUITYEXPOFLAG AS EQUITYEXPOFLAG -- ��Ȩ��¶��ʶ
                  , EQUITYINVESTTYPE AS EQUITYINVESTTYPE -- ��ȨͶ�ʶ�������
                  , EQUITYINVESTCAUSE AS EQUITYINVESTCAUSE -- ��ȨͶ���γ�ԭ��
                  , SLFLAG AS SLFLAG -- רҵ�����ʶ
                  , SLTYPE AS SLTYPE -- רҵ��������
                  , PFPHASE AS PFPHASE -- ��Ŀ���ʽ׶�
                  , REGURATING AS REGURATING -- �������
                  , CBRCMPRATINGFLAG AS CBRCMPRATINGFLAG -- ������϶������Ƿ��Ϊ����
                  , LARGEFLUCFLAG AS LARGEFLUCFLAG -- �Ƿ񲨶��Խϴ�
                  , LIQUEXPOFLAG AS LIQUEXPOFLAG -- �Ƿ���������з��ձ�¶
                  , PAYMENTDEALFLAG AS PAYMENTDEALFLAG -- �Ƿ����Ը�ģʽ
                  , DELAYTRADINGDAYS AS DELAYTRADINGDAYS -- �ӳٽ�������
                  , SECURITIESFLAG AS SECURITIESFLAG -- �м�֤ȯ��ʶ
                  , SECUISSUERID AS SECUISSUERID -- ֤ȯ������ID
                  , RATINGDURATIONTYPE AS RATINGDURATIONTYPE -- ������������
                  , SECUISSUERATING AS SECUISSUERATING -- ֤ȯ���еȼ�
                  , SECURESIDUALM AS SECURESIDUALM -- ֤ȯʣ������
                  , SECUREVAFREQUENCY AS SECUREVAFREQUENCY -- ֤ȯ�ع�Ƶ��
                  , CCPTRANFLAG AS CCPTRANFLAG -- �Ƿ����뽻�׶�����ؽ���
                  , CCPID AS CCPID -- ���뽻�׶���ID
                  , QUALCCPFLAG AS QUALCCPFLAG -- �Ƿ�ϸ����뽻�׶���
                  , BANKROLE AS BANKROLE -- ���н�ɫ
                  , CLEARINGMETHOD AS CLEARINGMETHOD -- ���㷽ʽ
                  , BANKASSETFLAG AS BANKASSETFLAG -- �Ƿ������ύ�ʲ�
                  , MATCHCONDITIONS AS MATCHCONDITIONS -- �����������
                  , SFTFLAG AS SFTFLAG -- ֤ȯ���ʽ��ױ�ʶ
                  , MASTERNETAGREEFLAG AS MASTERNETAGREEFLAG -- ���������Э���ʶ
                  , MASTERNETAGREEID AS MASTERNETAGREEID -- ���������Э��ID
                  , SFTTYPE AS SFTTYPE -- ֤ȯ���ʽ�������
                  , SECUOWNERTRANSFLAG AS SECUOWNERTRANSFLAG -- ֤ȯ����Ȩ�Ƿ�ת��
                  , OTCFLAG AS OTCFLAG -- �����������߱�ʶ
                  , VALIDNETTINGFLAG AS VALIDNETTINGFLAG -- ��Ч�������Э���ʶ
                  , VALIDNETAGREEMENTID AS VALIDNETAGREEMENTID -- ��Ч�������Э��ID
                  , OTCTYPE AS OTCTYPE -- ����������������
                  , DEPOSITRISKPERIOD AS DEPOSITRISKPERIOD -- ��֤������ڼ�
                  , MTM AS MTM -- ���óɱ�
                  , MTMCURRENCY AS MTMCURRENCY -- ���óɱ�����
                  , BUYERORSELLER AS BUYERORSELLER -- ������
                  , QUALROFLAG AS QUALROFLAG -- �ϸ�����ʲ���ʶ
                  , ROISSUERPERFORMFLAG AS ROISSUERPERFORMFLAG -- �����ʲ��������Ƿ�����Լ
                  , BUYERINSOLVENCYFLAG AS BUYERINSOLVENCYFLAG -- ���ñ������Ƿ��Ʋ�
                  , NONPAYMENTFEES AS NONPAYMENTFEES -- ��δ֧������
                  , RETAILEXPOFLAG AS RETAILEXPOFLAG -- ���۱�¶��ʶ
                  , RETAILCLAIMTYPE AS RETAILCLAIMTYPE -- ����ծȨ����
                  , MORTGAGETYPE AS MORTGAGETYPE -- ס����Ѻ��������
                  , DEBTORNUMBER AS DEBTORNUMBER -- ����˸���
                  , EXPONUMBER AS EXPONUMBER -- ���ձ�¶����
                  , PDPOOLMODELID AS PDPOOLMODELID -- PD�ֳ�ģ��ID
                  , LGDPOOLMODELID AS LGDPOOLMODELID -- LGD�ֳ�ģ��ID
                  , CCFPOOLMODELID AS CCFPOOLMODELID -- CCF�ֳ�ģ��ID
                  , PDPOOLID AS PDPOOLID -- ����PD��ID
                  , LGDPOOLID AS LGDPOOLID -- ����LGD��ID
                  , CCFPOOLID AS CCFPOOLID -- ����CCF��ID
                  , ABSUAFLAG AS ABSUAFLAG -- �ʲ�֤ȯ�������ʲ���ʶ
                  , ABSPOOLID AS ABSPOOLID -- ֤ȯ���ʲ���ID
                  , ABSPROPORTION AS ABSPROPORTION -- �ʲ�֤ȯ������
                  , GROUPID AS GROUPID -- ������
                  , ORGSORTNO AS ORGSORTNO --�������������
                  , LTV AS LTV --�����ֵ��
                  , AGING AS AGING --����
                  , NEWDEFAULTDEBTFLAG AS NEWDEFAULTDEBTFLAG --����ΥԼծ���ʶ
                  , DEFAULTDATE AS DEFAULTDATE --ΥԼʱ��
                   FROM RWA_DEV.RWA_ABS_ISSURE_EXPOSURE WHERE DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD'       );
  COMMIT;

  /*��������Ʒҵ�����÷��ձ�¶����Ϣ*/
  INSERT /*+ APPEND */
  INTO RWA_DEV.RWA_EI_EXPOSURE
    (DATADATE -- ��������
    ,
     DATANO -- ������ˮ��
    ,
     EXPOSUREID -- ���ձ�¶ID
    ,
     DUEID -- ծ��ID
    ,
     SSYSID -- ԴϵͳID
    ,
     CONTRACTID -- ��ͬID
    ,
     CLIENTID -- ��������ID
    ,
     SORGID -- Դ����ID
    ,
     SORGNAME -- Դ��������
    ,
     ORGID -- ��������ID
    ,
     ORGNAME -- ������������
    ,
     ACCORGID -- �������ID
    ,
     ACCORGNAME -- �����������
    ,
     INDUSTRYID -- ������ҵ����
    ,
     INDUSTRYNAME -- ������ҵ����
    ,
     BUSINESSLINE -- ����
    ,
     ASSETTYPE -- �ʲ�����
    ,
     ASSETSUBTYPE -- �ʲ�С��
    ,
     BUSINESSTYPEID -- ҵ��Ʒ�ִ���
    ,
     BUSINESSTYPENAME -- ҵ��Ʒ������
    ,
     CREDITRISKDATATYPE -- ���÷�����������
    ,
     ASSETTYPEOFHAIRCUTS -- �ۿ�ϵ����Ӧ�ʲ����
    ,
     BUSINESSTYPESTD -- Ȩ�ط�ҵ������
    ,
     EXPOCLASSSTD -- Ȩ�ط���¶����
    ,
     EXPOSUBCLASSSTD -- Ȩ�ط���¶С��
    ,
     EXPOCLASSIRB -- ��������¶����
    ,
     EXPOSUBCLASSIRB -- ��������¶С��
    ,
     EXPOBELONG -- ��¶������ʶ
    ,
     BOOKTYPE -- �˻����
    ,
     REGUTRANTYPE -- ��ܽ�������
    ,
     REPOTRANFLAG -- �ع����ױ�ʶ
    ,
     REVAFREQUENCY -- �ع�Ƶ��
    ,
     CURRENCY -- ����
    ,
     NORMALPRINCIPAL -- �����������
    ,
     OVERDUEBALANCE -- �������
    ,
     NONACCRUALBALANCE -- ��Ӧ�����
    ,
     ONSHEETBALANCE -- �������
    ,
     NORMALINTEREST -- ������Ϣ
    ,
     ONDEBITINTEREST -- ����ǷϢ
    ,
     OFFDEBITINTEREST -- ����ǷϢ
    ,
     EXPENSERECEIVABLE -- Ӧ�շ���
    ,
     ASSETBALANCE -- �ʲ����
    ,
     ACCSUBJECT1 -- ��Ŀһ
    ,
     ACCSUBJECT2 -- ��Ŀ��
    ,
     ACCSUBJECT3 -- ��Ŀ��
    ,
     STARTDATE -- ��ʼ����
    ,
     DUEDATE -- ��������
    ,
     ORIGINALMATURITY -- ԭʼ����
    ,
     RESIDUALM -- ʣ������
    ,
     RISKCLASSIFY -- ���շ���
    ,
     EXPOSURESTATUS -- ���ձ�¶״̬
    ,
     OVERDUEDAYS -- ��������
    ,
     SPECIALPROVISION -- ר��׼����
    ,
     GENERALPROVISION -- һ��׼����
    ,
     ESPECIALPROVISION -- �ر�׼����
    ,
     WRITTENOFFAMOUNT -- �Ѻ������
    ,
     OFFEXPOSOURCE -- ���Ⱪ¶��Դ
    ,
     OFFBUSINESSTYPE -- ����ҵ������
    ,
     OFFBUSINESSSDVSSTD -- Ȩ�ط�����ҵ������ϸ��
    ,
     UNCONDCANCELFLAG -- �Ƿ����ʱ����������
    ,
     CCFLEVEL -- ����ת��ϵ������
    ,
     CCFAIRB -- �߼�������ת��ϵ��
    ,
     CLAIMSLEVEL -- ծȨ����
    ,
     BONDFLAG -- �Ƿ�Ϊծȯ
    ,
     BONDISSUEINTENT -- ծȯ����Ŀ��
    ,
     NSUREALPROPERTYFLAG -- �Ƿ�����ò�����
    ,
     REPASSETTERMTYPE -- ��ծ�ʲ���������
    ,
     DEPENDONFPOBFLAG -- �Ƿ�����������δ��ӯ��
    ,
     IRATING -- �ڲ�����
    ,
     PD -- ΥԼ����
    ,
     LGDLEVEL -- ΥԼ��ʧ�ʼ���
    ,
     LGDAIRB -- �߼���ΥԼ��ʧ��
    ,
     MAIRB -- �߼�����Ч����
    ,
     EADAIRB -- �߼���ΥԼ���ձ�¶
    ,
     DEFAULTFLAG -- ΥԼ��ʶ
    ,
     BEEL -- ��ΥԼ��¶Ԥ����ʧ����
    ,
     DEFAULTLGD -- ��ΥԼ��¶ΥԼ��ʧ��
    ,
     EQUITYEXPOFLAG -- ��Ȩ��¶��ʶ
    ,
     EQUITYINVESTTYPE -- ��ȨͶ�ʶ�������
    ,
     EQUITYINVESTCAUSE -- ��ȨͶ���γ�ԭ��
    ,
     SLFLAG -- רҵ�����ʶ
    ,
     SLTYPE -- רҵ��������
    ,
     PFPHASE -- ��Ŀ���ʽ׶�
    ,
     REGURATING -- �������
    ,
     CBRCMPRATINGFLAG -- ������϶������Ƿ��Ϊ����
    ,
     LARGEFLUCFLAG -- �Ƿ񲨶��Խϴ�
    ,
     LIQUEXPOFLAG -- �Ƿ���������з��ձ�¶
    ,
     PAYMENTDEALFLAG -- �Ƿ����Ը�ģʽ
    ,
     DELAYTRADINGDAYS -- �ӳٽ�������
    ,
     SECURITIESFLAG -- �м�֤ȯ��ʶ
    ,
     SECUISSUERID -- ֤ȯ������ID
    ,
     RATINGDURATIONTYPE -- ������������
    ,
     SECUISSUERATING -- ֤ȯ���еȼ�
    ,
     SECURESIDUALM -- ֤ȯʣ������
    ,
     SECUREVAFREQUENCY -- ֤ȯ�ع�Ƶ��
    ,
     CCPTRANFLAG -- �Ƿ����뽻�׶�����ؽ���
    ,
     CCPID -- ���뽻�׶���ID
    ,
     QUALCCPFLAG -- �Ƿ�ϸ����뽻�׶���
    ,
     BANKROLE -- ���н�ɫ
    ,
     CLEARINGMETHOD -- ���㷽ʽ
    ,
     BANKASSETFLAG -- �Ƿ������ύ�ʲ�
    ,
     MATCHCONDITIONS -- �����������
    ,
     SFTFLAG -- ֤ȯ���ʽ��ױ�ʶ
    ,
     MASTERNETAGREEFLAG -- ���������Э���ʶ
    ,
     MASTERNETAGREEID -- ���������Э��ID
    ,
     SFTTYPE -- ֤ȯ���ʽ�������
    ,
     SECUOWNERTRANSFLAG -- ֤ȯ����Ȩ�Ƿ�ת��
    ,
     OTCFLAG -- �����������߱�ʶ
    ,
     VALIDNETTINGFLAG -- ��Ч�������Э���ʶ
    ,
     VALIDNETAGREEMENTID -- ��Ч�������Э��ID
    ,
     OTCTYPE -- ����������������
    ,
     DEPOSITRISKPERIOD -- ��֤������ڼ�
    ,
     MTM -- ���óɱ�
    ,
     MTMCURRENCY -- ���óɱ�����
    ,
     BUYERORSELLER -- ������
    ,
     QUALROFLAG -- �ϸ�����ʲ���ʶ
    ,
     ROISSUERPERFORMFLAG -- �����ʲ��������Ƿ�����Լ
    ,
     BUYERINSOLVENCYFLAG -- ���ñ������Ƿ��Ʋ�
    ,
     NONPAYMENTFEES -- ��δ֧������
    ,
     RETAILEXPOFLAG -- ���۱�¶��ʶ
    ,
     RETAILCLAIMTYPE -- ����ծȨ����
    ,
     MORTGAGETYPE -- ס����Ѻ��������
    ,
     DEBTORNUMBER -- ����˸���
    ,
     EXPONUMBER -- ���ձ�¶����
    ,
     PDPOOLMODELID -- PD�ֳ�ģ��ID
    ,
     LGDPOOLMODELID -- LGD�ֳ�ģ��ID
    ,
     CCFPOOLMODELID -- CCF�ֳ�ģ��ID
    ,
     PDPOOLID -- ����PD��ID
    ,
     LGDPOOLID -- ����LGD��ID
    ,
     CCFPOOLID -- ����CCF��ID
    ,
     ABSUAFLAG -- �ʲ�֤ȯ�������ʲ���ʶ
    ,
     ABSPOOLID -- ֤ȯ���ʲ���ID
    ,
     ABSPROPORTION -- �ʲ�֤ȯ������
    ,
     GROUPID -- ������
    ,
     ORGSORTNO --�������������
    ,
     LTV --�����ֵ��
    ,
     AGING --����
    ,
     NEWDEFAULTDEBTFLAG --����ΥԼծ���ʶ
    ,
     DEFAULTDATE --ΥԼʱ��
     )
    SELECT DATADATE   AS DATADATE -- ��������
          ,
           DATANO     AS DATANO -- ������ˮ��
          ,
           EXPOSUREID AS EXPOSUREID -- ���ձ�¶ID
          ,
           DUEID      AS DUEID -- ծ��ID
          ,
           SSYSID     AS SSYSID -- ԴϵͳID
          ,
           CONTRACTID AS CONTRACTID -- ��ͬID
          ,
           CLIENTID   AS CLIENTID -- ��������ID
          ,
           SORGID     AS SORGID -- Դ����ID
          ,
           SORGNAME   AS SORGNAME -- Դ��������
          ,
           ORGID      AS ORGID -- ��������ID
          ,
           ORGNAME    AS ORGNAME -- ������������
          ,
           ACCORGID   AS ACCORGID -- �������ID
          ,
           ACCORGNAME AS ACCORGNAME -- �����������
          ,
           INDUSTRYID AS INDUSTRYID --������ҵ����
          ,
           NVL       (INDUSTRYNAME, 'δ֪'        �� AS INDUSTRYNAME --������ҵ����
                  , BUSINESSLINE AS BUSINESSLINE -- ����
                  , ASSETTYPE AS ASSETTYPE -- �ʲ�����
                  , ASSETSUBTYPE AS ASSETSUBTYPE -- �ʲ�С��
                  , BUSINESSTYPEID AS BUSINESSTYPEID -- ҵ��Ʒ�ִ���
                  , BUSINESSTYPENAME AS BUSINESSTYPENAME -- ҵ��Ʒ������
                  , CREDITRISKDATATYPE AS CREDITRISKDATATYPE -- ���÷�����������
                  , ASSETTYPEOFHAIRCUTS AS ASSETTYPEOFHAIRCUTS -- �ۿ�ϵ����Ӧ�ʲ����
                  , BUSINESSTYPESTD AS BUSINESSTYPESTD -- Ȩ�ط�ҵ������
                  , EXPOCLASSSTD AS EXPOCLASSSTD -- Ȩ�ط���¶����
                  , EXPOSUBCLASSSTD AS EXPOSUBCLASSSTD -- Ȩ�ط���¶С��
                  , EXPOCLASSIRB AS EXPOCLASSIRB -- ��������¶����
                  , EXPOSUBCLASSIRB AS EXPOSUBCLASSIRB -- ��������¶С��
                  , EXPOBELONG AS EXPOBELONG -- ��¶������ʶ
                  , BOOKTYPE AS BOOKTYPE -- �˻����
                  , REGUTRANTYPE AS REGUTRANTYPE -- ��ܽ�������
                  , REPOTRANFLAG AS REPOTRANFLAG -- �ع����ױ�ʶ
                  , REVAFREQUENCY AS REVAFREQUENCY -- �ع�Ƶ��
                  , CURRENCY AS CURRENCY -- ����
                  , NORMALPRINCIPAL AS NORMALPRINCIPAL -- �����������
                  , OVERDUEBALANCE AS OVERDUEBALANCE -- �������
                  , NONACCRUALBALANCE AS NONACCRUALBALANCE -- ��Ӧ�����
                  , ONSHEETBALANCE AS ONSHEETBALANCE -- �������
                  , NORMALINTEREST AS NORMALINTEREST -- ������Ϣ
                  , ONDEBITINTEREST AS ONDEBITINTEREST -- ����ǷϢ
                  , OFFDEBITINTEREST AS OFFDEBITINTEREST -- ����ǷϢ
                  , EXPENSERECEIVABLE AS EXPENSERECEIVABLE -- Ӧ�շ���
                  , ASSETBALANCE AS ASSETBALANCE -- �ʲ����
                  , ACCSUBJECT1 AS ACCSUBJECT1 -- ��Ŀһ
                  , ACCSUBJECT2 AS ACCSUBJECT2 -- ��Ŀ��
                  , ACCSUBJECT3 AS ACCSUBJECT3 -- ��Ŀ��
                  , TO_CHAR(TO_DATE(STARTDATE, 'YYYY-MM-DD'       ), 'YYYY-MM-DD'       ) AS STARTDATE -- ��ʼ����
                  , TO_CHAR(TO_DATE(DUEDATE, 'YYYY-MM-DD'       ), 'YYYY-MM-DD'       ) AS DUEDATE -- ��������
                  , ORIGINALMATURITY AS ORIGINALMATURITY -- ԭʼ����
                  , RESIDUALM AS RESIDUALM -- ʣ������
                  , RISKCLASSIFY AS RISKCLASSIFY -- ���շ���
                  , EXPOSURESTATUS AS EXPOSURESTATUS -- ���ձ�¶״̬
                  , OVERDUEDAYS AS OVERDUEDAYS -- ��������
                  , SPECIALPROVISION AS SPECIALPROVISION -- ר��׼����
                  , GENERALPROVISION AS GENERALPROVISION -- һ��׼����
                  , ESPECIALPROVISION AS ESPECIALPROVISION -- �ر�׼����
                  , WRITTENOFFAMOUNT AS WRITTENOFFAMOUNT -- �Ѻ������
                  , OFFEXPOSOURCE AS OFFEXPOSOURCE -- ���Ⱪ¶��Դ
                  , OFFBUSINESSTYPE AS OFFBUSINESSTYPE -- ����ҵ������
                  , OFFBUSINESSSDVSSTD AS OFFBUSINESSSDVSSTD -- Ȩ�ط�����ҵ������ϸ��
                  , UNCONDCANCELFLAG AS UNCONDCANCELFLAG -- �Ƿ����ʱ����������
                  , CCFLEVEL AS CCFLEVEL -- ����ת��ϵ������
                  , CCFAIRB AS CCFAIRB -- �߼�������ת��ϵ��
                  , CLAIMSLEVEL AS CLAIMSLEVEL -- ծȨ����
                  , BONDFLAG AS BONDFLAG -- �Ƿ�Ϊծȯ
                  , BONDISSUEINTENT AS BONDISSUEINTENT -- ծȯ����Ŀ��
                  , NSUREALPROPERTYFLAG AS NSUREALPROPERTYFLAG -- �Ƿ�����ò�����
                  , REPASSETTERMTYPE AS REPASSETTERMTYPE -- ��ծ�ʲ���������
                  , DEPENDONFPOBFLAG AS DEPENDONFPOBFLAG -- �Ƿ�����������δ��ӯ��
                  , IRATING AS IRATING -- �ڲ�����
                  , PD AS PD -- ΥԼ����
                  , LGDLEVEL AS LGDLEVEL -- ΥԼ��ʧ�ʼ���
                  , LGDAIRB AS LGDAIRB -- �߼���ΥԼ��ʧ��
                  , MAIRB AS MAIRB -- �߼�����Ч����
                  , EADAIRB AS EADAIRB -- �߼���ΥԼ���ձ�¶
                  , DEFAULTFLAG AS DEFAULTFLAG -- ΥԼ��ʶ
                  , BEEL AS BEEL -- ��ΥԼ��¶Ԥ����ʧ����
                  , DEFAULTLGD AS DEFAULTLGD -- ��ΥԼ��¶ΥԼ��ʧ��
                  , EQUITYEXPOFLAG AS EQUITYEXPOFLAG -- ��Ȩ��¶��ʶ
                  , EQUITYINVESTTYPE AS EQUITYINVESTTYPE -- ��ȨͶ�ʶ�������
                  , EQUITYINVESTCAUSE AS EQUITYINVESTCAUSE -- ��ȨͶ���γ�ԭ��
                  , SLFLAG AS SLFLAG -- רҵ�����ʶ
                  , SLTYPE AS SLTYPE -- רҵ��������
                  , PFPHASE AS PFPHASE -- ��Ŀ���ʽ׶�
                  , REGURATING AS REGURATING -- �������
                  , CBRCMPRATINGFLAG AS CBRCMPRATINGFLAG -- ������϶������Ƿ��Ϊ����
                  , LARGEFLUCFLAG AS LARGEFLUCFLAG -- �Ƿ񲨶��Խϴ�
                  , LIQUEXPOFLAG AS LIQUEXPOFLAG -- �Ƿ���������з��ձ�¶
                  , PAYMENTDEALFLAG AS PAYMENTDEALFLAG -- �Ƿ����Ը�ģʽ
                  , DELAYTRADINGDAYS AS DELAYTRADINGDAYS -- �ӳٽ�������
                  , SECURITIESFLAG AS SECURITIESFLAG -- �м�֤ȯ��ʶ
                  , SECUISSUERID AS SECUISSUERID -- ֤ȯ������ID
                  , RATINGDURATIONTYPE AS RATINGDURATIONTYPE -- ������������
                  , SECUISSUERATING AS SECUISSUERATING -- ֤ȯ���еȼ�
                  , SECURESIDUALM AS SECURESIDUALM -- ֤ȯʣ������
                  , SECUREVAFREQUENCY AS SECUREVAFREQUENCY -- ֤ȯ�ع�Ƶ��
                  , CCPTRANFLAG AS CCPTRANFLAG -- �Ƿ����뽻�׶�����ؽ���
                  , CCPID AS CCPID -- ���뽻�׶���ID
                  , QUALCCPFLAG AS QUALCCPFLAG -- �Ƿ�ϸ����뽻�׶���
                  , BANKROLE AS BANKROLE -- ���н�ɫ
                  , CLEARINGMETHOD AS CLEARINGMETHOD -- ���㷽ʽ
                  , BANKASSETFLAG AS BANKASSETFLAG -- �Ƿ������ύ�ʲ�
                  , MATCHCONDITIONS AS MATCHCONDITIONS -- �����������
                  , SFTFLAG AS SFTFLAG -- ֤ȯ���ʽ��ױ�ʶ
                  , MASTERNETAGREEFLAG AS MASTERNETAGREEFLAG -- ���������Э���ʶ
                  , MASTERNETAGREEID AS MASTERNETAGREEID -- ���������Э��ID
                  , SFTTYPE AS SFTTYPE -- ֤ȯ���ʽ�������
                  , SECUOWNERTRANSFLAG AS SECUOWNERTRANSFLAG -- ֤ȯ����Ȩ�Ƿ�ת��
                  , OTCFLAG AS OTCFLAG -- �����������߱�ʶ
                  , VALIDNETTINGFLAG AS VALIDNETTINGFLAG -- ��Ч�������Э���ʶ
                  , VALIDNETAGREEMENTID AS VALIDNETAGREEMENTID -- ��Ч�������Э��ID
                  , OTCTYPE AS OTCTYPE -- ����������������
                  , DEPOSITRISKPERIOD AS DEPOSITRISKPERIOD -- ��֤������ڼ�
                  , MTM AS MTM -- ���óɱ�
                  , MTMCURRENCY AS MTMCURRENCY -- ���óɱ�����
                  , BUYERORSELLER AS BUYERORSELLER -- ������
                  , QUALROFLAG AS QUALROFLAG -- �ϸ�����ʲ���ʶ
                  , ROISSUERPERFORMFLAG AS ROISSUERPERFORMFLAG -- �����ʲ��������Ƿ�����Լ
                  , BUYERINSOLVENCYFLAG AS BUYERINSOLVENCYFLAG -- ���ñ������Ƿ��Ʋ�
                  , NONPAYMENTFEES AS NONPAYMENTFEES -- ��δ֧������
                  , RETAILEXPOFLAG AS RETAILEXPOFLAG -- ���۱�¶��ʶ
                  , RETAILCLAIMTYPE AS RETAILCLAIMTYPE -- ����ծȨ����
                  , MORTGAGETYPE AS MORTGAGETYPE -- ס����Ѻ��������
                  , DEBTORNUMBER AS DEBTORNUMBER -- ����˸���
                  , EXPONUMBER AS EXPONUMBER -- ���ձ�¶����
                  , PDPOOLMODELID AS PDPOOLMODELID -- PD�ֳ�ģ��ID
                  , LGDPOOLMODELID AS LGDPOOLMODELID -- LGD�ֳ�ģ��ID
                  , CCFPOOLMODELID AS CCFPOOLMODELID -- CCF�ֳ�ģ��ID
                  , PDPOOLID AS PDPOOLID -- ����PD��ID
                  , LGDPOOLID AS LGDPOOLID -- ����LGD��ID
                  , CCFPOOLID AS CCFPOOLID -- ����CCF��ID
                  , ABSUAFLAG AS ABSUAFLAG -- �ʲ�֤ȯ�������ʲ���ʶ
                  , ABSPOOLID AS ABSPOOLID -- ֤ȯ���ʲ���ID
                  , ABSPROPORTION AS ABSPROPORTION -- �ʲ�֤ȯ������
                  , GROUPID AS GROUPID -- ������
                  , ORGSORTNO AS ORGSORTNO --�������������
                  , LTV AS LTV --�����ֵ��
                  , AGING AS AGING --����
                  , NEWDEFAULTDEBTFLAG AS NEWDEFAULTDEBTFLAG --����ΥԼծ���ʶ
                  , DEFAULTDATE AS DEFAULTDATE --ΥԼʱ��
                   FROM RWA_DEV.RWA_YSP_EXPOSURE WHERE DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD'       );
  COMMIT;

  /*�������ѽ���ҵ�����÷��ձ�¶����Ϣ*/
  INSERT /*+ APPEND */
  INTO RWA_DEV.RWA_EI_EXPOSURE
    (DATADATE -- ��������
    ,
     DATANO -- ������ˮ��
    ,
     EXPOSUREID -- ���ձ�¶ID
    ,
     DUEID -- ծ��ID
    ,
     SSYSID -- ԴϵͳID
    ,
     CONTRACTID -- ��ͬID
    ,
     CLIENTID -- ��������ID
    ,
     SORGID -- Դ����ID
    ,
     SORGNAME -- Դ��������
    ,
     ORGID -- ��������ID
    ,
     ORGNAME -- ������������
    ,
     ACCORGID -- �������ID
    ,
     ACCORGNAME -- �����������
    ,
     INDUSTRYID -- ������ҵ����
    ,
     INDUSTRYNAME -- ������ҵ����
    ,
     BUSINESSLINE -- ����
    ,
     ASSETTYPE -- �ʲ�����
    ,
     ASSETSUBTYPE -- �ʲ�С��
    ,
     BUSINESSTYPEID -- ҵ��Ʒ�ִ���
    ,
     BUSINESSTYPENAME -- ҵ��Ʒ������
    ,
     CREDITRISKDATATYPE -- ���÷�����������
    ,
     ASSETTYPEOFHAIRCUTS -- �ۿ�ϵ����Ӧ�ʲ����
    ,
     BUSINESSTYPESTD -- Ȩ�ط�ҵ������
    ,
     EXPOCLASSSTD -- Ȩ�ط���¶����
    ,
     EXPOSUBCLASSSTD -- Ȩ�ط���¶С��
    ,
     EXPOCLASSIRB -- ��������¶����
    ,
     EXPOSUBCLASSIRB -- ��������¶С��
    ,
     EXPOBELONG -- ��¶������ʶ
    ,
     BOOKTYPE -- �˻����
    ,
     REGUTRANTYPE -- ��ܽ�������
    ,
     REPOTRANFLAG -- �ع����ױ�ʶ
    ,
     REVAFREQUENCY -- �ع�Ƶ��
    ,
     CURRENCY -- ����
    ,
     NORMALPRINCIPAL -- �����������
    ,
     OVERDUEBALANCE -- �������
    ,
     NONACCRUALBALANCE -- ��Ӧ�����
    ,
     ONSHEETBALANCE -- �������
    ,
     NORMALINTEREST -- ������Ϣ
    ,
     ONDEBITINTEREST -- ����ǷϢ
    ,
     OFFDEBITINTEREST -- ����ǷϢ
    ,
     EXPENSERECEIVABLE -- Ӧ�շ���
    ,
     ASSETBALANCE -- �ʲ����
    ,
     ACCSUBJECT1 -- ��Ŀһ
    ,
     ACCSUBJECT2 -- ��Ŀ��
    ,
     ACCSUBJECT3 -- ��Ŀ��
    ,
     STARTDATE -- ��ʼ����
    ,
     DUEDATE -- ��������
    ,
     ORIGINALMATURITY -- ԭʼ����
    ,
     RESIDUALM -- ʣ������
    ,
     RISKCLASSIFY -- ���շ���
    ,
     EXPOSURESTATUS -- ���ձ�¶״̬
    ,
     OVERDUEDAYS -- ��������
    ,
     SPECIALPROVISION -- ר��׼����
    ,
     GENERALPROVISION -- һ��׼����
    ,
     ESPECIALPROVISION -- �ر�׼����
    ,
     WRITTENOFFAMOUNT -- �Ѻ������
    ,
     OFFEXPOSOURCE -- ���Ⱪ¶��Դ
    ,
     OFFBUSINESSTYPE -- ����ҵ������
    ,
     OFFBUSINESSSDVSSTD -- Ȩ�ط�����ҵ������ϸ��
    ,
     UNCONDCANCELFLAG -- �Ƿ����ʱ����������
    ,
     CCFLEVEL -- ����ת��ϵ������
    ,
     CCFAIRB -- �߼�������ת��ϵ��
    ,
     CLAIMSLEVEL -- ծȨ����
    ,
     BONDFLAG -- �Ƿ�Ϊծȯ
    ,
     BONDISSUEINTENT -- ծȯ����Ŀ��
    ,
     NSUREALPROPERTYFLAG -- �Ƿ�����ò�����
    ,
     REPASSETTERMTYPE -- ��ծ�ʲ���������
    ,
     DEPENDONFPOBFLAG -- �Ƿ�����������δ��ӯ��
    ,
     IRATING -- �ڲ�����
    ,
     PD -- ΥԼ����
    ,
     LGDLEVEL -- ΥԼ��ʧ�ʼ���
    ,
     LGDAIRB -- �߼���ΥԼ��ʧ��
    ,
     MAIRB -- �߼�����Ч����
    ,
     EADAIRB -- �߼���ΥԼ���ձ�¶
    ,
     DEFAULTFLAG -- ΥԼ��ʶ
    ,
     BEEL -- ��ΥԼ��¶Ԥ����ʧ����
    ,
     DEFAULTLGD -- ��ΥԼ��¶ΥԼ��ʧ��
    ,
     EQUITYEXPOFLAG -- ��Ȩ��¶��ʶ
    ,
     EQUITYINVESTTYPE -- ��ȨͶ�ʶ�������
    ,
     EQUITYINVESTCAUSE -- ��ȨͶ���γ�ԭ��
    ,
     SLFLAG -- רҵ�����ʶ
    ,
     SLTYPE -- רҵ��������
    ,
     PFPHASE -- ��Ŀ���ʽ׶�
    ,
     REGURATING -- �������
    ,
     CBRCMPRATINGFLAG -- ������϶������Ƿ��Ϊ����
    ,
     LARGEFLUCFLAG -- �Ƿ񲨶��Խϴ�
    ,
     LIQUEXPOFLAG -- �Ƿ���������з��ձ�¶
    ,
     PAYMENTDEALFLAG -- �Ƿ����Ը�ģʽ
    ,
     DELAYTRADINGDAYS -- �ӳٽ�������
    ,
     SECURITIESFLAG -- �м�֤ȯ��ʶ
    ,
     SECUISSUERID -- ֤ȯ������ID
    ,
     RATINGDURATIONTYPE -- ������������
    ,
     SECUISSUERATING -- ֤ȯ���еȼ�
    ,
     SECURESIDUALM -- ֤ȯʣ������
    ,
     SECUREVAFREQUENCY -- ֤ȯ�ع�Ƶ��
    ,
     CCPTRANFLAG -- �Ƿ����뽻�׶�����ؽ���
    ,
     CCPID -- ���뽻�׶���ID
    ,
     QUALCCPFLAG -- �Ƿ�ϸ����뽻�׶���
    ,
     BANKROLE -- ���н�ɫ
    ,
     CLEARINGMETHOD -- ���㷽ʽ
    ,
     BANKASSETFLAG -- �Ƿ������ύ�ʲ�
    ,
     MATCHCONDITIONS -- �����������
    ,
     SFTFLAG -- ֤ȯ���ʽ��ױ�ʶ
    ,
     MASTERNETAGREEFLAG -- ���������Э���ʶ
    ,
     MASTERNETAGREEID -- ���������Э��ID
    ,
     SFTTYPE -- ֤ȯ���ʽ�������
    ,
     SECUOWNERTRANSFLAG -- ֤ȯ����Ȩ�Ƿ�ת��
    ,
     OTCFLAG -- �����������߱�ʶ
    ,
     VALIDNETTINGFLAG -- ��Ч�������Э���ʶ
    ,
     VALIDNETAGREEMENTID -- ��Ч�������Э��ID
    ,
     OTCTYPE -- ����������������
    ,
     DEPOSITRISKPERIOD -- ��֤������ڼ�
    ,
     MTM -- ���óɱ�
    ,
     MTMCURRENCY -- ���óɱ�����
    ,
     BUYERORSELLER -- ������
    ,
     QUALROFLAG -- �ϸ�����ʲ���ʶ
    ,
     ROISSUERPERFORMFLAG -- �����ʲ��������Ƿ�����Լ
    ,
     BUYERINSOLVENCYFLAG -- ���ñ������Ƿ��Ʋ�
    ,
     NONPAYMENTFEES -- ��δ֧������
    ,
     RETAILEXPOFLAG -- ���۱�¶��ʶ
    ,
     RETAILCLAIMTYPE -- ����ծȨ����
    ,
     MORTGAGETYPE -- ס����Ѻ��������
    ,
     DEBTORNUMBER -- ����˸���
    ,
     EXPONUMBER -- ���ձ�¶����
    ,
     PDPOOLMODELID -- PD�ֳ�ģ��ID
    ,
     LGDPOOLMODELID -- LGD�ֳ�ģ��ID
    ,
     CCFPOOLMODELID -- CCF�ֳ�ģ��ID
    ,
     PDPOOLID -- ����PD��ID
    ,
     LGDPOOLID -- ����LGD��ID
    ,
     CCFPOOLID -- ����CCF��ID
    ,
     ABSUAFLAG -- �ʲ�֤ȯ�������ʲ���ʶ
    ,
     ABSPOOLID -- ֤ȯ���ʲ���ID
    ,
     ABSPROPORTION -- �ʲ�֤ȯ������
    ,
     GROUPID -- ������
    ,
     ORGSORTNO --�������������
    ,
     LTV --�����ֵ��
    ,
     AGING --����
    ,
     NEWDEFAULTDEBTFLAG --����ΥԼծ���ʶ
    ,
     DEFAULTDATE --ΥԼʱ��
     )
    SELECT DATADATE   AS DATADATE -- ��������
          ,
           DATANO     AS DATANO -- ������ˮ��
          ,
           EXPOSUREID AS EXPOSUREID -- ���ձ�¶ID
          ,
           DUEID      AS DUEID -- ծ��ID
          ,
           SSYSID     AS SSYSID -- ԴϵͳID
          ,
           CONTRACTID AS CONTRACTID -- ��ͬID
          ,
           CLIENTID   AS CLIENTID -- ��������ID
          ,
           SORGID     AS SORGID -- Դ����ID
          ,
           SORGNAME   AS SORGNAME -- Դ��������
          ,
           ORGID      AS ORGID -- ��������ID
          ,
           ORGNAME    AS ORGNAME -- ������������
          ,
           ACCORGID   AS ACCORGID -- �������ID
          ,
           ACCORGNAME AS ACCORGNAME -- �����������
          ,
           INDUSTRYID AS INDUSTRYID --������ҵ����
          ,
           NVL       (INDUSTRYNAME, 'δ֪'        �� AS INDUSTRYNAME --������ҵ����
                  , BUSINESSLINE AS BUSINESSLINE -- ����
                  , ASSETTYPE AS ASSETTYPE -- �ʲ�����
                  , ASSETSUBTYPE AS ASSETSUBTYPE -- �ʲ�С��
                  , BUSINESSTYPEID AS BUSINESSTYPEID -- ҵ��Ʒ�ִ���
                  , BUSINESSTYPENAME AS BUSINESSTYPENAME -- ҵ��Ʒ������
                  , CREDITRISKDATATYPE AS CREDITRISKDATATYPE -- ���÷�����������
                  , ASSETTYPEOFHAIRCUTS AS ASSETTYPEOFHAIRCUTS -- �ۿ�ϵ����Ӧ�ʲ����
                  , BUSINESSTYPESTD AS BUSINESSTYPESTD -- Ȩ�ط�ҵ������
                  , EXPOCLASSSTD AS EXPOCLASSSTD -- Ȩ�ط���¶����
                  , EXPOSUBCLASSSTD AS EXPOSUBCLASSSTD -- Ȩ�ط���¶С��
                  , EXPOCLASSIRB AS EXPOCLASSIRB -- ��������¶����
                  , EXPOSUBCLASSIRB AS EXPOSUBCLASSIRB -- ��������¶С��
                  , EXPOBELONG AS EXPOBELONG -- ��¶������ʶ
                  , BOOKTYPE AS BOOKTYPE -- �˻����
                  , REGUTRANTYPE AS REGUTRANTYPE -- ��ܽ�������
                  , REPOTRANFLAG AS REPOTRANFLAG -- �ع����ױ�ʶ
                  , REVAFREQUENCY AS REVAFREQUENCY -- �ع�Ƶ��
                  , CURRENCY AS CURRENCY -- ����
                  , NORMALPRINCIPAL AS NORMALPRINCIPAL -- �����������
                  , OVERDUEBALANCE AS OVERDUEBALANCE -- �������
                  , NONACCRUALBALANCE AS NONACCRUALBALANCE -- ��Ӧ�����
                  , ONSHEETBALANCE AS ONSHEETBALANCE -- �������
                  , NORMALINTEREST AS NORMALINTEREST -- ������Ϣ
                  , ONDEBITINTEREST AS ONDEBITINTEREST -- ����ǷϢ
                  , OFFDEBITINTEREST AS OFFDEBITINTEREST -- ����ǷϢ
                  , EXPENSERECEIVABLE AS EXPENSERECEIVABLE -- Ӧ�շ���
                  , ASSETBALANCE AS ASSETBALANCE -- �ʲ����
                  , ACCSUBJECT1 AS ACCSUBJECT1 -- ��Ŀһ
                  , ACCSUBJECT2 AS ACCSUBJECT2 -- ��Ŀ��
                  , ACCSUBJECT3 AS ACCSUBJECT3 -- ��Ŀ��
                  , TO_CHAR(TO_DATE(STARTDATE, 'YYYY-MM-DD'       ), 'YYYY-MM-DD'       ) AS STARTDATE -- ��ʼ����
                  , TO_CHAR(TO_DATE(DUEDATE, 'YYYY-MM-DD'       ), 'YYYY-MM-DD'       ) AS DUEDATE -- ��������
                  , ORIGINALMATURITY AS ORIGINALMATURITY -- ԭʼ����
                  , RESIDUALM AS RESIDUALM -- ʣ������
                  , RISKCLASSIFY AS RISKCLASSIFY -- ���շ���
                  , EXPOSURESTATUS AS EXPOSURESTATUS -- ���ձ�¶״̬
                  , OVERDUEDAYS AS OVERDUEDAYS -- ��������
                  , SPECIALPROVISION AS SPECIALPROVISION -- ר��׼����
                  , GENERALPROVISION AS GENERALPROVISION -- һ��׼����
                  , ESPECIALPROVISION AS ESPECIALPROVISION -- �ر�׼����
                  , WRITTENOFFAMOUNT AS WRITTENOFFAMOUNT -- �Ѻ������
                  , OFFEXPOSOURCE AS OFFEXPOSOURCE -- ���Ⱪ¶��Դ
                  , OFFBUSINESSTYPE AS OFFBUSINESSTYPE -- ����ҵ������
                  , OFFBUSINESSSDVSSTD AS OFFBUSINESSSDVSSTD -- Ȩ�ط�����ҵ������ϸ��
                  , UNCONDCANCELFLAG AS UNCONDCANCELFLAG -- �Ƿ����ʱ����������
                  , CCFLEVEL AS CCFLEVEL -- ����ת��ϵ������
                  , CCFAIRB AS CCFAIRB -- �߼�������ת��ϵ��
                  , CLAIMSLEVEL AS CLAIMSLEVEL -- ծȨ����
                  , BONDFLAG AS BONDFLAG -- �Ƿ�Ϊծȯ
                  , BONDISSUEINTENT AS BONDISSUEINTENT -- ծȯ����Ŀ��
                  , NSUREALPROPERTYFLAG AS NSUREALPROPERTYFLAG -- �Ƿ�����ò�����
                  , REPASSETTERMTYPE AS REPASSETTERMTYPE -- ��ծ�ʲ���������
                  , DEPENDONFPOBFLAG AS DEPENDONFPOBFLAG -- �Ƿ�����������δ��ӯ��
                  , IRATING AS IRATING -- �ڲ�����
                  , PD AS PD -- ΥԼ����
                  , LGDLEVEL AS LGDLEVEL -- ΥԼ��ʧ�ʼ���
                  , LGDAIRB AS LGDAIRB -- �߼���ΥԼ��ʧ��
                  , MAIRB AS MAIRB -- �߼�����Ч����
                  , EADAIRB AS EADAIRB -- �߼���ΥԼ���ձ�¶
                  , DEFAULTFLAG AS DEFAULTFLAG -- ΥԼ��ʶ
                  , BEEL AS BEEL -- ��ΥԼ��¶Ԥ����ʧ����
                  , DEFAULTLGD AS DEFAULTLGD -- ��ΥԼ��¶ΥԼ��ʧ��
                  , EQUITYEXPOFLAG AS EQUITYEXPOFLAG -- ��Ȩ��¶��ʶ
                  , EQUITYINVESTTYPE AS EQUITYINVESTTYPE -- ��ȨͶ�ʶ�������
                  , EQUITYINVESTCAUSE AS EQUITYINVESTCAUSE -- ��ȨͶ���γ�ԭ��
                  , SLFLAG AS SLFLAG -- רҵ�����ʶ
                  , SLTYPE AS SLTYPE -- רҵ��������
                  , PFPHASE AS PFPHASE -- ��Ŀ���ʽ׶�
                  , REGURATING AS REGURATING -- �������
                  , CBRCMPRATINGFLAG AS CBRCMPRATINGFLAG -- ������϶������Ƿ��Ϊ����
                  , LARGEFLUCFLAG AS LARGEFLUCFLAG -- �Ƿ񲨶��Խϴ�
                  , LIQUEXPOFLAG AS LIQUEXPOFLAG -- �Ƿ���������з��ձ�¶
                  , PAYMENTDEALFLAG AS PAYMENTDEALFLAG -- �Ƿ����Ը�ģʽ
                  , DELAYTRADINGDAYS AS DELAYTRADINGDAYS -- �ӳٽ�������
                  , SECURITIESFLAG AS SECURITIESFLAG -- �м�֤ȯ��ʶ
                  , SECUISSUERID AS SECUISSUERID -- ֤ȯ������ID
                  , RATINGDURATIONTYPE AS RATINGDURATIONTYPE -- ������������
                  , SECUISSUERATING AS SECUISSUERATING -- ֤ȯ���еȼ�
                  , SECURESIDUALM AS SECURESIDUALM -- ֤ȯʣ������
                  , SECUREVAFREQUENCY AS SECUREVAFREQUENCY -- ֤ȯ�ع�Ƶ��
                  , CCPTRANFLAG AS CCPTRANFLAG -- �Ƿ����뽻�׶�����ؽ���
                  , CCPID AS CCPID -- ���뽻�׶���ID
                  , QUALCCPFLAG AS QUALCCPFLAG -- �Ƿ�ϸ����뽻�׶���
                  , BANKROLE AS BANKROLE -- ���н�ɫ
                  , CLEARINGMETHOD AS CLEARINGMETHOD -- ���㷽ʽ
                  , BANKASSETFLAG AS BANKASSETFLAG -- �Ƿ������ύ�ʲ�
                  , MATCHCONDITIONS AS MATCHCONDITIONS -- �����������
                  , SFTFLAG AS SFTFLAG -- ֤ȯ���ʽ��ױ�ʶ
                  , MASTERNETAGREEFLAG AS MASTERNETAGREEFLAG -- ���������Э���ʶ
                  , MASTERNETAGREEID AS MASTERNETAGREEID -- ���������Э��ID
                  , SFTTYPE AS SFTTYPE -- ֤ȯ���ʽ�������
                  , SECUOWNERTRANSFLAG AS SECUOWNERTRANSFLAG -- ֤ȯ����Ȩ�Ƿ�ת��
                  , OTCFLAG AS OTCFLAG -- �����������߱�ʶ
                  , VALIDNETTINGFLAG AS VALIDNETTINGFLAG -- ��Ч�������Э���ʶ
                  , VALIDNETAGREEMENTID AS VALIDNETAGREEMENTID -- ��Ч�������Э��ID
                  , OTCTYPE AS OTCTYPE -- ����������������
                  , DEPOSITRISKPERIOD AS DEPOSITRISKPERIOD -- ��֤������ڼ�
                  , MTM AS MTM -- ���óɱ�
                  , MTMCURRENCY AS MTMCURRENCY -- ���óɱ�����
                  , BUYERORSELLER AS BUYERORSELLER -- ������
                  , QUALROFLAG AS QUALROFLAG -- �ϸ�����ʲ���ʶ
                  , ROISSUERPERFORMFLAG AS ROISSUERPERFORMFLAG -- �����ʲ��������Ƿ�����Լ
                  , BUYERINSOLVENCYFLAG AS BUYERINSOLVENCYFLAG -- ���ñ������Ƿ��Ʋ�
                  , NONPAYMENTFEES AS NONPAYMENTFEES -- ��δ֧������
                  , RETAILEXPOFLAG AS RETAILEXPOFLAG -- ���۱�¶��ʶ
                  , RETAILCLAIMTYPE AS RETAILCLAIMTYPE -- ����ծȨ����
                  , MORTGAGETYPE AS MORTGAGETYPE -- ס����Ѻ��������
                  , DEBTORNUMBER AS DEBTORNUMBER -- ����˸���
                  , EXPONUMBER AS EXPONUMBER -- ���ձ�¶����
                  , PDPOOLMODELID AS PDPOOLMODELID -- PD�ֳ�ģ��ID
                  , LGDPOOLMODELID AS LGDPOOLMODELID -- LGD�ֳ�ģ��ID
                  , CCFPOOLMODELID AS CCFPOOLMODELID -- CCF�ֳ�ģ��ID
                  , PDPOOLID AS PDPOOLID -- ����PD��ID
                  , LGDPOOLID AS LGDPOOLID -- ����LGD��ID
                  , CCFPOOLID AS CCFPOOLID -- ����CCF��ID
                  , ABSUAFLAG AS ABSUAFLAG -- �ʲ�֤ȯ�������ʲ���ʶ
                  , ABSPOOLID AS ABSPOOLID -- ֤ȯ���ʲ���ID
                  , ABSPROPORTION AS ABSPROPORTION -- �ʲ�֤ȯ������
                  , GROUPID AS GROUPID -- ������
                  , ORGSORTNO AS ORGSORTNO --�������������
                  , LTV AS LTV --�����ֵ��
                  , AGING AS AGING --����
                  , NEWDEFAULTDEBTFLAG AS NEWDEFAULTDEBTFLAG --����ΥԼծ���ʶ
                  , DEFAULTDATE AS DEFAULTDATE --ΥԼʱ��
                   FROM RWA_DEV.RWA_XF_EXPOSURE WHERE DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD'       );
  COMMIT;

  --������ֵ��Ϣ
  /*UPDATE RWA_EI_EXPOSURE T1
     SET T1.GENERALPROVISION =
         (SELECT FINAL_ECL
            FROM SYS_IFRS9_RESULT T2
           WHERE T1.DATANO = T2.DATANO
             AND DECODE(T1.SSYSID, 'XYK', T1.EXPOSUREID, T1.DUEID) =
                 DECODE(T2.ITEM_CODE,
                        '���ÿ�����',
                        'BW_' || T2.CONTRACT_REFERENCE,
                        T2.CONTRACT_REFERENCE))
   WHERE T1.DATANO = p_data_dt_str
     AND EXISTS (SELECT 1
            FROM SYS_IFRS9_RESULT I
           WHERE I.DATANO = T1.DATANO
             AND I.CONTRACT_REFERENCE = T1.DUEID);
  COMMIT;*/

  EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_EI_EXPOSURE_TMP';
  INSERT /*+ APPEND */
  INTO RWA_EI_EXPOSURE_TMP
    SELECT T1.DATADATE,
           T1.DATANO,
           T1.EXPOSUREID,
           T1.DUEID,
           T1.SSYSID,
           T1.CONTRACTID,
           T1.CLIENTID,
           T1.SORGID,
           T1.SORGNAME,
           T1.ORGSORTNO,
           T1.ORGID,
           T1.ORGNAME,
           T1.ACCORGID,
           T1.ACCORGNAME,
           T1.INDUSTRYID,
           T1.INDUSTRYNAME,
           T1.BUSINESSLINE,
           T1.ASSETTYPE,
           T1.ASSETSUBTYPE,
           T1.BUSINESSTYPEID,
           T1.BUSINESSTYPENAME,
           T1.CREDITRISKDATATYPE,
           T1.ASSETTYPEOFHAIRCUTS,
           T1.BUSINESSTYPESTD,
           T1.EXPOCLASSSTD,
           T1.EXPOSUBCLASSSTD,
           T1.EXPOCLASSIRB,
           T1.EXPOSUBCLASSIRB,
           T1.EXPOBELONG,
           T1.BOOKTYPE,
           T1.REGUTRANTYPE,
           T1.REPOTRANFLAG,
           T1.REVAFREQUENCY,
           T1.CURRENCY,
           T1.NORMALPRINCIPAL,
           T1.OVERDUEBALANCE,
           T1.NONACCRUALBALANCE,
           T1.ONSHEETBALANCE,
           T1.NORMALINTEREST,
           T1.ONDEBITINTEREST,
           T1.OFFDEBITINTEREST,
           T1.EXPENSERECEIVABLE,
           T1.ASSETBALANCE,
           T1.ACCSUBJECT1,
           T1.ACCSUBJECT2,
           T1.ACCSUBJECT3,
           T1.STARTDATE,
           T1.DUEDATE,
           T1.ORIGINALMATURITY,
           T1.RESIDUALM,
           T1.RISKCLASSIFY,
           T1.EXPOSURESTATUS,
           T1.OVERDUEDAYS,
           T1.SPECIALPROVISION,
           CASE
             when ssysid = 'BL' or accsubject1 like '1503%' OR accsubject1 like '1301%'   --�������ʲ������Ƽ�ֵ�����ο�ȷ�� modify by YSJ
               THEN
                 NVL(T1.GENERALPROVISION, 0)
               ELSE
                 NVL(T2.FINAL_ECL, 0)
           END, --һ��׼���� 
           T1.ESPECIALPROVISION,
           T1.WRITTENOFFAMOUNT,
           T1.OFFEXPOSOURCE,
           T1.OFFBUSINESSTYPE,
           T1.OFFBUSINESSSDVSSTD,
           T1.UNCONDCANCELFLAG,
           T1.CCFLEVEL,
           T1.CCFAIRB,
           T1.CLAIMSLEVEL,
           T1.BONDFLAG,
           T1.BONDISSUEINTENT,
           T1.NSUREALPROPERTYFLAG,
           T1.REPASSETTERMTYPE,
           T1.DEPENDONFPOBFLAG,
           T1.IRATING,
           T1.PD,
           T1.LGDLEVEL,
           T1.LGDAIRB,
           T1.MAIRB,
           T1.EADAIRB,
           T1.DEFAULTFLAG,
           T1.BEEL,
           T1.DEFAULTLGD,
           T1.EQUITYEXPOFLAG,
           T1.EQUITYINVESTTYPE,
           T1.EQUITYINVESTCAUSE,
           T1.SLFLAG,
           T1.SLTYPE,
           T1.PFPHASE,
           T1.REGURATING,
           T1.CBRCMPRATINGFLAG,
           T1.LARGEFLUCFLAG,
           T1.LIQUEXPOFLAG,
           T1.PAYMENTDEALFLAG,
           T1.DELAYTRADINGDAYS,
           T1.SECURITIESFLAG,
           T1.SECUISSUERID,
           T1.RATINGDURATIONTYPE,
           T1.SECUISSUERATING,
           T1.SECURESIDUALM,
           T1.SECUREVAFREQUENCY,
           T1.CCPTRANFLAG,
           T1.CCPID,
           T1.QUALCCPFLAG,
           T1.BANKROLE,
           T1.CLEARINGMETHOD,
           T1.BANKASSETFLAG,
           T1.MATCHCONDITIONS,
           T1.SFTFLAG,
           T1.MASTERNETAGREEFLAG,
           T1.MASTERNETAGREEID,
           T1.SFTTYPE,
           T1.SECUOWNERTRANSFLAG,
           T1.OTCFLAG,
           T1.VALIDNETTINGFLAG,
           T1.VALIDNETAGREEMENTID,
           T1.OTCTYPE,
           T1.DEPOSITRISKPERIOD,
           T1.MTM,
           T1.MTMCURRENCY,
           T1.BUYERORSELLER,
           T1.QUALROFLAG,
           T1.ROISSUERPERFORMFLAG,
           T1.BUYERINSOLVENCYFLAG,
           T1.NONPAYMENTFEES,
           T1.RETAILEXPOFLAG,
           T1.RETAILCLAIMTYPE,
           T1.MORTGAGETYPE,
           T1.DEBTORNUMBER,
           T1.EXPONUMBER,
           T1.PDPOOLMODELID,
           T1.LGDPOOLMODELID,
           T1.CCFPOOLMODELID,
           T1.PDPOOLID,
           T1.LGDPOOLID,
           T1.CCFPOOLID,
           T1.ABSUAFLAG,
           T1.ABSPOOLID,
           T1.ABSPROPORTION,
           T1.GROUPID,
           T1.AGING,
           T1.LTV,
           T1.NEWDEFAULTDEBTFLAG,
           T1.DEFAULTDATE
      FROM RWA_EI_EXPOSURE T1
      LEFT JOIN SYS_IFRS9_RESULT T2
        ON T1.DATANO = T2.DATANO
       AND DECODE(T1.SSYSID, 'XYK', T1.EXPOSUREID, T1.DUEID) =
           DECODE(T2.ITEM_CODE,
                  '���ÿ�����',
                  'BW_' || T2.CONTRACT_REFERENCE,
                  T2.CONTRACT_REFERENCE)
     WHERE T1.DATANO = P_DATA_DT_STR;

  COMMIT;

  EXECUTE IMMEDIATE 'ALTER TABLE RWA_EI_EXPOSURE truncate PARTITION EXPOSURE' ||
                    P_DATA_DT_STR;
  INSERT /*+ APPEND */
  INTO RWA_EI_EXPOSURE
    SELECT * FROM RWA_EI_EXPOSURE_TMP;

  COMMIT;

  /*  �������������ʱ�ȸ�Ϊ1  
      BY WANGZEBO 
      20191119 Ϊ�˷�ֹ��������ȫ�����ǣ�����İѻ�������Ÿ���
  */
  update rwa_ei_exposure
     set orgsortno = '1',
         ORGID     = '9998',
         SORGID    = '9998',
         ACCORGID  = '9998',
         ORGNAME   = '���⴦��'
   where ORGID IS NULL
     AND datano = P_DATA_DT_STR;
  COMMIT;

  --�������Ϣ
  dbms_stats.gather_table_stats(ownname => 'RWA_DEV',
                                tabname => 'RWA_EI_EXPOSURE',
                                cascade => true);

  /* --��ʱ�ÿ���Ϣ����Ϣ����  ��ϵͳ�Ѵ����������������
  UPDATE RWA_DEV.RWA_EI_EXPOSURE T SET T.NORMALINTEREST = 0,T.ONDEBITINTEREST = 0,T.OFFDEBITINTEREST =0,T.EXPENSERECEIVABLE = 0,T.ASSETBALANCE = T.ONSHEETBALANCE WHERE T.DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD');
  
  COMMIT;
  
  --����EADIRB���ų���Ϣ������ҵ��Ҫ����ccf
  UPDATE RWA_DEV.RWA_EI_EXPOSURE T SET T.EADAIRB = T.ONSHEETBALANCE * (CASE WHEN T.EXPOBELONG = '01' THEN 1 ELSE NVL(T.CCFAIRB,1) END) WHERE T.DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD') AND T.CREDITRISKDATATYPE = '02';
  
  COMMIT; */

  /*Ŀ�������ͳ��*/
  --ͳ�Ʋ���ļ�¼
  SELECT COUNT(1)
    INTO v_count
    FROM RWA_DEV.RWA_EI_EXPOSURE
   WHERE DATADATE = TO_DATE(P_DATA_DT_STR, 'YYYYMMDD');

  p_po_rtncode := '1';
  p_po_rtnmsg  := '�ɹ�' || '-' || v_count;
  --�����쳣
EXCEPTION
  WHEN OTHERS THEN
    --Dbms_output.Put_line('������,�������Ϊ:'||sqlcode||';������ϢΪ:'||sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace);
    ROLLBACK;
    p_po_rtncode := sqlcode;
    p_po_rtnmsg  := '���ܷ��ձ�¶��(PRO_RWA_EI_EXPOSURE)ETLת��ʧ�ܣ�' || sqlerrm ||
                    ';��������Ϊ:' || dbms_utility.format_error_backtrace;
    RETURN;
END PRO_RWA_EI_EXPOSURE;
/

