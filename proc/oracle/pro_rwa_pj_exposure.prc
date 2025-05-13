CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_PJ_EXPOSURE(
			 											p_data_dt_str	IN	VARCHAR2,		--�������� yyyyMMdd
       											p_po_rtncode	OUT	VARCHAR2,		--���ر�� 1 �ɹ�,0 ʧ��
														p_po_rtnmsg		OUT	VARCHAR2		--��������
				)
  /*
    �洢��������:RWA_DEV.PRO_RWA_PJ_EXPOSURE
    ʵ�ֹ���:����ϵͳ-Ʊ��ת����-���÷��ձ�¶(������Դ����ϵͳ��ҵ�������Ϣȫ������RWAƱ��ת���ֽӿڱ���ձ�¶����)
    ���ݿھ�:ȫ��
    ����Ƶ��:�³�����
    ��  ��  :V1.0.0
    ��д��  :LISY
    ��дʱ��:2016-04-21
    ��  λ  :�Ϻ���˶��Ϣ�����ɷ����޹�˾
    Դ  ��1 :RWA_DEV.BRD_BILL|Ʊ����Ϣ
    Դ  ��2 :RWA.ORG_INFO|������Ϣ��
    Դ	 ��3 :RWA.CODE_LIBRARY|RWA���
    Դ  ��4 :RWA_DEV.IRS_CR_CUSTOMER_RATE
    Դ  ��5 :RWA_DEV.NCM_BREAKDEFINEDREMARK
    
    Ŀ���1 :RWA_DEV.RWA_PJ_EXPOSURE|Ʊ�����������÷��ձ�¶��
    �����¼(�޸���|�޸�ʱ��|�޸�����):
    pxl 2019/04/16 ȥ����¼���ݡ�����������ر�
    chengang 2019/04/23 ����RWA_DEV.BRD_BILL|Ʊ����Ϣ�Ļ�����
    */
  AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_PJ_EXPOSURE';
  --�����쳣����
  v_raise EXCEPTION;
  --���嵱ǰ����ļ�¼��
  v_count INTEGER;

  BEGIN
    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*�����ȫ�����ݼ��������Ŀ���*/
    --1.���Ŀ����е�ԭ�м�¼
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_PJ_EXPOSURE';
    

    --2.���������������ݴ�Դ����뵽Ŀ�����  �ⲿת����-��Ʊ,��Ʊ
    INSERT INTO RWA_DEV.RWA_PJ_EXPOSURE(
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
               ,DefaultDate                     --ΥԼʱ��
               ,ABSPROPORTION                   --�ʲ�֤ȯ������
               ,DEBTORNUMBER                    --����˸���
    )
    SELECT
          TO_DATE(p_data_dt_str,'YYYYMMDD') , --��������        
          p_data_dt_str , --������ˮ��       
          T1.ACCT_NO  , --���ձ�¶ID        
          T1.ACCT_NO  , --ծ��ID        
          'PJ'  , --ԴϵͳID       
          T1.ACCT_NO  , --��ͬID        
          CASE WHEN T1.SBJT_CD='13010511' THEN substr(T1.BILL_NO,2,12)   --�������תȡ�ж��У��ж���ͨ��Ʊ�Ź��������б��ȡ�ж�������
               ELSE T3.CUSTOMERID  
          END AS CLIENTID,  --��������ID        
          T1.ORG_CD , --Դ����ID       
          T2.ORGNAME  , --Դ��������       
          T2.SORTNO , --�������������       
          T1.ORG_CD , --��������ID        
          T2.ORGNAME  , --������������        
          T1.ORG_CD , --�������ID        
          T2.ORGNAME  , --�����������        
          NVL(T3.INDUSTRYTYPE,'J6621') , --������ҵ����        
          T4.ITEMNAME , --������ҵ����        
          '0401'  , --ҵ������        Ĭ�ϣ�ͬҵ(04)  0401:ͬҵ-�����г���
          ''  , --�ʲ�����        RWAӳ����򣬴�����
          ''  , --�ʲ�С��        RWAӳ����򣬴�����
          CASE WHEN SUBSTR(T1.SBJT_CD, 1, 6) = '130101' THEN '10302020' --��������
                     WHEN SUBSTR(T1.SBJT_CD, 1, 6) = '130103' THEN '10303010'--ת����
                       WHEN SUBSTR(T1.SBJT_CD, 1, 6) = '130105' THEN '10303011'--��ת���������ڣ�
                       WHEN SUBSTR(T1.SBJT_CD, 1, 6) = '130102' THEN '10302015'   --- ��ҵ��Ʊ����
                     ELSE 'δ֪' END  , --ҵ��Ʒ�ִ���        
          CASE WHEN SUBSTR(T1.SBJT_CD, 1, 6) = '130101' THEN '���л�Ʊ����' 
               WHEN SUBSTR(T1.SBJT_CD, 1, 6) = '130103' THEN '���л�Ʊת����ҵ��'
               WHEN SUBSTR(T1.SBJT_CD, 1, 6) = '130105' THEN '��ת-���л�Ʊת����'
               WHEN SUBSTR(T1.SBJT_CD, 1, 6) = '130102' THEN '��ҵ��Ʊ����'
               ELSE 'δ֪' END, --ҵ��Ʒ������       
          '01'  , --���÷�����������        Ĭ�ϣ�һ�������(01)
          '01'  , --�ۿ�ϵ����Ӧ�ʲ����        Ĭ��: �ֽ��ֽ�ȼ���(01)
          '07' , --Ȩ�ط�ҵ������       07:һ���ʲ�
          ''  , --Ȩ�ط���¶����       RWA����ӳ��
          ''  , --Ȩ�ط���¶С��       RWA����ӳ��
          ''  , --��������¶����       
          ''  , --��������¶С��       
          '01'  , --��¶������ʶ        Ĭ�ϣ�����(01)
          '01'  , --�˻����        Ĭ�ϣ�01-�����˻�
          '03'  , --��ܽ�������        Ĭ�ϣ���Ѻ����(03)
          '0' , --�ع����ױ�ʶ        Ĭ�ϣ���(0)
          1   , --�ع�Ƶ��        Ĭ�ϣ� 1
          NVL(T1.CCY_CD,'CNY') , --����        
          NVL(T1.ATL_PAY_AMT, 0)  , --�����������        
          0 , --�������        Ĭ�ϣ�0
          0 , --��Ӧ�����       Ĭ�ϣ�0
          NVL(T1.ATL_PAY_AMT, 0)  , --�������        �������=�����������+�������+��Ӧ�����
          0, --������Ϣ        ������Ϣ=����ǷϢ+����ǷϢ
          0 , --����ǷϢ        Ĭ�ϣ�0
          0 , --����ǷϢ        Ĭ�ϣ�0
          0 , --Ӧ�շ���        Ĭ�ϣ�0
          NVL(T1.ATL_PAY_AMT, 0)  , --�ʲ����        �����ʲ������=�������+Ӧ�շ���+������Ϣ+����ǷϢ, ����֤ȯ���ʽ��ף���Ϊ֤ȯ��ֵ��ع����
          --NVL(T1.ATL_PAY_AMT, 0)+NVL(T1.INTR_AMT, 0),  ---20191010 WZB ������Ϣ
          T1.SBJT_CD  , --��Ŀһ       
          ''  , --��Ŀ��       Ĭ�ϣ� ��
          ''  , --��Ŀ��       Ĭ�ϣ� ��
          CASE  WHEN SUBSTR(T1.SBJT_CD, 1, 6) = '130103' 
                THEN T1.DISC_DT--ת���� ������
          ELSE T1.ISSUE_DT--��ת���������ڣ�--�������� ��ʼ��
					   END , --��ʼ����        
          T1.MATU_DT  , --��������        
          CASE WHEN (TO_DATE(T1.MATU_DT,'YYYY-MM-DD') - 
                TO_DATE(DECODE(SUBSTR(T1.SBJT_CD, 1, 6),'130103',T1.DISC_DT,'130102',T1.ISSUE_DT,'130101',T1.ISSUE_DT,TO_CHAR(TO_DATE(T1.MATU_DT,'YYYY-MM-DD')-100,'YYYY-MM-DD')),'YYYY-MM-DD')) / 365<0
                                THEN 0
                                ELSE (TO_DATE(T1.MATU_DT,'YYYY-MM-DD') - 
                TO_DATE(DECODE(SUBSTR(T1.SBJT_CD, 1, 6),'130103',T1.DISC_DT,'130102',T1.ISSUE_DT,'130101',T1.ISSUE_DT,TO_CHAR(TO_DATE(T1.MATU_DT,'YYYY-MM-DD')-100,'YYYY-MM-DD')),'YYYY-MM-DD')) / 365
                          END , --��ת���������ϣ�  --ԭʼ����        ��λ����
          CASE WHEN (TO_DATE(T1.MATU_DT,'YYYY-MM-DD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365<0
                      THEN 0
                        ELSE (TO_DATE(T1.MATU_DT,'YYYY-MM-DD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365
                  END  , --ʣ������        ��λ����
          '01'  , --���շ���        Ĭ�ϣ�����(01)
          '01'  , --���ձ�¶״̬        Ĭ�ϣ�����(01)
          ''  , --��������        
          0 , --ר��׼����       RWAӳ�����
          0 , --һ��׼����       �����䣬��I9��ȡ
          0 , --�ر�׼����       RWAӳ�����
          0 , --�Ѻ������       
          ''  , --���Ⱪ¶��Դ        
          ''  , --����ҵ������        
          ''  , --Ȩ�ط�����ҵ������ϸ��       
          ''  , --�Ƿ����ʱ����������        
          ''  , --����ת��ϵ������        
          NULL  , --�߼�������ת��ϵ��       
          '01'  , --ծȨ����     01:�߼�ծȨ       
          '0' , --�Ƿ�Ϊծȯ       
          '02'  , --ծȯ����Ŀ��  02:����      
          '0' , --�Ƿ�����ò�����        
          ''  , --��ծ�ʲ���������        
          '0' , --�Ƿ�����������δ��ӯ��       
          T5.PDADJLEVEL , --�ڲ�����        ���׶��ֵ��ڲ�����-�ͻ�����
          T5.PD , --ΥԼ����        ���׶��ֵ��ڲ�����-�ͻ�����
          NULL  , --ΥԼ��ʧ�ʼ���       
          NULL  , --�߼���ΥԼ��ʧ��        
          NULL  , --�߼�����Ч����       
          NULL  , --�߼���ΥԼ���ձ�¶       
          CASE WHEN T6.BREAKDATE IS NOT NULL THEN '1' ELSE '0' END  , --ΥԼ��ʶ        
          ''  , --��ΥԼ��¶Ԥ����ʧ����       
          ''  , --��ΥԼ��¶ΥԼ��ʧ��        
          '0' , --��Ȩ��¶��ʶ        
          ''  , --��ȨͶ�ʶ�������        
          ''  , --��ȨͶ���γ�ԭ��        
          '0' , --רҵ�����ʶ        
          ''  , --רҵ��������        
          ''  , --��Ŀ���ʽ׶�        
          '01'  , --�������    01:��     
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
          ''  , --������        RWA����
          CASE WHEN T5.PDADJLEVEL = '0116' THEN TO_DATE(T5.PDVAVLIDDATE,'YYYYMMDD')
                   ELSE NULL
                   END , --ΥԼʱ��        ���׶��ֵ��ڲ�����-�ͻ�����
          ''  , --�ʲ�֤ȯ������       
          ''    --����˸���       
      FROM  BRD_BILL T1      
      LEFT JOIN RWA.ORG_INFO T2
             ON T1.ORG_CD = T2.ORGID
            AND T2.STATUS = '1' 
      LEFT JOIN RWA_DEV.NCM_CUSTOMER_INFO T3
             ON T1.CUST_NO = T3.MFCUSTOMERID
             AND T1.DATANO=T3.DATANO
            AND T3.CUSTOMERTYPE NOT LIKE '03%' --�Թ��ͻ�   
      LEFT JOIN RWA.CODE_LIBRARY T4
             ON T3.INDUSTRYTYPE = T4.ITEMNO
            AND T4.CODENO = 'IndustryType'
                /*LEFT JOIN RWA_DEV.IRS_CR_CUSTOMER_RATE T5
             ON T1.CUST_NO = T5.T_IT_CUSTOMER_ID   */
      LEFT JOIN RWA_TEMP_PDLEVEL T5
             ON T5.CUSTID = T3.CUSTOMERID
      LEFT JOIN RWA_DEV.NCM_BREAKDEFINEDREMARK T6
             ON T1.CUST_NO = T6.CUSTOMERID   
             AND T1.DATANO=T6.DATANO
      WHERE T1.ATL_PAY_AMT <> 0	--ȡ�Ķ��Ǳ���
        AND SUBSTR(T1.SBJT_CD, 1, 6) IN (
            '130101',	--�����ʲ�-���гжһ�Ʊ����
            '130103',	--�����ʲ�-���гжһ�Ʊת����
            '130105',	--�����ʲ�-�ڲ�ת����
            '130102'  --��ҵ��Ʊ����
        )
        AND T1.DATANO=p_data_dt_str;

    COMMIT;

		dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_PJ_EXPOSURE',cascade => true);

    /*Ŀ�������ͳ��*/
    --ͳ�Ʋ���ļ�¼��
    SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_PJ_EXPOSURE;

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
END PRO_RWA_PJ_EXPOSURE;
/

