CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_XD_EXPOSURE(
                             p_data_dt_str  IN  VARCHAR2,    --��������
                             p_po_rtncode   OUT  VARCHAR2,    --���ر��
                             p_po_rtnmsg    OUT VARCHAR2    --��������
)
  /*
    �洢��������:PRO_RWA_XD_EXPOSURE
    ʵ�ֹ���:�Ŵ�ϵͳ�����������Ϣȫ������RWA�ӿڱ����÷��ձ�¶����
    ���ݿھ�:ȫ��
    ����Ƶ��:�³�����
    ��  ��  :V1.0.0
    ��д��  :YUSJ
    ��дʱ��:2016-04-15
    ��  λ  :�Ϻ���˶��Ϣ�����ɷ����޹�˾
    Դ  ��1  :NCM_BUSINESS_CONTRACT|����ҵ���ͬ��
    Դ  ��2  :NCM_BUSINESS_DUEBILL|����ҵ������Ϣ��
    Դ  ��3  :NCM_ORG_INFO|������Ϣ��
    Դ  ��4  :NCM_BUSINESS_TYPE|ҵ��Ʒ����Ϣ��
    Դ  ��5  :NCM_CUSTOMER_INFO|�ͻ�������Ϣ��¼
    Դ  ��6  :NCM_CODE_LIBRARY|�����
    Դ  ��7  :NCM_ORG_CONTRAST|���Ļ������չ�ϵ��
    Ŀ���  :RWA_XD_EXPOSURE|��Ϣ����ϵͳ-���-�Թ�
    ������  :��
    �����¼(�޸���|�޸�ʱ��|�޸�����):
         pxl 2019/04/11 ����13100001 ���ڴ���-΢�������ڴ��� ȡ���߼������Ϻ����߼�
    
    */
  AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  --v_pro_name VARCHAR2(200) := 'PRO_RWA_XD_EXPOSURE';
  --�����쳣����
  v_raise EXCEPTION;
  --���嵱ǰ����ļ�¼��
  v_count1 INTEGER;

  BEGIN
    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*��ʼ���Թ��ͻ�������Ϣ��*/
    --��ʱ��ֻ����һ�����ݣ�����֮ǰɾ��������
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_TEMP_PDLEVEL';

    --����Թ��ͻ�����������Ϣ
    INSERT INTO RWA_DEV.RWA_TEMP_PDLEVEL(
                 CUSTID                           --01�ͻ�ID
                ,CUSTNAME                         --02�ͻ�����
                ,ORGCERTCODE                      --03��֯��������
                ,MODELID                          --04ģ��ID
                ,PDCODE                           --05PD�ȼ�
                ,PDLEVEL                          --06PD�ȼ�����
                ,PDADJCODE                        --07PD�����ȼ�
                ,PDADJLEVEL                       --08PD�����ȼ�����
                ,PD                               --09ΥԼ����
                ,PDVAVLIDDATE                     --10��������ʱ��(ΥԼʱ��)
    )
    SELECT T.CUSTID,                              --01�ͻ�ID
           T.CUSTNAME,                            --02�ͻ�����
           T.ORGCERTCODE,                         --03��֯��������
           T.MODELCODE     AS MODELID,            --04ģ��ID
           T.PDLEVEL      AS PDCODE,              --05PD�ȼ�
           TC.ITEMNO      AS PDLEVEL,             --06PD�ȼ�����
           T.PDADJLEVEL   AS PDADJCODE,           --07PD�����ȼ�
           TL.ITEMNO      AS PDADJLEVEL,          --08PD�����ȼ�����
           T.PD,                                  --09ΥԼ����
           T.PDVAVLIDDATE                         --10��������ʱ��(ΥԼʱ��)
		  FROM (SELECT T2.CUSTID,
		               T2.CUSTNAME,
		               T2.ORGCERTCODE,
		               T1.MODELCODE,
		               T1.PDLEVEL,
		               T1.PDADJLEVEL,
		               T3.PDVALUE   AS PD,
		               SUBSTR(REPLACE(T1.PDVAVLIDDATE,'/',''),1,8) AS PDVAVLIDDATE,
                   --һ���ͻ���һ��ʱ�����ж��������ȡPDVAVLIDDATE����һ�Σ����һ��������
		               ROW_NUMBER() OVER(PARTITION BY T2.CUSTID ORDER BY T1.PDVAVLIDDATE DESC) RM
		          FROM RWA_DEV.IRS_CR_CUSTOMER_RATE T1 --�ͻ�������(�Թ�)
		         INNER JOIN RWA_DEV.IRS_IT_CUSTOMER T2 --�ͻ���Ϣ���Ŵ�ϵͳ��(�Թ�)
		                 ON T1.T_IT_CUSTOMER_ID = T2.Custid
		                AND T2.DATANO = p_data_dt_str
		          LEFT JOIN RWA_DEV.IRS_MD_SCALE_PD T3 --���ΥԼ����
		                 ON T1.PDADJLEVEL = T3.PDLEVEL
		                AND T3.DATANO = p_data_dt_str
		              WHERE T1.STATUS = 'CO'
		              AND T1.PDADJLEVEL <> 'N'
		              AND T1.PDADJLEVEL IS NOT NULL
		              AND SUBSTR(REPLACE(T1.PDVAVLIDDATE,'/',''),1,8) <= p_data_dt_str
		              AND T1.DATANO = p_data_dt_str) T
		  LEFT JOIN RWA.CODE_LIBRARY TC
		       ON T.PDLEVEL = TC.ITEMNAME
		       AND TC.CODENO = 'IRating'
		       AND TC.ITEMNO LIKE '01%'
		  LEFT JOIN RWA.CODE_LIBRARY TL
		       ON T.PDADJLEVEL = TL.ITEMNAME
		       AND TL.CODENO = 'IRating'
		       AND TL.ITEMNO LIKE '01%'
		 WHERE T.RM = 1
      ;
      COMMIT;

    --����ͬҵ�ֹ���¼�ͻ���������Ϣ
    INSERT INTO RWA_DEV.RWA_TEMP_PDLEVEL(
                 CUSTID                       --01�ͻ�ID
                ,CUSTNAME                     --02�ͻ�����
                ,ORGCERTCODE                  --03��֯��������
                ,MODELID                      --04ģ��ID
                ,PDCODE                       --05PD�ȼ�
                ,PDLEVEL                      --06PD�ȼ�����
                ,PDADJCODE                    --07PD�����ȼ�
                ,PDADJLEVEL                   --08PD�����ȼ�����
                ,PD                           --09ΥԼ����
                ,PDVAVLIDDATE                 --10��������ʱ��(ΥԼʱ��)
    )
    SELECT      T1.CUSTOMERID                 --01�ͻ�ID
               ,T1.CUSTOMERNAME               --02�ͻ�����
               ,T1.CERTID                     --03��֯��������
               ,'CQM01'                       --04ģ��ID
               ,T2.ITEMNAME                   --05PD�ȼ�
               ,T1.RATINGCODE                 --06PD�ȼ�����
               ,T2.ITEMNAME                   --07PD�����ȼ�
               ,T1.RATINGCODE                 --08PD�����ȼ�����
               ,T3.PDVALUE                    --09ΥԼ����
               ,NULL                          --10��������ʱ��(ΥԼʱ��)
      FROM RWA.RWA_CD_BANKRATING T1   --���������ͻ��嵥
 LEFT JOIN		RWA.CODE_LIBRARY T2				--��ȡ�ڲ�ƽ�����Ӧ���ַ�����
	    ON					T1.RATINGCODE = T2.ITEMNO
	    AND					T2.CODENO = 'IRating'
 LEFT JOIN		RWA_DEV.IRS_MD_SCALE_PD	T3	--���ΥԼ����
	    ON					T2.ITEMNAME = T3.PDLEVEL
	    AND					T3.DATANO = P_DATA_DT_STR
	  WHERE T1.STATUS = '1'
	    AND NOT EXISTS(SELECT 1 FROM RWA_DEV.RWA_TEMP_PDLEVEL T4 WHERE T1.CUSTOMERID=T4.CUSTID)
	  ;
	  COMMIT;

    --�������Ϣ
    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_TEMP_PDLEVEL',cascade => true);

    --��ʱ��ֻ����һ�����ݣ�����֮ǰɾ��������
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_TEMP_LGDLEVEL';

      --��������ծ���������Ϣ
		--����
    INSERT INTO RWA_DEV.RWA_TEMP_LGDLEVEL
		  (BUSINESSTYPE,            --01ҵ��Ʒ��
		   BUSINESSID,              --02ҵ��ID
		   CUSTNAME,                --03�ͻ�����
		   CARDTYPE,                --04�ͻ�֤������
		   CARDNO,                  --05�ͻ�֤������
		   PDCODE,                  --06PD�ֳر��
		   PDVALUE,                 --07PDֵ
		   PDMODELNAME,             --08PDģ������
		   PDMODELCODE,             --09PDģ�ʹ���
		   LGDCODE,                 --10LGD�ֳر��
		   LGDVALUE,                --11LGDֵ
		   LGDMODELNAME,            --12LGDģ������
		   LGDMODELCODE,            --13LGDģ�ʹ���
		   BEELVALUE,               --14BEELֵ
		   CCFCODE,                 --15CCF�ֳر��
		   CCFVALUE,                --16CCFֵ
		   CCFMODELNAME,            --17CCFģ������
		   CCFMODELCODE,            --18CCFģ�ʹ���
		   RISK_EXPOSURE,           --19���ձ�¶����
		   DEFAULTFLAG,             --20�Ƿ�ΥԼ
		   MOB,                     --21����
		   UPDATETIME)              --22��������ʱ��(ΥԼʱ��)
		  SELECT BUSINESSTYPE,
		         BUSINESSID,
		         CUSTNAME,
		         CARDTYPE,
		         CARDNO,
		         PDCODE,
		         PDVALUE,
		         PDMODELNAME,
		         PDMODELCODE,
		         LGDCODE,
		         LGDVALUE,
		         LGDMODELNAME,
		         LGDMODELCODE,
		         BEELVALUE,
		         CCFCODE,
		         CCFVALUE,
		         CCFMODELNAME,
		         CCFMODELCODE,
		         RISK_EXPOSURE,
		         DEFAULTFLAG,
		         MOB,
		         UPDATETIME
		    FROM (SELECT TR.BUSSCODE AS BUSINESSTYPE,                                            --01ҵ��Ʒ��
                     TR.CONTRACTID AS BUSINESSID,                                            --02ҵ��ID
                     TR.CUSTNAME,                                                            --03�ͻ�����
                     TR.CARDTYPE,                                                            --04�ͻ�֤������
                     TR.CARDNO,                                                              --05�ͻ�֤������
                     TR.PDCODE,                                                              --06PD�ֳر��
                     CASE WHEN TR.PDVALUE=0 THEN NULL ELSE TR.PDVALUE END AS PDVALUE,        --07PDֵ
                     TR.PDMODELNAME,                                                         --08PDģ������
                     TR.PDMODELCODE,                                                         --09PDģ�ʹ���
                     TR.LGDCODE,                                                             --10LGD�ֳر��
                     CASE WHEN TR.LGDVALUE=0 THEN NULL ELSE TR.LGDVALUE END AS LGDVALUE,     --11LGDֵ
                     TR.LGDMODELNAME,                                                        --12LGDģ������
                     TR.LGDMODELCODE,                                                        --13LGDģ�ʹ���
                     CASE WHEN TR.BEELVALUE=0 THEN NULL ELSE TR.BEELVALUE END AS BEELVALUE,  --14BEELֵ
                     TR.CCFCODE,                                                             --15CCF�ֳر��
                     CASE WHEN TR.CCFVALUE=0 THEN NULL ELSE TR.CCFVALUE END AS CCFVALUE,     --16CCFֵ
                     TR.CCFMODELNAME,                                                        --17CCFģ������
                     TR.CCFMODELCODE,                                                        --18CCFģ�ʹ���
                     TR.RISK_EXPOSURE,                                                       --19���ձ�¶����
		                 CASE
		                   WHEN TR.PDDEFAULTFLAG = '1' OR TR.LGDDEFAULTFLAG = '1' THEN
		                    '1'
		                   ELSE
		                    '0'
		                 END AS DEFAULTFLAG,                                                     --20�Ƿ�ΥԼ
		                 TG.MOB,                                                                 --21����
		                 REPLACE(SUBSTR(TR.UPDATETIME,1,10),'-','') AS UPDATETIME,               --22��������ʱ��(ΥԼʱ��)
		                 --ȡ���һ�εĸ���ʱ��
                     ROW_NUMBER() OVER(PARTITION BY TR.CONTRACTID ORDER BY TR.UPDATETIME DESC) AS RECORDNUM
		            FROM RWA_DEV.RRS_MC_FC_RESULT_HIS TR --���۷ֳ�ģ�ͽ����ʷ��
		       LEFT JOIN RWA_DEV.RRS_PER_LOAN_GRADE_H TG --���˴���������Ϣ��ʷ��
		              ON TR.BUSSINESS_SEQ = TG.ID
		             AND TG.DATANO = p_data_dt_str
		           WHERE --TO_DATE(TR.UPDATETIME, 'YYYY-MM-DD HH24:MI:SS') <= TO_DATE(p_data_dt_str, 'YYYYMMDD')
		                 TR.BUSSCODE <> 'CREDITCARD'
		             AND TR.DATANO = p_data_dt_str)
		   WHERE RECORDNUM = 1 AND PDVALUE>0
		;
		COMMIT;

		--���ÿ�
		INSERT INTO RWA_DEV.RWA_TEMP_LGDLEVEL
		  (BUSINESSTYPE,
		   BUSINESSID,
		   CUSTNAME,
		   CARDTYPE,
		   CARDNO,
		   PDCODE,
		   PDVALUE,
		   PDMODELNAME,
		   PDMODELCODE,
		   LGDCODE,
		   LGDVALUE,
		   LGDMODELNAME,
		   LGDMODELCODE,
		   BEELVALUE,
		   CCFCODE,
		   CCFVALUE,
		   CCFMODELNAME,
		   CCFMODELCODE,
		   RISK_EXPOSURE,
		   DEFAULTFLAG,
		   MOB,
		   UPDATETIME)
		  SELECT BUSINESSTYPE,
		         BUSINESSID,
		         CUSTNAME,
		         CARDTYPE,
		         CARDNO,
		         PDCODE,
		         PDVALUE,
		         PDMODELNAME,
		         PDMODELCODE,
		         LGDCODE,
		         LGDVALUE,
		         LGDMODELNAME,
		         LGDMODELCODE,
		         BEELVALUE,
		         CCFCODE,
		         CCFVALUE,
		         CCFMODELNAME,
		         CCFMODELCODE,
		         RISK_EXPOSURE,
		         DEFAULTFLAG,
		         MOB,
		         UPDATETIME
		    FROM (SELECT TR.BUSSCODE AS BUSINESSTYPE,
		                 TR.CREDITCARDNO AS BUSINESSID,
		                 TR.CUSTNAME,
		                 TR.CARDTYPE,
		                 TR.CARDNO,
		                 TR.PDCODE,
		                 CASE WHEN TR.PDVALUE=0 THEN NULL ELSE TR.PDVALUE END AS PDVALUE,
		                 TR.PDMODELNAME,
		                 TR.PDMODELCODE,
		                 TR.LGDCODE,
		                 CASE WHEN TR.LGDVALUE=0 THEN NULL ELSE TR.LGDVALUE END AS LGDVALUE,
		                 TR.LGDMODELNAME,
		                 TR.LGDMODELCODE,
		                 CASE WHEN TR.BEELVALUE=0 THEN NULL ELSE TR.BEELVALUE END AS BEELVALUE,
		                 TR.CCFCODE,
		                 CASE WHEN TR.CCFVALUE=0 THEN NULL ELSE TR.CCFVALUE END AS CCFVALUE,
		                 TR.CCFMODELNAME,
		                 TR.CCFMODELCODE,
		                 TR.RISK_EXPOSURE,
		                 CASE
		                   WHEN TR.PDDEFAULTFLAG = '1' OR TR.LGDDEFAULTFLAG = '1' THEN
		                    '1'
		                   ELSE
		                    '0'
		                 END AS DEFAULTFLAG,
		                 TG.MOB,
		                 REPLACE(SUBSTR(TR.UPDATETIME,1,10),'-','') AS UPDATETIME,
		                 ROW_NUMBER() OVER(PARTITION BY TR.CREDITCARDNO ORDER BY TR.UPDATETIME DESC) AS RECORDNUM
		            FROM RWA_DEV.RRS_MC_FC_RESULT_HIS TR --���۷ֳ�ģ�ͽ����ʷ��
		            LEFT JOIN RWA_DEV.RRS_PER_CARD_GRADE_H TG --�������ÿ�������Ϣ��ʷ��
		              ON TR.BUSSINESS_SEQ = TG.ID
		             AND TG.DATANO = P_DATA_DT_STR
		           WHERE --TO_DATE(TR.UPDATETIME, 'YYYY-MM-DD HH24:MI:SS') <= TO_DATE(p_data_dt_str, 'YYYYMMDD')
		                 TR.BUSSCODE = 'CREDITCARD'
		             AND TR.DATANO = p_data_dt_str)
		   WHERE RECORDNUM = 1 AND PDVALUE>0
	  ;

	  COMMIT;

	  --�������Ϣ
    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_TEMP_LGDLEVEL',cascade => true);

    /*�����ȫ�����ݼ��������Ŀ���*/
    --1.���Ŀ����е�ԭ�м�¼
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_XD_EXPOSURE';    

    --2.���������������ݴ�Դ����뵽Ŀ�����
    /*1 ���� �Ŵ�ϵͳ�Թ���� ��Ŀ���*/ 
    INSERT INTO RWA_DEV.RWA_XD_EXPOSURE(
               DATADATE         --01 ��������
              ,DATANO           --02 ������ˮ��
              ,EXPOSUREID       --03 ���ձ�¶ID
              ,DUEID            --04 ծ��ID
              ,SSYSID           --05 ԴϵͳID
              ,CONTRACTID       --06 ��ͬID
              ,CLIENTID         --07 ��������ID
              ,SORGID           --08 Դ����ID
              ,SORGNAME         --09 Դ��������
              ,ORGSORTNO        --10 �������������
              ,ORGID            --11 ��������ID
              ,ORGNAME          --12 ������������
              ,ACCORGID         --13 �������ID
              ,ACCORGNAME       --14 �����������
              ,INDUSTRYID       --15 ������ҵ����
              ,INDUSTRYNAME     --16 ������ҵ����
              ,BUSINESSLINE     --17 ҵ������
              ,ASSETTYPE        --18 �ʲ�����
              ,ASSETSUBTYPE     --19 �ʲ�С��
              ,BUSINESSTYPEID   --20 ҵ��Ʒ�ִ���
              ,BUSINESSTYPENAME --21 ҵ��Ʒ������
              ,CREDITRISKDATATYPE    --22 ���÷�����������
              ,ASSETTYPEOFHAIRCUTS   --23 �ۿ�ϵ����Ӧ�ʲ����
              ,BUSINESSTYPESTD       --24 Ȩ�ط�ҵ������
              ,EXPOCLASSSTD          --25 Ȩ�ط���¶����
              ,EXPOSUBCLASSSTD       --26 Ȩ�ط���¶С��
              ,EXPOCLASSIRB          --27 ��������¶����
              ,EXPOSUBCLASSIRB       --28 ��������¶С��
              ,EXPOBELONG            --29 ��¶������ʶ
              ,BOOKTYPE              --30 �˻����
              ,REGUTRANTYPE          --31 ��ܽ�������
              ,REPOTRANFLAG          --32 �ع����ױ�ʶ
              ,REVAFREQUENCY         --33 �ع�Ƶ��
              ,CURRENCY              --34 ����
              ,NORMALPRINCIPAL       --35 �����������
              ,OVERDUEBALANCE        --36 �������
              ,NONACCRUALBALANCE     --37 ��Ӧ�����
              ,ONSHEETBALANCE        --38 �������
              ,NORMALINTEREST        --39 ������Ϣ
              ,ONDEBITINTEREST       --40 ����ǷϢ
              ,OFFDEBITINTEREST      --41 ����ǷϢ
              ,EXPENSERECEIVABLE     --42 Ӧ�շ���
              ,ASSETBALANCE          --43 �ʲ����
              ,ACCSUBJECT1           --44 ��Ŀһ
              ,ACCSUBJECT2           --45 ��Ŀ��
              ,ACCSUBJECT3           --46 ��Ŀ��
              ,STARTDATE             --47 ��ʼ����
              ,DUEDATE               --48 ��������
              ,ORIGINALMATURITY      --49 ԭʼ����
              ,RESIDUALM             --50 ʣ������
              ,RISKCLASSIFY          --51 ���շ���
              ,EXPOSURESTATUS        --52 ���ձ�¶״̬
              ,OVERDUEDAYS           --53 ��������
              ,SPECIALPROVISION      --54 ר��׼����
              ,GENERALPROVISION      --55 һ��׼����
              ,ESPECIALPROVISION     --56 �ر�׼����
              ,WRITTENOFFAMOUNT      --57 �Ѻ������
              ,OFFEXPOSOURCE         --58 ���Ⱪ¶��Դ
              ,OFFBUSINESSTYPE       --59 ����ҵ������
              ,OFFBUSINESSSDVSSTD    --60 Ȩ�ط�����ҵ������ϸ��
              ,UNCONDCANCELFLAG      --61 �Ƿ����ʱ����������
              ,CCFLEVEL              --62 ����ת��ϵ������
              ,CCFAIRB               --63 �߼�������ת��ϵ��
              ,CLAIMSLEVEL           --64 ծȨ����
              ,BONDFLAG              --65 �Ƿ�Ϊծȯ
              ,BONDISSUEINTENT       --66 ծȯ����Ŀ��
              ,NSUREALPROPERTYFLAG   --67 �Ƿ�����ò�����
              ,REPASSETTERMTYPE      --68 ��ծ�ʲ���������
              ,DEPENDONFPOBFLAG      --69 �Ƿ�����������δ��ӯ��
              ,IRATING               --70 �ڲ�����
              ,PD                    --71 ΥԼ����
              ,LGDLEVEL              --72 ΥԼ��ʧ�ʼ���
              ,LGDAIRB               --73 �߼���ΥԼ��ʧ��
              ,MAIRB                 --74 �߼�����Ч����
              ,EADAIRB               --75 �߼���ΥԼ���ձ�¶
              ,DEFAULTFLAG           --76 ΥԼ��ʶ
              ,BEEL                  --77 ��ΥԼ��¶Ԥ����ʧ����
              ,DEFAULTLGD            --78 ��ΥԼ��¶ΥԼ��ʧ��
              ,EQUITYEXPOFLAG        --79 ��Ȩ��¶��ʶ
              ,EQUITYINVESTTYPE      --80 ��ȨͶ�ʶ�������
              ,EQUITYINVESTCAUSE     --81 ��ȨͶ���γ�ԭ��
              ,SLFLAG                --82 רҵ�����ʶ
              ,SLTYPE                --83 רҵ��������
              ,PFPHASE               --84 ��Ŀ���ʽ׶�
              ,REGURATING            --85 �������
              ,CBRCMPRATINGFLAG      --86 ������϶������Ƿ��Ϊ����
              ,LARGEFLUCFLAG         --87 �Ƿ񲨶��Խϴ�
              ,LIQUEXPOFLAG          --88 �Ƿ���������з��ձ�¶
              ,PAYMENTDEALFLAG       --89 �Ƿ����Ը�ģʽ
              ,DELAYTRADINGDAYS      --90 �ӳٽ�������
              ,SECURITIESFLAG        --91 �м�֤ȯ��ʶ
              ,SECUISSUERID          --92 ֤ȯ������ID
              ,RATINGDURATIONTYPE    --93 ������������
              ,SECUISSUERATING       --94 ֤ȯ���еȼ�
              ,SECURESIDUALM         --95 ֤ȯʣ������
              ,SECUREVAFREQUENCY     --96 ֤ȯ�ع�Ƶ��
              ,CCPTRANFLAG           --97 �Ƿ����뽻�׶�����ؽ���
              ,CCPID                 --98 ���뽻�׶���ID
              ,QUALCCPFLAG           --99 �Ƿ�ϸ����뽻�׶���
              ,BANKROLE              --100 ���н�ɫ
              ,CLEARINGMETHOD        --101 ���㷽ʽ
              ,BANKASSETFLAG         --102 �Ƿ������ύ�ʲ�
              ,MATCHCONDITIONS       --103 �����������
              ,SFTFLAG               --104 ֤ȯ���ʽ��ױ�ʶ
              ,MASTERNETAGREEFLAG    --105 ���������Э���ʶ
              ,MASTERNETAGREEID      --106 ���������Э��ID
              ,SFTTYPE               --107 ֤ȯ���ʽ�������
              ,SECUOWNERTRANSFLAG    --108 ֤ȯ����Ȩ�Ƿ�ת��
              ,OTCFLAG               --109 �����������߱�ʶ
              ,VALIDNETTINGFLAG      --110 ��Ч�������Э���ʶ
              ,VALIDNETAGREEMENTID   --111 ��Ч�������Э��ID
              ,OTCTYPE               --112 ����������������
              ,DEPOSITRISKPERIOD     --113 ��֤������ڼ�
              ,MTM                   --114 ���óɱ�
              ,MTMCURRENCY           --115 ���óɱ�����
              ,BUYERORSELLER         --116 ������
              ,QUALROFLAG            --117 �ϸ�����ʲ���ʶ
              ,ROISSUERPERFORMFLAG   --118 �����ʲ��������Ƿ�����Լ
              ,BUYERINSOLVENCYFLAG   --119 ���ñ������Ƿ��Ʋ�
              ,NONPAYMENTFEES        --120 ��δ֧������
              ,RETAILEXPOFLAG        --121 ���۱�¶��ʶ
              ,RETAILCLAIMTYPE       --122 ����ծȨ����
              ,MORTGAGETYPE          --123 ס����Ѻ��������
              ,EXPONUMBER            --124 ���ձ�¶����
              ,LTV                   --125 �����ֵ��
              ,AGING                 --126 ����
              ,NEWDEFAULTDEBTFLAG    --127 ����ΥԼծ���ʶ
              ,PDPOOLMODELID         --128 PD�ֳ�ģ��ID
              ,LGDPOOLMODELID        --129 LGD�ֳ�ģ��ID
              ,CCFPOOLMODELID        --130 CCF�ֳ�ģ��ID
              ,PDPOOLID              --131 ����PD��ID
              ,LGDPOOLID             --132 ����LGD��ID
              ,CCFPOOLID             --133 ����CCF��ID
              ,ABSUAFLAG             --134 �ʲ�֤ȯ�������ʲ���ʶ
              ,ABSPOOLID             --135 ֤ȯ���ʲ���ID
              ,GROUPID               --136 ������
              ,DefaultDate           --137 ΥԼʱ��
              ,ABSPROPORTION         --138 �ʲ�֤ȯ������
              ,DEBTORNUMBER          --139 ����˸���
              ,flag
    
    )
    SELECT
                TO_DATE(T1.DATANO,'YYYYMMDD')                                           AS DATADATE                -- ��������
                ,T1.DATANO                                                              AS DATANO                  -- ������ˮ��
                ,T1.SERIALNO                                                            AS EXPOSUREID              -- ���ձ�¶ID
                ,T1.SERIALNO                                                            AS DUEID                   -- ծ��ID
                ,'XD'                                                                   AS SSYSID                  -- ԴϵͳID
                ,T1.RELATIVESERIALNO2                                                   AS CONTRACTID              -- ��ͬID
                ,CASE WHEN REPLACE(T1.CUSTOMERID,'NCM_','') IS NULL THEN DECODE(T3.LINETYPE,'0030','XN-GRKH','XN-YBGS') 								--���ͻ�IDΪ�գ�����Ϊ���˾�����Ϊ���˿ͻ�������Ϊһ�㹫˾
                			ELSE T1.CUSTOMERID
                 END					                                                          AS CLIENTID                -- ��������ID
                --,decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID) Ϊ������У�鱨��ͨ����������@��ͷ�Ļ�������Ϊ����
                -- �������ּӹ���ɺ��ٸĻ���
                ,case when substr(decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID),1,1)='@' then '01000000'
                     else decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID)
                 end                                                                     AS SORGID                  -- Դ����ID
                --,T4.ORGNAME                                                            AS SORGNAME                -- Դ��������
                ,nvl(T4.ORGNAME,'����')                                                  
                --,T4.SORTNO                                                              AS ORGSORTNO               -- �������������
                ,nvl(T4.SORTNO,'1010')
                --,decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID)                      AS ORGID                   -- ��������ID
                ,case when substr(decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID),1,1)='@' then '01000000'
                     else decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID)
                 end
                --,T4.ORGNAME                                                             AS ORGNAME                 -- ������������
                ,nvl(T4.ORGNAME,'����')
                --,decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID)                      AS ACCORGID                -- �������ID
                ,case when substr(decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID),1,1)='@' then '01000000'
                     else decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID)
                 end
                --,T4.ORGNAME                                                             AS ACCORGNAME              -- �����������
                ,nvl(T4.ORGNAME,'����')
                ,NVL(T3.DIRECTION,CPS.DIRECTION)                                        AS INDUSTRYID              -- ������ҵ����
                ,NVL(T5.ITEMNAME,CL.ITEMNAME)                                           AS INDUSTRYNAME            -- ������ҵ����
                ,CASE WHEN T1.BUSINESSCURRENCY<>'CNY' THEN '0102'     --��ҵı���ҵ��   ����-ó��
                      WHEN T1.BUSINESSTYPE IN('10352010','10352020') THEN '0101'--���гжһ�Ʊ�жң��ͷ��գ������гжһ�Ʊ�жң��ǵͷ��գ�	����-��˾ by chengang
                      WHEN T1.BUSINESSTYPE IN('10302010','10302015','10302020') THEN '0401'  --����ҵ��        ͬҵ�����г���
                      WHEN T3.LINETYPE='0010' THEN '0101'
                	    WHEN T3.LINETYPE='0020' THEN '0201'
                	    WHEN T3.LINETYPE='0030' THEN '0301'
                	    WHEN T3.LINETYPE='0040' THEN '0401'
                	    ELSE '0101'
                 END                                                                    AS BUSINESSLINE            -- ����
                ,''                                                                     AS ASSETTYPE               -- �ʲ�����
                ,''                                                                     AS ASSETSUBTYPE            -- �ʲ�С��
                ,T1.BUSINESSTYPE                                                        AS BUSINESSTYPEID          -- ҵ��Ʒ�ִ���
                ,T2.TYPENAME                                                            AS BUSINESSTYPENAME        -- ҵ��Ʒ������
                /*,CASE WHEN T1.SERIALNO='20170125c0000373' THEN '02'
                	    ELSE '01'
                 END                                                                    AS CREDITRISKDATATYPE      -- ���÷�����������          01-һ�������
                */
                ,CASE WHEN T16.CUSTOMERTYPE = '0310' OR T16.CERTTYPE LIKE 'Ind%'
                      THEN '02' --����
                      ELSE '01' --������
                  END                                                              			AS CREDITRISKDATATYPE  		 --���÷�����������
                ,'01'                                                                   AS ASSETTYPEOFHAIRCUTS     -- �ۿ�ϵ����Ӧ�ʲ����     01-�ֽ��ֽ�ȼ���
                ,''                                                                     AS BUSINESSTYPESTD         -- Ȩ�ط�ҵ������
                ,CASE WHEN REPLACE(T1.CUSTOMERID,'NCM_','') IS NULL THEN '0112' 																	 --���ͻ�IDΪ�գ�Ĭ�� ����(0112)
                			ELSE ''
                 END                                                                    AS EXPOCLASSSTD            -- Ȩ�ط���¶����
                ,CASE WHEN REPLACE(T1.CUSTOMERID,'NCM_','') IS NULL THEN DECODE(T3.LINETYPE,'0030','011215','011216') 												 --���ͻ�IDΪ�գ�����Ϊ���˾�Ĭ�� ��������75%����Ȩ�ص��ʲ�(011215)������Ĭ�� ��������100%����Ȩ�ص��ʲ�(011216)
                			ELSE ''
                 END                                                                    AS EXPOSUBCLASSSTD         -- Ȩ�ط���¶С��
                ,SUBSTR(T10.DITEMNO,1,4)                                                AS EXPOCLASSIRB            -- ��������¶����
                ,T10.DITEMNO                                                            AS EXPOSUBCLASSIRB         -- ��������¶С��
                ,CASE WHEN T2.OFFSHEETFLAG='EntOff' THEN '02'   --  02-����
                      ELSE '01'                                 --  01-����
                 END                                                                   AS EXPOBELONG              -- ��¶������ʶ
                ,'01'                                                                  AS BOOKTYPE                -- �˻����           01-�����˻�
                ,'03'                                                                  AS REGUTRANTYPE            -- ��ܽ�������      03-��Ѻ����
                ,'0'                                                                   AS REPOTRANFLAG            -- �ع����ױ�ʶ       0-��
                ,1                                                                     AS REVAFREQUENCY           -- �ع�Ƶ��
                ,NVL(T1.BUSINESSCURRENCY,'CNY')                                        AS CURRENCY                -- ����
                ,T31.Balance                                                     AS NORMALPRINCIPAL         -- �����������
                ,0                                                                     AS OVERDUEBALANCE          -- �������
                ,0                                                                     AS NONACCRUALBALANCE       -- ��Ӧ�����
                ,T31.Balance                                                     AS ONSHEETBALANCE          -- �������
                ,0                                                                     AS NORMALINTEREST          -- ������Ϣ
                ,0                                                                     AS ONDEBITINTEREST         -- ����ǷϢ
                ,0                                                                     AS OFFDEBITINTEREST        -- ����ǷϢ
                ,0                                                                     AS EXPENSERECEIVABLE       -- Ӧ�շ���
                ,T31.Balance                                                     AS ASSETBALANCE            -- �ʲ����
                ,CASE 
                   WHEN T1.SUBJECTNO = '@01010502' THEN '13050100' --����Ѻ���Ŀ���⴦��
                   WHEN T1.SUBJECTNO = '@01010501' THEN '13050200' --����Ѻ���Ŀ���⴦��
                   WHEN T1.SUBJECTNO = '@01010521' THEN '13050502' --����Ѻ���Ŀ���⴦��
                   when substr(t1.businesstype,1,6) = '103520' then '70020000' --�жһ�Ʊ��Ŀ���⴦��
                 ELSE T1.SUBJECTNO END                                                 AS ACCSUBJECT1             -- ��Ŀһ
                ,''                                                                    AS ACCSUBJECT2             -- ��Ŀ��
                ,''                                                                    AS ACCSUBJECT3             -- ��Ŀ��
                ,NVL(T1.PUTOUTDATE,T3.PUTOUTDATE)                                      AS STARTDATE               -- ��ʼ����
                ,NVL(T1.ACTUALMATURITY,T3.MATURITY)                                    AS DUEDATE                 -- ��������
                ,CASE WHEN (TO_DATE(NVL(T1.ACTUALMATURITY,T3.MATURITY),'YYYYMMDD')-TO_DATE(NVL(T1.PUTOUTDATE,T3.PUTOUTDATE),'YYYYMMDD'))/365<0
                      THEN 0
                      ELSE (TO_DATE(NVL(T1.ACTUALMATURITY,T3.MATURITY),'YYYYMMDD')-TO_DATE(NVL(T1.PUTOUTDATE,T3.PUTOUTDATE),'YYYYMMDD'))/365
                END                                                                    AS ORIGINALMATURITY        -- ԭʼ����
                ,CASE WHEN (TO_DATE(NVL(T1.ACTUALMATURITY,T3.MATURITY),'YYYYMMDD')-TO_DATE(T1.DATANO,'YYYYMMDD'))/365<0
                      THEN 0
                      ELSE (TO_DATE(NVL(T1.ACTUALMATURITY,T3.MATURITY),'YYYYMMDD')-TO_DATE(T1.DATANO,'YYYYMMDD'))/365
                END                                                                    AS RESIDUALM               -- ʣ������
                ,CASE WHEN NVL(T1.Classifyresult,SUBSTR(T11.FINALLYRESULT,1,1))='A' THEN '01'
                      WHEN NVL(T1.Classifyresult,SUBSTR(T11.FINALLYRESULT,1,1))='B' THEN '02'       --ʮ��������תΪ�弶����
                      WHEN NVL(T1.Classifyresult,SUBSTR(T11.FINALLYRESULT,1,1))='C' THEN '03'
                      WHEN NVL(T1.Classifyresult,SUBSTR(T11.FINALLYRESULT,1,1))='D' THEN '04'
                      WHEN NVL(T1.Classifyresult,SUBSTR(T11.FINALLYRESULT,1,1))='E' THEN '05'
                      ELSE NVL(T1.Classifyresult,SUBSTR(T11.FINALLYRESULT,1,1))
                 END                                                                   AS RISKCLASSIFY            -- ���շ���
                ,'01'                                                                  AS EXPOSURESTATUS          -- ���ձ�¶״̬  01-����
                ,T1.OVERDUEDAYS                                                        AS OVERDUEDAYS             -- ��������
                ,0                                                                     AS SPECIALPROVISION        -- ר��׼����-������
                ,0                                                                     AS GENERALPROVISION        -- һ��׼����
                ,0                                                                     AS ESPECIALPROVISION       -- �ر�׼����
                ,T1.CANCELSUM                                                          AS WRITTENOFFAMOUNT        -- �Ѻ������
                ,CASE WHEN T2.OFFSHEETFLAG='EntOff'  THEN '03'                         --  03-ʵ�ʱ���ҵ��
                      ELSE ''
                 END                                                                   AS OffExpoSource           -- ���Ⱪ¶��Դ
                ,''                                                                    AS OffBusinessType         -- ����ҵ������
                ,''                                                                    AS OffBusinessSdvsSTD      -- Ȩ�ط�����ҵ������ϸ��
                ,'1'                                                                   AS UncondCancelFlag        -- �Ƿ����ʱ����������
                ,''                                                                    AS CCFLevel                -- ����ת��ϵ������
                ,NULL                                                                  AS CCFAIRB                 -- �߼�������ת��ϵ��
                ,'01'                                                                  AS CLAIMSLEVEL             -- ծȨ����
                ,'0'                                                                   AS BONDFLAG                -- �Ƿ�Ϊծȯ
                ,'02'                                                                  AS BONDISSUEINTENT         -- ծȯ����Ŀ��
                ,'0'                                                                   AS NSUREALPROPERTYFLAG     -- �Ƿ�����ò�����
                ,''                                                                    AS REPASSETTERMTYPE        -- ��ծ�ʲ���������
                ,'0'                                                                   AS DEPENDONFPOBFLAG        -- �Ƿ�����������δ��ӯ��
                ,T6.PDADJLEVEL                                                         AS IRATING                 -- �ڲ�����
                ,T6.PD                                                                 AS PD                      -- ΥԼ����
                ,''                                                                    AS LGDLEVEL                -- ΥԼ��ʧ�ʼ���
                ,0                                                                     AS LGDAIRB                 -- �߼���ΥԼ��ʧ��
                ,0                                                                     AS MAIRB                   -- �߼�����Ч����
                ,T31.Balance                                                     AS EADAIRB                 -- �߼���ΥԼ���ձ�¶
                ,CASE WHEN T6.PDADJCODE='D' THEN '1'
                      ELSE '0'
                 END                                                                   AS DEFAULTFLAG             -- ΥԼ��ʶ
                ,0                                                                     AS BEEL                    -- ��ΥԼ��¶Ԥ����ʧ����
                ,0                                                                     AS DEFAULTLGD              -- ��ΥԼ��¶ΥԼ��ʧ��
                ,'0'                                                                   AS EQUITYEXPOFLAG          -- ��Ȩ��¶��ʶ
                ,''                                                                    AS EQUITYINVESTTYPE        -- ��ȨͶ�ʶ�������
                ,''                                                                    AS EQUITYINVESTCAUSE       -- ��ȨͶ���γ�ԭ��
                ,'0'                                                                   AS SLFLAG                  -- רҵ�����ʶ       רҵ��������ֶ�һ���ȸ���
                ,''                                                                    AS SLTYPE                  -- רҵ��������
                ,''                                                                    AS PFPHASE                 -- ��Ŀ���ʽ׶�
                ,'01'                                                                  AS REGURATING              -- �������
                ,''                                                                    AS CBRCMPRATINGFLAG        -- ������϶������Ƿ��Ϊ����
                ,''                                                                    AS LARGEFLUCFLAG           -- �Ƿ񲨶��Խϴ�
                ,'0'                                                                   AS LIQUEXPOFLAG            -- �Ƿ���������з��ձ�¶
                ,''                                                                    AS PAYMENTDEALFLAG         -- �Ƿ����Ը�ģʽ
                ,0                                                                     AS DELAYTRADINGDAYS        -- �ӳٽ�������
                ,'0'                                                                   AS SECURITIESFLAG          -- �м�֤ȯ��ʶ
                ,''                                                                    AS SECUISSUERID            -- ֤ȯ������ID
                ,''                                                                    AS RATINGDURATIONTYPE      -- ������������
                ,''                                                                    AS SECUISSUERATING         -- ֤ȯ���еȼ�
                ,0                                                                     AS SECURESIDUALM           -- ֤ȯʣ������
                ,1                                                                     AS SECUREVAFREQUENCY       -- ֤ȯ�ع�Ƶ��
                ,'0'                                                                   AS CCPTRANFLAG             -- �Ƿ����뽻�׶�����ؽ���
                ,''                                                                    AS CCPID                   -- ���뽻�׶���ID
                ,'0'                                                                   AS QUALCCPFLAG             -- �Ƿ�ϸ����뽻�׶���
                ,''                                                                    AS BANKROLE                -- ���н�ɫ
                ,''                                                                    AS CLEARINGMETHOD          -- ���㷽ʽ
                ,'0'                                                                   AS BANKASSETFLAG           -- �Ƿ������ύ�ʲ�
                ,''                                                                    AS MATCHCONDITIONS         -- �����������
                ,'0'                                                                   AS SFTFLAG                 -- ֤ȯ���ʽ��ױ�ʶ
                ,''                                                                    AS MASTERNETAGREEFLAG      -- ���������Э���ʶ
                ,''                                                                    AS MASTERNETAGREEID        -- ���������Э��ID
                ,''                                                                    AS SFTTYPE                 -- ֤ȯ���ʽ�������
                ,''                                                                    AS SECUOWNERTRANSFLAG      -- ֤ȯ����Ȩ�Ƿ�ת��
                ,'0'                                                                   AS OTCFLAG                 -- �����������߱�ʶ
                ,''                                                                    AS VALIDNETTINGFLAG        -- ��Ч�������Э���ʶ
                ,''                                                                    AS VALIDNETAGREEMENTID     -- ��Ч�������Э��ID
                ,''                                                                    AS OTCTYPE                 -- ����������������
                ,''                                                                    AS DEPOSITRISKPERIOD       -- ��֤������ڼ�
                ,0                                                                     AS MTM                     -- ���óɱ�
                ,''                                                                    AS MTMCURRENCY             -- ���óɱ�����
                ,''                                                                    AS BUYERORSELLER           -- ������
                ,''                                                                    AS QUALROFLAG              -- �ϸ�����ʲ���ʶ
                ,''                                                                    AS ROISSUERPERFORMFLAG     -- �����ʲ��������Ƿ�����Լ
                ,''                                                                    AS BUYERINSOLVENCYFLAG     -- ���ñ������Ƿ��Ʋ�
                ,0                                                                     AS NONPAYMENTFEES          -- ��δ֧������
                ,'0'                                                                   AS RETAILEXPOFLAG          -- ���۱�¶��ʶ
                ,''                                                                    AS RETAILCLAIMTYPE         -- ����ծȨ����
                ,''                                                                    AS MORTGAGETYPE            -- ס����Ѻ��������
                ,1                                                                     AS EXPONUMBER              -- ���ձ�¶����
                ,0.8                                                                   AS LTV                     --�����ֵ��
                ,0                                                                     AS AGING                   --����
                ,''                                                                    AS NEWDEFAULTDEBTFLAG      --����ΥԼծ���ʶ
                ,''                                                                    AS PDPOOLMODELID           -- PD�ֳ�ģ��ID
                ,''                                                                    AS LGDPOOLMODELID          -- LGD�ֳ�ģ��ID
                ,''                                                                    AS CCFPOOLMODELID          -- CCF�ֳ�ģ��ID
                ,''                                                                    AS PDPOOLID                -- ����PD��ID
                ,''                                                                    AS LGDPOOLID               -- ����LGD��ID
                ,''                                                                    AS CCFPOOLID               -- ����CCF��ID
                ,CASE WHEN T9.PROJECTNO IS NULL THEN '0'
                      ELSE '1'
                 END                                                                   AS ABSUAFLAG           --�ʲ�֤ȯ�������ʲ���ʶ
                ,CASE WHEN T9.PROJECTNO IS NULL THEN ''
                      ELSE T9.PROJECTNO
                 END                                                                   AS ABSPOOLID           --֤ȯ���ʲ���ID
                ,''                                                                    AS GROUPID                 -- ������
                ,CASE WHEN T6.PDADJCODE='D' THEN TO_DATE(T6.PDVAVLIDDATE,'YYYYMMDD')
                      ELSE NULL
                 END                                                                   AS DefaultDate             -- ΥԼʱ��
                ,0                                                                     AS ABSPROPORTION           --�ʲ�֤ȯ������
                ,0                                                                     AS DEBTORNUMBER            --����˸���
                ,'DG'
    FROM 				RWA_DEV.NCM_BUSINESS_DUEBILL T1
    INNER JOIN 	RWA_DEV.NCM_BUSINESS_TYPE T2
    ON 					T1.BUSINESSTYPE = T2.TYPENO
    AND 				T1.DATANO = T2.DATANO
    AND 				T2.ATTRIBUTE1 <> '2'                    --ֻȡ�Թ�ҵ��
    AND 				T2.TYPENO NOT LIKE '30%'               	--�ų������ҵ��
    INNER JOIN 	RWA_DEV.NCM_BUSINESS_CONTRACT T3
    ON 					T1.RELATIVESERIALNO2 = T3.SERIALNO      --�����Խ��Ϊ׼�����Թ�����ͬʱ����Ӧ�üӺ�ͬ����Ч����
    AND 				T1.DATANO = T3.DATANO
    INNER JOIN 	RWA_DEV.NCM_BUSINESS_HISTORY T31
    ON 					T1.SERIALNO = T31.SERIALNO
    AND 				T31.Balance > 0                   --ֻȡ�����������
    AND 				T31.DATANO = P_DATA_DT_STR
               /* rwa_dev.BRD_LOAN_NOR T31                  --֧��������������
    ON          T1.SERIALNO = T31.CRDT_ACCT_NO
    AND         t31.cur_bal > 0
    AND         t31.datano = P_DATA_DT_STR*/ 
    LEFT JOIN 	RWA.ORG_INFO T4
    ON 				 decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID) = T4.ORGID
    LEFT JOIN 	RWA.CODE_LIBRARY T5
    ON 					T3.DIRECTION = T5.ITEMNO
    AND 				T5.CODENO = 'IndustryType'
    LEFT JOIN 	RWA_DEV.RWA_TEMP_PDLEVEL T6
    ON 					T1.CUSTOMERID = T6.CUSTID
    LEFT JOIN 	(SELECT DISTINCT AA.PROJECTNO,AA.CONTRACTNO   --�ж��Ƿ��ʲ�֤ȯ��
               		 FROM RWA_DEV.NCM_ABS_PROJECT_ASSET AA --��Ŀ���ʲ�
             INNER JOIN RWA_DEV.NCM_ABS_PROJECT_INFO BB  --��Ŀ������Ϣ
               			 ON AA.PROJECTNO = BB.PROJECTNO
               			AND BB.DATANO = P_DATA_DT_STR
               			AND BB.PROJECTSTATUS = '0401'            	--����ɹ�
               		WHERE AA.DATANO = P_DATA_DT_STR
    						) T9
    ON 					T3.SERIALNO = T9.CONTRACTNO
    LEFT JOIN 	RWA_DEV.ncm_rwa_risk_expo_rst T3 --���ձ�¶�����Ϣ��
    ON 					T1.SERIALNO = T3.OBJECTNO
    AND 				T3.OBJECTTYPE = 'BusinessDuebill'
    AND 				T3.DATANO = P_DATA_DT_STR
    LEFT JOIN 	RWA_DEV.RWA_CD_CODE_MAPPING T10 --����ӳ��ת����
    ON  				T3.RISKEXPOSURERESULT = T10.SITEMNO
    AND 				T10.SCODENO = 'RwaResultType'
    LEFT JOIN 	RWA_DEV.NCM_CLASSIFY_RECORD T11 --�弶������Ϣ��
    ON 					T1.RELATIVESERIALNO2 = T11.OBJECTNO
    AND 				T11.OBJECTTYPE = 'TwelveClassify'
    AND 				T11.ISWORK = '1'
    AND 				T11.DATANO = P_DATA_DT_STR
    LEFT JOIN		RWA_DEV.NCM_CUSTOMER_INFO T16
    ON					T1.CUSTOMERID = T16.CUSTOMERID
    AND					T1.DATANO = T16.DATANO
    LEFT JOIN		(
    						select OBJECTNO, DIRECTION
								  from (select T.OBJECTNO,
								               T.DIRECTION,
								               ROW_NUMBER() OVER(PARTITION BY T.OBJECTNO order by T.SERIALNO DESC) AS RM
								          from RWA_DEV.NCM_PUTOUT_SCHEME T  --������ñ�
								         where T.DATANO = P_DATA_DT_STR
								           and T.OBJECTTYPE = 'BusinessContract'
								           and T.DIRECTION IS NOT NULL)
								 where RM = 1
								) CPS									--�����ҵ�����ҵͶ��������ñ�ȡ
    ON					T3.SERIALNO = CPS.OBJECTNO
    LEFT JOIN 	RWA.CODE_LIBRARY CL
    ON 					CPS.DIRECTION = CL.ITEMNO
    AND 				CL.CODENO = 'IndustryType'
    WHERE  			T1.DATANO = P_DATA_DT_STR
    --AND T1.BALANCE>0
    --AND T1.SERIALNO NOT LIKE 'BD%'
    AND 				T1.BUSINESSTYPE NOT IN ('1040101010','1040101020','1040101030','1040102010','1040102020','1040102040','1040105060','1040201010','1040201020','1040202010','105010','10303010'
                                ,'10201010','10202010','1035101010','1035101020','1035102010','1035102020','103550','108010','1040101040',
                                '10201080','10202091' --��ȡ����͢ 20190729���
                                ,'10302020','10302030' --��ȡ���гжһ�Ʊ���� ����Ʊ��ϵͳȡ 20190730���
                                )  --�ų�ͬҵ���ع���Ͷ�ʣ�ί�д���,ת����ҵ���Լ����ǴӺ��ķ��صĽ��
    and T1.SERIALNO not in ('60012004001001','60012004001002','60012004001003','42019999163401') --�ų��⼸�ʳ���Ѻ�� �ѵ�������Ϊ0������ 20190814
    ;
    COMMIT; 

    /*2 ���� �Ŵ�ϵͳ���۽�� ��Ŀ���*/ 
    INSERT INTO RWA_DEV.RWA_XD_EXPOSURE(
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
               ,flag
    )
    SELECT
                TO_DATE(T1.DATANO,'YYYYMMDD')                                           AS DATADATE                -- ��������
                ,T1.DATANO                                                              AS DATANO                  -- ������ˮ��
                ,T1.SERIALNO                                                            AS EXPOSUREID              -- ���ձ�¶ID
                ,T1.SERIALNO                                                            AS DUEID                   -- ծ��ID
                ,'XD'                                                                   AS SSYSID                  -- ԴϵͳID
                ,T1.RELATIVESERIALNO2                                                   AS CONTRACTID              -- ��ͬID
                ,CASE WHEN REPLACE(T1.CUSTOMERID,'NCM_','') IS NULL THEN DECODE(T3.LINETYPE,'0030','XN-GRKH','XN-YBGS')                 --���ͻ�IDΪ�գ�����Ϊ���˾�����Ϊ���˿ͻ�������Ϊһ�㹫˾
                      ELSE T1.CUSTOMERID
                 END                                                                    AS CLIENTID                -- ��������ID
                --,decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID)                      AS SORGID                  -- Դ����ID
                ,case when substr(decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID),1,1)='@' then '01000000'
                     else decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID)
                 end
                ,nvl(T4.ORGNAME,'����')                                                             AS SORGNAME                -- Դ��������
                ,nvl(T4.SORTNO,'1010')                                                              AS ORGSORTNO               -- �������������
                --,decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID)                      AS ORGID                   -- ��������ID
                ,case when substr(decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID),1,1)='@' then '01000000'
                     else decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID)
                 end
                ,nvl(T4.ORGNAME,'����')                                                             AS ORGNAME                 -- ������������
                --,decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID)                      AS ACCORGID                -- �������ID
                ,case when substr(decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID),1,1)='@' then '01000000'
                     else decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID)
                 end
                ,nvl(T4.ORGNAME,'����')                                                             AS ACCORGNAME              -- �����������
                ,NVL(T3.DIRECTION,CPS.DIRECTION)                                        AS INDUSTRYID              -- ������ҵ����
                ,NVL(T5.ITEMNAME,CL.ITEMNAME)                                           AS INDUSTRYNAME            -- ������ҵ����
                ,CASE WHEN T1.BUSINESSCURRENCY<>'CNY' THEN '0102'                            --��ҵı���ҵ��   ����-ó��
                      WHEN T1.BUSINESSTYPE IN('10302010','10302015','10302020') THEN '0401'  --����ҵ��        ͬҵ�����г���
                      WHEN T3.LINETYPE='0010' THEN '0101'
                      WHEN T3.LINETYPE='0020' THEN '0201'
                      WHEN T3.LINETYPE='0030' THEN '0301'
                      WHEN T3.LINETYPE='0040' THEN '0401'
                      ELSE '0101'
                 END                                                                    AS BUSINESSLINE            -- ����
                ,''                                                                     AS ASSETTYPE               -- �ʲ�����
                ,''                                                                     AS ASSETSUBTYPE            -- �ʲ�С��
                ,CASE WHEN T1.BUSINESSTYPE='11103019' AND T3.PURPOSE='010'                  --���ֳ�����ס����Ѻ����
                      THEN '11103040'
                      ELSE T1.BUSINESSTYPE
                 END                                                                    AS BUSINESSTYPEID          --ҵ��Ʒ�ִ���
                ,CASE WHEN T1.BUSINESSTYPE='11103019' AND T3.PURPOSE='010'
                      THEN '�����ۺ����Ѵ���(����ס������)'
                      ELSE T2.TYPENAME
                 END                                                                    AS BUSINESSTYPENAME        -- ҵ��Ʒ������
                --,'02'                                                                   AS CREDITRISKDATATYPE      -- ���÷�����������          01-һ�������
                ,CASE WHEN T16.CUSTOMERTYPE = '0310' OR T16.CERTTYPE LIKE 'Ind%'
                      THEN '02' --����
                      ELSE '01' --������
                  END                                                                   AS CREDITRISKDATATYPE      --���÷�����������
                ,'01'                                                                   AS ASSETTYPEOFHAIRCUTS     -- �ۿ�ϵ����Ӧ�ʲ����     01-�ֽ��ֽ�ȼ���
                ,''                                                                     AS BUSINESSTYPESTD         -- Ȩ�ط�ҵ������
                ,CASE WHEN REPLACE(T1.CUSTOMERID,'NCM_','') IS NULL THEN '0112'                                    --���ͻ�IDΪ�գ�Ĭ�� ����(0112)
                      ELSE ''
                 END                                                                    AS EXPOCLASSSTD            -- Ȩ�ط���¶����
                ,CASE WHEN REPLACE(T1.CUSTOMERID,'NCM_','') IS NULL THEN DECODE(T3.LINETYPE,'0030','011215','011216')                          --���ͻ�IDΪ�գ�����Ϊ���˾�Ĭ�� ��������75%����Ȩ�ص��ʲ�(011215)������Ĭ�� ��������100%����Ȩ�ص��ʲ�(011216)
                      ELSE ''
                 END                                                                    AS EXPOSUBCLASSSTD         -- Ȩ�ط���¶С��
                ,SUBSTR(T12.DITEMNO,1,4)                                                AS EXPOCLASSIRB            -- ��������¶����
                ,T12.DITEMNO                                                            AS EXPOSUBCLASSIRB         -- ��������¶С��
                ,'01'                                                                   AS EXPOBELONG              -- ��¶������ʶ
                ,'01'                                                                   AS BOOKTYPE                -- �˻����           01-�����˻�
                ,'03'                                                                   AS REGUTRANTYPE            -- ��ܽ�������      03-��Ѻ����
                ,'0'                                                                    AS REPOTRANFLAG            -- �ع����ױ�ʶ       0-��
                ,1                                                                      AS REVAFREQUENCY           -- �ع�Ƶ��
                ,NVL(T1.BUSINESSCURRENCY,'CNY')                                         AS CURRENCY                -- ����
                ,T31.Balance                                                            AS NORMALPRINCIPAL         -- �����������
                ,0                                                                      AS OVERDUEBALANCE          -- �������
                ,0                                                                      AS NONACCRUALBALANCE       -- ��Ӧ�����
                ,T31.Balance                                                            AS ONSHEETBALANCE          -- �������
                ,0                                                                      AS NORMALINTEREST          -- ������Ϣ
                ,0                                                                      AS ONDEBITINTEREST         -- ����ǷϢ
                ,0                                                                      AS OFFDEBITINTEREST        -- ����ǷϢ
                ,0                                                                      AS EXPENSERECEIVABLE       -- Ӧ�շ���
                ,T31.Balance                                                            AS ASSETBALANCE            -- �ʲ����
                ,T1.SUBJECTNO                                                           AS ACCSUBJECT1             -- ��Ŀһ
                ,''                                                                     AS ACCSUBJECT2             -- ��Ŀ��
                ,''                                                                     AS ACCSUBJECT3             -- ��Ŀ��
                ,T1.PUTOUTDATE                                                          AS STARTDATE               -- ��ʼ����
                ,T1.ACTUALMATURITY                                                      AS DUEDATE                 -- ��������
                ,CASE WHEN (TO_DATE(T1.ACTUALMATURITY,'YYYYMMDD')-TO_DATE(T1.PUTOUTDATE,'YYYYMMDD'))/365<0
                      THEN 0
                      ELSE (TO_DATE(T1.ACTUALMATURITY,'YYYYMMDD')-TO_DATE(T1.PUTOUTDATE,'YYYYMMDD'))/365
                END                                                                     AS ORIGINALMATURITY        -- ԭʼ����
                ,CASE WHEN (TO_DATE(T1.ACTUALMATURITY,'YYYYMMDD')-TO_DATE(T1.DATANO,'YYYYMMDD'))/365<0
                      THEN 0
                      ELSE (TO_DATE(T1.ACTUALMATURITY,'YYYYMMDD')-TO_DATE(T1.DATANO,'YYYYMMDD'))/365
                END                                                                     AS RESIDUALM               -- ʣ������
                ,CASE WHEN NVL(T1.Classifyresult,SUBSTR(T13.FINALLYRESULT,1,1))='A' THEN '01'
                      WHEN NVL(T1.Classifyresult,SUBSTR(T13.FINALLYRESULT,1,1))='B' THEN '02'       --ʮ��������תΪ�弶����
                      WHEN NVL(T1.Classifyresult,SUBSTR(T13.FINALLYRESULT,1,1))='C' THEN '03'
                      WHEN NVL(T1.Classifyresult,SUBSTR(T13.FINALLYRESULT,1,1))='D' THEN '04'
                      WHEN NVL(T1.Classifyresult,SUBSTR(T13.FINALLYRESULT,1,1))='E' THEN '05'
                      ELSE NVL(T1.Classifyresult,SUBSTR(T13.FINALLYRESULT,1,1))
                 END                                                                    AS RISKCLASSIFY            -- ���շ���
                ,'01'                                                                   AS EXPOSURESTATUS          -- ���ձ�¶״̬  01-����
                ,T1.OVERDUEDAYS                                                         AS OVERDUEDAYS             -- ��������
                ,0                                                                      AS SPECIALPROVISION        -- ר��׼����-������
                ,0                                                                      AS GENERALPROVISION        -- һ��׼����
                ,0                                                                      AS ESPECIALPROVISION       -- �ر�׼����
                ,T1.CANCELSUM                                                           AS WRITTENOFFAMOUNT        -- �Ѻ������
                ,''                                                                     AS OffExpoSource           -- ���Ⱪ¶��Դ
                ,''                                                                     AS OffBusinessType         -- ����ҵ������
                ,''                                                                     AS OffBusinessSdvsSTD      -- Ȩ�ط�����ҵ������ϸ��
                ,'1'                                                                    AS UncondCancelFlag        -- �Ƿ����ʱ����������
                ,''                                                                     AS CCFLevel                -- ����ת��ϵ������
                ,T6.CCFVALUE                                                            AS CCFAIRB                 -- �߼�������ת��ϵ��
                ,'01'                                                                   AS CLAIMSLEVEL             -- ծȨ����
                ,'0'                                                                    AS BONDFLAG                -- �Ƿ�Ϊծȯ
                ,'02'                                                                   AS BONDISSUEINTENT         -- ծȯ����Ŀ��
                ,'0'                                                                    AS NSUREALPROPERTYFLAG     -- �Ƿ�����ò�����
                ,''                                                                     AS REPASSETTERMTYPE        -- ��ծ�ʲ���������
                ,'0'                                                                    AS DEPENDONFPOBFLAG        -- �Ƿ�����������δ��ӯ��
                ,''                                                                     AS IRATING                 -- �ڲ�����
                ,T6.PDVALUE                                                             AS PD                      -- ΥԼ����
                ,''                                                                     AS LGDLEVEL                -- ΥԼ��ʧ�ʼ���
                ,T6.LGDVALUE                                                            AS LGDAIRB                 -- �߼���ΥԼ��ʧ��
                ,0                                                                      AS MAIRB                   -- �߼�����Ч����
                ,T31.Balance                                                            AS EADAIRB                 -- �߼���ΥԼ���ձ�¶
                ,CASE WHEN T6.DEFAULTFLAG='1' THEN '1'
                      ELSE '0'
                 END                                                                    AS DEFAULTFLAG             -- ΥԼ��ʶ
                ,T6.BEELVALUE                                                           AS BEEL                    -- ��ΥԼ��¶Ԥ����ʧ����
                ,T6.LGDVALUE                                                            AS DEFAULTLGD              -- ��ΥԼ��¶ΥԼ��ʧ��
                ,'0'                                                                    AS EQUITYEXPOFLAG          -- ��Ȩ��¶��ʶ
                ,''                                                                     AS EQUITYINVESTTYPE        -- ��ȨͶ�ʶ�������
                ,''                                                                     AS EQUITYINVESTCAUSE       -- ��ȨͶ���γ�ԭ��
                ,'0'                                                                    AS SLFLAG                  -- רҵ�����ʶ       רҵ��������ֶ�һ���ȸ���
                ,''                                                                     AS SLTYPE                  -- רҵ��������
                ,''                                                                     AS PFPHASE                 -- ��Ŀ���ʽ׶�
                ,'01'                                                                   AS REGURATING              -- �������
                ,''                                                                     AS CBRCMPRATINGFLAG        -- ������϶������Ƿ��Ϊ����
                ,''                                                                     AS LARGEFLUCFLAG           -- �Ƿ񲨶��Խϴ�
                ,'0'                                                                    AS LIQUEXPOFLAG            -- �Ƿ���������з��ձ�¶
                ,''                                                                     AS PAYMENTDEALFLAG         -- �Ƿ����Ը�ģʽ
                ,0                                                                      AS DELAYTRADINGDAYS        -- �ӳٽ�������
                ,'0'                                                                    AS SECURITIESFLAG          -- �м�֤ȯ��ʶ
                ,''                                                                     AS SECUISSUERID            -- ֤ȯ������ID
                ,''                                                                     AS RATINGDURATIONTYPE      -- ������������
                ,''                                                                     AS SECUISSUERATING         -- ֤ȯ���еȼ�
                ,0                                                                      AS SECURESIDUALM           -- ֤ȯʣ������
                ,1                                                                      AS SECUREVAFREQUENCY       -- ֤ȯ�ع�Ƶ��
                ,'0'                                                                    AS CCPTRANFLAG             -- �Ƿ����뽻�׶�����ؽ���
                ,''                                                                     AS CCPID                   -- ���뽻�׶���ID
                ,'0'                                                                    AS QUALCCPFLAG             -- �Ƿ�ϸ����뽻�׶���
                ,''                                                                     AS BANKROLE                -- ���н�ɫ
                ,''                                                                     AS CLEARINGMETHOD          -- ���㷽ʽ
                ,'0'                                                                    AS BANKASSETFLAG           -- �Ƿ������ύ�ʲ�
                ,''                                                                     AS MATCHCONDITIONS         -- �����������
                ,'0'                                                                    AS SFTFLAG                 -- ֤ȯ���ʽ��ױ�ʶ
                ,''                                                                     AS MASTERNETAGREEFLAG      -- ���������Э���ʶ
                ,''                                                                     AS MASTERNETAGREEID        -- ���������Э��ID
                ,''                                                                     AS SFTTYPE                 -- ֤ȯ���ʽ�������
                ,''                                                                     AS SECUOWNERTRANSFLAG      -- ֤ȯ����Ȩ�Ƿ�ת��
                ,'0'                                                                    AS OTCFLAG                 -- �����������߱�ʶ
                ,''                                                                     AS VALIDNETTINGFLAG        -- ��Ч�������Э���ʶ
                ,''                                                                     AS VALIDNETAGREEMENTID     -- ��Ч�������Э��ID
                ,''                                                                     AS OTCTYPE                 -- ����������������
                ,''                                                                     AS DEPOSITRISKPERIOD       -- ��֤������ڼ�
                ,0                                                                      AS MTM                     -- ���óɱ�
                ,''                                                                     AS MTMCURRENCY             -- ���óɱ�����
                ,''                                                                     AS BUYERORSELLER           -- ������
                ,''                                                                     AS QUALROFLAG              -- �ϸ�����ʲ���ʶ
                ,''                                                                     AS ROISSUERPERFORMFLAG     -- �����ʲ��������Ƿ�����Լ
                ,''                                                                     AS BUYERINSOLVENCYFLAG     -- ���ñ������Ƿ��Ʋ�
                ,0                                                                      AS NONPAYMENTFEES          -- ��δ֧������
                ,'1'                                                                    AS RETAILEXPOFLAG          -- ���۱�¶��ʶ
                ,CASE WHEN T6.RISK_EXPOSURE='01' THEN '020401'
                      WHEN T6.RISK_EXPOSURE='02' THEN '020403'
                      ELSE '020402'
                 END                                                                    AS RETAILCLAIMTYPE         -- ����ծȨ����
                ,CASE WHEN T6.RISK_EXPOSURE='01' THEN '01'
                      ELSE '02'
                 END                                                                    AS MORTGAGETYPE            -- ס����Ѻ��������
                ,1                                                                      AS EXPONUMBER              -- ���ձ�¶����
                ,0.8                                                                    AS LTV                     --�����ֵ��  ͳһ����
                ,T6.MOB                                                                 AS AGING                   --����
                ,CASE WHEN T1.NEWDEFAULTFLAG='0' THEN '1'
                      ELSE '0'
                 END                                                                    AS NEWDEFAULTDEBTFLAG      --����ΥԼծ���ʶ
                ,T6.PDMODELCODE                                                         AS PDPOOLMODELID           -- PD�ֳ�ģ��ID
                ,T6.LGDMODELCODE                                                        AS LGDPOOLMODELID          -- LGD�ֳ�ģ��ID
                ,T6.CCFMODELCODE                                                        AS CCFPOOLMODELID          -- CCF�ֳ�ģ��ID
                ,T6.PDCODE                                                              AS PDPOOLID                -- ����PD��ID
                ,T6.LGDCODE                                                             AS LGDPOOLID               -- ����LGD��ID
                ,T6.CCFCODE                                                             AS CCFPOOLID               -- ����CCF��ID
                ,CASE WHEN T10.PROJECTNO IS NULL THEN '0'
                      ELSE '1'
                 END                                                                    AS ABSUAFLAG           --�ʲ�֤ȯ�������ʲ���ʶ
                ,CASE WHEN T10.PROJECTNO IS NULL THEN ''
                      ELSE T10.PROJECTNO
                 END                                                                    AS ABSPOOLID           --֤ȯ���ʲ���ID
                ,''                                                                     AS GROUPID                 -- ������
                ,CASE WHEN T6.DEFAULTFLAG='1' THEN TO_DATE(T6.UPDATETIME,'YYYYMMDD')
                      ELSE NULL
                 END                                                                    AS DefaultDate             -- ΥԼʱ��
                ,0                                                                      AS ABSPROPORTION           --�ʲ�֤ȯ������
                ,0                                                                      AS DEBTORNUMBER            --����˸���
                ,'LS'
    FROM        RWA_DEV.NCM_BUSINESS_DUEBILL T1
    INNER JOIN  RWA_DEV.NCM_BUSINESS_TYPE T2
    ON          T1.BUSINESSTYPE = T2.TYPENO
    AND         T1.DATANO = T2.DATANO
    AND         T2.ATTRIBUTE1 = '2'                       --ֻȡ����ҵ��
    INNER JOIN  RWA_DEV.NCM_BUSINESS_CONTRACT T3
    ON          T1.RELATIVESERIALNO2 = T3.SERIALNO        --�����Խ��Ϊ׼�����Թ�����ͬʱ����Ӧ�üӺ�ͬ����Ч����
    AND         T1.DATANO = T3.DATANO
    INNER JOIN  RWA_DEV.NCM_BUSINESS_HISTORY T31
    ON          T1.SERIALNO = T31.SERIALNO
    AND         T31.Balance > 0                     --ֻȡ�����������
    AND         T31.DATANO = P_DATA_DT_STR
              /*  rwa_dev.brd_loan_nor t31
    ON          t1.serialno=t31.crdt_acct_no
    AND         t31.cur_bal > 0 
    AND         t31.DATANO = P_DATA_DT_STR*/
    LEFT JOIN   RWA.ORG_INFO T4
    ON          decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID) = T4.ORGID
    LEFT JOIN   RWA.CODE_LIBRARY T5
    ON          T3.DIRECTION = T5.ITEMNO
    AND         T5.CODENO = 'IndustryType'
    LEFT JOIN   RWA_DEV.RWA_TEMP_LGDLEVEL T6              --����ծ������
    ON          T1.RELATIVESERIALNO2 = T6.BUSINESSID
    AND         T6.BUSINESSTYPE <> 'CREDITCARD'               -- ��ȡ���ÿ�
    LEFT JOIN   (SELECT DISTINCT AA.PROJECTNO,AA.CONTRACTNO   --�ж��Ƿ��ʲ�֤ȯ��
                   FROM RWA_DEV.NCM_ABS_PROJECT_ASSET AA
             INNER JOIN RWA_DEV.NCM_ABS_PROJECT_INFO BB
                     ON AA.PROJECTNO = BB.PROJECTNO
                    AND BB.DATANO = P_DATA_DT_STR
                    AND BB.PROJECTSTATUS = '0401'             --����ɹ�
                  WHERE AA.DATANO = P_DATA_DT_STR
                ) T10
    ON          T3.SERIALNO = T10.CONTRACTNO
    LEFT JOIN   RWA_DEV.ncm_rwa_risk_expo_rst T11
    ON          T1.SERIALNO = T11.OBJECTNO
    AND         T11.OBJECTTYPE = 'BusinessDuebill'
    AND         T11.DATANO = P_DATA_DT_STR
    LEFT JOIN   RWA_DEV.RWA_CD_CODE_MAPPING T12
    ON          T11.RISKEXPOSURERESULT=T12.SITEMNO
    AND         T12.SCODENO = 'RwaResultType'
    LEFT JOIN   RWA_DEV.NCM_CLASSIFY_RECORD T13
    ON          T1.RELATIVESERIALNO2=T13.OBJECTNO
    AND         T13.OBJECTTYPE = 'TwelveClassify'
    AND         T13.ISWORK = '1'
    AND         T13.DATANO = P_DATA_DT_STR
    LEFT JOIN   RWA_DEV.NCM_CUSTOMER_INFO T16
    ON					T1.CUSTOMERID = T16.CUSTOMERID
    AND					T1.DATANO = T16.DATANO
    LEFT JOIN   (
                select OBJECTNO, DIRECTION
                  from (select T.OBJECTNO,
                               T.DIRECTION,
                               ROW_NUMBER() OVER(PARTITION BY T.OBJECTNO order by T.SERIALNO DESC) AS RM
                          from RWA_DEV.NCM_PUTOUT_SCHEME T
                         where T.DATANO = P_DATA_DT_STR
                           and T.OBJECTTYPE = 'BusinessContract'
                           and T.DIRECTION IS NOT NULL)
                 where RM = 1
                ) CPS                 --�����ҵ�����ҵͶ��������ñ�ȡ
    ON          T3.SERIALNO = CPS.OBJECTNO
    LEFT JOIN   RWA.CODE_LIBRARY CL
    ON          CPS.DIRECTION = CL.ITEMNO
    AND         CL.CODENO = 'IndustryType'
    WHERE       T1.DATANO=P_DATA_DT_STR
    --AND T1.BALANCE>0
    --AND T1.SERIALNO NOT LIKE 'BD%'
    AND         T1.BUSINESSTYPE NOT IN ('11105010','11105020','11103030')  --�ų�����ί�д���ҵ�� �ų�΢����
    ;
    COMMIT;
    
    /*���ֽ�ݱ�Ŀ�Ŀû�гɹ�ת��@,�ò��ֿ�Ŀ����brd_loan_nor��Ӧ�Ŀ�ĿΪ13100000,71090101;
      13100000�������������ڲ����Ѿ����룬71090101�Ѻ�������ȡ��*/
       DELETE FROM RWA_XD_EXPOSURE T
        WHERE T.ACCSUBJECT1 LIKE '@%'
        AND T.DATANO=p_data_dt_str
          AND EXISTS (SELECT 1
                 FROM BRD_LOAN_NOR B
                WHERE T.DUEID = B.CRDT_ACCT_NO
                  AND T.CONTRACTID = B.CONTRACT_NO
                  AND T.DATANO = B.DATANO
                  AND B.SBJT_CD IN ('13100000', '71090101'));
         COMMIT;
      
    
    /*3 �Ŵ�ϵͳ���۽��-΢����*/
    INSERT INTO RWA_DEV.RWA_XD_EXPOSURE(
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
               ,flag
    )
    SELECT
                TO_DATE(T1.DATANO,'YYYYMMDD')                                           AS DATADATE                -- ��������
                ,T1.DATANO                                                              AS DATANO                  -- ������ˮ��
                ,T1.SERIALNO                                                            AS EXPOSUREID              -- ���ձ�¶ID
                ,T1.SERIALNO                                                            AS DUEID                   -- ծ��ID
                ,'XD'                                                                   AS SSYSID                  -- ԴϵͳID
                ,T1.RELATIVESERIALNO2                                                   AS CONTRACTID              -- ��ͬID
                ,CASE WHEN REPLACE(T1.CUSTOMERID,'NCM_','') IS NULL THEN DECODE(T3.LINETYPE,'0030','XN-GRKH','XN-YBGS')                 --���ͻ�IDΪ�գ�����Ϊ���˾�����Ϊ���˿ͻ�������Ϊһ�㹫˾
                      ELSE T1.CUSTOMERID
                 END                                                                    AS CLIENTID                -- ��������ID
                --,decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID)                      AS SORGID                  -- Դ����ID
                ,case when substr(decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID),1,1)='@' then '01000000'
                     else decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID)
                 end
                ,nvl(T4.ORGNAME,'����')                                                             AS SORGNAME                -- Դ��������
                ,nvl(T4.SORTNO,'1010')                                                              AS ORGSORTNO               -- �������������
                --,decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID)                      AS ORGID                   -- ��������ID
                ,case when substr(decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID),1,1)='@' then '01000000'
                     else decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID)
                 end
                ,nvl(T4.ORGNAME,'����')                                                              AS ORGNAME                 -- ������������
                --,decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID)                      AS ACCORGID                -- �������ID
                ,case when substr(decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID),1,1)='@' then '01000000'
                     else decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID)
                 end
                ,nvl(T4.ORGNAME,'����')                                                              AS ACCORGNAME              -- �����������
                ,NVL(T3.DIRECTION,CPS.DIRECTION)                                        AS INDUSTRYID              -- ������ҵ����
                ,NVL(T5.ITEMNAME,CL.ITEMNAME)                                           AS INDUSTRYNAME            -- ������ҵ����
                ,CASE WHEN T1.BUSINESSCURRENCY<>'CNY' THEN '0102'                            --��ҵı���ҵ��   ����-ó��
                      WHEN T1.BUSINESSTYPE IN('10302010','10302015','10302020') THEN '0401'  --����ҵ��        ͬҵ�����г���
                      WHEN T3.LINETYPE='0010' THEN '0101'
                      WHEN T3.LINETYPE='0020' THEN '0201'
                      WHEN T3.LINETYPE='0030' THEN '0301'
                      WHEN T3.LINETYPE='0040' THEN '0401'
                      ELSE '0101'
                 END                                                                    AS BUSINESSLINE            -- ����
                ,''                                                                     AS ASSETTYPE               -- �ʲ�����
                ,''                                                                     AS ASSETSUBTYPE            -- �ʲ�С��
                ,'11103030'                                                             AS BUSINESSTYPEID          --ҵ��Ʒ�ִ���
                ,'΢����'   AS BUSINESSTYPENAME        -- ҵ��Ʒ������
                --,'02'                                                                   AS CREDITRISKDATATYPE      -- ���÷�����������          01-һ�������
                ,CASE WHEN T16.CUSTOMERTYPE = '0310' OR T16.CERTTYPE LIKE 'Ind%'
                      THEN '02' --����
                      ELSE '01' --������
                  END                                                                   AS CREDITRISKDATATYPE      --���÷�����������
                ,'01'                                                                   AS ASSETTYPEOFHAIRCUTS     -- �ۿ�ϵ����Ӧ�ʲ����     01-�ֽ��ֽ�ȼ���
                ,''                                                                     AS BUSINESSTYPESTD         -- Ȩ�ط�ҵ������
                ,CASE WHEN REPLACE(T1.CUSTOMERID,'NCM_','') IS NULL THEN '0112'                                    --���ͻ�IDΪ�գ�Ĭ�� ����(0112)
                      ELSE ''
                 END                                                                    AS EXPOCLASSSTD            -- Ȩ�ط���¶����
                ,CASE WHEN REPLACE(T1.CUSTOMERID,'NCM_','') IS NULL THEN DECODE(T3.LINETYPE,'0030','011215','011216')                          --���ͻ�IDΪ�գ�����Ϊ���˾�Ĭ�� ��������75%����Ȩ�ص��ʲ�(011215)������Ĭ�� ��������100%����Ȩ�ص��ʲ�(011216)
                      ELSE ''
                 END                                                                    AS EXPOSUBCLASSSTD         -- Ȩ�ط���¶С��
                ,SUBSTR(T12.DITEMNO,1,4)                                                AS EXPOCLASSIRB            -- ��������¶����
                ,T12.DITEMNO                                                            AS EXPOSUBCLASSIRB         -- ��������¶С��
                ,'01'                                                                   AS EXPOBELONG              -- ��¶������ʶ
                ,'01'                                                                   AS BOOKTYPE                -- �˻����           01-�����˻�
                ,'03'                                                                   AS REGUTRANTYPE            -- ��ܽ�������      03-��Ѻ����
                ,'0'                                                                    AS REPOTRANFLAG            -- �ع����ױ�ʶ       0-��
                ,1                                                                      AS REVAFREQUENCY           -- �ع�Ƶ��
                ,case when t31.ccy_cd ='01' or t31.ccy_cd is null
                      then 'CNY'  end                                                   AS CURRENCY                -- ����
                ,T1.BALANCE                                                            AS NORMALPRINCIPAL         -- �����������
                ,0                                                                      AS OVERDUEBALANCE          -- �������
                ,0                                                                      AS NONACCRUALBALANCE       -- ��Ӧ�����
                ,T1.BALANCE                                                            AS ONSHEETBALANCE          -- �������
                ,0                                                                      AS NORMALINTEREST          -- ������Ϣ
                ,0                                                                      AS ONDEBITINTEREST         -- ����ǷϢ
                ,0                                                                      AS OFFDEBITINTEREST        -- ����ǷϢ
                ,0                                                                      AS EXPENSERECEIVABLE       -- Ӧ�շ���
                ,T1.BALANCE                                                            AS ASSETBALANCE            -- �ʲ����
                ,T31.sbjt_cd                                                           AS ACCSUBJECT1             -- ��Ŀһ
                ,''                                                                     AS ACCSUBJECT2             -- ��Ŀ��
                ,''                                                                     AS ACCSUBJECT3             -- ��Ŀ��
                ,T1.PUTOUTDATE                                                          AS STARTDATE               -- ��ʼ����
                ,T1.ACTUALMATURITY                                                      AS DUEDATE                 -- ��������
                ,CASE WHEN (TO_DATE(T1.ACTUALMATURITY,'YYYYMMDD')-TO_DATE(T1.PUTOUTDATE,'YYYYMMDD'))/365<0
                      THEN 0
                      ELSE (TO_DATE(T1.ACTUALMATURITY,'YYYYMMDD')-TO_DATE(T1.PUTOUTDATE,'YYYYMMDD'))/365
                END                                                                     AS ORIGINALMATURITY        -- ԭʼ����
                ,CASE WHEN (TO_DATE(T1.ACTUALMATURITY,'YYYYMMDD')-TO_DATE(T1.DATANO,'YYYYMMDD'))/365<0
                      THEN 0
                      ELSE (TO_DATE(T1.ACTUALMATURITY,'YYYYMMDD')-TO_DATE(T1.DATANO,'YYYYMMDD'))/365
                END                                                                     AS RESIDUALM               -- ʣ������
                ,CASE WHEN NVL(T1.Classifyresult,SUBSTR(T13.FINALLYRESULT,1,1))='A' THEN '01'
                      WHEN NVL(T1.Classifyresult,SUBSTR(T13.FINALLYRESULT,1,1))='B' THEN '02'       --ʮ��������תΪ�弶����
                      WHEN NVL(T1.Classifyresult,SUBSTR(T13.FINALLYRESULT,1,1))='C' THEN '03'
                      WHEN NVL(T1.Classifyresult,SUBSTR(T13.FINALLYRESULT,1,1))='D' THEN '04'
                      WHEN NVL(T1.Classifyresult,SUBSTR(T13.FINALLYRESULT,1,1))='E' THEN '05'
                      ELSE NVL(T1.Classifyresult,SUBSTR(T13.FINALLYRESULT,1,1))
                 END                                                                    AS RISKCLASSIFY            -- ���շ���
                ,'01'                                                                   AS EXPOSURESTATUS          -- ���ձ�¶״̬  01-����
                ,T1.OVERDUEDAYS                                                         AS OVERDUEDAYS             -- ��������
                ,0                                                                      AS SPECIALPROVISION        -- ר��׼����-������
                ,0                                                                      AS GENERALPROVISION        -- һ��׼����
                ,0                                                                      AS ESPECIALPROVISION       -- �ر�׼����
                ,T1.CANCELSUM                                                           AS WRITTENOFFAMOUNT        -- �Ѻ������
                ,''                                                                     AS OffExpoSource           -- ���Ⱪ¶��Դ
                ,''                                                                     AS OffBusinessType         -- ����ҵ������
                ,''                                                                     AS OffBusinessSdvsSTD      -- Ȩ�ط�����ҵ������ϸ��
                ,'1'                                                                    AS UncondCancelFlag        -- �Ƿ����ʱ����������
                ,''                                                                     AS CCFLevel                -- ����ת��ϵ������
                ,T6.CCFVALUE                                                            AS CCFAIRB                 -- �߼�������ת��ϵ��
                ,'01'                                                                   AS CLAIMSLEVEL             -- ծȨ����
                ,'0'                                                                    AS BONDFLAG                -- �Ƿ�Ϊծȯ
                ,'02'                                                                   AS BONDISSUEINTENT         -- ծȯ����Ŀ��
                ,'0'                                                                    AS NSUREALPROPERTYFLAG     -- �Ƿ�����ò�����
                ,''                                                                     AS REPASSETTERMTYPE        -- ��ծ�ʲ���������
                ,'0'                                                                    AS DEPENDONFPOBFLAG        -- �Ƿ�����������δ��ӯ��
                ,''                                                                     AS IRATING                 -- �ڲ�����
                ,T6.PDVALUE                                                             AS PD                      -- ΥԼ����
                ,''                                                                     AS LGDLEVEL                -- ΥԼ��ʧ�ʼ���
                ,T6.LGDVALUE                                                            AS LGDAIRB                 -- �߼���ΥԼ��ʧ��
                ,0                                                                      AS MAIRB                   -- �߼�����Ч����
                ,T1.BALANCE                                                            AS EADAIRB                 -- �߼���ΥԼ���ձ�¶
                ,CASE WHEN T6.DEFAULTFLAG='1' THEN '1'
                      ELSE '0'
                 END                                                                    AS DEFAULTFLAG             -- ΥԼ��ʶ
                ,T6.BEELVALUE                                                           AS BEEL                    -- ��ΥԼ��¶Ԥ����ʧ����
                ,T6.LGDVALUE                                                            AS DEFAULTLGD              -- ��ΥԼ��¶ΥԼ��ʧ��
                ,'0'                                                                    AS EQUITYEXPOFLAG          -- ��Ȩ��¶��ʶ
                ,''                                                                     AS EQUITYINVESTTYPE        -- ��ȨͶ�ʶ�������
                ,''                                                                     AS EQUITYINVESTCAUSE       -- ��ȨͶ���γ�ԭ��
                ,'0'                                                                    AS SLFLAG                  -- רҵ�����ʶ       רҵ��������ֶ�һ���ȸ���
                ,''                                                                     AS SLTYPE                  -- רҵ��������
                ,''                                                                     AS PFPHASE                 -- ��Ŀ���ʽ׶�
                ,'01'                                                                   AS REGURATING              -- �������
                ,''                                                                     AS CBRCMPRATINGFLAG        -- ������϶������Ƿ��Ϊ����
                ,''                                                                     AS LARGEFLUCFLAG           -- �Ƿ񲨶��Խϴ�
                ,'0'                                                                    AS LIQUEXPOFLAG            -- �Ƿ���������з��ձ�¶
                ,''                                                                     AS PAYMENTDEALFLAG         -- �Ƿ����Ը�ģʽ
                ,0                                                                      AS DELAYTRADINGDAYS        -- �ӳٽ�������
                ,'0'                                                                    AS SECURITIESFLAG          -- �м�֤ȯ��ʶ
                ,''                                                                     AS SECUISSUERID            -- ֤ȯ������ID
                ,''                                                                     AS RATINGDURATIONTYPE      -- ������������
                ,''                                                                     AS SECUISSUERATING         -- ֤ȯ���еȼ�
                ,0                                                                      AS SECURESIDUALM           -- ֤ȯʣ������
                ,1                                                                      AS SECUREVAFREQUENCY       -- ֤ȯ�ع�Ƶ��
                ,'0'                                                                    AS CCPTRANFLAG             -- �Ƿ����뽻�׶�����ؽ���
                ,''                                                                     AS CCPID                   -- ���뽻�׶���ID
                ,'0'                                                                    AS QUALCCPFLAG             -- �Ƿ�ϸ����뽻�׶���
                ,''                                                                     AS BANKROLE                -- ���н�ɫ
                ,''                                                                     AS CLEARINGMETHOD          -- ���㷽ʽ
                ,'0'                                                                    AS BANKASSETFLAG           -- �Ƿ������ύ�ʲ�
                ,''                                                                     AS MATCHCONDITIONS         -- �����������
                ,'0'                                                                    AS SFTFLAG                 -- ֤ȯ���ʽ��ױ�ʶ
                ,''                                                                     AS MASTERNETAGREEFLAG      -- ���������Э���ʶ
                ,''                                                                     AS MASTERNETAGREEID        -- ���������Э��ID
                ,''                                                                     AS SFTTYPE                 -- ֤ȯ���ʽ�������
                ,''                                                                     AS SECUOWNERTRANSFLAG      -- ֤ȯ����Ȩ�Ƿ�ת��
                ,'0'                                                                    AS OTCFLAG                 -- �����������߱�ʶ
                ,''                                                                     AS VALIDNETTINGFLAG        -- ��Ч�������Э���ʶ
                ,''                                                                     AS VALIDNETAGREEMENTID     -- ��Ч�������Э��ID
                ,''                                                                     AS OTCTYPE                 -- ����������������
                ,''                                                                     AS DEPOSITRISKPERIOD       -- ��֤������ڼ�
                ,0                                                                      AS MTM                     -- ���óɱ�
                ,''                                                                     AS MTMCURRENCY             -- ���óɱ�����
                ,''                                                                     AS BUYERORSELLER           -- ������
                ,''                                                                     AS QUALROFLAG              -- �ϸ�����ʲ���ʶ
                ,''                                                                     AS ROISSUERPERFORMFLAG     -- �����ʲ��������Ƿ�����Լ
                ,''                                                                     AS BUYERINSOLVENCYFLAG     -- ���ñ������Ƿ��Ʋ�
                ,0                                                                      AS NONPAYMENTFEES          -- ��δ֧������
                ,'1'                                                                    AS RETAILEXPOFLAG          -- ���۱�¶��ʶ
                ,CASE WHEN T6.RISK_EXPOSURE='01' THEN '020401'
                      WHEN T6.RISK_EXPOSURE='02' THEN '020403'
                      ELSE '020402'
                 END                                                                    AS RETAILCLAIMTYPE         -- ����ծȨ����
                ,CASE WHEN T6.RISK_EXPOSURE='01' THEN '01'
                      ELSE '02'
                 END                                                                    AS MORTGAGETYPE            -- ס����Ѻ��������
                ,1                                                                      AS EXPONUMBER              -- ���ձ�¶����
                ,0.8                                                                    AS LTV                     --�����ֵ��  ͳһ����
                ,T6.MOB                                                                 AS AGING                   --����
                ,CASE WHEN T1.NEWDEFAULTFLAG='0' THEN '1'
                      ELSE '0'
                 END                                                                    AS NEWDEFAULTDEBTFLAG      --����ΥԼծ���ʶ
                ,T6.PDMODELCODE                                                         AS PDPOOLMODELID           -- PD�ֳ�ģ��ID
                ,T6.LGDMODELCODE                                                        AS LGDPOOLMODELID          -- LGD�ֳ�ģ��ID
                ,T6.CCFMODELCODE                                                        AS CCFPOOLMODELID          -- CCF�ֳ�ģ��ID
                ,T6.PDCODE                                                              AS PDPOOLID                -- ����PD��ID
                ,T6.LGDCODE                                                             AS LGDPOOLID               -- ����LGD��ID
                ,T6.CCFCODE                                                             AS CCFPOOLID               -- ����CCF��ID
                ,CASE WHEN T10.PROJECTNO IS NULL THEN '0'
                      ELSE '1'
                 END                                                                    AS ABSUAFLAG           --�ʲ�֤ȯ�������ʲ���ʶ
                ,CASE WHEN T10.PROJECTNO IS NULL THEN ''
                      ELSE T10.PROJECTNO
                 END                                                                    AS ABSPOOLID           --֤ȯ���ʲ���ID
                ,''                                                                     AS GROUPID                 -- ������
                ,CASE WHEN T6.DEFAULTFLAG='1' THEN TO_DATE(T6.UPDATETIME,'YYYYMMDD')
                      ELSE NULL
                 END                                                                    AS DefaultDate             -- ΥԼʱ��
                ,0                                                                      AS ABSPROPORTION           --�ʲ�֤ȯ������
                ,0                                                                      AS DEBTORNUMBER            --����˸���
                ,'WLD'
    FROM        RWA_DEV.NCM_BUSINESS_DUEBILL T1
    INNER JOIN  RWA_DEV.NCM_BUSINESS_CONTRACT T3
    ON          T1.RELATIVESERIALNO2 = T3.SERIALNO        --�����Խ��Ϊ׼�����Թ�����ͬʱ����Ӧ�üӺ�ͬ����Ч����
    AND         T1.DATANO = T3.DATANO
    INNER JOIN  rwa_dev.brd_loan_nor t31
    ON          t1.serialno=t31.crdt_acct_no
    AND         t31.cur_bal > 0 
    AND         t31.DATANO = P_DATA_DT_STR
  and     t31.sbjt_cd in('13030206','13030106') --13030106/13030206 ���˶���/�г���΢��������� 
    LEFT JOIN   RWA.ORG_INFO T4
    ON           decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID) = T4.ORGID
    LEFT JOIN   RWA.CODE_LIBRARY T5
    ON          T3.DIRECTION = T5.ITEMNO
    AND         T5.CODENO = 'IndustryType'
    LEFT JOIN   RWA_DEV.RWA_TEMP_LGDLEVEL T6              --����ծ������
    ON          T1.RELATIVESERIALNO2 = T6.BUSINESSID
    AND         T6.BUSINESSTYPE <> 'CREDITCARD'               -- ��ȡ���ÿ�
    LEFT JOIN   (SELECT DISTINCT AA.PROJECTNO,AA.CONTRACTNO   --�ж��Ƿ��ʲ�֤ȯ��
                   FROM RWA_DEV.NCM_ABS_PROJECT_ASSET AA
             INNER JOIN RWA_DEV.NCM_ABS_PROJECT_INFO BB
                     ON AA.PROJECTNO = BB.PROJECTNO
                    AND BB.DATANO = P_DATA_DT_STR
                    AND BB.PROJECTSTATUS = '0401'             --����ɹ�
                  WHERE AA.DATANO = P_DATA_DT_STR
                ) T10
    ON          T3.SERIALNO = T10.CONTRACTNO
    LEFT JOIN   RWA_DEV.ncm_rwa_risk_expo_rst T11
    ON          T1.SERIALNO = T11.OBJECTNO
    AND         T11.OBJECTTYPE = 'BusinessDuebill'
    AND         T11.DATANO = P_DATA_DT_STR
    LEFT JOIN   RWA_DEV.RWA_CD_CODE_MAPPING T12
    ON          T11.RISKEXPOSURERESULT=T12.SITEMNO
    AND         T12.SCODENO = 'RwaResultType'
    LEFT JOIN   RWA_DEV.NCM_CLASSIFY_RECORD T13
    ON          T1.RELATIVESERIALNO2=T13.OBJECTNO
    AND         T13.OBJECTTYPE = 'TwelveClassify'
    AND         T13.ISWORK = '1'
    AND         T13.DATANO = P_DATA_DT_STR
    LEFT JOIN   RWA_DEV.NCM_CUSTOMER_INFO T16
    ON          T1.CUSTOMERID = T16.CUSTOMERID
    AND         T1.DATANO = T16.DATANO
    LEFT JOIN   (
                select OBJECTNO, DIRECTION
                  from (select T.OBJECTNO,
                               T.DIRECTION,
                               ROW_NUMBER() OVER(PARTITION BY T.OBJECTNO order by T.SERIALNO DESC) AS RM
                          from RWA_DEV.NCM_PUTOUT_SCHEME T
                         where T.DATANO = P_DATA_DT_STR
                           and T.OBJECTTYPE = 'BusinessContract'
                           and T.DIRECTION IS NOT NULL)
                 where RM = 1
                ) CPS                 --�����ҵ�����ҵͶ��������ñ�ȡ
    ON          T3.SERIALNO = CPS.OBJECTNO
    LEFT JOIN   RWA.CODE_LIBRARY CL
    ON          CPS.DIRECTION = CL.ITEMNO
    AND         CL.CODENO = 'IndustryType'
    WHERE       T1.DATANO=P_DATA_DT_STR
    ;
    commit;
    
    /*4 ������Ҫ������֤�������������ŵ(���ǶԹ�ҵ��)*/ 
    INSERT INTO RWA_DEV.RWA_XD_EXPOSURE(
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
               ,flag
    )
    SELECT
                TO_DATE(T1.DATANO,'YYYYMMDD')                                           AS DATADATE                -- ��������
                ,T1.DATANO                                                              AS DATANO                  -- ������ˮ��
                ,T1.SERIALNO                                                            AS EXPOSUREID              -- ���ձ�¶ID
                ,T1.SERIALNO                                                            AS DUEID                   -- ծ��ID
                ,'XD'                                                                   AS SSYSID                  -- ԴϵͳID
                ,T1.RELATIVESERIALNO2                                                   AS CONTRACTID              -- ��ͬID
                ,CASE WHEN REPLACE(T1.CUSTOMERID,'NCM_','') IS NULL THEN DECODE(T4.LINETYPE,'0030','XN-GRKH','XN-YBGS')                 --���ͻ�IDΪ�գ�����Ϊ���˾�����Ϊ���˿ͻ�������Ϊһ�㹫˾
                      ELSE T1.CUSTOMERID
                 END                                                                    AS CLIENTID                -- ��������ID
                --,decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID)                      AS SORGID                  -- Դ����ID
                ,case when substr(decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID),1,1)='@' then '01000000'
                     else decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID)
                 end
                ,nvl(T3.ORGNAME,'����')                                                              AS SORGNAME                -- Դ��������
                ,nvl(T3.SORTNO,'1010')                                                              AS ORGSORTNO               -- �������������
                --,decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID)                      AS ORGID                   -- ��������ID
                ,case when substr(decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID),1,1)='@' then '01000000'
                     else decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID)
                 end
                ,nvl(T3.ORGNAME,'����')                                                             AS ORGNAME                 -- ������������
                --,decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID)                      AS ACCORGID                -- �������ID
                ,case when substr(decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID),1,1)='@' then '01000000'
                     else decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID)
                 end
                ,nvl(T3.ORGNAME,'����')                                                             AS ACCORGNAME              -- �����������
                ,NVL(T4.DIRECTION,CPS.DIRECTION)                                        AS INDUSTRYID              -- ������ҵ����
                ,NVL(T5.ITEMNAME,CL.ITEMNAME)                                           AS INDUSTRYNAME            -- ������ҵ����
                ,CASE WHEN T1.BUSINESSTYPE IN('10202010','10201010','1035102010','1035102020') THEN '0102'        --��������֤���羳����   �鵽 ����-ó��
                      WHEN T1.BUSINESSTYPE IN('1035101020','1035101010') THEN '0101' --�����Ա��� �������Ա��� �鵽����-�ͻ� by chengang
                      WHEN T4.LINETYPE='0010' THEN '0101'
                      WHEN T4.LINETYPE='0020' THEN '0201'
                      WHEN T4.LINETYPE='0030' THEN '0301'
                      WHEN T4.LINETYPE='0040' THEN '0401'
                      ELSE '0101'
                 END                                                                    AS BUSINESSLINE            -- ����
                ,''                                                                     AS ASSETTYPE               -- �ʲ�����
                ,''                                                                     AS ASSETSUBTYPE            -- �ʲ�С��
                ,T1.BUSINESSTYPE                                                        AS BUSINESSTYPEID          -- ҵ��Ʒ�ִ���
                ,T2.TYPENAME                                                            AS BUSINESSTYPENAME        -- ҵ��Ʒ������
                --,'01'                                                                   AS CREDITRISKDATATYPE      -- ���÷�����������          01-һ�������
                ,CASE WHEN T16.CUSTOMERTYPE = '0310' OR T16.CERTTYPE LIKE 'Ind%'
                      THEN '02' --����
                      ELSE '01' --������
                  END                                                                   AS CREDITRISKDATATYPE  --���÷�����������
                ,'01'                                                                   AS ASSETTYPEOFHAIRCUTS     -- �ۿ�ϵ����Ӧ�ʲ����     01-�ֽ��ֽ�ȼ���
                ,'07'                                                                   AS BUSINESSTYPESTD         -- Ȩ�ط�ҵ������
                ,CASE WHEN REPLACE(T1.CUSTOMERID,'NCM_','') IS NULL THEN '0112'                                    --���ͻ�IDΪ�գ�Ĭ�� ����(0112)
                      ELSE ''
                 END                                                                    AS EXPOCLASSSTD            -- Ȩ�ط���¶����
                ,CASE WHEN REPLACE(T1.CUSTOMERID,'NCM_','') IS NULL THEN DECODE(T4.LINETYPE,'0030','011215','011216')                          --���ͻ�IDΪ�գ�����Ϊ���˾�Ĭ�� ��������75%����Ȩ�ص��ʲ�(011215)������Ĭ�� ��������100%����Ȩ�ص��ʲ�(011216)
                      ELSE ''
                 END                                                                    AS EXPOSUBCLASSSTD         -- Ȩ�ط���¶С��
                ,SUBSTR(T12.DITEMNO,1,4)                                                AS EXPOCLASSIRB            -- ��������¶����
                ,T12.DITEMNO                                                            AS EXPOSUBCLASSIRB         -- ��������¶С��
                ,CASE WHEN T2.OFFSHEETFLAG='EntOff' THEN '02'   --  02-����
                      ELSE '01'                                 --  01-����
                 END                                                                   AS EXPOBELONG              -- ��¶������ʶ
                ,'01'                                                                  AS BOOKTYPE                -- �˻����           01-�����˻�
                ,'03'                                                                  AS REGUTRANTYPE            -- ��ܽ�������      03-��Ѻ����
                ,'0'                                                                   AS REPOTRANFLAG            -- �ع����ױ�ʶ       0-��
                ,1                                                                     AS REVAFREQUENCY           -- �ع�Ƶ��
                ,NVL(T1.BUSINESSCURRENCY,'CNY')                                        AS CURRENCY                -- ����
                ,T31.BALANCE                                                           AS NORMALPRINCIPAL         -- �����������
                ,0                                                                     AS OVERDUEBALANCE          -- �������
                ,0                                                                     AS NONACCRUALBALANCE       -- ��Ӧ�����
                ,T31.BALANCE                                                           AS ONSHEETBALANCE          -- �������
                ,0                                                                     AS NORMALINTEREST          -- ������Ϣ
                ,0                                                                     AS ONDEBITINTEREST         -- ����ǷϢ
                ,0                                                                     AS OFFDEBITINTEREST        -- ����ǷϢ
                ,0                                                                     AS EXPENSERECEIVABLE       -- Ӧ�շ���
                ,T31.BALANCE                                                           AS ASSETBALANCE            -- �ʲ����
                ,CASE WHEN T1.BUSINESSTYPE IN ('10201010','10202010') THEN '70010100'  --����֤
                      /*T1.BUSINESSTYPE IN ('10201010','10202010')                     --����֤
                      THEN CASE WHEN (TO_DATE(T4.MATURITY,'YYYYMMDD')-TO_DATE(T4.OCCURDATE,'YYYYMMDD'))<=365      --������ȷ�ϣ�����֤������ ������-��������
                                THEN '70010100'                                        --��������֤
                                ELSE '70010200'                                        --Զ������֤
                           END*/
                      WHEN T1.BUSINESSTYPE IN ('1035101010','1035102010') THEN '70180001'  --���ʱ���
                      WHEN T1.BUSINESSTYPE IN ('1035101020','1035102020') THEN '70180002'  --�����ʱ���
                      --WHEN SUBSTR(T1.BUSINESSTYPE,1,6) ='103510' THEN '70180000'       --����
                      WHEN T1.BUSINESSTYPE='103550' THEN '70030000'                    --�����ŵ
                      WHEN T1.BUSINESSTYPE='1040101040' THEN '11112000'                --���뷵�����������ʲ�
                      ELSE '13070100'                                                  --���-�жҵ��
                 END                                                                   AS ACCSUBJECT1             -- ��Ŀһ
                ,''                                                                    AS ACCSUBJECT2             -- ��Ŀ��
                ,''                                                                    AS ACCSUBJECT3             -- ��Ŀ��
                ,NVL(T4.PUTOUTDATE,T4.OCCURDATE)                                       AS STARTDATE               -- ��ʼ����
                ,T4.MATURITY                                                           AS DUEDATE                 --��������
                ,CASE WHEN MONTHS_BETWEEN(TO_DATE(T4.MATURITY,'YYYYMMDD'),TO_DATE(NVL(T4.PUTOUTDATE,T4.OCCURDATE),'YYYYMMDD'))/12<0
                      THEN 0
                      ELSE MONTHS_BETWEEN(TO_DATE(T4.MATURITY,'YYYYMMDD'),TO_DATE(NVL(T4.PUTOUTDATE,T4.OCCURDATE),'YYYYMMDD'))/12
                 END AS OriginalMaturity    --ԭʼ����
                 /*
                ,CASE WHEN MONTHS_BETWEEN(TO_DATE(T4.MATURITY,'YYYYMMDD'),TO_DATE(T1.DATANO,'YYYYMMDD'))/12<0
                                THEN 0
                                ELSE MONTHS_BETWEEN(TO_DATE(T4.MATURITY,'YYYYMMDD'),TO_DATE(T1.DATANO,'YYYYMMDD'))/12
                 END                                                                   AS ResidualM           --ʣ������
                
                
  ------20191127   BY  WZB  ϵͳ�������ް�365�����
                CASE WHEN (TO_DATE(T4.MATURITY,'YYYYMMDD')-TO_DATE(CASE WHEN T1.BUSINESSTYPE IN ('10201010','10202010') THEN NVL(T4.PUTOUTDATE,T4.OCCURDATE)
                                                                       ELSE T4.PUTOUTDATE
                                                                    END,'YYYYMMDD'))/365<0
                      THEN 0
                      ELSE (TO_DATE(T4.MATURITY,'YYYYMMDD')-TO_DATE(CASE WHEN T1.BUSINESSTYPE IN ('10201010','10202010') THEN NVL(T4.PUTOUTDATE,T4.OCCURDATE)
                                                                       ELSE T4.PUTOUTDATE
                                                                    END,'YYYYMMDD'))/365
                END                                                                     AS ORIGINALMATURITY        -- ԭʼ����
                */
                ,CASE WHEN (TO_DATE(T4.MATURITY,'YYYYMMDD')-TO_DATE(T1.DATANO,'YYYYMMDD'))/365<0
                      THEN 0
                      ELSE (TO_DATE(T4.MATURITY,'YYYYMMDD')-TO_DATE(T1.DATANO,'YYYYMMDD'))/365
                END                                                                 AS RESIDUALM               -- ʣ������
                ,CASE WHEN NVL(T1.Classifyresult,SUBSTR(T13.FINALLYRESULT,1,1))='A' THEN '01'
                      WHEN NVL(T1.Classifyresult,SUBSTR(T13.FINALLYRESULT,1,1))='B' THEN '02'       --ʮ��������תΪ�弶����
                      WHEN NVL(T1.Classifyresult,SUBSTR(T13.FINALLYRESULT,1,1))='C' THEN '03'
                      WHEN NVL(T1.Classifyresult,SUBSTR(T13.FINALLYRESULT,1,1))='D' THEN '04'
                      WHEN NVL(T1.Classifyresult,SUBSTR(T13.FINALLYRESULT,1,1))='E' THEN '05'
                      ELSE NVL(T1.Classifyresult,SUBSTR(T13.FINALLYRESULT,1,1))
                 END                                                                   AS RISKCLASSIFY            -- ���շ���
                ,'01'                                                                  AS EXPOSURESTATUS          -- ���ձ�¶״̬  01-����
                ,0                                                                     AS OVERDUEDAYS             -- ��������
                ,0                                                                     AS SPECIALPROVISION        -- ר��׼����-������
                ,0                                                                     AS GENERALPROVISION        -- һ��׼����
                ,0                                                                     AS ESPECIALPROVISION       -- �ر�׼����
                ,0                                                                     AS WRITTENOFFAMOUNT        -- �Ѻ������
                ,CASE WHEN T2.OFFSHEETFLAG='EntOff'  THEN '03'                         --  03-ʵ�ʱ���ҵ��
                      ELSE ''
                 END                                                                   AS OffExpoSource           -- ���Ⱪ¶��Դ
                ,''                                                                    AS OffBusinessType         -- ����ҵ������
                ,''                                                                    AS OffBusinessSdvsSTD      -- Ȩ�ط�����ҵ������ϸ��
                ,'1'                                                                   AS UncondCancelFlag        -- �Ƿ����ʱ����������
                ,''                                                                    AS CCFLevel                -- ����ת��ϵ������
                ,NULL                                                                  AS CCFAIRB                 -- �߼�������ת��ϵ��
                ,'01'                                                                  AS CLAIMSLEVEL             -- ծȨ����
                ,'0'                                                                   AS BONDFLAG                -- �Ƿ�Ϊծȯ
                ,'02'                                                                  AS BONDISSUEINTENT         -- ծȯ����Ŀ��
                ,'0'                                                                   AS NSUREALPROPERTYFLAG     -- �Ƿ�����ò�����
                ,''                                                                    AS REPASSETTERMTYPE        -- ��ծ�ʲ���������
                ,'0'                                                                   AS DEPENDONFPOBFLAG        -- �Ƿ�����������δ��ӯ��
                ,T6.PDADJLEVEL                                                         AS IRATING                 -- �ڲ�����
                ,T6.PD                                                                 AS PD                      -- ΥԼ����
                ,NULL                                                                  AS LGDLEVEL                -- ΥԼ��ʧ�ʼ���
                ,0                                                                     AS LGDAIRB                 -- �߼���ΥԼ��ʧ��
                ,0                                                                     AS MAIRB                   -- �߼�����Ч����
                ,T31.BALANCE                                                           AS EADAIRB                 -- �߼���ΥԼ���ձ�¶
                ,CASE WHEN T6.PDADJCODE='D' THEN '1'
                      ELSE '0'
                 END                                                                   AS DEFAULTFLAG             -- ΥԼ��ʶ
                ,0                                                                     AS BEEL                    -- ��ΥԼ��¶Ԥ����ʧ����
                ,0                                                                     AS DEFAULTLGD              -- ��ΥԼ��¶ΥԼ��ʧ��
                ,'0'                                                                   AS EQUITYEXPOFLAG          -- ��Ȩ��¶��ʶ
                ,''                                                                    AS EQUITYINVESTTYPE        -- ��ȨͶ�ʶ�������
                ,''                                                                    AS EQUITYINVESTCAUSE       -- ��ȨͶ���γ�ԭ��
                ,'0'                                                                   AS SLFLAG                  -- רҵ�����ʶ       רҵ��������ֶ�һ���ȸ���
                ,''                                                                    AS SLTYPE                  -- רҵ��������
                ,''                                                                    AS PFPHASE                 -- ��Ŀ���ʽ׶�
                ,'01'                                                                  AS REGURATING              -- �������
                ,''                                                                    AS CBRCMPRATINGFLAG        -- ������϶������Ƿ��Ϊ����
                ,''                                                                    AS LARGEFLUCFLAG           -- �Ƿ񲨶��Խϴ�
                ,'0'                                                                   AS LIQUEXPOFLAG            -- �Ƿ���������з��ձ�¶
                ,''                                                                    AS PAYMENTDEALFLAG         -- �Ƿ����Ը�ģʽ
                ,0                                                                     AS DELAYTRADINGDAYS        -- �ӳٽ�������
                ,'0'                                                                   AS SECURITIESFLAG          -- �м�֤ȯ��ʶ
                ,''                                                                    AS SECUISSUERID            -- ֤ȯ������ID
                ,''                                                                    AS RATINGDURATIONTYPE      -- ������������
                ,''                                                                    AS SECUISSUERATING         -- ֤ȯ���еȼ�
                ,0                                                                     AS SECURESIDUALM           -- ֤ȯʣ������
                ,1                                                                     AS SECUREVAFREQUENCY       -- ֤ȯ�ع�Ƶ��
                ,'0'                                                                   AS CCPTRANFLAG             -- �Ƿ����뽻�׶�����ؽ���
                ,''                                                                    AS CCPID                   -- ���뽻�׶���ID
                ,'0'                                                                   AS QUALCCPFLAG             -- �Ƿ�ϸ����뽻�׶���
                ,''                                                                    AS BANKROLE                -- ���н�ɫ
                ,''                                                                    AS CLEARINGMETHOD          -- ���㷽ʽ
                ,'0'                                                                   AS BANKASSETFLAG           -- �Ƿ������ύ�ʲ�
                ,''                                                                    AS MATCHCONDITIONS         -- �����������
                ,'0'                                                                   AS SFTFLAG                 -- ֤ȯ���ʽ��ױ�ʶ
                ,''                                                                    AS MASTERNETAGREEFLAG      -- ���������Э���ʶ
                ,''                                                                    AS MASTERNETAGREEID        -- ���������Э��ID
                ,''                                                                    AS SFTTYPE                 -- ֤ȯ���ʽ�������
                ,''                                                                    AS SECUOWNERTRANSFLAG      -- ֤ȯ����Ȩ�Ƿ�ת��
                ,'0'                                                                   AS OTCFLAG                 -- �����������߱�ʶ
                ,''                                                                    AS VALIDNETTINGFLAG        -- ��Ч�������Э���ʶ
                ,''                                                                    AS VALIDNETAGREEMENTID     -- ��Ч�������Э��ID
                ,''                                                                    AS OTCTYPE                 -- ����������������
                ,''                                                                    AS DEPOSITRISKPERIOD       -- ��֤������ڼ�
                ,0                                                                     AS MTM                     -- ���óɱ�
                ,''                                                                    AS MTMCURRENCY             -- ���óɱ�����
                ,''                                                                    AS BUYERORSELLER           -- ������
                ,''                                                                    AS QUALROFLAG              -- �ϸ�����ʲ���ʶ
                ,''                                                                    AS ROISSUERPERFORMFLAG     -- �����ʲ��������Ƿ�����Լ
                ,''                                                                    AS BUYERINSOLVENCYFLAG     -- ���ñ������Ƿ��Ʋ�
                ,0                                                                     AS NONPAYMENTFEES          -- ��δ֧������
                ,'0'                                                                   AS RETAILEXPOFLAG          -- ���۱�¶��ʶ
                ,''                                                                    AS RETAILCLAIMTYPE         -- ����ծȨ����
                ,''                                                                    AS MORTGAGETYPE            -- ס����Ѻ��������
                ,1                                                                     AS EXPONUMBER              -- ���ձ�¶����
                ,0.8                                                                   AS LTV                     --�����ֵ��
                ,0                                                                     AS AGING                   --����
                ,''                                                                    AS NEWDEFAULTDEBTFLAG      --����ΥԼծ���ʶ
                ,''                                                                    AS PDPOOLMODELID           -- PD�ֳ�ģ��ID
                ,''                                                                    AS LGDPOOLMODELID          -- LGD�ֳ�ģ��ID
                ,''                                                                    AS CCFPOOLMODELID          -- CCF�ֳ�ģ��ID
                ,''                                                                    AS PDPOOLID                -- ����PD��ID
                ,''                                                                    AS LGDPOOLID               -- ����LGD��ID
                ,''                                                                    AS CCFPOOLID               -- ����CCF��ID
                ,CASE WHEN T10.PROJECTNO IS NULL THEN '0'
                      ELSE '1'
                 END                                                                   AS ABSUAFLAG           --�ʲ�֤ȯ�������ʲ���ʶ
                ,CASE WHEN T10.PROJECTNO IS NULL THEN ''
                      ELSE T10.PROJECTNO
                 END                                                                   AS ABSPOOLID           --֤ȯ���ʲ���ID
                ,''                                                                    AS GROUPID                 -- ������
                ,CASE WHEN T6.PDADJCODE='D' THEN TO_DATE(T6.PDVAVLIDDATE,'YYYY/MM/DD')
                      ELSE NULL
                 END                                                                   AS DefaultDate             -- ΥԼʱ��
                ,0                                                                     AS ABSPROPORTION           --�ʲ�֤ȯ������
                ,0                                                                     AS DEBTORNUMBER            --����˸���
                ,'BW'
    FROM        RWA_DEV.NCM_BUSINESS_DUEBILL T1
    INNER JOIN  RWA_DEV.NCM_BUSINESS_TYPE T2
    ON          T1.BUSINESSTYPE = T2.TYPENO
    AND         T1.DATANO = T2.DATANO
    INNER JOIN  RWA_DEV.NCM_BUSINESS_CONTRACT T4
    ON          T1.RELATIVESERIALNO2 = T4.SERIALNO
    AND         T1.DATANO = T4.DATANO
    INNER JOIN  RWA_DEV.NCM_BUSINESS_HISTORY T31
    ON          T1.SERIALNO = T31.SERIALNO
    AND         T31.BALANCE > 0
    AND         T31.DATANO = P_DATA_DT_STR
                /*rwa_dev.brd_loan_nor t31
    ON          t1.serialno = t31.CRDT_ACCT_NO
    AND         t31.CUR_BAL > 0
    AND         T31.DATANO = P_DATA_DT_STR*/
    LEFT JOIN   RWA.ORG_INFO T3
    ON          decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID)  = T3.ORGID
    LEFT JOIN   RWA.CODE_LIBRARY T5
    ON          T4.DIRECTION = T5.ITEMNO
    AND         T5.CODENO = 'IndustryType'
    LEFT JOIN   RWA_DEV.RWA_TEMP_PDLEVEL T6
    ON          T1.CUSTOMERID=T6.CUSTID
    LEFT JOIN   (SELECT DISTINCT AA.PROJECTNO,AA.CONTRACTNO   --�ж��Ƿ��ʲ�֤ȯ��
                   FROM RWA_DEV.NCM_ABS_PROJECT_ASSET AA
             INNER JOIN RWA_DEV.NCM_ABS_PROJECT_INFO BB
                     ON AA.PROJECTNO = BB.PROJECTNO
                    AND BB.DATANO = P_DATA_DT_STR
                    AND BB.PROJECTSTATUS = '0401'            --����ɹ�
                  WHERE AA.DATANO = P_DATA_DT_STR
                ) T10
    ON          T1.RELATIVESERIALNO2 = T10.CONTRACTNO
    LEFT JOIN   RWA_DEV.ncm_rwa_risk_expo_rst T11
    ON          T1.SERIALNO = T11.OBJECTNO
    AND         T11.OBJECTTYPE = 'BusinessDuebill'
    AND         T11.DATANO = P_DATA_DT_STR
    LEFT JOIN   RWA_DEV.RWA_CD_CODE_MAPPING T12
    ON          T11.RISKEXPOSURERESULT = T12.SITEMNO
    AND         T12.SCODENO = 'RwaResultType'
    LEFT JOIN   RWA_DEV.NCM_CLASSIFY_RECORD T13
    ON          T1.RELATIVESERIALNO2 = T13.OBJECTNO
    AND         T13.OBJECTTYPE = 'TwelveClassify'
    AND         T13.ISWORK = '1'
    AND         T13.DATANO = P_DATA_DT_STR
    LEFT JOIN   RWA_DEV.NCM_CUSTOMER_INFO T16
    ON          T1.CUSTOMERID = T16.CUSTOMERID
    AND         T1.DATANO = T16.DATANO
    LEFT JOIN   (
                select OBJECTNO, DIRECTION
                  from (select T.OBJECTNO,
                               T.DIRECTION,
                               ROW_NUMBER() OVER(PARTITION BY T.OBJECTNO order by T.SERIALNO DESC) AS RM
                          from RWA_DEV.NCM_PUTOUT_SCHEME T
                         where T.DATANO = P_DATA_DT_STR
                           and T.OBJECTTYPE = 'BusinessContract'
                           and T.DIRECTION IS NOT NULL)
                 where RM = 1
                ) CPS                 --�����ҵ�����ҵͶ��������ñ�ȡ
    ON          T4.SERIALNO = CPS.OBJECTNO
    LEFT JOIN   RWA.CODE_LIBRARY CL
    ON          CPS.DIRECTION = CL.ITEMNO
    AND         CL.CODENO = 'IndustryType'
    WHERE       T1.DATANO = P_DATA_DT_STR
    AND         T1.SERIALNO <> 'BD2014110400000001'
    AND         T1.BUSINESSTYPE IN ('10201010','10202010','1035101010','1035101020','1035102010','1035102020','103550','1040101040'
                --,'10352010','10352020'--�жһ�Ʊ�Ѿ�����������Թ�ҵ������
                ) 
    --20170131�ڴκ�bl2017041800000067�ñ����뷵�����������ʲ���ǰ���ڣ�����ݺ�BH�����һ��(balance)����ǿ���ų��ñ�����
    --20190820 ���������뷵�����������ʲ���ǰ����'bl2017041800000065', 'bl2017041800000063' ,ǿ�ƹ���
    AND         CASE WHEN P_DATA_DT_STR > '20171031' AND T1.SERIALNO IN('bl2017041800000065', 'bl2017041800000063', 'bl2017041800000067') THEN '0' ELSE '1' END = '1'       
    --AND T1.BALANCE>0
    ;
    COMMIT;
    
    /*5 ���� ���(�жҵ��+�������+����֤���) �Թ�ҵ��flag=DK*/
    INSERT INTO RWA_DEV.RWA_XD_EXPOSURE(
               DATADATE         --01 ��������
              ,DATANO           --02 ������ˮ��
              ,EXPOSUREID       --03 ���ձ�¶ID
              ,DUEID            --04 ծ��ID
              ,SSYSID           --05 ԴϵͳID
              ,CONTRACTID       --06 ��ͬID
              ,CLIENTID         --07 ��������ID
              ,SORGID           --08 Դ����ID
              ,SORGNAME         --09 Դ��������
              ,ORGSORTNO        --10 �������������
              ,ORGID            --11 ��������ID
              ,ORGNAME          --12 ������������
              ,ACCORGID         --13 �������ID
              ,ACCORGNAME       --14 �����������
              ,INDUSTRYID       --15 ������ҵ����
              ,INDUSTRYNAME     --16 ������ҵ����
              ,BUSINESSLINE     --17 ҵ������
              ,ASSETTYPE        --18 �ʲ�����
              ,ASSETSUBTYPE     --19 �ʲ�С��
              ,BUSINESSTYPEID   --20 ҵ��Ʒ�ִ���
              ,BUSINESSTYPENAME --21 ҵ��Ʒ������
              ,CREDITRISKDATATYPE    --22 ���÷�����������
              ,ASSETTYPEOFHAIRCUTS   --23 �ۿ�ϵ����Ӧ�ʲ����
              ,BUSINESSTYPESTD       --24 Ȩ�ط�ҵ������
              ,EXPOCLASSSTD          --25 Ȩ�ط���¶����
              ,EXPOSUBCLASSSTD       --26 Ȩ�ط���¶С��
              ,EXPOCLASSIRB          --27 ��������¶����
              ,EXPOSUBCLASSIRB       --28 ��������¶С��
              ,EXPOBELONG            --29 ��¶������ʶ
              ,BOOKTYPE              --30 �˻����
              ,REGUTRANTYPE          --31 ��ܽ�������
              ,REPOTRANFLAG          --32 �ع����ױ�ʶ
              ,REVAFREQUENCY         --33 �ع�Ƶ��
              ,CURRENCY              --34 ����
              ,NORMALPRINCIPAL       --35 �����������
              ,OVERDUEBALANCE        --36 �������
              ,NONACCRUALBALANCE     --37 ��Ӧ�����
              ,ONSHEETBALANCE        --38 �������
              ,NORMALINTEREST        --39 ������Ϣ
              ,ONDEBITINTEREST       --40 ����ǷϢ
              ,OFFDEBITINTEREST      --41 ����ǷϢ
              ,EXPENSERECEIVABLE     --42 Ӧ�շ���
              ,ASSETBALANCE          --43 �ʲ����
              ,ACCSUBJECT1           --44 ��Ŀһ
              ,ACCSUBJECT2           --45 ��Ŀ��
              ,ACCSUBJECT3           --46 ��Ŀ��
              ,STARTDATE             --47 ��ʼ����
              ,DUEDATE               --48 ��������
              ,ORIGINALMATURITY      --49 ԭʼ����
              ,RESIDUALM             --50 ʣ������
              ,RISKCLASSIFY          --51 ���շ���
              ,EXPOSURESTATUS        --52 ���ձ�¶״̬
              ,OVERDUEDAYS           --53 ��������
              ,SPECIALPROVISION      --54 ר��׼����
              ,GENERALPROVISION      --55 һ��׼����
              ,ESPECIALPROVISION     --56 �ر�׼����
              ,WRITTENOFFAMOUNT      --57 �Ѻ������
              ,OFFEXPOSOURCE         --58 ���Ⱪ¶��Դ
              ,OFFBUSINESSTYPE       --59 ����ҵ������
              ,OFFBUSINESSSDVSSTD    --60 Ȩ�ط�����ҵ������ϸ��
              ,UNCONDCANCELFLAG      --61 �Ƿ����ʱ����������
              ,CCFLEVEL              --62 ����ת��ϵ������
              ,CCFAIRB               --63 �߼�������ת��ϵ��
              ,CLAIMSLEVEL           --64 ծȨ����
              ,BONDFLAG              --65 �Ƿ�Ϊծȯ
              ,BONDISSUEINTENT       --66 ծȯ����Ŀ��
              ,NSUREALPROPERTYFLAG   --67 �Ƿ�����ò�����
              ,REPASSETTERMTYPE      --68 ��ծ�ʲ���������
              ,DEPENDONFPOBFLAG      --69 �Ƿ�����������δ��ӯ��
              ,IRATING               --70 �ڲ�����
              ,PD                    --71 ΥԼ����
              ,LGDLEVEL              --72 ΥԼ��ʧ�ʼ���
              ,LGDAIRB               --73 �߼���ΥԼ��ʧ��
              ,MAIRB                 --74 �߼�����Ч����
              ,EADAIRB               --75 �߼���ΥԼ���ձ�¶
              ,DEFAULTFLAG           --76 ΥԼ��ʶ
              ,BEEL                  --77 ��ΥԼ��¶Ԥ����ʧ����
              ,DEFAULTLGD            --78 ��ΥԼ��¶ΥԼ��ʧ��
              ,EQUITYEXPOFLAG        --79 ��Ȩ��¶��ʶ
              ,EQUITYINVESTTYPE      --80 ��ȨͶ�ʶ�������
              ,EQUITYINVESTCAUSE     --81 ��ȨͶ���γ�ԭ��
              ,SLFLAG                --82 רҵ�����ʶ
              ,SLTYPE                --83 רҵ��������
              ,PFPHASE               --84 ��Ŀ���ʽ׶�
              ,REGURATING            --85 �������
              ,CBRCMPRATINGFLAG      --86 ������϶������Ƿ��Ϊ����
              ,LARGEFLUCFLAG         --87 �Ƿ񲨶��Խϴ�
              ,LIQUEXPOFLAG          --88 �Ƿ���������з��ձ�¶
              ,PAYMENTDEALFLAG       --89 �Ƿ����Ը�ģʽ
              ,DELAYTRADINGDAYS      --90 �ӳٽ�������
              ,SECURITIESFLAG        --91 �м�֤ȯ��ʶ
              ,SECUISSUERID          --92 ֤ȯ������ID
              ,RATINGDURATIONTYPE    --93 ������������
              ,SECUISSUERATING       --94 ֤ȯ���еȼ�
              ,SECURESIDUALM         --95 ֤ȯʣ������
              ,SECUREVAFREQUENCY     --96 ֤ȯ�ع�Ƶ��
              ,CCPTRANFLAG           --97 �Ƿ����뽻�׶�����ؽ���
              ,CCPID                 --98 ���뽻�׶���ID
              ,QUALCCPFLAG           --99 �Ƿ�ϸ����뽻�׶���
              ,BANKROLE              --100 ���н�ɫ
              ,CLEARINGMETHOD        --101 ���㷽ʽ
              ,BANKASSETFLAG         --102 �Ƿ������ύ�ʲ�
              ,MATCHCONDITIONS       --103 �����������
              ,SFTFLAG               --104 ֤ȯ���ʽ��ױ�ʶ
              ,MASTERNETAGREEFLAG    --105 ���������Э���ʶ
              ,MASTERNETAGREEID      --106 ���������Э��ID
              ,SFTTYPE               --107 ֤ȯ���ʽ�������
              ,SECUOWNERTRANSFLAG    --108 ֤ȯ����Ȩ�Ƿ�ת��
              ,OTCFLAG               --109 �����������߱�ʶ
              ,VALIDNETTINGFLAG      --110 ��Ч�������Э���ʶ
              ,VALIDNETAGREEMENTID   --111 ��Ч�������Э��ID
              ,OTCTYPE               --112 ����������������
              ,DEPOSITRISKPERIOD     --113 ��֤������ڼ�
              ,MTM                   --114 ���óɱ�
              ,MTMCURRENCY           --115 ���óɱ�����
              ,BUYERORSELLER         --116 ������
              ,QUALROFLAG            --117 �ϸ�����ʲ���ʶ
              ,ROISSUERPERFORMFLAG   --118 �����ʲ��������Ƿ�����Լ
              ,BUYERINSOLVENCYFLAG   --119 ���ñ������Ƿ��Ʋ�
              ,NONPAYMENTFEES        --120 ��δ֧������
              ,RETAILEXPOFLAG        --121 ���۱�¶��ʶ
              ,RETAILCLAIMTYPE       --122 ����ծȨ����
              ,MORTGAGETYPE          --123 ס����Ѻ��������
              ,EXPONUMBER            --124 ���ձ�¶����
              ,LTV                   --125 �����ֵ��
              ,AGING                 --126 ����
              ,NEWDEFAULTDEBTFLAG    --127 ����ΥԼծ���ʶ
              ,PDPOOLMODELID         --128 PD�ֳ�ģ��ID
              ,LGDPOOLMODELID        --129 LGD�ֳ�ģ��ID
              ,CCFPOOLMODELID        --130 CCF�ֳ�ģ��ID
              ,PDPOOLID              --131 ����PD��ID
              ,LGDPOOLID             --132 ����LGD��ID
              ,CCFPOOLID             --133 ����CCF��ID
              ,ABSUAFLAG             --134 �ʲ�֤ȯ�������ʲ���ʶ
              ,ABSPOOLID             --135 ֤ȯ���ʲ���ID
              ,GROUPID               --136 ������
              ,DefaultDate           --137 ΥԼʱ��
              ,ABSPROPORTION         --138 �ʲ�֤ȯ������
              ,DEBTORNUMBER          --139 ����˸���
              ,flag
    
    )
    SELECT
                TO_DATE(T1.DATANO,'YYYYMMDD')                                           AS DATADATE                -- ��������
                ,T1.DATANO                                                              AS DATANO                  -- ������ˮ��
                ,T1.SERIALNO                                                            AS EXPOSUREID              -- ���ձ�¶ID
                ,T1.SERIALNO                                                            AS DUEID                   -- ծ��ID
                ,'XD'                                                                   AS SSYSID                  -- ԴϵͳID
                ,T1.RELATIVESERIALNO2                                                   AS CONTRACTID              -- ��ͬID
                ,CASE WHEN REPLACE(T1.CUSTOMERID,'NCM_','') IS NULL THEN DECODE(T3.LINETYPE,'0030','XN-GRKH','XN-YBGS')                 --���ͻ�IDΪ�գ�����Ϊ���˾�����Ϊ���˿ͻ�������Ϊһ�㹫˾
                      ELSE T1.CUSTOMERID
                 END                                                                    AS CLIENTID                -- ��������ID
                --,decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID)                      AS SORGID                  -- Դ����ID
                ,case when substr(decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID),1,1)='@' then '01000000'
                     else decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID)
                 end
                ,nvl(T4.ORGNAME,'����')                                                             AS SORGNAME                -- Դ��������
                ,nvl(T4.SORTNO,'1010')                                                              AS ORGSORTNO               -- �������������
                --,decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID)                      AS ORGID                   -- ��������ID
                ,case when substr(decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID),1,1)='@' then '01000000'
                     else decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID)
                 end
                ,nvl(T4.ORGNAME,'����')  
                --,decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID)                      AS ACCORGID                -- �������ID
                ,case when substr(decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID),1,1)='@' then '01000000'
                     else decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID)
                 end
                ,nvl(T4.ORGNAME,'����')                                                             AS ACCORGNAME              -- �����������
                ,NVL(T3.DIRECTION,CPS.DIRECTION)                                        AS INDUSTRYID              -- ������ҵ����
                ,NVL(T5.ITEMNAME,CL.ITEMNAME)                                           AS INDUSTRYNAME            -- ������ҵ����
                ,CASE WHEN T1.BUSINESSCURRENCY<>'CNY' THEN '0102'                            --��ҵı���ҵ��   ����-ó��
                      WHEN T1.BUSINESSTYPE IN('10302010','10302015','10302020') THEN '0401'  --����ҵ��        ͬҵ�����г���
                      WHEN T3.LINETYPE='0010' THEN '0101'
                      WHEN T3.LINETYPE='0020' THEN '0201'
                      WHEN T3.LINETYPE='0030' THEN '0301'
                      WHEN T3.LINETYPE='0040' THEN '0401'
                      ELSE '0101'
                 END                                                                    AS BUSINESSLINE            -- ����
                ,''                                                                     AS ASSETTYPE               -- �ʲ�����
                ,''                                                                     AS ASSETSUBTYPE            -- �ʲ�С��
                ,T1.BUSINESSTYPE                                                        AS BUSINESSTYPEID          -- ҵ��Ʒ�ִ���
                ,T2.TYPENAME                                                            AS BUSINESSTYPENAME        -- ҵ��Ʒ������
                /*,CASE WHEN T1.SERIALNO='20170125c0000373' THEN '02'
                      ELSE '01'
                 END                                                                    AS CREDITRISKDATATYPE      -- ���÷�����������          01-һ�������
                */
                ,CASE WHEN T16.CUSTOMERTYPE = '0310' OR T16.CERTTYPE LIKE 'Ind%'
                      THEN '02' --����
                      ELSE '01' --������
                  END                                                                   AS CREDITRISKDATATYPE      --���÷�����������
                ,'01'                                                                   AS ASSETTYPEOFHAIRCUTS     -- �ۿ�ϵ����Ӧ�ʲ����     01-�ֽ��ֽ�ȼ���
                ,''                                                                     AS BUSINESSTYPESTD         -- Ȩ�ط�ҵ������
                ,CASE WHEN REPLACE(T1.CUSTOMERID,'NCM_','') IS NULL THEN '0112'                                    --���ͻ�IDΪ�գ�Ĭ�� ����(0112)
                      ELSE ''
                 END                                                                    AS EXPOCLASSSTD            -- Ȩ�ط���¶����
                ,CASE WHEN REPLACE(T1.CUSTOMERID,'NCM_','') IS NULL THEN DECODE(T3.LINETYPE,'0030','011215','011216')                          --���ͻ�IDΪ�գ�����Ϊ���˾�Ĭ�� ��������75%����Ȩ�ص��ʲ�(011215)������Ĭ�� ��������100%����Ȩ�ص��ʲ�(011216)
                      ELSE ''
                 END                                                                    AS EXPOSUBCLASSSTD         -- Ȩ�ط���¶С��
                ,SUBSTR(T10.DITEMNO,1,4)                                                AS EXPOCLASSIRB            -- ��������¶����
                ,T10.DITEMNO                                                            AS EXPOSUBCLASSIRB         -- ��������¶С��
                ,CASE WHEN T2.OFFSHEETFLAG='EntOff' THEN '02'   --  02-����
                      ELSE '01'                                 --  01-����
                 END                                                                   AS EXPOBELONG              -- ��¶������ʶ
                ,'01'                                                                  AS BOOKTYPE                -- �˻����           01-�����˻�
                ,'03'                                                                  AS REGUTRANTYPE            -- ��ܽ�������      03-��Ѻ����
                ,'0'                                                                   AS REPOTRANFLAG            -- �ع����ױ�ʶ       0-��
                ,1                                                                     AS REVAFREQUENCY           -- �ع�Ƶ��
                ,NVL(T1.BUSINESSCURRENCY,'CNY')                                        AS CURRENCY                -- ����
                ,t1.balance                                                            AS NORMALPRINCIPAL         -- �����������
                ,0                                                                     AS OVERDUEBALANCE          -- �������
                ,0                                                                     AS NONACCRUALBALANCE       -- ��Ӧ�����
                ,t1.balance                                                            AS ONSHEETBALANCE          -- �������
                ,0                                                                     AS NORMALINTEREST          -- ������Ϣ
                ,0                                                                     AS ONDEBITINTEREST         -- ����ǷϢ
                ,0                                                                     AS OFFDEBITINTEREST        -- ����ǷϢ
                ,0                                                                     AS EXPENSERECEIVABLE       -- Ӧ�շ���
                ,t1.balance                                                            AS ASSETBALANCE            -- �ʲ����
                ,T31.sbjt_cd                                                          AS ACCSUBJECT1             -- ��Ŀһ
                ,''                                                                    AS ACCSUBJECT2             -- ��Ŀ��
                ,''                                                                    AS ACCSUBJECT3             -- ��Ŀ��
                ,NVL(T1.PUTOUTDATE,T3.PUTOUTDATE)                                      AS STARTDATE               -- ��ʼ����
                ,NVL(T1.ACTUALMATURITY,T3.MATURITY)                                    AS DUEDATE                 -- ��������
                ,CASE WHEN (TO_DATE(NVL(T1.ACTUALMATURITY,T3.MATURITY),'YYYYMMDD')-TO_DATE(NVL(T1.PUTOUTDATE,T3.PUTOUTDATE),'YYYYMMDD'))/365<0
                      THEN 0
                      ELSE (TO_DATE(NVL(T1.ACTUALMATURITY,T3.MATURITY),'YYYYMMDD')-TO_DATE(NVL(T1.PUTOUTDATE,T3.PUTOUTDATE),'YYYYMMDD'))/365
                END                                                                    AS ORIGINALMATURITY        -- ԭʼ����
                ,CASE WHEN (TO_DATE(NVL(T1.ACTUALMATURITY,T3.MATURITY),'YYYYMMDD')-TO_DATE(T1.DATANO,'YYYYMMDD'))/365<0
                      THEN 0
                      ELSE (TO_DATE(NVL(T1.ACTUALMATURITY,T3.MATURITY),'YYYYMMDD')-TO_DATE(T1.DATANO,'YYYYMMDD'))/365
                END                                                                    AS RESIDUALM               -- ʣ������
                ,CASE WHEN NVL(T1.Classifyresult,SUBSTR(T11.FINALLYRESULT,1,1))='A' THEN '01'
                      WHEN NVL(T1.Classifyresult,SUBSTR(T11.FINALLYRESULT,1,1))='B' THEN '02'       --ʮ��������תΪ�弶����
                      WHEN NVL(T1.Classifyresult,SUBSTR(T11.FINALLYRESULT,1,1))='C' THEN '03'
                      WHEN NVL(T1.Classifyresult,SUBSTR(T11.FINALLYRESULT,1,1))='D' THEN '04'
                      WHEN NVL(T1.Classifyresult,SUBSTR(T11.FINALLYRESULT,1,1))='E' THEN '05'
                      ELSE NVL(T1.Classifyresult,SUBSTR(T11.FINALLYRESULT,1,1))
                 END                                                                   AS RISKCLASSIFY            -- ���շ���
                ,'01'                                                                  AS EXPOSURESTATUS          -- ���ձ�¶״̬  01-����
                ,T1.OVERDUEDAYS                                                        AS OVERDUEDAYS             -- ��������
                ,0                                                                     AS SPECIALPROVISION        -- ר��׼����-������
                ,0                                                                     AS GENERALPROVISION        -- һ��׼����
                ,0                                                                     AS ESPECIALPROVISION       -- �ر�׼����
                ,T1.CANCELSUM                                                          AS WRITTENOFFAMOUNT        -- �Ѻ������
                ,CASE WHEN T2.OFFSHEETFLAG='EntOff'  THEN '03'                         --  03-ʵ�ʱ���ҵ��
                      ELSE ''
                 END                                                                   AS OffExpoSource           -- ���Ⱪ¶��Դ
                ,''                                                                    AS OffBusinessType         -- ����ҵ������
                ,''                                                                    AS OffBusinessSdvsSTD      -- Ȩ�ط�����ҵ������ϸ��
                ,'1'                                                                   AS UncondCancelFlag        -- �Ƿ����ʱ����������
                ,''                                                                    AS CCFLevel                -- ����ת��ϵ������
                ,NULL                                                                  AS CCFAIRB                 -- �߼�������ת��ϵ��
                ,'01'                                                                  AS CLAIMSLEVEL             -- ծȨ����
                ,'0'                                                                   AS BONDFLAG                -- �Ƿ�Ϊծȯ
                ,'02'                                                                  AS BONDISSUEINTENT         -- ծȯ����Ŀ��
                ,'0'                                                                   AS NSUREALPROPERTYFLAG     -- �Ƿ�����ò�����
                ,''                                                                    AS REPASSETTERMTYPE        -- ��ծ�ʲ���������
                ,'0'                                                                   AS DEPENDONFPOBFLAG        -- �Ƿ�����������δ��ӯ��
                ,T6.PDADJLEVEL                                                         AS IRATING                 -- �ڲ�����
                ,T6.PD                                                                 AS PD                      -- ΥԼ����
                ,''                                                                    AS LGDLEVEL                -- ΥԼ��ʧ�ʼ���
                ,0                                                                     AS LGDAIRB                 -- �߼���ΥԼ��ʧ��
                ,0                                                                     AS MAIRB                   -- �߼�����Ч����
                ,T1.Balance                                                            AS EADAIRB                 -- �߼���ΥԼ���ձ�¶
                ,CASE WHEN T6.PDADJCODE='D' THEN '1'
                      ELSE '0'
                 END                                                                   AS DEFAULTFLAG             -- ΥԼ��ʶ
                ,0                                                                     AS BEEL                    -- ��ΥԼ��¶Ԥ����ʧ����
                ,0                                                                     AS DEFAULTLGD              -- ��ΥԼ��¶ΥԼ��ʧ��
                ,'0'                                                                   AS EQUITYEXPOFLAG          -- ��Ȩ��¶��ʶ
                ,''                                                                    AS EQUITYINVESTTYPE        -- ��ȨͶ�ʶ�������
                ,''                                                                    AS EQUITYINVESTCAUSE       -- ��ȨͶ���γ�ԭ��
                ,'0'                                                                   AS SLFLAG                  -- רҵ�����ʶ       רҵ��������ֶ�һ���ȸ���
                ,''                                                                    AS SLTYPE                  -- רҵ��������
                ,''                                                                    AS PFPHASE                 -- ��Ŀ���ʽ׶�
                ,'01'                                                                  AS REGURATING              -- �������
                ,''                                                                    AS CBRCMPRATINGFLAG        -- ������϶������Ƿ��Ϊ����
                ,''                                                                    AS LARGEFLUCFLAG           -- �Ƿ񲨶��Խϴ�
                ,'0'                                                                   AS LIQUEXPOFLAG            -- �Ƿ���������з��ձ�¶
                ,''                                                                    AS PAYMENTDEALFLAG         -- �Ƿ����Ը�ģʽ
                ,0                                                                     AS DELAYTRADINGDAYS        -- �ӳٽ�������
                ,'0'                                                                   AS SECURITIESFLAG          -- �м�֤ȯ��ʶ
                ,''                                                                    AS SECUISSUERID            -- ֤ȯ������ID
                ,''                                                                    AS RATINGDURATIONTYPE      -- ������������
                ,''                                                                    AS SECUISSUERATING         -- ֤ȯ���еȼ�
                ,0                                                                     AS SECURESIDUALM           -- ֤ȯʣ������
                ,1                                                                     AS SECUREVAFREQUENCY       -- ֤ȯ�ع�Ƶ��
                ,'0'                                                                   AS CCPTRANFLAG             -- �Ƿ����뽻�׶�����ؽ���
                ,''                                                                    AS CCPID                   -- ���뽻�׶���ID
                ,'0'                                                                   AS QUALCCPFLAG             -- �Ƿ�ϸ����뽻�׶���
                ,''                                                                    AS BANKROLE                -- ���н�ɫ
                ,''                                                                    AS CLEARINGMETHOD          -- ���㷽ʽ
                ,'0'                                                                   AS BANKASSETFLAG           -- �Ƿ������ύ�ʲ�
                ,''                                                                    AS MATCHCONDITIONS         -- �����������
                ,'0'                                                                   AS SFTFLAG                 -- ֤ȯ���ʽ��ױ�ʶ
                ,''                                                                    AS MASTERNETAGREEFLAG      -- ���������Э���ʶ
                ,''                                                                    AS MASTERNETAGREEID        -- ���������Э��ID
                ,''                                                                    AS SFTTYPE                 -- ֤ȯ���ʽ�������
                ,''                                                                    AS SECUOWNERTRANSFLAG      -- ֤ȯ����Ȩ�Ƿ�ת��
                ,'0'                                                                   AS OTCFLAG                 -- �����������߱�ʶ
                ,''                                                                    AS VALIDNETTINGFLAG        -- ��Ч�������Э���ʶ
                ,''                                                                    AS VALIDNETAGREEMENTID     -- ��Ч�������Э��ID
                ,''                                                                    AS OTCTYPE                 -- ����������������
                ,''                                                                    AS DEPOSITRISKPERIOD       -- ��֤������ڼ�
                ,0                                                                     AS MTM                     -- ���óɱ�
                ,''                                                                    AS MTMCURRENCY             -- ���óɱ�����
                ,''                                                                    AS BUYERORSELLER           -- ������
                ,''                                                                    AS QUALROFLAG              -- �ϸ�����ʲ���ʶ
                ,''                                                                    AS ROISSUERPERFORMFLAG     -- �����ʲ��������Ƿ�����Լ
                ,''                                                                    AS BUYERINSOLVENCYFLAG     -- ���ñ������Ƿ��Ʋ�
                ,0                                                                     AS NONPAYMENTFEES          -- ��δ֧������
                ,'0'                                                                   AS RETAILEXPOFLAG          -- ���۱�¶��ʶ
                ,''                                                                    AS RETAILCLAIMTYPE         -- ����ծȨ����
                ,''                                                                    AS MORTGAGETYPE            -- ס����Ѻ��������
                ,1                                                                     AS EXPONUMBER              -- ���ձ�¶����
                ,0.8                                                                   AS LTV                     --�����ֵ��
                ,0                                                                     AS AGING                   --����
                ,''                                                                    AS NEWDEFAULTDEBTFLAG      --����ΥԼծ���ʶ
                ,''                                                                    AS PDPOOLMODELID           -- PD�ֳ�ģ��ID
                ,''                                                                    AS LGDPOOLMODELID          -- LGD�ֳ�ģ��ID
                ,''                                                                    AS CCFPOOLMODELID          -- CCF�ֳ�ģ��ID
                ,''                                                                    AS PDPOOLID                -- ����PD��ID
                ,''                                                                    AS LGDPOOLID               -- ����LGD��ID
                ,''                                                                    AS CCFPOOLID               -- ����CCF��ID
                ,CASE WHEN T9.PROJECTNO IS NULL THEN '0'
                      ELSE '1'
                 END                                                                   AS ABSUAFLAG           --�ʲ�֤ȯ�������ʲ���ʶ
                ,CASE WHEN T9.PROJECTNO IS NULL THEN ''
                      ELSE T9.PROJECTNO
                 END                                                                   AS ABSPOOLID           --֤ȯ���ʲ���ID
                ,''                                                                    AS GROUPID                 -- ������
                ,CASE WHEN T6.PDADJCODE='D' THEN TO_DATE(T6.PDVAVLIDDATE,'YYYYMMDD')
                      ELSE NULL
                 END                                                                   AS DefaultDate             -- ΥԼʱ��
                ,0                                                                     AS ABSPROPORTION           --�ʲ�֤ȯ������
                ,0                                                                     AS DEBTORNUMBER            --����˸���
                ,'DK'
    FROM        RWA_DEV.NCM_BUSINESS_DUEBILL T1
    INNER JOIN  RWA_DEV.NCM_BUSINESS_TYPE T2
    ON          T1.BUSINESSTYPE = T2.TYPENO
    AND         T1.DATANO = T2.DATANO
    --AND         T2.ATTRIBUTE1 <> '2'                    --ֻȡ�Թ�ҵ��
    --AND         T2.TYPENO NOT LIKE '30%'                --�ų������ҵ��
    INNER JOIN  RWA_DEV.NCM_BUSINESS_CONTRACT T3
    ON          T1.RELATIVESERIALNO2 = T3.SERIALNO      --�����Խ��Ϊ׼�����Թ�����ͬʱ����Ӧ�üӺ�ͬ����Ч����
    AND         T1.DATANO = T3.DATANO
    INNER JOIN  rwa_dev.BRD_LOAN_NOR T31                  --֧��������������
    ON          T1.SERIALNO = T31.CRDT_ACCT_NO
    AND         t31.cur_bal > 0
    AND         t31.datano = P_DATA_DT_STR
    AND         T31.SBJT_CD LIKE '1307%'  --ֻȡ���   
    LEFT JOIN   RWA.ORG_INFO T4
    ON          decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID)  = T4.ORGID
    LEFT JOIN   RWA.CODE_LIBRARY T5
    ON          T3.DIRECTION = T5.ITEMNO
    AND         T5.CODENO = 'IndustryType'
    LEFT JOIN   RWA_DEV.RWA_TEMP_PDLEVEL T6
    ON          T1.CUSTOMERID = T6.CUSTID
    LEFT JOIN   (SELECT DISTINCT AA.PROJECTNO,AA.CONTRACTNO   --�ж��Ƿ��ʲ�֤ȯ��
                   FROM RWA_DEV.NCM_ABS_PROJECT_ASSET AA --��Ŀ���ʲ�
             INNER JOIN RWA_DEV.NCM_ABS_PROJECT_INFO BB  --��Ŀ������Ϣ
                     ON AA.PROJECTNO = BB.PROJECTNO
                    AND BB.DATANO = P_DATA_DT_STR
                    AND BB.PROJECTSTATUS = '0401'             --����ɹ�
                  WHERE AA.DATANO = P_DATA_DT_STR
                ) T9
    ON          T3.SERIALNO = T9.CONTRACTNO
    LEFT JOIN   RWA_DEV.ncm_rwa_risk_expo_rst T3 --���ձ�¶�����Ϣ��
    ON          T1.SERIALNO = T3.OBJECTNO
    AND         T3.OBJECTTYPE = 'BusinessDuebill'
    AND         T3.DATANO = P_DATA_DT_STR
    LEFT JOIN   RWA_DEV.RWA_CD_CODE_MAPPING T10 --����ӳ��ת����
    ON          T3.RISKEXPOSURERESULT = T10.SITEMNO
    AND         T10.SCODENO = 'RwaResultType'
    LEFT JOIN   RWA_DEV.NCM_CLASSIFY_RECORD T11 --�弶������Ϣ��
    ON          T1.RELATIVESERIALNO2 = T11.OBJECTNO
    AND         T11.OBJECTTYPE = 'TwelveClassify'
    AND         T11.ISWORK = '1'
    AND         T11.DATANO = P_DATA_DT_STR
    LEFT JOIN   RWA_DEV.NCM_CUSTOMER_INFO T16
    ON          T1.CUSTOMERID = T16.CUSTOMERID
    AND         T1.DATANO = T16.DATANO
    LEFT JOIN   (
                select OBJECTNO, DIRECTION
                  from (select T.OBJECTNO,
                               T.DIRECTION,
                               ROW_NUMBER() OVER(PARTITION BY T.OBJECTNO order by T.SERIALNO DESC) AS RM
                          from RWA_DEV.NCM_PUTOUT_SCHEME T  --������ñ�
                         where T.DATANO = P_DATA_DT_STR
                           and T.OBJECTTYPE = 'BusinessContract'
                           and T.DIRECTION IS NOT NULL)
                 where RM = 1
                ) CPS                 --�����ҵ�����ҵͶ��������ñ�ȡ
    ON          T3.SERIALNO = CPS.OBJECTNO
    LEFT JOIN   RWA.CODE_LIBRARY CL
    ON          CPS.DIRECTION = CL.ITEMNO
    AND         CL.CODENO = 'IndustryType'
    WHERE       T1.DATANO = P_DATA_DT_STR
    AND         T1.BALANCE>0
   ;
    COMMIT; 

    /*6 ���� �Ŵ�ϵͳ���۽�� ��Ŀ���*/    --�������ڴ��΢��������ϸ��¶ 
    INSERT INTO RWA_DEV.RWA_XD_EXPOSURE(
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
               ,flag
    )
    SELECT
                TO_DATE(T1.DATANO,'YYYYMMDD')                                           AS DATADATE                -- ��������
                ,T1.DATANO                                                              AS DATANO                  -- ������ˮ��
                ,T1.SERIALNO                                                            AS EXPOSUREID              -- ���ձ�¶ID
                ,T1.SERIALNO                                                            AS DUEID                   -- ծ��ID
                ,'XD'                                                                   AS SSYSID                  -- ԴϵͳID
                ,T1.RELATIVESERIALNO2                                                   AS CONTRACTID              -- ��ͬID
                ,CASE WHEN REPLACE(T1.CUSTOMERID,'NCM_','') IS NULL THEN DECODE(T3.LINETYPE,'0030','XN-GRKH','XN-YBGS')                 --���ͻ�IDΪ�գ�����Ϊ���˾�����Ϊ���˿ͻ�������Ϊһ�㹫˾
                      ELSE T1.CUSTOMERID
                 END                                                                    AS CLIENTID                -- ��������ID
                --,decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID)                      AS SORGID                  -- Դ����ID
                ,case when substr(decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID),1,1)='@' then '01000000'
                     else decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID)
                 end
                ,nvl(T4.ORGNAME,'����')                                                             AS SORGNAME                -- Դ��������
                ,nvl(T4.SORTNO,'1010')                                                              AS ORGSORTNO               -- �������������
                --,decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID)                      AS ORGID                   -- ��������ID
                ,case when substr(decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID),1,1)='@' then '01000000'
                     else decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID)
                 end
                ,nvl(T4.ORGNAME,'����')                                                             AS ORGNAME                 -- ������������
                --,decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID)                      AS ACCORGID                -- �������ID
                ,case when substr(decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID),1,1)='@' then '01000000'
                     else decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID)
                 end
                ,nvl(T4.ORGNAME,'����')                                                             AS ACCORGNAME              -- �����������
                ,NVL(T3.DIRECTION,CPS.DIRECTION)                                        AS INDUSTRYID              -- ������ҵ����
                ,NVL(T5.ITEMNAME,CL.ITEMNAME)                                           AS INDUSTRYNAME            -- ������ҵ����
                ,CASE WHEN T1.BUSINESSCURRENCY<>'CNY' THEN '0102'                            --��ҵı���ҵ��   ����-ó��
                      WHEN T1.BUSINESSTYPE IN('10302010','10302015','10302020') THEN '0401'  --����ҵ��        ͬҵ�����г���
                      WHEN T3.LINETYPE='0010' THEN '0101'
                      WHEN T3.LINETYPE='0020' THEN '0201'
                      WHEN T3.LINETYPE='0030' THEN '0301'
                      WHEN T3.LINETYPE='0040' THEN '0401'
                      ELSE '0101'
                 END                                                                    AS BUSINESSLINE            -- ����
                ,''                                                                     AS ASSETTYPE               -- �ʲ�����
                ,''                                                                     AS ASSETSUBTYPE            -- �ʲ�С��
                ,'11103030'                                                             AS BUSINESSTYPEID          --ҵ��Ʒ�ִ���
                ,'΢����'                                                               AS BUSINESSTYPENAME        -- ҵ��Ʒ������
                --,'02'                                                                   AS CREDITRISKDATATYPE      -- ���÷�����������          01-һ�������
                ,CASE WHEN T16.CUSTOMERTYPE = '0310' OR T16.CERTTYPE LIKE 'Ind%'
                      THEN '02' --����
                      ELSE '01' --������
                  END                                                                   AS CREDITRISKDATATYPE  --���÷�����������
                ,'01'                                                                   AS ASSETTYPEOFHAIRCUTS     -- �ۿ�ϵ����Ӧ�ʲ����     01-�ֽ��ֽ�ȼ���
                ,''                                                                     AS BUSINESSTYPESTD         -- Ȩ�ط�ҵ������
                ,CASE WHEN REPLACE(T1.CUSTOMERID,'NCM_','') IS NULL THEN '0112'                                    --���ͻ�IDΪ�գ�Ĭ�� ����(0112)
                      ELSE ''
                 END                                                                    AS EXPOCLASSSTD            -- Ȩ�ط���¶����
                ,CASE WHEN REPLACE(T1.CUSTOMERID,'NCM_','') IS NULL THEN DECODE(T3.LINETYPE,'0030','011215','011216')                          --���ͻ�IDΪ�գ�����Ϊ���˾�Ĭ�� ��������75%����Ȩ�ص��ʲ�(011215)������Ĭ�� ��������100%����Ȩ�ص��ʲ�(011216)
                      ELSE ''
                 END                                                                    AS EXPOSUBCLASSSTD         -- Ȩ�ط���¶С��
                ,SUBSTR(T12.DITEMNO,1,4)                                                AS EXPOCLASSIRB            -- ��������¶����
                ,T12.DITEMNO                                                            AS EXPOSUBCLASSIRB         -- ��������¶С��
                ,'01'                                                                   AS EXPOBELONG              -- ��¶������ʶ
                ,'01'                                                                   AS BOOKTYPE                -- �˻����           01-�����˻�
                ,'03'                                                                   AS REGUTRANTYPE            -- ��ܽ�������      03-��Ѻ����
                ,'0'                                                                    AS REPOTRANFLAG            -- �ع����ױ�ʶ       0-��
                ,1                                                                      AS REVAFREQUENCY           -- �ع�Ƶ��
                ,t31.ccy_cd                                                             AS CURRENCY                -- ����
                ,T1.BALANCE                                                            AS NORMALPRINCIPAL         -- �����������
                ,0                                                                      AS OVERDUEBALANCE          -- �������
                ,0                                                                      AS NONACCRUALBALANCE       -- ��Ӧ�����
                ,T1.BALANCE                                                            AS ONSHEETBALANCE          -- �������
                ,0                                                                      AS NORMALINTEREST          -- ������Ϣ
                ,0                                                                      AS ONDEBITINTEREST         -- ����ǷϢ
                ,0                                                                      AS OFFDEBITINTEREST        -- ����ǷϢ
                ,0                                                                      AS EXPENSERECEIVABLE       -- Ӧ�շ���
                ,T1.BALANCE                                                            AS ASSETBALANCE            -- �ʲ����
                ,'13100001'                                                             AS ACCSUBJECT1             -- ��Ŀһ
                ,''                                                                     AS ACCSUBJECT2             -- ��Ŀ��
                ,''                                                                     AS ACCSUBJECT3             -- ��Ŀ��
                ,T1.PUTOUTDATE                                                          AS STARTDATE               -- ��ʼ����
                ,T1.ACTUALMATURITY                                                      AS DUEDATE                 -- ��������
                ,CASE WHEN (TO_DATE(T1.ACTUALMATURITY,'YYYYMMDD')-TO_DATE(T1.PUTOUTDATE,'YYYYMMDD'))/365<0
                      THEN 0
                      ELSE (TO_DATE(T1.ACTUALMATURITY,'YYYYMMDD')-TO_DATE(T1.PUTOUTDATE,'YYYYMMDD'))/365
                END                                                                     AS ORIGINALMATURITY        -- ԭʼ����
                ,CASE WHEN (TO_DATE(T1.ACTUALMATURITY,'YYYYMMDD')-TO_DATE(T1.DATANO,'YYYYMMDD'))/365<0
                      THEN 0
                      ELSE (TO_DATE(T1.ACTUALMATURITY,'YYYYMMDD')-TO_DATE(T1.DATANO,'YYYYMMDD'))/365
                END                                                                     AS RESIDUALM               -- ʣ������
                ,CASE WHEN NVL(T1.Classifyresult,SUBSTR(T13.FINALLYRESULT,1,1))='A' THEN '01'
                      WHEN NVL(T1.Classifyresult,SUBSTR(T13.FINALLYRESULT,1,1))='B' THEN '02'       --ʮ��������תΪ�弶����
                      WHEN NVL(T1.Classifyresult,SUBSTR(T13.FINALLYRESULT,1,1))='C' THEN '03'
                      WHEN NVL(T1.Classifyresult,SUBSTR(T13.FINALLYRESULT,1,1))='D' THEN '04'
                      WHEN NVL(T1.Classifyresult,SUBSTR(T13.FINALLYRESULT,1,1))='E' THEN '05'
                      ELSE NVL(T1.Classifyresult,SUBSTR(T13.FINALLYRESULT,1,1))
                 END                                                                    AS RISKCLASSIFY            -- ���շ���
                ,'01'                                                                   AS EXPOSURESTATUS          -- ���ձ�¶״̬  01-����
                ,T1.OVERDUEDAYS                                                         AS OVERDUEDAYS             -- ��������
                ,0                                                                      AS SPECIALPROVISION        -- ר��׼����-������
                ,0                                                                      AS GENERALPROVISION        -- һ��׼����
                ,0                                                                      AS ESPECIALPROVISION       -- �ر�׼����
                ,T1.CANCELSUM                                                           AS WRITTENOFFAMOUNT        -- �Ѻ������
                ,''                                                                     AS OffExpoSource           -- ���Ⱪ¶��Դ
                ,''                                                                     AS OffBusinessType         -- ����ҵ������
                ,''                                                                     AS OffBusinessSdvsSTD      -- Ȩ�ط�����ҵ������ϸ��
                ,'1'                                                                    AS UncondCancelFlag        -- �Ƿ����ʱ����������
                ,''                                                                     AS CCFLevel                -- ����ת��ϵ������
                ,T6.CCFVALUE                                                            AS CCFAIRB                 -- �߼�������ת��ϵ��
                ,'01'                                                                   AS CLAIMSLEVEL             -- ծȨ����
                ,'0'                                                                    AS BONDFLAG                -- �Ƿ�Ϊծȯ
                ,'02'                                                                   AS BONDISSUEINTENT         -- ծȯ����Ŀ��
                ,'0'                                                                    AS NSUREALPROPERTYFLAG     -- �Ƿ�����ò�����
                ,''                                                                     AS REPASSETTERMTYPE        -- ��ծ�ʲ���������
                ,'0'                                                                    AS DEPENDONFPOBFLAG        -- �Ƿ�����������δ��ӯ��
                ,''                                                                     AS IRATING                 -- �ڲ�����
                ,T6.PDVALUE                                                             AS PD                      -- ΥԼ����
                ,''                                                                     AS LGDLEVEL                -- ΥԼ��ʧ�ʼ���
                ,T6.LGDVALUE                                                            AS LGDAIRB                 -- �߼���ΥԼ��ʧ��
                ,0                                                                      AS MAIRB                   -- �߼�����Ч����
                ,T1.BALANCE                                                            AS EADAIRB                 -- �߼���ΥԼ���ձ�¶
                ,CASE WHEN T6.DEFAULTFLAG='1' THEN '1'
                      ELSE '0'
                 END                                                                    AS DEFAULTFLAG             -- ΥԼ��ʶ
                ,T6.BEELVALUE                                                           AS BEEL                    -- ��ΥԼ��¶Ԥ����ʧ����
                ,T6.LGDVALUE                                                            AS DEFAULTLGD              -- ��ΥԼ��¶ΥԼ��ʧ��
                ,'0'                                                                    AS EQUITYEXPOFLAG          -- ��Ȩ��¶��ʶ
                ,''                                                                     AS EQUITYINVESTTYPE        -- ��ȨͶ�ʶ�������
                ,''                                                                     AS EQUITYINVESTCAUSE       -- ��ȨͶ���γ�ԭ��
                ,'0'                                                                    AS SLFLAG                  -- רҵ�����ʶ       רҵ��������ֶ�һ���ȸ���
                ,''                                                                     AS SLTYPE                  -- רҵ��������
                ,''                                                                     AS PFPHASE                 -- ��Ŀ���ʽ׶�
                ,'01'                                                                   AS REGURATING              -- �������
                ,''                                                                     AS CBRCMPRATINGFLAG        -- ������϶������Ƿ��Ϊ����
                ,''                                                                     AS LARGEFLUCFLAG           -- �Ƿ񲨶��Խϴ�
                ,'0'                                                                    AS LIQUEXPOFLAG            -- �Ƿ���������з��ձ�¶
                ,''                                                                     AS PAYMENTDEALFLAG         -- �Ƿ����Ը�ģʽ
                ,0                                                                      AS DELAYTRADINGDAYS        -- �ӳٽ�������
                ,'0'                                                                    AS SECURITIESFLAG          -- �м�֤ȯ��ʶ
                ,''                                                                     AS SECUISSUERID            -- ֤ȯ������ID
                ,''                                                                     AS RATINGDURATIONTYPE      -- ������������
                ,''                                                                     AS SECUISSUERATING         -- ֤ȯ���еȼ�
                ,0                                                                      AS SECURESIDUALM           -- ֤ȯʣ������
                ,1                                                                      AS SECUREVAFREQUENCY       -- ֤ȯ�ع�Ƶ��
                ,'0'                                                                    AS CCPTRANFLAG             -- �Ƿ����뽻�׶�����ؽ���
                ,''                                                                     AS CCPID                   -- ���뽻�׶���ID
                ,'0'                                                                    AS QUALCCPFLAG             -- �Ƿ�ϸ����뽻�׶���
                ,''                                                                     AS BANKROLE                -- ���н�ɫ
                ,''                                                                     AS CLEARINGMETHOD          -- ���㷽ʽ
                ,'0'                                                                    AS BANKASSETFLAG           -- �Ƿ������ύ�ʲ�
                ,''                                                                     AS MATCHCONDITIONS         -- �����������
                ,'0'                                                                    AS SFTFLAG                 -- ֤ȯ���ʽ��ױ�ʶ
                ,''                                                                     AS MASTERNETAGREEFLAG      -- ���������Э���ʶ
                ,''                                                                     AS MASTERNETAGREEID        -- ���������Э��ID
                ,''                                                                     AS SFTTYPE                 -- ֤ȯ���ʽ�������
                ,''                                                                     AS SECUOWNERTRANSFLAG      -- ֤ȯ����Ȩ�Ƿ�ת��
                ,'0'                                                                    AS OTCFLAG                 -- �����������߱�ʶ
                ,''                                                                     AS VALIDNETTINGFLAG        -- ��Ч�������Э���ʶ
                ,''                                                                     AS VALIDNETAGREEMENTID     -- ��Ч�������Э��ID
                ,''                                                                     AS OTCTYPE                 -- ����������������
                ,''                                                                     AS DEPOSITRISKPERIOD       -- ��֤������ڼ�
                ,0                                                                      AS MTM                     -- ���óɱ�
                ,''                                                                     AS MTMCURRENCY             -- ���óɱ�����
                ,''                                                                     AS BUYERORSELLER           -- ������
                ,''                                                                     AS QUALROFLAG              -- �ϸ�����ʲ���ʶ
                ,''                                                                     AS ROISSUERPERFORMFLAG     -- �����ʲ��������Ƿ�����Լ
                ,''                                                                     AS BUYERINSOLVENCYFLAG     -- ���ñ������Ƿ��Ʋ�
                ,0                                                                      AS NONPAYMENTFEES          -- ��δ֧������
                ,'1'                                                                    AS RETAILEXPOFLAG          -- ���۱�¶��ʶ
                ,CASE WHEN T6.RISK_EXPOSURE='01' THEN '020401'
                      WHEN T6.RISK_EXPOSURE='02' THEN '020403'
                      ELSE '020402'
                 END                                                                    AS RETAILCLAIMTYPE         -- ����ծȨ����
                ,CASE WHEN T6.RISK_EXPOSURE='01' THEN '01'
                      ELSE '02'
                 END                                                                    AS MORTGAGETYPE            -- ס����Ѻ��������
                ,1                                                                      AS EXPONUMBER              -- ���ձ�¶����
                ,0.8                                                                    AS LTV                     --�����ֵ��  ͳһ����
                ,T6.MOB                                                                 AS AGING                   --����
                ,CASE WHEN T1.NEWDEFAULTFLAG='0' THEN '1'
                      ELSE '0'
                 END                                                                    AS NEWDEFAULTDEBTFLAG      --����ΥԼծ���ʶ
                ,T6.PDMODELCODE                                                         AS PDPOOLMODELID           -- PD�ֳ�ģ��ID
                ,T6.LGDMODELCODE                                                        AS LGDPOOLMODELID          -- LGD�ֳ�ģ��ID
                ,T6.CCFMODELCODE                                                        AS CCFPOOLMODELID          -- CCF�ֳ�ģ��ID
                ,T6.PDCODE                                                              AS PDPOOLID                -- ����PD��ID
                ,T6.LGDCODE                                                             AS LGDPOOLID               -- ����LGD��ID
                ,T6.CCFCODE                                                             AS CCFPOOLID               -- ����CCF��ID
                ,CASE WHEN T10.PROJECTNO IS NULL THEN '0'
                      ELSE '1'
                 END                                                                    AS ABSUAFLAG           --�ʲ�֤ȯ�������ʲ���ʶ
                ,CASE WHEN T10.PROJECTNO IS NULL THEN ''
                      ELSE T10.PROJECTNO
                 END                                                                    AS ABSPOOLID           --֤ȯ���ʲ���ID
                ,''                                                                     AS GROUPID                 -- ������
                ,CASE WHEN T6.DEFAULTFLAG='1' THEN TO_DATE(T6.UPDATETIME,'YYYYMMDD')
                      ELSE NULL
                 END                                                                    AS DefaultDate             -- ΥԼʱ��
                ,0                                                                      AS ABSPROPORTION           --�ʲ�֤ȯ������
                ,0                                                                      AS DEBTORNUMBER            --����˸���
                ,'YQWLD'
    FROM        RWA_DEV.NCM_BUSINESS_DUEBILL T1
    INNER JOIN  RWA_DEV.NCM_BUSINESS_CONTRACT T3
    ON          T1.RELATIVESERIALNO2 = T3.SERIALNO        --�����Խ��Ϊ׼�����Թ�����ͬʱ����Ӧ�üӺ�ͬ����Ч����
    AND         T1.DATANO = T3.DATANO
    INNER JOIN  /*RWA_DEV.NCM_BUSINESS_HISTORY T31
    ON          T1.SERIALNO = T31.SERIALNO
    AND         (T31.OVERDUEBALANCE + T31.DULLBALANCE + T31.BADBALANCE) > 0   --ȡ�����ڵļ�¼
    AND         T31.DATANO = P_DATA_DT_STR*/
                (SELECT CRDT_ACCT_NO LNCBCERNO, -- �Ŵ�ϵͳ��ݺ�
                        L.SBJT_CD,--��Ŀ
                        CASE WHEN CCY_CD = '01' OR CCY_CD IS NULL THEN 'CNY' ELSE CCY_CD END as CCY_CD, --����
                        CUR_BAL BALANCE --���
                   FROM rwa_dev.BRD_LOAN_NOR L --BRD_LOAN_NOR-��������
                  WHERE L.SBJT_CD = '13100001' --΢�������ڴ���
                  AND   L.DATANO = P_DATA_DT_STR
                  AND   L.CUR_BAL>0
                    ) T31
    ON           T1.SERIALNO = T31.LNCBCERNO
    LEFT JOIN   RWA.ORG_INFO T4
    ON           decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID) = T4.ORGID
    LEFT JOIN   RWA.CODE_LIBRARY T5
    ON          T3.DIRECTION = T5.ITEMNO
    AND         T5.CODENO = 'IndustryType'
    LEFT JOIN   RWA_DEV.RWA_TEMP_LGDLEVEL T6    --����ծ������
    ON          T1.RELATIVESERIALNO2 = T6.BUSINESSID
    AND         T6.BUSINESSTYPE <> 'CREDITCARD'                       -- ��ȡ���ÿ�
    LEFT JOIN   (SELECT DISTINCT AA.PROJECTNO,AA.CONTRACTNO   --�ж��Ƿ��ʲ�֤ȯ��
                   FROM RWA_DEV.NCM_ABS_PROJECT_ASSET AA
             INNER JOIN RWA_DEV.NCM_ABS_PROJECT_INFO BB
                     ON AA.PROJECTNO = BB.PROJECTNO
                    AND BB.DATANO = P_DATA_DT_STR
                    AND BB.PROJECTSTATUS = '0401'            --����ɹ�
                  WHERE AA.DATANO = P_DATA_DT_STR
                ) T10
    ON          T3.SERIALNO = T10.CONTRACTNO
    LEFT JOIN   RWA_DEV.ncm_rwa_risk_expo_rst T11
    ON          T1.SERIALNO = T11.OBJECTNO
    AND         T11.OBJECTTYPE = 'BusinessDuebill'
    AND         T11.DATANO = P_DATA_DT_STR
    LEFT JOIN   RWA_DEV.RWA_CD_CODE_MAPPING T12
    ON          T11.RISKEXPOSURERESULT = T12.SITEMNO
    AND         T12.SCODENO = 'RwaResultType'
    LEFT JOIN   RWA_DEV.NCM_CLASSIFY_RECORD T13
    ON          T1.RELATIVESERIALNO2=T13.OBJECTNO
    AND         T13.OBJECTTYPE = 'TwelveClassify'
    AND         T13.ISWORK = '1'
    AND         T13.DATANO = P_DATA_DT_STR
    LEFT JOIN   RWA_DEV.NCM_CUSTOMER_INFO T16
    ON          T1.CUSTOMERID = T16.CUSTOMERID
    AND         T1.DATANO = T16.DATANO
    LEFT JOIN   (
                select OBJECTNO, DIRECTION
                  from (select T.OBJECTNO,
                               T.DIRECTION,
                               ROW_NUMBER() OVER(PARTITION BY T.OBJECTNO order by T.SERIALNO DESC) AS RM
                          from RWA_DEV.NCM_PUTOUT_SCHEME T
                         where T.DATANO = P_DATA_DT_STR
                           and T.OBJECTTYPE = 'BusinessContract'
                           and T.DIRECTION IS NOT NULL)
                 where RM = 1
                ) CPS                 --�����ҵ�����ҵͶ��������ñ�ȡ
    ON          T3.SERIALNO = CPS.OBJECTNO
    LEFT JOIN   RWA.CODE_LIBRARY CL
    ON          CPS.DIRECTION = CL.ITEMNO
    AND         CL.CODENO = 'IndustryType'
    WHERE       T1.DATANO = P_DATA_DT_STR
    --AND         T1.BUSINESSTYPE = '11103030'  --ֻȡ΢����ҵ��
    ;
    COMMIT;

    /*7 ���� ��ͨҵ������ڴ�����ϸ ���뵽Ŀ���*/ 
    INSERT INTO RWA_DEV.RWA_XD_EXPOSURE(
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
               ,flag
    )WITH TEMP_EXPOSURE AS (
                 --����ȡ����߼� pxl 2019/04/11                           
                 SELECT CRDT_ACCT_NO LNCBCERNO, -- �Ŵ�ϵͳ��ݺ�
                        L.SBJT_CD,--��Ŀ
                        CASE
                          WHEN CCY_CD = '01' OR CCY_CD IS NULL THEN
                           'CNY' ELSE CCY_CD
                        END CCY_CD, --����
                        CUR_BAL BALANCE --���
                   FROM rwa_dev.BRD_LOAN_NOR L --BRD_LOAN_NOR-��������
                  WHERE substr(L.SBJT_CD,1,4) = '1310' 
                    AND L.SBJT_CD <> '13100001' ----���в���΢���������ڴ���
                    AND L.DATANO = P_DATA_DT_STR
                    AND L.CUR_BAL>0
                           )
    SELECT
                TO_DATE(T1.DATANO,'YYYYMMDD')                                           AS DATADATE                -- ��������
                ,T1.DATANO                                                              AS DATANO                  -- ������ˮ��
                ,'YQ'||T1.SERIALNO                                                      AS EXPOSUREID              -- ���ձ�¶ID
                ,T1.SERIALNO                                                            AS DUEID                   -- ծ��ID
                ,'XD'                                                                   AS SSYSID                  -- ԴϵͳID
                ,T1.RELATIVESERIALNO2                                                   AS CONTRACTID              -- ��ͬID
                ,CASE WHEN REPLACE(T1.CUSTOMERID,'NCM_','') IS NULL THEN DECODE(T3.LINETYPE,'0030','XN-GRKH','XN-YBGS')                 --���ͻ�IDΪ�գ�����Ϊ���˾�����Ϊ���˿ͻ�������Ϊһ�㹫˾
                      ELSE T1.CUSTOMERID
                 END                                                                    AS CLIENTID                -- ��������ID
                --,decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID)                      AS SORGID                  -- Դ����ID
                ,case when substr(decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID),1,1)='@' then '01000000'
                     else decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID)
                 end
                ,nvl(T4.ORGNAME,'����')                                                             AS SORGNAME                -- Դ��������
                ,nvl(T4.SORTNO,'1010')                                                              AS ORGSORTNO               -- �������������
                --,decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID)                      AS ORGID                   -- ��������ID
                ,case when substr(decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID),1,1)='@' then '01000000'
                     else decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID)
                 end
                ,nvl(T4.ORGNAME,'����')                                                             AS ORGNAME                 -- ������������
                --,decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID)                      AS ACCORGID                -- �������ID
                ,case when substr(decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID),1,1)='@' then '01000000'
                     else decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID)
                 end
                ,nvl(T4.ORGNAME,'����')                                                             AS ACCORGNAME              -- �����������
                ,NVL(T3.DIRECTION,CPS.DIRECTION)                                        AS INDUSTRYID              -- ������ҵ����
                ,NVL(T5.ITEMNAME,CL.ITEMNAME)                                           AS INDUSTRYNAME            -- ������ҵ����
                ,CASE WHEN T1.BUSINESSCURRENCY<>'CNY' THEN '0102'                            --��ҵı���ҵ��   ����-ó��
                      WHEN T1.BUSINESSTYPE IN('10302010','10302015','10302020') THEN '0401'  --����ҵ��        ͬҵ�����г���
                      WHEN T3.LINETYPE='0010' THEN '0101'
                      WHEN T3.LINETYPE='0020' THEN '0201'
                      WHEN T3.LINETYPE='0030' THEN '0301'
                      WHEN T3.LINETYPE='0040' THEN '0401'
                      ELSE '0101'
                 END                                                                    AS BUSINESSLINE            -- ����
                ,''                                                                     AS ASSETTYPE               -- �ʲ�����
                ,''                                                                     AS ASSETSUBTYPE            -- �ʲ�С��
                ,T1.BUSINESSTYPE                                                        AS BUSINESSTYPEID          -- ҵ��Ʒ�ִ���
                ,T2.TYPENAME                                                            AS BUSINESSTYPENAME        -- ҵ��Ʒ������
                /*,CASE WHEN T1.SERIALNO='20170125c0000373' THEN '02'
                      ELSE '01'
                 END                                                                    AS CREDITRISKDATATYPE      -- ���÷�����������          01-һ�������
                */
                ,CASE WHEN T16.CUSTOMERTYPE = '0310' OR T16.CERTTYPE LIKE 'Ind%'
                      THEN '02' --����
                      ELSE '01' --������
                  END                                                                   AS CREDITRISKDATATYPE  --���÷�����������
                ,'01'                                                                   AS ASSETTYPEOFHAIRCUTS     -- �ۿ�ϵ����Ӧ�ʲ����     01-�ֽ��ֽ�ȼ���
                ,''                                                                     AS BUSINESSTYPESTD         -- Ȩ�ط�ҵ������
                ,CASE WHEN REPLACE(T1.CUSTOMERID,'NCM_','') IS NULL THEN '0112'                                    --���ͻ�IDΪ�գ�Ĭ�� ����(0112)
                      ELSE ''
                 END                                                                    AS EXPOCLASSSTD            -- Ȩ�ط���¶����
                ,CASE WHEN REPLACE(T1.CUSTOMERID,'NCM_','') IS NULL THEN DECODE(T3.LINETYPE,'0030','011215','011216')                          --���ͻ�IDΪ�գ�����Ϊ���˾�Ĭ�� ��������75%����Ȩ�ص��ʲ�(011215)������Ĭ�� ��������100%����Ȩ�ص��ʲ�(011216)
                      ELSE ''
                 END                                                                    AS EXPOSUBCLASSSTD         -- Ȩ�ط���¶С��
                ,SUBSTR(T10.DITEMNO,1,4)                                                AS EXPOCLASSIRB            -- ��������¶����
                ,T10.DITEMNO                                                            AS EXPOSUBCLASSIRB         -- ��������¶С��
                ,CASE WHEN T2.OFFSHEETFLAG='EntOff' THEN '02'   --  02-����
                      ELSE '01'                                 --  01-����
                 END                                                                   AS EXPOBELONG              -- ��¶������ʶ
                ,'01'                                                                  AS BOOKTYPE                -- �˻����           01-�����˻�
                ,'03'                                                                  AS REGUTRANTYPE            -- ��ܽ�������      03-��Ѻ����
                ,'0'                                                                   AS REPOTRANFLAG            -- �ع����ױ�ʶ       0-��
                ,1                                                                     AS REVAFREQUENCY           -- �ع�Ƶ��
                ,t31.CCY_CD/*NVL(T1.BUSINESSCURRENCY,'CNY')*/                                        AS CURRENCY                -- ����
                ,T1.BALANCE                                                           AS NORMALPRINCIPAL         -- �����������
                ,0                                                                     AS OVERDUEBALANCE          -- �������
                ,0                                                                     AS NONACCRUALBALANCE       -- ��Ӧ�����
                ,T1.BALANCE                                                           AS ONSHEETBALANCE          -- �������
                ,0                                                                     AS NORMALINTEREST          -- ������Ϣ
                ,0                                                                     AS ONDEBITINTEREST         -- ����ǷϢ
                ,0                                                                     AS OFFDEBITINTEREST        -- ����ǷϢ
                ,0                                                                     AS EXPENSERECEIVABLE       -- Ӧ�շ���
                ,T1.BALANCE                                                           AS ASSETBALANCE            -- �ʲ����
                ,'13100000'                                                            AS ACCSUBJECT1             -- ��Ŀһ
                ,''                                                                    AS ACCSUBJECT2             -- ��Ŀ��
                ,''                                                                    AS ACCSUBJECT3             -- ��Ŀ��
                ,NVL(T1.PUTOUTDATE,T3.PUTOUTDATE)                                      AS STARTDATE               -- ��ʼ����
                ,NVL(T1.ACTUALMATURITY,T3.MATURITY)                                    AS DUEDATE                 -- ��������
                ,CASE WHEN (TO_DATE(NVL(T1.ACTUALMATURITY,T3.MATURITY),'YYYYMMDD')-TO_DATE(NVL(T1.PUTOUTDATE,T3.PUTOUTDATE),'YYYYMMDD'))/365<0
                      THEN 0
                      ELSE (TO_DATE(NVL(T1.ACTUALMATURITY,T3.MATURITY),'YYYYMMDD')-TO_DATE(NVL(T1.PUTOUTDATE,T3.PUTOUTDATE),'YYYYMMDD'))/365
                END                                                                    AS ORIGINALMATURITY        -- ԭʼ����
                ,CASE WHEN (TO_DATE(NVL(T1.ACTUALMATURITY,T3.MATURITY),'YYYYMMDD')-TO_DATE(T1.DATANO,'YYYYMMDD'))/365<0
                      THEN 0
                      ELSE (TO_DATE(NVL(T1.ACTUALMATURITY,T3.MATURITY),'YYYYMMDD')-TO_DATE(T1.DATANO,'YYYYMMDD'))/365
                END                                                                    AS RESIDUALM               -- ʣ������
                ,CASE WHEN NVL(T1.Classifyresult,SUBSTR(T11.FINALLYRESULT,1,1))='A' THEN '01'
                      WHEN NVL(T1.Classifyresult,SUBSTR(T11.FINALLYRESULT,1,1))='B' THEN '02'       --ʮ��������תΪ�弶����
                      WHEN NVL(T1.Classifyresult,SUBSTR(T11.FINALLYRESULT,1,1))='C' THEN '03'
                      WHEN NVL(T1.Classifyresult,SUBSTR(T11.FINALLYRESULT,1,1))='D' THEN '04'
                      WHEN NVL(T1.Classifyresult,SUBSTR(T11.FINALLYRESULT,1,1))='E' THEN '05'
                      ELSE NVL(T1.Classifyresult,SUBSTR(T11.FINALLYRESULT,1,1))
                 END                                                                   AS RISKCLASSIFY            -- ���շ���
                ,'01'                                                                  AS EXPOSURESTATUS          -- ���ձ�¶״̬  01-����
                ,T1.OVERDUEDAYS                                                        AS OVERDUEDAYS             -- ��������
                ,0                                                                     AS SPECIALPROVISION        -- ר��׼����-������
                ,0                                                                     AS GENERALPROVISION        -- һ��׼����
                ,0                                                                     AS ESPECIALPROVISION       -- �ر�׼����
                ,T1.CANCELSUM                                                          AS WRITTENOFFAMOUNT        -- �Ѻ������
                ,CASE WHEN T2.OFFSHEETFLAG='EntOff'  THEN '03'                         --  03-ʵ�ʱ���ҵ��
                      ELSE ''
                 END                                                                   AS OffExpoSource           -- ���Ⱪ¶��Դ
                ,''                                                                    AS OffBusinessType         -- ����ҵ������
                ,''                                                                    AS OffBusinessSdvsSTD      -- Ȩ�ط�����ҵ������ϸ��
                ,'1'                                                                   AS UncondCancelFlag        -- �Ƿ����ʱ����������
                ,''                                                                    AS CCFLevel                -- ����ת��ϵ������
                ,NULL                                                                  AS CCFAIRB                 -- �߼�������ת��ϵ��
                ,'01'                                                                  AS CLAIMSLEVEL             -- ծȨ����
                ,'0'                                                                   AS BONDFLAG                -- �Ƿ�Ϊծȯ
                ,'02'                                                                  AS BONDISSUEINTENT         -- ծȯ����Ŀ��
                ,'0'                                                                   AS NSUREALPROPERTYFLAG     -- �Ƿ�����ò�����
                ,''                                                                    AS REPASSETTERMTYPE        -- ��ծ�ʲ���������
                ,'0'                                                                   AS DEPENDONFPOBFLAG        -- �Ƿ�����������δ��ӯ��
                ,T6.PDADJLEVEL                                                         AS IRATING                 -- �ڲ�����
                ,T6.PD                                                                 AS PD                      -- ΥԼ����
                ,''                                                                    AS LGDLEVEL                -- ΥԼ��ʧ�ʼ���
                ,0                                                                     AS LGDAIRB                 -- �߼���ΥԼ��ʧ��
                ,0                                                                     AS MAIRB                   -- �߼�����Ч����
                ,T1.BALANCE                                                           AS EADAIRB                 -- �߼���ΥԼ���ձ�¶
                ,CASE WHEN T6.PDADJCODE='D' THEN '1'
                      ELSE '0'
                 END                                                                   AS DEFAULTFLAG             -- ΥԼ��ʶ
                ,0                                                                  AS BEEL                    -- ��ΥԼ��¶Ԥ����ʧ����
                ,0                                                                     AS DEFAULTLGD              -- ��ΥԼ��¶ΥԼ��ʧ��
                ,'0'                                                                   AS EQUITYEXPOFLAG          -- ��Ȩ��¶��ʶ
                ,''                                                                    AS EQUITYINVESTTYPE        -- ��ȨͶ�ʶ�������
                ,''                                                                    AS EQUITYINVESTCAUSE       -- ��ȨͶ���γ�ԭ��
                ,'0'                                                                   AS SLFLAG                  -- רҵ�����ʶ       רҵ��������ֶ�һ���ȸ���
                ,''                                                                    AS SLTYPE                  -- רҵ��������
                ,''                                                                    AS PFPHASE                 -- ��Ŀ���ʽ׶�
                ,'01'                                                                  AS REGURATING              -- �������
                ,''                                                                    AS CBRCMPRATINGFLAG        -- ������϶������Ƿ��Ϊ����
                ,''                                                                    AS LARGEFLUCFLAG           -- �Ƿ񲨶��Խϴ�
                ,'0'                                                                   AS LIQUEXPOFLAG            -- �Ƿ���������з��ձ�¶
                ,''                                                                    AS PAYMENTDEALFLAG         -- �Ƿ����Ը�ģʽ
                ,0                                                                     AS DELAYTRADINGDAYS        -- �ӳٽ�������
                ,'0'                                                                   AS SECURITIESFLAG          -- �м�֤ȯ��ʶ
                ,''                                                                    AS SECUISSUERID            -- ֤ȯ������ID
                ,''                                                                    AS RATINGDURATIONTYPE      -- ������������
                ,''                                                                    AS SECUISSUERATING         -- ֤ȯ���еȼ�
                ,0                                                                     AS SECURESIDUALM           -- ֤ȯʣ������
                ,1                                                                     AS SECUREVAFREQUENCY       -- ֤ȯ�ع�Ƶ��
                ,'0'                                                                   AS CCPTRANFLAG             -- �Ƿ����뽻�׶�����ؽ���
                ,''                                                                    AS CCPID                   -- ���뽻�׶���ID
                ,'0'                                                                   AS QUALCCPFLAG             -- �Ƿ�ϸ����뽻�׶���
                ,''                                                                    AS BANKROLE                -- ���н�ɫ
                ,''                                                                    AS CLEARINGMETHOD          -- ���㷽ʽ
                ,'0'                                                                   AS BANKASSETFLAG           -- �Ƿ������ύ�ʲ�
                ,''                                                                    AS MATCHCONDITIONS         -- �����������
                ,'0'                                                                   AS SFTFLAG                 -- ֤ȯ���ʽ��ױ�ʶ
                ,''                                                                    AS MASTERNETAGREEFLAG      -- ���������Э���ʶ
                ,''                                                                    AS MASTERNETAGREEID        -- ���������Э��ID
                ,''                                                                    AS SFTTYPE                 -- ֤ȯ���ʽ�������
                ,''                                                                    AS SECUOWNERTRANSFLAG      -- ֤ȯ����Ȩ�Ƿ�ת��
                ,'0'                                                                   AS OTCFLAG                 -- �����������߱�ʶ
                ,''                                                                    AS VALIDNETTINGFLAG        -- ��Ч�������Э���ʶ
                ,''                                                                    AS VALIDNETAGREEMENTID     -- ��Ч�������Э��ID
                ,''                                                                    AS OTCTYPE                 -- ����������������
                ,''                                                                    AS DEPOSITRISKPERIOD       -- ��֤������ڼ�
                ,0                                                                     AS MTM                     -- ���óɱ�
                ,''                                                                    AS MTMCURRENCY             -- ���óɱ�����
                ,''                                                                    AS BUYERORSELLER           -- ������
                ,''                                                                    AS QUALROFLAG              -- �ϸ�����ʲ���ʶ
                ,''                                                                    AS ROISSUERPERFORMFLAG     -- �����ʲ��������Ƿ�����Լ
                ,''                                                                    AS BUYERINSOLVENCYFLAG     -- ���ñ������Ƿ��Ʋ�
                ,0                                                                     AS NONPAYMENTFEES          -- ��δ֧������
                ,'0'                                                                   AS RETAILEXPOFLAG          -- ���۱�¶��ʶ
                ,''                                                                    AS RETAILCLAIMTYPE         -- ����ծȨ����
                ,''                                                                    AS MORTGAGETYPE            -- ס����Ѻ��������
                ,1                                                                     AS EXPONUMBER              -- ���ձ�¶����
                ,0.8                                                                   AS LTV                     --�����ֵ��
                ,0                                                                     AS AGING                   --����
                ,''                                                                    AS NEWDEFAULTDEBTFLAG      --����ΥԼծ���ʶ
                ,''                                                                    AS PDPOOLMODELID           -- PD�ֳ�ģ��ID
                ,''                                                                    AS LGDPOOLMODELID          -- LGD�ֳ�ģ��ID
                ,''                                                                    AS CCFPOOLMODELID          -- CCF�ֳ�ģ��ID
                ,''                                                                    AS PDPOOLID                -- ����PD��ID
                ,''                                                                    AS LGDPOOLID               -- ����LGD��ID
                ,''                                                                    AS CCFPOOLID               -- ����CCF��ID
                ,CASE WHEN T9.PROJECTNO IS NULL THEN '0'
                      ELSE '1'
                 END                                                                   AS ABSUAFLAG           --�ʲ�֤ȯ�������ʲ���ʶ
                ,CASE WHEN T9.PROJECTNO IS NULL THEN ''
                      ELSE T9.PROJECTNO
                 END                                                                   AS ABSPOOLID           --֤ȯ���ʲ���ID
                ,''                                                                    AS GROUPID                 -- ������
                ,CASE WHEN T6.PDADJCODE='D' THEN TO_DATE(T6.PDVAVLIDDATE,'YYYYMMDD')
                      ELSE NULL
                 END                                                                   AS DefaultDate             -- ΥԼʱ��
                ,0                                                                     AS ABSPROPORTION           --�ʲ�֤ȯ������
                ,0                                                                     AS DEBTORNUMBER            --����˸���
                ,'YQ'
    FROM        RWA_DEV.NCM_BUSINESS_DUEBILL T1
    INNER JOIN  RWA_DEV.NCM_BUSINESS_TYPE T2
    ON          T1.BUSINESSTYPE = T2.TYPENO
    AND         T1.DATANO = T2.DATANO
    INNER JOIN  RWA_DEV.NCM_BUSINESS_CONTRACT T3
    ON          T1.RELATIVESERIALNO2 = T3.SERIALNO          --�����Խ��Ϊ׼�����Թ�����ͬʱ����Ӧ�üӺ�ͬ����Ч����
    AND         T1.DATANO = T3.DATANO
    INNER JOIN  TEMP_EXPOSURE T31                           --������ʱ��ȡ�����
    ON          T1.SERIALNO = T31.LNCBCERNO
    LEFT JOIN   RWA.ORG_INFO T4
    ON          decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID)  = T4.ORGID
    LEFT JOIN   RWA.CODE_LIBRARY T5
    ON          T3.DIRECTION = T5.ITEMNO
    AND         T5.CODENO = 'IndustryType'
    LEFT JOIN   RWA_DEV.RWA_TEMP_PDLEVEL T6
    ON          T1.CUSTOMERID = T6.CUSTID
    LEFT JOIN   (SELECT DISTINCT AA.PROJECTNO,AA.CONTRACTNO   --�ж��Ƿ��ʲ�֤ȯ��
                   FROM RWA_DEV.NCM_ABS_PROJECT_ASSET AA
             INNER JOIN RWA_DEV.NCM_ABS_PROJECT_INFO BB
                     ON AA.PROJECTNO = BB.PROJECTNO
                    AND BB.DATANO = P_DATA_DT_STR
                    AND BB.PROJECTSTATUS = '0401'            --����ɹ�
                  WHERE AA.DATANO = P_DATA_DT_STR
                ) T9
    ON          T3.SERIALNO = T9.CONTRACTNO
    LEFT JOIN   RWA_DEV.ncm_rwa_risk_expo_rst T13
    ON          T1.SERIALNO = T13.OBJECTNO
    AND         T13.OBJECTTYPE = 'BusinessDuebill'
    AND         T13.DATANO = P_DATA_DT_STR
    LEFT JOIN   RWA_DEV.RWA_CD_CODE_MAPPING T10
    ON          T13.RISKEXPOSURERESULT = T10.SITEMNO
    AND         T10.SCODENO = 'RwaResultType'
    LEFT JOIN   RWA_DEV.NCM_CLASSIFY_RECORD T11
    ON          T1.RELATIVESERIALNO2 = T11.OBJECTNO
    AND         T11.OBJECTTYPE = 'TwelveClassify'
    AND         T11.ISWORK = '1'
    AND         T11.DATANO = P_DATA_DT_STR
    LEFT JOIN   RWA_DEV.NCM_CUSTOMER_INFO T16
    ON          T1.CUSTOMERID = T16.CUSTOMERID
    AND         T1.DATANO = T16.DATANO
    LEFT JOIN   (
                select OBJECTNO, DIRECTION
                  from (select T.OBJECTNO,
                               T.DIRECTION,
                               ROW_NUMBER() OVER(PARTITION BY T.OBJECTNO order by T.SERIALNO DESC) AS RM
                          from RWA_DEV.NCM_PUTOUT_SCHEME T
                         where T.DATANO = P_DATA_DT_STR
                           and T.OBJECTTYPE = 'BusinessContract'
                           and T.DIRECTION IS NOT NULL)
                 where RM = 1
                ) CPS                 --�����ҵ�����ҵͶ��������ñ�ȡ
    ON          T3.SERIALNO = CPS.OBJECTNO
    LEFT JOIN   RWA.CODE_LIBRARY CL
    ON          CPS.DIRECTION = CL.ITEMNO
    AND         CL.CODENO = 'IndustryType'
    WHERE       T1.DATANO = P_DATA_DT_STR
    ;
    COMMIT;
    
    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_XD_EXPOSURE',cascade => true);

    /*Ŀ�������ͳ��*/
    --ͳ�Ʋ���ļ�¼
    SELECT COUNT(1) INTO v_count1 FROM RWA_DEV.RWA_XD_EXPOSURE;

    p_po_rtncode := '1';
    p_po_rtnmsg  := '�ɹ�'||'-'||v_count1;
    --�����쳣
    EXCEPTION
    WHEN OTHERS THEN
         --Dbms_output.Put_line('������,�������Ϊ:'||sqlcode||';������ϢΪ:'||sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace);
          ROLLBACK;
          p_po_rtncode := sqlcode;
          p_po_rtnmsg  := '�Ŵ�ϵͳ�����Ϣ(RWA_XD_EXPOSURE)ETLת��ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_XD_EXPOSURE;
/

