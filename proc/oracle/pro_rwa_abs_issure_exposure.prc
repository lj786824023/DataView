CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_ABS_ISSURE_EXPOSURE(
                             p_data_dt_str  IN  VARCHAR2,    --��������
                             p_po_rtncode   OUT  VARCHAR2,    --���ر��
                             p_po_rtnmsg    OUT VARCHAR2    --��������
)
  /*
    �洢��������:PRO_RWA_ABS_ISSURE_EXPOSURE
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
    */
  AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  v_pro_name VARCHAR2(200) := 'PRO_RWA_ABS_ISSURE_EXPOSURE';
  --�����쳣����
  v_raise EXCEPTION;
  --���嵱ǰ����ļ�¼��
  v_count1 INTEGER;

  BEGIN
    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));


    /*�����ȫ�����ݼ��������Ŀ���*/
    --1.���Ŀ����е�ԭ�м�¼
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_ABS_ISSURE_EXPOSURE';

    /*������Ҫ�ʲ�֤ȯ��ҵ����Ϣ(���ǶԹ�ҵ��)*/
    INSERT INTO RWA_DEV.RWA_ABS_ISSURE_EXPOSURE(
                 DATADATE                                                     -- ��������
                ,DATANO                                                       -- ������ˮ��
                ,EXPOSUREID                                                   -- ���ձ�¶ID
                ,DUEID                                                        -- ծ��ID
                ,SSYSID                                                       -- ԴϵͳID
                ,CONTRACTID                                                   -- ��ͬID
                ,CLIENTID                                                     -- ��������ID
                ,SORGID                                                       -- Դ����ID
                ,SORGNAME                                                     -- Դ��������
                ,ORGID                                                        -- ��������ID
                ,ORGNAME                                                      -- ������������
                ,ACCORGID                                                     -- �������ID
                ,ACCORGNAME                                                   -- �����������
                ,INDUSTRYID                                                   -- ������ҵ����
                ,INDUSTRYNAME                                                 -- ������ҵ����
                ,BUSINESSLINE                                                 -- ����
                ,ASSETTYPE                                                    -- �ʲ�����
                ,ASSETSUBTYPE                                                 -- �ʲ�С��
                ,BUSINESSTYPEID                                               -- ҵ��Ʒ�ִ���
                ,BUSINESSTYPENAME                                             -- ҵ��Ʒ������
                ,CREDITRISKDATATYPE                                           -- ���÷�����������
                ,ASSETTYPEOFHAIRCUTS                                          -- �ۿ�ϵ����Ӧ�ʲ����
                ,BUSINESSTYPESTD                                              -- Ȩ�ط�ҵ������
                ,EXPOCLASSSTD                                                 -- Ȩ�ط���¶����
                ,EXPOSUBCLASSSTD                                              -- Ȩ�ط���¶С��
                ,EXPOCLASSIRB                                                 -- ��������¶����
                ,EXPOSUBCLASSIRB                                              -- ��������¶С��
                ,EXPOBELONG                                                   -- ��¶������ʶ
                ,BOOKTYPE                                                     -- �˻����
                ,REGUTRANTYPE                                                 -- ��ܽ�������
                ,REPOTRANFLAG                                                 -- �ع����ױ�ʶ
                ,REVAFREQUENCY                                                -- �ع�Ƶ��
                ,CURRENCY                                                     -- ����
                ,NORMALPRINCIPAL                                              -- �����������
                ,OVERDUEBALANCE                                               -- �������
                ,NONACCRUALBALANCE                                            -- ��Ӧ�����
                ,ONSHEETBALANCE                                               -- �������
                ,NORMALINTEREST                                               -- ������Ϣ
                ,ONDEBITINTEREST                                              -- ����ǷϢ
                ,OFFDEBITINTEREST                                             -- ����ǷϢ
                ,EXPENSERECEIVABLE                                            -- Ӧ�շ���
                ,ASSETBALANCE                                                 -- �ʲ����
                ,ACCSUBJECT1                                                  -- ��Ŀһ
                ,ACCSUBJECT2                                                  -- ��Ŀ��
                ,ACCSUBJECT3                                                  -- ��Ŀ��
                ,STARTDATE                                                    -- ��ʼ����
                ,DUEDATE                                                      -- ��������
                ,ORIGINALMATURITY                                             -- ԭʼ����
                ,RESIDUALM                                                    -- ʣ������
                ,RISKCLASSIFY                                                 -- ���շ���
                ,EXPOSURESTATUS                                               -- ���ձ�¶״̬
                ,OVERDUEDAYS                                                  -- ��������
                ,SPECIALPROVISION                                             -- ר��׼����
                ,GENERALPROVISION                                             -- һ��׼����
                ,ESPECIALPROVISION                                            -- �ر�׼����
                ,WRITTENOFFAMOUNT                                             -- �Ѻ������
                ,OFFEXPOSOURCE                                                -- ���Ⱪ¶��Դ
                ,OFFBUSINESSTYPE                                              -- ����ҵ������
                ,OFFBUSINESSSDVSSTD                                           -- Ȩ�ط�����ҵ������ϸ��
                ,UNCONDCANCELFLAG                                             -- �Ƿ����ʱ����������
                ,CCFLEVEL                                                     -- ����ת��ϵ������
                ,CCFAIRB                                                      -- �߼�������ת��ϵ��
                ,CLAIMSLEVEL                                                  -- ծȨ����
                ,BONDFLAG                                                     -- �Ƿ�Ϊծȯ
                ,BONDISSUEINTENT                                              -- ծȯ����Ŀ��
                ,NSUREALPROPERTYFLAG                                          -- �Ƿ�����ò�����
                ,REPASSETTERMTYPE                                             -- ��ծ�ʲ���������
                ,DEPENDONFPOBFLAG                                             -- �Ƿ�����������δ��ӯ��
                ,IRATING                                                      -- �ڲ�����
                ,PD                                                           -- ΥԼ����
                ,LGDLEVEL                                                     -- ΥԼ��ʧ�ʼ���
                ,LGDAIRB                                                      -- �߼���ΥԼ��ʧ��
                ,MAIRB                                                        -- �߼�����Ч����
                ,EADAIRB                                                      -- �߼���ΥԼ���ձ�¶
                ,DEFAULTFLAG                                                  -- ΥԼ��ʶ
                ,BEEL                                                         -- ��ΥԼ��¶Ԥ����ʧ����
                ,DEFAULTLGD                                                   -- ��ΥԼ��¶ΥԼ��ʧ��
                ,EQUITYEXPOFLAG                                               -- ��Ȩ��¶��ʶ
                ,EQUITYINVESTTYPE                                             -- ��ȨͶ�ʶ�������
                ,EQUITYINVESTCAUSE                                            -- ��ȨͶ���γ�ԭ��
                ,SLFLAG                                                       -- רҵ�����ʶ
                ,SLTYPE                                                       -- רҵ��������
                ,PFPHASE                                                      -- ��Ŀ���ʽ׶�
                ,REGURATING                                                   -- �������
                ,CBRCMPRATINGFLAG                                             -- ������϶������Ƿ��Ϊ����
                ,LARGEFLUCFLAG                                                -- �Ƿ񲨶��Խϴ�
                ,LIQUEXPOFLAG                                                 -- �Ƿ���������з��ձ�¶
                ,PAYMENTDEALFLAG                                              -- �Ƿ����Ը�ģʽ
                ,DELAYTRADINGDAYS                                             -- �ӳٽ�������
                ,SECURITIESFLAG                                               -- �м�֤ȯ��ʶ
                ,SECUISSUERID                                                 -- ֤ȯ������ID
                ,RATINGDURATIONTYPE                                           -- ������������
                ,SECUISSUERATING                                              -- ֤ȯ���еȼ�
                ,SECURESIDUALM                                                -- ֤ȯʣ������
                ,SECUREVAFREQUENCY                                            -- ֤ȯ�ع�Ƶ��
                ,CCPTRANFLAG                                                  -- �Ƿ����뽻�׶�����ؽ���
                ,CCPID                                                        -- ���뽻�׶���ID
                ,QUALCCPFLAG                                                  -- �Ƿ�ϸ����뽻�׶���
                ,BANKROLE                                                      -- ���н�ɫ
                ,CLEARINGMETHOD                                               -- ���㷽ʽ
                ,BANKASSETFLAG                                                 -- �Ƿ������ύ�ʲ�
                ,MATCHCONDITIONS                                              -- �����������
                ,SFTFLAG                                                      -- ֤ȯ���ʽ��ױ�ʶ
                ,MASTERNETAGREEFLAG                                           -- ���������Э���ʶ
                ,MASTERNETAGREEID                                             -- ���������Э��ID
                ,SFTTYPE                                                      -- ֤ȯ���ʽ�������
                ,SECUOWNERTRANSFLAG                                           -- ֤ȯ����Ȩ�Ƿ�ת��
                ,OTCFLAG                                                      -- �����������߱�ʶ
                ,VALIDNETTINGFLAG                                              -- ��Ч�������Э���ʶ
                ,VALIDNETAGREEMENTID                                          -- ��Ч�������Э��ID
                ,OTCTYPE                                                      -- ����������������
                ,DEPOSITRISKPERIOD                                            -- ��֤������ڼ�
                ,MTM                                                          -- ���óɱ�
                ,MTMCURRENCY                                                  -- ���óɱ�����
                ,BUYERORSELLER                                                -- ������
                ,QUALROFLAG                                                   -- �ϸ�����ʲ���ʶ
                ,ROISSUERPERFORMFLAG                                          -- �����ʲ��������Ƿ�����Լ
                ,BUYERINSOLVENCYFLAG                                          -- ���ñ������Ƿ��Ʋ�
                ,NONPAYMENTFEES                                               -- ��δ֧������
                ,RETAILEXPOFLAG                                               -- ���۱�¶��ʶ
                ,RETAILCLAIMTYPE                                              -- ����ծȨ����
                ,MORTGAGETYPE                                                 -- ס����Ѻ��������
                ,DEBTORNUMBER                                                 -- ����˸���
                ,EXPONUMBER                                                   -- ���ձ�¶����
                ,PDPOOLMODELID                                                -- PD�ֳ�ģ��ID
                ,LGDPOOLMODELID                                               -- LGD�ֳ�ģ��ID
                ,CCFPOOLMODELID                                               -- CCF�ֳ�ģ��ID
                ,PDPOOLID                                                     -- ����PD��ID
                ,LGDPOOLID                                                    -- ����LGD��ID
                ,CCFPOOLID                                                    -- ����CCF��ID
                ,ABSUAFLAG                                                    -- �ʲ�֤ȯ�������ʲ���ʶ
                ,ABSPOOLID                                                    -- ֤ȯ���ʲ���ID
                ,ABSPROPORTION                                                -- �ʲ�֤ȯ������
                ,GROUPID                                                      -- ������
                ,ORGSORTNO                                                    --�������������
                ,LTV                                                          --�����ֵ��
                ,AGING                                                        --����
                ,NEWDEFAULTDEBTFLAG                                           --����ΥԼծ���ʶ
                ,DefaultDate                                                  --ΥԼʱ��
    )
    WITH TMP_ABS_POOL AS (
    			SELECT  		DISTINCT RWAIE.ZCCBH AS ZCCBH          --�ʲ��ش���
          FROM 				RWA.RWA_WS_ABS_ISSUE_EXPOSURE RWAIE
          INNER JOIN  RWA.RWA_WP_DATASUPPLEMENT RWD
          ON          RWAIE.SUPPORGID=RWD.ORGID
          AND         RWD.DATADATE=TO_DATE(p_data_dt_str,'YYYYMMDD')
          AND         RWD.SUPPTMPLID='M-0131'
          AND         RWD.SUBMITFLAG='1'
          INNER JOIN	RWA.RWA_WS_ABS_ISSUE_POOL RWAIP
          ON					RWAIE.ZCCBH = RWAIP.ZCCBH
          AND					RWAIE.DATADATE = RWAIP.DATADATE
          INNER JOIN	RWA.RWA_WP_DATASUPPLEMENT RWD1
          ON 					RWAIP.SUPPORGID = RWD1.ORGID
          AND 				RWD1.DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
          AND 				RWD1.SUPPTMPLID = 'M-0132'
          AND 				RWD1.SUBMITFLAG = '1'
          WHERE				RWAIE.DATADATE=TO_DATE(p_data_dt_str,'YYYYMMDD')
    )
    SELECT
                TO_DATE(T1.DATANO,'YYYYMMDD')                                           AS DATADATE                -- ��������
                ,T1.DATANO                                                              AS DATANO                  -- ������ˮ��
                ,'ABS'||T1.SERIALNO                                                     AS EXPOSUREID              -- ���ձ�¶ID
                ,'ABS'||T1.SERIALNO                                                     AS DUEID                   -- ծ��ID
                ,'ABS'                                                                  AS SSYSID                  -- ԴϵͳID
                ,'ABS'||T1.SERIALNO                                                     AS CONTRACTID              -- ��ͬID
                ,T1.CUSTOMERID                                                          AS CLIENTID                -- ��������ID
                ,T1.OPERATEORGID                                                        AS SORGID                  -- Դ����ID
                ,T3.ORGNAME                                                             AS SORGNAME                -- Դ��������
                ,T1.OPERATEORGID                                                        AS ORGID                   -- ��������ID
                ,T3.ORGNAME                                                             AS ORGNAME                 -- ������������
                ,T1.OPERATEORGID                                                        AS SORGID                  -- Դ����ID
                ,T3.ORGNAME                                                             AS ACCORGNAME              -- �����������
                ,CASE WHEN T2.ATTRIBUTE1='1' THEN T1.DIRECTION
                      ELSE ''
                 END                                                                    AS INDUSTRYID              -- ������ҵ����
                ,CASE WHEN T2.ATTRIBUTE1='1' THEN T5.ITEMNAME
                      ELSE ''
                 END                                                                    AS INDUSTRYNAME            -- ������ҵ����
                ,CASE WHEN T1.BUSINESSCURRENCY<>'CNY' THEN '0102'                                --��ҵı���ҵ��         ����-ó��
                      WHEN T1.BUSINESSTYPE IN('10302010','10302015','10302020') THEN '0401'      --����ҵ��               ͬҵ�����г���
                      WHEN T1.BUSINESSTYPE IN('10201010','1035102010','1035102020') THEN '0102'  --��������֤���羳����   �鵽 ����-ó��
                      WHEN T1.LINETYPE='0010' THEN '0101'
                	    WHEN T1.LINETYPE='0020' THEN '0201'
                	    WHEN T1.LINETYPE='0030' THEN '0301'
                	    WHEN T1.LINETYPE='0040' THEN '0401'
                	    ELSE '0101'
                 END                                                            AS BUSINESSLINE            -- ����
                ,'310'                                                                 AS ASSETTYPE               -- �ʲ�����
                ,'31001'                                                               AS ASSETSUBTYPE            -- �ʲ�С��
                ,T1.BUSINESSTYPE                                                       AS BUSINESSTYPEID          -- ҵ��Ʒ�ִ���
                ,T2.TYPENAME                                                           AS BUSINESSTYPENAME        -- ҵ��Ʒ������
                ,'01'                                                                  AS CREDITRISKDATATYPE      -- ���÷�����������          01-һ�������
                ,'01'                                                                  AS ASSETTYPEOFHAIRCUTS     -- �ۿ�ϵ����Ӧ�ʲ����     01-�ֽ��ֽ�ȼ���
                ,'07'                                                                  AS BUSINESSTYPESTD         -- Ȩ�ط�ҵ������
                ,''                                                                    AS EXPOCLASSSTD            -- Ȩ�ط���¶����
                ,''                                                                    AS EXPOSUBCLASSSTD         -- Ȩ�ط���¶С��
                ,''                                                                    AS EXPOCLASSIRB            -- ��������¶����
                ,''                                                                    AS EXPOSUBCLASSIRB         -- ��������¶С��
                ,'01'                                                                  AS EXPOBELONG              -- ��¶������ʶ
                ,'01'                                                                  AS BOOKTYPE                -- �˻����           01-�����˻�
                ,'03'                                                                  AS REGUTRANTYPE            -- ��ܽ�������      03-��Ѻ����
                ,'0'                                                                   AS REPOTRANFLAG            -- �ع����ױ�ʶ       0-��
                ,1                                                                     AS REVAFREQUENCY           -- �ع�Ƶ��
                ,NVL(T1.BUSINESSCURRENCY,'CNY')                                        AS CURRENCY                -- ����
                ,ROUND(TO_NUMBER(REPLACE(NVL(RWAIU.HTYE,'0'),',','')),6)               AS NORMALPRINCIPAL         -- �����������
                ,0                                                                     AS OVERDUEBALANCE          -- �������
                ,0                                                                     AS NONACCRUALBALANCE       -- ��Ӧ�����
                ,ROUND(TO_NUMBER(REPLACE(NVL(RWAIU.HTYE,'0'),',','')),6)               AS ONSHEETBALANCE          -- �������
                ,0                                                                     AS NORMALINTEREST          -- ������Ϣ
                ,T1.INTERESTBALANCE1                                                   AS ONDEBITINTEREST         -- ����ǷϢ
                ,T1.INTERESTBALANCE2                                                   AS OFFDEBITINTEREST        -- ����ǷϢ
                ,0                                                                     AS EXPENSERECEIVABLE       -- Ӧ�շ���
                ,ROUND(TO_NUMBER(REPLACE(NVL(RWAIU.HTYE,'0'),',','')),6)+T1.INTERESTBALANCE1+T1.INTERESTBALANCE2
                																									                     AS ASSETBALANCE            -- �ʲ����
                ,T4.SUBJECTNO                                                          AS ACCSUBJECT1             -- ��Ŀһ
                ,''                                                                    AS ACCSUBJECT2             -- ��Ŀ��
                ,''                                                                    AS ACCSUBJECT3             -- ��Ŀ��
                ,T1.PUTOUTDATE                                                         AS STARTDATE               -- ��ʼ����
                ,T1.MATURITY                                                           AS DUEDATE             --��������
                ,CASE WHEN (TO_DATE(T1.MATURITY,'YYYYMMDD')-TO_DATE(T1.PUTOUTDATE,'YYYYMMDD'))/365<0
                      THEN 0
                      ELSE (TO_DATE(T1.MATURITY,'YYYYMMDD')-TO_DATE(T1.PUTOUTDATE,'YYYYMMDD'))/365
                 END                                                                   AS OriginalMaturity    --ԭʼ����
                ,CASE WHEN (TO_DATE(T1.MATURITY,'YYYYMMDD')-TO_DATE(T1.DATANO,'YYYYMMDD'))/365<0
                      THEN 0
                      ELSE (TO_DATE(T1.MATURITY,'YYYYMMDD')-TO_DATE(T1.DATANO,'YYYYMMDD'))/365
                 END                                                                   AS ResidualM           --ʣ������
                ,CASE WHEN T1.TWELEVECLASSIFYRESULT IN('B1','B2','B3') THEN '02'       --ʮ��������תΪ�弶����
                      WHEN T1.TWELEVECLASSIFYRESULT IN('C1','C2') THEN '03'
                      WHEN T1.TWELEVECLASSIFYRESULT IN('D1','D2') THEN '04'
                      WHEN T1.TWELEVECLASSIFYRESULT='E' THEN '05'
                      ELSE '01'
                 END                                                                   AS RISKCLASSIFY            -- ���շ���
                ,'01'                                                                  AS EXPOSURESTATUS          -- ���ձ�¶״̬  01-����
                ,0                                                                     AS OVERDUEDAYS             -- ��������
                ,0                                                                     AS SPECIALPROVISION        -- ר��׼����
                ,0                                                                     AS GENERALPROVISION        -- һ��׼����
                ,0                                                                     AS ESPECIALPROVISION       -- �ر�׼����
                ,0                                                                     AS WRITTENOFFAMOUNT        -- �Ѻ������
                ,''                                                                    AS OffExpoSource           -- ���Ⱪ¶��Դ   --  03-ʵ�ʱ���ҵ��
                ,''                                                                    AS OffBusinessType         -- ����ҵ������
                ,''                                                                    AS OffBusinessSdvsSTD      -- Ȩ�ط�����ҵ������ϸ��
                ,'0'                                                                   AS UncondCancelFlag        -- �Ƿ����ʱ����������
                ,''                                                                    AS CCFLevel                -- ����ת��ϵ������
                ,0                                                                     AS CCFAIRB                 -- �߼�������ת��ϵ��
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
                ,0                                                                     AS EADAIRB                 -- �߼���ΥԼ���ձ�¶
                ,'0'                                                                   AS DEFAULTFLAG             -- ΥԼ��ʶ
                ,0.45                                                                  AS BEEL                    -- ��ΥԼ��¶Ԥ����ʧ����
                ,0.45                                                                  AS DEFAULTLGD              -- ��ΥԼ��¶ΥԼ��ʧ��
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
                ,0                                                                     AS DEBTORNUMBER            -- ����˸���
                ,1                                                                     AS EXPONUMBER              -- ���ձ�¶����
                ,''                                                                    AS PDPOOLMODELID           -- PD�ֳ�ģ��ID
                ,''                                                                    AS LGDPOOLMODELID          -- LGD�ֳ�ģ��ID
                ,''                                                                    AS CCFPOOLMODELID          -- CCF�ֳ�ģ��ID
                ,''                                                                    AS PDPOOLID                -- ����PD��ID
                ,''                                                                    AS LGDPOOLID               -- ����LGD��ID
                ,''                                                                    AS CCFPOOLID               -- ����CCF��ID
                ,'1'                                                                   AS ABSUAFLAG               -- �ʲ�֤ȯ�������ʲ���ʶ
                ,RWAIU.ZCCBH                                                           AS ABSPOOLID               -- ֤ȯ���ʲ���ID
                ,1                                                                     AS ABSPROPORTION           -- �ʲ�֤ȯ������
                ,''                                                                    AS GROUPID                 -- ������
                ,T3.SORTNO                                                             AS ORGSORTNO               --�������������
                ,0.8                                                                   AS LTV                     --�����ֵ��
                ,0                                                                     AS AGING                   --����
                ,NULL                                                                  AS NEWDEFAULTDEBTFLAG      --����ΥԼծ���ʶ
                ,NULL                                                                  AS DefaultDate             -- ΥԼʱ��
    FROM 				RWA_DEV.NCM_BUSINESS_CONTRACT T1
    INNER JOIN (SELECT RELATIVESERIALNO2,SUBJECTNO
                FROM   RWA_DEV.NCM_BUSINESS_DUEBILL
                WHERE  DATANO = P_DATA_DT_STR
                AND    ROWID IN (SELECT MAX(ROWID)
                                FROM RWA_DEV.NCM_BUSINESS_DUEBILL
                                WHERE DATANO = P_DATA_DT_STR
                                GROUP BY RELATIVESERIALNO2
                                )
                ) T4
    ON 					T1.SERIALNO = T4.RELATIVESERIALNO2
    LEFT JOIN 	RWA_DEV.NCM_BUSINESS_TYPE T2
    ON 					T1.BUSINESSTYPE = T2.TYPENO
    AND 				T1.DATANO = T2.DATANO
    AND 				T2.SORTNO NOT LIKE '3%'  --�ų������ҵ��
    LEFT JOIN 	RWA.ORG_INFO T3
    ON 					T1.OPERATEORGID = T3.ORGID
    LEFT JOIN 	RWA.CODE_LIBRARY T5
    ON 					T1.DIRECTION = T5.ITEMNO
    AND 				T5.CODENO = 'IndustryType'
    LEFT JOIN 	RWA_DEV.RWA_TEMP_PDLEVEL T6
    ON 					T1.CUSTOMERID = T6.CUSTID
    INNER JOIN 	RWA.RWA_WS_ABS_ISSUE_UNDERASSET RWAIU
    ON 					T1.SERIALNO = RWAIU.HTBH
    AND 				RWAIU.DATADATE = TO_DATE(P_DATA_DT_STR,'YYYYMMDD')
    INNER JOIN  RWA.RWA_WP_DATASUPPLEMENT RWD
    ON          RWAIU.SUPPORGID = RWD.ORGID
    AND         RWD.DATADATE = TO_DATE(P_DATA_DT_STR,'YYYYMMDD')
    AND         RWD.SUPPTMPLID = 'M-0133'
    AND         RWD.SUBMITFLAG = '1'
    INNER JOIN	TMP_ABS_POOL TAP
    ON 					RWAIU.ZCCBH = TAP.ZCCBH
    WHERE 			T1.DATANO = P_DATA_DT_STR
    ;
    COMMIT;





    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_ABS_ISSURE_EXPOSURE',cascade => true);


    /*Ŀ�������ͳ��*/
    --ͳ�Ʋ���ļ�¼
    SELECT COUNT(1) INTO v_count1 FROM RWA_DEV.RWA_ABS_ISSURE_EXPOSURE;

    p_po_rtncode := '1';
    p_po_rtnmsg  := '�ɹ�'||'-'||v_count1;
    --�����쳣
    EXCEPTION
    WHEN OTHERS THEN
         --Dbms_output.Put_line('������,�������Ϊ:'||sqlcode||';������ϢΪ:'||sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace);
          ROLLBACK;
          p_po_rtncode := sqlcode;
          p_po_rtnmsg  := '�ʲ�֤ȯ�������Ϣ(RWA_ABS_ISSURE_EXPOSURE)ETLת��ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_ABS_ISSURE_EXPOSURE;
/

