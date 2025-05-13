CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_DZ_EXPOSURE(
                             p_data_dt_str  IN  VARCHAR2,    --��������
                             p_po_rtncode  OUT  VARCHAR2,    --���ر��
                             p_po_rtnmsg    OUT  VARCHAR2    --��������
)
  /*
    �洢��������:RWA_DEV.PRO_RWA_DZ_EXPOSURE
    ʵ�ֹ���:����¼�ĵ�ծ�ʲ���¶��Ϣ���뵽��¶��
    ���ݿھ�:ȫ��
    ����Ƶ��:�³�����
    ��  ��  :V1.0.0
    ��д��  :YUSJ
    ��дʱ��:2016-04-15
    ��  λ  :�Ϻ���˶��Ϣ�����ɷ����޹�˾
    Դ  ��1  :NCM_ASSET_DEBT_INFO |��ծ�ʲ���Ϣ��
    Ŀ���  :RWA_DEV.RWA_DZ_EXPOSURE|��ծ�ʲ���¶��
    ������  :��
    �����¼(�޸���|�޸�ʱ��|�޸�����):
    */
  AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_DZ_EXPOSURE';
  --�����쳣����
  v_raise EXCEPTION;
  --���嵱ǰ����ļ�¼��
  v_count1 INTEGER;

  BEGIN
    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*�����ȫ�����ݼ��������Ŀ���*/
    --1.���Ŀ����е�ԭ�м�¼
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_DZ_EXPOSURE';
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_DZ_CONTRACT';

    --2.���������������ݴ�Դ����뵽Ŀ�����
    /*���� �Ŵ�ϵͳ�Թ���� ��Ŀ���*/
  /*
    INSERT INTO RWA_DEV.RWA_DZ_EXPOSURE(
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
                 TO_DATE(T1.DATANO,'YYYYMMDD')                                         AS DATADATE                -- ��������
                ,T1.DATANO                                                             AS DATANO                  -- ������ˮ��
                ,'DZ-' || T1.GUARANTYID                                                AS EXPOSUREID              -- ���ձ�¶ID
                ,T1.GUARANTYID                                                         AS DUEID                   -- ծ��ID
                ,'DZ'                                                                  AS SSYSID                  -- ԴϵͳID   ��ծ�ʲ�
                ,'DZ-' || T1.GUARANTYID                                                AS CONTRACTID              -- ��ͬID
                ,'DZ-' || T1.GUARANTYID                                                AS CLIENTID                -- ��������ID
                ,'01050000'	                                                       		 AS SORGID                  -- Դ����ID
                ,'�����ʲ���ծ����'                                                  AS SORGNAME                -- Դ��������
                ,'1010050'                                                             AS ORGSORTNO               --�������������
                ,'01050000'	                                                       		 AS ORGID                   -- ��������ID
                ,'�����ʲ���ծ����'                                                  AS ORGNAME                 -- ������������
                ,'01050000'	                                                       		 AS ACCORGID                -- �������ID
                ,'�����ʲ���ծ����'                                                  AS ACCORGNAME              -- �����������
                ,'999999'                                                              AS INDUSTRYID              -- ������ҵ����
                ,'δ֪'                                                                AS INDUSTRYNAME            -- ������ҵ����
                ,CASE WHEN T4.TYPEDIVISION='1' THEN '0501'
                			ELSE '0401'
                 END	                                                                 AS BUSINESSLINE            -- ����         Ĭ�� ͬҵ
                ,'129'                                                                 AS ASSETTYPE               -- �ʲ�����
                ,'12901'                                                               AS ASSETSUBTYPE            -- �ʲ�С��
                ,CASE WHEN T4.TYPEDIVISION='1' THEN '109040'
                      ELSE '109050'
                 END                                                                   AS BUSINESSTYPEID      		--ҵ��Ʒ�ִ���
                ,CASE WHEN T4.TYPEDIVISION='1' THEN '��ծ�ʲ���������'
                      ELSE '��ծ�ʲ��ǲ�������'
                 END                                                                   AS BUSINESSTYPENAME        -- ҵ��Ʒ������
                ,'01'                                                                  AS CREDITRISKDATATYPE      -- ���÷�����������          01-һ�������
                ,'01'                                                                  AS ASSETTYPEOFHAIRCUTS     -- �ۿ�ϵ����Ӧ�ʲ����     01-�ֽ��ֽ�ȼ���
                ,'10'                                                                  AS BUSINESSTYPESTD         -- Ȩ�ط�ҵ������
                ,''                                                                    AS EXPOCLASSSTD            -- Ȩ�ط���¶����
                ,''                                                                    AS EXPOSUBCLASSSTD         -- Ȩ�ط���¶С��
                ,''                                                                    AS EXPOCLASSIRB            -- ��������¶����
                ,''                                                                    AS EXPOSUBCLASSIRB         -- ��������¶С��
                ,'01'                                                                  AS EXPOBELONG              -- ��¶������ʶ
                ,'01'                                                                  AS BOOKTYPE                -- �˻����           01-�����˻�
                ,'03'                                                                  AS REGUTRANTYPE            -- ��ܽ�������      03-��Ѻ����
                ,'0'                                                                   AS REPOTRANFLAG            -- �ع����ױ�ʶ       0-��
                ,1                                                                     AS REVAFREQUENCY           -- �ع�Ƶ��
                ,'CNY'                                                                 AS CURRENCY                -- ����
                ,T1.ENTRYVALUE                                                         AS NORMALPRINCIPAL         -- �����������
                ,0                                                                     AS OVERDUEBALANCE          -- �������
                ,0                                                                     AS NONACCRUALBALANCE       -- ��Ӧ�����
                ,T1.ENTRYVALUE                                                         AS ONSHEETBALANCE          -- �������
                ,0                                                                     AS NORMALINTEREST          -- ������Ϣ
                ,0                                                                     AS ONDEBITINTEREST         -- ����ǷϢ
                ,0                                                                     AS OFFDEBITINTEREST        -- ����ǷϢ
                ,0                                                                     AS EXPENSERECEIVABLE       -- Ӧ�շ���
                ,T1.ENTRYVALUE	                                                       AS ASSETBALANCE            -- �ʲ����
                ,T3.DITEMNO                                                            AS ACCSUBJECT1             -- ��Ŀһ
                ,''                                                                    AS ACCSUBJECT2             -- ��Ŀ��
                ,''                                                                    AS ACCSUBJECT3             -- ��Ŀ��
                ,T1.ACQUIREDATE                                                    		 AS STARTDATE           		--��ʼ����
                ,T1.DATANO                                 	                       		 AS DUEDATE             		--��������
                ,CASE WHEN (TO_DATE(T1.DATANO,'YYYYMMDD')-TO_DATE(REPLACE(T1.ACQUIREDATE,'/',''),'YYYYMMDD'))/365<0
                      THEN 0
                      ELSE (TO_DATE(T1.DATANO,'YYYYMMDD')-TO_DATE(REPLACE(T1.ACQUIREDATE,'/',''),'YYYYMMDD'))/365
                 END                                                               AS OriginalMaturity    --ԭʼ����
                ,CASE WHEN (TO_DATE(T1.DATANO,'YYYYMMDD')-TO_DATE(REPLACE(T1.ACQUIREDATE,'/',''),'YYYYMMDD'))/365<0
                      THEN 0
                      ELSE (TO_DATE(T1.DATANO,'YYYYMMDD')-TO_DATE(REPLACE(T1.ACQUIREDATE,'/',''),'YYYYMMDD'))/365
                 END                                                                   AS ResidualM           --ʣ������
                ,CASE WHEN T1.CLASSIFYRESULT IN('B1','B2','B3','B') THEN '02'       --ʮ��������תΪ�弶����
                      WHEN T1.CLASSIFYRESULT IN('C1','C2','C') THEN '03'
                      WHEN T1.CLASSIFYRESULT IN('D1','D2','D') THEN '04'
                      WHEN T1.CLASSIFYRESULT='E' THEN '05'
                      ELSE '01'
                 END                                                                   AS RISKCLASSIFY            -- ���շ���
                ,'01'                                                                  AS EXPOSURESTATUS          -- ���ձ�¶״̬  01-����
                ,0                                                                     AS OVERDUEDAYS             -- ��������
                ,0											                                               AS SPECIALPROVISION        -- ר��׼����
                ,T1.SUBTRACTVALUEPREPARE                                               AS GENERALPROVISION        -- һ��׼����
                ,0                                                                     AS ESPECIALPROVISION       -- �ر�׼����
                ,0                                                                     AS WRITTENOFFAMOUNT        -- �Ѻ������
                ,''                                                                    AS OffExpoSource           -- ���Ⱪ¶��Դ
                ,''                                                                    AS OffBusinessType         -- ����ҵ������
                ,''                                                                    AS OffBusinessSdvsSTD      -- Ȩ�ط�����ҵ������ϸ��
                ,'0'                                                                   AS UncondCancelFlag        -- �Ƿ����ʱ����������
                ,''                                                                    AS CCFLevel                -- ����ת��ϵ������
                ,0                                                                     AS CCFAIRB                 -- �߼�������ת��ϵ��
                ,'01'                                                                  AS CLAIMSLEVEL             -- ծȨ����
                ,'0'                                                                   AS BONDFLAG                -- �Ƿ�Ϊծȯ
                ,'02'                                                                  AS BONDISSUEINTENT         -- ծȯ����Ŀ��
                ,\*CASE WHEN T4.TYPEDIVISION='1' AND T1.HELPONESELF='2' THEN '1'
                      ELSE '0'
                 END   *\ ---BY ����
                 1                                                               AS NSUREALPROPERTYFLAG     -- �Ƿ�����ò�����
                ,CASE WHEN (TO_DATE(T1.DATANO,'YYYYMMDD')-TO_DATE(REPLACE(T1.ACQUIREDATE,'/',''),'YYYYMMDD'))/365<=2
                      THEN '01'
                      ELSE '02'
                 END                                                                   AS REPASSETTERMTYPE        -- ��ծ�ʲ���������
                ,'0'                                                                   AS DEPENDONFPOBFLAG        -- �Ƿ�����������δ��ӯ��
                ,NULL                                                                  AS IRATING                 -- �ڲ�����
                ,NULL                                                                  AS PD                      -- ΥԼ����
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
                ,1                                                                     AS EXPONUMBER              -- ���ձ�¶����
                ,0.8                                                                   AS LTV                      --�����ֵ��  ͳһ����
                ,''                                                                    AS AGING                    --����
                ,''                                                                    AS NEWDEFAULTDEBTFLAG       --����ΥԼծ���ʶ
                ,''                                                                    AS PDPOOLMODELID           -- PD�ֳ�ģ��ID
                ,''                                                                    AS LGDPOOLMODELID          -- LGD�ֳ�ģ��ID
                ,''                                                                    AS CCFPOOLMODELID          -- CCF�ֳ�ģ��ID
                ,''                                                                    AS PDPOOLID                -- ����PD��ID
                ,''                                                                    AS LGDPOOLID               -- ����LGD��ID
                ,''                                                                    AS CCFPOOLID               -- ����CCF��ID
                ,'0'                                                                   AS ABSUAFLAG               -- �ʲ�֤ȯ�������ʲ���ʶ
                ,''                                                                    AS ABSPOOLID               -- ֤ȯ���ʲ���ID
                ,''                                                                    AS GROUPID                 -- ������
                ,''                                                                    AS DefaultDate             --ΥԼʱ��
		            ,NULL                                                                  AS ABSPROPORTION           --�ʲ�֤ȯ������
                ,NULL                                                                  AS DEBTORNUMBER            --����˸���
    FROM 				RWA_DEV.NCM_ASSET_DEBT_INFO T1
    LEFT JOIN 	RWA.ORG_INFO T2
    ON 					T1.MANAGEORGID=T2.ORGID
    LEFT JOIN   RWA_DEV.RWA_CD_CODE_MAPPING T3
    ON          T1.GUARANTYTYPEID=T3.SITEMNO
    AND         T3.SCODENO='GuarantySubjectNO'
    LEFT JOIN   RWA_DEV.NCM_COL_PARAM T4
    ON          T1.GUARANTYTYPEID=T4.GUARANTYTYPE
    AND         T4.DATANO=P_DATA_DT_STR
    WHERE  			T1.DATANO=P_DATA_DT_STR
    AND         T1.ENTRYVALUE <> 0
    ;
    COMMIT;*/

---�����ծ�ʲ���¼����
INSERT INTO RWA_DEV.RWA_DZ_EXPOSURE(
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
                TO_DATE(p_data_dt_str,'YYYYMMDD')                                                                   AS DATADATE                -- ��������
                ,p_data_dt_str                                                              AS DATANO                  -- ������ˮ��
                ,'DZ-' || T1.SUPPSERIALNO                                                AS EXPOSUREID              -- ���ձ�¶ID
                ,T1.SUPPSERIALNO                                                        AS DUEID                   -- ծ��ID
                ,'BL'                                                                 AS SSYSID                  -- ԴϵͳID   ��ծ�ʲ�
                ,'DZ-' || T1.SUPPSERIALNO                                               AS CONTRACTID              -- ��ͬID
                ,'XN-YBGS'                                                           AS CLIENTID                -- ��������ID
                 , '9998',                       --Դ����ID
                 '�������йɷ����޹�˾',              --Դ��������
                 '1',                        --�������������
                 '9998',                       --��������ID
                 '�������йɷ����޹�˾',              --������������
                 '9998',                       --�������ID
                 '�������йɷ����޹�˾'                 --�����������
               /* ,'01050000'                                                            AS SORGID                  -- Դ����ID
                ,'�����ʲ���ծ����'                                                  AS SORGNAME                -- Դ��������
                ,'1010050'                                                             AS ORGSORTNO               --�������������
                ,'01050000'                                                            AS ORGID                   -- ��������ID
                ,'�����ʲ���ծ����'                                                  AS ORGNAME                 -- ������������
                ,'01050000'                                                            AS ACCORGID                -- �������ID
                ,'�����ʲ���ծ����'                                                  AS ACCORGNAME              -- �����������*/
                ,'999999'                                                              AS INDUSTRYID              -- ������ҵ����
                ,'δ֪'                                                                AS INDUSTRYNAME            -- ������ҵ����
                ,'0501'                                                                  AS BUSINESSLINE            -- ����        
                ,'129'                                                                 AS ASSETTYPE               -- �ʲ�����
                ,'12901'                                                               AS ASSETSUBTYPE            -- �ʲ�С��
                ,CASE WHEN T1.DZLX='������' THEN '109040'
                      ELSE '109050'
                 END                                                                   AS BUSINESSTYPEID          --ҵ��Ʒ�ִ���
                ,CASE WHEN T1.DZLX='������' THEN '��ծ�ʲ���������'
                      ELSE '��ծ�ʲ��ǲ�������'
                 END                                                                   AS BUSINESSTYPENAME        -- ҵ��Ʒ������
                ,'01'                                                                  AS CREDITRISKDATATYPE      -- ���÷�����������          01-һ�������
                ,'01'                                                                  AS ASSETTYPEOFHAIRCUTS     -- �ۿ�ϵ����Ӧ�ʲ����     01-�ֽ��ֽ�ȼ���
                ,'10'                                                                  AS BUSINESSTYPESTD         -- Ȩ�ط�ҵ������
                ,''                                                                    AS EXPOCLASSSTD            -- Ȩ�ط���¶����
                ,''                                                                    AS EXPOSUBCLASSSTD         -- Ȩ�ط���¶С��
                ,''                                                                    AS EXPOCLASSIRB            -- ��������¶����
                ,''                                                                    AS EXPOSUBCLASSIRB         -- ��������¶С��
                ,'01'                                                                  AS EXPOBELONG              -- ��¶������ʶ
                ,'01'                                                                  AS BOOKTYPE                -- �˻����           01-�����˻�
                ,'03'                                                                  AS REGUTRANTYPE            -- ��ܽ�������      03-��Ѻ����
                ,'0'                                                                   AS REPOTRANFLAG            -- �ع����ױ�ʶ       0-��
                ,1                                                                     AS REVAFREQUENCY           -- �ع�Ƶ��
                ,'CNY'                                                                 AS CURRENCY                -- ����
                ,T1.YE                                                                 AS NORMALPRINCIPAL         -- �����������
                ,0                                                                     AS OVERDUEBALANCE          -- �������
                ,0                                                                     AS NONACCRUALBALANCE       -- ��Ӧ�����
                ,T1.YE                                                                 AS ONSHEETBALANCE          -- �������
                ,0                                                                     AS NORMALINTEREST          -- ������Ϣ
                ,0                                                                     AS ONDEBITINTEREST         -- ����ǷϢ
                ,0                                                                     AS OFFDEBITINTEREST        -- ����ǷϢ
                ,0                                                                     AS EXPENSERECEIVABLE       -- Ӧ�շ���
                ,T1.YE                                                                 AS ASSETBALANCE            -- �ʲ����
                ,'14410100'                                                            AS ACCSUBJECT1             -- ��Ŀһ
                ,''                                                                    AS ACCSUBJECT2             -- ��Ŀ��
                ,''                                                                    AS ACCSUBJECT3             -- ��Ŀ��
                ,p_data_dt_str                                                                    AS STARTDATE               --��ʼ����
                ,p_data_dt_str                                                                    AS DUEDATE                 --��������
                ,CASE WHEN T1.DZZCMC='һ������'
                      THEN '0.5'
                      WHEN T1.DZZCMC='һ�����ϵ�����'
                      THEN '1.5'
                      WHEN T1.DZZCMC='�������ϵ�����'
                      THEN '2.5'
                      WHEN T1.DZZCMC='��������'
                      THEN '3.5'
                 END                                                                   AS OriginalMaturity    --ԭʼ����
                ,'0'                                                                   AS ResidualM           --ʣ������
                ,'02'                                                                  AS RISKCLASSIFY            -- ���շ���
                ,'01'                                                                  AS EXPOSURESTATUS          -- ���ձ�¶״̬  01-����
                ,0                                                                     AS OVERDUEDAYS             -- ��������
                ,0                                                                     AS SPECIALPROVISION        -- ר��׼����
                ,T1.JZ                                                                 AS GENERALPROVISION        -- һ��׼����
                ,0                                                                     AS ESPECIALPROVISION       -- �ر�׼����
                ,0                                                                     AS WRITTENOFFAMOUNT        -- �Ѻ������
                ,''                                                                    AS OffExpoSource           -- ���Ⱪ¶��Դ
                ,''                                                                    AS OffBusinessType         -- ����ҵ������
                ,''                                                                    AS OffBusinessSdvsSTD      -- Ȩ�ط�����ҵ������ϸ��
                ,'0'                                                                   AS UncondCancelFlag        -- �Ƿ����ʱ����������
                ,''                                                                    AS CCFLevel                -- ����ת��ϵ������
                ,0                                                                     AS CCFAIRB                 -- �߼�������ת��ϵ��
                ,'01'                                                                  AS CLAIMSLEVEL             -- ծȨ����
                ,'0'                                                                   AS BONDFLAG                -- �Ƿ�Ϊծȯ
                ,'02'                                                                  AS BONDISSUEINTENT         -- ծȯ����Ŀ��
                ,CASE WHEN T1.DZLX='������'  THEN '1'
                      ELSE '0'
                 END   
                                                                     AS NSUREALPROPERTYFLAG     -- �Ƿ�����ò�����
                ,/*CASE WHEN (TO_DATE(T1.DATANO,'YYYYMMDD')-TO_DATE(REPLACE(T1.ACQUIREDATE,'/',''),'YYYYMMDD'))/365<=2
                      THEN '01'
                      ELSE '02'
                 END*/  
                 '01'                                                                 AS REPASSETTERMTYPE        -- ��ծ�ʲ���������
                ,'0'                                                                   AS DEPENDONFPOBFLAG        -- �Ƿ�����������δ��ӯ��
                ,NULL                                                                  AS IRATING                 -- �ڲ�����
                ,NULL                                                                  AS PD                      -- ΥԼ����
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
                ,1                                                                     AS EXPONUMBER              -- ���ձ�¶����
                ,0.8                                                                   AS LTV                      --�����ֵ��  ͳһ����
                ,''                                                                    AS AGING                    --����
                ,''                                                                    AS NEWDEFAULTDEBTFLAG       --����ΥԼծ���ʶ
                ,''                                                                    AS PDPOOLMODELID           -- PD�ֳ�ģ��ID
                ,''                                                                    AS LGDPOOLMODELID          -- LGD�ֳ�ģ��ID
                ,''                                                                    AS CCFPOOLMODELID          -- CCF�ֳ�ģ��ID
                ,''                                                                    AS PDPOOLID                -- ����PD��ID
                ,''                                                                    AS LGDPOOLID               -- ����LGD��ID
                ,''                                                                    AS CCFPOOLID               -- ����CCF��ID
                ,'0'                                                                   AS ABSUAFLAG               -- �ʲ�֤ȯ�������ʲ���ʶ
                ,''                                                                    AS ABSPOOLID               -- ֤ȯ���ʲ���ID
                ,''                                                                    AS GROUPID                 -- ������
                ,''                                                                    AS DefaultDate             --ΥԼʱ��
                ,NULL                                                                  AS ABSPROPORTION           --�ʲ�֤ȯ������
                ,NULL                                                                  AS DEBTORNUMBER            --����˸���
    FROM  RWA.RWA_WS_DZH_BL T1
    WHERE T1.DATADATE= TO_DATE(p_data_dt_str,'YYYYMMDD')            
    ;
    
    COMMIT;

---������׷��Ȩ�������ʲ���������Ʊ�ͷ�����Ʊ�� 
INSERT INTO RWA_DEV.RWA_DZ_EXPOSURE(
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
   TO_DATE(p_data_dt_str,'YYYYMMDD'),  --��������
    p_data_dt_str,                    --������ˮ��
   'ZHS'||T1.SUPPSERIALNO,            --���ձ�¶ID
    T1.SUPPSERIALNO,                         --ծ��ID
   'BL',                             --ԴϵͳID
   'ZHS'||T1.SUPPSERIALNO,             --��ͬID
   CASE WHEN T1.ZCMC='�����ϵ����гжһ�Ʊ' THEN   'XN-ZGSYYH'
   ELSE
   'XN-YBGS'
    END,                             --��������ID
   '9998',                       --Դ����ID
   '�������йɷ����޹�˾',              --Դ��������
   '1',                        --�������������
   '9998',                       --��������ID
   '�������йɷ����޹�˾',              --������������
   '9998',                       --�������ID
   '�������йɷ����޹�˾',                  --�����������
   'J',                          --������ҵ����
   '����ҵ',                 --������ҵ����
   '0401',                           --ҵ������
   '213',                            --�ʲ�����
   '21301',                          --�ʲ�С��
   '1020301010',                             --ҵ��Ʒ�ִ���
   '��׷��Ȩ��������',                             --ҵ��Ʒ������
   '01',                             --���÷�����������
   '01',                             --�ۿ�ϵ����Ӧ�ʲ����
   '07',                             --Ȩ�ط�ҵ������
 CASE WHEN T1.ZCMC='�����ϵ����гжһ�Ʊ' THEN   '0104'
   ELSE
   '0106'
    END,                           --Ȩ�ط���¶����
  CASE WHEN T1.ZCMC='�����ϵ����гжһ�Ʊ' THEN   '010406'
   ELSE
   '010601'
    END,                         --Ȩ�ط���¶С��
   '0203',                           --��������¶����
   '020301',                         --��������¶С��
   '02',                             --��¶������ʶ
   '01',                             --�˻����
   '03',                             --��ܽ�������
   '0',                              --�ع����ױ�ʶ
   1,                                --�ع�Ƶ��
   'CNY',                             --����
   T1.YE,                       --�����������
   0.000000,                         --�������
   0.000000,                         --��Ӧ�����
   T1.YE,                       --�������
   0.000000,                         --������Ϣ
   0.000000,                         --����ǷϢ
   0.000000,                         --����ǷϢ
   0.000000,                         --Ӧ�շ���
   T1.YE,                          --�ʲ����
   '',                              --��Ŀһ
   null,                             --��Ŀ��
   null,                             --��Ŀ��
   p_data_dt_str,                     --��ʼ����
   p_data_dt_str,                     --��������
   0.495890,                         --ԭʼ����
   0.328767,                         --ʣ������
   '01',                             --���շ���
   '01',                             --���ձ�¶״̬
   0,                                --��������
   0.000000,                         --ר��׼����
   0.000000,                         --һ��׼����
   0.000000,                         --�ر�׼����
   0.000000,                         --�Ѻ������
   '03',                             --���Ⱪ¶��Դ
   '10',                             --����ҵ������
   '1002',                           --Ȩ�ط�����ҵ������ϸ��
   null,                             --�Ƿ����ʱ����������
   null,                             --����ת��ϵ������
   null,                             --�߼�������ת��ϵ��
   '01',                             --ծȨ����
   '0',                              --�Ƿ�Ϊծȯ
   '02',                             --ծȯ����Ŀ��
   '0',                              --�Ƿ�����ò�����
   null,                             --��ծ�ʲ���������
   '0',                              --�Ƿ�����������δ��ӯ��
   '0106',                           --�ڲ�����
   null,                             --ΥԼ����
   null,                             --ΥԼ��ʧ�ʼ���
   null,                             --�߼���ΥԼ��ʧ��
   null,                             --�߼�����Ч����
   null,                             --�߼���ΥԼ���ձ�¶
   '0',                             --ΥԼ��ʶ
   null,                             --��ΥԼ��¶Ԥ����ʧ����
   null,                             --��ΥԼ��¶ΥԼ��ʧ��
   null,                             --��Ȩ��¶��ʶ
   null,                             --��ȨͶ�ʶ�������
   null,                             --��ȨͶ���γ�ԭ��
   null,                             --רҵ�����ʶ
   null,                             --רҵ��������
   null,                             --��Ŀ���ʽ׶�
   null,                             --�������
   null,                             --������϶������Ƿ��Ϊ����
   null,                             --�Ƿ񲨶��Խϴ�
   null,                             --�Ƿ���������з��ձ�¶
   null,                             --�Ƿ����Ը�ģʽ
   null,                             --�ӳٽ�������
   null,                             --�м�֤ȯ��ʶ
   null,                             --֤ȯ������ID
   null,                             --������������
   null,                             --֤ȯ���еȼ�
   null,                             --֤ȯʣ������
   null,                             --֤ȯ�ع�Ƶ��
   null,                             --�Ƿ����뽻�׶�����ؽ���
   null,                             --���뽻�׶���ID
   null,                             --�Ƿ�ϸ����뽻�׶���
   null,                             --���н�ɫ
   null,                             --���㷽ʽ
   null,                             --�Ƿ������ύ�ʲ�
   null,                             --�����������
   '0',                              --֤ȯ���ʽ��ױ�ʶ
   null,                             --���������Э���ʶ
   null,                             --���������Э��ID
   null,                             --֤ȯ���ʽ�������
   null,                             --֤ȯ����Ȩ�Ƿ�ת��
   '0',                              --�����������߱�ʶ
   null,                             --��Ч�������Э���ʶ
   null,                             --��Ч�������Э��ID
   null,                             --����������������
   null,                             --��֤������ڼ�
   null,                             --���óɱ�
   null,                             --���óɱ�����
   null,                             --������
   null,                             --�ϸ�����ʲ���ʶ
   null,                             --�����ʲ��������Ƿ�����Լ
   null,                             --���ñ������Ƿ��Ʋ�
   null,                             --��δ֧������
   null,                             --���۱�¶��ʶ
   null,                             --����ծȨ����
   null,                             --ס����Ѻ��������
   null,                             --���ձ�¶����
   null,                             --�����ֵ��
   null,                             --����
   null,                             --����ΥԼծ���ʶ
   null,                             --PD�ֳ�ģ��ID
   null,                             --LGD�ֳ�ģ��ID
   null,                             --CCF�ֳ�ģ��ID
   null,                             --����PD��ID
   '0',                              --����LGD��ID
   null,                             --����CCF��ID
   null,                             --�ʲ�֤ȯ�������ʲ���ʶ
   '',                   --֤ȯ���ʲ���ID
   null,                             --������
   null,                             --ΥԼʱ��
   null,                             --�ʲ�֤ȯ������
   null                              --����˸���
   FROM RWA.RWA_WS_ZHS_BL T1
   WHERE T1.DATADATE=TO_DATE(p_data_dt_str,'YYYYMMDD') ;        

    COMMIT;
    
    
---������������ʱ�����Ĵ����ŵ
INSERT INTO RWA_DEV.RWA_DZ_EXPOSURE(
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
   TO_DATE(p_data_dt_str,'YYYYMMDD'),  --��������
    p_data_dt_str,                    --������ˮ��
   'DKCN'||T1.SUPPSERIALNO,            --���ձ�¶ID
    T1.SUPPSERIALNO,                         --ծ��ID
   'BL',                             --ԴϵͳID
   'DKCN'||T1.SUPPSERIALNO,             --��ͬID
   'XN-YBGS',                       --��������ID
   '9998',                       --Դ����ID
   '�������йɷ����޹�˾',              --Դ��������
   '1',                        --�������������
   '9998',                       --��������ID
   '�������йɷ����޹�˾',              --������������
   '9998',                       --�������ID
   '�������йɷ����޹�˾',                  --�����������
   'J',                          --������ҵ����
   '����ҵ',                 --������ҵ����
   '0501',                           --ҵ������
   '215',                            --�ʲ�����
   '21501',                          --�ʲ�С��
   '102040',                             --ҵ��Ʒ�ִ���
   '�����ŵ',                             --ҵ��Ʒ������
   '01',                             --���÷�����������
   '01',                             --�ۿ�ϵ����Ӧ�ʲ����
   '07',                             --Ȩ�ط�ҵ������
   '0106',                           --Ȩ�ط���¶����
   '010601',                         --Ȩ�ط���¶С��
   '0203',                           --��������¶����
   '020301',                         --��������¶С��
   '02',                             --��¶������ʶ
   '01',                             --�˻����
   '03',                             --��ܽ�������
   '0',                              --�ع����ױ�ʶ
   1,                                --�ع�Ƶ��
   'CNY',                             --����
   T1.YE,                       --�����������
   0.000000,                         --�������
   0.000000,                         --��Ӧ�����
   T1.YE,                       --�������
   0.000000,                         --������Ϣ
   0.000000,                         --����ǷϢ
   0.000000,                         --����ǷϢ
   0.000000,                         --Ӧ�շ���
   T1.YE,                          --�ʲ����
   '',                              --��Ŀһ
   null,                             --��Ŀ��
   null,                             --��Ŀ��
  p_data_dt_str,                             --��ʼ����
   p_data_dt_str,                             --��������
   0.495890,                         --ԭʼ����
   0.328767,                         --ʣ������
   '01',                             --���շ���
   '01',                             --���ձ�¶״̬
   0,                                --��������
   0.000000,                         --ר��׼����
   0.000000,                         --һ��׼����
   0.000000,                         --�ر�׼����
   0.000000,                         --�Ѻ������
   '03',                             --���Ⱪ¶��Դ
   '02',                             --����ҵ������
   '0201',                           --Ȩ�ط�����ҵ������ϸ��
   '1',                             --�Ƿ����ʱ����������
   null,                             --����ת��ϵ������
   null,                             --�߼�������ת��ϵ��
   '01',                             --ծȨ����
   '0',                              --�Ƿ�Ϊծȯ
   '02',                             --ծȯ����Ŀ��
   '0',                              --�Ƿ�����ò�����
   null,                             --��ծ�ʲ���������
   '0',                              --�Ƿ�����������δ��ӯ��
   '0106',                           --�ڲ�����
   null,                             --ΥԼ����
   null,                             --ΥԼ��ʧ�ʼ���
   null,                             --�߼���ΥԼ��ʧ��
   null,                             --�߼�����Ч����
   null,                             --�߼���ΥԼ���ձ�¶
   '0',                             --ΥԼ��ʶ
   null,                             --��ΥԼ��¶Ԥ����ʧ����
   null,                             --��ΥԼ��¶ΥԼ��ʧ��
   null,                             --��Ȩ��¶��ʶ
   null,                             --��ȨͶ�ʶ�������
   null,                             --��ȨͶ���γ�ԭ��
   null,                             --רҵ�����ʶ
   null,                             --רҵ��������
   null,                             --��Ŀ���ʽ׶�
   null,                             --�������
   null,                             --������϶������Ƿ��Ϊ����
   null,                             --�Ƿ񲨶��Խϴ�
   null,                             --�Ƿ���������з��ձ�¶
   null,                             --�Ƿ����Ը�ģʽ
   null,                             --�ӳٽ�������
   null,                             --�м�֤ȯ��ʶ
   null,                             --֤ȯ������ID
   null,                             --������������
   null,                             --֤ȯ���еȼ�
   null,                             --֤ȯʣ������
   null,                             --֤ȯ�ع�Ƶ��
   null,                             --�Ƿ����뽻�׶�����ؽ���
   null,                             --���뽻�׶���ID
   null,                             --�Ƿ�ϸ����뽻�׶���
   null,                             --���н�ɫ
   null,                             --���㷽ʽ
   null,                             --�Ƿ������ύ�ʲ�
   null,                             --�����������
   '0',                              --֤ȯ���ʽ��ױ�ʶ
   null,                             --���������Э���ʶ
   null,                             --���������Э��ID
   null,                             --֤ȯ���ʽ�������
   null,                             --֤ȯ����Ȩ�Ƿ�ת��
   '0',                              --�����������߱�ʶ
   null,                             --��Ч�������Э���ʶ
   null,                             --��Ч�������Э��ID
   null,                             --����������������
   null,                             --��֤������ڼ�
   null,                             --���óɱ�
   null,                             --���óɱ�����
   null,                             --������
   null,                             --�ϸ�����ʲ���ʶ
   null,                             --�����ʲ��������Ƿ�����Լ
   null,                             --���ñ������Ƿ��Ʋ�
   null,                             --��δ֧������
   null,                             --���۱�¶��ʶ
   null,                             --����ծȨ����
   null,                             --ס����Ѻ��������
   null,                             --���ձ�¶����
   null,                             --�����ֵ��
   null,                             --����
   null,                             --����ΥԼծ���ʶ
   null,                             --PD�ֳ�ģ��ID
   null,                             --LGD�ֳ�ģ��ID
   null,                             --CCF�ֳ�ģ��ID
   null,                             --����PD��ID
   '0',                              --����LGD��ID
   null,                             --����CCF��ID
   null,                             --�ʲ�֤ȯ�������ʲ���ʶ
   '',                         --֤ȯ���ʲ���ID
   null,                             --������
   null,                             --ΥԼʱ��
   null,                             --�ʲ�֤ȯ������
   null                              --����˸���
   FROM RWA.RWA_WS_DKCN_BL T1
   WHERE T1.DATADATE=TO_DATE(p_data_dt_str,'YYYYMMDD') ;        

    COMMIT;
    
    /*�����ծ�ͱ���׷��Ȩ��ͬ*/
       INSERT INTO RWA_DEV.RWA_DZ_CONTRACT(
               DATADATE                              --��������
              ,DATANO                                --������ˮ��
              ,CONTRACTID                            --��ͬID
              ,SCONTRACTID                           --Դ��ͬID
              ,SSYSID                                --ԴϵͳID
              ,CLIENTID                              --��������ID
              ,SORGID                                --Դ����ID
              ,SORGNAME                              --Դ��������
              ,ORGSORTNO                             --�������������
              ,ORGID                                 --��������ID
              ,ORGNAME                               --������������
              ,INDUSTRYID                            --������ҵ����
              ,INDUSTRYNAME                          --������ҵ����
              ,BUSINESSLINE                          --����
              ,ASSETTYPE                             --�ʲ�����
              ,ASSETSUBTYPE                          --�ʲ�С��
              ,BUSINESSTYPEID                        --ҵ��Ʒ�ִ���
              ,BUSINESSTYPENAME                      --ҵ��Ʒ������
              ,CREDITRISKDATATYPE                    --���÷�����������
              ,STARTDATE                             --��ʼ����
              ,DUEDATE                               --��������
              ,ORIGINALMATURITY                      --ԭʼ����
              ,RESIDUALM                             --ʣ������
              ,SETTLEMENTCURRENCY                    --�������
              ,CONTRACTAMOUNT                        --��ͬ�ܽ��
              ,NOTEXTRACTPART                        --��ͬδ��ȡ����
              ,UNCONDCANCELFLAG                      --�Ƿ����ʱ����������
              ,ABSUAFLAG                             --�ʲ�֤ȯ�������ʲ���ʶ
              ,ABSPOOLID                             --֤ȯ���ʲ���ID
              ,GROUPID                               --������
              ,GUARANTEETYPE                         --��Ҫ������ʽ
              ,ABSPROPORTION                         --�ʲ�֤ȯ������
    )
     SELECT
                 TO_DATE(T1.DATANO,'YYYYMMDD')                                     AS DATADATE            --��������
                ,T1.DATANO                                                         AS DATANO              --������ˮ��
                ,T1.CONTRACTID                                         AS CONTRACTID          --��ͬID
                ,T1.CONTRACTID                                                    AS SCONTRACTID         --Դ��ͬID
                ,T1.SSYSID                                                             AS SSYSID              --ԴϵͳID
                ,T1.CLIENTID                                            AS CLIENTID            --��������ID
                ,T1.SORGID                                                       AS SORGID              --Դ����ID
                ,T1.SORGNAME                                              AS SORGNAME            --Դ��������
                ,T1.ORGSORTNO                                                         AS ORGSORTNO           --�������������
                ,T1.ORGID                                                        AS ORGID               --��������ID
                ,T1.ORGNAME                                             AS ORGNAME             --������������
                ,T1.INDUSTRYID                                                          AS INDUSTRYID          --������ҵ����
                ,T1.INDUSTRYNAME                                                         AS INDUSTRYNAME        --������ҵ����
                ,T1.BUSINESSLINE                                                               AS BUSINESSLINE        --����
                ,T1.ASSETTYPE                                                             AS ASSETTYPE           --�ʲ�����
                ,T1.ASSETSUBTYPE                                                         AS ASSETSUBTYPE        --�ʲ�С��
                ,T1.BUSINESSTYPEID                                                              AS BUSINESSTYPEID      --ҵ��Ʒ�ִ���
                ,T1.BUSINESSTYPENAME                                                              AS BUSINESSTYPENAME    --ҵ��Ʒ������
                ,'01'                                                              AS CREDITRISKDATATYPE  --���÷�����������
                ,T1.STARTDATE                                                    AS STARTDATE           --��ʼ����
                ,T1.DUEDATE                                                         AS DUEDATE             --��������
                ,T1.ORIGINALMATURITY                                                              AS OriginalMaturity    --ԭʼ����
                ,T1.RESIDUALM                                                              AS ResidualM           --ʣ������
                ,'CNY'                                                             AS SETTLEMENTCURRENCY  --�������
                ,T1.NORMALPRINCIPAL                                                     AS CONTRACTAMOUNT      --��ͬ�ܽ��
                ,0                                                                 AS NOTEXTRACTPART      --��ͬδ��ȡ����
                ,T1.UNCONDCANCELFLAG                                                              AS UNCONDCANCELFLAG    --�Ƿ����ʱ����������    0:��1����
                ,'0'                                                               AS ABSUAFLAG           --�ʲ�֤ȯ�������ʲ���ʶ
                ,NULL                                                              AS ABSPOOLID           --֤ȯ���ʲ���ID
                ,''                                                                AS GROUPID             --������
                ,''                                                                AS GUARANTEETYPE       --��Ҫ������ʽ
                ,NULL                                                              AS ABSPROPORTION       --�ʲ�֤ȯ������
    FROM        RWA_DZ_EXPOSURE T1
    WHERE       T1.DATANO=P_DATA_DT_STR
    ;
    COMMIT; 
    
    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_DZ_EXPOSURE',cascade => true);

    /*Ŀ�������ͳ��*/
    --ͳ�Ʋ���ļ�¼
    SELECT COUNT(1) INTO v_count1 FROM RWA_DEV.RWA_DZ_EXPOSURE;

    p_po_rtncode := '1';
    p_po_rtnmsg  := '�ɹ�'||'-'||v_count1;
    --�����쳣
    EXCEPTION
    WHEN OTHERS THEN
         --Dbms_output.Put_line('������,�������Ϊ:'||sqlcode||';������ϢΪ:'||sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace);
          ROLLBACK;
          p_po_rtncode := sqlcode;
          p_po_rtnmsg  := '��ծ�ʲ���¼��¶��(RWA_DEV.RWA_DZ_EXPOSURE)ETLת��ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_DZ_EXPOSURE;
/

