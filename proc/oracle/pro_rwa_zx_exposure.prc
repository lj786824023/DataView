CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_ZX_EXPOSURE(
                            p_data_dt_str IN  VARCHAR2,   --��������
                            p_po_rtncode  OUT VARCHAR2,   --���ر��
                            p_po_rtnmsg   OUT VARCHAR2    --��������
)
  /*
    �洢��������:RWA_DEV.PRO_RWA_ZX_EXPOSURE
    ʵ�ֹ���:����ϵͳ-ֱ�����е��-���÷��ձ�¶
    ���ݿھ�:ȫ��
    ����Ƶ��:��ĩ����
    ��  ��  :V1.0.0
    ��д��  :YUSJ
    ��дʱ��:2016-10-18
    ��  λ  :�Ϻ���˶��Ϣ�����ɷ����޹�˾
    Դ  ��1 :RWA_DEV.CBS_IAC|ͨ�÷ֻ���
    Դ  ��2 :RWA.ORG_INFO|������
    Դ  ��3 :RWA.RWA_WS_DSBANK_ADV|ֱ�����е�¼��
    Դ  ��4 :RWA.RWA_WP_DATASUPPLEMENT|��¼������ϸ��Ϣ��
    Ŀ���1 :RWA_DEV.RWA_ZX_EXPOSURE|ֱ�����б�¶��
    ������ :��
    �����¼(�޸���|�޸�ʱ��|�޸�����):
    */
  AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_ZX_EXPOSURE';
  --�����쳣����
  v_raise EXCEPTION;
  --���嵱ǰ����ļ�¼��
  v_count1 INTEGER;
  --v_count2 INTEGER;

  BEGIN
    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*�����ȫ�����ݼ��������Ŀ���*/
    --1.���Ŀ����е�ԭ�м�¼
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_ZX_EXPOSURE';

    --2.���������������ݴ�Դ����뵽Ŀ�����
    /*���� ��¶��ֱ�����е��ҵ�� ��Ŀ���*/
    INSERT INTO RWA_DEV.RWA_ZX_EXPOSURE(
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
                ,T1.IACAC_NO                                                           AS EXPOSUREID              -- ���ձ�¶ID
                ,T1.IACAC_NO                                                           AS DUEID                   -- ծ��ID
                ,'ZX'                                                                  AS SSYSID                  -- ԴϵͳID
                ,T1.IACAC_NO                                                           AS CONTRACTID              -- ��ͬID
                ,T2.CUSTID1                                                            AS CLIENTID                -- ��������ID   ȡ����������������ID
                ,T1.IACGACBR                                                           AS SORGID                  -- Դ����ID
                ,T3.ORGNAME                                                            AS SORGNAME                -- Դ��������
                ,T3.SORTNO                                                             AS ORGSORTNO               -- �������������
                ,T1.IACGACBR                                                           AS ORGID                   -- ��������ID
                ,T3.ORGNAME                                                            AS ORGNAME                 -- ������������
                ,T1.IACGACBR                                                           AS ACCORGID                -- �������ID
                ,T3.ORGNAME                                                            AS ACCORGNAME              -- �����������
                ,T2.INDUSTRYID                                                         AS INDUSTRYID              -- ������ҵ����
                ,T4.ITEMNAME                                                           AS INDUSTRYNAME            -- ������ҵ����
                ,'0101'                                                                AS BUSINESSLINE            -- ����                      Ĭ�� 01-����
                ,''                                                                    AS ASSETTYPE               -- �ʲ�����                  Ĭ�ϡ����ͬҵ��11601  ��Ϊ��¶�������
                ,''                                                                    AS ASSETSUBTYPE            -- �ʲ�С��                  Ĭ�ϡ����ͬҵ��11601  ��Ϊ��¶�������
                ,'109010'                                                              AS BUSINESSTYPEID          -- ҵ��Ʒ�ִ���              Ĭ�ϡ����ͬҵ��11601
                ,'ֱ�����е��'                                                         AS BUSINESSTYPENAME        -- ҵ��Ʒ������              Ĭ�ϡ����ͬҵ��11601
                ,'01'                                                                  AS CREDITRISKDATATYPE      -- ���÷�����������          Ĭ��һ������� 01
                ,'01'                                                                  AS ASSETTYPEOFHAIRCUTS     -- �ۿ�ϵ����Ӧ�ʲ����      Ĭ�ϡ��ֽ��ֽ�ȼ��
                ,''                                                                    AS BUSINESSTYPESTD         -- Ȩ�ط�ҵ������            Ĭ�ϡ�һ���ʲ���07
                ,''                                                                    AS EXPOCLASSSTD            -- Ȩ�ط���¶����            RWA ����Ϊ��
                ,''                                                                    AS EXPOSUBCLASSSTD         -- Ȩ�ط���¶С��            RWA ����Ϊ��
                ,''                                                                    AS EXPOCLASSIRB            -- ��������¶����            RWA ����Ϊ��
                ,''                                                                    AS EXPOSUBCLASSIRB         -- ��������¶С��            RWA ����Ϊ��
                ,'01'                                                                  AS EXPOBELONG              -- ��¶������ʶ              Ĭ��01 ����
                ,'01'                                                                  AS BOOKTYPE                -- �˻����                  01-�����˻�
                ,'03'                                                                  AS REGUTRANTYPE            -- ��ܽ�������              02-�����ʱ��г�����
                ,'0'                                                                   AS REPOTRANFLAG            -- �ع����ױ�ʶ              0-��
                ,1                                                                     AS REVAFREQUENCY           -- �ع�Ƶ��                  Ĭ��Ϊ1
                ,'CNY'                                                                 AS CURRENCY                -- ����
                ,ABS(T1.IACCURBAL)                                                     AS NORMALPRINCIPAL         -- �����������              ��ȡ����ֵ
                ,0                                                                     AS OVERDUEBALANCE          -- �������                  Ĭ��Ϊ0
                ,0                                                                     AS NONACCRUALBALANCE       -- ��Ӧ�����                Ĭ��Ϊ0
                ,ABS(T1.IACCURBAL)+0+0                                                 AS ONSHEETBALANCE          -- �������                  �������=�����������+�������+��Ӧ����
                ,0                                                                     AS NORMALINTEREST          -- ������Ϣ
                ,0                                                                     AS ONDEBITINTEREST         -- ����ǷϢ                  Ĭ��Ϊ0
                ,0                                                                     AS OFFDEBITINTEREST        -- ����ǷϢ                  Ĭ��Ϊ0
                ,0                                                                     AS EXPENSERECEIVABLE       -- Ӧ�շ���                  Ĭ��Ϊ0
                ,nvl(ABS(T1.IACCURBAL),0)                                              AS ASSETBALANCE            -- �ʲ����
                ,T1.IACITMNO                                                           AS ACCSUBJECT1             -- ��Ŀһ
                ,''                                                                    AS ACCSUBJECT2             -- ��Ŀ��
                ,''                                                                    AS ACCSUBJECT3             -- ��Ŀ��
                ,NVL(T2.IACCRTDAT,P_DATA_DT_STR)                                       AS STARTDATE           -- ��ʼ����
                ,NVL(T2.IACDLTDAT,TO_CHAR(TO_DATE(P_DATA_DT_STR,'YYYYMMDD')+30,'YYYYMMDD'))           AS DUEDATE             -- ��������
                ,NVL((TO_DATE(T2.IACDLTDAT,'YYYYMMDD')-TO_DATE(T2.IACCRTDAT,'YYYYMMDD'))/365,30/365)  AS ORIGINALMATURITY    --ԭʼ����
                ,NVL((TO_DATE(T2.IACDLTDAT,'YYYYMMDD')-TO_DATE(P_DATA_DT_STR,'YYYYMMDD'))/365,30/365) AS RESIDUALM           --ʣ������
                ,'01'                                                                  AS RISKCLASSIFY            -- ���շ���                  01-����
                ,'01'                                                                  AS EXPOSURESTATUS          -- ���ձ�¶״̬              01-����
                ,0                                                                     AS OVERDUEDAYS             -- ��������                  Ĭ��Ϊ0
                ,0                                                                     AS SPECIALPROVISION        -- ר��׼����                RWA����
                ,0                                                                     AS GENERALPROVISION        -- һ��׼����                RWA����
                ,0                                                                     AS ESPECIALPROVISION       -- �ر�׼����                RWA����
                ,NULL                                                                  AS WRITTENOFFAMOUNT        -- �Ѻ������                Ĭ��Ϊ��
                ,''                                                                    AS OffExpoSource           -- ���Ⱪ¶��Դ              Ĭ��Ϊ�� ���ö��01
                ,''                                                                    AS OffBusinessType         -- ����ҵ������              Ĭ��Ϊ�� ��ͬ�ڴ��������ҵ��01
                ,''                                                                    AS OffBusinessSdvsSTD      -- Ȩ�ط�����ҵ������ϸ��    Ĭ��Ϊ�� ���гжһ�Ʊ0101
                ,'0'                                                                   AS UncondCancelFlag        -- �Ƿ����ʱ����������      Ĭ��Ϊ�� Ĭ��Ϊ��0
                ,''                                                                    AS CCFLevel                -- ����ת��ϵ������          Ĭ��Ϊ�� �߼���ccf
                ,NULL                                                                  AS CCFAIRB                 -- �߼�������ת��ϵ��        Ĭ��Ϊ��
                ,'01'                                                                  AS CLAIMSLEVEL             -- ծȨ����                  RWA               Ĭ��01���߼�Ȩծ
                ,'0'                                                                   AS BONDFLAG                -- �Ƿ�Ϊծȯ                RWA               Ĭ��Ϊ�� 0
                ,'01'                                                                  AS BONDISSUEINTENT         -- ծȯ����Ŀ��              RWA               �չ��������в�������01
                ,'0'                                                                   AS NSUREALPROPERTYFLAG     -- �Ƿ�����ò�����          RWA               Ĭ��Ϊ�� 0
                ,'01'                                                                  AS REPASSETTERMTYPE        -- ��ծ�ʲ���������          RWA               ���ɹ涨����������01
                ,'0'                                                                   AS DEPENDONFPOBFLAG        -- �Ƿ�����������δ��ӯ��    Ĭ�Ϸ�0
                ,''                                                                    AS IRATING                 -- �ڲ�����                  ȡ������        Ĭ��Ϊ��
                ,NULL                                                                  AS PD                      -- ΥԼ����                  ȡ������        Ĭ��Ϊ0
                ,NULL                                                                  AS LGDLEVEL                -- ΥԼ��ʧ�ʼ���            Ĭ��ΪNULL
                ,NULL                                                                  AS LGDAIRB                 -- �߼���ΥԼ��ʧ��          NULL
                ,NULL                                                                  AS MAIRB                   -- �߼�����Ч����            NULL
                ,NULL                                                                  AS EADAIRB                 -- �߼���ΥԼ���ձ�¶        NULL
                ,'0'                                                                   AS DEFAULTFLAG             -- ΥԼ��ʶ                  Ĭ��Ϊ�� 0
                ,0.45                                                                  AS BEEL                    -- ��ΥԼ��¶Ԥ����ʧ����    Ĭ��Ϊ45%
                ,0.45                                                                  AS DEFAULTLGD              -- ��ΥԼ��¶ΥԼ��ʧ��      Ĭ��Ϊ45%
                ,'0'                                                                   AS EQUITYEXPOFLAG          -- ��Ȩ��¶��ʶ              Ĭ��Ϊ�� 0
                ,''                                                                    AS EQUITYINVESTTYPE        -- ��ȨͶ�ʶ�������          ��ҵ����0202
                ,''                                                                    AS EQUITYINVESTCAUSE       -- ��ȨͶ���γ�ԭ��          ��������01
                ,'0'                                                                   AS SLFLAG                  -- רҵ�����ʶ              Ĭ��Ϊ�� 0
                ,''                                                                    AS SLTYPE                  -- רҵ��������              ��Ŀ����02030301
                ,''                                                                    AS PFPHASE                 -- ��Ŀ���ʽ׶�              ������01
                ,''                                                                    AS REGURATING              -- �������                  ��01
                ,'0'                                                                   AS CBRCMPRATINGFLAG        -- ������϶������Ƿ��Ϊ����  Ĭ��Ϊ�� 0
                ,'0'                                                                   AS LARGEFLUCFLAG           -- �Ƿ񲨶��Խϴ�              Ĭ��Ϊ�� 0
                ,'0'                                                                   AS LIQUEXPOFLAG            -- �Ƿ���������з��ձ�¶      Ĭ��Ϊ�� 0
                ,'0'                                                                   AS PAYMENTDEALFLAG         -- �Ƿ����Ը�ģʽ            Ĭ��Ϊ�� 0
                ,0                                                                     AS DELAYTRADINGDAYS        -- �ӳٽ�������                Ĭ��0
                ,'0'                                                                   AS SECURITIESFLAG          -- �м�֤ȯ��ʶ                02
                ,''                                                                    AS SECUISSUERID            -- ֤ȯ������ID
                ,''                                                                    AS RATINGDURATIONTYPE      -- ������������                ������������01
                ,''                                                                    AS SECUISSUERATING         -- ֤ȯ���еȼ�                AAA
                ,NULL                                                                  AS SECURESIDUALM           -- ֤ȯʣ������                Ĭ�Ͽ�
                ,1                                                                     AS SECUREVAFREQUENCY       -- ֤ȯ�ع�Ƶ��                Ĭ��0
                ,'0'                                                                   AS CCPTRANFLAG             -- �Ƿ����뽻�׶�����ؽ���    Ĭ��Ϊ�� 0
                ,''                                                                    AS CCPID                   -- ���뽻�׶���ID
                ,'0'                                                                   AS QUALCCPFLAG             -- �Ƿ�ϸ����뽻�׶���       Ĭ��Ϊ�� 0
                ,''                                                                    AS BANKROLE                -- ���н�ɫ                   �ͻ�-02
                ,''                                                                    AS CLEARINGMETHOD          -- ���㷽ʽ                   Ϊ�ͻ�-02
                ,'0'                                                                   AS BANKASSETFLAG           -- �Ƿ������ύ�ʲ�           Ĭ��Ϊ��-0
                ,''                                                                    AS MATCHCONDITIONS         -- �����������               ��ȫ��������01
                ,'0'                                                                   AS SFTFLAG                 -- ֤ȯ���ʽ��ױ�ʶ           Ĭ��Ϊ��-0
                ,'0'                                                                  AS MASTERNETAGREEFLAG      -- ���������Э���ʶ         ����01
                ,NULL                                                                  AS MASTERNETAGREEID        -- ���������Э��ID           ��Ϊ�գ������û��
                ,''                                                                    AS SFTTYPE                 -- ֤ȯ���ʽ�������           ���ع�01
                ,'0'                                                                   AS SECUOWNERTRANSFLAG      -- ֤ȯ����Ȩ�Ƿ�ת��         Ĭ��Ϊ��-0
                ,'0'                                                                   AS OTCFLAG                 -- �����������߱�ʶ           05
                ,'0'                                                                   AS VALIDNETTINGFLAG        -- ��Ч�������Э���ʶ       Ĭ��Ϊ��-0
                ,''                                                                    AS VALIDNETAGREEMENTID     -- ��Ч�������Э��ID         ��Ϊ�գ������û��
                ,''                                                                    AS OTCTYPE                 -- ����������������           ����01
                ,0                                                                     AS DEPOSITRISKPERIOD       -- ��֤������ڼ�             Ĭ��0
                ,0                                                                     AS MTM                     -- ���óɱ�                   Ĭ��0
                ,''                                                                    AS MTMCURRENCY             -- ���óɱ�����               01
                ,''                                                                    AS BUYERORSELLER           -- ������                   ���ñ�����01
                ,'0'                                                                   AS QUALROFLAG              -- �ϸ�����ʲ���ʶ           Ĭ�Ϸ�0
                ,'0'                                                                   AS ROISSUERPERFORMFLAG     -- �����ʲ��������Ƿ�����Լ   Ĭ�Ϸ�0
                ,'0'                                                                   AS BUYERINSOLVENCYFLAG     -- ���ñ������Ƿ��Ʋ�       Ĭ�Ϸ�0
                ,0                                                                     AS NONPAYMENTFEES          -- ��δ֧������               Ĭ��0
                ,'0'                                                                   AS RETAILEXPOFLAG          -- ���۱�¶��ʶ               Ĭ�Ϸ�0
                ,''                                                                    AS RETAILCLAIMTYPE         -- ����ծȨ����               ����ס����Ѻ����020401
                ,''                                                                    AS MORTGAGETYPE            -- ס����Ѻ��������           ����ס����Ѻ׷�Ӵ���01
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
                ,'0'                                                                   AS ABSUAFLAG               -- �ʲ�֤ȯ�������ʲ���ʶ     Ĭ�Ϸ�0
                ,''                                                                    AS ABSPOOLID               -- ֤ȯ���ʲ���ID             �ʲ�֤ȯ��07
                ,''                                                                    AS GROUPID                 -- ������
                ,''                                                                    AS DefaultDate             --ΥԼʱ��
                ,NULL                                                                  AS ABSPROPORTION           --�ʲ�֤ȯ������
                ,NULL                                                                  AS DEBTORNUMBER            --����˸���
    FROM        RWA_DEV.CBS_IAC T1                                        --ͨ�÷ֻ���
    LEFT JOIN   (SELECT WDA.IACAC_NO
                       ,WDA.CUSTID1
                       ,WDA.INDUSTRYID
                       ,TO_CHAR(TO_DATE(WDA.IACCRTDAT,'YYYY-MM-DD'),'YYYYMMDD') AS IACCRTDAT
                       ,TO_CHAR(TO_DATE(WDA.IACDLTDAT,'YYYY-MM-DD'),'YYYYMMDD') AS IACDLTDAT
                   FROM RWA.RWA_WS_DSBANK_ADV WDA                         --ֱ�����е�����ݲ�¼��
             INNER JOIN RWA.RWA_WP_DATASUPPLEMENT T6                      --���ݲ�¼��
                     ON WDA.SUPPORGID = T6.ORGID
                    AND T6.DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
                    AND T6.SUPPTMPLID = 'M-0190'
                    AND T6.SUBMITFLAG = '1'
                  WHERE WDA.DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
                ) T2
    ON          T1.IACAC_NO = T2.IACAC_NO
    LEFT JOIN   RWA.ORG_INFO T3
    ON          T1.IACGACBR = T3.ORGID
    LEFT JOIN   RWA.CODE_LIBRARY  T4                                      --�����ȡ��ҵ
    ON          T2.INDUSTRYID = T4.ITEMNO
    AND         T4.CODENO = 'IndustryType'
    WHERE       T1.IACITMNO='13070800'                                    --ֱ�����е��
    AND         T1.IACCURBAL <> 0                                         --�˻�������0
    AND         T1.DATANO = p_data_dt_str
    ;
    COMMIT;

    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_ZX_EXPOSURE',cascade => true);

    /*Ŀ�������ͳ��*/
    --ͳ�Ʋ���ļ�¼
    SELECT COUNT(1) INTO v_count1 FROM RWA_DEV.RWA_ZX_EXPOSURE;


    p_po_rtncode := '1';
    p_po_rtnmsg  := '�ɹ�' || '-' || v_count1;
    --�����쳣
    EXCEPTION
    WHEN OTHERS THEN
         --Dbms_output.Put_line('������,�������Ϊ:'||sqlcode||';������ϢΪ:'||sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace);
         ROLLBACK;
         p_po_rtncode := sqlcode;
         p_po_rtnmsg  := '���÷��ձ�¶('|| v_pro_name ||')ETLת��ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_ZX_EXPOSURE;
/

