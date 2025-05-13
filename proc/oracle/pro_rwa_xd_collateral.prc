CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_XD_COLLATERAL(
                             P_DATA_DT_STR  IN  VARCHAR2,    --��������
                             P_PO_RTNCODE  OUT  VARCHAR2,    --���ر��
                            P_PO_RTNMSG    OUT  VARCHAR2    --��������
)
  /*
    �洢��������:RWA_DEV.PRO_RWA_XD_COLLATERAL
    ʵ�ֹ���:��Ϣ����ϵͳ-����Ѻ,��ṹΪ����ѺƷ��
    ���ݿھ�:ȫ��
    ����Ƶ��:�³�
    ��  ��  :V1.0.0
    ��д��  :YUSJ
    ��дʱ��:2014-04-26
    ��  λ  :�Ϻ���˶��Ϣ�����ɷ����޹�˾
    Դ  ��1  :NCM_GUARANTY_INFO|��������Ϣ��
    Դ  ��2  :NCM_BUSINESS_DUEBILL|����ҵ������Ϣ��
    Դ  ��3  :NCM_BUSINESS_CONTRACT|����ҵ���ͬ��
    Դ  ��4  :NCM_GUARANTY_CONTRACT|������ͬ��Ϣ��
    Դ  ��5  :NCM_CONTRACT_RELATIVE|��ͬ������
    Դ  ��6  :NCM_GUARANTY_RELATIVE|������ͬ�뵣���������
    Դ  ��7  :NCM_CUSTOMER_INFO|�ͻ�������Ϣ��
    Դ  ��8  :NCM_CODE_LIBRARY|�����
    Ŀ���  :RWA_XD_COLLATERAL|�Ŵ�ϵͳ-����ѺƷ��
    ������  :��
    �����¼(�޸���|�޸�ʱ��|�޸�����)��
  */
  AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  --v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_XD_COLLATERAL';
  --�����ж�ֵ����
  v_count1 INTEGER;
  --�����쳣����
  v_raise EXCEPTION;
  --������ʱ����
  --v_tabname VARCHAR2(200);
  --���崴�����
  --v_create VARCHAR2(1000) ;

  BEGIN
    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*�����ȫ�����ݼ��������Ŀ���*/
    --1.���Ŀ����е�ԭ�м�¼
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_XD_COLLATERAL';

    /*1.1 ��Ч����º�ͬ��Ӧ�ĵ���ѺƷ��Ϣ(��ͨ)*/
    INSERT INTO RWA_DEV.RWA_XD_COLLATERAL(
                 DATADATE                                --��������
                ,DATANO                                 --������ˮ��
                ,COLLATERALID                           --����ѺƷID
                ,SSYSID                                 --ԴϵͳID
                ,SGUARCONTRACTID                        --Դ������ͬID
                ,SCOLLATERALID                          --Դ����ѺƷID
                ,COLLATERALNAME                         --����ѺƷ����
                ,ISSUERID                               --������ID
                ,PROVIDERID                             --�ṩ��ID
                ,CREDITRISKDATATYPE                     --���÷�����������
                ,GUARANTEEWAY                            --������ʽ
                ,SOURCECOLTYPE                          --Դ����ѺƷ����
                ,SOURCECOLSUBTYPE                       --Դ����ѺƷС��
                ,SPECPURPBONDFLAG                       --�Ƿ�Ϊ�չ��������в�����������е�ծȯ
                ,QUALFLAGSTD                            --Ȩ�ط��ϸ��ʶ
                ,QUALFLAGFIRB                           --�����������ϸ��ʶ
                ,COLLATERALTYPESTD                      --Ȩ�ط�����ѺƷ����
                ,COLLATERALSDVSSTD                      --Ȩ�ط�����ѺƷϸ��
                ,COLLATERALTYPEIRB                      --����������ѺƷ����
                ,COLLATERALAMOUNT                        --��Ѻ�ܶ�
                ,CURRENCY                               --����
                ,STARTDATE                              --��ʼ����
                ,DUEDATE                                --��������
                ,ORIGINALMATURITY                       --ԭʼ����
                ,RESIDUALM                              --ʣ������
                ,INTEHAIRCUTSFLAG                       --���й����ۿ�ϵ����ʶ
                ,INTERNALHC                             --�ڲ��ۿ�ϵ��
                ,FCTYPE                                 --������ѺƷ����
                ,ABSFLAG                                --�ʲ�֤ȯ����ʶ
                ,RATINGDURATIONTYPE                     --������������
                ,FCISSUERATING                          --������ѺƷ���еȼ�
                ,FCISSUERTYPE                           --������ѺƷ���������
                ,FCISSUERSTATE                          --������ѺƷ������ע�����
                ,FCRESIDUALM                            --������ѺƷʣ������
                ,REVAFREQUENCY                          --�ع�Ƶ��
                ,GROUPID                                --������
                ,RCERating                              --�����˾���ע����ⲿ����
                ,flag
    )WITH TEMP_COLLATERAL1 AS(SELECT T3.SERIALNO AS CONTRACTNO
                                    ,MIN(CASE WHEN T1.BUSINESSTYPE IN ('10201010','10202010','1035101010','1035101020','1035102010','1035102020','103550','1040101040') --����֤,����
                                              THEN NVL(T3.PUTOUTDATE,T3.OCCURDATE)   --����֤������ȡ��ͬ��ʼ����
                                              ELSE T1.PUTOUTDATE 
                                         END) AS PUTOUTDATE
                                    ,MAX(CASE WHEN T1.BUSINESSTYPE IN ('10201010','10202010','1035101010','1035101020','1035102010','1035102020','103550','1040101040') --����֤,����
                                              THEN T3.MATURITY
                                              ELSE T1.ACTUALMATURITY
                                         END) AS MATURITY
                                    ,MIN(T4.CREDITRISKDATATYPE) AS CREDITRISKDATATYPE
                               FROM RWA_DEV.NCM_BUSINESS_DUEBILL T1
                               INNER JOIN RWA_DEV.NCM_BUSINESS_TYPE T2
                               ON T1.BUSINESSTYPE=T2.TYPENO
                               AND T1.DATANO=T2.DATANO
                               AND T2.SORTNO NOT LIKE '30%' --�ų������ҵ��
                               INNER JOIN RWA_DEV.NCM_BUSINESS_CONTRACT T3
                               ON T1.RELATIVESERIALNO2=T3.SERIALNO        --�����Խ��Ϊ׼�����Թ�����ͬʱ����Ӧ�üӺ�ͬ����Ч����
                               AND T1.DATANO=T3.DATANO
                               INNER JOIN RWA_DEV.NCM_BUSINESS_HISTORY T31
                               ON T1.SERIALNO=T31.SERIALNO
                               AND T31.DATANO=P_DATA_DT_STR
                               AND T31.BALANCE>0
                                          /*rwa_dev.brd_loan_nor t31
                               ON  t1.serialno = t31.crdt_acct_no
                               AND t31.datano = P_DATA_DT_STR
                               AND t31.cur_bal > 0*/
                               INNER JOIN RWA_DEV.RWA_XD_CONTRACT T4
    													 ON T1.RELATIVESERIALNO2 = T4.CONTRACTID
    													 AND T1.DATANO = T4.DATANO
                               WHERE T1.DATANO=P_DATA_DT_STR
                               --AND T1.BALANCE>0           --��Ч�ж�����
                               AND T1.SERIALNO<>'BD2014110400000001'
                               AND T1.BUSINESSTYPE NOT IN ('1040101010','1040101020','1040101030','1040102010','1040102020','1040102040','10303010'
                                   ,'1040105060','1040201010','1040201020','1040202010','105010','11105010','11105020','10302010','10302015','10302020')  --�ų�ͬҵ���ع���Ͷ�ʣ�ί�д���ҵ��,���ֺ�ת��Ҳ�ų�����Ʊ����Ϣ��Ϊ����
                               GROUP BY T3.SERIALNO
                             )
    ,TEMP_RELATIVE AS (SELECT T5.GUARANTYID,
                              MIN(T1.PUTOUTDATE) AS PUTOUTDATE,
                              MAX(T1.MATURITY) AS MATURITY,
                              MIN(T1.CREDITRISKDATATYPE) AS CREDITRISKDATATYPE
                       FROM TEMP_COLLATERAL1 T1
                       INNER JOIN
                           (SELECT SERIALNO,OBJECTNO
                            FROM RWA_DEV.NCM_CONTRACT_RELATIVE
                            WHERE DATANO=P_DATA_DT_STR
                            AND OBJECTTYPE = 'GuarantyContract'
                            GROUP BY  SERIALNO,OBJECTNO
                            ) T2
                      ON T1.CONTRACTNO=T2.SERIALNO
                      INNER JOIN RWA_DEV.NCM_GUARANTY_CONTRACT T3
                      ON T2.OBJECTNO = T3.SERIALNO
                      AND T3.DATANO=P_DATA_DT_STR
                      INNER JOIN (SELECT CONTRACTNO, GUARANTYID
                                  FROM RWA_DEV.NCM_GUARANTY_RELATIVE
                                  WHERE DATANO=P_DATA_DT_STR
                                  GROUP BY CONTRACTNO, GUARANTYID
                                  ) T4
                      ON T3.SERIALNO=T4.CONTRACTNO
                      INNER JOIN RWA_DEV.NCM_GUARANTY_INFO T5
                      ON T4.GUARANTYID=T5.GUARANTYID
                      AND T5.DATANO=P_DATA_DT_STR
                      GROUP BY T5.GUARANTYID
                   )
    SELECT
                 TO_DATE(T1.DATANO,'YYYYMMDD')                                                AS DATADATE              --��������
                ,T1.DATANO                                                                    AS DATANO                --������ˮ��
                ,'YP'||T1.GUARANTYID                                                          AS COLLATERALID          --����ѺƷID
                ,'XD'                                                                         AS SSYSID                --ԴϵͳID
                ,''                                                                           AS SGUARCONTRACTID       --Դ������ͬID
                ,T1.GUARANTYID                                                                AS SCOLLATERALID         --Դ����ѺƷID
                ,T6.ITEMNAME                                                                  AS COLLATERALNAME        --����ѺƷ����
                ,CASE WHEN (T1.GUARANTYTYPEID LIKE '001003%' OR T1.GUARANTYTYPEID LIKE '001004%' OR T1.GUARANTYTYPEID LIKE '001001%') --������ѺƷ��Ҫ������
                      THEN NVL(T3.OPENBANKNO,'�й���ҵ����')
                      ELSE ''
                 END                                                                           AS ISSUERID              --������ID
                ,T1.CLRERID                                                                    AS PROVIDERID            --�ṩ��ID
                ,T2.CREDITRISKDATATYPE                                                         AS CREDITRISKDATATYPE    --���÷�����������
                ,T1.GUARANTYTYPE                                                               AS GUARANTEEWAY          --������ʽ
                ,SUBSTR(T1.GUARANTYTYPEID,1,6)                                                 AS SOURCECOLTYPE         --Դ����ѺƷ����
                ,T1.GUARANTYTYPEID                                                             AS SOURCECOLSUBTYPE      --Դ����ѺƷС��
                ,CASE WHEN T3.BONDPUBLISHPURPOSE='0010' THEN '1'
                      ELSE '0'
                 END                                                                           AS SPECPURPBONDFLAG      --�Ƿ�Ϊ�չ��������в�����������е�ծȯ
                ,''                                                                            AS QUALFLAGSTD           --Ȩ�ط��ϸ��ʶ
                ,CASE WHEN T1.QUALIFYFLAG03='01' THEN '1'
                	    WHEN T1.QUALIFYFLAG03='02' THEN '0'
                      ELSE ''
                 END                                                                           AS QUALFLAGFIRB          --�����������ϸ��ʶ
                ,''                                                                            AS COLLATERALTYPESTD     --Ȩ�ط�����ѺƷ����
                ,''                                                                            AS COLLATERALSDVSSTD     --Ȩ�ط�����ѺƷϸ��
                ,CASE WHEN T1.QUALIFYFLAG03 = '01' THEN T4.SLOWRELEASETYPE
                      ELSE ''
                 END                                                                           AS COLLATERALTYPEIRB     --����������ѺƷ����
                ,T1.AFFIRMVALUE0                                                               AS COLLATERALAMOUNT     --��Ѻ�ܶ�                              -
                ,CASE WHEN T1.AFFIRMCURRENCY='...' or T1.AFFIRMCURRENCY like '@%' THEN 'CNY'
                	    ELSE NVL(T1.AFFIRMCURRENCY,'CNY')
                 END                                                                           AS CURRENCY              --����
                ,T2.PUTOUTDATE                                                                 AS STARTDATE             --��ʼ����
                ,T2.MATURITY                                                                   AS DUEDATE               --��������
                ,CASE WHEN (TO_DATE(T2.MATURITY,'YYYYMMDD')-TO_DATE(T2.PUTOUTDATE,'YYYYMMDD'))/365<0
                      THEN 0
                      ELSE (TO_DATE(T2.MATURITY,'YYYYMMDD')-TO_DATE(T2.PUTOUTDATE,'YYYYMMDD'))/365
                END                                                                            AS ORIGINALMATURITY      --ԭʼ����
                ,CASE WHEN (TO_DATE(T2.MATURITY,'YYYYMMDD')-TO_DATE(P_DATA_DT_STR,'YYYYMMDD'))/365<0
                      THEN 0
                      ELSE (TO_DATE(T2.MATURITY,'YYYYMMDD')-TO_DATE(P_DATA_DT_STR,'YYYYMMDD'))/365
                END                                                                            AS RESIDUALM             --ʣ������
                ,'0'                                                                           AS INTEHAIRCUTSFLAG      --���й����ۿ�ϵ����ʶ
                ,1                                                                             AS INTERNALHC            --�ڲ��ۿ�ϵ��
                ,CASE WHEN T1.QUALIFYFLAG03 = '01' THEN T4.FINANCIALPLEDGETYPE
                      ELSE ''
                 END                                                                            AS FCTYPE                --������ѺƷ����
                ,CASE WHEN T3.ABSFLAG='01'THEN '1'
                      ELSE '0'
                 END                                                                           AS ABSFLAG               --�ʲ�֤ȯ����ʶ
                ,''                                                                            AS RATINGDURATIONTYPE    --������������
                ,T3.BONDRATING                                                                 AS FCISSUERATING         --������ѺƷ���еȼ�
                ,CASE WHEN (T3.OPENBANKTYPE LIKE '10%' OR T3.OPENBANKTYPE LIKE '01%') THEN '01'
                	    ELSE '02'
                 END                                                                           AS FCISSUERTYPE          --������ѺƷ���������
                ,CASE WHEN T3.BONDPUBLISHCOUNTRY<>'CHN' THEN '02'
                	    ELSE '01'
                 END                                                                           AS FCISSUERSTATE         --������ѺƷ������ע�����
                ,CASE WHEN (TO_DATE(T2.MATURITY,'YYYYMMDD')-TO_DATE(P_DATA_DT_STR,'YYYYMMDD')) / 365 < 0
                      THEN 0
                      ELSE (TO_DATE(T2.MATURITY,'YYYYMMDD')-TO_DATE(P_DATA_DT_STR,'YYYYMMDD')) / 365
                 END                                                                           AS FCRESIDUALM           --������ѺƷʣ������
                ,1                                                                             AS REVAFREQUENCY         --�ع�Ƶ��
                ,''                                                                            AS GROUPID               --������
                ,T5.RATINGRESULT                                                               AS RCERating             --�����˾���ע����ⲿ����
                ,'DZY|PT'
        FROM RWA_DEV.NCM_GUARANTY_INFO T1
  INNER JOIN TEMP_RELATIVE T2
        ON T1.GUARANTYID = T2.GUARANTYID
   LEFT JOIN RWA_DEV.NCM_ASSET_FINANCE T3
        ON T1.GUARANTYID=T3.GUARANTYID
        AND T3.DATANO=P_DATA_DT_STR
   LEFT JOIN	RWA_DEV.NCM_COL_PARAM T4
        ON T1.GUARANTYTYPEID = T4.GUARANTYTYPE
        AND	T4.DATANO = p_data_dt_str
   LEFT JOIN RWA.CODE_LIBRARY T6
        ON T1.GUARANTYTYPEID=T6.ITEMNO
        AND T6.CODENO='GuarantyList'
   LEFT JOIN RWA.RWA_WP_COUNTRYRATING T5
        ON T3.BONDPUBLISHCOUNTRY = T5.COUNTRYCODE
        AND	T5.ISINUSE = '1'
   WHERE T1.DATANO=P_DATA_DT_STR
     --AND T1.CLRSTATUS='01'               --modify by yushuangjiang
     --AND T1.CLRGNTSTATUS IN ('03','10')  --���ھ����Ŵ��¿���ȷ������������״̬������ȥ��
     AND T1.GUARANTYTYPEID NOT IN('004001004001','004001005001','004001006001','004001006002','001001003001')  --����֤����������֤,�����Ա������������Ա��� ����Ϊ��֤
     ;
    COMMIT;
    
    /*1.2 ��Ч����º�ͬ��Ӧ�ĵ���ѺƷ��Ϣ(���ڴ���-΢����)*/
   /* INSERT INTO RWA_DEV.RWA_XD_COLLATERAL(
                 DATADATE                                --��������
                ,DATANO                                 --������ˮ��
                ,COLLATERALID                           --����ѺƷID
                ,SSYSID                                 --ԴϵͳID
                ,SGUARCONTRACTID                        --Դ������ͬID
                ,SCOLLATERALID                          --Դ����ѺƷID
                ,COLLATERALNAME                         --����ѺƷ����
                ,ISSUERID                               --������ID
                ,PROVIDERID                             --�ṩ��ID
                ,CREDITRISKDATATYPE                     --���÷�����������
                ,GUARANTEEWAY                            --������ʽ
                ,SOURCECOLTYPE                          --Դ����ѺƷ����
                ,SOURCECOLSUBTYPE                       --Դ����ѺƷС��
                ,SPECPURPBONDFLAG                       --�Ƿ�Ϊ�չ��������в�����������е�ծȯ
                ,QUALFLAGSTD                            --Ȩ�ط��ϸ��ʶ
                ,QUALFLAGFIRB                           --�����������ϸ��ʶ
                ,COLLATERALTYPESTD                      --Ȩ�ط�����ѺƷ����
                ,COLLATERALSDVSSTD                      --Ȩ�ط�����ѺƷϸ��
                ,COLLATERALTYPEIRB                      --����������ѺƷ����
                ,COLLATERALAMOUNT                        --��Ѻ�ܶ�
                ,CURRENCY                               --����
                ,STARTDATE                              --��ʼ����
                ,DUEDATE                                --��������
                ,ORIGINALMATURITY                       --ԭʼ����
                ,RESIDUALM                              --ʣ������
                ,INTEHAIRCUTSFLAG                       --���й����ۿ�ϵ����ʶ
                ,INTERNALHC                             --�ڲ��ۿ�ϵ��
                ,FCTYPE                                 --������ѺƷ����
                ,ABSFLAG                                --�ʲ�֤ȯ����ʶ
                ,RATINGDURATIONTYPE                     --������������
                ,FCISSUERATING                          --������ѺƷ���еȼ�
                ,FCISSUERTYPE                           --������ѺƷ���������
                ,FCISSUERSTATE                          --������ѺƷ������ע�����
                ,FCRESIDUALM                            --������ѺƷʣ������
                ,REVAFREQUENCY                          --�ع�Ƶ��
                ,GROUPID                                --������
                ,RCERating                              --�����˾���ע����ⲿ����
                ,flag
    )WITH TEMP_COLLATERAL1 AS(SELECT T3.SERIALNO AS CONTRACTNO,
                                     MIN(NVL(T1.PUTOUTDATE,
                                         CASE WHEN T1.BUSINESSTYPE IN ('10201010','10202010') 
                                         THEN NVL(T3.OCCURDATE,T3.PUTOUTDATE) 
                                         ELSE T3.PUTOUTDATE END)) AS PUTOUTDATE
                                    ,MAX(NVL(T1.ACTUALMATURITY,T3.MATURITY)) AS MATURITY,
                                     MIN(T4.CREDITRISKDATATYPE) AS CREDITRISKDATATYPE
                               FROM RWA_DEV.NCM_BUSINESS_DUEBILL T1
                               INNER JOIN RWA_DEV.NCM_BUSINESS_TYPE T2
                               ON T1.BUSINESSTYPE=T2.TYPENO
                               AND T1.DATANO=T2.DATANO
                               AND T2.SORTNO NOT LIKE '30%' --�ų������ҵ��
                               INNER JOIN RWA_DEV.NCM_BUSINESS_CONTRACT T3
                               ON T1.RELATIVESERIALNO2=T3.SERIALNO        --�����Խ��Ϊ׼�����Թ�����ͬʱ����Ӧ�üӺ�ͬ����Ч����
                               AND T1.DATANO=T3.DATANO
                               INNER JOIN RWA_DEV.NCM_BUSINESS_HISTORY T31
                               ON T1.SERIALNO=T31.SERIALNO
                               AND T31.DATANO=P_DATA_DT_STR
                               AND (T31.OVERDUEBALANCE+T31.DULLBALANCE+T31.BADBALANCE)>0   --ȡ�����ڵļ�¼
                                          \*rwa_dev.brd_loan_nor t31
                               ON t1.serialno = t31.crdt_acct_no
                               AND t31.datano = P_DATA_DT_STR
                               AND t31.sbjt_cd = '13100001' --����΢����*\
                               INNER JOIN RWA_DEV.RWA_XD_CONTRACT T4
    													 ON T1.RELATIVESERIALNO2 = T4.CONTRACTID
    													 AND T1.DATANO = T4.DATANO
                               WHERE  T1.DATANO=P_DATA_DT_STR
                               AND T1.BUSINESSTYPE='11103030'  --ֻȡ΢����ҵ��
                               GROUP BY T3.SERIALNO
                             )
    ,TEMP_RELATIVE AS (SELECT T5.GUARANTYID,MIN(T1.PUTOUTDATE) AS PUTOUTDATE,MAX(T1.MATURITY) AS MATURITY,MIN(T1.CREDITRISKDATATYPE) AS CREDITRISKDATATYPE
                       FROM TEMP_COLLATERAL1 T1
                       INNER JOIN
                           (SELECT SERIALNO,OBJECTNO
                            FROM RWA_DEV.NCM_CONTRACT_RELATIVE
                            WHERE DATANO=P_DATA_DT_STR
                            AND OBJECTTYPE = 'GuarantyContract'
                            GROUP BY  SERIALNO,OBJECTNO
                            ) T2
                      ON T1.CONTRACTNO=T2.SERIALNO
                      INNER JOIN RWA_DEV.NCM_GUARANTY_CONTRACT T3
                      ON T2.OBJECTNO = T3.SERIALNO
                      AND T3.DATANO=P_DATA_DT_STR
                      INNER JOIN (SELECT CONTRACTNO, GUARANTYID
                                  FROM RWA_DEV.NCM_GUARANTY_RELATIVE
                                  WHERE DATANO=P_DATA_DT_STR
                                  GROUP BY CONTRACTNO, GUARANTYID
                                  ) T4
                      ON T3.SERIALNO=T4.CONTRACTNO
                      INNER JOIN RWA_DEV.NCM_GUARANTY_INFO T5
                      ON T4.GUARANTYID=T5.GUARANTYID
                      AND T5.DATANO=P_DATA_DT_STR
                      GROUP BY T5.GUARANTYID
                   )
    SELECT
                 TO_DATE(T1.DATANO,'YYYYMMDD')                                                AS DATADATE              --��������
                ,T1.DATANO                                                                    AS DATANO                --������ˮ��
                ,'YP'||T1.GUARANTYID                                                          AS COLLATERALID          --����ѺƷID
                ,'XD'                                                                         AS SSYSID                --ԴϵͳID
                ,''                                                                           AS SGUARCONTRACTID       --Դ������ͬID
                ,T1.GUARANTYID                                                                AS SCOLLATERALID         --Դ����ѺƷID
                ,T6.ITEMNAME                                                                  AS COLLATERALNAME        --����ѺƷ����
                ,CASE WHEN (T1.GUARANTYTYPEID LIKE '001003%' OR T1.GUARANTYTYPEID LIKE '001004%' OR T1.GUARANTYTYPEID LIKE '001001%') --������ѺƷ��Ҫ������
                      THEN T3.OPENBANKNO
                      ELSE ''
                 END                                                                           AS ISSUERID              --������ID
                ,T1.CLRERID                                                                    AS PROVIDERID            --�ṩ��ID
                ,T2.CREDITRISKDATATYPE                                                         AS CREDITRISKDATATYPE    --���÷�����������
                ,T1.GUARANTYTYPE                                                               AS GUARANTEEWAY          --������ʽ
                ,SUBSTR(T1.GUARANTYTYPEID,1,6)                                                 AS SOURCECOLTYPE         --Դ����ѺƷ����
                ,T1.GUARANTYTYPEID                                                             AS SOURCECOLSUBTYPE      --Դ����ѺƷС��
                ,CASE WHEN T3.BONDPUBLISHPURPOSE='0010' THEN '1'
                      ELSE '0'
                 END                                                                           AS SPECPURPBONDFLAG      --�Ƿ�Ϊ�չ��������в�����������е�ծȯ
                ,''                                                                            AS QUALFLAGSTD           --Ȩ�ط��ϸ��ʶ
                ,CASE WHEN T1.QUALIFYFLAG03='01' THEN '1'
                	    WHEN T1.QUALIFYFLAG03='02' THEN '0'
                      ELSE ''
                 END                                                                           AS QUALFLAGFIRB          --�����������ϸ��ʶ
                ,''                                                                            AS COLLATERALTYPESTD     --Ȩ�ط�����ѺƷ����
                ,''                                                                            AS COLLATERALSDVSSTD     --Ȩ�ط�����ѺƷϸ��
                ,CASE WHEN T1.QUALIFYFLAG03 = '01' THEN T4.SLOWRELEASETYPE
                      ELSE ''
                 END                                                                           AS COLLATERALTYPEIRB     --����������ѺƷ����
                ,T1.AFFIRMVALUE0                                                               AS COLLATERALAMOUNT     --��Ѻ�ܶ�                              -
                ,CASE WHEN T1.AFFIRMCURRENCY='...' THEN 'CNY'
                	    ELSE NVL(T1.AFFIRMCURRENCY,'CNY')
                 END                                                                           AS CURRENCY              --����
                ,T2.PUTOUTDATE                                                                 AS STARTDATE             --��ʼ����
                ,T2.MATURITY                                                                   AS DUEDATE               --��������
                ,CASE WHEN (TO_DATE(T2.MATURITY,'YYYYMMDD')-TO_DATE(T2.PUTOUTDATE,'YYYYMMDD'))/365<0
                      THEN 0
                      ELSE (TO_DATE(T2.MATURITY,'YYYYMMDD')-TO_DATE(T2.PUTOUTDATE,'YYYYMMDD'))/365
                END                                                                            AS ORIGINALMATURITY      --ԭʼ����
                ,CASE WHEN (TO_DATE(T2.MATURITY,'YYYYMMDD')-TO_DATE(P_DATA_DT_STR,'YYYYMMDD'))/365<0
                      THEN 0
                      ELSE (TO_DATE(T2.MATURITY,'YYYYMMDD')-TO_DATE(P_DATA_DT_STR,'YYYYMMDD'))/365
                END                                                                            AS RESIDUALM             --ʣ������
                ,'0'                                                                           AS INTEHAIRCUTSFLAG      --���й����ۿ�ϵ����ʶ
                ,1                                                                             AS INTERNALHC            --�ڲ��ۿ�ϵ��
                ,CASE WHEN T1.QUALIFYFLAG03 = '01' THEN T4.FINANCIALPLEDGETYPE
                      ELSE ''
                 END                                                                            AS FCTYPE                --������ѺƷ����
                ,CASE WHEN T3.ABSFLAG='01'THEN '1'
                      ELSE '0'
                 END                                                                           AS ABSFLAG               --�ʲ�֤ȯ����ʶ
                ,''                                                                            AS RATINGDURATIONTYPE    --������������
                ,T3.BONDRATING                                                                 AS FCISSUERATING         --������ѺƷ���еȼ�
                ,CASE WHEN (T3.OPENBANKTYPE LIKE '10%' OR T3.OPENBANKTYPE LIKE '01%') THEN '01'
                	    ELSE '02'
                 END                                                                           AS FCISSUERTYPE          --������ѺƷ���������
                ,CASE WHEN T3.BONDPUBLISHCOUNTRY<>'CHN' THEN '02'
                	    ELSE '01'
                 END                                                                           AS FCISSUERSTATE         --������ѺƷ������ע�����
                ,CASE WHEN (TO_DATE(T2.MATURITY,'YYYYMMDD')-TO_DATE(P_DATA_DT_STR,'YYYYMMDD')) / 365 < 0
                      THEN 0
                      ELSE (TO_DATE(T2.MATURITY,'YYYYMMDD')-TO_DATE(P_DATA_DT_STR,'YYYYMMDD')) / 365
                 END                                                                           AS FCRESIDUALM           --������ѺƷʣ������
                ,1                                                                             AS REVAFREQUENCY         --�ع�Ƶ��
                ,''                                                                            AS GROUPID               --������
                ,T5.RATINGRESULT                                                               AS RCERating             --�����˾���ע����ⲿ����
                ,'DZY|YQWLD'
    FROM RWA_DEV.NCM_GUARANTY_INFO T1
    INNER JOIN TEMP_RELATIVE T2
    ON T1.GUARANTYID = T2.GUARANTYID
    LEFT JOIN RWA_DEV.NCM_ASSET_FINANCE T3
    ON T1.GUARANTYID=T3.GUARANTYID
    AND T3.DATANO=P_DATA_DT_STR
    LEFT JOIN	RWA_DEV.NCM_COL_PARAM T4
    ON T1.GUARANTYTYPEID = T4.GUARANTYTYPE
    AND	T4.DATANO = p_data_dt_str
    LEFT JOIN RWA.CODE_LIBRARY T6
    ON T1.GUARANTYTYPEID=T6.ITEMNO
    AND T6.CODENO='GuarantyList'
    LEFT JOIN RWA.RWA_WP_COUNTRYRATING T5
    ON T3.BONDPUBLISHCOUNTRY = T5.COUNTRYCODE
    AND	T5.ISINUSE = '1'
    WHERE T1.DATANO=P_DATA_DT_STR
    --AND T1.CLRSTATUS='01'               --modify by yushuangjiang
     --AND T1.CLRGNTSTATUS IN ('03','10')  --���ھ����Ŵ��¿���ȷ������������״̬������ȥ��
    AND T1.GUARANTYTYPEID NOT IN('004001004001','004001005001','004001006001','004001006002','001001003001')  --����֤����������֤,�����Ա������������Ա��� ����Ϊ��֤
    AND NOT EXISTS(SELECT 1 FROM RWA_DEV.RWA_XD_COLLATERAL T6 WHERE 'YP'||T1.GUARANTYID=T6.COLLATERALID)
    ;
    COMMIT;*/
    
    /*1.3 ��Ч����º�ͬ��Ӧ�ĵ���ѺƷ��Ϣ(���ڴ���-����ҵ��)*/
    INSERT INTO RWA_DEV.RWA_XD_COLLATERAL(
                 DATADATE                                --��������
                ,DATANO                                 --������ˮ��
                ,COLLATERALID                           --����ѺƷID
                ,SSYSID                                 --ԴϵͳID
                ,SGUARCONTRACTID                        --Դ������ͬID
                ,SCOLLATERALID                          --Դ����ѺƷID
                ,COLLATERALNAME                         --����ѺƷ����
                ,ISSUERID                               --������ID
                ,PROVIDERID                             --�ṩ��ID
                ,CREDITRISKDATATYPE                     --���÷�����������
                ,GUARANTEEWAY                            --������ʽ
                ,SOURCECOLTYPE                          --Դ����ѺƷ����
                ,SOURCECOLSUBTYPE                       --Դ����ѺƷС��
                ,SPECPURPBONDFLAG                       --�Ƿ�Ϊ�չ��������в�����������е�ծȯ
                ,QUALFLAGSTD                            --Ȩ�ط��ϸ��ʶ
                ,QUALFLAGFIRB                           --�����������ϸ��ʶ
                ,COLLATERALTYPESTD                      --Ȩ�ط�����ѺƷ����
                ,COLLATERALSDVSSTD                      --Ȩ�ط�����ѺƷϸ��
                ,COLLATERALTYPEIRB                      --����������ѺƷ����
                ,COLLATERALAMOUNT                        --��Ѻ�ܶ�
                ,CURRENCY                               --����
                ,STARTDATE                              --��ʼ����
                ,DUEDATE                                --��������
                ,ORIGINALMATURITY                       --ԭʼ����
                ,RESIDUALM                              --ʣ������
                ,INTEHAIRCUTSFLAG                       --���й����ۿ�ϵ����ʶ
                ,INTERNALHC                             --�ڲ��ۿ�ϵ��
                ,FCTYPE                                 --������ѺƷ����
                ,ABSFLAG                                --�ʲ�֤ȯ����ʶ
                ,RATINGDURATIONTYPE                     --������������
                ,FCISSUERATING                          --������ѺƷ���еȼ�
                ,FCISSUERTYPE                           --������ѺƷ���������
                ,FCISSUERSTATE                          --������ѺƷ������ע�����
                ,FCRESIDUALM                            --������ѺƷʣ������
                ,REVAFREQUENCY                          --�ع�Ƶ��
                ,GROUPID                                --������
                ,RCERating                              --�����˾���ע����ⲿ����
                ,flag
    )WITH TEMP_COLLATERAL1 AS(SELECT T3.SERIALNO AS CONTRACTNO
                                    ,MIN(CASE WHEN T1.BUSINESSTYPE IN ('10201010','10202010','1035101010','1035101020','1035102010','1035102020','103550','1040101040') --����֤,����
                                              THEN NVL(T3.PUTOUTDATE,T3.OCCURDATE)   --����֤������ȡ��ͬ��ʼ����
                                              ELSE T1.PUTOUTDATE 
                                         END) AS PUTOUTDATE
                                    ,MAX(CASE WHEN T1.BUSINESSTYPE IN ('10201010','10202010','1035101010','1035101020','1035102010','1035102020','103550','1040101040') --����֤,����
                                              THEN T3.MATURITY
                                              ELSE T1.ACTUALMATURITY
                                         END) AS MATURITY
                                    ,MIN(T4.CREDITRISKDATATYPE) AS CREDITRISKDATATYPE
                               FROM RWA_DEV.NCM_BUSINESS_DUEBILL T1
                               INNER JOIN RWA_DEV.NCM_BUSINESS_TYPE T2
                               ON T1.BUSINESSTYPE=T2.TYPENO
                               AND T1.DATANO=T2.DATANO
                               AND T2.SORTNO NOT LIKE '30%' --�ų������ҵ��
                               INNER JOIN RWA_DEV.NCM_BUSINESS_CONTRACT T3
                               ON T1.RELATIVESERIALNO2=T3.SERIALNO        --�����Խ��Ϊ׼�����Թ�����ͬʱ����Ӧ�üӺ�ͬ����Ч����
                               AND T1.DATANO=T3.DATANO
                               INNER JOIN BRD_LOAN_NOR T4 --��������
                               ON  T4.DATANO = p_data_dt_str
                               AND T4.CRDT_ACCT_NO = T1.SERIALNO 
                               AND substr(T4.SBJT_CD,1,4) = '1310' --��Ŀ���
                               AND T4.SBJT_CD != '13100001' --���в���΢���������ڴ���                               
                               INNER JOIN RWA_DEV.RWA_XD_CONTRACT T4
                               ON T1.RELATIVESERIALNO2 = T4.CONTRACTID
                               AND T1.DATANO = T4.DATANO
                               WHERE  T1.DATANO=P_DATA_DT_STR
                               GROUP BY T3.SERIALNO                            
                             )
    ,TEMP_RELATIVE AS (SELECT T5.GUARANTYID,MIN(T1.PUTOUTDATE) AS PUTOUTDATE,MAX(T1.MATURITY) AS MATURITY,MIN(T1.CREDITRISKDATATYPE) AS CREDITRISKDATATYPE
                       FROM TEMP_COLLATERAL1 T1
                       INNER JOIN
                           (SELECT SERIALNO,OBJECTNO
                            FROM RWA_DEV.NCM_CONTRACT_RELATIVE
                            WHERE DATANO=P_DATA_DT_STR
                            AND OBJECTTYPE = 'GuarantyContract'
                            GROUP BY  SERIALNO,OBJECTNO
                            ) T2
                      ON T1.CONTRACTNO=T2.SERIALNO
                      INNER JOIN RWA_DEV.NCM_GUARANTY_CONTRACT T3
                      ON T2.OBJECTNO = T3.SERIALNO
                      AND T3.DATANO=P_DATA_DT_STR
                      INNER JOIN (SELECT CONTRACTNO, GUARANTYID
                                  FROM RWA_DEV.NCM_GUARANTY_RELATIVE
                                  WHERE DATANO=P_DATA_DT_STR
                                  GROUP BY CONTRACTNO, GUARANTYID
                                  ) T4
                      ON T3.SERIALNO=T4.CONTRACTNO
                      INNER JOIN RWA_DEV.NCM_GUARANTY_INFO T5
                      ON T4.GUARANTYID=T5.GUARANTYID
                      AND T5.DATANO=P_DATA_DT_STR
                      GROUP BY T5.GUARANTYID
                   )
    SELECT
                 TO_DATE(T1.DATANO,'YYYYMMDD')                                                AS DATADATE              --��������
                ,T1.DATANO                                                                    AS DATANO                --������ˮ��
                ,'YP'||T1.GUARANTYID                                                          AS COLLATERALID          --����ѺƷID
                ,'XD'                                                                         AS SSYSID                --ԴϵͳID
                ,''                                                                           AS SGUARCONTRACTID       --Դ������ͬID
                ,T1.GUARANTYID                                                                AS SCOLLATERALID         --Դ����ѺƷID
                ,T6.ITEMNAME                                                                  AS COLLATERALNAME        --����ѺƷ����
                ,CASE WHEN (T1.GUARANTYTYPEID LIKE '001003%' OR T1.GUARANTYTYPEID LIKE '001004%' OR T1.GUARANTYTYPEID LIKE '001001%') --������ѺƷ��Ҫ������
                      THEN NVL(T3.OPENBANKNO,'�й���ҵ����')
                      ELSE ''
                 END                                                                           AS ISSUERID              --������ID
                ,T1.CLRERID                                                                    AS PROVIDERID            --�ṩ��ID
                ,T2.CREDITRISKDATATYPE                                                         AS CREDITRISKDATATYPE    --���÷�����������
                ,T1.GUARANTYTYPE                                                               AS GUARANTEEWAY          --������ʽ
                ,SUBSTR(T1.GUARANTYTYPEID,1,6)                                                 AS SOURCECOLTYPE         --Դ����ѺƷ����
                ,T1.GUARANTYTYPEID                                                             AS SOURCECOLSUBTYPE      --Դ����ѺƷС��
                ,CASE WHEN T3.BONDPUBLISHPURPOSE='0010' THEN '1'
                      ELSE '0'
                 END                                                                           AS SPECPURPBONDFLAG      --�Ƿ�Ϊ�չ��������в�����������е�ծȯ
                ,''                                                                            AS QUALFLAGSTD           --Ȩ�ط��ϸ��ʶ
                ,CASE WHEN T1.QUALIFYFLAG03='01' THEN '1'
                	    WHEN T1.QUALIFYFLAG03='02' THEN '0'
                      ELSE ''
                 END                                                                           AS QUALFLAGFIRB          --�����������ϸ��ʶ
                ,''                                                                            AS COLLATERALTYPESTD     --Ȩ�ط�����ѺƷ����
                ,''                                                                            AS COLLATERALSDVSSTD     --Ȩ�ط�����ѺƷϸ��
                ,CASE WHEN T1.QUALIFYFLAG03 = '01' THEN T4.SLOWRELEASETYPE
                      ELSE ''
                 END                                                                           AS COLLATERALTYPEIRB     --����������ѺƷ����
                ,T1.AFFIRMVALUE0                                                               AS COLLATERALAMOUNT     --��Ѻ�ܶ�                              -
                ,CASE WHEN T1.AFFIRMCURRENCY='...' or T1.AFFIRMCURRENCY like '@%' THEN 'CNY'
                	    ELSE NVL(T1.AFFIRMCURRENCY,'CNY')
                 END                                                                           AS CURRENCY              --����
                ,T2.PUTOUTDATE                                                                 AS STARTDATE             --��ʼ����
                ,T2.MATURITY                                                                   AS DUEDATE               --��������
                ,CASE WHEN (TO_DATE(T2.MATURITY,'YYYYMMDD')-TO_DATE(T2.PUTOUTDATE,'YYYYMMDD'))/365<0
                      THEN 0
                      ELSE (TO_DATE(T2.MATURITY,'YYYYMMDD')-TO_DATE(T2.PUTOUTDATE,'YYYYMMDD'))/365
                END                                                                            AS ORIGINALMATURITY      --ԭʼ����
                ,CASE WHEN (TO_DATE(T2.MATURITY,'YYYYMMDD')-TO_DATE(P_DATA_DT_STR,'YYYYMMDD'))/365<0
                      THEN 0
                      ELSE (TO_DATE(T2.MATURITY,'YYYYMMDD')-TO_DATE(P_DATA_DT_STR,'YYYYMMDD'))/365
                END                                                                            AS RESIDUALM             --ʣ������
                ,'0'                                                                           AS INTEHAIRCUTSFLAG      --���й����ۿ�ϵ����ʶ
                ,1                                                                             AS INTERNALHC            --�ڲ��ۿ�ϵ��
                ,CASE WHEN T1.QUALIFYFLAG03 = '01' THEN T4.FINANCIALPLEDGETYPE
                      ELSE ''
                 END                                                                            AS FCTYPE                --������ѺƷ����
                ,CASE WHEN T3.ABSFLAG='01'THEN '1'
                      ELSE '0'
                 END                                                                           AS ABSFLAG               --�ʲ�֤ȯ����ʶ
                ,''                                                                            AS RATINGDURATIONTYPE    --������������
                ,T3.BONDRATING                                                                 AS FCISSUERATING         --������ѺƷ���еȼ�
                ,CASE WHEN (T3.OPENBANKTYPE LIKE '10%' OR T3.OPENBANKTYPE LIKE '01%') THEN '01'
                	    ELSE '02'
                 END                                                                           AS FCISSUERTYPE          --������ѺƷ���������
                ,CASE WHEN T3.BONDPUBLISHCOUNTRY<>'CHN' THEN '02'
                	    ELSE '01'
                 END                                                                           AS FCISSUERSTATE         --������ѺƷ������ע�����
                ,CASE WHEN (TO_DATE(T2.MATURITY,'YYYYMMDD')-TO_DATE(P_DATA_DT_STR,'YYYYMMDD')) / 365 < 0
                      THEN 0
                      ELSE (TO_DATE(T2.MATURITY,'YYYYMMDD')-TO_DATE(P_DATA_DT_STR,'YYYYMMDD')) / 365
                 END                                                                           AS FCRESIDUALM           --������ѺƷʣ������
                ,1                                                                             AS REVAFREQUENCY         --�ع�Ƶ��
                ,''                                                                            AS GROUPID               --������
                ,T5.RATINGRESULT                                                               AS RCERating             --�����˾���ע����ⲿ����
                ,'DZY|YQ'
    FROM RWA_DEV.NCM_GUARANTY_INFO T1
    INNER JOIN TEMP_RELATIVE T2
    ON T1.GUARANTYID = T2.GUARANTYID
    LEFT JOIN RWA_DEV.NCM_ASSET_FINANCE T3
    ON T1.GUARANTYID=T3.GUARANTYID
    AND T3.DATANO=P_DATA_DT_STR
    LEFT JOIN	RWA_DEV.NCM_COL_PARAM T4
    ON T1.GUARANTYTYPEID = T4.GUARANTYTYPE
    AND	T4.DATANO = p_data_dt_str
    LEFT JOIN RWA.CODE_LIBRARY T6
    ON T1.GUARANTYTYPEID=T6.ITEMNO
    AND T6.CODENO='GuarantyList'
    LEFT JOIN RWA.RWA_WP_COUNTRYRATING T5
    ON T3.BONDPUBLISHCOUNTRY = T5.COUNTRYCODE
    AND	T5.ISINUSE = '1'
    WHERE T1.DATANO=P_DATA_DT_STR
    --AND T1.CLRSTATUS='01'               --modify by yushuangjiang
     --AND T1.CLRGNTSTATUS IN ('03','10')  --���ھ����Ŵ��¿���ȷ������������״̬������ȥ��
    AND T1.GUARANTYTYPEID NOT IN('004001004001','004001005001','004001006001','004001006002','001001003001')  --����֤����������֤,�����Ա������������Ա��� ����Ϊ��֤
    AND NOT EXISTS(SELECT 1 FROM RWA_DEV.RWA_XD_COLLATERAL T6 WHERE 'YP'||T1.GUARANTYID=T6.COLLATERALID)
    ;
    COMMIT;
    
    /*1.4 ��Ч����º�ͬ��Ӧ�ĵ���ѺƷ��Ϣ(׷�ӵ�PUTOUT�� �ϵĵ���ѺƷ��Ϣ)*/
    INSERT INTO RWA_DEV.RWA_XD_COLLATERAL(
                 DATADATE                                --��������
                ,DATANO                                 --������ˮ��
                ,COLLATERALID                           --����ѺƷID
                ,SSYSID                                 --ԴϵͳID
                ,SGUARCONTRACTID                        --Դ������ͬID
                ,SCOLLATERALID                          --Դ����ѺƷID
                ,COLLATERALNAME                         --����ѺƷ����
                ,ISSUERID                               --������ID
                ,PROVIDERID                             --�ṩ��ID
                ,CREDITRISKDATATYPE                     --���÷�����������
                ,GUARANTEEWAY                            --������ʽ
                ,SOURCECOLTYPE                          --Դ����ѺƷ����
                ,SOURCECOLSUBTYPE                       --Դ����ѺƷС��
                ,SPECPURPBONDFLAG                       --�Ƿ�Ϊ�չ��������в�����������е�ծȯ
                ,QUALFLAGSTD                            --Ȩ�ط��ϸ��ʶ
                ,QUALFLAGFIRB                           --�����������ϸ��ʶ
                ,COLLATERALTYPESTD                      --Ȩ�ط�����ѺƷ����
                ,COLLATERALSDVSSTD                      --Ȩ�ط�����ѺƷϸ��
                ,COLLATERALTYPEIRB                      --����������ѺƷ����
                ,COLLATERALAMOUNT                        --��Ѻ�ܶ�
                ,CURRENCY                               --����
                ,STARTDATE                              --��ʼ����
                ,DUEDATE                                --��������
                ,ORIGINALMATURITY                       --ԭʼ����
                ,RESIDUALM                              --ʣ������
                ,INTEHAIRCUTSFLAG                       --���й����ۿ�ϵ����ʶ
                ,INTERNALHC                             --�ڲ��ۿ�ϵ��
                ,FCTYPE                                 --������ѺƷ����
                ,ABSFLAG                                --�ʲ�֤ȯ����ʶ
                ,RATINGDURATIONTYPE                     --������������
                ,FCISSUERATING                          --������ѺƷ���еȼ�
                ,FCISSUERTYPE                           --������ѺƷ���������
                ,FCISSUERSTATE                          --������ѺƷ������ע�����
                ,FCRESIDUALM                            --������ѺƷʣ������
                ,REVAFREQUENCY                          --�ع�Ƶ��
                ,GROUPID                                --������
                ,RCERating                              --�����˾���ע����ⲿ����
                ,flag
    )WITH TEMP_COLLATERAL1 AS(SELECT T3.SERIALNO AS CONTRACTNO,MIN(NVL(T1.PUTOUTDATE,CASE WHEN T1.BUSINESSTYPE IN ('10201010','10202010') THEN NVL(T3.OCCURDATE,T3.PUTOUTDATE) ELSE T3.PUTOUTDATE END)) AS PUTOUTDATE
                                    ,MAX(NVL(T3.MATURITY,T1.ACTUALMATURITY)) AS MATURITY,MIN(T6.CREDITRISKDATATYPE) AS CREDITRISKDATATYPE,T4.SERIALNO AS BPSERIALNO
                               FROM RWA_DEV.NCM_BUSINESS_DUEBILL T1
                               INNER JOIN RWA_DEV.NCM_BUSINESS_TYPE T2
                               ON T1.BUSINESSTYPE=T2.TYPENO
                               AND T1.DATANO=T2.DATANO
                               AND T2.SORTNO NOT LIKE '30%' --�ų������ҵ��
                               INNER JOIN RWA_DEV.NCM_BUSINESS_CONTRACT T3
                               ON T1.RELATIVESERIALNO2=T3.SERIALNO        --�����Խ��Ϊ׼�����Թ�����ͬʱ����Ӧ�üӺ�ͬ����Ч����
                               AND T1.DATANO=T3.DATANO
                               INNER JOIN RWA_DEV.NCM_BUSINESS_PUTOUT T4
                               ON T3.SERIALNO=T4.CONTRACTSERIALNO
                               AND T4.DATANO=P_DATA_DT_STR
                               INNER JOIN RWA_DEV.NCM_BUSINESS_HISTORY T31
                               ON T1.SERIALNO=T31.SERIALNO
                               AND T31.DATANO=P_DATA_DT_STR
                               AND T31.BALANCE>0
                                         /* rwa_dev.brd_loan_nor t31
                               ON t1.serialno = t31.crdt_acct_no
                               AND t31.datano = P_DATA_DT_STR
                               AND t31.cur_bal > 0*/
                               INNER JOIN RWA_DEV.RWA_XD_CONTRACT T6
    													 ON T1.RELATIVESERIALNO2 = T6.CONTRACTID
    													 AND T1.DATANO = T6.DATANO
                               WHERE T1.DATANO=P_DATA_DT_STR
                               --AND T1.BALANCE>0           --��Ч�ж�����
                               AND T1.SERIALNO<>'BD2014110400000001'
                               AND T1.BUSINESSTYPE NOT IN ('1040101010','1040101020','1040101030','1040102010','1040102020','1040102040','10303010'
                                   ,'1040105060','1040201010','1040201020','1040202010','105010','11105010','11105020','10302010','10302015','10302020')  --�ų�ͬҵ���ع���Ͷ�ʣ�ί�д���ҵ��,���ֺ�ת��Ҳ�ų�����Ʊ����Ϣ��Ϊ����
                               GROUP BY T3.SERIALNO,T4.SERIALNO
                             )
    ,TEMP_RELATIVE AS (SELECT T5.GUARANTYID,MIN(T1.PUTOUTDATE) AS PUTOUTDATE,MAX(T1.MATURITY) AS MATURITY,MIN(T1.CREDITRISKDATATYPE) AS CREDITRISKDATATYPE
                       FROM TEMP_COLLATERAL1 T1
                      INNER JOIN (SELECT OBJECTNO, GUARANTYID
                                  FROM RWA_DEV.NCM_GUARANTY_RELATIVE
                                  WHERE DATANO=P_DATA_DT_STR
                                  AND OBJECTTYPE='PutOutApply'
                                  GROUP BY OBJECTNO, GUARANTYID
                                  ) T4
                      ON T1.BPSERIALNO=T4.OBJECTNO
                      INNER JOIN RWA_DEV.NCM_GUARANTY_INFO T5
                      ON T4.GUARANTYID=T5.GUARANTYID
                      AND T5.DATANO=P_DATA_DT_STR
                      GROUP BY T5.GUARANTYID
                   )
    SELECT
                 TO_DATE(T1.DATANO,'YYYYMMDD')                                                AS DATADATE              --��������
                ,T1.DATANO                                                                    AS DATANO                --������ˮ��
                ,'YP'||T1.GUARANTYID                                                          AS COLLATERALID          --����ѺƷID
                ,'XD'                                                                         AS SSYSID                --ԴϵͳID
                ,''                                                                           AS SGUARCONTRACTID       --Դ������ͬID
                ,T1.GUARANTYID                                                                AS SCOLLATERALID         --Դ����ѺƷID
                ,T6.ITEMNAME                                                                  AS COLLATERALNAME        --����ѺƷ����
                ,CASE WHEN (T1.GUARANTYTYPEID LIKE '001003%' OR T1.GUARANTYTYPEID LIKE '001004%' OR T1.GUARANTYTYPEID LIKE '001001%') --������ѺƷ��Ҫ������
                      THEN T3.OPENBANKNO
                      ELSE ''
                 END                                                                           AS ISSUERID              --������ID
                ,T1.CLRERID                                                                    AS PROVIDERID            --�ṩ��ID
                ,T2.CREDITRISKDATATYPE                                                         AS CREDITRISKDATATYPE    --���÷�����������
                ,T1.GUARANTYTYPE                                                               AS GUARANTEEWAY          --������ʽ
                ,SUBSTR(T1.GUARANTYTYPEID,1,6)                                                 AS SOURCECOLTYPE         --Դ����ѺƷ����
                ,T1.GUARANTYTYPEID                                                             AS SOURCECOLSUBTYPE      --Դ����ѺƷС��
                ,CASE WHEN T3.BONDPUBLISHPURPOSE='0010' THEN '1'
                      ELSE '0'
                 END                                                                           AS SPECPURPBONDFLAG      --�Ƿ�Ϊ�չ��������в�����������е�ծȯ
                ,''                                                                            AS QUALFLAGSTD           --Ȩ�ط��ϸ��ʶ
                ,CASE WHEN T1.QUALIFYFLAG03='01' THEN '1'
                	    WHEN T1.QUALIFYFLAG03='02' THEN '0'
                      ELSE ''
                 END                                                                           AS QUALFLAGFIRB          --�����������ϸ��ʶ
                ,''                                                                            AS COLLATERALTYPESTD     --Ȩ�ط�����ѺƷ����
                ,''                                                                            AS COLLATERALSDVSSTD     --Ȩ�ط�����ѺƷϸ��
                ,CASE WHEN T1.QUALIFYFLAG03 = '01' THEN T4.SLOWRELEASETYPE
                      ELSE ''
                 END                                                                           AS COLLATERALTYPEIRB     --����������ѺƷ����
                ,T1.AFFIRMVALUE0                                                               AS COLLATERALAMOUNT     --��Ѻ�ܶ�                              -
                ,CASE WHEN T1.AFFIRMCURRENCY='...' THEN 'CNY'
                	    ELSE NVL(T1.AFFIRMCURRENCY,'CNY')
                 END                                                                           AS CURRENCY              --����
                ,T2.PUTOUTDATE                                                                 AS STARTDATE             --��ʼ����
                ,T2.MATURITY                                                                   AS DUEDATE               --��������
                ,CASE WHEN (TO_DATE(T2.MATURITY,'YYYYMMDD')-TO_DATE(T2.PUTOUTDATE,'YYYYMMDD'))/365<0
                      THEN 0
                      ELSE (TO_DATE(T2.MATURITY,'YYYYMMDD')-TO_DATE(T2.PUTOUTDATE,'YYYYMMDD'))/365
                END                                                                            AS ORIGINALMATURITY      --ԭʼ����
                ,CASE WHEN (TO_DATE(T2.MATURITY,'YYYYMMDD')-TO_DATE(P_DATA_DT_STR,'YYYYMMDD'))/365<0
                      THEN 0
                      ELSE (TO_DATE(T2.MATURITY,'YYYYMMDD')-TO_DATE(P_DATA_DT_STR,'YYYYMMDD'))/365
                END                                                                            AS RESIDUALM             --ʣ������
                ,'0'                                                                           AS INTEHAIRCUTSFLAG      --���й����ۿ�ϵ����ʶ
                ,1                                                                             AS INTERNALHC            --�ڲ��ۿ�ϵ��
                ,CASE WHEN T1.QUALIFYFLAG03 = '01' THEN T4.FINANCIALPLEDGETYPE
                      ELSE ''
                 END                                                                            AS FCTYPE                --������ѺƷ����
                ,CASE WHEN T3.ABSFLAG='01'THEN '1'
                      ELSE '0'
                 END                                                                           AS ABSFLAG               --�ʲ�֤ȯ����ʶ
                ,CASE WHEN T1.QUALIFYFLAG03 = '01' THEN T4.FINANCIALPLEDGETYPE
                      ELSE ''
                 END                                                                           AS RATINGDURATIONTYPE    --������������
                ,T3.BONDRATING                                                                 AS FCISSUERATING         --������ѺƷ���еȼ�
                ,CASE WHEN (T3.OPENBANKTYPE LIKE '10%' OR T3.OPENBANKTYPE LIKE '01%') THEN '01'
                	    ELSE '02'
                 END                                                                           AS FCISSUERTYPE          --������ѺƷ���������
                ,CASE WHEN T3.BONDPUBLISHCOUNTRY<>'CHN' THEN '02'
                	    ELSE '01'
                 END                                                                           AS FCISSUERSTATE         --������ѺƷ������ע�����
                ,CASE WHEN (TO_DATE(T2.MATURITY,'YYYYMMDD')-TO_DATE(P_DATA_DT_STR,'YYYYMMDD')) / 365 < 0
                      THEN 0
                      ELSE (TO_DATE(T2.MATURITY,'YYYYMMDD')-TO_DATE(P_DATA_DT_STR,'YYYYMMDD')) / 365
                 END                                                                           AS FCRESIDUALM           --������ѺƷʣ������
                ,1                                                                             AS REVAFREQUENCY         --�ع�Ƶ��
                ,''                                                                            AS GROUPID               --������
                ,T5.RATINGRESULT                                                               AS RCERating             --�����˾���ע����ⲿ����
                ,'DZY|PUTOUT'
    FROM RWA_DEV.NCM_GUARANTY_INFO T1
    INNER JOIN TEMP_RELATIVE T2
    ON T1.GUARANTYID = T2.GUARANTYID
    LEFT JOIN NCM_ASSET_FINANCE T3
    ON T1.GUARANTYID=T3.GUARANTYID
    AND T3.DATANO=P_DATA_DT_STR
    LEFT JOIN	RWA_DEV.NCM_COL_PARAM T4
    ON T1.GUARANTYTYPEID = T4.GUARANTYTYPE
    AND	T4.DATANO = P_DATA_DT_STR
    LEFT JOIN RWA.CODE_LIBRARY T6
    ON T1.GUARANTYTYPEID=T6.ITEMNO
    AND T6.CODENO='GuarantyList'
    LEFT JOIN RWA.RWA_WP_COUNTRYRATING T5
    ON T3.BONDPUBLISHCOUNTRY = T5.COUNTRYCODE
    AND	T5.ISINUSE = '1'
    WHERE T1.DATANO=P_DATA_DT_STR
    --AND T1.CLRSTATUS='01'               --modify by yushuangjiang
     --AND T1.CLRGNTSTATUS IN ('03','10')  --���ھ����Ŵ��¿���ȷ������������״̬������ȥ��
    AND T1.GUARANTYTYPEID NOT IN('004001004001','004001005001','004001006001','004001006002','001001003001')     --����֤����������֤,�����Ա������������Ա��� ����Ϊ��֤
    AND NOT EXISTS (SELECT 1 FROM RWA_DEV.RWA_XD_COLLATERAL T7 WHERE 'YP'||T1.GUARANTYID=T7.COLLATERALID )
    ;
    COMMIT;
    
    /*1.5 ��Ч����º�ͬ��Ӧ�ĵ���ѺƷ��Ϣ��Ʊ�����֣�ת����_��ת��*/
    /*INSERT INTO RWA_DEV.RWA_XD_COLLATERAL(
                 DATADATE                                --��������
                ,DATANO                                 --������ˮ��
                ,COLLATERALID                           --����ѺƷID
                ,SSYSID                                 --ԴϵͳID
                ,SGUARCONTRACTID                        --Դ������ͬID
                ,SCOLLATERALID                          --Դ����ѺƷID
                ,COLLATERALNAME                         --����ѺƷ����
                ,ISSUERID                               --������ID
                ,PROVIDERID                             --�ṩ��ID
                ,CREDITRISKDATATYPE                     --���÷�����������
                ,GUARANTEEWAY                            --������ʽ
                ,SOURCECOLTYPE                          --Դ����ѺƷ����
                ,SOURCECOLSUBTYPE                       --Դ����ѺƷС��
                ,SPECPURPBONDFLAG                       --�Ƿ�Ϊ�չ��������в�����������е�ծȯ
                ,QUALFLAGSTD                            --Ȩ�ط��ϸ��ʶ
                ,QUALFLAGFIRB                           --�����������ϸ��ʶ
                ,COLLATERALTYPESTD                      --Ȩ�ط�����ѺƷ����
                ,COLLATERALSDVSSTD                      --Ȩ�ط�����ѺƷϸ��
                ,COLLATERALTYPEIRB                      --����������ѺƷ����
                ,COLLATERALAMOUNT                        --��Ѻ�ܶ�
                ,CURRENCY                               --����
                ,STARTDATE                              --��ʼ����
                ,DUEDATE                                --��������
                ,ORIGINALMATURITY                       --ԭʼ����
                ,RESIDUALM                              --ʣ������
                ,INTEHAIRCUTSFLAG                       --���й����ۿ�ϵ����ʶ
                ,INTERNALHC                             --�ڲ��ۿ�ϵ��
                ,FCTYPE                                 --������ѺƷ����
                ,ABSFLAG                                --�ʲ�֤ȯ����ʶ
                ,RATINGDURATIONTYPE                     --������������
                ,FCISSUERATING                          --������ѺƷ���еȼ�
                ,FCISSUERTYPE                           --������ѺƷ���������
                ,FCISSUERSTATE                          --������ѺƷ������ע�����
                ,FCRESIDUALM                            --������ѺƷʣ������
                ,REVAFREQUENCY                          --�ع�Ƶ��
                ,GROUPID                                --������
                ,RCERating														  --�����˾���ע����ⲿ����
                ,flag
    )WITH TEMP_COLLATERAL1 AS(SELECT  T3.SERIALNO AS CONTRACTNO,MIN(NVL(T1.PUTOUTDATE,T3.PUTOUTDATE)) AS PUTOUTDATE,MAX(NVL(T1.ACTUALMATURITY,T3.MATURITY)) AS MATURITY,T3.BUSINESSTYPE
                               FROM RWA_DEV.NCM_BUSINESS_DUEBILL T1
                               INNER JOIN RWA_DEV.NCM_BUSINESS_CONTRACT T3
                               ON T1.RELATIVESERIALNO2=T3.SERIALNO        --�����Խ��Ϊ׼�����Թ�����ͬʱ����Ӧ�üӺ�ͬ����Ч����
                               AND T1.DATANO=T3.DATANO
                               --AND T3.BUSINESSTYPE || T3.BUSINESSNATURE <> '1030301005'   --�ų��ⲿת����
                               INNER JOIN RWA_DEV.NCM_BUSINESS_HISTORY T31
                               ON T1.SERIALNO=T31.SERIALNO
                               AND T31.DATANO=P_DATA_DT_STR
                               AND T31.BALANCE>0
                                          \*rwa_dev.brd_loan_nor t31
                               ON t1.serialno = t31.crdt_acct_no
                               AND t31.datano = P_DATA_DT_STR
                               AND t31.cur_bal > 0*\
                               WHERE T1.DATANO=P_DATA_DT_STR
                               --AND T1.BALANCE>0           --��Ч�ж�����
                               AND T1.BUSINESSTYPE IN ('10302010','10302015','10302020')  --���֣���Ʊ����Ϣ��Ϊ����
                               GROUP BY T3.SERIALNO,T3.BUSINESSTYPE
                             )
    SELECT
                 TO_DATE(T1.DATANO,'YYYYMMDD')                                                 AS DATADATE              --��������
                ,T1.DATANO                                                                     AS DATANO                --������ˮ��
                ,'PJ'||T1.SERIALNO                                                             AS COLLATERALID          --����ѺƷID
                ,'XD'                                                                          AS SSYSID                --ԴϵͳID
                ,''                                                                            AS SGUARCONTRACTID       --Դ������ͬID
                ,T1.SERIALNO                                                                   AS SCOLLATERALID         --Դ����ѺƷID
                ,CASE WHEN T2.BUSINESSTYPE='10302020' THEN '�й���ҵ���гжһ�Ʊ'
                      ELSE '��ҵ�жһ�Ʊ'
                 END                                                                           AS COLLATERALNAME        --����ѺƷ����
                ,NVL(T1.ACCEPTORBANKID,'XN-ZGSYYH')                                            AS ISSUERID              --������ID
                ,T1.HOLDERID                                                                   AS PROVIDERID            --�ṩ��ID
                ,'01'                                                                          AS CREDITRISKDATATYPE    --���÷�����������
                ,'060'                                                                         AS GUARANTEEWAY          --������ʽ
                ,'001004'                                                                      AS SOURCECOLTYPE         --Դ����ѺƷ����
                ,CASE WHEN T2.BUSINESSTYPE='10302020' THEN '001004002001'
                      ELSE '001004004001'
                 END                                                                           AS SOURCECOLSUBTYPE      --Դ����ѺƷС��
                ,'0'                                                                           AS SPECPURPBONDFLAG      --�Ƿ�Ϊ�չ��������в�����������е�ծȯ
                ,''                                                                            AS QUALFLAGSTD           --Ȩ�ط��ϸ��ʶ
                ,''                                                                            AS QUALFLAGFIRB          --�����������ϸ��ʶ
                ,''                                                                            AS COLLATERALTYPESTD     --Ȩ�ط�����ѺƷ����
                ,''                                                                            AS COLLATERALSDVSSTD     --Ȩ�ط�����ѺƷϸ��
                ,''                                                                            AS COLLATERALTYPEIRB     --����������ѺƷ����
                ,T1.BILLSUM                                                                    AS COLLATERALAMOUNT     --��Ѻ�ܶ�                              -
                ,T1.LCCURRENCY                                                                 AS CURRENCY              --����
                ,T2.PUTOUTDATE                                                                 AS STARTDATE             --��ʼ����
                ,T2.MATURITY                                                                   AS DUEDATE               --��������
                ,CASE WHEN (TO_DATE(T2.MATURITY,'YYYYMMDD')-TO_DATE(T2.PUTOUTDATE,'YYYYMMDD'))/365<0
                      THEN 0
                      ELSE (TO_DATE(T2.MATURITY,'YYYYMMDD')-TO_DATE(T2.PUTOUTDATE,'YYYYMMDD'))/365
                END                                                                            AS ORIGINALMATURITY      --ԭʼ����
                ,CASE WHEN (TO_DATE(T2.MATURITY,'YYYYMMDD')-TO_DATE(P_DATA_DT_STR,'YYYYMMDD'))/365<0
                      THEN 0
                      ELSE (TO_DATE(T2.MATURITY,'YYYYMMDD')-TO_DATE(P_DATA_DT_STR,'YYYYMMDD'))/365
                END                                                                            AS RESIDUALM             --ʣ������
                ,'0'                                                                           AS INTEHAIRCUTSFLAG      --���й����ۿ�ϵ����ʶ
                ,1                                                                             AS INTERNALHC            --�ڲ��ۿ�ϵ��
                ,''                                                                            AS FCTYPE                --������ѺƷ����
                ,'0'                                                                           AS ABSFLAG               --�ʲ�֤ȯ����ʶ
                ,''                                                                            AS RATINGDURATIONTYPE    --������������
                ,''                                                                            AS FCISSUERATING         --������ѺƷ���еȼ�
                ,'02'                                                                          AS FCISSUERTYPE          --������ѺƷ���������
                ,CASE WHEN T3.COUNTRYCODE<>'CHN' THEN '02'
                	    ELSE '01'
                 END                                                                           AS FCISSUERSTATE         --������ѺƷ������ע�����
                ,CASE WHEN (TO_DATE(T2.MATURITY,'YYYYMMDD')-TO_DATE(P_DATA_DT_STR,'YYYYMMDD')) / 365 < 0
                      THEN 0
                      ELSE (TO_DATE(T2.MATURITY,'YYYYMMDD')-TO_DATE(P_DATA_DT_STR,'YYYYMMDD')) / 365
                 END                                                                           AS FCRESIDUALM           --������ѺƷʣ������
                ,1                                                                             AS REVAFREQUENCY         --�ع�Ƶ��
                ,''                                                                            AS GROUPID               --������
                ,T5.RATINGRESULT                                                               AS RCERating             --�����˾���ע����ⲿ����
                ,'DZY|TXZT'
    FROM RWA_DEV.NCM_BILL_INFO T1
    INNER JOIN TEMP_COLLATERAL1 T2
    ON T1.OBJECTNO = T2.CONTRACTNO
    LEFT JOIN NCM_CUSTOMER_INFO T3
    ON T1.ACCEPTORBANKID=T3.CUSTOMERID
    AND T3.DATANO=P_DATA_DT_STR
    LEFT JOIN RWA.RWA_WP_COUNTRYRATING T5
    ON T3.COUNTRYCODE = T5.COUNTRYCODE
    AND	T5.ISINUSE = '1'
    WHERE T1.DATANO=P_DATA_DT_STR
    AND T1.OBJECTTYPE='BusinessContract'
    ;
    COMMIT;*/
    
    /*2.1 ���뱣֤����Ϣ������ѺƷ��-����ҵ��*/
    INSERT INTO RWA_DEV.RWA_XD_COLLATERAL(
                 DATADATE                                --��������
                ,DATANO                                 --������ˮ��
                ,COLLATERALID                           --����ѺƷID
                ,SSYSID                                 --ԴϵͳID
                ,SGUARCONTRACTID                        --Դ������ͬID
                ,SCOLLATERALID                          --Դ����ѺƷID
                ,COLLATERALNAME                         --����ѺƷ����
                ,ISSUERID                               --������ID
                ,PROVIDERID                             --�ṩ��ID
                ,CREDITRISKDATATYPE                     --���÷�����������
                ,GUARANTEEWAY                            --������ʽ
                ,SOURCECOLTYPE                          --Դ����ѺƷ����
                ,SOURCECOLSUBTYPE                       --Դ����ѺƷС��
                ,SPECPURPBONDFLAG                       --�Ƿ�Ϊ�չ��������в�����������е�ծȯ
                ,QUALFLAGSTD                            --Ȩ�ط��ϸ��ʶ
                ,QUALFLAGFIRB                           --�����������ϸ��ʶ
                ,COLLATERALTYPESTD                      --Ȩ�ط�����ѺƷ����
                ,COLLATERALSDVSSTD                      --Ȩ�ط�����ѺƷϸ��
                ,COLLATERALTYPEIRB                      --����������ѺƷ����
                ,COLLATERALAMOUNT                        --��Ѻ�ܶ�
                ,CURRENCY                               --����
                ,STARTDATE                              --��ʼ����
                ,DUEDATE                                --��������
                ,ORIGINALMATURITY                       --ԭʼ����
                ,RESIDUALM                              --ʣ������
                ,INTEHAIRCUTSFLAG                       --���й����ۿ�ϵ����ʶ
                ,INTERNALHC                             --�ڲ��ۿ�ϵ��
                ,FCTYPE                                 --������ѺƷ����
                ,ABSFLAG                                --�ʲ�֤ȯ����ʶ
                ,RATINGDURATIONTYPE                     --������������
                ,FCISSUERATING                          --������ѺƷ���еȼ�
                ,FCISSUERTYPE                           --������ѺƷ���������
                ,FCISSUERSTATE                          --������ѺƷ������ע�����
                ,FCRESIDUALM                            --������ѺƷʣ������
                ,REVAFREQUENCY                          --�ع�Ƶ��
                ,GROUPID                                --������
                ,RCERating                              --�����˾���ע����ⲿ����
                ,flag
    )WITH TEMP_COLLATERAL3 AS(SELECT T3.SERIALNO AS CONTRACTNO
                                    ,MIN(CASE WHEN T1.BUSINESSTYPE IN ('10201010','10202010','1035101010','1035101020','1035102010','1035102020','103550','1040101040') --����֤,����
                                              THEN NVL(T3.PUTOUTDATE,T3.OCCURDATE)   --����֤������ȡ��ͬ��ʼ����
                                              ELSE T1.PUTOUTDATE 
                                         END) AS PUTOUTDATE
                                    ,MAX(CASE WHEN T1.BUSINESSTYPE IN ('10201010','10202010','1035101010','1035101020','1035102010','1035102020','103550','1040101040') --����֤,����
                                              THEN T3.MATURITY
                                              ELSE T1.ACTUALMATURITY
                                         END) AS MATURITY
                                    ,MIN(T6.CREDITRISKDATATYPE) AS CREDITRISKDATATYPE
                               FROM RWA_DEV.NCM_BUSINESS_DUEBILL T1
                               INNER JOIN RWA_DEV.NCM_BUSINESS_TYPE T2
                               ON T1.BUSINESSTYPE=T2.TYPENO
                               AND T1.DATANO=T2.DATANO
                               AND T2.SORTNO NOT LIKE '30%' --�ų������ҵ��
                               INNER JOIN RWA_DEV.NCM_BUSINESS_CONTRACT T3
                               ON T1.RELATIVESERIALNO2=T3.SERIALNO        --�����Խ��Ϊ׼�����Թ�����ͬʱ����Ӧ�üӺ�ͬ����Ч����
                               AND T1.DATANO=T3.DATANO
                               --AND T3.BUSINESSTYPE || T3.BUSINESSNATURE <> '1030301005'   --�ų��ⲿת����
                               INNER JOIN RWA_DEV.NCM_BUSINESS_HISTORY T31
                               ON T1.SERIALNO=T31.SERIALNO
                               AND T31.DATANO=P_DATA_DT_STR
                               AND T31.BALANCE>0
                                          /*rwa_dev.brd_loan_nor t31
                               ON t1.serialno = t31.crdt_acct_no
                               AND t31.datano = P_DATA_DT_STR
                               AND t31.cur_bal > 0*/
                               INNER JOIN RWA_DEV.RWA_XD_CONTRACT T6
    													 ON T1.RELATIVESERIALNO2 = T6.CONTRACTID
    													 AND T1.DATANO = T6.DATANO
                               WHERE T1.DATANO=P_DATA_DT_STR
                               --AND T1.BALANCE>0           --��Ч�ж�����
                               AND T1.SERIALNO<>'BD2014110400000001'
                               AND T1.BUSINESSTYPE NOT IN ('1040101010','1040101020','1040101030','1040102010','1040102020','1040102040','10303010'
                                   ,'1040105060','1040201010','1040201020','1040202010','105010','11105010','11105020','10302010','10302015','10302020')  --�ų�ͬҵ���ع���Ͷ�ʣ�ί�д���ҵ��,���ֺ�ת��Ҳ�ų�����Ʊ����Ϣ��Ϊ����
                               GROUP BY T3.SERIALNO
                             )
    SELECT
                TO_DATE(T1.DATANO,'YYYYMMDD')                                                  AS DATADATE              --��������
                ,T1.DATANO                                                                     AS DATANO                --������ˮ��
                ,'HT'||T1.SERIALNO||T3.BAILCURRENCY                                            AS COLLATERALID          --����ѺƷID
                ,'XD'                                                                          AS SSYSID                --ԴϵͳID
                ,T1.SERIALNO                                                                   AS SGUARCONTRACTID       --Դ������ͬID
                ,T1.SERIALNO                                                                   AS SCOLLATERALID         --Դ����ѺƷID
                ,'��֤��'                                                                      AS COLLATERALNAME        --����ѺƷ����
                ,''                                                                            AS ISSUERID              --������ID
                ,T1.CUSTOMERID                                                                 AS PROVIDERID            --�ṩ��ID
                ,T2.CREDITRISKDATATYPE                                                         AS CREDITRISKDATATYPE    --���÷�����������
                ,'060'                                                                         AS GUARANTEEWAY          --������ʽ
                ,'001001'                                                                      AS SOURCECOLTYPE         --Դ����ѺƷ����
                ,'001001003001'                                                                AS SOURCECOLSUBTYPE      --Դ����ѺƷС��
                ,'0'                                                                           AS SPECPURPBONDFLAG      --�Ƿ�Ϊ�չ��������в�����������е�ծȯ
                ,'1'                                                                           AS QUALFLAGSTD           --Ȩ�ط��ϸ��ʶ
                ,'1'                                                                           AS QUALFLAGFIRB          --�����������ϸ��ʶ
                ,'030103'                                                                      AS COLLATERALTYPESTD     --Ȩ�ط�����ѺƷ����
                ,'01'                                                                          AS COLLATERALSDVSSTD     --Ȩ�ط�����ѺƷϸ��
                ,'030201'                                                                      AS COLLATERALTYPEIRB     --����������ѺƷ����
                ,T3.BAILBALANCE                                                                AS COLLATERALAMOUNT     --��Ѻ�ܶ�                              -
                ,NVL(T3.BAILCURRENCY,'CNY')                                                    AS CURRENCY              --����
                ,T2.PUTOUTDATE                                                                 AS STARTDATE             --��ʼ����
                ,T2.MATURITY                                                                   AS DUEDATE               --��������
                ,CASE WHEN (TO_DATE(T2.MATURITY,'YYYYMMDD')-TO_DATE(T2.PUTOUTDATE,'YYYYMMDD'))/365<0
                      THEN 0
                      ELSE (TO_DATE(T2.MATURITY,'YYYYMMDD')-TO_DATE(T2.PUTOUTDATE,'YYYYMMDD'))/365
                END                                                                            AS ORIGINALMATURITY      --ԭʼ����
                ,CASE WHEN (TO_DATE(T2.MATURITY,'YYYYMMDD')-TO_DATE(P_DATA_DT_STR,'YYYYMMDD'))/365<0
                      THEN 0
                      ELSE (TO_DATE(T2.MATURITY,'YYYYMMDD')-TO_DATE(P_DATA_DT_STR,'YYYYMMDD'))/365
                END                                                                            AS RESIDUALM             --ʣ������
                ,'0'                                                                           AS INTEHAIRCUTSFLAG      --���й����ۿ�ϵ����ʶ
                ,1                                                                             AS INTERNALHC            --�ڲ��ۿ�ϵ��
                ,'01'                                                                          AS FCTYPE                --������ѺƷ����
                ,'0'                                                                           AS ABSFLAG               --�ʲ�֤ȯ����ʶ
                ,''                                                                            AS RATINGDURATIONTYPE    --������������
                ,''                                                                            AS FCISSUERATING         --������ѺƷ���еȼ�
                ,NULL                                                                          AS FCISSUERTYPE          --������ѺƷ���������
                ,'01'                                                                            AS FCISSUERSTATE         --������ѺƷ������ע�����
                ,''                                                                            AS FCRESIDUALM           --������ѺƷʣ������
                ,1                                                                             AS REVAFREQUENCY         --�ع�Ƶ��
                ,''                                                                            AS GROUPID               --������
                ,NULL                                                                          AS RCERating             --�����˾���ע����ⲿ����
                ,'BZJ|PT'
    FROM  RWA_DEV.NCM_BUSINESS_CONTRACT T1
    INNER JOIN TEMP_COLLATERAL3 T2
    ON T1.SERIALNO=T2.CONTRACTNO
    INNER JOIN RWA_DEV.RWA_TEMP_BAIL1 T3
    ON T1.SERIALNO=T3.CONTRACTNO
    AND T3.BAILBALANCE<9999999999999     --�ų�������
    --AND T3.ISMAX='1' --�����һ�ڵ��߼������������BAIL2����Ϊ����BAIL1����Ҫ�������־  modify by yushuangjiang
    WHERE T1.DATANO=P_DATA_DT_STR
    ;
    COMMIT;
    
    /*2.2 ���뱣֤����Ϣ������ѺƷ��(���ڴ���-΢����)*/  
    /*INSERT INTO RWA_DEV.RWA_XD_COLLATERAL(
                 DATADATE                                --��������
                ,DATANO                                 --������ˮ��
                ,COLLATERALID                           --����ѺƷID
                ,SSYSID                                 --ԴϵͳID
                ,SGUARCONTRACTID                        --Դ������ͬID
                ,SCOLLATERALID                          --Դ����ѺƷID
                ,COLLATERALNAME                         --����ѺƷ����
                ,ISSUERID                               --������ID
                ,PROVIDERID                             --�ṩ��ID
                ,CREDITRISKDATATYPE                     --���÷�����������
                ,GUARANTEEWAY                            --������ʽ
                ,SOURCECOLTYPE                          --Դ����ѺƷ����
                ,SOURCECOLSUBTYPE                       --Դ����ѺƷС��
                ,SPECPURPBONDFLAG                       --�Ƿ�Ϊ�չ��������в�����������е�ծȯ
                ,QUALFLAGSTD                            --Ȩ�ط��ϸ��ʶ
                ,QUALFLAGFIRB                           --�����������ϸ��ʶ
                ,COLLATERALTYPESTD                      --Ȩ�ط�����ѺƷ����
                ,COLLATERALSDVSSTD                      --Ȩ�ط�����ѺƷϸ��
                ,COLLATERALTYPEIRB                      --����������ѺƷ����
                ,COLLATERALAMOUNT                        --��Ѻ�ܶ�
                ,CURRENCY                               --����
                ,STARTDATE                              --��ʼ����
                ,DUEDATE                                --��������
                ,ORIGINALMATURITY                       --ԭʼ����
                ,RESIDUALM                              --ʣ������
                ,INTEHAIRCUTSFLAG                       --���й����ۿ�ϵ����ʶ
                ,INTERNALHC                             --�ڲ��ۿ�ϵ��
                ,FCTYPE                                 --������ѺƷ����
                ,ABSFLAG                                --�ʲ�֤ȯ����ʶ
                ,RATINGDURATIONTYPE                     --������������
                ,FCISSUERATING                          --������ѺƷ���еȼ�
                ,FCISSUERTYPE                           --������ѺƷ���������
                ,FCISSUERSTATE                          --������ѺƷ������ע�����
                ,FCRESIDUALM                            --������ѺƷʣ������
                ,REVAFREQUENCY                          --�ع�Ƶ��
                ,GROUPID                                --������
                ,RCERating                              --�����˾���ע����ⲿ����
                ,flag
    )WITH TEMP_COLLATERAL3 AS(SELECT T1.RELATIVESERIALNO2 AS CONTRACTNO,MIN(NVL(T1.PUTOUTDATE,CASE WHEN T1.BUSINESSTYPE IN ('10201010','10202010') THEN NVL(T3.OCCURDATE,T3.PUTOUTDATE) ELSE T3.PUTOUTDATE END)) AS PUTOUTDATE
                                     ,MAX(NVL(T1.ACTUALMATURITY,T3.MATURITY)) AS MATURITY,MIN(T6.CREDITRISKDATATYPE) AS CREDITRISKDATATYPE
                               FROM RWA_DEV.NCM_BUSINESS_DUEBILL T1
                               INNER JOIN RWA_DEV.NCM_BUSINESS_TYPE T2
                               ON T1.BUSINESSTYPE=T2.TYPENO
                               AND T1.DATANO=T2.DATANO
                               AND T2.SORTNO NOT LIKE '30%' --�ų������ҵ��
                               INNER JOIN RWA_DEV.NCM_BUSINESS_CONTRACT T3
                               ON T1.RELATIVESERIALNO2=T3.SERIALNO        --�����Խ��Ϊ׼�����Թ�����ͬʱ����Ӧ�üӺ�ͬ����Ч����
                               AND T1.DATANO=T3.DATANO
                               --AND T3.BUSINESSTYPE || T3.BUSINESSNATURE <> '1030301005'   --�ų��ⲿת����
                               INNER JOIN RWA_DEV.NCM_BUSINESS_HISTORY T31
                               ON T1.SERIALNO=T31.SERIALNO
                               AND T31.DATANO=P_DATA_DT_STR
                               AND (T31.OVERDUEBALANCE+T31.DULLBALANCE+T31.BADBALANCE)>0   --ȡ�����ڵļ�¼
                                         \* rwa_dev.brd_loan_nor t31
                               ON t1.serialno = t31.crdt_acct_no
                               AND t31.datano = P_DATA_DT_STR
                               AND t31.sbjt_cd = '13100001' --΢���������ڴ���*\
                               INNER JOIN RWA_DEV.RWA_XD_CONTRACT T6
    													 ON T1.RELATIVESERIALNO2 = T6.CONTRACTID
    													 AND T1.DATANO = T6.DATANO
                               WHERE  T1.DATANO=P_DATA_DT_STR
                               AND T1.BUSINESSTYPE='11103030'  --ֻȡ΢����ҵ��
                               GROUP BY T1.RELATIVESERIALNO2
                             )
    SELECT
                TO_DATE(T1.DATANO,'YYYYMMDD')                                                  AS DATADATE              --��������
                ,T1.DATANO                                                                     AS DATANO                --������ˮ��
                ,'HT'||T1.SERIALNO||T3.BAILCURRENCY                                            AS COLLATERALID          --����ѺƷID
                ,'XD'                                                                          AS SSYSID                --ԴϵͳID
                ,T1.SERIALNO                                                                   AS SGUARCONTRACTID       --Դ������ͬID
                ,T1.SERIALNO                                                                   AS SCOLLATERALID         --Դ����ѺƷID
                ,'��֤��'                                                                      AS COLLATERALNAME        --����ѺƷ����
                ,''                                                                            AS ISSUERID              --������ID
                ,T1.CUSTOMERID                                                                 AS PROVIDERID            --�ṩ��ID
                ,T2.CREDITRISKDATATYPE                                                         AS CREDITRISKDATATYPE    --���÷�����������
                ,'060'                                                                         AS GUARANTEEWAY          --������ʽ
                ,'001001'                                                                      AS SOURCECOLTYPE         --Դ����ѺƷ����
                ,'001001003001'                                                                AS SOURCECOLSUBTYPE      --Դ����ѺƷС��
                ,'0'                                                                           AS SPECPURPBONDFLAG      --�Ƿ�Ϊ�չ��������в�����������е�ծȯ
                ,'1'                                                                           AS QUALFLAGSTD           --Ȩ�ط��ϸ��ʶ
                ,'1'                                                                           AS QUALFLAGFIRB          --�����������ϸ��ʶ
                ,'030103'                                                                      AS COLLATERALTYPESTD     --Ȩ�ط�����ѺƷ����
                ,'01'                                                                          AS COLLATERALSDVSSTD     --Ȩ�ط�����ѺƷϸ��
                ,'030201'                                                                      AS COLLATERALTYPEIRB     --����������ѺƷ����
                ,T3.BAILBALANCE                                                                AS COLLATERALAMOUNT     --��Ѻ�ܶ�                              -
                ,NVL(T3.BAILCURRENCY,'CNY')                                                    AS CURRENCY              --����
                ,T2.PUTOUTDATE                                                                 AS STARTDATE             --��ʼ����
                ,T2.MATURITY                                                                   AS DUEDATE               --��������
                ,CASE WHEN (TO_DATE(T2.MATURITY,'YYYYMMDD')-TO_DATE(T2.PUTOUTDATE,'YYYYMMDD'))/365<0
                      THEN 0
                      ELSE (TO_DATE(T2.MATURITY,'YYYYMMDD')-TO_DATE(T2.PUTOUTDATE,'YYYYMMDD'))/365
                END                                                                            AS ORIGINALMATURITY      --ԭʼ����
                ,CASE WHEN (TO_DATE(T2.MATURITY,'YYYYMMDD')-TO_DATE(P_DATA_DT_STR,'YYYYMMDD'))/365<0
                      THEN 0
                      ELSE (TO_DATE(T2.MATURITY,'YYYYMMDD')-TO_DATE(P_DATA_DT_STR,'YYYYMMDD'))/365
                END                                                                            AS RESIDUALM             --ʣ������
                ,'0'                                                                           AS INTEHAIRCUTSFLAG      --���й����ۿ�ϵ����ʶ
                ,1                                                                             AS INTERNALHC            --�ڲ��ۿ�ϵ��
                ,'01'                                                                          AS FCTYPE                --������ѺƷ����
                ,'0'                                                                           AS ABSFLAG               --�ʲ�֤ȯ����ʶ
                ,''                                                                            AS RATINGDURATIONTYPE    --������������
                ,''                                                                            AS FCISSUERATING         --������ѺƷ���еȼ�
                ,NULL                                                                          AS FCISSUERTYPE          --������ѺƷ���������
                ,'01'                                                                            AS FCISSUERSTATE         --������ѺƷ������ע�����
                ,''                                                                            AS FCRESIDUALM           --������ѺƷʣ������
                ,1                                                                             AS REVAFREQUENCY         --�ع�Ƶ��
                ,''                                                                            AS GROUPID               --������
                ,NULL                                                                          AS RCERating             --�����˾���ע����ⲿ����
                ,'BZJ|YQWLD'
    FROM  RWA_DEV.NCM_BUSINESS_CONTRACT T1
    INNER JOIN TEMP_COLLATERAL3 T2
    ON T1.SERIALNO=T2.CONTRACTNO
    INNER JOIN RWA_DEV.RWA_TEMP_BAIL1 T3
    ON T1.SERIALNO=T3.CONTRACTNO
    --AND T3.ISMAX='1'    --�����һ�ڵ��߼������������BAIL2����Ϊ����BAIL1����Ҫ�������־  modify by yushuangjiang
    WHERE T1.DATANO=P_DATA_DT_STR
    AND NOT EXISTS(SELECT 1 FROM RWA_DEV.RWA_XD_COLLATERAL T6 WHERE 'HT'||T1.SERIALNO||T3.BAILCURRENCY=T6.COLLATERALID)
    ;
    COMMIT;*/
    
    /*2.3 ���뱣֤����Ϣ������ѺƷ��-��������ҵ��*/
    INSERT INTO RWA_DEV.RWA_XD_COLLATERAL(
                 DATADATE                                --��������
                ,DATANO                                 --������ˮ��
                ,COLLATERALID                           --����ѺƷID
                ,SSYSID                                 --ԴϵͳID
                ,SGUARCONTRACTID                        --Դ������ͬID
                ,SCOLLATERALID                          --Դ����ѺƷID
                ,COLLATERALNAME                         --����ѺƷ����
                ,ISSUERID                               --������ID
                ,PROVIDERID                             --�ṩ��ID
                ,CREDITRISKDATATYPE                     --���÷�����������
                ,GUARANTEEWAY                            --������ʽ
                ,SOURCECOLTYPE                          --Դ����ѺƷ����
                ,SOURCECOLSUBTYPE                       --Դ����ѺƷС��
                ,SPECPURPBONDFLAG                       --�Ƿ�Ϊ�չ��������в�����������е�ծȯ
                ,QUALFLAGSTD                            --Ȩ�ط��ϸ��ʶ
                ,QUALFLAGFIRB                           --�����������ϸ��ʶ
                ,COLLATERALTYPESTD                      --Ȩ�ط�����ѺƷ����
                ,COLLATERALSDVSSTD                      --Ȩ�ط�����ѺƷϸ��
                ,COLLATERALTYPEIRB                      --����������ѺƷ����
                ,COLLATERALAMOUNT                        --��Ѻ�ܶ�
                ,CURRENCY                               --����
                ,STARTDATE                              --��ʼ����
                ,DUEDATE                                --��������
                ,ORIGINALMATURITY                       --ԭʼ����
                ,RESIDUALM                              --ʣ������
                ,INTEHAIRCUTSFLAG                       --���й����ۿ�ϵ����ʶ
                ,INTERNALHC                             --�ڲ��ۿ�ϵ��
                ,FCTYPE                                 --������ѺƷ����
                ,ABSFLAG                                --�ʲ�֤ȯ����ʶ
                ,RATINGDURATIONTYPE                     --������������
                ,FCISSUERATING                          --������ѺƷ���еȼ�
                ,FCISSUERTYPE                           --������ѺƷ���������
                ,FCISSUERSTATE                          --������ѺƷ������ע�����
                ,FCRESIDUALM                            --������ѺƷʣ������
                ,REVAFREQUENCY                          --�ع�Ƶ��
                ,GROUPID                                --������
                ,RCERating                              --�����˾���ע����ⲿ����
                ,flag
    )WITH TEMP_COLLATERAL3 AS(SELECT T3.SERIALNO AS CONTRACTNO
                                    ,MIN(CASE WHEN T1.BUSINESSTYPE IN ('10201010','10202010','1035101010','1035101020','1035102010','1035102020','103550','1040101040') --����֤,����
                                              THEN NVL(T3.PUTOUTDATE,T3.OCCURDATE)   --����֤������ȡ��ͬ��ʼ����
                                              ELSE T1.PUTOUTDATE 
                                         END) AS PUTOUTDATE
                                    ,MAX(CASE WHEN T1.BUSINESSTYPE IN ('10201010','10202010','1035101010','1035101020','1035102010','1035102020','103550','1040101040') --����֤,����
                                              THEN T3.MATURITY
                                              ELSE T1.ACTUALMATURITY
                                         END) AS MATURITY
                                    ,MIN(T6.CREDITRISKDATATYPE) AS CREDITRISKDATATYPE
                               FROM RWA_DEV.NCM_BUSINESS_DUEBILL T1
                               INNER JOIN RWA_DEV.NCM_BUSINESS_TYPE T2
                               ON T1.BUSINESSTYPE=T2.TYPENO
                               AND T1.DATANO=T2.DATANO
                               AND T2.SORTNO NOT LIKE '30%' --�ų������ҵ��
                               INNER JOIN RWA_DEV.NCM_BUSINESS_CONTRACT T3
                               ON T1.RELATIVESERIALNO2=T3.SERIALNO        --�����Խ��Ϊ׼�����Թ�����ͬʱ����Ӧ�üӺ�ͬ����Ч����
                               AND T1.DATANO=T3.DATANO
                               INNER JOIN BRD_LOAN_NOR T4 --��������
                               ON  T4.DATANO = p_data_dt_str
                               AND T4.CRDT_ACCT_NO = T1.SERIALNO
                               AND substr(t4.sbjt_cd,1,4) = '1310' --��Ŀ���
                               AND T4.SBJT_CD != '13100001' --���в���΢���������ڴ���  
                               INNER JOIN RWA_DEV.RWA_XD_CONTRACT T6
                               ON T1.RELATIVESERIALNO2 = T6.CONTRACTID
                               AND T1.DATANO = T6.DATANO
                               WHERE  T1.DATANO=P_DATA_DT_STR
                               GROUP BY T3.SERIALNO
                             )
    SELECT
                TO_DATE(T1.DATANO,'YYYYMMDD')                                                  AS DATADATE              --��������
                ,T1.DATANO                                                                     AS DATANO                --������ˮ��
                ,'HT'||T1.SERIALNO||T3.BAILCURRENCY                                            AS COLLATERALID          --����ѺƷID
                ,'XD'                                                                          AS SSYSID                --ԴϵͳID
                ,T1.SERIALNO                                                                   AS SGUARCONTRACTID       --Դ������ͬID
                ,T1.SERIALNO                                                                   AS SCOLLATERALID         --Դ����ѺƷID
                ,'��֤��'                                                                      AS COLLATERALNAME        --����ѺƷ����
                ,''                                                                            AS ISSUERID              --������ID
                ,T1.CUSTOMERID                                                                 AS PROVIDERID            --�ṩ��ID
                ,T2.CREDITRISKDATATYPE                                                         AS CREDITRISKDATATYPE    --���÷�����������
                ,'060'                                                                         AS GUARANTEEWAY          --������ʽ
                ,'001001'                                                                      AS SOURCECOLTYPE         --Դ����ѺƷ����
                ,'001001003001'                                                                AS SOURCECOLSUBTYPE      --Դ����ѺƷС��
                ,'0'                                                                           AS SPECPURPBONDFLAG      --�Ƿ�Ϊ�չ��������в�����������е�ծȯ
                ,'1'                                                                           AS QUALFLAGSTD           --Ȩ�ط��ϸ��ʶ
                ,'1'                                                                           AS QUALFLAGFIRB          --�����������ϸ��ʶ
                ,'030103'                                                                      AS COLLATERALTYPESTD     --Ȩ�ط�����ѺƷ����
                ,'01'                                                                          AS COLLATERALSDVSSTD     --Ȩ�ط�����ѺƷϸ��
                ,'030201'                                                                      AS COLLATERALTYPEIRB     --����������ѺƷ����
                ,T3.BAILBALANCE                                                                AS COLLATERALAMOUNT     --��Ѻ�ܶ�                              -
                ,NVL(T3.BAILCURRENCY,'CNY')                                                    AS CURRENCY              --����
                ,T2.PUTOUTDATE                                                                 AS STARTDATE             --��ʼ����
                ,T2.MATURITY                                                                   AS DUEDATE               --��������
                ,CASE WHEN (TO_DATE(T2.MATURITY,'YYYYMMDD')-TO_DATE(T2.PUTOUTDATE,'YYYYMMDD'))/365<0
                      THEN 0
                      ELSE (TO_DATE(T2.MATURITY,'YYYYMMDD')-TO_DATE(T2.PUTOUTDATE,'YYYYMMDD'))/365
                END                                                                            AS ORIGINALMATURITY      --ԭʼ����
                ,CASE WHEN (TO_DATE(T2.MATURITY,'YYYYMMDD')-TO_DATE(P_DATA_DT_STR,'YYYYMMDD'))/365<0
                      THEN 0
                      ELSE (TO_DATE(T2.MATURITY,'YYYYMMDD')-TO_DATE(P_DATA_DT_STR,'YYYYMMDD'))/365
                END                                                                            AS RESIDUALM             --ʣ������
                ,'0'                                                                           AS INTEHAIRCUTSFLAG      --���й����ۿ�ϵ����ʶ
                ,1                                                                             AS INTERNALHC            --�ڲ��ۿ�ϵ��
                ,'01'                                                                          AS FCTYPE                --������ѺƷ����
                ,'0'                                                                           AS ABSFLAG               --�ʲ�֤ȯ����ʶ
                ,''                                                                            AS RATINGDURATIONTYPE    --������������
                ,''                                                                            AS FCISSUERATING         --������ѺƷ���еȼ�
                ,NULL                                                                          AS FCISSUERTYPE          --������ѺƷ���������
                ,'01'                                                                            AS FCISSUERSTATE         --������ѺƷ������ע�����
                ,''                                                                            AS FCRESIDUALM           --������ѺƷʣ������
                ,1                                                                             AS REVAFREQUENCY         --�ع�Ƶ��
                ,''                                                                            AS GROUPID               --������
                ,NULL                                                                          AS RCERating             --�����˾���ע����ⲿ����
                ,'BZJ|YQ'
    FROM  RWA_DEV.NCM_BUSINESS_CONTRACT T1
    INNER JOIN TEMP_COLLATERAL3 T2
    ON T1.SERIALNO=T2.CONTRACTNO
    INNER JOIN RWA_DEV.RWA_TEMP_BAIL1 T3
    ON T1.SERIALNO=T3.CONTRACTNO
    AND T3.BAILBALANCE<9999999999999
    --AND T3.ISMAX='1'  --�����һ�ڵ��߼������������BAIL2����Ϊ����BAIL1����Ҫ�������־  modify by yushuangjiang
    WHERE T1.DATANO=P_DATA_DT_STR
    AND NOT EXISTS(SELECT 1 FROM RWA_DEV.RWA_XD_COLLATERAL T6 WHERE 'HT'||T1.SERIALNO||T3.BAILCURRENCY=T6.COLLATERALID)
    ;
    COMMIT;
    
    --���±�¶��Ĵ����ֵ��,Ҫ�õ�����ѺƷ�ܶ���Է����������
    UPDATE RWA_DEV.RWA_XD_EXPOSURE T1
       SET LTV=(SELECT T1.ASSETBALANCE/T4.CollateralAmount
                FROM ( SELECT T2.CONTRACTID AS ,SUM(T3.CollateralAmount) AS CollateralAmount
                       FROM RWA_DEV.RWA_XD_CMRELEVENCE T2
                       INNER JOIN RWA_DEV.RWA_XD_COLLATERAL T3
                       ON T2.MITIGATIONID=T3.COLLATERALID
                       AND T2.DATANO=p_data_dt_str
                       AND T2.DATANO=T3.DATANO
                       GROUP BY T2.CONTRACTID
                     )T4
                WHERE T1.CONTRACTID=T4.CONTRACTID
                AND T4.CollateralAmount<>0)
    WHERE T1.BUSINESSTYPEID='11103040'
    AND T1.DATANO=p_data_dt_str;
    COMMIT;

    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_XD_COLLATERAL',cascade => true);

    /*Ŀ�������ͳ��*/
    SELECT COUNT(1) INTO v_count1 FROM RWA_DEV.RWA_XD_COLLATERAL;

    p_po_rtncode := '1';
    p_po_rtnmsg  := '�ɹ�'||'-'||v_count1;
    --�����쳣
    EXCEPTION
    WHEN OTHERS THEN
         --Dbms_output.Put_line('������,�������Ϊ:'||sqlcode||';������ϢΪ:'||sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace);
          ROLLBACK;
          p_po_rtncode := sqlcode;
          p_po_rtnmsg  := '�Ŵ�ϵͳ-����ѺƷ(pro_rwa_xd_collateral)ETLת��ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;
         RETURN;

END PRO_RWA_XD_COLLATERAL;
/

