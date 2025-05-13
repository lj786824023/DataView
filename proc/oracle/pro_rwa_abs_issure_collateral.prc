CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_ABS_ISSURE_COLLATERAL(
                             P_DATA_DT_STR  IN  VARCHAR2,    --��������
                             P_PO_RTNCODE  OUT  VARCHAR2,    --���ر��
                            P_PO_RTNMSG    OUT  VARCHAR2    --��������
)
  /*
    �洢��������:RWA_DEV.PRO_RWA_ABS_ISSURE_COLLATERAL
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
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_ABS_ISSURE_COLLATERAL';
  --�����ж�ֵ����
  v_count1 INTEGER;
  --�����쳣����
  v_raise EXCEPTION;

  BEGIN
    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*�����ȫ�����ݼ��������Ŀ���*/
    --1.���Ŀ����е�ԭ�м�¼
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_ABS_ISSURE_COLLATERAL';

    /*��Ч����º�ͬ��Ӧ�ĵ���ѺƷ��Ϣ*/
    INSERT INTO RWA_DEV.RWA_ABS_ISSURE_COLLATERAL(
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
    )WITH TEMP_COLLATERAL1 AS(SELECT T3.SERIALNO AS CONTRACTNO,MIN(NVL(T1.PUTOUTDATE,CASE WHEN T1.BUSINESSTYPE IN ('10201010','10202010') THEN NVL(T3.OCCURDATE,T3.PUTOUTDATE) ELSE T3.PUTOUTDATE END)) AS PUTOUTDATE
                                    ,MAX(NVL(T1.ACTUALMATURITY,T3.MATURITY)) AS MATURITY, MIN(T2.ATTRIBUTE1) AS ATTRIBUTE1
                               FROM RWA_DEV.NCM_BUSINESS_DUEBILL T1
                               INNER JOIN RWA_DEV.NCM_BUSINESS_TYPE T2
                               ON T1.BUSINESSTYPE=T2.TYPENO
                               AND T1.DATANO=T2.DATANO
                               AND T2.SORTNO NOT LIKE '3%'  --�ų������ҵ��
                               INNER JOIN RWA_DEV.NCM_BUSINESS_CONTRACT T3
                               ON T1.RELATIVESERIALNO2=T3.SERIALNO        --�����Խ��Ϊ׼�����Թ�����ͬʱ����Ӧ�üӺ�ͬ����Ч����
                               AND T1.DATANO=T3.DATANO
                               INNER JOIN RWA.RWA_WS_ABS_ISSUE_UNDERASSET RWAIU
                               ON T1.SERIALNO=RWAIU.HTBH
                               AND RWAIU.DATADATE = TO_DATE(P_DATA_DT_STR,'YYYYMMDD')
                               INNER JOIN  RWA.RWA_WP_DATASUPPLEMENT RWD
                               ON          RWAIU.SUPPORGID=RWD.ORGID
                               AND         RWD.DATADATE=TO_DATE(P_DATA_DT_STR,'YYYYMMDD')
                               AND         RWD.SUPPTMPLID='M-0133'
                               AND         RWD.SUBMITFLAG='1'
                               INNER JOIN 	RWA_DEV.RWA_ABS_ISSURE_CONTRACT T4
    													 ON					'ABS'||T3.SERIALNO = T4.CONTRACTID
                               WHERE T1.DATANO=P_DATA_DT_STR
                               GROUP BY T3.SERIALNO
                             )
    ,TEMP_RELATIVE AS (SELECT T5.GUARANTYID,MIN(T1.PUTOUTDATE) AS PUTOUTDATE,MAX(T1.MATURITY) AS MATURITY,MIN(ATTRIBUTE1) AS ATTRIBUTE1
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
                ,'ABS'||T1.GUARANTYID                                                         AS COLLATERALID          --����ѺƷID
                ,'ABS'                                                                        AS SSYSID                --ԴϵͳID
                ,''                                                                           AS SGUARCONTRACTID       --Դ������ͬID
                ,'ABS'||T1.GUARANTYID                                                         AS SCOLLATERALID         --Դ����ѺƷID
                ,T6.ITEMNAME                                                                  AS COLLATERALNAME        --����ѺƷ����
                ,CASE WHEN (T1.GUARANTYTYPEID LIKE '001003%' OR T1.GUARANTYTYPEID LIKE '001004%') --������ѺƷ��Ҫ������
                      THEN T3.OPENBANKNO
                      ELSE ''
                 END                                                                           AS ISSUERID              --������ID
                ,T1.CLRERID                                                                    AS PROVIDERID            --�ṩ��ID
                ,CASE WHEN T2.ATTRIBUTE1='1' THEN '01'
                      ELSE '02'
                 END                                                                           AS CREDITRISKDATATYPE    --���÷�����������
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
                ,T1.AFFIRMCURRENCY                                                             AS CURRENCY              --����
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
    FROM RWA_DEV.NCM_GUARANTY_INFO T1
    INNER JOIN TEMP_RELATIVE T2
    ON T1.GUARANTYID = T2.GUARANTYID
    LEFT JOIN NCM_ASSET_FINANCE T3
    ON T1.GUARANTYID=T3.GUARANTYID
    AND T3.DATANO=P_DATA_DT_STR
    LEFT JOIN	RWA_DEV.NCM_COL_PARAM T4
    ON T1.GUARANTYTYPEID = T4.GUARANTYTYPE
    AND	T4.DATANO = p_data_dt_str
    LEFT JOIN RWA.RWA_WP_COUNTRYRATING T5
    ON T3.BONDPUBLISHCOUNTRY = T5.COUNTRYCODE
    AND	T5.ISINUSE = '1'
    LEFT JOIN RWA.CODE_LIBRARY T6
    ON T1.GUARANTYTYPE=T6.ITEMNO
    AND T6.CODENO='GuarantyList'
    WHERE T1.DATANO=P_DATA_DT_STR
    AND T1.GUARANTYTYPE NOT IN('004001004001','004001005001','004001006001','004001006002')     --����֤����������֤,�����Ա������������Ա��� ����Ϊ��֤
    ;
    COMMIT;

     /*���뱣֤����Ϣ������ѺƷ��*/
     INSERT INTO RWA_DEV.RWA_ABS_ISSURE_COLLATERAL(
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
    )
    SELECT
                T2.DATADATE									                                                   AS DATADATE              --��������
                ,T2.DATANO                                                                     AS DATANO                --������ˮ��
                ,'ABS'||T1.CONTRACTNO                                                          AS COLLATERALID          --����ѺƷID
                ,'ABS'                                                                         AS SSYSID                --ԴϵͳID
                ,'ABS'||T1.CONTRACTNO                                                          AS SGUARCONTRACTID       --Դ������ͬID
                ,'ABS'||T1.CONTRACTNO                                                          AS SCOLLATERALID         --Դ����ѺƷID
                ,'��֤��'                                                                      AS COLLATERALNAME        --����ѺƷ����
                ,''                                                                            AS ISSUERID              --������ID
                ,T2.CLIENTID	                                                                 AS PROVIDERID            --�ṩ��ID
                ,T2.CREDITRISKDATATYPE                                               					 AS CREDITRISKDATATYPE    --���÷�����������
                ,'060'                                                                         AS GUARANTEEWAY          --������ʽ
                ,'001001'                                                                      AS SOURCECOLTYPE         --Դ����ѺƷ����
                ,'001001003001'                                                                AS SOURCECOLSUBTYPE      --Դ����ѺƷС��
                ,'0'                                                                           AS SPECPURPBONDFLAG      --�Ƿ�Ϊ�չ��������в�����������е�ծȯ
                ,'01'                                                                          AS QUALFLAGSTD           --Ȩ�ط��ϸ��ʶ
                ,'01'                                                                          AS QUALFLAGFIRB          --�����������ϸ��ʶ
                ,'030103'                                                                      AS COLLATERALTYPESTD     --Ȩ�ط�����ѺƷ����
                ,'01'                                                                          AS COLLATERALSDVSSTD     --Ȩ�ط�����ѺƷϸ��
                ,'030201'                                                                      AS COLLATERALTYPEIRB     --����������ѺƷ����
                ,T1.BAILBALANCE                                                                AS COLLATERALAMOUNT     --��Ѻ�ܶ�                              -
                ,T1.BAILCURRENCY					                                                     AS CURRENCY              --����
                ,T2.STARTDATE	                                                                 AS STARTDATE             --��ʼ����
                ,T2.DUEDATE	                                                                   AS DUEDATE               --��������
                ,T2.ORIGINALMATURITY                                                           AS ORIGINALMATURITY      --ԭʼ����
                ,T2.RESIDUALM                                                                  AS RESIDUALM             --ʣ������
                ,'0'                                                                           AS INTEHAIRCUTSFLAG      --���й����ۿ�ϵ����ʶ
                ,1                                                                             AS INTERNALHC            --�ڲ��ۿ�ϵ��
                ,'01'                                                                          AS FCTYPE                --������ѺƷ����
                ,'0'                                                                           AS ABSFLAG               --�ʲ�֤ȯ����ʶ
                ,''                                                                            AS RATINGDURATIONTYPE    --������������
                ,''                                                                            AS FCISSUERATING         --������ѺƷ���еȼ�
                ,NULL                                                                          AS FCISSUERTYPE          --������ѺƷ���������
                ,''                                                                            AS FCISSUERSTATE         --������ѺƷ������ע�����
                ,''                                                                            AS FCRESIDUALM           --������ѺƷʣ������
                ,1                                                                             AS REVAFREQUENCY         --�ع�Ƶ��
                ,''                                                                            AS GROUPID               --������
                ,NULL                                                                          AS RCERating             --�����˾���ע����ⲿ����
    FROM				RWA_DEV.RWA_TEMP_BAIL2 T1															--�Ŵ���ͬ��
    INNER JOIN	RWA_DEV.RWA_ABS_ISSURE_CONTRACT T2										--�Ŵ���ݱ�
    ON					'ABS' || T1.CONTRACTNO = T2.CONTRACTID
		WHERE 			T1.ISMAX = '1'																				--ȡ��ͬ��ͬ������һ����Ϊ���
    ;
    COMMIT;

    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_ABS_ISSURE_COLLATERAL',cascade => true);

    /*Ŀ�������ͳ��*/
    SELECT COUNT(1) INTO v_count1 FROM RWA_DEV.RWA_ABS_ISSURE_COLLATERAL;
    --Dbms_output.Put_line('rwa_xd_collateral��ǰ��������ݼ�¼Ϊ:' || (v_count3-v_count2) || '��');
    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̽��� ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    p_po_rtncode := '1';
    p_po_rtnmsg  := '�ɹ�'||'-'||v_count1;
    --�����쳣
    EXCEPTION
    WHEN OTHERS THEN
         --Dbms_output.Put_line('������,�������Ϊ:'||sqlcode||';������ϢΪ:'||sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace);
          ROLLBACK;
          p_po_rtncode := sqlcode;
          p_po_rtnmsg  := '�ʲ�֤ȯ��-����ѺƷ('||v_pro_name||')ETLת��ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;
         RETURN;

END PRO_RWA_ABS_ISSURE_COLLATERAL;
/

