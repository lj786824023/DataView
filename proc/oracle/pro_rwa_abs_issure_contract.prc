CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_ABS_ISSURE_CONTRACT(
                             p_data_dt_str  IN  VARCHAR2,    --��������
                             p_po_rtncode  OUT  VARCHAR2,    --���ر�� 1 �ɹ�,0 ʧ��
                            p_po_rtnmsg    OUT  VARCHAR2    --��������
        )
  /*
    �洢��������:PRO_RWA_ABS_ISSURE_CONTRACT
    ʵ�ֹ���:�Ŵ�ϵͳ��ͬ��,�����ͬ�й���Ϣ
    ���ݿھ�:ȫ��
    ����Ƶ��:�³�
    ��  ��  :V1.0.0
    ��д��  :YUSJ
    ��дʱ��:2016-04-05
    ��  λ  :�Ϻ���˶��Ϣ�����ɷ����޹�˾
    Դ  ��1  :NCM_BUSINESS_CONTRACT|����ҵ���ͬ��
    Դ  ��2  :NCM_BUSINESS_TYPE|ҵ��Ʒ����Ϣ��
    Դ  ��3  :RWA.ORG_INFO|������Ϣ��
    Դ  ��5  :RWA.CODE_LIBIARY|�����
    Դ  ��6  :NCM_BUSINESS_DUEBILL|����ҵ������Ϣ��
    Ŀ���  :RWA_XD_CONTRACT|�Ŵ�ϵͳ��ͬ��
    ������  :��
    �����¼(�޸���|�޸�ʱ��|�޸�����)��
    */
  AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_ABS_ISSURE_CONTRACT';
  --�����ж�ֵ����
  v_count1 INTEGER;
  --�����쳣����
  v_raise EXCEPTION;

  BEGIN
    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*�����ȫ�����ݼ��������Ŀ���*/
    --1.���Ŀ����е�ԭ�м�¼
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_ABS_ISSURE_CONTRACT';

    /*������Ч�ĺ�ͬ��Ϣ-��Ҫ������֤�������������Щ����̨��ҵ��*/
    INSERT INTO RWA_DEV.RWA_ABS_ISSURE_CONTRACT(
               DATADATE                              --��������
              ,DATANO                                --������ˮ��
              ,CONTRACTID                            --��ͬID
              ,SCONTRACTID                           --Դ��ͬID
              ,SSYSID                                --ԴϵͳID
              ,CLIENTID                              --��������ID
              ,SORGID                                --Դ����ID
              ,SORGNAME                              --Դ��������
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
              ,ABSUAFLAG                              --�ʲ�֤ȯ�������ʲ���ʶ
              ,ABSPOOLID                              --֤ȯ���ʲ���ID
              ,ABSPROPORTION                          --�ʲ�֤ȯ������
              ,GROUPID                                --������
              ,GUARANTEETYPE                          --��Ҫ������ʽ
              ,ORGSORTNO                              --�������������
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
                TO_DATE(T1.DATANO,'YYYYMMDD')                                      AS DATADATE            --��������
                ,T1.DATANO                                                         AS DATANO              --������ˮ��
                ,'ABS'||T1.SERIALNO                                                AS CONTRACTID          --��ͬID
                ,'ABS'||T1.SERIALNO                                                AS SCONTRACTID         --Դ��ͬID
                ,'ABS'                                                             AS SSYSID              --ԴϵͳID
                ,T1.CUSTOMERID                                                     AS CLIENTID            --��������ID
                ,T1.OPERATEORGID                                                   AS SORGID              --Դ����ID
                ,T3.ORGNAME                                                        AS SORGNAME            --Դ��������
                ,T1.OPERATEORGID                                                   AS ORGID               --��������ID
                ,T3.ORGNAME                                                        AS ORGNAME             --������������
                ,CASE WHEN T2.ATTRIBUTE1='1' THEN T1.DIRECTION
                	    ELSE ''
                 END                                                               AS INDUSTRYID          --������ҵ����
                ,CASE WHEN T2.ATTRIBUTE1='1' THEN T5.ITEMNAME
                	    ELSE ''
                 END                                                               AS INDUSTRYNAME        --������ҵ����
                ,CASE WHEN T1.BUSINESSCURRENCY<>'CNY' THEN '0102'                                --��ҵı���ҵ��         ����-ó��
                      WHEN T1.BUSINESSTYPE IN('10302010','10302015','10302020') THEN '0401'      --����ҵ��               ͬҵ�����г���
                      WHEN T1.BUSINESSTYPE IN('10201010','1035102010','1035102020') THEN '0102'  --��������֤���羳����   �鵽 ����-ó��
                      WHEN T1.LINETYPE='0010' THEN '0101'
                	    WHEN T1.LINETYPE='0020' THEN '0201'
                	    WHEN T1.LINETYPE='0030' THEN '0301'
                	    WHEN T1.LINETYPE='0040' THEN '0401'
                	    ELSE '0101'
                 END                                                       AS BUSINESSLINE        --����  :01-С΢,02-����,03-����,04-��
                ,'310'                                                             AS ASSETTYPE           --�ʲ�����
                ,'31001'                                                           AS ASSETSUBTYPE        --�ʲ�С��
                ,T1.BUSINESSTYPE                                                   AS BUSINESSTYPEID      --ҵ��Ʒ�ִ���
                ,T2.TYPENAME                                                       AS BUSINESSTYPENAME    --ҵ��Ʒ������
                ,CASE WHEN T2.ATTRIBUTE1='1'
                      THEN '01' --������
                      ELSE '02' --����
                  END                                                              AS CREDITRISKDATATYPE  --���÷�����������
                ,T1.PUTOUTDATE                                                     AS STARTDATE           -- ��ʼ����
                ,T1.MATURITY                                                       AS DUEDATE             --��������
                ,CASE WHEN (TO_DATE(T1.MATURITY,'YYYYMMDD')-TO_DATE(T1.PUTOUTDATE,'YYYYMMDD'))/365<0
                      THEN 0
                      ELSE (TO_DATE(T1.MATURITY,'YYYYMMDD')-TO_DATE(T1.PUTOUTDATE,'YYYYMMDD'))/365
                 END                                                               AS OriginalMaturity    --ԭʼ����
                ,CASE WHEN (TO_DATE(T1.MATURITY,'YYYYMMDD')-TO_DATE(T1.DATANO,'YYYYMMDD'))/365<0
                      THEN 0
                      ELSE (TO_DATE(T1.MATURITY,'YYYYMMDD')-TO_DATE(T1.DATANO,'YYYYMMDD'))/365
                 END                                                               AS ResidualM           --ʣ������
                ,T1.BUSINESSCURRENCY                                               AS SETTLEMENTCURRENCY  --�������
                ,T1.BUSINESSSUM                                                    AS CONTRACTAMOUNT      --��ͬ�ܽ��
                ,0                                                                 AS NOTEXTRACTPART      --��ͬδ��ȡ����
                ,'0'                                                               AS UNCONDCANCELFLAG    --�Ƿ����ʱ����������    0:��1����
                ,'1'                                                               AS ABSUAFLAG           --�ʲ�֤ȯ�������ʲ���ʶ
                ,RWAIU.ZCCBH                                                       AS ABSPOOLID           --֤ȯ���ʲ���ID
                ,1                                                                 AS ABSPROPORTION       --�ʲ�֤ȯ������
                ,''                                                                AS GROUPID             --������
                ,T1.VOUCHTYPE                                                      AS GUARANTEETYPE       --��Ҫ������ʽ
                ,T3.SORTNO                                                         AS ORGSORTNO           --�������������
    FROM 				RWA_DEV.NCM_BUSINESS_CONTRACT T1
    LEFT JOIN 	RWA_DEV.NCM_BUSINESS_TYPE T2
    ON 					T1.BUSINESSTYPE = T2.TYPENO
    AND 				T1.DATANO = T2.DATANO
    AND 				T2.SORTNO NOT LIKE '3%'  --�ų������ҵ��
    LEFT JOIN 	RWA.ORG_INFO T3
    ON 					T1.OPERATEORGID = T3.ORGID
    LEFT JOIN 	RWA.CODE_LIBRARY T5
    ON 					T1.DIRECTION = T5.ITEMNO
    AND 				T5.CODENO = 'IndustryType'
    INNER JOIN 	RWA.RWA_WS_ABS_ISSUE_UNDERASSET RWAIU             --�ʲ�֤ȯ����¼��
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

    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_ABS_ISSURE_CONTRACT',cascade => true);

     --ͳ�Ʋ���ļ�¼
    SELECT COUNT(1) INTO v_count1 FROM RWA_DEV.RWA_ABS_ISSURE_CONTRACT;

    p_po_rtncode := '1';
    p_po_rtnmsg  := '�ɹ�'||'-'||v_count1;
    --�����쳣
    EXCEPTION
    WHEN OTHERS THEN
         --Dbms_output.Put_line('������,�������Ϊ:'||sqlcode||';������ϢΪ:'||sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace);
          ROLLBACK;
          p_po_rtncode := sqlcode;
          p_po_rtnmsg  := '�ʲ�֤ȯ����ͬ��(RWA_ABS_ISSURE_CONTRACT)ETLת��ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace ;
         RETURN;
END PRO_RWA_ABS_ISSURE_CONTRACT;
/

