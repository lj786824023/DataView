CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_XD_CONTRACT(
                             p_data_dt_str  IN  VARCHAR2,    --��������
                             p_po_rtncode  OUT  VARCHAR2,    --���ر�� 1 �ɹ�,0 ʧ��
                             p_po_rtnmsg    OUT  VARCHAR2    --��������
        )
  /*
    �洢��������:PRO_RWA_XD_CONTRACT
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
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_XD_CONTRACT';
  --�����ж�ֵ����
  v_count1 INTEGER;
  --�����쳣����
  v_raise EXCEPTION;

  BEGIN
    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*�����ȫ�����ݼ��������Ŀ���*/
    --1.���Ŀ����е�ԭ�м�¼
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_XD_CONTRACT';

    /*������Ч����µĺ�ͬ��Ϣ(������Ч����µĺ�ͬ��Ϣ)*/
    INSERT INTO RWA_DEV.RWA_XD_CONTRACT(
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
    )WITH TEMP_CONTRACT AS(SELECT DISTINCT T1.CONTRACTID AS CONTRACTNO
                           FROM RWA_DEV.RWA_XD_EXPOSURE T1
                           WHERE accsubject1 <> '70230101' ---20191014 by YSJ  �ų��ʲ�֤ȯ��
                           AND ACCSUBJECT1 NOT LIKE '@%' --20191123 BY YSJ   �ų�
                           )
     SELECT
                TO_DATE(T1.DATANO,'YYYYMMDD')                                      AS DATADATE            --��������
                ,T1.DATANO                                                         AS DATANO              --������ˮ��
                ,T1.SERIALNO                                                       AS CONTRACTID          --��ͬID
                ,T1.SERIALNO                                                       AS SCONTRACTID         --Դ��ͬID
                ,'XD'                                                              AS SSYSID              --ԴϵͳID
                ,CASE WHEN REPLACE(T1.CUSTOMERID,'NCM_','') IS NULL THEN DECODE(T1.LINETYPE,'0030','XN-GRKH','XN-YBGS') 								--���ͻ�IDΪ�գ�����Ϊ���˾�����Ϊ���˿ͻ�������Ϊһ�㹫˾
                			ELSE T1.CUSTOMERID
                 END                                                     AS CLIENTID            --��������ID
                ,NVL(T1.OPERATEORGID,'19999999')                                   AS SORGID              --Դ����ID
                ,NVL(T3.ORGNAME,'δ֪')                                            AS SORGNAME            --Դ��������
                ,NVL(T3.SORTNO,'19999999')                                         AS ORGSORTNO           --������������
                ,decode(substr(T1.OPERATEORGID,1,1),'@','01000000',T1.OPERATEORGID)                                   AS ORGID               --��������ID
                ,NVL(T3.ORGNAME,'����')                                            AS ORGNAME             --������������
                ,NVL(T1.DIRECTION,T8.DIRECTION)                                    AS INDUSTRYID          --������ҵ����
                ,NVL(T5.ITEMNAME,T9.ITEMNAME)                                      AS INDUSTRYNAME        --������ҵ����
                ,CASE WHEN T1.BUSINESSCURRENCY<>'CNY' THEN '0102'                                --��ҵı���ҵ��         ����-ó��
                      WHEN T1.BUSINESSTYPE IN('10302010','10302015','10302020') THEN '0401'      --����ҵ��               ͬҵ�����г���
                      WHEN T1.BUSINESSTYPE IN('10201010','1035102010','1035102020') THEN '0102'  --��������֤���羳����   �鵽 ����-ó��
                      WHEN T1.LINETYPE='0010' THEN '0101'
                	    WHEN T1.LINETYPE='0020' THEN '0201'
                	    WHEN T1.LINETYPE='0030' THEN '0301'
                	    WHEN T1.LINETYPE='0040' THEN '0401'
                	    ELSE '0101'
                 END                                                               AS BUSINESSLINE        --����
                ,''                                                                AS ASSETTYPE           --�ʲ�����
                ,''                                                                AS ASSETSUBTYPE        --�ʲ�С��
                ,CASE WHEN T1.BUSINESSTYPE='11103019' AND T1.PURPOSE='010'                  --���ֳ�����ס����Ѻ����
                      THEN '11103040'
                      ELSE T1.BUSINESSTYPE
                 END                                                               AS BUSINESSTYPEID          --ҵ��Ʒ�ִ���
                ,CASE WHEN T1.BUSINESSTYPE='11103019' AND T1.PURPOSE='010'
                      THEN '�����ۺ����Ѵ���(����ס������)'
                      ELSE T2.TYPENAME
                 END                                                               AS BUSINESSTYPENAME    --ҵ��Ʒ������
                /*,CASE WHEN T2.ATTRIBUTE1='1'
                      THEN '01' --������
                      ELSE '02' --����
                  END                                                              AS CREDITRISKDATATYPE  --���÷�����������
                */
                ,CASE WHEN T7.CUSTOMERTYPE = '0310' OR T7.CERTTYPE LIKE 'Ind%'
                      THEN '02' --����
                      ELSE '01' --������
                  END                                                              AS CREDITRISKDATATYPE  --���÷�����������
                ,T1.PUTOUTDATE                                                     AS STARTDATE           --��ʼ����
                ,T1.MATURITY                                                       AS DUEDATE             --��������
                ,CASE WHEN (TO_DATE(T1.MATURITY,'YYYYMMDD')-TO_DATE(T1.PUTOUTDATE,'YYYYMMDD'))/365<0
                      THEN 0
                      ELSE (TO_DATE(T1.MATURITY,'YYYYMMDD')-TO_DATE(T1.PUTOUTDATE,'YYYYMMDD'))/365
                END                                                                AS OriginalMaturity    --ԭʼ����
                ,CASE WHEN (TO_DATE(T1.MATURITY,'YYYYMMDD')-TO_DATE(P_DATA_DT_STR,'YYYYMMDD'))/365<0
                      THEN 0
                      ELSE (TO_DATE(T1.MATURITY,'YYYYMMDD')-TO_DATE(P_DATA_DT_STR,'YYYYMMDD'))/365
                END                                                                AS ResidualM           --ʣ������
                ,decode(T1.BUSINESSCURRENCY,'@','CNY',T1.BUSINESSCURRENCY)                                               AS SETTLEMENTCURRENCY  --�������
                ,T1.BUSINESSSUM                                                    AS CONTRACTAMOUNT      --��ͬ�ܽ��
                ,0                                                                 AS NOTEXTRACTPART      --��ͬδ��ȡ����
                ,'1'                                                               AS UNCONDCANCELFLAG    --�Ƿ����ʱ����������    0:��1����
                ,CASE WHEN T6.PROJECTNO IS NULL THEN '0'
                      ELSE '1'
                 END                                                               AS ABSUAFLAG           --�ʲ�֤ȯ�������ʲ���ʶ
                ,CASE WHEN T6.PROJECTNO IS NULL THEN ''
                      ELSE T6.PROJECTNO
                 END                                                               AS ABSPOOLID           --֤ȯ���ʲ���ID
                ,''                                                                AS GROUPID             --������
                ,T1.VOUCHTYPE                                                      AS GUARANTEETYPE       --��Ҫ������ʽ
                ,NULL                                                              AS ABSPROPORTION       --�ʲ�֤ȯ������
    FROM 				RWA_DEV.NCM_BUSINESS_CONTRACT T1
    INNER JOIN 	TEMP_CONTRACT T4
    ON 					T1.SERIALNO = T4.CONTRACTNO
    LEFT JOIN 	RWA_DEV.NCM_BUSINESS_TYPE T2
    ON 					T1.BUSINESSTYPE = T2.TYPENO
    AND 				T1.DATANO = T2.DATANO
    LEFT JOIN 	RWA.ORG_INFO T3
    ON 					T1.OPERATEORGID = T3.ORGID
    LEFT JOIN 	RWA.CODE_LIBRARY T5
    ON 					T1.DIRECTION = T5.ITEMNO
    AND 				T5.CODENO = 'IndustryType'
    LEFT JOIN 	(SELECT DISTINCT AA.PROJECTNO,AA.CONTRACTNO   --�ж��Ƿ��ʲ�֤ȯ��
               		 FROM RWA_DEV.NCM_ABS_PROJECT_ASSET AA
             INNER JOIN RWA_DEV.NCM_ABS_PROJECT_INFO BB
               			 ON AA.PROJECTNO = BB.PROJECTNO
               			AND BB.DATANO = P_DATA_DT_STR
               			AND BB.PROJECTSTATUS='0401'            --����ɹ�
               		WHERE AA.DATANO = P_DATA_DT_STR
    						) T6
    ON 					T1.SERIALNO = T6.CONTRACTNO
    LEFT JOIN		RWA_DEV.NCM_CUSTOMER_INFO T7
    ON					T1.CUSTOMERID = T7.CUSTOMERID
    AND					T1.DATANO = T7.DATANO
    LEFT JOIN		(
    						select OBJECTNO, DIRECTION
								  from (select T.OBJECTNO,
								               T.DIRECTION,
								               ROW_NUMBER() OVER(PARTITION BY T.OBJECTNO order by T.SERIALNO DESC) AS RM
								          from RWA_DEV.NCM_PUTOUT_SCHEME T
								         where T.DATANO = P_DATA_DT_STR
								           and T.OBJECTTYPE = 'BusinessContract'
								           and T.DIRECTION IS NOT NULL)
								 where RM = 1
								) T8									--�����ҵ�����ҵͶ��������ñ�ȡ
    ON					T1.SERIALNO = T8.OBJECTNO
    LEFT JOIN 	RWA.CODE_LIBRARY T9
    ON 					T8.DIRECTION = T9.ITEMNO
    AND 				T9.CODENO = 'IndustryType'
    WHERE 			T1.DATANO = P_DATA_DT_STR
    ;
    COMMIT;

    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_XD_CONTRACT',cascade => true);

     --ͳ�Ʋ���ļ�¼
    SELECT COUNT(1) INTO v_count1 FROM RWA_DEV.RWA_XD_CONTRACT;

    p_po_rtncode := '1';
    p_po_rtnmsg  := '�ɹ�'||'-'||v_count1;
    --�����쳣
    EXCEPTION
    WHEN OTHERS THEN
         --Dbms_output.Put_line('������,�������Ϊ:'||sqlcode||';������ϢΪ:'||sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace);
          ROLLBACK;
          p_po_rtncode := sqlcode;
          p_po_rtnmsg  := '���÷��պ�ͬ��(PRO_RWA_EI_CONTRACT)ETLת��ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace ;
         RETURN;
END PRO_RWA_XD_CONTRACT;
/

