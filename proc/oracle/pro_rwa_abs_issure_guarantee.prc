CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_ABS_ISSURE_GUARANTEE(
			 											p_data_dt_str	IN	VARCHAR2,		--��������
       											p_po_rtncode	OUT	VARCHAR2,		--���ر�� 1 �ɹ�,0 ʧ��
														p_po_rtnmsg		OUT	VARCHAR2		--��������
				)
  /*
    �洢��������:RWA_DEV.PRO_RWA_ABS_ISSURE_GUARANTEE
    ʵ�ֹ���:��Ϣ����ϵͳ-��֤,��ṹΪ��֤��
    ���ݿھ�:ȫ��
    ����Ƶ��:�³�
    ��  ��  :V1.0.0
    ��д��  :YUSJ
    ��дʱ��:2015-05-26
    ��  λ	:�Ϻ���˶��Ϣ�����ɷ����޹�˾
    Դ  ��1	:NCM_GUARANTY_INFO|��������Ϣ��
    Դ  ��2	:NCM_BUSINESS_DUEBILL|����ҵ������Ϣ��
    Դ  ��3	:NCM_BUSINESS_CONTRACT|����ҵ���ͬ��
    Դ  ��4	:NCM_GUARANTY_CONTRACT|������ͬ��Ϣ��
    Դ  ��5	:NCM_CONTRACT_RELATIVE|��ͬ������
    Դ  ��6	:NCM_GUARANTY_RELATIVE|������ͬ�뵣���������
    Դ  ��7	:NCM_CUSTOMER_INFO|�ͻ�������Ϣ��
    Դ  ��8 :NCM_GUARANTY_INFO|��������Ϣ��
    Ŀ���	:RWA_XD_GUARANTEE|�Ŵ�ϵͳ-��֤��
    �����¼(�޸���|�޸�ʱ��|�޸�����):
  */
  AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_ABS_ISSURE_GUARANTEE';
  --�����ж�ֵ����
  v_count1 INTEGER;
  --�����쳣����
  v_raise EXCEPTION;

  BEGIN
    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*�����ȫ�����ݼ��������Ŀ���*/
    --1.���Ŀ����е�ԭ�м�¼
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_ABS_ISSURE_GUARANTEE';

    --����Ч����¶�Ӧ��ͬ�ı�֤���뵽Ŀ�����
    INSERT INTO RWA_DEV.RWA_ABS_ISSURE_GUARANTEE(
         				 DATADATE          												  --��������
								,DATANO                                     --������ˮ��
								,GUARANTEEID                                --��֤ID
								,SSYSID                                     --ԴϵͳID
								,GUARANTEECONID                             --��֤��ͬID
								,GUARANTORID                                --��֤��ID
								,CREDITRISKDATATYPE                         --���÷�����������
								,GUARANTEEWAY                            		--������ʽ
								,QUALFLAGSTD                            		--Ȩ�ط��ϸ��ʶ
								,QUALFLAGFIRB                               --�����������ϸ��ʶ
								,GUARANTEETYPESTD                           --Ȩ�ط���֤����
								,GUARANTORSDVSSTD                           --Ȩ�ط���֤��ϸ��
								,GUARANTEETYPEIRB                           --��������֤����
								,GUARANTEEAMOUNT                            --��֤�ܶ�
								,CURRENCY                                   --����
								,STARTDATE                                  --��ʼ����
								,DUEDATE                                    --��������
								,ORIGINALMATURITY                           --ԭʼ����
								,RESIDUALM                                  --ʣ������
								,GUARANTORIRATING                           --��֤���ڲ�����
								,GUARANTORPD                                --��֤��ΥԼ����
								,GROUPID                                    --������
    )WITH TEMP_GUARANTEE1 AS(SELECT T3.SERIALNO AS CONTRACTNO,MIN(NVL(T1.PUTOUTDATE,CASE WHEN T1.BUSINESSTYPE IN ('10201010','10202010') THEN NVL(T3.OCCURDATE,T3.PUTOUTDATE) ELSE T3.PUTOUTDATE END)) AS PUTOUTDATE
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
		,TEMP_RELATIVE AS (SELECT T3.SERIALNO,MIN(T1.PUTOUTDATE) AS PUTOUTDATE,MAX(T1.MATURITY) AS MATURITY,MIN(ATTRIBUTE1) AS ATTRIBUTE1,MIN(T2.QUALIFYFLAG) AS QUALIFYFLAG
                       FROM TEMP_GUARANTEE1 T1
                       INNER JOIN
                           (SELECT SERIALNO,OBJECTNO,MIN(QUALIFYFLAG) AS QUALIFYFLAG
                            FROM RWA_DEV.NCM_CONTRACT_RELATIVE
                            WHERE DATANO=P_DATA_DT_STR
                            AND OBJECTTYPE = 'GuarantyContract'
                            GROUP BY  SERIALNO,OBJECTNO
                            ) T2
                      ON T1.CONTRACTNO=T2.SERIALNO
                      INNER JOIN RWA_DEV.NCM_GUARANTY_CONTRACT T3
                      ON T2.OBJECTNO = T3.SERIALNO
                      AND T3.DATANO=P_DATA_DT_STR
                      GROUP BY T3.SERIALNO
                    )
    SELECT
         				 TO_DATE(T1.DATANO,'YYYYMMDD')										AS  DATADATE          		 --��������
         				,T1.DATANO																 		    AS	DATANO               --������ˮ��
         				,'ABS'||T1.SERIALNO													      AS	GUARANTEEID          --��֤ID
								,'ABS'																						AS	SSYSID               --ԴϵͳID
								,'ABS'||T1.SERIALNO																AS	GUARANTEECONID       --��֤��ͬID
								,T1.GUARANTORID																		AS	GUARANTORID          --��֤��ID
								,CASE WHEN T2.ATTRIBUTE1='1' THEN '01'
                      ELSE '02'
                 END																							AS	CREDITRISKDATATYPE   --���÷�����������
								,T1.GUARANTYTYPE																	AS	GUARANTEEWAY       	 --������ʽ
								,''																								AS	QUALFLAGSTD          --Ȩ�ط��ϸ��ʶ
								,CASE WHEN T2.QUALIFYFLAG	= '01' THEN '1'
											WHEN T2.QUALIFYFLAG	= '02' THEN '0'
								 			ELSE ''
								 END																						  AS	QUALFLAGFIRB         --�����������ϸ��ʶ
								,''																								AS	GUARANTEETYPESTD     --Ȩ�ط���֤����
								,''																								AS	GUARANTORSDVSSTD     --Ȩ�ط���֤��ϸ��
								,CASE WHEN T2.QUALIFYFLAG = '01' THEN '020201'
								      ELSE ''
								 END																							AS	GUARANTEETYPEIRB     --��������֤����
								,T1.GUARANTYVALUE																	AS	GUARANTEEAMOUNT      --��֤�ܶ�
								,NVL(T1.GUARANTYCURRENCY,'CNY')										AS	CURRENCY             --����
								,T2.PUTOUTDATE																	  AS	STARTDATE            --��ʼ����
								,T2.MATURITY																			AS	DUEDATE              --��������
								,CASE WHEN (TO_DATE(T2.MATURITY,'YYYYMMDD')-TO_DATE(T2.PUTOUTDATE,'YYYYMMDD'))/365<0
								      THEN 0
								      ELSE (TO_DATE(T2.MATURITY,'YYYYMMDD')-TO_DATE(T2.PUTOUTDATE,'YYYYMMDD'))/365
								END                                               AS  ORIGINALMATURITY   	 --ԭʼ����
								,CASE WHEN (TO_DATE(T2.MATURITY,'YYYYMMDD')-TO_DATE(T1.DATANO,'YYYYMMDD'))/365<0
								      THEN 0
								      ELSE (TO_DATE(T2.MATURITY,'YYYYMMDD')-TO_DATE(T1.DATANO,'YYYYMMDD'))/365
								END      											                    AS	RESIDUALM            --ʣ������
								,T6.PDADJLEVEL																		AS	GUARANTORIRATING     --��֤���ڲ�����
								,T6.PD																						AS	GUARANTORPD          --��֤��ΥԼ����
								,''																								AS	GROUPID              --������
    FROM RWA_DEV.NCM_GUARANTY_CONTRACT T1
    INNER JOIN TEMP_RELATIVE T2
    ON T1.SERIALNO=T2.SERIALNO
    LEFT JOIN RWA_DEV.RWA_TEMP_PDLEVEL T6
    ON T1.GUARANTORID=T6.CUSTID
    WHERE T1.DATANO=P_DATA_DT_STR
    AND T1.GUARANTYTYPE='010'
    ;
    COMMIT;

    --����Ч����¶�Ӧ��ͬ�ı�֤���뵽Ŀ����У�����ѺƷ����Ϊ����֤����������֤����Ϊ��֤��
    INSERT INTO RWA_DEV.RWA_XD_GUARANTEE(
         				 DATADATE          												  --��������
								,DATANO                                     --������ˮ��
								,GUARANTEEID                                --��֤ID
								,SSYSID                                     --ԴϵͳID
								,GUARANTEECONID                             --��֤��ͬID
								,GUARANTORID                                --��֤��ID
								,CREDITRISKDATATYPE                         --���÷�����������
								,GUARANTEEWAY                            		--������ʽ
								,QUALFLAGSTD                            		--Ȩ�ط��ϸ��ʶ
								,QUALFLAGFIRB                               --�����������ϸ��ʶ
								,GUARANTEETYPESTD                           --Ȩ�ط���֤����
								,GUARANTORSDVSSTD                           --Ȩ�ط���֤��ϸ��
								,GUARANTEETYPEIRB                           --��������֤����
								,GUARANTEEAMOUNT                            --��֤�ܶ�
								,CURRENCY                                   --����
								,STARTDATE                                  --��ʼ����
								,DUEDATE                                    --��������
								,ORIGINALMATURITY                           --ԭʼ����
								,RESIDUALM                                  --ʣ������
								,GUARANTORIRATING                           --��֤���ڲ�����
								,GUARANTORPD                                --��֤��ΥԼ����
								,GROUPID                                    --������
    )WITH TEMP_GUARANTEE1 AS(SELECT T3.SERIALNO AS CONTRACTNO,MIN(NVL(T1.PUTOUTDATE,CASE WHEN T1.BUSINESSTYPE IN ('10201010','10202010') THEN NVL(T3.OCCURDATE,T3.PUTOUTDATE) ELSE T3.PUTOUTDATE END)) AS PUTOUTDATE
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
                       FROM TEMP_GUARANTEE1 T1
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
		 , TEMP_CUST_INFO AS (
    								SELECT CUSTOMERID, CERTTYPE, CERTID
                      FROM (SELECT T1.CUSTOMERID,
                                   T1.CERTTYPE,
                                   T1.CERTID,
                                   ROW_NUMBER() OVER(PARTITION BY T1.CERTTYPE, T1.CERTID ORDER BY T1.CUSTOMERID) AS RM
                              FROM RWA_DEV.NCM_CUSTOMER_INFO T1
                             WHERE EXISTS
                             (SELECT 1
                                      FROM RWA_DEV.NCM_GUARANTY_INFO T2
                                     WHERE T1.CERTID = T2.OBLIGEEIDNUMBER
                                       AND T2.DATANO = p_data_dt_str
                                       AND T2.GUARANTYTYPEID IN
                                           ('004001004001',
                                            '004001005001',
                                            '004001006001',
                                            '004001006002',
                                            '001001003001')
                                       AND T2.AFFIRMVALUE0 > 0)
                               AND T1.DATANO = p_data_dt_str)
                     WHERE RM = 1
    )
    SELECT
         				 TO_DATE(T1.DATANO,'YYYYMMDD')										AS  DATADATE          		 --��������
         				,T1.DATANO																 		    AS	DATANO               --������ˮ��
         				,'ABS'||T1.GUARANTYID												      AS	GUARANTEEID          --��֤ID
								,'ABS'																						AS	SSYSID               --ԴϵͳID
								,''																		            AS	GUARANTEECONID       --��֤��ͬID
								,NVL(T7.CUSTOMERID,'XN-YBGS')									    AS	GUARANTORID          --��֤��ID
								,CASE WHEN T2.ATTRIBUTE1='1' THEN '01'
                      ELSE '02'
                 END																							AS	CREDITRISKDATATYPE   --���÷�����������
								,T1.GUARANTYTYPE																	AS	GUARANTEEWAY       	 --������ʽ
								,''																								AS	QUALFLAGSTD          --Ȩ�ط��ϸ��ʶ
								,CASE WHEN T1.QUALIFYFLAG03='01' THEN '1'
                	    WHEN T1.QUALIFYFLAG03='02' THEN '0'
                      ELSE ''
                 END																							AS	QUALFLAGFIRB         --�����������ϸ��ʶ
								,''																								AS	GUARANTEETYPESTD     --Ȩ�ط���֤����
								,''																								AS	GUARANTORSDVSSTD     --Ȩ�ط���֤��ϸ��
								,CASE WHEN T1.QUALIFYFLAG03='01' THEN '020201'
								      ELSE ''
								 END																							AS	GUARANTEETYPEIRB     --��������֤����
								,T1.AFFIRMVALUE0																	AS	GUARANTEEAMOUNT      --��֤�ܶ�
								,NVL(T1.AFFIRMCURRENCY,'CNY')										  AS	CURRENCY             --����
								,T2.PUTOUTDATE																	  AS	STARTDATE            --��ʼ����
								,T2.MATURITY																			AS	DUEDATE              --��������
								,CASE WHEN (TO_DATE(T2.MATURITY,'YYYYMMDD')-TO_DATE(T2.PUTOUTDATE,'YYYYMMDD'))/365<0
								      THEN 0
								      ELSE (TO_DATE(T2.MATURITY,'YYYYMMDD')-TO_DATE(T2.PUTOUTDATE,'YYYYMMDD'))/365
								END                                               AS  ORIGINALMATURITY   	 --ԭʼ����
								,CASE WHEN (TO_DATE(T2.MATURITY,'YYYYMMDD')-TO_DATE(T1.DATANO,'YYYYMMDD'))/365<0
								      THEN 0
								      ELSE (TO_DATE(T2.MATURITY,'YYYYMMDD')-TO_DATE(T1.DATANO,'YYYYMMDD'))/365
								END      											                    AS	RESIDUALM            --ʣ������
								,T6.PDADJLEVEL																		AS	GUARANTORIRATING     --��֤���ڲ�����
								,T6.PD																						AS	GUARANTORPD          --��֤��ΥԼ����
								,''																								AS	GROUPID              --������
    FROM RWA_DEV.NCM_GUARANTY_INFO T1
    INNER JOIN TEMP_RELATIVE T2
    ON T1.GUARANTYID = T2.GUARANTYID
    LEFT JOIN RWA_DEV.RWA_TEMP_PDLEVEL T6
    ON T1.CLRERID=T6.CUSTID
    LEFT JOIN		TEMP_CUST_INFO T7
    ON					T1.OBLIGEEIDTYPE = T7.CERTTYPE
    AND 				T1.OBLIGEEIDNUMBER = T7.CERTID
    WHERE T1.DATANO=P_DATA_DT_STR
    AND T1.GUARANTYTYPE  IN('004001004001','004001005001','004001006001','004001006002')     --����֤����������֤,�����Ա������������Ա��� ����Ϊ��֤
    ;
    COMMIT;

    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_ABS_ISSURE_GUARANTEE',cascade => true);

    /*Ŀ�������ͳ��*/
    SELECT COUNT(1) INTO v_count1 FROM RWA_DEV.RWA_ABS_ISSURE_GUARANTEE;

    p_po_rtncode := '1';
    p_po_rtnmsg  := '�ɹ�'||'-'||v_count1;
    --�����쳣
    EXCEPTION
    WHEN OTHERS THEN
         --Dbms_output.Put_line('������,�������Ϊ:'||sqlcode||';������ϢΪ:'||sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace);
          ROLLBACK;
          p_po_rtncode := sqlcode;
          p_po_rtnmsg  := '�ʲ�֤ȯ��-��֤(PRO_RWA_ABS_ISSURE_GUARANTEE)ETLת��ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_ABS_ISSURE_GUARANTEE;
/

