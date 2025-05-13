CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_XD_GUARANTEE(
			 											p_data_dt_str	IN	VARCHAR2,		--��������
       											p_po_rtncode	OUT	VARCHAR2,		--���ر�� 1 �ɹ�,0 ʧ��
														p_po_rtnmsg		OUT	VARCHAR2		--��������
				)
  /*
    �洢��������:RWA_DEV.pro_rwa_xd_guarantee
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
  --v_pro_name VARCHAR2(200) := 'RWA_DEV.pro_rwa_xd_guarantee';
  --�����ж�ֵ����
  v_count1 INTEGER;
  --�����쳣����
  v_raise EXCEPTION;

  BEGIN
    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*�����ȫ�����ݼ��������Ŀ���*/
    --1.���Ŀ����е�ԭ�м�¼
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_XD_GUARANTEE';

    --����Ч����¶�Ӧ��ͬ�ı�֤���뵽Ŀ�����-��ͨ��֤
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
                ,flag
    )WITH TEMP_GUARANTEE1 AS(SELECT T1.RELATIVESERIALNO2 AS CONTRACTNO,MIN(NVL(T1.PUTOUTDATE,CASE WHEN T1.BUSINESSTYPE IN ('10201010','10202010') THEN NVL(T3.OCCURDATE,T3.PUTOUTDATE) ELSE T3.PUTOUTDATE END)) AS PUTOUTDATE
                                   ,MAX(NVL(T1.ACTUALMATURITY,T3.MATURITY)) AS MATURITY, MIN(T4.CREDITRISKDATATYPE) AS CREDITRISKDATATYPE
                               FROM RWA_DEV.NCM_BUSINESS_DUEBILL T1
                               INNER JOIN RWA_DEV.NCM_BUSINESS_TYPE T2
                               ON T1.BUSINESSTYPE=T2.TYPENO
                               AND T1.DATANO=T2.DATANO
                               AND T2.SORTNO NOT LIKE '30%' --�ų������ҵ��
                               INNER JOIN RWA_DEV.NCM_BUSINESS_CONTRACT T3
                               ON T1.RELATIVESERIALNO2=T3.SERIALNO        --�����Խ��Ϊ׼�����Թ�����ͬʱ����Ӧ�üӺ�ͬ����Ч����
                               AND T1.DATANO=T3.DATANO
                               --AND T3.BUSINESSTYPE || T3.BUSINESSNATURE <> '1030301005'
                               INNER JOIN RWA_DEV.NCM_BUSINESS_HISTORY T31
                               ON T1.SERIALNO=T31.SERIALNO
                               AND T31.DATANO=P_DATA_DT_STR
                               AND T31.BALANCE>0
                                         /* rwa_dev.brd_loan_nor t31
                               ON t1.serialno = t31.crdt_acct_no
                               AND t31.datano = P_DATA_DT_STR
                               AND t31.cur_bal > 0*/
                               INNER JOIN RWA_DEV.RWA_XD_CONTRACT T4
                               ON T1.RELATIVESERIALNO2 = T4.CONTRACTID
                               AND T1.DATANO=T4.DATANO
                               WHERE T1.DATANO=P_DATA_DT_STR
                               --AND T1.BALANCE>0           --��Ч�ж�����
                               AND T1.SERIALNO<>'BD2014110400000001'
                               AND T1.BUSINESSTYPE NOT IN ('1040101010','1040101020','1040101030','1040102010','1040102020','1040102040','10303010','10201060','10202080','10201080','1020301010','1020301020'
                                   ,'1040105060','1040201010','1040201020','1040202010','105010','11105010','11105020','10302010','10302015','10302020')  --�ų�ͬҵ���ع���Ͷ�ʣ�ί�д���ҵ��,���ֺ�ת��Ҳ�ų�����Ʊ����Ϣ��Ϊ����
                               GROUP BY T1.RELATIVESERIALNO2
                             )
		,TEMP_RELATIVE AS (SELECT T3.SERIALNO,MIN(T1.PUTOUTDATE) AS PUTOUTDATE,MAX(T1.MATURITY) AS MATURITY,MIN(T1.CREDITRISKDATATYPE) AS CREDITRISKDATATYPE,MIN(T2.QUALIFYFLAG) AS QUALIFYFLAG
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
    ,TEMP_CUST_INFO AS (
    								SELECT CUSTOMERID, CUSTOMERNAME, CERTTYPE, CERTID
										  FROM (SELECT CUSTOMERID,
										               CUSTOMERNAME,
										               CERTTYPE,
										               CERTID,
										               ROW_NUMBER() OVER(PARTITION BY CERTTYPE, CERTID ORDER BY CUSTOMERID DESC) AS RN
										          FROM RWA_DEV.NCM_CUSTOMER_INFO
										         WHERE DATANO = P_DATA_DT_STR)
										 WHERE RN = 1
    )
    SELECT
         				 TO_DATE(T1.DATANO,'YYYYMMDD')										AS  DATADATE          		 --��������
         				,T1.DATANO																 		    AS	DATANO               --������ˮ��
         				,'BZ'||T1.SERIALNO													      AS	GUARANTEEID          --��֤ID
								,'XD'																						  AS	SSYSID               --ԴϵͳID
								,T1.SERIALNO																		  AS	GUARANTEECONID       --��֤��ͬID
								,CASE WHEN REPLACE(T1.GUARANTORID,'NCM_','') IS NULL AND T7.CUSTOMERID IS NULL THEN
													 CASE WHEN T1.CERTTYPE LIKE 'Ind%' THEN 'XN-GRKH'
													 			ELSE 'XN-YBGS'
													 END
											WHEN REPLACE(T1.GUARANTORID,'NCM_','') IS NULL THEN T7.CUSTOMERID
											ELSE T1.GUARANTORID
								 END																							AS	GUARANTORID          --��֤��ID
								,T2.CREDITRISKDATATYPE														AS	CREDITRISKDATATYPE   --���÷�����������
								,'010'																	          AS	GUARANTEEWAY       	 --������ʽ
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
                ,'BZ|PT'
    FROM 				RWA_DEV.NCM_GUARANTY_CONTRACT T1
    INNER JOIN 	TEMP_RELATIVE T2
    ON 					T1.SERIALNO = T2.SERIALNO
    LEFT JOIN 	TEMP_CUST_INFO T7
    ON					T1.CERTTYPE = T7.CERTTYPE
    AND					T1.CERTID = T7.CERTID
    LEFT JOIN 	RWA_DEV.RWA_TEMP_PDLEVEL T6
    ON 					DECODE(T1.GUARANTORID,'NCM_',T7.CUSTOMERID,NULL,T7.CUSTOMERID,'',T7.CUSTOMERID,T1.GUARANTORID) = T6.CUSTID
    WHERE 			T1.DATANO = P_DATA_DT_STR
    AND 				T1.GUARANTYTYPE = '010'
    ;
    COMMIT;
    
    --����Ч����¶�Ӧ��ͬ�ı�֤���뵽Ŀ����У����ڴ���-΢������
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
                ,flag
    )WITH TEMP_GUARANTEE1 AS(SELECT T1.RELATIVESERIALNO2 AS CONTRACTNO,MIN(NVL(T1.PUTOUTDATE,CASE WHEN T1.BUSINESSTYPE IN ('10201010','10202010') THEN NVL(T3.OCCURDATE,T3.PUTOUTDATE) ELSE T3.PUTOUTDATE END)) AS PUTOUTDATE
                                   ,MAX(NVL(T1.ACTUALMATURITY,T3.MATURITY)) AS MATURITY, MIN(T4.CREDITRISKDATATYPE) AS CREDITRISKDATATYPE
                               FROM RWA_DEV.NCM_BUSINESS_DUEBILL T1
                               INNER JOIN RWA_DEV.NCM_BUSINESS_TYPE T2
                               ON T1.BUSINESSTYPE=T2.TYPENO
                               AND T1.DATANO=T2.DATANO
                               AND T2.SORTNO NOT LIKE '30%' --�ų������ҵ��
                               INNER JOIN RWA_DEV.NCM_BUSINESS_CONTRACT T3
                               ON T1.RELATIVESERIALNO2=T3.SERIALNO        --�����Խ��Ϊ׼�����Թ�����ͬʱ����Ӧ�üӺ�ͬ����Ч����
                               AND T1.DATANO=T3.DATANO
                               --AND T3.BUSINESSTYPE || T3.BUSINESSNATURE <> '1030301005'
                               INNER JOIN RWA_DEV.NCM_BUSINESS_HISTORY T31
                               ON T1.SERIALNO=T31.SERIALNO
                               AND T31.DATANO=P_DATA_DT_STR
                               AND (T31.OVERDUEBALANCE+T31.DULLBALANCE+T31.BADBALANCE)>0   --ȡ�����ڵļ�¼
                                          /*rwa_dev.brd_loan_nor t31
                               ON t1.serialno = t31.crdt_acct_no
                               AND t31.datano = P_DATA_DT_STR
                               AND t31.sbjt_cd = '13100001' --����΢������*/
                               INNER JOIN RWA_DEV.RWA_XD_CONTRACT T4
                               ON T1.RELATIVESERIALNO2 = T4.CONTRACTID
                               AND T1.DATANO=T4.DATANO
                               WHERE  T1.DATANO=P_DATA_DT_STR
                               AND T1.BUSINESSTYPE='11103030'  --ֻȡ΢����ҵ��
                               GROUP BY T1.RELATIVESERIALNO2
                             )
		,TEMP_RELATIVE AS (SELECT T3.SERIALNO,MIN(T1.PUTOUTDATE) AS PUTOUTDATE,MAX(T1.MATURITY) AS MATURITY,MIN(T1.CREDITRISKDATATYPE) AS CREDITRISKDATATYPE,MIN(T2.QUALIFYFLAG) AS QUALIFYFLAG
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
    ,TEMP_CUST_INFO AS (
    								SELECT CUSTOMERID, CUSTOMERNAME, CERTTYPE, CERTID
										  FROM (SELECT CUSTOMERID,
										               CUSTOMERNAME,
										               CERTTYPE,
										               CERTID,
										               ROW_NUMBER() OVER(PARTITION BY CERTTYPE, CERTID ORDER BY CUSTOMERID DESC) AS RN
										          FROM RWA_DEV.NCM_CUSTOMER_INFO
										         WHERE DATANO = P_DATA_DT_STR)
										 WHERE RN = 1
    )
    SELECT
         				 TO_DATE(T1.DATANO,'YYYYMMDD')										AS  DATADATE          		 --��������
         				,T1.DATANO																 		    AS	DATANO               --������ˮ��
         				,'BZ'||T1.SERIALNO													      AS	GUARANTEEID          --��֤ID
								,'XD'																						  AS	SSYSID               --ԴϵͳID
								,T1.SERIALNO																		  AS	GUARANTEECONID       --��֤��ͬID
								,CASE WHEN REPLACE(T1.GUARANTORID,'NCM_','') IS NULL AND T7.CUSTOMERID IS NULL THEN
													 CASE WHEN T1.CERTTYPE LIKE 'Ind%' THEN 'XN-GRKH'
													 			ELSE 'XN-YBGS'
													 END
											WHEN REPLACE(T1.GUARANTORID,'NCM_','') IS NULL THEN T7.CUSTOMERID
											ELSE T1.GUARANTORID
								 END																							AS	GUARANTORID          --��֤��ID
								,T2.CREDITRISKDATATYPE														AS	CREDITRISKDATATYPE   --���÷�����������
								,'010'																	          AS	GUARANTEEWAY       	 --������ʽ
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
                ,'BZ|YQWLD'
    FROM 				RWA_DEV.NCM_GUARANTY_CONTRACT T1
    INNER JOIN 	TEMP_RELATIVE T2
    ON 					T1.SERIALNO=T2.SERIALNO
    LEFT JOIN 	TEMP_CUST_INFO T7
    ON					T1.CERTTYPE = T7.CERTTYPE
    AND					T1.CERTID = T7.CERTID
    LEFT JOIN 	RWA_DEV.RWA_TEMP_PDLEVEL T6
    ON 					DECODE(T1.GUARANTORID,'NCM_',T7.CUSTOMERID,NULL,T7.CUSTOMERID,'',T7.CUSTOMERID,T1.GUARANTORID) = T6.CUSTID
    WHERE 			T1.DATANO=P_DATA_DT_STR
    AND 				T1.GUARANTYTYPE='010'
    AND NOT EXISTS(SELECT 1 FROM RWA_DEV.RWA_XD_GUARANTEE T3 WHERE 'BZ'||T1.SERIALNO=T3.GUARANTEEID)
    ;
    COMMIT;
    
    --����Ч����¶�Ӧ��ͬ�ı�֤���뵽Ŀ����У����ڴ���-����ҵ��
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
                ,flag
    )WITH TEMP_GUARANTEE1 AS(    
                               SELECT T1.RELATIVESERIALNO2 AS CONTRACTNO,MIN(NVL(T1.PUTOUTDATE,CASE WHEN T1.BUSINESSTYPE IN ('10201010','10202010') THEN NVL(T3.OCCURDATE,T3.PUTOUTDATE) ELSE T3.PUTOUTDATE END)) AS PUTOUTDATE
                                   ,MAX(NVL(T1.ACTUALMATURITY,T3.MATURITY)) AS MATURITY, MIN(T4.CREDITRISKDATATYPE) AS CREDITRISKDATATYPE
                               FROM RWA_DEV.NCM_BUSINESS_DUEBILL T1
                               INNER JOIN RWA_DEV.NCM_BUSINESS_TYPE T2
                               ON T1.BUSINESSTYPE=T2.TYPENO
                               AND T1.DATANO=T2.DATANO
                               AND T2.SORTNO NOT LIKE '30%' --�ų������ҵ��
                               INNER JOIN RWA_DEV.NCM_BUSINESS_CONTRACT T3
                               ON T1.RELATIVESERIALNO2=T3.SERIALNO        --�����Խ��Ϊ׼�����Թ�����ͬʱ����Ӧ�üӺ�ͬ����Ч����
                               AND T1.DATANO=T3.DATANO
                               --AND T3.BUSINESSTYPE || T3.BUSINESSNATURE <> '1030301005'
                               INNER JOIN BRD_LOAN_NOR T4 
                               ON  T4.DATANO = p_data_dt_str
                               AND T4.CRDT_ACCT_NO = T1.SERIALNO
                               AND substr(t4.sbjt_cd,1,4) = '1310'--��Ŀ���
                               AND T4.SBJT_CD != '13100001' --������΢������������д���      
                               INNER JOIN RWA_DEV.RWA_XD_CONTRACT T4
                               ON T1.RELATIVESERIALNO2 = T4.CONTRACTID
                               AND T1.DATANO=T4.DATANO
                               WHERE  T1.DATANO=p_data_dt_str
                               GROUP BY T1.RELATIVESERIALNO2
                             )
		,TEMP_RELATIVE AS (SELECT T3.SERIALNO,MIN(T1.PUTOUTDATE) AS PUTOUTDATE,MAX(T1.MATURITY) AS MATURITY,MIN(T1.CREDITRISKDATATYPE) AS CREDITRISKDATATYPE,MIN(T2.QUALIFYFLAG) AS QUALIFYFLAG
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
		,TEMP_CUST_INFO AS (
    								SELECT CUSTOMERID, CUSTOMERNAME, CERTTYPE, CERTID
										  FROM (SELECT CUSTOMERID,
										               CUSTOMERNAME,
										               CERTTYPE,
										               CERTID,
										               ROW_NUMBER() OVER(PARTITION BY CERTTYPE, CERTID ORDER BY CUSTOMERID DESC) AS RN
										          FROM RWA_DEV.NCM_CUSTOMER_INFO
										         WHERE DATANO = P_DATA_DT_STR)
										 WHERE RN = 1
    )
    SELECT
         				 TO_DATE(T1.DATANO,'YYYYMMDD')										AS  DATADATE          		 --��������
         				,T1.DATANO																 		    AS	DATANO               --������ˮ��
         				,'BZ'||T1.SERIALNO													      AS	GUARANTEEID          --��֤ID
								,'XD'																						  AS	SSYSID               --ԴϵͳID
								,T1.SERIALNO																		  AS	GUARANTEECONID       --��֤��ͬID
								,CASE WHEN REPLACE(T1.GUARANTORID,'NCM_','') IS NULL AND T7.CUSTOMERID IS NULL THEN
													 CASE WHEN T1.CERTTYPE LIKE 'Ind%' THEN 'XN-GRKH'
													 			ELSE 'XN-YBGS'
													 END
											WHEN REPLACE(T1.GUARANTORID,'NCM_','') IS NULL THEN T7.CUSTOMERID
											ELSE T1.GUARANTORID
								 END																							AS	GUARANTORID          --��֤��ID
								,T2.CREDITRISKDATATYPE														AS	CREDITRISKDATATYPE   --���÷�����������
								,'010'																	          AS	GUARANTEEWAY       	 --������ʽ
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
                ,'BZ|YQ'
    FROM 				RWA_DEV.NCM_GUARANTY_CONTRACT T1
    INNER JOIN 	TEMP_RELATIVE T2
    ON 					T1.SERIALNO = T2.SERIALNO
    LEFT JOIN 	TEMP_CUST_INFO T7
    ON					T1.CERTTYPE = T7.CERTTYPE
    AND					T1.CERTID = T7.CERTID
    LEFT JOIN 	RWA_DEV.RWA_TEMP_PDLEVEL T6
    ON 					DECODE(T1.GUARANTORID,'NCM_',T7.CUSTOMERID,NULL,T7.CUSTOMERID,'',T7.CUSTOMERID,T1.GUARANTORID) = T6.CUSTID
    WHERE 			T1.DATANO = P_DATA_DT_STR
    AND 				T1.GUARANTYTYPE = '010'
    AND NOT EXISTS(SELECT 1 FROM RWA_DEV.RWA_XD_GUARANTEE T3 WHERE 'BZ'||T1.SERIALNO=T3.GUARANTEEID)
    ;
    COMMIT;
    
    --����Ч����¶�Ӧ��ͬ�ı�֤���뵽Ŀ�����-׷�ӵ�PUTOUT���ϵı�֤
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
                ,flag
    )WITH TEMP_GUARANTEE1 AS(SELECT DISTINCT  T1.RELATIVESERIALNO2 AS CONTRACTNO
                                              ,MIN(NVL(T1.PUTOUTDATE,CASE WHEN T1.BUSINESSTYPE IN ('10201010','10202010') THEN NVL(T3.OCCURDATE,T3.PUTOUTDATE) ELSE T3.PUTOUTDATE END)) AS PUTOUTDATE
                                              ,MAX(NVL(T1.ACTUALMATURITY,T3.MATURITY)) AS MATURITY, MIN(T6.CREDITRISKDATATYPE) AS CREDITRISKDATATYPE,T4.SERIALNO AS BPSERLANO
                               FROM RWA_DEV.NCM_BUSINESS_DUEBILL T1
                               INNER JOIN RWA_DEV.NCM_BUSINESS_TYPE T2
                               ON T1.BUSINESSTYPE=T2.TYPENO
                               AND T1.DATANO=T2.DATANO
                               AND T2.SORTNO NOT LIKE '30%' --�ų������ҵ��
                               INNER JOIN RWA_DEV.NCM_BUSINESS_CONTRACT T3
                               ON T1.RELATIVESERIALNO2=T3.SERIALNO        --�����Խ��Ϊ׼�����Թ�����ͬʱ����Ӧ�üӺ�ͬ����Ч����
                               AND T1.DATANO=T3.DATANO
                               --AND T3.BUSINESSTYPE || T3.BUSINESSNATURE <> '1030301005'
                               INNER JOIN RWA_DEV.NCM_BUSINESS_PUTOUT T4
                               ON T3.SERIALNO=T4.CONTRACTSERIALNO
                               AND T4.DATANO=P_DATA_DT_STR
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
                               AND T1.DATANO=T6.DATANO
                               WHERE T1.DATANO=P_DATA_DT_STR
                               --AND T1.BALANCE>0           --��Ч�ж�����
                               AND T1.SERIALNO<>'BD2014110400000001'
                               AND T1.BUSINESSTYPE NOT IN ('1040101010','1040101020','1040101030','1040102010','1040102020','1040102040','10303010','10201060','10202080','10201080','1020301010','1020301020'
                                   ,'1040105060','1040201010','1040201020','1040202010','105010','11105010','11105020','10302010','10302015','10302020')  --�ų�ͬҵ���ع���Ͷ�ʣ�ί�д���ҵ��,���ֺ�ת��Ҳ�ų�����Ʊ����Ϣ��Ϊ����
                               GROUP BY T1.RELATIVESERIALNO2,T4.SERIALNO
                              )
		,TEMP_RELATIVE AS (SELECT T3.SERIALNO,MIN(T1.PUTOUTDATE) AS PUTOUTDATE,MAX(T1.MATURITY) AS MATURITY,MIN(T1.CREDITRISKDATATYPE) AS CREDITRISKDATATYPE
                       FROM TEMP_GUARANTEE1 T1
                       INNER JOIN (SELECT OBJECTNO, CONTRACTNO
                                  FROM RWA_DEV.NCM_GUARANTY_RELATIVE
                                  WHERE DATANO=P_DATA_DT_STR
                                  AND OBJECTTYPE='PutOutApply'
                                  GROUP BY OBJECTNO, CONTRACTNO
                                  ) T2
                      ON T1.BPSERLANO=T2.OBJECTNO
                      INNER JOIN RWA_DEV.NCM_GUARANTY_CONTRACT T3
                      ON T2.CONTRACTNO = T3.SERIALNO
                      AND T3.DATANO=P_DATA_DT_STR
                      AND T3.GUARANTYTYPE='010'
                      GROUP BY T3.SERIALNO
                    )
    ,TEMP_CUST_INFO AS (
    								SELECT CUSTOMERID, CUSTOMERNAME, CERTTYPE, CERTID
										  FROM (SELECT CUSTOMERID,
										               CUSTOMERNAME,
										               CERTTYPE,
										               CERTID,
										               ROW_NUMBER() OVER(PARTITION BY CERTTYPE, CERTID ORDER BY CUSTOMERID DESC) AS RN
										          FROM RWA_DEV.NCM_CUSTOMER_INFO
										         WHERE DATANO = P_DATA_DT_STR)
										 WHERE RN = 1
    )
    SELECT
         				 TO_DATE(T1.DATANO,'YYYYMMDD')										AS  DATADATE          		 --��������
         				,T1.DATANO																 		    AS	DATANO               --������ˮ��
         				,'BZ'||T1.SERIALNO													      AS	GUARANTEEID          --��֤ID
								,'XD'																						  AS	SSYSID               --ԴϵͳID
								,T1.SERIALNO																		  AS	GUARANTEECONID       --��֤��ͬID
								,CASE WHEN REPLACE(T1.GUARANTORID,'NCM_','') IS NULL AND T7.CUSTOMERID IS NULL THEN
													 CASE WHEN T1.CERTTYPE LIKE 'Ind%' THEN 'XN-GRKH'
													 			ELSE 'XN-YBGS'
													 END
											WHEN REPLACE(T1.GUARANTORID,'NCM_','') IS NULL THEN T7.CUSTOMERID
											ELSE T1.GUARANTORID
								 END																							AS	GUARANTORID          --��֤��ID
								,T2.CREDITRISKDATATYPE														AS	CREDITRISKDATATYPE   --���÷�����������
								,'010'																	          AS	GUARANTEEWAY       	 --������ʽ
								,''																								AS	QUALFLAGSTD          --Ȩ�ط��ϸ��ʶ
								,''																						    AS	QUALFLAGFIRB         --�����������ϸ��ʶ
								,''																								AS	GUARANTEETYPESTD     --Ȩ�ط���֤����
								,''																								AS	GUARANTORSDVSSTD     --Ȩ�ط���֤��ϸ��
								,''																							  AS	GUARANTEETYPEIRB     --��������֤����
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
                ,'BZ|PUTOUT'
    FROM 				RWA_DEV.NCM_GUARANTY_CONTRACT T1
    INNER JOIN 	TEMP_RELATIVE T2
    ON 					T1.SERIALNO = T2.SERIALNO
    LEFT JOIN 	TEMP_CUST_INFO T7
    ON					T1.CERTTYPE = T7.CERTTYPE
    AND					T1.CERTID = T7.CERTID
    LEFT JOIN 	RWA_DEV.RWA_TEMP_PDLEVEL T6
    ON 					DECODE(T1.GUARANTORID,'NCM_',T7.CUSTOMERID,NULL,T7.CUSTOMERID,'',T7.CUSTOMERID,T1.GUARANTORID) = T6.CUSTID
    WHERE 			T1.DATANO = P_DATA_DT_STR
    AND 				T1.GUARANTYTYPE = '010'
    ;
    COMMIT;
    
    --����Ч����¶�Ӧ��ͬ�ı�֤���뵽Ŀ�����-����Ѻ�㡢����Ѻ�㡢����͢���ж��в�Ϊ��-��ҵ����Ϣ��Ϊ������ͬ��Ϣ��������ʽ�Ǳ�֤��
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
                ,flag
    )WITH TEMP_GUARANTEE1 AS(SELECT T1.RELATIVESERIALNO2 AS CONTRACTNO,MIN(T1.PUTOUTDATE) AS PUTOUTDATE,MAX(T1.ACTUALMATURITY) AS MATURITY
                               FROM RWA_DEV.NCM_BUSINESS_DUEBILL T1
                               INNER JOIN RWA_DEV.NCM_BUSINESS_CONTRACT T3
                               ON T1.RELATIVESERIALNO2=T3.SERIALNO        --�����Խ��Ϊ׼�����Թ�����ͬʱ����Ӧ�üӺ�ͬ����Ч����
                               AND T1.DATANO=T3.DATANO
                               INNER JOIN RWA_DEV.NCM_BUSINESS_HISTORY T31
                               ON T1.SERIALNO=T31.SERIALNO
                               AND T31.DATANO=P_DATA_DT_STR
                               AND T31.BALANCE>0
                                          /*rwa_dev.brd_loan_nor t31
                               ON t1.serialno = t31.crdt_acct_no
                               AND t31.datano = P_DATA_DT_STR
                               AND t31.cur_bal > 0*/
                               WHERE T1.DATANO=P_DATA_DT_STR
                               --AND T1.BALANCE>0           --��Ч�ж�����
                               --AND T1.SERIALNO NOT LIKE 'BD%'
                               AND T1.BUSINESSTYPE
                                   IN ('10201060','10202080','10201080'�� --����Ѻ�㡢����Ѻ�㡢����͢
                               GROUP BY T1.RELATIVESERIALNO2
                             )
    SELECT
         				 TO_DATE(T1.DATANO,'YYYYMMDD')										AS  DATADATE          		 --��������
         				,T1.DATANO																 		    AS	DATANO               --������ˮ��
         				,'BZ'||T1.SERIALNO													      AS	GUARANTEEID          --��֤ID
								,'XD'																						  AS	SSYSID               --ԴϵͳID
								,T1.SERIALNO																		  AS	GUARANTEECONID       --��֤��ͬID
								,T3.ACCEPTORBANKID																AS	GUARANTORID          --��֤��ID
								,T4.CREDITRISKDATATYPE														AS	CREDITRISKDATATYPE   --���÷�����������
								,'010'																	          AS	GUARANTEEWAY       	 --������ʽ
								,''																								AS	QUALFLAGSTD          --Ȩ�ط��ϸ��ʶ
								,''																								AS	QUALFLAGFIRB         --�����������ϸ��ʶ
								,''																								AS	GUARANTEETYPESTD     --Ȩ�ط���֤����
								,''																								AS	GUARANTORSDVSSTD     --Ȩ�ط���֤��ϸ��
								,''																								AS	GUARANTEETYPEIRB     --��������֤����
								,T1.BUSINESSSUM																	  AS	GUARANTEEAMOUNT      --��֤�ܶ�
								,NVL(T1.BUSINESSCURRENCY,'CNY')										AS	CURRENCY             --����
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
                ,'BZ|HY'
    FROM 				RWA_DEV.NCM_BUSINESS_CONTRACT T1
    INNER JOIN 	TEMP_GUARANTEE1 T2
    ON 					T1.SERIALNO=T2.CONTRACTNO
    INNER JOIN 	RWA_DEV.NCM_BUSINESS_PUTOUT T3
    ON 					T1.SERIALNO=T3.CONTRACTSERIALNO
    AND 				T1.DATANO=T3.DATANO
    AND 				T3.ACCEPTORBANKID IS NOT NULL          --�ж��в�Ϊ�ղ�������֤
    INNER JOIN 	RWA_DEV.RWA_XD_CONTRACT T4
    ON					T1.SERIALNO = T4.CONTRACTID
    AND					T1.DATANO = T4.DATANO
    LEFT JOIN RWA_DEV.RWA_TEMP_PDLEVEL T6
    ON T3.ACCEPTORBANKID=T6.CUSTID
    WHERE T1.DATANO=P_DATA_DT_STR
    ;
    COMMIT;
    
    --����Ч����¶�Ӧ��ͬ�ı�֤���뵽Ŀ�����-��׷��Ȩ����������׷��Ȩ�������������̲�Ϊ��-��ҵ����Ϣ��Ϊ������ͬ��Ϣ��������ʽ�Ǳ�֤��
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
                ,flag
    )WITH TEMP_GUARANTEE1 AS(SELECT T1.RELATIVESERIALNO2 AS CONTRACTNO,MIN(T1.PUTOUTDATE) AS PUTOUTDATE,MAX(T1.ACTUALMATURITY) AS MATURITY
                               FROM RWA_DEV.NCM_BUSINESS_DUEBILL T1
                               INNER JOIN RWA_DEV.NCM_BUSINESS_CONTRACT T3
                               ON T1.RELATIVESERIALNO2=T3.SERIALNO        --�����Խ��Ϊ׼�����Թ�����ͬʱ����Ӧ�üӺ�ͬ����Ч����
                               AND T1.DATANO=T3.DATANO
                               INNER JOIN RWA_DEV.NCM_BUSINESS_HISTORY T31
                               ON T1.SERIALNO=T31.SERIALNO
                               AND T31.DATANO=P_DATA_DT_STR
                               AND T31.NORMALBALANCE>0
                                          /*rwa_dev.brd_loan_nor t31
                               ON t1.serialno = t31.crdt_acct_no
                               AND t31.datano = P_DATA_DT_STR
                               AND t31.cur_bal > 0*/
                               WHERE T1.DATANO=P_DATA_DT_STR
                               --AND T1.BALANCE>0           --��Ч�ж�����
                               --AND T1.SERIALNO NOT LIKE 'BD%'
                               AND T1.BUSINESSTYPE
                                   IN ('1020301010','1020301020'�� --��׷��Ȩ����������׷��Ȩ��������
                               GROUP BY T1.RELATIVESERIALNO2
                             )
    SELECT
         				 TO_DATE(T1.DATANO,'YYYYMMDD')										AS  DATADATE          		 --��������
         				,T1.DATANO																 		    AS	DATANO               --������ˮ��
         				,'BZ'||T1.SERIALNO													      AS	GUARANTEEID          --��֤ID
								,'XD'																						  AS	SSYSID               --ԴϵͳID
								,T1.SERIALNO																		  AS	GUARANTEECONID       --��֤��ͬID
								,T3.FACTORID																		  AS	GUARANTORID          --��֤��ID
								,T4.CREDITRISKDATATYPE														AS	CREDITRISKDATATYPE   --���÷�����������
								,'010'																	          AS	GUARANTEEWAY       	 --������ʽ
								,''																								AS	QUALFLAGSTD          --Ȩ�ط��ϸ��ʶ
								,''																								AS	QUALFLAGFIRB         --�����������ϸ��ʶ
								,''																								AS	GUARANTEETYPESTD     --Ȩ�ط���֤����
								,''																								AS	GUARANTORSDVSSTD     --Ȩ�ط���֤��ϸ��
								,''																								AS	GUARANTEETYPEIRB     --��������֤����
								,T1.BUSINESSSUM																	  AS	GUARANTEEAMOUNT      --��֤�ܶ�
								,NVL(T1.BUSINESSCURRENCY,'CNY')										AS	CURRENCY             --����
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
                ,'BZ|BL'
    FROM RWA_DEV.NCM_BUSINESS_CONTRACT T1
    INNER JOIN TEMP_GUARANTEE1 T2
    ON T1.SERIALNO=T2.CONTRACTNO
    INNER JOIN RWA_DEV.NCM_BUSINESS_PUTOUT T3
    ON T1.SERIALNO=T3.CONTRACTSERIALNO
    AND T1.DATANO=T3.DATANO
    AND T3.FACTORID IS NOT NULL               --�����̲�Ϊ����Ϊ��֤
    INNER JOIN 	RWA_DEV.RWA_XD_CONTRACT T4
    ON					T1.SERIALNO = T4.CONTRACTID
    AND					T1.DATANO = T4.DATANO
    LEFT JOIN RWA_DEV.RWA_TEMP_PDLEVEL T6
    ON T3.ACCEPTORBANKID=T6.CUSTID
    WHERE T1.DATANO=P_DATA_DT_STR
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
                ,flag
    )WITH TEMP_GUARANTEE1 AS(SELECT T3.SERIALNO AS CONTRACTNO,MIN(NVL(T1.PUTOUTDATE,CASE WHEN T1.BUSINESSTYPE IN ('10201010','10202010') THEN NVL(T3.OCCURDATE,T3.PUTOUTDATE) ELSE T3.PUTOUTDATE END)) AS PUTOUTDATE
                                    ,MAX(NVL(T1.ACTUALMATURITY,T3.MATURITY)) AS MATURITY,MIN(T4.CREDITRISKDATATYPE) AS CREDITRISKDATATYPE
                               FROM RWA_DEV.NCM_BUSINESS_DUEBILL T1
                               INNER JOIN RWA_DEV.NCM_BUSINESS_TYPE T2
                               ON T1.BUSINESSTYPE=T2.TYPENO
                               AND T1.DATANO=T2.DATANO
                               AND T2.SORTNO NOT LIKE '30%' --�ų������ҵ��
                               INNER JOIN RWA_DEV.NCM_BUSINESS_CONTRACT T3
                               ON T1.RELATIVESERIALNO2=T3.SERIALNO        --�����Խ��Ϊ׼�����Թ�����ͬʱ����Ӧ�üӺ�ͬ����Ч����
                               AND T1.DATANO=T3.DATANO
                               --AND T3.BUSINESSTYPE || T3.BUSINESSNATURE <> '1030301005'
                               INNER JOIN RWA_DEV.NCM_BUSINESS_HISTORY T31
                               ON T1.SERIALNO=T31.SERIALNO
                               AND T31.DATANO=P_DATA_DT_STR
                               AND T31.BALANCE>0
                                          /*rwa_dev.brd_loan_nor t31
                               ON t1.serialno = t31.crdt_acct_no
                               AND t31.datano =  P_DATA_DT_STR
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
		,TEMP_RELATIVE AS (SELECT T5.GUARANTYID,MIN(T1.PUTOUTDATE) AS PUTOUTDATE,MAX(T1.MATURITY) AS MATURITY,MIN(T1.CREDITRISKDATATYPE) AS CREDITRISKDATATYPE
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
         				,'BZ'||T1.GUARANTYID												      AS	GUARANTEEID          --��֤ID
								,'XD'																						  AS	SSYSID               --ԴϵͳID
								,T1.GUARANTYID																		AS	GUARANTEECONID       --��֤��ͬID
								,NVL(T7.CUSTOMERID,'XN-YBGS')									    AS	GUARANTORID          --��֤��ID
								,T2.CREDITRISKDATATYPE														AS	CREDITRISKDATATYPE   --���÷�����������
								,'010'																	          AS	GUARANTEEWAY       	 --������ʽ
								,''																								AS	QUALFLAGSTD          --Ȩ�ط��ϸ��ʶ
								,CASE WHEN T1.QUALIFYFLAG03='01' THEN '1'
                	    WHEN T1.QUALIFYFLAG03='02' THEN '0'
                      ELSE ''
                 END																							AS	QUALFLAGFIRB         --�����������ϸ��ʶ
								,''																								AS	GUARANTEETYPESTD     --Ȩ�ط���֤����
								,''																								AS	GUARANTORSDVSSTD     --Ȩ�ط���֤��ϸ��
								,CASE WHEN T1.QUALIFYFLAG03='01' THEN '020201'
								      ELSE ''
								 END																						  AS	GUARANTEETYPEIRB     --��������֤����
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
                ,'BZ|XYZ'
    FROM RWA_DEV.NCM_GUARANTY_INFO T1
    INNER JOIN TEMP_RELATIVE T2
    ON T1.GUARANTYID = T2.GUARANTYID
    LEFT JOIN RWA_DEV.RWA_TEMP_PDLEVEL T6
    ON T1.CLRERID=T6.CUSTID
    LEFT JOIN		TEMP_CUST_INFO T7
    ON					T1.OBLIGEEIDTYPE = T7.CERTTYPE
    AND 				T1.OBLIGEEIDNUMBER = T7.CERTID
    WHERE T1.DATANO=P_DATA_DT_STR
    --AND T1.CLRSTATUS='01'                  --modify by yushuangjiang
    --AND T1.CLRGNTSTATUS IN ('03','10')     --���ھ������Ŵ��¿�ȷ����������״̬�޶�����ȥ��
    AND T1.GUARANTYTYPEID  IN('004001004001','004001005001','004001006001','004001006002')     --����֤����������֤,�����Ա������������Ա��� ����Ϊ��֤
    ;
    COMMIT;
    
    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_XD_GUARANTEE',cascade => true);

    /*Ŀ�������ͳ��*/
    SELECT COUNT(1) INTO v_count1 FROM RWA_DEV.RWA_XD_GUARANTEE;

    p_po_rtncode := '1';
    p_po_rtnmsg  := '�ɹ�'||'-'||v_count1;
    --�����쳣
    EXCEPTION
    WHEN OTHERS THEN
         --Dbms_output.Put_line('������,�������Ϊ:'||sqlcode||';������ϢΪ:'||sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace);
          ROLLBACK;
          p_po_rtncode := sqlcode;
          p_po_rtnmsg  := '�Ŵ�ϵͳ-��֤(PRO_RWA_XD_GUARANTEE)ETLת��ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_XD_GUARANTEE;
/

