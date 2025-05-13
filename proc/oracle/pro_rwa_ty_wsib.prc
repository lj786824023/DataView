CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_TY_WSIB(
			 											p_data_dt_str	IN	VARCHAR2,		--�������� yyyyMMdd
       											p_po_rtncode	OUT	VARCHAR2,		--���ر�� 1 �ɹ�,0 ʧ��
														p_po_rtnmsg		OUT	VARCHAR2		--��������
				)
  /*
    �洢��������:RWA_DEV.PRO_RWA_TY_WSIB
    ʵ�ֹ���:����ϵͳ-ͬҵ-��¼�̵�(������Դ����ϵͳ��ҵ�������Ϣȫ������RWA�������ͬҵ��¼�̵ױ���)
    ���ݿھ�:ȫ��
    ����Ƶ��:�³�����
    ��  ��  :V1.0.0
    ��д��  :LISY
    ��дʱ��:2016-06-20
    ��  λ  :�Ϻ���˶��Ϣ�����ɷ����޹�˾
    Դ  ��1 :RWA_DEV.CBS_BRW|�ʽ���Ǽǲ�
    Դ  ��2 :RWA_DEV.CBS_BBR|ͬҵ�˻��Ǽǲ�
    Դ  ��3 :RWA_DEV.CBS_IAC|ͨ�÷ֻ���
    Դ  ��4 :RWA.ORG_INFO|������Ϣ��
    Դ  ��5 :RWA.RWA_WS_INNERBANK|�������ͬҵ��¼��
    Դ  ��6 :RWA.RWA_WP_SUPPTASKORG|��¼��������ַ����ñ�
    Դ  ��7 :RWA.RWA_WP_SUPPTASK|��¼���񷢲���
    Դ  ��8 :RWA_DEV.BL_CUSTOMER_INFO|��¼�ͻ����ܱ�
    Ŀ���1 :RWA.RWA_WSIB_INNERBANK|�������ͬҵ�̵ױ�
    �����¼(�޸���|�޸�ʱ��|�޸�����):
    */
  AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_TY_WSIB';
  --�����쳣����
  v_raise EXCEPTION;
  --���嵱ǰ����ļ�¼��
  v_count INTEGER;

  BEGIN
    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*�����ȫ�����ݼ��������Ŀ���*/
    --1.���Ŀ����е�ԭ�м�¼
    --��ղ������ͬҵ�̵ױ�
    DELETE FROM RWA.RWA_WSIB_INNERBANK WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD');

    COMMIT;

    --2.���������������ݴ�Դ����뵽Ŀ�����
    --2.1 ����ϵͳ-���ͬҵҵ��
    INSERT INTO RWA.RWA_WSIB_INNERBANK(
                DATADATE                               --��������
                ,ORGID                             	 	 --����ID
                ,ACCSERIALNO                        	 --�����˺�
                ,BUSINESSTYPE													 --ҵ������
                ,BELONGORGCODE                         --ҵ����������
                ,IACCURBAL														 --��ǰ���
                ,IACCRTDAT														 --��ʼ��
                ,IACDLTDAT														 --������
                ,CLIENTNAME                         	 --���׶�������
                ,ORGANIZATIONCODE                   	 --���׶�����֯��������
                ,COUNTRYCODE                        	 --���׶��ֹ��Ҵ���
                ,INDUSTRYID                         	 --���׶�����ҵ����
                ,CLIENTCATEGORY												 --�ͻ�����
    )
    WITH TMP_SUPPORG AS (
								SELECT T1.ORGID AS ORGID
										   ,CASE WHEN T3.ORGLEVEL > 2 AND T3.SORTNO LIKE '%610' THEN T4.SORTNO WHEN T1.ORGID = '01370000' THEN '1100000' ELSE T3.SORTNO END AS SORTNO  --�������г�����Ҫ�ᵽ���в㼶��������������ǿ�Ʒֵ����н���ͬҵ����
								  FROM RWA.RWA_WP_SUPPTASKORG T1
						INNER JOIN RWA.RWA_WP_SUPPTASK T2
								    ON T1.SUPPTASKID = T2.SUPPTASKID
								   AND T2.ENABLEFLAG = '01'
						 LEFT JOIN RWA.ORG_INFO T3
								    ON T1.ORGID = T3.ORGID
						 LEFT JOIN RWA.ORG_INFO T4
	                	ON T3.BELONGORGID = T4.ORGID
								 WHERE T1.SUPPTMPLID = 'M-0050'
							ORDER BY T3.SORTNO
		)
		, TMP_BL_CUST AS (
								SELECT CUSTOMERNAME, CERTID
									FROM RWA_DEV.BL_CUSTOMER_INFO
								WHERE CERTTYPE = 'Ent02'
									AND ROWID IN (SELECT MAX(ROWID)
											            FROM RWA_DEV.BL_CUSTOMER_INFO
											           WHERE CERTTYPE = 'Ent02'
											           GROUP BY CUSTOMERNAME)
		)
		SELECT
								DATADATE                               --��������
                ,ORGID                             	 	 --����ID
                ,ACCSERIALNO                        	 --�����˺�
                ,BUSINESSTYPE													 --ҵ������
                ,BELONGORGCODE                         --ҵ����������
                ,IACCURBAL														 --��ǰ���
                ,IACCRTDAT														 --��ʼ��
                ,IACDLTDAT														 --������
                ,CLIENTNAME                         	 --���׶�������
                ,ORGANIZATIONCODE                   	 --���׶�����֯��������
                ,COUNTRYCODE                        	 --���׶��ֹ��Ҵ���
                ,INDUSTRYID                         	 --���׶�����ҵ����
                ,CLIENTCATEGORY												 --�ͻ�����
		FROM (
    SELECT
                TO_DATE(p_data_dt_str,'YYYYMMDD')            AS DATADATE                 --��������
                ,NVL(T7.ORGID,(SELECT ORGID FROM TMP_SUPPORG WHERE ROWNUM = 1))
                												                     AS ORGID										 --����ID                     		���ղ�¼����������������������г�������ֲ�
                ,RANK() OVER(PARTITION BY T1.ACCNO ORDER BY LENGTH(NVL(T7.SORTNO,(SELECT SORTNO FROM TMP_SUPPORG WHERE ROWNUM = 1))) DESC)
                																						 AS RECORDNO								 --�������
                ,T1.ACCNO		                             		 AS ACCSERIALNO              --�����˺�
                ,'01'																				 AS BUSINESSTYPE						 --ҵ������												Ĭ�ϣ�01-���
                ,T2.IACGACBR																 AS BELONGORGCODE            --ҵ����������
                ,ABS(T2.IACCURBAL)													 AS IACCURBAL								 --��ǰ���
                ,TO_CHAR(TO_DATE(T1.SDATE,'YYYYMMDD'),'YYYY-MM-DD')
                																						 AS IACCRTDAT								 --��ʼ��
                ,TO_CHAR(TO_DATE(T1.EDATE,'YYYYMMDD'),'YYYY-MM-DD')
                																						 AS IACDLTDAT								 --������
                ,NVL(T4.CLIENTNAME,T1.CUSTNAME)              AS CLIENTNAME               --���׶�������
                ,NVL(T4.ORGANIZATIONCODE,T8.CERTID)          AS ORGANIZATIONCODE         --���׶�����֯��������
                ,NVL(T4.COUNTRYCODE,'CHN')                   AS COUNTRYCODE              --���׶��ֹ��Ҵ���    						Ĭ��CHN-�й�
                ,NVL(T4.INDUSTRYID,'J66')					           AS INDUSTRYID               --���׶�����ҵ���� 							Ĭ��J66-���ҽ��ڷ���
                ,NVL(T4.CLIENTCATEGORY,'0202')					     AS CLIENTCATEGORY           --�ͻ�����					 							Ĭ��0202-�й���ҵ����

    FROM				RWA_DEV.CBS_BRW T1	             		 											--�ʽ���Ǽǲ�
		INNER JOIN 	RWA_DEV.CBS_IAC T2																				--ͨ�÷ֻ���
		ON 					T1.ACCNO = T2.IACAC_NO
		AND					T2.IACCURBAL <> 0																					--�˻�������0
		AND					T2.DATANO = p_data_dt_str
		LEFT	JOIN	RWA.ORG_INFO T3																						--������Ϣ��
	  ON					T2.IACGACBR = T3.ORGID
	  LEFT JOIN   (SELECT ACCSERIALNO
	  									 ,CLIENTNAME
	  									 ,ORGANIZATIONCODE
	  									 ,COUNTRYCODE
	  									 ,INDUSTRYID
	  									 ,CLIENTCATEGORY
	  							 FROM RWA.RWA_WS_INNERBANK
	  							WHERE BUSINESSTYPE = '01'
    								AND DATADATE = (SELECT MAX(DATADATE) FROM RWA.RWA_WS_INNERBANK WHERE DATADATE < TO_DATE(p_data_dt_str,'YYYYMMDD') AND BUSINESSTYPE = '01')
    						) T4																											--ȡ���һ�ڲ�¼�����̵�
    ON          T1.ACCNO = T4.ACCSERIALNO
    LEFT	JOIN  TMP_SUPPORG T7
    ON          T3.SORTNO LIKE T7.SORTNO || '%'
    LEFT	JOIN	TMP_BL_CUST T8
    ON					T1.CUSTNAME = T8.CUSTOMERNAME
		WHERE 			T1.FLAG = '1'																							--����־ ҵ����0�����룻1�����
    AND 				T1.STATUS = '00'																					--״̬ 00��������01���ѹ黹
    AND NOT EXISTS (SELECT 1 FROM RWA_DEV.NCM_BUSINESS_DUEBILL CBD WHERE 'BRW' || T1.CNTRNO = CBD.THIRDPARTYACCOUNTS AND CBD.DATANO = p_data_dt_str)
    AND 				T1.DATANO = p_data_dt_str
    )
    WHERE RECORDNO = 1
    ORDER BY		ACCSERIALNO
		;

    COMMIT;

    /*Ŀ�������ͳ��*/
    --ͳ�Ʋ���ļ�¼��
    --SELECT COUNT(1) INTO v_count FROM RWA.RWA_WSIB_INNERBANK WHERE BUSINESSTYPE = '01' AND DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD');
    --Dbms_output.Put_line('RWA.RWA_WSIB_INNERBANK��ǰ����ĺ���ϵͳ-���ͬҵ�̵����ݼ�¼Ϊ: ' || v_count || ' ��');


    --2.2 ����ϵͳ-���ͬҵҵ��
    INSERT INTO RWA.RWA_WSIB_INNERBANK(
                DATADATE                               --��������
                ,ORGID                             	 	 --����ID
                ,ACCSERIALNO                        	 --�����˺�
                ,BUSINESSTYPE													 --ҵ������
                ,BELONGORGCODE                         --ҵ����������
                ,IACCURBAL														 --��ǰ���
                ,IACCRTDAT														 --��ʼ��
                ,IACDLTDAT														 --������
                ,CLIENTNAME                         	 --���׶�������
                ,ORGANIZATIONCODE                   	 --���׶�����֯��������
                ,COUNTRYCODE                        	 --���׶��ֹ��Ҵ���
                ,INDUSTRYID                         	 --���׶�����ҵ����
                ,CLIENTCATEGORY												 --�ͻ�����
    )
    WITH TMP_SUPPORG AS (
								SELECT T1.ORGID AS ORGID
										   ,CASE WHEN T3.ORGLEVEL > 2 AND T3.SORTNO LIKE '%610' THEN T4.SORTNO WHEN T1.ORGID = '01370000' THEN '1100000' ELSE T3.SORTNO END AS SORTNO  --�������г�����Ҫ�ᵽ���в㼶��������������ǿ�Ʒֵ����н���ͬҵ����
								  FROM RWA.RWA_WP_SUPPTASKORG T1
						INNER JOIN RWA.RWA_WP_SUPPTASK T2
								    ON T1.SUPPTASKID = T2.SUPPTASKID
								   AND T2.ENABLEFLAG = '01'
						 LEFT JOIN RWA.ORG_INFO T3
								    ON T1.ORGID = T3.ORGID
						 LEFT JOIN RWA.ORG_INFO T4
	                	ON T3.BELONGORGID = T4.ORGID
								 WHERE T1.SUPPTMPLID = 'M-0050'
							ORDER BY T3.SORTNO
		)
		, TMP_BL_CUST AS (
								SELECT CUSTOMERNAME, CERTID
									FROM RWA_DEV.BL_CUSTOMER_INFO
								WHERE CERTTYPE = 'Ent02'
									AND ROWID IN (SELECT MAX(ROWID)
											            FROM RWA_DEV.BL_CUSTOMER_INFO
											           WHERE CERTTYPE = 'Ent02'
											           GROUP BY CUSTOMERNAME)
		)
		SELECT
								DATADATE                               --��������
                ,ORGID                             	 	 --����ID
                ,ACCSERIALNO                        	 --�����˺�
                ,BUSINESSTYPE													 --ҵ������
                ,BELONGORGCODE                         --ҵ����������
                ,IACCURBAL														 --��ǰ���
                ,IACCRTDAT														 --��ʼ��
                ,IACDLTDAT														 --������
                ,CLIENTNAME                         	 --���׶�������
                ,ORGANIZATIONCODE                   	 --���׶�����֯��������
                ,COUNTRYCODE                        	 --���׶��ֹ��Ҵ���
                ,INDUSTRYID                         	 --���׶�����ҵ����
                ,CLIENTCATEGORY												 --�ͻ�����
		FROM (
    SELECT
                TO_DATE(p_data_dt_str,'YYYYMMDD')            AS DATADATE                 --��������
                ,NVL(T7.ORGID,(SELECT ORGID FROM TMP_SUPPORG WHERE ROWNUM = 1))
                												                     AS ORGID										 --����ID                     		���ղ�¼����������������������г�������ֲ�
                ,RANK() OVER(PARTITION BY T1.IACAC_NO ORDER BY LENGTH(NVL(T7.SORTNO,(SELECT SORTNO FROM TMP_SUPPORG WHERE ROWNUM = 1))) DESC)
                																						 AS RECORDNO								 --�������
                ,T1.IACAC_NO								                 AS ACCSERIALNO              --�����˺�
                ,'02'																				 AS BUSINESSTYPE						 --ҵ������												Ĭ�ϣ�02-���
                ,T1.IACGACBR																 AS BELONGORGCODE            --ҵ����������
                ,ABS(T1.IACCURBAL)													 AS IACCURBAL								 --��ǰ���
                ,NVL(T4.IACCRTDAT,TO_CHAR(TO_DATE(NVL(T2.SDATE,T1.IACCRTDAT),'YYYYMMDD'),'YYYY-MM-DD'))
                																				 		 AS IACCRTDAT								 --��ʼ��
                ,NVL(T4.IACDLTDAT,TO_CHAR(TO_DATE(NVL(T2.EDATE,T1.IACDLTDAT),'YYYYMMDD'),'YYYY-MM-DD'))
                																				 		 AS IACDLTDAT								 --������
                ,NVL(T4.CLIENTNAME,T1.IACAC_NAM)	           AS CLIENTNAME               --���׶�������
                ,NVL(T4.ORGANIZATIONCODE,T8.CERTID)          AS ORGANIZATIONCODE         --���׶�����֯��������
                ,NVL(T4.COUNTRYCODE,'CHN')                   AS COUNTRYCODE              --���׶��ֹ��Ҵ���    						Ĭ��CHN-�й�
                ,NVL(T4.INDUSTRYID,'J66')					           AS INDUSTRYID               --���׶�����ҵ���� 							Ĭ��J66-���ҽ��ڷ���
                ,NVL(T4.CLIENTCATEGORY,'0202')					     AS CLIENTCATEGORY           --�ͻ�����					 							Ĭ��0202-�й���ҵ����

    FROM				RWA_DEV.CBS_IAC T1	             		 											--ͨ�÷ֻ���
    LEFT JOIN		RWA_DEV.CBS_BBR T2
    ON					T1.IACAC_NO = T2.ACCNO
		AND					T2.TYPE IN ('1','0')																			--ͬҵ����Ϊ 0-���ͬҵ��1-�������
    AND 				T2.STATUS = '00'																					--�˻�״̬Ϊ 00-����
    AND 				T2.DATANO = p_data_dt_str
		LEFT JOIN		RWA.ORG_INFO T3																						--������Ϣ��
	  ON					T1.IACGACBR = T3.ORGID
	  LEFT JOIN   (SELECT ACCSERIALNO
	  									 ,CLIENTNAME
	  									 ,ORGANIZATIONCODE
	  									 ,COUNTRYCODE
	  									 ,INDUSTRYID
	  									 ,IACCRTDAT
	  									 ,IACDLTDAT
	  									 ,CLIENTCATEGORY
	  							 FROM RWA.RWA_WS_INNERBANK
	  							WHERE BUSINESSTYPE = '02'
    								AND DATADATE = (SELECT MAX(DATADATE) FROM RWA.RWA_WS_INNERBANK WHERE DATADATE < TO_DATE(p_data_dt_str,'YYYYMMDD') AND BUSINESSTYPE = '02')
    						) T4																											--ȡ���һ�ڲ�¼�����̵�
    ON          T1.IACAC_NO = T4.ACCSERIALNO
    LEFT	JOIN  TMP_SUPPORG T7
    ON          T3.SORTNO LIKE T7.SORTNO || '%'
    LEFT	JOIN	TMP_BL_CUST T8
    ON					T1.IACAC_NAM = T8.CUSTOMERNAME
		WHERE 			T1.IACAC_STS = '2'																				--����״̬Ϊ��2-����
    AND					T1.IACCURBAL <> 0																					--�˻�������0
    AND					T1.IACITMNO LIKE '1011%'																	--1003-����������п���(��Ϊ�������嶼�����У�ͳһ����);1011-���ͬҵ
    AND NOT EXISTS (SELECT 1 FROM RWA_DEV.NCM_BUSINESS_DUEBILL CBD WHERE 'BBR' || T2.ACCNO || '_' || T2.ACCNOSEQ = CBD.THIRDPARTYACCOUNTS AND CBD.DATANO = p_data_dt_str)
		AND					T1.DATANO = p_data_dt_str
    )
    WHERE RECORDNO = 1
    ORDER BY		ACCSERIALNO
		;

    COMMIT;

    dbms_stats.gather_table_stats(ownname => 'RWA',tabname => 'RWA_WSIB_INNERBANK',cascade => true);

    /*Ŀ�������ͳ��*/
    --ͳ�Ʋ���ļ�¼��
    SELECT COUNT(1) INTO v_count FROM RWA.RWA_WSIB_INNERBANK WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD');
    --SELECT COUNT(1) INTO v_count FROM RWA.RWA_WSIB_INNERBANK WHERE BUSINESSTYPE = '02' AND DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD');
    --Dbms_output.Put_line('RWA.RWA_WSIB_INNERBANK��ǰ����ĺ���ϵͳ-���ͬҵ�̵����ݼ�¼Ϊ: ' || v_count || ' ��');



    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̽��� ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    p_po_rtncode := '1';
	  p_po_rtnmsg  := '�ɹ�' || '-' || v_count;
		--�����쳣
		EXCEPTION
    WHEN OTHERS THEN
				 --Dbms_output.Put_line('������,�������Ϊ:'||sqlcode||';������ϢΪ:'||sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace);
       	 ROLLBACK;
   			 p_po_rtncode := sqlcode;
   			 p_po_rtnmsg  := '�������ͬҵ��¼�����̵�('|| v_pro_name ||')����ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_TY_WSIB;
/

