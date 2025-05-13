CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_PJ_WSIB(
			 											p_data_dt_str	IN	VARCHAR2,		--�������� yyyyMMdd
       											p_po_rtncode	OUT	VARCHAR2,		--���ر�� 1 �ɹ�,0 ʧ��
														p_po_rtnmsg		OUT	VARCHAR2		--��������
				)
  /*
    �洢��������:RWA_DEV.PRO_RWA_PJ_WSIB
    ʵ�ֹ���:����ϵͳ-Ʊ��ת����-��¼�̵�(������Դ����ϵͳ��ҵ�������Ϣȫ������RWAƱ��ת���ֲ�¼�̵ױ���)
    ���ݿھ�:ȫ��
    ����Ƶ��:�³�����
    ��  ��  :V1.0.0
    ��д��  :LISY
    ��дʱ��:2016-06-06
    ��  λ  :�Ϻ���˶��Ϣ�����ɷ����޹�˾
    Դ  ��1 :RWA_DEV.CBS_LNU|���ֿ�Ƭ��
    Դ  ��2 :RWA_DEV.CBS_LNM|�������
    Դ  ��3 :RWA_DEV.CBS_ACS|�ʻ���ͻ���ϵ���ϱ�
    Դ  ��4 :RWA_DEV.CMS_CUSTOMER_INFO|ͳһ�ͻ���Ϣ��
    Դ  ��5 :RWA.ORG_INFO|������Ϣ��
    Դ  ��6 :RWA.RWA_WS_BO_BILLREDISCOUNT|�ⲿƱ��ת����_��Ʊ��¼��
    Դ  ��7 :RWA.RWA_WS_CO_BILLREDISCOUNT|�ⲿƱ��ת����_��Ʊ��¼��
    Դ  ��8 :RWA.RWA_WS_BI_BILLREDISCOUNT|�ڲ�Ʊ��ת����_��Ʊ��¼��
    Դ  ��9 :RWA.RWA_WP_SUPPTASKORG|��¼��������ַ����ñ�
    Դ  ��10:RWA.RWA_WP_SUPPTASK|��¼���񷢲���
    Դ  ��11:RWA_DEV.BL_CUSTOMER_INFO|��¼�ͻ����ܱ�
    Ŀ���1 :RWA.RWA_WSIB_BO_BILLREDISCOUNT|�ⲿƱ��ת����_��Ʊ�̵ױ�
    Ŀ���2 :RWA.RWA_WSIB_CO_BILLREDISCOUNT|�ⲿƱ��ת����_��Ʊ�̵ױ�
    Ŀ���3 :RWA.RWA_WSIB_BI_BILLREDISCOUNT|�ڲ�Ʊ��ת����_��Ʊ�̵ױ�
    �����¼(�޸���|�޸�ʱ��|�޸�����):
    */
  AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_PJ_WSIB';
  --�����쳣����
  v_raise EXCEPTION;
  --���嵱ǰ����ļ�¼��
  v_count INTEGER;

  BEGIN
    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*�����ȫ�����ݼ��������Ŀ���*/
    --1.���Ŀ����е�ԭ�м�¼
    --���Ʊ��ת���ֲ�¼�̵ױ�
    DELETE FROM RWA.RWA_WSIB_BI_BILLREDISCOUNT WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD');

    COMMIT;

    --2.���������������ݴ�Դ����뵽Ŀ�����
    --2.1 ����ϵͳ-�ڲ�Ʊ��ת����_��Ʊ
    INSERT INTO RWA.RWA_WSIB_BI_BILLREDISCOUNT(
                DATADATE                               --��������
                ,ORGID                             	 	 --����ID
                ,LNUCAR_NO                           	 --�˺�
                ,LNUCARNO                            	 --��Ƭ��
                ,LNUCERNO                            	 --Ʊ�ݱ��
                ,LNMAC_NAM														 --����
                ,LNUDISAMT														 --���ֽ��
                ,LNUCURBAL														 --Ʊ�����
                ,LNUDISDAT														 --��������
                ,LNUEXPDAT														 --���ֵ�������
                ,BELONGORGCODE			                 	 --ҵ����������
                ,ACCEPTOR                            	 --�ж�������
                ,ACCEPTORGCODE                       	 --�ж�����֯��������
                ,ACCEPTCOUNTRYCODE                   	 --�ж���ע����Ҵ���
                ,ACCEPTINDUSTRYID              			 	 --�ж���������ҵ����
                ,CLIENTCATEGORY												 --�ж��пͻ�����
    )
    WITH TMP_SUPPORG AS (
								SELECT T1.ORGID AS ORGID
										   ,CASE WHEN T3.ORGLEVEL > 2 THEN T4.SORTNO ELSE T3.SORTNO END AS SORTNO
								  FROM RWA.RWA_WP_SUPPTASKORG T1
						INNER JOIN RWA.RWA_WP_SUPPTASK T2
								    ON T1.SUPPTASKID = T2.SUPPTASKID
								   AND T2.ENABLEFLAG = '01'
						 LEFT JOIN RWA.ORG_INFO T3
								    ON T1.ORGID = T3.ORGID
						 LEFT JOIN RWA.ORG_INFO T4
	                	ON T3.BELONGORGID = T4.ORGID
								 WHERE T1.SUPPTMPLID = 'M-0043'
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
                ,LNUCAR_NO                           	 --�˺�
                ,LNUCARNO                            	 --��Ƭ��
                ,LNUCERNO                            	 --Ʊ�ݱ��
                ,LNMAC_NAM														 --����
                ,LNUDISAMT														 --���ֽ��
                ,LNUCURBAL														 --Ʊ�����
                ,LNUDISDAT														 --��������
                ,LNUEXPDAT														 --���ֵ�������
                ,BELONGORGCODE			                 	 --ҵ����������
                ,ACCEPTOR                            	 --�ж�������
                ,ACCEPTORGCODE                       	 --�ж�����֯��������
                ,ACCEPTCOUNTRYCODE                   	 --�ж���ע����Ҵ���
                ,ACCEPTINDUSTRYID              			 	 --�ж���������ҵ����
                ,CLIENTCATEGORY												 --�ж��пͻ�����
		FROM (
    SELECT
                TO_DATE(p_data_dt_str,'YYYYMMDD')            AS DATADATE                 --��������
                ,NVL(T7.ORGID,(SELECT ORGID FROM TMP_SUPPORG WHERE ROWNUM = 1))
                												                     AS ORGID										 --����ID                     		���ղ�¼����������������������г�������ֲ�
                ,RANK() OVER(PARTITION BY T1.LNUAC_NO,T1.LNUCARNO ORDER BY LENGTH(NVL(T7.SORTNO,(SELECT SORTNO FROM TMP_SUPPORG WHERE ROWNUM = 1))) DESC)
                																						 AS RECORDNO								 --�������
                ,T1.LNUAC_NO                             		 AS LNUCAR_NO           		 --�˺�
                ,T1.LNUCARNO																 AS LNUCARNO                 --��Ƭ��
                ,T1.LNUCERNO                          			 AS LNUCERNO               	 --Ʊ�ݱ��
                ,T2.LNMAC_NAM														 		 AS LNMAC_NAM								 --����
                ,ABS(T1.LNUDISAMT)											 		 AS LNUDISAMT								 --���ֽ��
                ,ABS(T1.LNUCURBAL)											 		 AS LNUCURBAL								 --Ʊ�����
                ,TO_CHAR(TO_DATE(T1.LNUDISDAT,'YYYYMMDD'),'YYYY-MM-DD')
                																				 		 AS LNUDISDAT								 --��������
                ,TO_CHAR(TO_DATE(T1.LNUEXPDAT,'YYYYMMDD'),'YYYY-MM-DD')
                																				 		 AS LNUEXPDAT								 --���ֵ�������
                ,T2.LNMGACBK	                   				 		 AS BELONGORGCODE            --ҵ����������
                ,NVL(T6.ACCEPTOR,T1.LNCCERPAY)           		 AS ACCEPTOR                 --�ж�������
                ,NVL(T6.ACCEPTORGCODE,T8.CERTID)						 AS ACCEPTORGCODE            --�ж�����֯��������
                ,NVL(T6.ACCEPTCOUNTRYCODE,'CHN')           	 AS ACCEPTCOUNTRYCODE        --�ж���ע����Ҵ���							Ĭ��CHN-�й�
                ,NVL(T6.ACCEPTINDUSTRYID,'J66')              AS ACCEPTINDUSTRYID         --�ж���������ҵ����							Ĭ��J66-���ҽ��ڷ���
                ,NVL(T6.CLIENTCATEGORY,'0202')               AS CLIENTCATEGORY	         --�ж��пͻ�����									Ĭ��0202-�й���ҵ����

    FROM				RWA_DEV.CBS_LNU T1	             		 															--���ֿ�Ƭ��
		INNER JOIN 	RWA_DEV.CBS_LNM T2																								--�������
		ON 					T1.LNUAC_NO = T2.LNMAC_NO
		AND					T2.DATANO = p_data_dt_str
		AND         T2.LNMITMNO IN ('13010501','13010505','13010511')      						--13010501-�����ʲ�-ֱ�����гжһ�Ʊ����13010505-�����ʲ�-ת�����гжһ�Ʊ����13010511-�����ʲ�-��ת�������гжһ�Ʊ����
	  LEFT	JOIN	RWA.ORG_INFO T5																										--������Ϣ��
	  ON					T2.LNMGACBK = T5.ORGID
	  LEFT	JOIN	(SELECT LNUCAR_NO
	  									 ,LNUCARNO
	  									 ,ACCEPTOR
	  									 ,ACCEPTORGCODE
	  									 ,ACCEPTCOUNTRYCODE
	  									 ,ACCEPTINDUSTRYID
	  									 ,CLIENTCATEGORY
	  							 FROM RWA.RWA_WS_BI_BILLREDISCOUNT
	  							WHERE DATADATE = (SELECT MAX(DATADATE) FROM RWA.RWA_WS_BI_BILLREDISCOUNT WHERE DATADATE < TO_DATE(p_data_dt_str,'YYYYMMDD'))
	  						) T6																															--ȡ���һ�ڲ�¼�����̵�
	  ON					T1.LNUAC_NO = T6.LNUCAR_NO
	  AND					T1.LNUCARNO = T6.LNUCARNO
	  LEFT	JOIN  TMP_SUPPORG T7
    ON          T5.SORTNO LIKE T7.SORTNO || '%'
    LEFT	JOIN	TMP_BL_CUST T8
    ON					T1.LNCCERPAY = T8.CUSTOMERNAME
		WHERE				T1.LNURVSFLG = '0' 																								--Ĩ�˱�ʶΪ����
		AND 				T1.LNUCURBAL <> 0																									--������0
		AND					T1.DATANO = p_data_dt_str
		)
		WHERE RECORDNO = 1
		ORDER BY 		LNUCAR_NO,TO_NUMBER(LNUCARNO)
		;

    COMMIT;

    dbms_stats.gather_table_stats(ownname => 'RWA',tabname => 'RWA_WSIB_BI_BILLREDISCOUNT',cascade => true);

    /*Ŀ�������ͳ��*/
    --ͳ�Ʋ���ļ�¼��
    SELECT COUNT(1) INTO v_count FROM RWA.RWA_WSIB_BI_BILLREDISCOUNT WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD');
    --Dbms_output.Put_line('RWA.RWA_WSIB_BI_BILLREDISCOUNT��ǰ����ĺ���ϵͳ-�ⲿƱ��ת����_��Ʊ�̵����ݼ�¼Ϊ: ' || v_count1 || ' ��');


    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̽��� ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    p_po_rtncode := '1';
	  p_po_rtnmsg  := '�ɹ�' || '-' || v_count;
		--�����쳣
		EXCEPTION
    WHEN OTHERS THEN
				 --Dbms_output.Put_line('������,�������Ϊ:'||sqlcode||';������ϢΪ:'||sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace);
       	 ROLLBACK;
   			 p_po_rtncode := sqlcode;
   			 p_po_rtnmsg  := 'Ʊ��ת����ҵ��¼�����̵�('|| v_pro_name ||')����ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_PJ_WSIB;
/

