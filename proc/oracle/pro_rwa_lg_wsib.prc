CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_LG_WSIB(
			 											p_data_dt_str	IN	VARCHAR2,		--�������� yyyyMMdd
       											p_po_rtncode	OUT	VARCHAR2,		--���ر�� 1 �ɹ�,0 ʧ��
														p_po_rtnmsg		OUT	VARCHAR2		--��������
				)
  /*
    �洢��������:RWA_DEV.PRO_RWA_LG_WSIB
    ʵ�ֹ���:��������������ͷ�����������������̵ף�ȡ����Ŀ
    ���ݿھ�:ȫ��
    ����Ƶ��:�³�����
    ��  ��  :V1.0.0
    ��д��  :LISY
    ��дʱ��:2016-11-07
    ��  λ  :�Ϻ���˶��Ϣ�����ɷ����޹�˾
    Դ  ��1 :RWA_DEV.NCM_BUSINESS_CONTRACT|����ҵ���ͬ��
    Դ  ��2 :RWA_DEV.NCM_BUSINESS_TYPE|ҵ��Ʒ����Ϣ��
    Դ  ��3 :RWA.ORG_INFO|������Ϣ��
    Դ  ��4 :RWA.RWA_WS_LG|�Ŵ�������¼��
    Դ  ��5 :RWA.RWA_WP_SUPPTASKORG|��¼��������ַ����ñ�
    Դ  ��6 :RWA.RWA_WP_SUPPTASK|��¼���񷢲���
    Ŀ���1 :RWA.RWA_WSIB_LG|�Ŵ������̵ױ�
    �����¼(�޸���|�޸�ʱ��|�޸�����):
    */
  AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_LG_WSIB';
  --�����쳣����
  v_raise EXCEPTION;
  --���嵱ǰ����ļ�¼��
  v_count INTEGER;

  BEGIN
    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*�����ȫ�����ݼ��������Ŀ���*/
    --1.���Ŀ����е�ԭ�м�¼
    --��ղ������ͬҵ�̵ױ�
    DELETE FROM RWA.RWA_WSIB_LG WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD');

    COMMIT;

    --2.���������������ݴ�Դ����뵽Ŀ�����
    --2.1 �Ŵ�ϵͳ-���Ᵽ���Ͷ��ⵣ��
    INSERT INTO RWA.RWA_WSIB_LG(
                 DATADATE                              --��������
                ,ORGID                             	 	 --����ID
                ,CONTRACTNO                        	   --��ͬ��ˮ��
                ,BELONGORGCODE                         --ҵ����������
                ,CUSTOMERNAME                          --�ͻ�����
                ,BUSINESSTYPE													 --ҵ������
                ,LGTYPE                                --��������
                ,BEGINDATE                             --��ͬ��ʼ��
                ,ENDDATE                         	     --��ͬ������
                ,CURRENCY                   	         --����
                ,BALANCE                        	     --��ͬ���
                ,FINANCEFLAG                         	 --�Ƿ�������ҵ��
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
								 WHERE T1.SUPPTMPLID = 'M-0200'
							ORDER BY T3.SORTNO
		)
		SELECT
								 DATADATE                              --��������
                ,ORGID                             	 	 --����ID
                ,CONTRACTNO                        	   --��ͬ��ˮ��
                ,BELONGORGCODE                         --ҵ����������
                ,CUSTOMERNAME                          --�ͻ�����
                ,BUSINESSTYPE													 --ҵ������
                ,LGTYPE                                --��������
                ,BEGINDATE                             --��ͬ��ʼ��
                ,ENDDATE                         	     --��ͬ������
                ,CURRENCY                   	         --����
                ,BALANCE                        	     --��ͬ���
                ,FINANCEFLAG                         	 --�Ƿ�������ҵ��
		FROM (
    SELECT
                TO_DATE(p_data_dt_str,'YYYYMMDD')            AS DATADATE                 --��������
                ,NVL(T7.ORGID,(SELECT ORGID FROM TMP_SUPPORG WHERE ROWNUM = 1))
                												                     AS ORGID										 --����ID                     		���ղ�¼����������������������г�������ֲ�
                ,RANK() OVER(PARTITION BY T1.SERIALNO ORDER BY LENGTH(NVL(T7.SORTNO,(SELECT SORTNO FROM TMP_SUPPORG WHERE ROWNUM = 1))) DESC)
                																						 AS RECORDNO								 --�������
                ,T1.SERIALNO		                             AS CONTRACTNO               --��ͬ��
                ,T1.OPERATEORGID														 AS BELONGORGCODE						 --ҵ����������
                ,T1.CUSTOMERNAME													   AS CUSTOMERNAME             --�ͻ�����
                ,T1.BUSINESSTYPE                             AS BUSINESSTYPE             --ҵ������
                ,T1.SAFEGUARDTYPE			                       AS LGTYPE                   --��������
                ,TO_CHAR(TO_DATE(T1.PUTOUTDATE,'YYYYMMDD'),'YYYY-MM-DD')
                                                             AS BEGINDATE                --��ͬ��ʼ��
                ,TO_CHAR(TO_DATE(T1.MATURITY,'YYYYMMDD'),'YYYY-MM-DD')
                                                             AS ENDDATE                  --��ͬ������
                ,T1.BUSINESSCURRENCY                         AS CURRENCY                 --����
                ,T1.BALANCE                                  AS BALANCE                  --��ͬ���
                ,T4.FINANCEFLAG                              AS FINANCEFLAG              --�Ƿ�������ҵ��
    FROM        RWA_DEV.NCM_BUSINESS_CONTRACT T1
		LEFT	JOIN	RWA.ORG_INFO T3																						--������Ϣ��
	  ON					T1.OPERATEORGID = T3.ORGID
	  LEFT JOIN   (SELECT CONTRACTNO
	  									 ,FINANCEFLAG
	  							 FROM RWA.RWA_WS_LG
	  							WHERE DATADATE = (SELECT MAX(DATADATE) FROM RWA.RWA_WS_LG WHERE DATADATE < TO_DATE(p_data_dt_str,'YYYYMMDD'))
	  						) T4																											--ȡ���һ�ڲ�¼�����̵�
    ON          T1.SERIALNO = T4.CONTRACTNO
    LEFT	JOIN  TMP_SUPPORG T7
    ON          T3.SORTNO LIKE T7.SORTNO || '%'
		WHERE       T1.DATANO = P_DATA_DT_STR
    AND         T1.BUSINESSTYPE IN ('105120','102050')       							--���ڱ���ֱ��Ĭ�������ౣ�������ڱ������ò�¼
    AND         T1.BALANCE > 0
    AND         SUBSTR(T1.SERIALNO,3,8) <= P_DATA_DT_STR
    ORDER BY		T1.SERIALNO
    )
    WHERE RECORDNO = 1
		;

    COMMIT;

    /*Ŀ�������ͳ��*/
    --ͳ�Ʋ���ļ�¼��
    SELECT COUNT(1) INTO v_count FROM RWA.RWA_WSIB_LG WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD');



    p_po_rtncode := '1';
	  p_po_rtnmsg  := '�ɹ�' || '-' || v_count;
		--�����쳣
		EXCEPTION
    WHEN OTHERS THEN
				 --Dbms_output.Put_line('������,�������Ϊ:'||sqlcode||';������ϢΪ:'||sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace);
       	 ROLLBACK;
   			 p_po_rtncode := sqlcode;
   			 p_po_rtnmsg  := '�Ŵ�������¼�����̵�('|| v_pro_name ||')����ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_LG_WSIB;
/

