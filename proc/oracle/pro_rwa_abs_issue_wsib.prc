CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_ABS_ISSUE_WSIB(
			 											p_data_dt_str	IN	VARCHAR2,		--�������� yyyyMMdd
       											p_po_rtncode	OUT	VARCHAR2,		--���ر�� 1 �ɹ�,0 ʧ��
														p_po_rtnmsg		OUT	VARCHAR2		--��������
				)
  /*
    �洢��������:RWA_DEV.PRO_RWA_ABS_ISSUE_WSIB
    ʵ�ֹ���:����ϵͳ-ծȯͶ��(�ʲ�֤ȯ��-���л���)-��¼�̵�(������Դ����ϵͳ��ҵ�������Ϣȫ������RWA�ʲ�֤ȯ��-���л�����¼�̵ױ���)
    ���ݿھ�:ȫ��
    ����Ƶ��:�³�����
    ��  ��  :V1.0.0
    ��д��  :LISY
    ��дʱ��:2016-06-20
    ��  λ  :�Ϻ���˶��Ϣ�����ɷ����޹�˾
    Դ  ��1 :RWA_DEV.FNS_BND_INFO_B|����ϵͳծȯ��Ϣ��
    Դ  ��2 :RWA_DEV.FNS_BND_BOOK_B|����ϵͳ������
    Դ  ��3 :RWA.RWA_WS_ABS_ISSUE_EXPOSURE|�ʲ�֤ȯ��-���л���-���ձ�¶��¼��
    Դ  ��4 :RWA.CODE_LIBRARY|�������ñ�
    Ŀ���1 :RWA.RWA_WSIB_ABS_ISSUE_EXPOSURE|�ʲ�֤ȯ��-���л���-���ձ�¶�̵ױ�
    �����¼(�޸���|�޸�ʱ��|�޸�����):
    */
  AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_ABS_ISSUE_WSIB';
  --�����쳣����
  v_raise EXCEPTION;
  --���嵱ǰ����ļ�¼��
  v_count INTEGER;

  BEGIN
    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*�����ȫ�����ݼ��������Ŀ���*/
    --1.���Ŀ����е�ԭ�м�¼
    --����ʲ�֤ȯ��-���л�����¼�����̵��̵ױ�
    DELETE FROM RWA.RWA_WSIB_ABS_ISSUE_EXPOSURE WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD');

    COMMIT;

    --2.���������������ݴ�Դ����뵽Ŀ�����
    --2.1 ����ϵͳ-�ʲ�֤ȯ��Ͷ��
    INSERT INTO RWA.RWA_WSIB_ABS_ISSUE_EXPOSURE(
                DATADATE                               --��������
                ,ORGID                             	 	 --����ID
                ,ZQNM																	 --ծȯ����
                ,YHJS										 							 --���н�ɫ
                ,YWSSJG                  							 --ҵ����������
                ,ZCCBH                    	 		 	 		 --�ʲ��ر��
                ,ZCCDH                   							 --�ʲ��ش���
                ,ZCZQHMC                 							 --�ʲ�֤ȯ������
                ,TX                      							 --����
                ,FDSXH                   							 --�ֵ�˳���
                ,DCMC                    							 --��������
                ,SFZYXDC                 							 --�Ƿ������ȵ���
                ,ZZCZQHBZ                							 --���ʲ�֤ȯ����ʶ
                ,ZCYE                    							 --�ʲ����
                ,BZ                      							 --����
                ,JZZB                    							 --��ֵ׼��
                ,FXR                     							 --������
                ,DQR                     							 --������
                ,WBZXJGDPJJG             							 --�ⲿ���Ż�������������
                ,FQSHSTGZWBPJ            							 --����ʱ�����ṩ���ⲿ����
                ,DQHSTGZWBPJ             							 --��ǰ�����ṩ���ⲿ����
                ,XYFXHSSFTGGTBMDDJG      							 --���÷��ջ����Ƿ��ṩ���ر�Ŀ�Ļ���
                ,SFTGXYZCBFYDWBPJ        							 --�Ƿ��ṩ����֧�ֲ���ӳ���ⲿ����
                ,ZQWBPJJG                							 --ծȯ�ⲿ��������
                ,ZQWBPJQX                							 --ծȯ�ⲿ��������
                ,ZQWBPJDJ                							 --ծȯ�ⲿ�����ȼ�
                ,ZQWBPJRQ															 --ծȯ�ⲿ��������
                ,HGLDXBLBZ               							 --�ϸ������Ա�����ʶ
                ,HGXJTZBLBZ              							 --�ϸ��ֽ�͸֧������ʶ
                ,XJTZBLSFKSSWTJCX        							 --�ֽ�͸֧�����Ƿ����ʱ����������
                ,FXBLZB																 --���ձ�¶ռ��
                ,TQTHLXDM															 --��ǰ̯������
                ,LSCNLXDM															 --���۳�ŵ����
                ,SGYPJCELC														 --������ƽ����������
                ,CELCSDD															 --��������������
    )
    WITH TEMP_BND_BOOK AS (SELECT BOND_ID,
												       INITIAL_COST,
												       INT_ADJUST,
												       MKT_VALUE_CHANGE,
												       RECEIVABLE_INT,
												       ACCOUNTABLE_INT
												  FROM (SELECT BOND_ID,
												               INITIAL_COST,
												               INT_ADJUST,
												               MKT_VALUE_CHANGE,
												               RECEIVABLE_INT,
												               ACCOUNTABLE_INT,
												               ROW_NUMBER() OVER(PARTITION BY BOND_ID ORDER BY SORT_SEQ DESC) RM
												          FROM RWA_DEV.FNS_BND_BOOK_B
												         WHERE AS_OF_DATE <= p_data_dt_str
												           AND DATANO = p_data_dt_str)
												 WHERE RM = 1
												   AND (NVL(INITIAL_COST, 0) + NVL(INT_ADJUST, 0) + NVL(MKT_VALUE_CHANGE, 0) + NVL(ACCOUNTABLE_INT, 0) <> 0
												   OR BOND_ID IN (SELECT ITEMNO FROM RWA.CODE_LIBRARY WHERE CODENO = 'FNS_ABS_BOND' AND ISINUSE = '1'))
		)
		, TMP_SUPPORG AS (
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
								 WHERE T1.SUPPTMPLID = 'M-0131'
							ORDER BY T3.SORTNO
		)
		SELECT
								DATADATE                               --��������
                ,ORGID                             	 	 --����ID
                ,ZQNM																	 --ծȯ����
                ,YHJS										 							 --���н�ɫ
                ,YWSSJG                  							 --ҵ����������
                ,ZCCBH                    	 		 	 		 --�ʲ��ر��
                ,ZCCDH                   							 --�ʲ��ش���
                ,ZCZQHMC                 							 --�ʲ�֤ȯ������
                ,TX                      							 --����
                ,FDSXH                   							 --�ֵ�˳���
                ,DCMC                    							 --��������
                ,SFZYXDC                 							 --�Ƿ������ȵ���
                ,ZZCZQHBZ                							 --���ʲ�֤ȯ����ʶ
                ,ZCYE                    							 --�ʲ����
                ,BZ                      							 --����
                ,JZZB                    							 --��ֵ׼��
                ,FXR                     							 --������
                ,DQR                     							 --������
                ,WBZXJGDPJJG             							 --�ⲿ���Ż�������������
                ,FQSHSTGZWBPJ            							 --����ʱ�����ṩ���ⲿ����
                ,DQHSTGZWBPJ             							 --��ǰ�����ṩ���ⲿ����
                ,XYFXHSSFTGGTBMDDJG      							 --���÷��ջ����Ƿ��ṩ���ر�Ŀ�Ļ���
                ,SFTGXYZCBFYDWBPJ        							 --�Ƿ��ṩ����֧�ֲ���ӳ���ⲿ����
                ,ZQWBPJJG                							 --ծȯ�ⲿ��������
                ,ZQWBPJQX                							 --ծȯ�ⲿ��������
                ,ZQWBPJDJ                							 --ծȯ�ⲿ�����ȼ�
                ,ZQWBPJRQ															 --ծȯ�ⲿ��������
                ,HGLDXBLBZ               							 --�ϸ������Ա�����ʶ
                ,HGXJTZBLBZ              							 --�ϸ��ֽ�͸֧������ʶ
                ,XJTZBLSFKSSWTJCX        							 --�ֽ�͸֧�����Ƿ����ʱ����������
                ,FXBLZB																 --���ձ�¶ռ��
                ,TQTHLXDM															 --��ǰ̯������
                ,LSCNLXDM															 --���۳�ŵ����
                ,SGYPJCELC														 --������ƽ����������
                ,CELCSDD															 --��������������
		FROM (
    SELECT
                TO_DATE(p_data_dt_str,'YYYYMMDD')            AS DATADATE                 --��������
                ,(SELECT ORGID FROM TMP_SUPPORG WHERE ROWNUM = 1)
                						                     				 		 AS ORGID                		 --����ID                     		Ĭ�ϣ��ʲ���ծ����(01050000)
                ,T1.BOND_ID																	 AS ZQNM										 --ծȯ����
                ,T3.YHJS		                             		 AS YHJS										 --���н�ɫ
                ,T1.DEPARTMENT															 AS YWSSJG                   --ҵ����������
                ,T3.ZCCBH																		 AS ZCCBH                    --�ʲ��ر��
                ,T3.ZCCDH																		 AS ZCCDH                    --�ʲ��ش���
                ,T1.BOND_NAME                          			 AS ZCZQHMC                  --�ʲ�֤ȯ������
                ,T3.TX	                         				 		 AS TX                       --����
                ,T3.FDSXH                            	 			 AS FDSXH                    --�ֵ�˳���
                ,T3.DCMC				                             AS DCMC                     --��������
                ,T3.SFZYXDC																	 AS SFZYXDC                  --�Ƿ������ȵ���
                ,T3.ZZCZQHBZ                                 AS ZZCZQHBZ                 --���ʲ�֤ȯ����ʶ
                ,CASE WHEN T1.ASSET_CLASS = '40' AND T1.BOND_TYPE1 IN ('091','099') THEN
                			DECODE(T1.CLOSED,'1',nvl(T2.INITIAL_COST,0),0) +
                			DECODE(T1.CLOSED,'1',nvl(T2.INT_ADJUST,0),0) +
                			DECODE(T1.CLOSED,'1',nvl(T2.MKT_VALUE_CHANGE,0),0) +
                			DECODE(T1.CLOSED,'1',nvl(T2.ACCOUNTABLE_INT,0),0)
                 ELSE nvl(T2.INITIAL_COST,0) +
                 			nvl(T2.INT_ADJUST,0) +
                 			nvl(T2.MKT_VALUE_CHANGE,0) +
                 			nvl(T2.ACCOUNTABLE_INT,0)
                 END                                         AS ZCYE                     --�ʲ����
                ,NVL(T1.CURRENCY_CODE,'CNY')                 AS BZ                       --����
                ,T3.JZZB                                     AS JZZB                     --��ֵ׼��
                ,TO_CHAR(TO_DATE(T1.EFFECT_DATE,'YYYYMMDD'),'YYYY-MM-DD')
                														                 AS FXR                    	 --������
                ,TO_CHAR(TO_DATE(T1.MATURITY_DATE,'YYYYMMDD'),'YYYY-MM-DD')
                								                             AS DQR                    	 --������
                ,T3.WBZXJGDPJJG                              AS WBZXJGDPJJG              --�ⲿ���Ż�������������
                ,T3.FQSHSTGZWBPJ	                           AS FQSHSTGZWBPJ             --����ʱ�����ṩ���ⲿ����
                ,T3.DQHSTGZWBPJ                              AS DQHSTGZWBPJ              --��ǰ�����ṩ���ⲿ����
                ,T3.XYFXHSSFTGGTBMDDJG                       AS XYFXHSSFTGGTBMDDJG       --���÷��ջ����Ƿ��ṩ���ر�Ŀ�Ļ���
                ,T3.SFTGXYZCBFYDWBPJ                         AS SFTGXYZCBFYDWBPJ         --�Ƿ��ṩ����֧�ֲ���ӳ���ⲿ����
                ,T3.ZQWBPJJG                                 AS ZQWBPJJG                 --ծȯ�ⲿ��������
                ,T3.ZQWBPJQX                                 AS ZQWBPJQX                 --ծȯ�ⲿ��������
                ,T3.ZQWBPJDJ                                 AS ZQWBPJDJ                 --ծȯ�ⲿ�����ȼ�
                ,T3.ZQWBPJRQ																 AS ZQWBPJRQ								 --ծȯ�ⲿ��������
                ,T3.HGLDXBLBZ                                AS HGLDXBLBZ                --�ϸ������Ա�����ʶ
                ,T3.HGXJTZBLBZ                               AS HGXJTZBLBZ               --�ϸ��ֽ�͸֧������ʶ
                ,T3.XJTZBLSFKSSWTJCX                         AS XJTZBLSFKSSWTJCX         --�ֽ�͸֧�����Ƿ����ʱ����������
                ,T3.FXBLZB																	 AS FXBLZB									 --���ձ�¶ռ��
								,T3.TQTHLXDM	                               AS TQTHLXDM                 --��ǰ̯������
                ,T3.LSCNLXDM	                               AS LSCNLXDM	               --���۳�ŵ����
                ,T3.SGYPJCELC				                         AS SGYPJCELC				         --������ƽ����������
                ,T3.CELCSDD																	 AS CELCSDD									 --��������������

    FROM				RWA_DEV.FNS_BND_INFO_B T1
		INNER JOIN	TEMP_BND_BOOK T2
		ON 					T1.BOND_ID = T2.BOND_ID
		LEFT	JOIN	(SELECT ZQNM
											 ,YHJS
											 ,ZCCBH
											 ,ZCCDH
											 ,TX
											 ,FDSXH
											 ,DCMC
											 ,SFZYXDC
											 ,ZZCZQHBZ
											 ,JZZB
											 ,WBZXJGDPJJG
											 ,FQSHSTGZWBPJ
											 ,DQHSTGZWBPJ
											 ,XYFXHSSFTGGTBMDDJG
											 ,SFTGXYZCBFYDWBPJ
											 ,ZQWBPJJG
											 ,ZQWBPJQX
											 ,ZQWBPJDJ
											 ,ZQWBPJRQ
											 ,HGLDXBLBZ
											 ,HGXJTZBLBZ
											 ,XJTZBLSFKSSWTJCX
											 ,FXBLZB
											 ,TQTHLXDM
											 ,LSCNLXDM
											 ,SGYPJCELC
											 ,CELCSDD
									 FROM RWA.RWA_WS_ABS_ISSUE_EXPOSURE
									WHERE DATADATE = (SELECT MAX(DATADATE) FROM RWA.RWA_WS_ABS_ISSUE_EXPOSURE WHERE DATADATE < TO_DATE(p_data_dt_str,'YYYYMMDD'))
								) T3																				 				--ȡ���һ�ڲ�¼�����̵�
		ON					T1.BOND_ID = T3.ZQNM
		WHERE 			T1.ASSET_CLASS IN ('10','20','40')									--ͨ���ʲ�������ȷ��ծȯ����Ӧ�տ�Ͷ�ʡ�
																																		--10 �������ʲ�
																																		--20 �������������ʲ�
																																		--40 �ɹ��������ʲ�
		AND					(T1.BOND_TYPE1 = '060'															--�ʲ�֧��֤ȯ
								OR T1.BOND_ID IN (SELECT ITEMNO FROM RWA.CODE_LIBRARY WHERE CODENO = 'FNS_ABS_BOND' AND ISINUSE = '1')
								)																										--���ߴ����ñ��ȡ�ʲ�֤ȯ����ծȯ����
		AND 				T1.DATANO = p_data_dt_str														--ծȯ��Ϣ��,��ȡ��Ч��ծȯ��Ϣ
		AND NOT EXISTS (SELECT 1 FROM RWA.RWA_WS_ABS_INVEST_EXPOSURE WHERE DATADATE = (SELECT MAX(DATADATE) FROM RWA.RWA_WS_ABS_INVEST_EXPOSURE WHERE DATADATE < TO_DATE(p_data_dt_str,'YYYYMMDD')) AND T1.BOND_ID = ZQNM AND YHJS = '02')
		)
		WHERE				YHJS <> '02' 																				--��Ͷ�ʻ�����Ҫ�̵�
		OR					YHJS IS NULL
		ORDER BY		ZQNM,ZCCBH
		;

    COMMIT;

    dbms_stats.gather_table_stats(ownname => 'RWA',tabname => 'RWA_WSIB_ABS_ISSUE_EXPOSURE',cascade => true);

    /*Ŀ�������ͳ��*/
    --ͳ�Ʋ���ļ�¼��
    SELECT COUNT(1) INTO v_count FROM RWA.RWA_WSIB_ABS_ISSUE_EXPOSURE WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD');
    --Dbms_output.Put_line('RWA.RWA_WSIB_ABS_ISSUE_EXPOSURE��ǰ����Ĳ���ϵͳ-�ʲ�֤ȯ��-���л����̵����ݼ�¼Ϊ: ' || v_count || ' ��');



    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̽��� ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    p_po_rtncode := '1';
	  p_po_rtnmsg  := '�ɹ�' || '-' || v_count;
		--�����쳣
		EXCEPTION
    WHEN OTHERS THEN
				 --Dbms_output.Put_line('������,�������Ϊ:'||sqlcode||';������ϢΪ:'||sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace);
       	 ROLLBACK;
   			 p_po_rtncode := sqlcode;
   			 p_po_rtnmsg  := '����ϵͳ-�ʲ�֤ȯ��-���л�����¼�����̵�('|| v_pro_name ||')����ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_ABS_ISSUE_WSIB;
/

