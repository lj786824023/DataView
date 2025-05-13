CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_ABS_ISSUE_POOL_WSIB(
			 											p_data_dt_str	IN	VARCHAR2,		--�������� yyyyMMdd
       											p_po_rtncode	OUT	VARCHAR2,		--���ر�� 1 �ɹ�,0 ʧ��
														p_po_rtnmsg		OUT	VARCHAR2		--��������
				)
  /*
    �洢��������:RWA_DEV.PRO_RWA_ABS_ISSUE_POOL_WSIB
    ʵ�ֹ���:̨��-�ʲ�֤ȯ��-���л���-��Լ���-��¼�̵�(��RWA�ʲ�֤ȯ��-���л���-��Լ��ز�¼���е�������������RWA�ʲ�֤ȯ��-���л���-��Լ��ز�¼�̵ױ���)
    ���ݿھ�:ȫ��
    ����Ƶ��:�³�����
    ��  ��  :V1.0.0
    ��д��  :LISY
    ��дʱ��:2016-12-08
    ��  λ  :�Ϻ���˶��Ϣ�����ɷ����޹�˾
    Դ  ��1 :RWA.RWA_WS_ABS_ISSUE_POOL|�ʲ�֤ȯ��-���л���-��Լ��ز�¼��
    Ŀ���1 :RWA.RWA_WSIB_ABS_ISSUE_POOL|�ʲ�֤ȯ��-���л���-��Լ����̵ױ�
    �����¼(�޸���|�޸�ʱ��|�޸�����):
    */
  AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_ABS_ISSUE_POOL_WSIB';
  --�����쳣����
  v_raise EXCEPTION;
  --���嵱ǰ����ļ�¼��
  v_count INTEGER;

  BEGIN
    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*�����ȫ�����ݼ��������Ŀ���*/
    --1.���Ŀ����е�ԭ�м�¼
    --����ʲ�֤ȯ��-���л���-��Լ����̵ױ�
    DELETE FROM RWA.RWA_WSIB_ABS_ISSUE_POOL WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD');

    COMMIT;

    --2.���������������ݴ�Դ����뵽Ŀ�����
    INSERT INTO RWA.RWA_WSIB_ABS_ISSUE_POOL(
                DATADATE                               --��������
                ,ORGID                             	 	 --����ID
                ,YWSSJG                        	 		 	 --ҵ����������
                ,ZCCBH                    	 		 	 		 --�ʲ��ر��
                ,ZCCDH       										 		 	 --�ʲ��ش���
                ,ZCZQHMC                          		 --�ʲ�֤ȯ������
                ,ZQHFQRZZJGDM                  	 		 	 --֤ȯ����������֯��������
                ,ZCZQHLX                       	 		 	 --�ʲ�֤ȯ������
                ,JCZCYWLX                      	 		 	 --�����ʲ�ҵ������
                ,SFFHGLTJ                      	 		 	 --�Ƿ���Ϲ�������
                ,SFHGZCZQH   												 	 --�Ƿ�Ϲ��ʲ�֤ȯ��
                ,SFTGYXZC                              --�Ƿ��ṩ����֧��
                ,XSLD                                  --��������
    )
    SELECT
                TO_DATE(p_data_dt_str,'YYYYMMDD')            AS DATADATE                 --��������
                ,T1.SUPPORGID						                     AS ORGID										 --����ID
                ,T1.YWSSJG                        	 		 		 AS	YWSSJG                   --ҵ����������
                ,T1.ZCCBH       										 		 		 AS	ZCCBH       						 --�ʲ��ر��
                ,T1.ZCCDH       										 		 		 AS	ZCCDH       						 --�ʲ��ش���
                ,T1.ZCZQHMC                          	 		 	 AS	ZCZQHMC                  --�ʲ�֤ȯ������
                ,T1.ZQHFQRZZJGDM                  	 		 		 AS	ZQHFQRZZJGDM             --֤ȯ����������֯��������
                ,T1.ZCZQHLX                       	 		 		 AS	ZCZQHLX                  --�ʲ�֤ȯ������
                ,T1.JCZCYWLX                      	 		 		 AS	JCZCYWLX                 --�����ʲ�ҵ������
                ,T1.SFFHGLTJ                      	 		 		 AS	SFFHGLTJ                 --�Ƿ���Ϲ�������
                ,T1.SFHGZCZQH   												 		 AS	SFHGZCZQH   						 --�Ƿ�Ϲ��ʲ�֤ȯ��
                ,T1.SFTGYXZC                            		 AS	SFTGYXZC                 --�Ƿ��ṩ����֧��
                ,T1.XSLD                                		 AS	XSLD                     --��������

    FROM				RWA.RWA_WS_ABS_ISSUE_POOL T1            		 --�ʲ�֤ȯ��-���л���-��Լ��ز�¼��ȡ���һ�ڲ�¼�����̵�
		WHERE 			T1.DATADATE = (SELECT MAX(DATADATE) FROM RWA.RWA_WS_ABS_ISSUE_POOL WHERE DATADATE < TO_DATE(p_data_dt_str,'YYYYMMDD'))
		ORDER BY		T1.SUPPSERIALNO
		;

    COMMIT;

		dbms_stats.gather_table_stats(ownname => 'RWA',tabname => 'RWA_WSIB_ABS_ISSUE_POOL',cascade => true);

    /*Ŀ�������ͳ��*/
    --ͳ�Ʋ���ļ�¼��
    SELECT COUNT(1) INTO v_count FROM RWA.RWA_WSIB_ABS_ISSUE_POOL WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD');
    --Dbms_output.Put_line('RWA.RWA_WSIB_ABS_ISSUE_POOL��ǰ�����̨��-�ʲ�֤ȯ��-���л���-��Լ����̵����ݼ�¼Ϊ: ' || v_count || ' ��');


    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̽��� ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    p_po_rtncode := '1';
	  p_po_rtnmsg  := '�ɹ�' || '-' || v_count;
		--�����쳣
		EXCEPTION
    WHEN OTHERS THEN
				 --Dbms_output.Put_line('������,�������Ϊ:'||sqlcode||';������ϢΪ:'||sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace);
       	 ROLLBACK;
   			 p_po_rtncode := sqlcode;
   			 p_po_rtnmsg  := '�ʲ�֤ȯ��-���л���-��Լ��ز�¼�����̵�('|| v_pro_name ||')����ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_ABS_ISSUE_POOL_WSIB;
/

