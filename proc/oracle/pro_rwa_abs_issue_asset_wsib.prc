CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_ABS_ISSUE_ASSET_WSIB(
			 											p_data_dt_str	IN	VARCHAR2,		--�������� yyyyMMdd
       											p_po_rtncode	OUT	VARCHAR2,		--���ر�� 1 �ɹ�,0 ʧ��
														p_po_rtnmsg		OUT	VARCHAR2		--��������
				)
  /*
    �洢��������:RWA_DEV.PRO_RWA_ABS_ISSUE_ASSET_WSIB
    ʵ�ֹ���:̨��-�ʲ�֤ȯ��-���л���-�����ʲ�-��¼�̵�(��RWA�ʲ�֤ȯ��-���л���-�����ʲ���¼���е�������������RWA�ʲ�֤ȯ��-���л���-�����ʲ���¼�̵ױ���)
    ���ݿھ�:ȫ��
    ����Ƶ��:�³�����
    ��  ��  :V1.0.0
    ��д��  :LISY
    ��дʱ��:2016-12-08
    ��  λ  :�Ϻ���˶��Ϣ�����ɷ����޹�˾
    Դ  ��1 :RWA.RWA_WS_ABS_ISSUE_UNDERASSET|�ʲ�֤ȯ��-���л���-�����ʲ���¼��
    Ŀ���1 :RWA.RWA_WSIB_ABS_ISSUE_UNDERASSET|�ʲ�֤ȯ��-���л���-�����ʲ��̵ױ�
    �����¼(�޸���|�޸�ʱ��|�޸�����):
    */
  AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_ABS_ISSUE_ASSET_WSIB';
  --�����쳣����
  v_raise EXCEPTION;
  --���嵱ǰ����ļ�¼��
  v_count INTEGER;

  BEGIN
    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*�����ȫ�����ݼ��������Ŀ���*/
    --1.���Ŀ����е�ԭ�м�¼
    --����ʲ�֤ȯ��-���л���-�����ʲ��̵ױ�
    DELETE FROM RWA.RWA_WSIB_ABS_ISSUE_UNDERASSET WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD');

    COMMIT;

    --2.���������������ݴ�Դ����뵽Ŀ�����
    INSERT INTO RWA.RWA_WSIB_ABS_ISSUE_UNDERASSET(
                DATADATE                               --��������
                ,ORGID                             	 	 --����ID
                ,ZCCBH                    	 		 	 		 --�ʲ��ر��
                ,ZCCDH                    	 		 	 		 --�ʲ��ش���
                ,ZCZQHMC										 		 	 		 --�ʲ�֤ȯ������
                ,HTBH                        		 			 --��ͬ���
                ,HTYE                     	 		 	 		 --��ͬ���
    )
    SELECT
                TO_DATE(p_data_dt_str,'YYYYMMDD')            AS DATADATE                 --��������
                ,T1.SUPPORGID						                     AS ORGID										 --����ID
                ,T1.ZCCBH                    	 		 		 			 AS	ZCCBH                    --�ʲ��ر��
                ,T1.ZCCDH                    	 		 		 			 AS	ZCCDH                    --�ʲ��ش���
                ,T1.ZCZQHMC										 		 		 			 AS	ZCZQHMC									 --�ʲ�֤ȯ������
                ,T1.HTBH                        	 		 	 		 AS	HTBH                     --��ͬ���
                ,T1.HTYE                     	 		 		 			 AS	HTYE                     --��ͬ���

    FROM				RWA.RWA_WS_ABS_ISSUE_UNDERASSET T1           --�ʲ�֤ȯ��-���л���-�����ʲ���¼��ȡ���һ�ڲ�¼�����̵�
		WHERE 			T1.DATADATE = (SELECT MAX(DATADATE) FROM RWA.RWA_WS_ABS_ISSUE_UNDERASSET WHERE DATADATE < TO_DATE(p_data_dt_str,'YYYYMMDD'))
		ORDER BY		T1.SUPPSERIALNO
		;

    COMMIT;

    dbms_stats.gather_table_stats(ownname => 'RWA',tabname => 'RWA_WSIB_ABS_ISSUE_UNDERASSET',cascade => true);

    /*Ŀ�������ͳ��*/
    --ͳ�Ʋ���ļ�¼��
    SELECT COUNT(1) INTO v_count FROM RWA.RWA_WSIB_ABS_ISSUE_UNDERASSET WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD');
    --Dbms_output.Put_line('RWA.RWA_WSIB_ABS_ISSUE_UNDERASSET��ǰ�����̨��-�ʲ�֤ȯ��-���л���-�����ʲ��̵����ݼ�¼Ϊ: ' || v_count || ' ��');


    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̽��� ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    p_po_rtncode := '1';
	  p_po_rtnmsg  := '�ɹ�' || '-' || v_count;
		--�����쳣
		EXCEPTION
    WHEN OTHERS THEN
				 --Dbms_output.Put_line('������,�������Ϊ:'||sqlcode||';������ϢΪ:'||sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace);
       	 ROLLBACK;
   			 p_po_rtncode := sqlcode;
   			 p_po_rtnmsg  := '�ʲ�֤ȯ��-���л���-�����ʲ���¼�����̵�('|| v_pro_name ||')����ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_ABS_ISSUE_ASSET_WSIB;
/

