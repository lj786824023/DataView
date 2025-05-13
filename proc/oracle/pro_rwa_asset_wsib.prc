CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_ASSET_WSIB(
			 											p_data_dt_str	IN	VARCHAR2,		--�������� yyyyMMdd
       											p_po_rtncode	OUT	VARCHAR2,		--���ر�� 1 �ɹ�,0 ʧ��
														p_po_rtnmsg		OUT	VARCHAR2		--��������
				)
  /*
    �洢��������:RWA_DEV.PRO_RWA_ASSET_WSIB
    ʵ�ֹ���:̨��-��ծ�ʲ�-��¼�̵�(��RWA��ծ�ʲ���¼���е�������������RWA��ծ�ʲ���¼�̵ױ���)
    ���ݿھ�:ȫ��
    ����Ƶ��:�³�����
    ��  ��  :V1.0.0
    ��д��  :LISY
    ��дʱ��:2016-12-08
    ��  λ  :�Ϻ���˶��Ϣ�����ɷ����޹�˾
    Դ  ��1 :RWA.RWA_WS_ASSET|��ծ�ʲ���¼��
    Ŀ���1 :RWA.RWA_WSIB_ASSET|��ծ�ʲ��̵ױ�
    �����¼(�޸���|�޸�ʱ��|�޸�����):
    */
  AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_ASSET_WSIB';
  --�����쳣����
  v_raise EXCEPTION;
  --���嵱ǰ����ļ�¼��
  v_count INTEGER;

  BEGIN
    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*�����ȫ�����ݼ��������Ŀ���*/
    --1.���Ŀ����е�ԭ�м�¼
    --��յ�ծ�ʲ��̵ױ�
    DELETE FROM RWA.RWA_WSIB_ASSET WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD');

    COMMIT;

    --2.���������������ݴ�Դ����뵽Ŀ�����
    INSERT INTO RWA.RWA_WSIB_ASSET(
                DATADATE                               --��������
                ,ORGID                             	 	 --����ID
                ,BELONGORGCODE                  	 		 --ҵ����������
                ,ASSETNO      										 		 --�ʲ����
                ,PROJECTNAME                       		 --��Ŀ����
                ,GAINDATE                       	 		 --ȡ��ʱ��
                ,ASSETTYPE                      	 		 --�ʲ�����
                ,ASSETSUBTYPE                   	 		 --��ծ�ʲ�����
                ,SELFFLAG                       	 		 --�Ƿ�����
                ,ACCOUNTVALUE 												 --���˼�ֵ
                ,PROVISION                             --��ֵ׼��
                ,CLASSIFY                              --�弶����
    )
    SELECT
                TO_DATE(p_data_dt_str,'YYYYMMDD')            AS DATADATE                 --��������
                ,T1.SUPPORGID						                     AS ORGID										 --����ID
                ,T1.BELONGORGCODE                  	 		 		 AS	BELONGORGCODE				 		 --ҵ����������
                ,T1.ASSETNO      										 		 		 AS	ASSETNO      				 		 --�ʲ����
                ,T1.PROJECTNAME                       	 		 AS	PROJECTNAME  				 		 --��Ŀ����
                ,T1.GAINDATE                       	 		 		 AS	GAINDATE     				 		 --ȡ��ʱ��
                ,T1.ASSETTYPE                      	 		 		 AS	ASSETTYPE    				 		 --�ʲ�����
                ,T1.ASSETSUBTYPE                   	 		 		 AS	ASSETSUBTYPE 				 		 --��ծ�ʲ�����
                ,T1.SELFFLAG                       	 		 		 AS	SELFFLAG     				 		 --�Ƿ�����
                ,T1.ACCOUNTVALUE 												 		 AS	ACCOUNTVALUE 				 		 --���˼�ֵ
                ,T1.PROVISION                            		 AS	PROVISION    				 		 --��ֵ׼��
                ,T1.CLASSIFY                             		 AS	CLASSIFY     				 		 --�弶����

    FROM				RWA.RWA_WS_ASSET T1            		 					 --��ծ�ʲ���¼��ȡ���һ�ڲ�¼�����̵�
		WHERE 			T1.DATADATE = (SELECT MAX(DATADATE) FROM RWA.RWA_WS_ASSET WHERE DATADATE < TO_DATE(p_data_dt_str,'YYYYMMDD'))
		;

    COMMIT;

    /*Ŀ�������ͳ��*/
    --ͳ�Ʋ���ļ�¼��
    SELECT COUNT(1) INTO v_count FROM RWA.RWA_WSIB_ASSET WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD');
    --Dbms_output.Put_line('RWA.RWA_WSIB_ASSET��ǰ�����̨��-��ծ�ʲ��̵����ݼ�¼Ϊ: ' || v_count || ' ��');


    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̽��� ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    p_po_rtncode := '1';
	  p_po_rtnmsg  := '�ɹ�' || '-' || v_count;
		--�����쳣
		EXCEPTION
    WHEN OTHERS THEN
				 --Dbms_output.Put_line('������,�������Ϊ:'||sqlcode||';������ϢΪ:'||sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace);
       	 ROLLBACK;
   			 p_po_rtncode := sqlcode;
   			 p_po_rtnmsg  := '��ծ�ʲ���¼�����̵�('|| v_pro_name ||')����ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_ASSET_WSIB;
/

