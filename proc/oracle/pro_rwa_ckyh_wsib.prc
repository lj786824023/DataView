CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_CKYH_WSIB(
			 											p_data_dt_str	IN	VARCHAR2,		--�������� yyyyMMdd
       											p_po_rtncode	OUT	VARCHAR2,		--���ر�� 1 �ɹ�,0 ʧ��
														p_po_rtnmsg		OUT	VARCHAR2		--��������
				)
  /*
    �洢��������:RWA_DEV.PRO_RWA_CKYH_WSIB
    ʵ�ֹ���:�Ŵ�ϵͳ-����Ѻ��-��¼�̵�(������Դ�Ŵ�ϵͳ��ҵ�������Ϣȫ������RWA����Ѻ�㲹¼�̵ױ���)
    ���ݿھ�:ȫ��
    ����Ƶ��:�³�����
    ��  ��  :V1.0.0
    ��д��  :LISY
    ��дʱ��:2016-06-20
    ��  λ  :�Ϻ���˶��Ϣ�����ɷ����޹�˾
    Դ  ��1 :NCM_BUSINESS_DUEBILL|����ҵ������Ϣ��
    Ŀ���1 :RWA.RWA_WSIB_OUTWAEDBILL|����Ѻ�㲹¼�̵ױ�
    �����¼(�޸���|�޸�ʱ��|�޸�����):
    */
  AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_CKYH_WSIB';
  --�����쳣����
  v_raise EXCEPTION;
  --���嵱ǰ����ļ�¼��
  v_count INTEGER;

  BEGIN
    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*�����ȫ�����ݼ��������Ŀ���*/
    --1.���Ŀ����е�ԭ�м�¼
    --��ճ���Ѻ���̵ױ�
    --EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA.RWA_WSIB_OUTWAEDBILL';
    DELETE FROM RWA.RWA_WSIB_OUTWAEDBILL WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD');

    COMMIT;

    --2.���������������ݴ�Դ����뵽Ŀ�����
    --�Ŵ�ϵͳ-����Ѻ��ҵ��
    INSERT INTO RWA.RWA_WSIB_OUTWAEDBILL(
                DATADATE                               --��������
                ,ORGID                             	 	 --����ID
                ,BDSERIALNO                         	 --��ݱ��
                ,CONTRACTNO														 --��ͬ��
                ,ACCEPTORID       										 --�ж���/�ж���ҵID
                ,ACCEPTOR                           	 --�ж���/�ж���ҵ����
                ,ACCEPTORGCODE                      	 --�ж���/�ж���ҵ��֯��������
                ,ACCEPTCOUNTRYCODE                  	 --�ж���/�ж���ҵע����Ҵ���
                ,ACCEPTINDUSTRYID                   	 --�ж���/�ж���ҵ������ҵ����
                ,ACCEPTFLAG														 --�Ƿ�ж�
    )
    SELECT
                TO_DATE(p_data_dt_str,'YYYYMMDD')            AS DATADATE                 --��������
                ,'01170000'			                 				 		 AS ORGID                		 --����ID              ���ղ�¼������������Ĭ��Ϊ���й���ҵ��(01170000)
                ,T1.SERIALNO                             		 AS BDSERIALNO               --��ݱ��
                ,T1.RELATIVESERIALNO2												 AS CONTRACTNO							 --��ͬ��
                ,p_data_dt_str || 'CKYH' ||lpad(rownum, 10, '0')
                						                           			 AS ACCEPTORID               --�ж���/�ж���ҵID
                ,''			                         				 		 AS ACCEPTOR                 --�ж���/�ж���ҵ����
                ,''                              	 					 AS ACCEPTORGCODE            --�ж���/�ж���ҵ��֯��������
                ,''				                              		 AS ACCEPTCOUNTRYCODE        --�ж���/�ж���ҵע����Ҵ���
                ,''																					 AS ACCEPTINDUSTRYID 				 --�ж���/�ж���ҵ������ҵ����
                ,''																					 AS ACCEPTFLAG							 --�Ƿ�ж�

    FROM				RWA_DEV.NCM_BUSINESS_DUEBILL T1	             		 					--����ҵ������Ϣ��
		WHERE 			T1.BALANCE > 0																						--������0
    AND 				(T1.FINISHDATE IS NULL OR  T1.FINISHDATE = '')						--δ�������Ч���
    AND 				T1.BUSINESSTYPE = '105040'																--105040=����Ѻ��
    AND 				T1.DATANO = p_data_dt_str
		;

    COMMIT;

    /*Ŀ�������ͳ��*/
    --ͳ�Ʋ���ļ�¼��
    SELECT COUNT(1) INTO v_count FROM RWA.RWA_WSIB_OUTWAEDBILL WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD');
    --Dbms_output.Put_line('RWA.RWA_WSIB_OUTWAEDBILL��ǰ������Ŵ�ϵͳ-����Ѻ���̵����ݼ�¼Ϊ: ' || v_count || ' ��');



    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̽��� ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    p_po_rtncode := '1';
	  p_po_rtnmsg  := '�ɹ�' || '-' || v_count;
		--�����쳣
		EXCEPTION
    WHEN OTHERS THEN
				 --Dbms_output.Put_line('������,�������Ϊ:'||sqlcode||';������ϢΪ:'||sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace);
       	 ROLLBACK;
   			 p_po_rtncode := sqlcode;
   			 p_po_rtnmsg  := '����͢��¼�����̵�('|| v_pro_name ||')����ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_CKYH_WSIB;
/

