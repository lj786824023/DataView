CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_TX_WSIB(
			 											p_data_dt_str	IN	VARCHAR2,		--�������� yyyyMMdd
       											p_po_rtncode	OUT	VARCHAR2,		--���ر�� 1 �ɹ�,0 ʧ��
														p_po_rtnmsg		OUT	VARCHAR2		--��������
				)
  /*
    �洢��������:RWA_DEV.PRO_RWA_TX_WSIB
    ʵ�ֹ���:�Ŵ�ϵͳ-Ʊ������-��¼�̵�(������Դ�Ŵ�ϵͳ��ҵ�������Ϣȫ������RWAƱ�����ֲ�¼�̵ױ���)
    ���ݿھ�:ȫ��
    ����Ƶ��:�³�����
    ��  ��  :V1.0.0
    ��д��  :LISY
    ��дʱ��:2016-06-20
    ��  λ  :�Ϻ���˶��Ϣ�����ɷ����޹�˾
    Դ  ��1 :NCM_BILL_INFO|Ʊ����Ϣ��(��ʹ��)
    Դ  ��2 :NCM_BUSINESS_DUEBILL|����ҵ������Ϣ��
    Ŀ���1 :RWA.RWA_WSIB_BILLDISCOUNT|Ʊ�����ֲ�¼�̵ױ�
    �����¼(�޸���|�޸�ʱ��|�޸�����):
    */
  AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_TX_WSIB';
  --�����쳣����
  v_raise EXCEPTION;
  --���嵱ǰ����ļ�¼��
  v_count INTEGER;

  BEGIN
    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*�����ȫ�����ݼ��������Ŀ���*/
    --1.���Ŀ����е�ԭ�м�¼
    --���Ʊ�������̵ױ�
    --EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA.RWA_WSIB_BILLDISCOUNT';
    DELETE FROM RWA.RWA_WSIB_BILLDISCOUNT WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD');

    COMMIT;

    --2.���������������ݴ�Դ����뵽Ŀ�����
    --�Ŵ�ϵͳ-Ʊ������ҵ��
    INSERT INTO RWA.RWA_WSIB_BILLDISCOUNT(
                DATADATE                               --��������
                ,ORGID                             	 	 --����ID
                ,BDSERIALNO                         	 --��ݱ��
                ,BILLNO           										 --Ʊ�ݱ��
                ,BILLTYPE                           	 --Ʊ������
                ,ACCEPTORID                         	 --�ж���/�ж���ҵID
                ,ACCEPTOR                           	 --�ж���/�ж���ҵ����
                ,ACCEPTORGCODE                      	 --�ж���/�ж���ҵ��֯��������
                ,ACCEPTCOUNTRYCODE                  	 --�ж���/�ж���ҵע����Ҵ���
                ,ACCEPTINDUSTRYID 										 --�ж���/�ж���ҵ������ҵ����
                ,ACCEPTSCOPE      										 --�ж���ҵ��ģ
    )
    SELECT
                TO_DATE(p_data_dt_str,'YYYYMMDD')            AS DATADATE                 --��������
                ,'01370000'			                 				 		 AS ORGID                		 --����ID              ���ղ�¼������������Ĭ��Ϊ���н���ͬҵ����(01370000)
                ,T1.SERIALNO                             		 AS BDSERIALNO               --��ݱ��
                ,T1.BILLNO																	 AS BILLNO           				 --Ʊ�ݱ��
                ,CASE WHEN T1.BUSINESSTYPE = '104010' THEN '10'
                 ELSE '20'
                 END																				 AS BILLTYPE                 --Ʊ������
                ,p_data_dt_str || 'TX' ||lpad(rownum, 10, '0')
                						                           			 AS ACCEPTORID               --�ж���/�ж���ҵID
                ,''			                         				 		 AS ACCEPTOR                 --�ж���/�ж���ҵ����
                ,''                              	 					 AS ACCEPTORGCODE            --�ж���/�ж���ҵ��֯��������
                ,''				                              		 AS ACCEPTCOUNTRYCODE        --�ж���/�ж���ҵע����Ҵ���
                ,''																					 AS ACCEPTINDUSTRYID 				 --�ж���/�ж���ҵ������ҵ����
                ,''																					 AS ACCEPTSCOPE      				 --�ж���ҵ��ģ

    FROM				RWA_DEV.NCM_BUSINESS_DUEBILL T1	             		 					--����ҵ������Ϣ��
		/*INNER JOIN 	RWA_DEV.NCM_BILL_INFO T2																	--Ʊ����Ϣ��,��������ҵ��ʱû��¼��Ʊ�ݳж���Ϣ���ڽ���е�Ʊ�ݿ�����Ʊ�ݱ���û��
		ON 					REPLACE(T1.BILLNO,' ','') = REPLACE(T2.BILLNO,' ','')
		AND 				T1.RELATIVESERIALNO2 = T2.OBJECTNO
		AND 				T2.OBJECTTYPE = 'BusinessContract'
		AND					T2.DATANO = p_data_dt_str*/
		WHERE 			T1.BALANCE > 0																						--������0
    AND 				(T1.FINISHDATE IS NULL OR  T1.FINISHDATE = '')						--δ�������Ч���
    AND 				T1.BUSINESSTYPE IN ('104010','104020')										--104010=���гжһ�Ʊ���֣�104020=��ҵ�жһ�Ʊ����
    AND 				T1.DATANO = p_data_dt_str
		;

    COMMIT;

    /*Ŀ�������ͳ��*/
    --ͳ�Ʋ���ļ�¼��
    SELECT COUNT(1) INTO v_count FROM RWA.RWA_WSIB_BILLDISCOUNT WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD');
    --Dbms_output.Put_line('RWA.RWA_WSIB_BILLDISCOUNT��ǰ������Ŵ�ϵͳ-Ʊ�������̵����ݼ�¼Ϊ: ' || v_count || ' ��');



    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̽��� ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    p_po_rtncode := '1';
	  p_po_rtnmsg  := '�ɹ�' || '-' || v_count;
		--�����쳣
		EXCEPTION
    WHEN OTHERS THEN
				 --Dbms_output.Put_line('������,�������Ϊ:'||sqlcode||';������ϢΪ:'||sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace);
       	 ROLLBACK;
   			 p_po_rtncode := sqlcode;
   			 p_po_rtnmsg  := 'Ʊ�����ֲ�¼�����̵�('|| v_pro_name ||')����ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_TX_WSIB;
/

