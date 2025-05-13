CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_HG_CMRELEVENCE(
			 											p_data_dt_str	IN	VARCHAR2,		--�������� yyyyMMdd
       											p_po_rtncode	OUT	VARCHAR2,		--���ر�� 1 �ɹ�,0 ʧ��
														p_po_rtnmsg		OUT	VARCHAR2		--��������
				)
  /*
    �洢��������:RWA_DEV.PRO_RWA_HG_CMRELEVENCE
    ʵ�ֹ���:����ϵͳ-�ع�-��ͬ���������(������Դ����ϵͳ���ع��������Ϣȫ������RWA�ع��ӿڱ��ͬ�������������)
    ���ݿھ�:ȫ��
    ����Ƶ��:�³�����
    ��  ��  :V1.0.0
    ��д��  :LISY
    ��дʱ��:2016-04-21
    ��  λ  :�Ϻ���˶��Ϣ�����ɷ����޹�˾
    Դ  ��1 :RWA_DEV.BRD_REPO_PORTFOLIO|ծȯ�ع���Ѻ��Ϣ
    Դ  ��2 :RWA_DEV.BRD_SECURITY_POSI|ծȯͷ��
    Դ  ��3 :RWA_DEV.BRD_BOND|ծȯ��Ϣ
    Դ  ��4 :RWA_DEV.BRD_REPO|ծȯ�ع�
    Դ  ��5 :RWA_DEV.BRD_BILL_REPO_PORTF|Ʊ�ݻع���Ѻ��Ϣ
    Դ  ��5 :RWA_DEV.BRD_BILL_REPO|Ʊ�ݻع���Ϣ 
    Դ  ��5 :RWA_DEV.BRD_BILL|Ʊ����Ϣ 
    
    Ŀ���  :RWA_DEV.RWA_HG_CMRELEVENCE|����ϵͳ�ع����ͬ�����������
    �����¼(�޸���|�޸�ʱ��|�޸�����):
    pxl 2019/04/15 ȥ����¼���Ϻ���ϵͳ��
    
    */
  AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_HG_CMRELEVENCE';
  --�����쳣����
  v_raise EXCEPTION;
  --���嵱ǰ����ļ�¼��
  v_count INTEGER;

  BEGIN
    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*�����ȫ�����ݼ��������Ŀ���*/
    --1.���Ŀ����е�ԭ�м�¼
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_HG_CMRELEVENCE';

    --3.���������������ݴ�Դ����뵽Ŀ�����
    --3.1 ����ϵͳ-���뷵��ծȯ�ع�-��Ѻʽ
    INSERT INTO RWA_DEV.RWA_HG_CMRELEVENCE(
                DATADATE                               --��������
                ,DATANO                                --������ˮ��
                ,CONTRACTID                       	 	 --��ͬID
                ,MITIGATIONID                     	 	 --������ID
                ,MITIGCATEGORY                    	 	 --����������
                ,SGUARCONTRACTID                  	 	 --Դ������ͬID
                ,GROUPID                          	 	 --������
    )
    SELECT
        TO_DATE(p_data_dt_str,'YYYYMMDD')	, --��������
        p_data_dt_str	, --������ˮ��
        'MRFSZQ' || T3.ACCT_NO	, --��ͬID
        T1.REPO_REFERENCE || T1.SECURITY_REFERENCE	, --������ID
        '03'	, --����������
        'MRFSZQ' || T3.ACCT_NO	, --Դ������ͬID
        '' --������
    FROM  BRD_REPO_PORTFOLIO T1 --ծȯ�ع���Ѻ��Ϣ
    INNER JOIN BRD_REPO T3 --ծȯ�ع�
            ON T1.REPO_REFERENCE = T3.ACCT_NO --�ع����ױ��
            AND T1.DATANO=T3.DATANO
    LEFT JOIN BRD_BOND T4 --ծȯ��Ϣ
           ON T1.SECURITY_REFERENCE = T4.BOND_ID
           AND T1.DATANO=T4.DATANO
    WHERE  T3.CASH_NOMINAL <> 0
         --AND T3.CLIENT_PROPRIETARY = 'F'  --��Ѻʽ  �Ƿ��������Ѻ Դϵͳ�ֶ�ȫ��Ϊ��  ��ֻ����Ѻʽҵ��
         AND T3.REPO_TYPE IN ( '4', 'RB')  --���뷵��
         AND T3.PRINCIPAL_GLNO LIKE '111103%'  --���뷵��ծȯ�ع�-��Ѻʽ-ծȯ
         AND T4.ISSUER_CODE IS NOT NULL    
         AND T3.ACCT_NO IS NOT NULL   
         AND T1.DATANO=p_data_dt_str
    ;

    COMMIT;

		--3.2 ����ϵͳ-���뷵��Ʊ�ݻع�-��Ѻʽ
    INSERT INTO RWA_DEV.RWA_HG_CMRELEVENCE(
                DATADATE                               --��������
                ,DATANO                                --������ˮ��
                ,CONTRACTID                       	 	 --��ͬID
                ,MITIGATIONID                     	 	 --������ID
                ,MITIGCATEGORY                    	 	 --����������
                ,SGUARCONTRACTID                  	 	 --Դ������ͬID
                ,GROUPID                          	 	 --������
    )
    SELECT TO_DATE(p_data_dt_str, 'YYYYMMDD'), --��������
           p_data_dt_str, --������ˮ��
           'MRFSPJ' || T2.ACCT_NO, --��ͬID
           T1.REPO_REFERENCE, --������ID
           '03', --����������
           'MRFSPJ' || T2.ACCT_NO, --Դ������ͬID
           '' --������
      FROM BRD_BILL_REPO_PORTF T1
      LEFT JOIN BRD_BILL_REPO T2
        ON T1.REPO_REFERENCE = T2.ACCT_NO
        AND T1.DATANO=T2.DATANO
       AND T1.SECURITY_REFERENCE = T2.SECURITY_REFERENCE
      LEFT JOIN BRD_BILL T3
        ON T1.REPO_REFERENCE = T3.ACCT_NO
        AND T1.DATANO=T3.DATANO
       AND T1.SECURITY_REFERENCE = T3.BILL_NO
     WHERE --T2.CLIENT_PROPRIETARY = 'Y'  --��Ѻʽ �Ƿ��������Ѻ Դϵͳ�ֶ�ȫ��Ϊ��  ��ֻ����Ѻʽҵ��
     T2.REPO_TYPE IN ('4', 'RB') --���뷵��
     AND T2.CASH_NOMINAL <> 0 --������Ч����
     AND T2.PRINCIPAL_GLNO IS NOT NULL --��ALM���з���  ��ĿΪ�յ����ݲ�����Ϊ��ʷ����
     AND SUBSTR(T2.PRINCIPAL_GLNO, 1, 6) = '111102' --���뷵�۽����ʲ�-���뷵��Ʊ��
     AND (T2.CLIENT_PROPRIETARY <> 'N' OR T2.CLIENT_PROPRIETARY IS NULL) --�Ƿ��������Ѻ NΪ���ʽ  ��N��Ѻʽ
     AND T1.DATANO = p_data_dt_str
    ;

    COMMIT;
    
    --3.2 ����ϵͳ-���뷵��Ʊ�ݻع�-���׶��ֲ������У���Ʊ����Ϊ����
    INSERT INTO RWA_DEV.RWA_HG_CMRELEVENCE(
                DATADATE                               --��������
                ,DATANO                                --������ˮ��
                ,CONTRACTID                            --��ͬID
                ,MITIGATIONID                          --������ID
                ,MITIGCATEGORY                         --����������
                ,SGUARCONTRACTID                       --Դ������ͬID
                ,GROUPID                               --������
    )
    SELECT TO_DATE(p_data_dt_str, 'YYYYMMDD'), --��������
           p_data_dt_str, --������ˮ��
           'MRFSPJ' || T1.ACCT_NO, --��ͬID
           T1.SECURITY_REFERENCE, --������ID
           '03', --����������
           'MRFSPJ' || T1.ACCT_NO, --Դ������ͬID
           '' --������
      FROM RWA_DEV.BRD_BILL_REPO T1 --Ʊ�ݻع�            
      LEFT JOIN RWA_DEV.NCM_CUSTOMER_INFO T3
        ON T1.CUST_NO = T3.MFCUSTOMERID
        AND T1.DATANO=T3.DATANO
       AND T3.CUSTOMERTYPE NOT LIKE '03%' --�Թ��ͻ�                   
     WHERE T1.CASH_NOMINAL <> 0 --������Ч����
       AND T1.PRINCIPAL_GLNO IS NOT NULL --��ALM���з���  ��ĿΪ�յ����ݲ�����Ϊ��ʷ����
       AND SUBSTR(T1.PRINCIPAL_GLNO, 1, 6) = '111102' --���뷵�۽����ʲ�-���뷵��Ʊ��
       AND (T1.CLIENT_PROPRIETARY <> 'N' OR T1.CLIENT_PROPRIETARY IS NULL) --�Ƿ��������Ѻ NΪ���ʽ  ��N��Ѻʽ
       AND T3.CUSTOMERNAME NOT LIKE '%����%' --���׶��ֲ������в���Ҫ��Ʊ��������
       AND T1.DATANO = p_data_dt_str
    ;

    COMMIT;

    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_HG_CMRELEVENCE',cascade => true);

    /*Ŀ�������ͳ��*/
    --ͳ�Ʋ���ļ�¼��
    SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_HG_CMRELEVENCE;
    --Dbms_output.Put_line('RWA_DEV.RWA_HG_CMRELEVENCE��ǰ����ĺ���ϵͳ-�ع����ݼ�¼Ϊ: ' || v_count || ' ��');



    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̽��� ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    p_po_rtncode := '1';
	  p_po_rtnmsg  := '�ɹ�' || '-' || v_count;
		--�����쳣
		EXCEPTION
    WHEN OTHERS THEN
				 --Dbms_output.Put_line('������,�������Ϊ:'||sqlcode||';������ϢΪ:'||sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace);
       	 ROLLBACK;
   			 p_po_rtncode := sqlcode;
   			 p_po_rtnmsg  := '��ͬ���������('|| v_pro_name ||')ETLת��ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_HG_CMRELEVENCE;
/

