CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_ZQ_TRADBONDPOSITION(
			 											p_data_dt_str	IN	VARCHAR2,		--�������� yyyyMMdd
       											p_po_rtncode	OUT	VARCHAR2,		--���ر�� 1 �ɹ�,0 ʧ��
														p_po_rtnmsg		OUT	VARCHAR2		--��������
				)
  /*
    �洢��������:RWA_DEV.PRO_RWA_ZQ_TRADBONDPOSITION
    ʵ�ֹ���:����ϵͳ-ծȯ-�г�����-����ծȯͷ��(������Դ����ϵͳ��ҵ�������Ϣȫ������RWA�г�����ծȯ�ӿڱ���ծȯͷ�����)
    ���ݿھ�:ȫ��
    ����Ƶ��:�³�����
    ��  ��  :V1.0.0
    ��д��  :LISY
    ��дʱ��:2016-04-12
    ��  λ  :�Ϻ���˶��Ϣ�����ɷ����޹�˾
    Դ  ��1 :RWA_DEV.FNS_BND_INFO_B|����ϵͳծȯ��Ϣ��
    Դ  ��2 :RWA_DEV.FNS_BND_BOOK_B|����ϵͳ������
    Դ  ��3 :RWA_DEV.FNS_BND_TRANSACTION_B|���׻��
    Դ  ��4 :RWA.RWA_WS_BONDTRADE|ծȯͶ�ʲ�¼��Ϣ��
    Դ  ��5 :RWA.RWA_WP_DATASUPPLEMENT|��¼������ϸ��Ϣ��
    Ŀ���  :RWA_DEV.RWA_ZQ_TRADBONDPOSITION|����ϵͳծȯ����ծȯͷ���
    �����¼(�޸���|�޸�ʱ��|�޸�����):
    */
  AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_ZQ_TRADBONDPOSITION';
  --�����쳣����
  v_raise EXCEPTION;
  --���嵱ǰ����ļ�¼��
  v_count INTEGER;

  BEGIN
    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*�����ȫ�����ݼ��������Ŀ���*/
    --1.���Ŀ����е�ԭ�м�¼
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_ZQ_TRADBONDPOSITION';

    --2.���������������ݴ�Դ����뵽Ŀ�����
    --2.1 ����ϵͳ-ծȯ
    INSERT INTO RWA_DEV.RWA_ZQ_TRADBONDPOSITION(
                DATADATE                               --��������
                ,POSITIONID                          	 --ͷ��ID
                ,BONDID                              	 --ծȯID
                ,TRANORGID                           	 --���׻���ID
                ,ACCORGID                            	 --�������ID
                ,INSTRUMENTSTYPE                     	 --���ڹ�������
                ,ACCSUBJECTS                         	 --��ƿ�Ŀ
                ,DENOMINATION                        	 --���
                ,MARKETVALUE                         	 --��ֵ
                ,DISCOUNTPREMIUM                     	 --�����
                ,FAIRVALUECHANGE                     	 --���ʼ�ֵ�䶯
                ,BOOKBALANCE                         	 --�������
                ,CURRENCY                            	 --����

    )
    WITH TEMP_BND_BOOK AS (
    										SELECT BOND_ID,
												       INITIAL_COST,
												       INT_ADJUST,
												       MKT_VALUE_CHANGE,
												       RECEIVABLE_INT,
												       ACCOUNTABLE_INT,
												       PAR_VALUE
												  FROM (SELECT BOND_ID,
												               INITIAL_COST,
												               INT_ADJUST,
												               MKT_VALUE_CHANGE,
												               RECEIVABLE_INT,
												               ACCOUNTABLE_INT,
												               PAR_VALUE,
												               ROW_NUMBER() OVER(PARTITION BY BOND_ID ORDER BY SORT_SEQ DESC) RM
												          FROM FNS_BND_BOOK_B
												         WHERE AS_OF_DATE <= p_data_dt_str
												           AND DATANO = p_data_dt_str)
												 WHERE RM = 1
												   AND NVL(INITIAL_COST, 0) + NVL(INT_ADJUST, 0) + NVL(MKT_VALUE_CHANGE, 0) + NVL(ACCOUNTABLE_INT, 0) <> 0
		)
    SELECT
                TO_DATE(p_data_dt_str,'YYYYMMDD')            AS DATADATE                 --��������
                ,T1.BOND_ID														     	 AS POSITIONID               --ͷ��ID
                ,T1.BOND_ID	                    				 		 AS BONDID                   --ծȯID
                ,T1.DEPARTMENT  	                      		 AS TRANORGID                --���׻���ID
                ,T1.DEPARTMENT	                             AS ACCORGID                 --�������ID
                ,'0101'			                           			 AS INSTRUMENTSTYPE          --���ڹ�������					 Ĭ�ϣ�ծȯ(0101)
                ,CASE WHEN T1.BOND_TYPE2 IN ('30', '50') THEN '11012001'			 					 --����������Ͷ�ʱ���
                			ELSE '11010101'																					 					 --������Ͷ�ʱ���
                 END		 						                         AS ACCSUBJECTS              --��ƿ�Ŀ    					 ����ԭϵͳ���ʲ�������ջ�ƿ�Ŀ��ȷ��
                ,T3.PAR_VALUE			 						               AS DENOMINATION             --���
                ,NVL(T3.INITIAL_COST,0) + NVL(T3.MKT_VALUE_CHANGE,0)
                		                            						 AS MARKETVALUE              --��ֵ
                ,NULL		                              			 AS DISCOUNTPREMIUM          --�����      					 Ĭ�ϣ���
                ,NVL(T3.MKT_VALUE_CHANGE,0)                  AS FAIRVALUECHANGE          --���ʼ�ֵ�䶯
                ,NVL(T3.INITIAL_COST,0) + NVL(T3.INT_ADJUST,0) + NVL(T3.MKT_VALUE_CHANGE,0) + NVL(T3.ACCOUNTABLE_INT,0)
                 																						 AS BOOKBALANCE              --�������
                ,NVL(T1.CURRENCY_CODE,'CNY')                 AS CURRENCY                 --����

    FROM				RWA_DEV.FNS_BND_INFO_B T1
		INNER JOIN	TEMP_BND_BOOK T3
		ON					T1.BOND_ID = T3.BOND_ID
		WHERE 			T1.ASSET_CLASS = '10'																			--���������˻������г�����
		AND					T1.DATANO = p_data_dt_str
		AND 				T1.BOND_CODE IS NOT NULL																	--�ų���Ч��ծȯ����
	  ;

    COMMIT;

    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_ZQ_TRADBONDPOSITION',cascade => true);

    /*Ŀ�������ͳ��*/
    --ͳ�Ʋ���ļ�¼��
    SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_ZQ_TRADBONDPOSITION;
    --Dbms_output.Put_line('RWA_DEV.RWA_ZQ_TRADBONDPOSITION��ǰ����Ĳ���ϵͳ-ծȯ(�г�����)-����ծȯͷ���¼Ϊ: ' || v_count || ' ��');



    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̽��� ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    p_po_rtncode := '1';
	  p_po_rtnmsg  := '�ɹ�' || '-' || v_count;
		--�����쳣
		EXCEPTION
    WHEN OTHERS THEN
				 --Dbms_output.Put_line('������,�������Ϊ:'||sqlcode||';������ϢΪ:'||sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace);
       	 ROLLBACK;
   			 p_po_rtncode := sqlcode;
   			 p_po_rtnmsg  := '����ծȯͷ��('|| v_pro_name ||')ETLת��ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_ZQ_TRADBONDPOSITION;
/

