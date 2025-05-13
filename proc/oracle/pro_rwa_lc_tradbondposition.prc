CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_LC_TRADBONDPOSITION(
														p_data_dt_str IN  VARCHAR2, --��������
                            p_po_rtncode  OUT VARCHAR2, --���ر��
                            p_po_rtnmsg   OUT VARCHAR2  --��������
)
  /*
    �洢��������:RWA_DEV.PRO_RWA_LC_TRADBONDPOSITION
    ʵ�ֹ���:���ϵͳ-ծȯ���Ͷ��-�г�����-����ծȯͷ��(������Դ���ϵͳ��ҵ�������Ϣȫ������RWA�г�������ƽӿڱ���ծȯͷ�����)
    ���ݿھ�:ȫ��
    ����Ƶ��:�³�����
    ��  ��  :V1.0.0
    ��д��  :QHJIANG
    ��дʱ��:2016-04-14
    ��  λ  :�Ϻ���˶��Ϣ�����ɷ����޹�˾
    Դ  ��1 :RWA_DEV.ZGS_INVESTASSETDETAI|�ʲ������
    Դ  ��2 :RWA_DEV.ZGS_FINANCING_INFO|��Ʒ��Ϣ��
    Դ  ��3 :RWA_DEV.ZGS_ATBOND|ծȯ��Ϣ��
    Ŀ���1 :RWA_DEV.RWA_LC_TRADBONDPOSITION|����ծȯͷ���
    ������  :��
    �����¼(�޸���|�޸�ʱ��|�޸�����):
    */

  AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_LC_TRADBONDPOSITION';
  --�����쳣����
  v_raise EXCEPTION;
  --���嵱ǰ����ļ�¼��
  v_count INTEGER;

  BEGIN

    --DBMS_OUTPUT.PUT_LINE('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    --���Ŀ����е�ԭ�м�¼
    --DELETE FROM RWA_LC_TRADBONDPOSITION WHERE DATADATE=TO_DATE(p_data_dt_str,'yyyyMMdd');
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_LC_TRADBONDPOSITION';


    --DBMS_OUTPUT.PUT_LINE('��ʼ�����롾����ծȯͷ���' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));
    INSERT INTO RWA_DEV.RWA_LC_TRADBONDPOSITION(
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
    WITH TEMP_INVESTASSETDETAIL AS (
        SELECT  T3.FLD_ASSET_CODE						AS FLD_ASSET_CODE
        			 ,T3.FLD_FINANC_CODE					AS FLD_FINANC_CODE
        			 ,T3.FLD_ASSET_SHARES					AS FLD_ASSET_SHARES
        			 ,T3.FLD_CURRENCY							AS FLD_CURRENCY
        			 ,T3.FLD_MARKET_AMOUNT				AS FLD_MARKET_AMOUNT
        			 ,T3.FLD_MTM_AMOUNT						AS FLD_MTM_AMOUNT
        			 ,T4.FLD_TRANSWAY							AS FLD_TRANSWAY
        			 ,T4.FLD_SELLOBJ							AS FLD_SELLOBJ
        			 ,T4.FLD_INCOME_TYPE					AS FLD_INCOME_TYPE
          FROM RWA_DEV.ZGS_INVESTASSETDETAIL T3
    INNER JOIN RWA_DEV.ZGS_FINANCING_INFO T4
            ON T3.FLD_FINANC_CODE = T4.FLD_FINANC_CODE
           AND T4.FLD_INCOME_TYPE <> '3'																		--3���ų��Ǳ�������
           AND T4.DATANO = p_data_dt_str
         WHERE T3.FLD_ASSET_TYPE = '2'																			-- 2��ծȯ��24���ʲ�����ƻ�
           AND T3.FLD_ASSET_STATUS = '1' 																		--1��״̬����
           AND T3.FLD_ASSET_FLAG = '1'   																		--1����Ʋ�Ʒ
           AND T3.C_ACC_TYPE = 'D'																					--D�������࣬�ò���������Ϊ�г�����
           AND T3.FLD_DATE = p_data_dt_str																	--��Ч����Ʋ�Ʒ���ֵ����ÿ�ո���
           AND T3.DATANO = p_data_dt_str
    )
    , TEMP_BALANCE AS (
    							SELECT T1.C_CODE					AS FLD_FINANC_CODE
    										,CASE WHEN T2.C_VOUCHER_NO = '160226000000101' THEN 'C109311602051SZ' 			--����ƾ֤Ϊ160226000000101�ļ�¼�����ʲ�����û�У�Ӧ����C109311602051SZ������
    										 ELSE T1.C_ASSET_CODE
    										 END								AS FLD_ASSET_CODE
    										,T2.C_SUBJECT_CODE	AS C_SUBJECT_CODE
    										,SUM(DECODE(T2.C_CD_FLAG,'C',-T2.F_AMT,T2.F_AMT)) AS BALANCE
    								FROM RWA_DEV.ZGS_ATAGENTVOUCHER T1																		--���ͻ��ƾ֤��
    					INNER JOIN RWA_DEV.ZGS_ATAGENTVOUCHER_DT T2																	--����ƾ֤��¼��
    									ON T1.C_VOUCHER_NO = T2.C_VOUCHER_NO
    								 AND T2.C_SUBJECT_CODE IN ('12220101',
													                     '12220102',
													                     '12220103',
													                     '12220201',
													                     '12220202',
													                     '12220203')
    								 AND T2.DATANO = p_data_dt_str
    							 WHERE T1.C_ACCT_STATUS <> '3'
    								 AND T1.D_ACCT_DATE <= p_data_dt_str
    								 AND T1.DATANO = p_data_dt_str
    						GROUP BY T1.C_CODE,CASE WHEN T2.C_VOUCHER_NO = '160226000000101' THEN 'C109311602051SZ' ELSE T1.C_ASSET_CODE END,T2.C_SUBJECT_CODE
    )
    SELECT
        				TO_DATE(p_data_dt_str,'yyyyMMdd')      				AS DATADATE        					--RWAϵͳ��ֵ
        				,T1.FLD_FINANC_CODE || T1.FLD_ASSET_CODE			AS POSITIONID      					--��Ʒ����+��Ĵ���
        				,T2.C_BOND_CODE																AS BONDID          					--ծȯ����
        				,'9998'                          					AS TRANORGID       					--Ĭ�� �����ʲ�����(01160000)
        				,'9998'                          					AS ACCORGID        					--Ĭ�� �����ʲ�����(01160000)
        				,'0101'                              					AS INSTRUMENTSTYPE 					--Ĭ�ϣ�ծȯ InstrumentsType ���ڹ�������: 0101  ծȯ
        				,/*CASE WHEN T1.FLD_INCOME_TYPE = '1' AND T1.FLD_SELLOBJ = '0' THEN '12220101'
        							WHEN T1.FLD_INCOME_TYPE = '1' AND T1.FLD_SELLOBJ = '5' THEN '12220103'
        							WHEN T1.FLD_INCOME_TYPE = '1' AND T1.FLD_SELLOBJ = '7' THEN '12220102'
        							WHEN T1.FLD_INCOME_TYPE = '2' AND T1.FLD_SELLOBJ = '0' THEN '12220201'
        							WHEN T1.FLD_INCOME_TYPE = '2' AND T1.FLD_SELLOBJ = '5' THEN '12220203'
        							WHEN T1.FLD_INCOME_TYPE = '2' AND T1.FLD_SELLOBJ = '7' THEN '12220202'
        							WHEN T1.FLD_INCOME_TYPE = '3' THEN '13212003'
        							ELSE ''
        				END*/
        				 T3.C_SUBJECT_CODE                   					AS ACCSUBJECTS     					--ͬ���÷��տ�Ŀӳ���߼�һ�� �߼���ʲô��
        				,T1.FLD_ASSET_SHARES                 					AS DENOMINATION    					--�ݶ�
        				,T1.FLD_MTM_AMOUNT                   					AS MARKETVALUE     					--�ʲ�mtmֵ(ծȯ�������мۼ���)
        				,''                                  					AS DISCOUNTPREMIUM 					--Ĭ�� ��
        				,''                                  					AS FAIRVALUECHANGE 					--Ĭ�� ��
        				,T1.FLD_MARKET_AMOUNT                					AS BOOKBALANCE     					--�ʲ���ֵ
        				,T1.FLD_CURRENCY                     					AS CURRENCY        					--����

    FROM				TEMP_INVESTASSETDETAIL T1																					--������ϸ������¼�¼
    INNER JOIN	RWA_DEV.ZGS_ATBOND T2
    ON					T2.C_BOND_CODE = T1.FLD_ASSET_CODE
    AND					T2.DATANO = p_data_dt_str
    INNER JOIN	TEMP_BALANCE T3																										--�����ܱ�
    ON					T1.FLD_FINANC_CODE = T3.FLD_FINANC_CODE
    AND					T1.FLD_ASSET_CODE = T3.FLD_ASSET_CODE
    ;

		COMMIT;

		dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_LC_TRADBONDPOSITION',cascade => true);

    --DBMS_OUTPUT.PUT_LINE('���������롾����ծȯͷ���' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*Ŀ�������ͳ��*/
    --ͳ�Ʋ���ļ�¼
    SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_LC_TRADBONDPOSITION;
    --Dbms_output.Put_line('RWA_DEV.RWA_LC_TRADBONDPOSITION��ǰ��������ϵͳ-ծȯ���Ͷ��(�г�����)-����ծȯͷ���¼Ϊ: ' || v_count || ' ��');

    --DBMS_OUTPUT.PUT_LINE('��ִ�� ' || v_pro_name || ' �洢���̽��� ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));
    p_po_rtncode := '1';
    p_po_rtnmsg  := '�ɹ�' || '-' || v_count;


    --�����쳣
    EXCEPTION WHEN OTHERS THEN
        --DBMS_OUTPUT.PUT_LINE('������,�������Ϊ:'||sqlcode||';������ϢΪ:'||sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace);
        ROLLBACK;
        p_po_rtncode := sqlcode;
        p_po_rtnmsg  := '����ծȯͷ��('|| v_pro_name ||')ETLת��ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;

    RETURN;

END PRO_RWA_LC_TRADBONDPOSITION;
/

