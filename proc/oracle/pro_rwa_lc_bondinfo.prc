CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_LC_BONDINFO(
														p_data_dt_str  IN  VARCHAR2, --��������
                            p_po_rtncode   OUT VARCHAR2, --���ر��
                            p_po_rtnmsg    OUT VARCHAR2  --��������
)
  /*
    �洢��������:RWA_DEV.PRO_RWA_LC_BONDINFO
    ʵ�ֹ���:���ϵͳ-ծȯ���Ͷ��-�г�����-ծȯ��Ϣ(������Դ���ϵͳ��ҵ�������Ϣȫ������RWA�г�������ƽӿڱ�ծȯ��Ϣ����)
    ���ݿھ�:ȫ��
    ����Ƶ��:�³�����
    ��  ��  :V1.0.0
    ��д��  :QHJIANG
    ��дʱ��:2016-04-14
    ��  λ  :�Ϻ���˶��Ϣ�����ɷ����޹�˾
    Դ  ��1 :RWA_DEV.ZGS_ATBOND|ծȯ��Ϣ��
    Դ  ��2 :RWA.RWA_WS_FCII_BOND|ծȯ���Ͷ�ʲ�¼��
    Դ  ��3 :RWA_DEV.ZGS_INVESTASSETDETAI|�ʲ������
    Դ  ��4 :RWA_DEV.ZGS_FINANCING_INFO|��Ʒ��Ϣ��
    Դ  ��5 :RWA.RWA_WP_DATASUPPLEMENT|��¼������ϸ��Ϣ��
    Ŀ���1 :RWA_DEV.RWA_LC_BONDINFO|ծȯ��Ϣ��
    ������  :��
    �����¼(�޸���|�޸�ʱ��|�޸�����):
    */

  AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_LC_BONDINFO';
  --�����쳣����
  v_raise EXCEPTION;
  --���嵱ǰ����ļ�¼��
  v_count INTEGER;

  BEGIN

    --DBMS_OUTPUT.PUT_LINE('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    --���Ŀ����е�ԭ�м�¼
    --DELETE FROM RWA_LC_BONDINFO WHERE DATADATE=TO_DATE(p_data_dt_str,'yyyyMMdd');
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_LC_BONDINFO';


    --DBMS_OUTPUT.PUT_LINE('��ʼ�����롾ծȯ��Ϣ��' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));
    INSERT INTO RWA_DEV.RWA_LC_BONDINFO(
        				DATADATE                               --��������
                ,BONDID                                --ծȯID
                ,BONDNAME                              --ծȯ����
                ,BONDTYPE                              --ծȯ����
                ,ERATING                               --�ⲿ����
                ,ISSUERID                              --������ID
                ,ISSUERNAME                            --����������
                ,ISSUERTYPE                            --�����˴���
                ,ISSUERSUBTYPE                         --������С��
                ,ISSUERREGISTSTATE                     --������ע�����
                ,ISSUERSMBFLAG                         --������С΢��ҵ��ʶ
                ,BONDISSUEINTENT                       --ծȯ����Ŀ��
                ,REABSFLAG                             --���ʲ�֤ȯ����ʶ
                ,ORIGINATORFLAG                        --�Ƿ������
                ,STARTDATE                             --��ʼ����
                ,DUEDATE                               --��������
                ,ORIGINALMATURITY                      --ԭʼ����
                ,RESIDUALM                             --ʣ������
                ,RATETYPE                              --��������
                ,EXECUTIONRATE                         --ִ������
                ,NEXTREPRICEDATE                       --�´��ض�����
                ,NEXTREPRICEM                          --�´��ض�������
                ,MODIFIEDDURATION                      --��������
                ,DENOMINATION                          --���
                ,CURRENCY                              --����
    )
    WITH TEMP_INVESTASSETDETAIL AS (
        SELECT  DISTINCT
        				T3.FLD_ASSET_CODE						AS FLD_ASSET_CODE
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
    , TEMP_CUST_INFO AS (
    								SELECT CUSTOMERID
    											,CUSTOMERNAME
    											,CERTTYPE
    											,CERTID
    											,RWACUSTOMERTYPE
    											,ISSUPERVISESTANDARSMENT
    									FROM RWA_DEV.NCM_CUSTOMER_INFO
    								 WHERE ROWID IN (SELECT MAX(ROWID) FROM RWA_DEV.NCM_CUSTOMER_INFO WHERE DATANO = p_data_dt_str AND CERTTYPE IN ('Ent01','Ent02') GROUP BY CERTID)
    								 	 AND DATANO = p_data_dt_str
    )
    SELECT
        				TO_DATE(p_data_dt_str,'YYYYMMDD')            AS DATADATE                 --��������
                ,T1.C_BOND_CODE												     	 AS BONDID                   --ծȯID
                ,T1.C_BOND_NAME                  				 		 AS BONDNAME                 --ծȯ����
                ,T1.C_BOND_TYPE                          		 AS BONDTYPE                 --ծȯ����
                ,T1.C_RISK_SCORE													   AS ERATING                  --�ⲿ����          					 ��¼��ͨ�����ޡ��������ȼ�ת��Ϊ����
                ,CASE WHEN T1.C_BOND_TYPE IN ('01','17','19') THEN 'ZGZYZF'							 --��ծ��������Ĭ��Ϊ�й���������()
        				 ELSE 'LC' || T1.C_ISSUER_IDENTIFICATION_TYPE || T1.C_ISSUER_IDENTIFICATION_NO
        				 END				                           			 AS ISSUERID                 --������ID          					 ��¼
                ,CASE WHEN T1.C_BOND_TYPE IN ('01','17','19') THEN '�й���������'				 --��ծ��������Ĭ��Ϊ�й���������()
        				 ELSE NVL(T1.C_RWA_PUBLISHNAME,T4.C_ORG_NAME)
        				 END															           AS ISSUERNAME               --����������        					 ��¼
                ,T10.DITEMNO									               AS ISSUERTYPE               --�����˴���        					 ����ӳ��
                ,T11.DITEMNO																 AS ISSUERSUBTYPE            --������С��        					  ����ӳ��
                ,CASE WHEN NVL(T1.C_ISSUER_REGCOUNTRY_CODE,'CHN') = 'CHN' THEN '01'
                 ELSE '02'
                 END								                  			 AS ISSUERREGISTSTATE        --������ע�����    					 	Ĭ�ϣ�01
                ,CASE WHEN T1.C_ISSUER_ENTERPRISE_SIZE IN ('02','03') THEN '1'
                 ELSE '0'
                 END
                																		         AS ISSUERSMBFLAG            --������С΢��ҵ��ʶ					 Ĭ�ϣ���(0)
                ,CASE WHEN T1.C_RELEASE_PURPOSE = '0' THEN '01'
                 ELSE '02'
                 END													               AS BONDISSUEINTENT          --ծȯ����Ŀ��      					 Ĭ�ϣ�����(02)
                ,'0'						                             AS REABSFLAG                --���ʲ�֤ȯ����ʶ  					 	Ĭ�ϣ���(0)
                ,CASE WHEN REPLACE(T1.C_ISSUER_IDENTIFICATION_NO,'-','') = '202869177' THEN '1'
                 ELSE '0'
                 END																				 AS ORIGINATORFLAG   				 --�Ƿ������      					 1. ���������ƣ���������(202869177)����Ϊ�ǣ� 2. ����Ϊ��
                ,T1.D_VALUE_DATE                             AS STARTDATE                --��ʼ����          					 ��¼
                ,T1.D_END_DATE	                             AS DUEDATE                  --��������
                ,CASE WHEN (TO_DATE(T1.D_END_DATE,'YYYYMMDD') - TO_DATE(T1.D_VALUE_DATE,'YYYYMMDD')) / 365<0
                      THEN 0
                      ELSE (TO_DATE(T1.D_END_DATE,'YYYYMMDD') - TO_DATE(T1.D_VALUE_DATE,'YYYYMMDD')) / 365
                END                                          AS ORIGINALMATURITY         --ԭʼ����
                ,CASE WHEN (TO_DATE(T1.D_END_DATE,'YYYYMMDD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365<0
                      THEN 0
                      ELSE (TO_DATE(T1.D_END_DATE,'YYYYMMDD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365
                END                                          AS RESIDUALM                --ʣ������
                ,CASE WHEN T1.C_INTEREST_TYPE = '1' THEN '01'														 --�̶�����
                 ELSE '02'
                 END							                    		 	 AS RATETYPE                 --��������
                ,T1.F_BOND_RATE                              AS EXECUTIONRATE            --ִ������
                ,CASE WHEN T1.C_INTEREST_TYPE = '1' OR T1.C_REPRICING_DATE < p_data_dt_str THEN T1.D_END_DATE
                 ELSE T1.C_REPRICING_DATE
                 END                                   		 	 AS NEXTREPRICEDATE          --�´��ض�����      					1. ���������ͣ��̶������´��ض����գ��������ڣ�2. ����ȡϵͳ�ֶ� ��¼
                /*,CASE WHEN T2.RATETYPE = '01' THEN NULL
                ELSE CASE WHEN REPLACE(T2.BONDREDATE,'-','') < p_data_dt_str THEN 12
                		 ELSE ROUND(TO_NUMBER(T2.BONDREFREQUENCY),0)
                		 END
                END								                           AS NEXTREPRICEM             --�´��ض�������(��¼)   			1. ���������ͣ��̶�����Ĭ��Ϊ��NULL��2. ����ȡϵͳ�ֶΣ���λ����
                */
                ,CASE WHEN T1.C_INTEREST_TYPE = '1' THEN NULL
                 ELSE CASE WHEN (TO_DATE(CASE WHEN T1.C_REPRICING_DATE < p_data_dt_str THEN T1.D_END_DATE ELSE T1.C_REPRICING_DATE END,'YYYYMMDD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365 < 0 THEN 0
                 			ELSE (TO_DATE(CASE WHEN T1.C_REPRICING_DATE < p_data_dt_str THEN T1.D_END_DATE ELSE T1.C_REPRICING_DATE END,'YYYYMMDD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365
                 			END
                 END																				 AS NEXTREPRICEM             --�´��ض�������							 1. ���������ͣ��̶�����Ĭ��Ϊ��NULL��2. ����ȡ�´��ض�����-�������ڣ���λ����
                ,NULL                                        AS MODIFIEDDURATION         --��������
                ,T1.F_PAR_VAL                                AS DENOMINATION             --���
                ,T1.C_CURR_TYPE 	                           AS CURRENCY                 --����

   	FROM				RWA_DEV.ZGS_ATBOND T1																							--ծȯ��Ϣ��
   	INNER JOIN	TEMP_INVESTASSETDETAIL T2																					--������ϸ������¼�¼
   	ON					T1.C_BOND_CODE = T2.FLD_ASSET_CODE
    LEFT JOIN		TEMP_CUST_INFO T3																									--
    ON					REPLACE(T1.C_ISSUER_IDENTIFICATION_NO,'-','') = REPLACE(T3.CERTID,'-','')
    LEFT JOIN		RWA_DEV.ZGS_ATTYORG T4
    ON					T1.C_PUBLISHER = T4.C_ORG_ID
    AND					T4.DATANO = p_data_dt_str
    LEFT JOIN		RWA_DEV.RWA_CD_CODE_MAPPING T10
	  ON					T1.C_ISSUERTYPE_1 = T10.SITEMNO
	  AND					T10.SCODENO = 'ClientCategory'
	  AND					T10.SYSID = 'LC'
	  LEFT JOIN		RWA_DEV.RWA_CD_CODE_MAPPING T11
	  ON					T1.C_ISSUERTYPE_2 = T11.SITEMNO
	  AND					T11.SCODENO = 'ClientCategory'
	  AND					T11.SYSID = 'LC'
    WHERE				T1.DATANO = p_data_dt_str
 		;

 		COMMIT;

 		dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_LC_BONDINFO',cascade => true);


    --DBMS_OUTPUT.PUT_LINE('���������롾ծȯ��Ϣ��' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*Ŀ�������ͳ��*/
    --ͳ�Ʋ���ļ�¼
    SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_LC_BONDINFO;
    --Dbms_output.Put_line('RWA_DEV.RWA_LC_BONDINFO��ǰ��������ϵͳ-ծȯ���Ͷ��(�г�����)-ծȯ��Ϣ��¼Ϊ: ' || v_count || ' ��');



    --DBMS_OUTPUT.PUT_LINE('��ִ�� ' || v_pro_name || ' �洢���̽��� ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));
    p_po_rtncode := '1';
    p_po_rtnmsg  := '�ɹ�' || '-' || v_count;

    --�����쳣
    EXCEPTION WHEN OTHERS THEN
        --DBMS_OUTPUT.PUT_LINE('������,�������Ϊ:'||sqlcode||';������ϢΪ:'||sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace);
        ROLLBACK;
        p_po_rtncode := sqlcode;
        p_po_rtnmsg  := 'ծȯ��Ϣ('|| v_pro_name ||')ETLת��ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;

    RETURN;

END PRO_RWA_LC_BONDINFO;
/

