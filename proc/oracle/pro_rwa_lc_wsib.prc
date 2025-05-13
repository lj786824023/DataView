CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_LC_WSIB(
			 											p_data_dt_str	IN	VARCHAR2,		--�������� yyyyMMdd
       											p_po_rtncode	OUT	VARCHAR2,		--���ر�� 1 �ɹ�,0 ʧ��
														p_po_rtnmsg		OUT	VARCHAR2		--��������
				)
  /*
    �洢��������:RWA_DEV.PRO_RWA_LC_WSIB
    ʵ�ֹ���:���ϵͳ-���-��¼�̵�(������Դ���ϵͳ��ҵ�������Ϣȫ������RWA��Ʋ�¼�̵ױ���)
    ���ݿھ�:ȫ��
    ����Ƶ��:�³�����
    ��  ��  :V1.0.0
    ��д��  :LISY
    ��дʱ��:2016-06-06
    ��  λ  :�Ϻ���˶��Ϣ�����ɷ����޹�˾
    Դ  ��1 :RWA_DEV.ZGS_INVESTASSETDETAIL|������ϸ��
    Դ  ��2 :RWA_DEV.ZGS_FINANCING_INFO|��Ʒ��Ϣ��
    Դ  ��3 :RWA_DEV.ZGS_ATBOND|ծȯ��Ϣ��
    Դ  ��4 :RWA_DEV.ZGS_ATINTRUST_PLAN|�ʲ�����ƻ���
    Դ  ��5 :RWA.RWA_WS_FCII_BOND|ծȯ���Ͷ�ʲ�¼��
    Դ  ��6 :RWA.RWA_WS_FCII_PLAN|�ʹܼƻ����Ͷ�ʲ�¼��
    Դ  ��7 :RWA.RWA_WP_SUPPTASKORG|��¼��������ַ����ñ�
    Դ  ��8 :RWA.RWA_WP_SUPPTASK|��¼���񷢲���
    Ŀ���1 :RWA.RWA_WSIB_FCII_BOND|ծȯ���Ͷ�ʲ�¼�̵ױ�
    Ŀ���2 :RWA.RWA_WSIB_FCII_PLAN|�ʹܼƻ����Ͷ�ʲ�¼�̵ױ�
    �����¼(�޸���|�޸�ʱ��|�޸�����):
    */
  AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_LC_WSIB';
  --�����쳣����
  v_raise EXCEPTION;
  --���嵱ǰ����ļ�¼��
  v_count INTEGER;
  v_count1 INTEGER;

  BEGIN
    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*�����ȫ�����ݼ��������Ŀ���*/
    --1.���Ŀ����е�ԭ�м�¼
    --���ծȯ���Ͷ���̵ױ�
    --EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA.RWA_WSIB_FCII_BOND';
    DELETE FROM RWA.RWA_WSIB_FCII_BOND WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD');

    --����ʹܼƻ����Ͷ���̵ױ�
    --EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA.RWA_WSIB_FCII_PLAN';
    DELETE FROM RWA.RWA_WSIB_FCII_PLAN WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD');

    COMMIT;

    --2.���������������ݴ�Դ����뵽Ŀ�����
    --2.1 ���ϵͳ-ծȯ���Ͷ��ҵ��
    INSERT INTO RWA.RWA_WSIB_FCII_BOND(
                DATADATE                               --��������
                ,ORGID                              	 --����ID
                ,C_BOND_CODE       	       	 				 	 --ծȯ����
                ,C_BOND_ID         	       	 				 	 --ծȯ����
                ,C_BOND_NAME       	       	 				 	 --ծȯ����
                ,C_BOND_TYPE													 --ծȯ����
                ,BELONGORGCODE		 		     	 				 	 --ҵ����������
                ,ISSUERNAME                	 				 	 --ծȯ����������
                ,ISSUERORGCODE             	 				 	 --ծȯ��������֯��������
                ,ISSUERCOUNTRYCODE         	 				 	 --ծȯ������ע����Ҵ���
                ,ISSUERINDUSTRYID          	 				 	 --ծȯ����������������ҵ
                ,ISSUERMSMBFLAG            	 				 	 --ծȯ��������ҵ��ģ
                ,ISSUERRATINGORGCODE       	 				 	 --ծȯ�������ⲿ��������
                ,ISSUERRATING              	 				 	 --ծȯ�������ⲿ�������
                ,ISSUERRATINGDATE											 --ծȯ��������������
                ,ISSUERRATINGORGCODE2      	 				 	 --ծȯ�������ⲿ��������2
                ,ISSUERRATING2             	 				 	 --ծȯ�������ⲿ�������2
                ,ISSUERRATINGDATE2										 --ծȯ��������������2
                ,BONDRATINGORGCODE         	 				 	 --ծȯ��������
                ,BONDRATINGTYPE       		 	 				 	 --ծȯ������������
                ,BONDRATING                	 				 	 --ծȯ�����ȼ�
                ,BONDRATINGDATE												 --ծȯ��������
                ,BONDRATINGORGCODE2         	 				 --ծȯ��������2
                ,BONDRATINGTYPE2       		 	 				 	 --ծȯ������������2
                ,BONDRATING2                	 				 --ծȯ�����ȼ�2
                ,BONDRATINGDATE2											 --ծȯ��������2
                ,BONDLEVEL						     	 				 	 --ծȯ����
                ,RATETYPE                  	 				 	 --��������
                ,BONDREDATE                	 				 	 --ծȯ�ض�����
                ,BONDREFREQUENCY              			 	 --�ض���Ƶ��
    )
    WITH TEMP_INVESTASSETDETAIL AS (
        SELECT DISTINCT
        			 T3.FLD_ASSET_CODE						 AS FLD_ASSET_CODE
          FROM RWA_DEV.ZGS_INVESTASSETDETAIL T3
    INNER JOIN RWA_DEV.ZGS_FINANCING_INFO T4
            ON T3.FLD_FINANC_CODE = T4.FLD_FINANC_CODE
           AND T4.FLD_INCOME_TYPE <> '3'																		--3���ų��Ǳ�������
           AND T4.DATANO = p_data_dt_str
         WHERE T3.FLD_ASSET_TYPE = '2'																			-- 2��ծȯ��24���ʲ�����ƻ�
           AND T3.FLD_ASSET_STATUS = '1' 																		--1��״̬����
           AND T3.FLD_ASSET_FLAG = '1'   																		--1����Ʋ�Ʒ
           AND T3.FLD_DATE  = p_data_dt_str																	--��Ч����Ʋ�Ʒ���ֵ����ÿ�ո���
           AND T3.DATANO = p_data_dt_str
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
								 WHERE T1.SUPPTMPLID = 'M-0090'
							ORDER BY T3.SORTNO
		)
    SELECT
                TO_DATE(p_data_dt_str,'YYYYMMDD')            AS DATADATE                 --��������
                ,(SELECT ORGID FROM TMP_SUPPORG WHERE ROWNUM = 1)
                						                     				 		 AS ORGID                  	 --����ID                			���ղ�¼������������Ĭ��Ϊ�����ʲ�����(01160000)
                ,T1.C_BOND_CODE                              AS C_BOND_CODE       	     --ծȯ����
                ,T1.C_BOND_ID 										 					 AS C_BOND_ID         	     --ծȯ����
                ,T1.C_BOND_NAME                           	 AS C_BOND_NAME       	     --ծȯ����
                ,T1.C_BOND_TYPE															 AS C_BOND_TYPE							 --ծȯ����
                ,NVL(T3.BELONGORGCODE,'9998')				 		 AS BELONGORGCODE		 		     --ҵ����������            		 Ĭ�ϣ�9998(����)
                ,NVL(T3.ISSUERNAME,
                 CASE WHEN T1.C_BOND_TYPE IN ('01','17','19') THEN '�й���������'				 --��ծ��Ĭ���й���������
                 			WHEN T1.C_BOND_TYPE = '03' THEN '���ҿ������йɷ����޹�˾'					 --�����Խ���ծ��Ĭ�Ϲ��ҿ������йɷ����޹�˾
                 			ELSE ''
                 END)						                 	 					 AS ISSUERNAME               --ծȯ����������
                ,NVL(T3.ISSUERORGCODE,
                 CASE WHEN T1.C_BOND_TYPE IN ('01','17','19') THEN 'ZGZYZFZZJGDM'				 --��ծ��Ĭ��ZGZYZFZZJGDM
                 			WHEN T1.C_BOND_TYPE = '03' THEN '00001845-4'											 --�����Խ���ծ��Ĭ��00001845-4
                 			ELSE ''
                 END)                        		 						 AS ISSUERORGCODE            --ծȯ��������֯��������
                ,NVL(T3.ISSUERCOUNTRYCODE,'CHN')     				 AS ISSUERCOUNTRYCODE        --ծȯ������ע����Ҵ���			 Ĭ��CHN-�й�
                ,NVL(T3.ISSUERINDUSTRYID,
                 CASE WHEN T1.C_BOND_TYPE IN ('01','17','19') THEN '999999'							 --��ծ��Ĭ��δ֪
                 			ELSE 'J66'
                 END)          		 													 AS ISSUERINDUSTRYID         --ծȯ����������������ҵ			 Ĭ��J66-���ҽ��ڷ���
                ,T3.ISSUERMSMBFLAG                   				 AS ISSUERMSMBFLAG           --ծȯ��������ҵ��ģ
                ,T3.ISSUERRATINGORGCODE                  		 AS ISSUERRATINGORGCODE      --ծȯ�������ⲿ��������
                ,T3.ISSUERRATING     												 AS ISSUERRATING             --ծȯ�������ⲿ�������
                ,T3.ISSUERRATINGDATE												 AS ISSUERRATINGDATE				 --ծȯ��������������
                ,T3.ISSUERRATINGORGCODE2                 		 AS ISSUERRATINGORGCODE2     --ծȯ�������ⲿ��������2
                ,T3.ISSUERRATING2    												 AS ISSUERRATING2            --ծȯ�������ⲿ�������2
                ,T3.ISSUERRATINGDATE2												 AS ISSUERRATINGDATE2				 --ծȯ��������������2
                ,T3.BONDRATINGORGCODE                      	 AS BONDRATINGORGCODE        --ծȯ��������
                ,T3.BONDRATINGTYPE                         	 AS BONDRATINGTYPE       		 --ծȯ������������
                ,T3.BONDRATING	                             AS BONDRATING               --ծȯ�����ȼ�
                ,T3.BONDRATINGDATE													 AS BONDRATINGDATE					 --ծȯ��������
                ,T3.BONDRATINGORGCODE2                     	 AS BONDRATINGORGCODE2       --ծȯ��������2
                ,T3.BONDRATINGTYPE2                        	 AS BONDRATINGTYPE2      		 --ծȯ������������2
                ,T3.BONDRATING2	                             AS BONDRATING2              --ծȯ�����ȼ�2
                ,T3.BONDRATINGDATE2													 AS BONDRATINGDATE2					 --ծȯ��������2
                ,T3.BONDLEVEL                                AS BONDLEVEL						     --ծȯ����
                ,NVL(T3.RATETYPE,CASE WHEN T1.C_INTEREST_TYPE = '1' THEN '01'						 --�̶�����
                 ELSE '02'																															 --��������
                 END)																				 AS RATETYPE                 --��������
                ,T3.BONDREDATE 															 AS BONDREDATE               --ծȯ�ض�����
                ,T3.BONDREFREQUENCY                          AS BONDREFREQUENCY          --�ض���Ƶ��

    FROM				RWA_DEV.ZGS_ATBOND T1
		INNER JOIN	TEMP_INVESTASSETDETAIL T2
		ON 					T1.C_BOND_CODE = T2.FLD_ASSET_CODE
		LEFT	JOIN	(SELECT C_BOND_CODE
											 ,BELONGORGCODE
											 ,ISSUERNAME
											 ,ISSUERORGCODE
											 ,ISSUERCOUNTRYCODE
											 ,ISSUERINDUSTRYID
											 ,ISSUERMSMBFLAG
											 ,ISSUERRATINGORGCODE
											 ,ISSUERRATING
											 ,ISSUERRATINGDATE
											 ,ISSUERRATINGORGCODE2
											 ,ISSUERRATING2
											 ,ISSUERRATINGDATE2
											 ,BONDRATINGORGCODE
											 ,BONDRATINGTYPE
											 ,BONDRATING
											 ,BONDRATINGDATE
											 ,BONDRATINGORGCODE2
											 ,BONDRATINGTYPE2
											 ,BONDRATING2
											 ,BONDRATINGDATE2
											 ,BONDLEVEL
											 ,RATETYPE
											 ,BONDREDATE
											 ,BONDREFREQUENCY
									 FROM RWA.RWA_WS_FCII_BOND
									WHERE DATADATE = (SELECT MAX(DATADATE) FROM RWA.RWA_WS_FCII_BOND WHERE DATADATE < TO_DATE(p_data_dt_str,'YYYYMMDD'))
								) T3																								--ȡ���һ�ڲ�¼�����̵�
		ON					T1.C_BOND_CODE = T3.C_BOND_CODE
		WHERE 			T1.DATANO = p_data_dt_str														--ծȯ��Ϣ��,��ȡ��Ч��ծȯ��Ϣ
		ORDER BY		T1.C_BOND_CODE
		;

    COMMIT;

    /*Ŀ�������ͳ��*/
    --ͳ�Ʋ���ļ�¼��
    SELECT COUNT(1) INTO v_count FROM RWA.RWA_WSIB_FCII_BOND WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD');
    --Dbms_output.Put_line('RWA.RWA_WSIB_FCII_BOND��ǰ��������ϵͳ-ծȯ���Ͷ���̵����ݼ�¼Ϊ: ' || v_count || ' ��');


    --2.2 ���ϵͳ-�ʹܼƻ�Ͷ��ҵ��
    INSERT INTO RWA.RWA_WSIB_FCII_PLAN(
                DATADATE                               --��������
                ,ORGID                                 --����ID
                ,TRUSTCODE         	                   --���б��
                ,BELONGORGCODE		 		                 --ҵ����������
                ,FINANCERNAME                          --���׶�������
                ,FINANCERORGCODE                       --���׶�����֯��������
                ,FINANCERCOUNTRYCODE                   --���׶���ע����Ҵ���
                ,FINANCERINDUSTRYID                    --���׶���������ҵ����
                ,FINANCERMSMBFLAG                      --���׶�����ҵ��ģ
                ,GUARANTEETYPE                         --��������
                ,GUARANTEEBEGINDATE                    --������ʼ��
                ,GUARANTEEENDDATE                      --����������
                ,GUARANTORNAME                         --����������
                ,GUARANTORORGCODE                      --��������֯��������/���֤��
                ,GUARANTORCOUNTRYCODE           			 --������ע����Ҵ���
                ,GUARANTEECURRENCY                     --��������
                ,GUARANTEEVALUE                        --������ֵ

    )
    WITH TEMP_INVESTASSETDETAIL AS (
        SELECT 	T3.FLD_ASSET_CODE						AS FLD_ASSET_CODE
        			 ,T3.FLD_FINANC_CODE					AS FLD_FINANC_CODE
          FROM RWA_DEV.ZGS_INVESTASSETDETAIL T3
    INNER JOIN RWA_DEV.ZGS_FINANCING_INFO T4
            ON T3.FLD_FINANC_CODE = T4.FLD_FINANC_CODE
           AND T4.FLD_INCOME_TYPE <> '3'																		--3���ų��Ǳ�������
           AND T4.DATANO = p_data_dt_str
         WHERE T3.FLD_ASSET_TYPE = '24'																			-- 2��ծȯ��24���ʲ�����ƻ�
           AND T3.FLD_ASSET_STATUS = '1' 																		--1��״̬����
           AND T3.FLD_ASSET_FLAG = '1'   																		--1����Ʋ�Ʒ
           AND T3.FLD_DATE  = p_data_dt_str																	--��Ч����Ʋ�Ʒ���ֵ����ÿ�ո���
           AND T3.DATANO = p_data_dt_str
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
								 WHERE T1.SUPPTMPLID = 'M-0100'
							ORDER BY T3.SORTNO
		)
    SELECT
                TO_DATE(p_data_dt_str,'YYYYMMDD')            AS DATADATE                 --��������
                ,(SELECT ORGID FROM TMP_SUPPORG WHERE ROWNUM = 1)
                						                     				 		 AS ORGID                  	 --����ID                					���ղ�¼������������Ĭ��Ϊ�����ʲ�����(01160000)
                ,T2.FLD_FINANC_CODE || T1.C_PRD_CODE       	 AS TRUSTCODE         	     --���б��
                ,NVL(T3.BELONGORGCODE,'9998')						 AS BELONGORGCODE		 		     --ҵ����������
                ,T3.FINANCERNAME														 AS FINANCERNAME             --���׶�������
                ,T3.FINANCERORGCODE													 AS FINANCERORGCODE          --���׶�����֯��������
                ,NVL(T3.FINANCERCOUNTRYCODE,'CHN')			 		 AS FINANCERCOUNTRYCODE      --���׶���ע����Ҵ���        		Ĭ��CHN-�й�
                ,NVL(T3.FINANCERINDUSTRYID,'J66')          	 AS FINANCERINDUSTRYID       --���׶���������ҵ����        		Ĭ��J66-���ҽ��ڷ���
                ,T3.FINANCERMSMBFLAG                     		 AS FINANCERMSMBFLAG         --���׶�����ҵ��ģ
                ,T3.GUARANTEETYPE                            AS GUARANTEETYPE            --��������
                ,T3.GUARANTEEBEGINDATE											 AS GUARANTEEBEGINDATE       --������ʼ��
                ,T3.GUARANTEEENDDATE                         AS GUARANTEEENDDATE         --����������
                ,T3.GUARANTORNAME                       		 AS GUARANTORNAME            --����������
                ,T3.GUARANTORORGCODE 												 AS GUARANTORORGCODE         --��������֯��������/���֤��
                ,NVL(T3.GUARANTORCOUNTRYCODE,'CHN')        	 AS GUARANTORCOUNTRYCODE     --������ע����Ҵ���           	Ĭ��CHN-�й�
                ,NVL(T3.GUARANTEECURRENCY,'CNY')             AS GUARANTEECURRENCY        --��������                    		Ĭ��CNY-�����
                ,T3.GUARANTEEVALUE                           AS GUARANTEEVALUE           --������ֵ

    FROM				RWA_DEV.ZGS_ATINTRUST_PLAN T1
		INNER JOIN	TEMP_INVESTASSETDETAIL T2
		ON 					T1.C_PRD_CODE = T2.FLD_ASSET_CODE
		LEFT	JOIN	(SELECT TRUSTCODE
											 ,BELONGORGCODE
											 ,FINANCERNAME
											 ,FINANCERORGCODE
											 ,FINANCERCOUNTRYCODE
											 ,FINANCERINDUSTRYID
											 ,FINANCERMSMBFLAG
											 ,GUARANTEETYPE
											 ,GUARANTEEBEGINDATE
											 ,GUARANTEEENDDATE
											 ,GUARANTORNAME
											 ,GUARANTORORGCODE
											 ,GUARANTORCOUNTRYCODE
											 ,GUARANTEECURRENCY
											 ,GUARANTEEVALUE
									 FROM RWA.RWA_WS_FCII_PLAN
									WHERE DATADATE = (SELECT MAX(DATADATE) FROM RWA.RWA_WS_FCII_PLAN WHERE DATADATE < TO_DATE(p_data_dt_str,'YYYYMMDD'))
								) T3																								--ȡ���һ�ڲ�¼�����̵�
		ON					T2.FLD_FINANC_CODE || T1.C_PRD_CODE = T3.TRUSTCODE
		WHERE 			T1.DATANO = p_data_dt_str														--ծȯ��Ϣ��,��ȡ��Ч��ծȯ��Ϣ
		ORDER BY		T2.FLD_FINANC_CODE || T1.C_PRD_CODE
		;

    COMMIT;

    /*Ŀ�������ͳ��*/
    --ͳ�Ʋ���ļ�¼��
    SELECT COUNT(1) INTO v_count1 FROM RWA.RWA_WSIB_FCII_PLAN WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD');
    --Dbms_output.Put_line('RWA.RWA_WSIB_FCII_PLAN��ǰ��������ϵͳ-�ʹܼƻ�Ͷ���̵����ݼ�¼Ϊ: ' || v_count || ' ��');



    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̽��� ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    p_po_rtncode := '1';
	  p_po_rtnmsg  := '�ɹ�' || '-' || (v_count + v_count1);
		--�����쳣
		EXCEPTION
    WHEN OTHERS THEN
				 --Dbms_output.Put_line('������,�������Ϊ:'||sqlcode||';������ϢΪ:'||sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace);
       	 ROLLBACK;
   			 p_po_rtncode := sqlcode;
   			 p_po_rtnmsg  := '���Ͷ��ҵ��¼�����̵�('|| v_pro_name ||')����ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_LC_WSIB;
/

