CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_ABS_ISSURE_CMRELEVENCE(
			 											P_DATA_DT_STR	IN	VARCHAR2,		--��������
       											P_PO_RTNCODE	OUT	VARCHAR2,		--���ر�� 1 �ɹ�,0 ʧ��
														P_PO_RTNMSG		OUT	VARCHAR2		--��������
				)
  /*
    �洢��������:RWA_DEV.PRO_RWA_ABS_ISSURE_CMRELEVENCE
    ʵ�ֹ���:�Ŵ�ϵͳ-��ͬ�뻺�������,��ṹΪ��ͬ�����������
    ���ݿھ�:ȫ��
    ����Ƶ��:�³�
    ��  ��  :V1.0.0
    ��д��  :YUSJ
    ��дʱ��:2016-04-28
    ��  λ	:�Ϻ���˶��Ϣ�����ɷ����޹�˾
    Դ  ��1	:CMS_GUARANTY_INFO|��������Ϣ��
    Դ  ��2	:CMS_BUSINESS_DUEBILL|����ҵ������Ϣ��
    Դ  ��3	:CMS_BUSINESS_CONTRACT|����ҵ���ͬ��
    Դ  ��4	:CMS_GUARANTY_CONTRACT|������ͬ��Ϣ��
    Դ  ��5	:CMS_CONTRACT_RELATIVE|��ͬ������
    Դ  ��6	:CMS_GUARANTY_RELATIVE|������ͬ�뵣���������
    Ŀ���	:RWA_XD_CMRELEVENCE|�Ŵ�ϵͳ-��ͬ�뻺�������
    ������	:��
    �����¼(�޸���|�޸�ʱ��|�޸�����):
  */
  AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_ABS_ISSURE_CMRELEVENCE';
  --�����ж�ֵ����
  v_count1 INTEGER;
  --�����쳣����
  v_raise EXCEPTION;

  BEGIN
    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*�����ȫ�����ݼ��������Ŀ���*/
    --1.���Ŀ����е�ԭ�м�¼
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_ABS_ISSURE_CMRELEVENCE';

    /*������Ч����º�ͬ��Ӧ�ĵ���ѺƷ����*/
    INSERT INTO RWA_DEV.RWA_ABS_ISSURE_CMRELEVENCE(
         				 DATADATE       									--��������
         				,DATANO                           --������ˮ��
         				,CONTRACTID                       --��ͬ����
         				,MITIGATIONID                     --���������
         				,MITIGCATEGORY                    --����������
         				,SGUARCONTRACTID                  --Դ������ͬ����
         				,GROUPID                          --������
    )WITH TEMP_RELATIVE AS (SELECT DISTINCT T5.GUARANTYID,T1.CONTRACTID,T3.GUARANTYTYPE
                       FROM RWA_DEV.RWA_ABS_ISSURE_CONTRACT T1
                       INNER JOIN
                           (SELECT SERIALNO,OBJECTNO
                            FROM RWA_DEV.NCM_CONTRACT_RELATIVE
                            WHERE DATANO=P_DATA_DT_STR
                            AND OBJECTTYPE = 'GuarantyContract'
                            GROUP BY  SERIALNO,OBJECTNO
                            ) T2
                      ON T1.CONTRACTID = 'ABS' || T2.SERIALNO
                      INNER JOIN RWA_DEV.NCM_GUARANTY_CONTRACT T3
                      ON T2.OBJECTNO = T3.SERIALNO
                      AND T3.DATANO=P_DATA_DT_STR
                      INNER JOIN (SELECT CONTRACTNO, GUARANTYID
                                  FROM RWA_DEV.NCM_GUARANTY_RELATIVE
                                  WHERE DATANO=P_DATA_DT_STR
                                  GROUP BY CONTRACTNO, GUARANTYID
                                  ) T4
                      ON T3.SERIALNO=T4.CONTRACTNO
                      INNER JOIN RWA_DEV.NCM_GUARANTY_INFO T5
                      ON T4.GUARANTYID=T5.GUARANTYID
                      AND T5.DATANO=P_DATA_DT_STR
                   )

     SELECT
         				TO_DATE(P_DATA_DT_STR,'YYYYMMDD')										          AS	datadate       									--��������
         				,T1.DATANO																			              AS	datano              						--������ˮ��
         				,T2.CONTRACTID																						    AS	contractid          						--��ͬ����  (��������ͬ���ж�����ͬ�Ƿ���Ч)
         				,'ABS'||T1.GUARANTYID                         							  AS 	mitigationid        						--���������(��֤���ݵĻ��ͺ�ͬ�͵���Ѻ�Ļ��ͺ�ͬ�ֿ�ȡ)
         				,'03' 						  																          AS	mitigcategory       						--����������   ȫ�ǵ���ѺƷ
         				,''      																	                    AS	sguarcontractid     						--Դ������ͬ����(�������)
         				,''																									          AS	groupid             						--������
    FROM   			RWA_DEV.NCM_GUARANTY_INFO T1
    INNER JOIN 	TEMP_RELATIVE T2
    ON 					T1.GUARANTYID = T2.GUARANTYID
    WHERE 			T1.DATANO=P_DATA_DT_STR
    AND 				T1.GUARANTYTYPE NOT IN ('020080','020090')     --����֤����������֤����Ϊ��֤
    ;
    COMMIT;

    /*������Ч����º�ͬ��Ӧ�ĵ���ѺƷ����(����֤��Ϊ��֤)*/
    INSERT INTO RWA_DEV.RWA_ABS_ISSURE_CMRELEVENCE(
         				 DATADATE       									--��������
         				,DATANO                           --������ˮ��
         				,CONTRACTID                       --��ͬ����
         				,MITIGATIONID                     --���������
         				,MITIGCATEGORY                    --����������
         				,SGUARCONTRACTID                  --Դ������ͬ����
         				,GROUPID                          --������
    )WITH
    TEMP_RELATIVE AS (SELECT DISTINCT T5.GUARANTYID,T1.CONTRACTID,T3.GUARANTYTYPE
                       FROM RWA_DEV.RWA_ABS_ISSURE_CONTRACT T1
                       INNER JOIN
                           (SELECT SERIALNO,OBJECTNO
                            FROM RWA_DEV.NCM_CONTRACT_RELATIVE
                            WHERE DATANO=P_DATA_DT_STR
                            AND OBJECTTYPE = 'GuarantyContract'
                            GROUP BY  SERIALNO,OBJECTNO
                            ) T2
                      ON T1.CONTRACTID = 'ABS' || T2.SERIALNO
                      INNER JOIN RWA_DEV.NCM_GUARANTY_CONTRACT T3
                      ON T2.OBJECTNO = T3.SERIALNO
                      AND T3.DATANO=P_DATA_DT_STR
                      INNER JOIN (SELECT CONTRACTNO, GUARANTYID
                                  FROM RWA_DEV.NCM_GUARANTY_RELATIVE
                                  WHERE DATANO=P_DATA_DT_STR
                                  GROUP BY CONTRACTNO, GUARANTYID
                                  ) T4
                      ON T3.SERIALNO=T4.CONTRACTNO
                      INNER JOIN RWA_DEV.NCM_GUARANTY_INFO T5
                      ON T4.GUARANTYID=T5.GUARANTYID
                      AND T5.DATANO=P_DATA_DT_STR
                   )

     SELECT
         				TO_DATE(P_DATA_DT_STR,'YYYYMMDD')										          AS	datadate       									--��������
         				,T1.DATANO																			              AS	datano              						--������ˮ��
         				,T2.CONTRACTID																						    AS	contractid          						--��ͬ����  (��������ͬ���ж�����ͬ�Ƿ���Ч)
         				,'ABS'||T1.GUARANTYID                         							  AS 	mitigationid        						--���������(��֤���ݵĻ��ͺ�ͬ�͵���Ѻ�Ļ��ͺ�ͬ�ֿ�ȡ)
         				,'02' 						  																          AS	mitigcategory       						--����������   ȫ�ǵ���ѺƷ
         				,''      																	                    AS	sguarcontractid     						--Դ������ͬ����(�������)
         				,''																									          AS	groupid             						--������
    FROM   			RWA_DEV.NCM_GUARANTY_INFO T1
    INNER JOIN 	TEMP_RELATIVE T2
    ON 					T1.GUARANTYID = T2.GUARANTYID
    WHERE 			T1.DATANO = P_DATA_DT_STR
    AND 				T1.GUARANTYTYPE IN ('020080','020090')     --����֤����������֤����Ϊ��֤
    ;
    COMMIT;


    /*������Ч����º�ͬ�б�֤�������*/
    INSERT INTO RWA_DEV.RWA_ABS_ISSURE_CMRELEVENCE(
         				 DATADATE       									--��������
         				,DATANO                           --������ˮ��
         				,CONTRACTID                       --��ͬ����
         				,MITIGATIONID                     --���������
         				,MITIGCATEGORY                    --����������
         				,SGUARCONTRACTID                  --Դ������ͬ����
         				,GROUPID                          --������
    )
    SELECT
         				TO_DATE(P_DATA_DT_STR,'YYYYMMDD')										         AS	datadate       									--��������
         				,P_DATA_DT_STR																	             AS	datano              						--������ˮ��
         				,'ABS'||T1.CONTRACTNO																		     AS	contractid          						--��ͬ����  (��������ͬ���ж�����ͬ�Ƿ���Ч)
         				,'ABS'||T1.CONTRACTNO                        								 AS mitigationid        						--���������(��֤���ݵĻ��ͺ�ͬ�͵���Ѻ�Ļ��ͺ�ͬ�ֿ�ȡ)
         				,'03'								  																       AS	mitigcategory       						--����������   ��֤���ǽ�����ѺƷ
         				,T1.CONTRACTNO     																	         AS	sguarcontractid     						--Դ������ͬ����(�������)
         				,''																									         AS	groupid             						--������

		FROM  			RWA_DEV.RWA_TEMP_BAIL2 T1															--�Ŵ���ͬ��
    INNER JOIN	RWA_DEV.RWA_ABS_ISSURE_CONTRACT T2										--�Ŵ���ݱ�
    ON					'ABS' || T1.CONTRACTNO = T2.CONTRACTID
		WHERE 			T1.ISMAX = '1'																				--ȡ��ͬ��ͬ������һ����Ϊ���
    ;
		COMMIT;


		--���뱣֤������Ϣ��
		INSERT INTO RWA_DEV.RWA_ABS_ISSURE_CMRELEVENCE(
         				 DATADATE       									--��������
         				,DATANO                           --������ˮ��
         				,CONTRACTID                       --��ͬ����
         				,MITIGATIONID                     --���������
         				,MITIGCATEGORY                    --����������
         				,SGUARCONTRACTID                  --Դ������ͬ����
         				,GROUPID                          --������
    )
    WITH TEMP_RELATIVE AS (SELECT DISTINCT T1.CONTRACTID,T3.SERIALNO
                       FROM RWA_DEV.RWA_ABS_ISSURE_CONTRACT T1
                       INNER JOIN
                           (SELECT SERIALNO,OBJECTNO
                            FROM RWA_DEV.NCM_CONTRACT_RELATIVE
                            WHERE DATANO=P_DATA_DT_STR
                            AND OBJECTTYPE = 'GuarantyContract'
                            GROUP BY  SERIALNO,OBJECTNO
                            ) T2
                      ON T1.CONTRACTID = 'ABS' || T2.SERIALNO
                      INNER JOIN RWA_DEV.NCM_GUARANTY_CONTRACT T3
                      ON T2.OBJECTNO = T3.SERIALNO
                      AND T3.DATANO=P_DATA_DT_STR
                    )
    SELECT
         				TO_DATE(P_DATA_DT_STR,'YYYYMMDD')										         AS	datadate       									--��������
         				,T1.DATANO																			             AS	datano              						--������ˮ��
         				,T2.CONTRACTID																						   AS	contractid          						--��ͬ����  (��������ͬ���ж�����ͬ�Ƿ���Ч)
         				,'ABS'||T1.SERIALNO                         							   AS mitigationid        						--���������(��֤���ݵĻ��ͺ�ͬ�͵���Ѻ�Ļ��ͺ�ͬ�ֿ�ȡ)
         				,'02'								  																       AS	mitigcategory       						--����������   02-��֤
         				,T2.CONTRACTID     																	         AS	sguarcontractid     						--Դ������ͬ����(�������)
         				,''																									         AS	groupid             						--������

		FROM 				RWA_DEV.NCM_GUARANTY_CONTRACT T1
    INNER JOIN 	TEMP_RELATIVE T2
    ON 					T1.SERIALNO = T2.SERIALNO
    WHERE 			T1.DATANO = P_DATA_DT_STR
    AND 				T1.GUARANTYTYPE IN ('010010','010020','010030')
    ;
    COMMIT;

    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_ABS_ISSURE_CMRELEVENCE',cascade => true);

    /*Ŀ�������ͳ��*/
    SELECT COUNT(1) INTO v_count1 FROM RWA_DEV.RWA_ABS_ISSURE_CMRELEVENCE;

		P_PO_RTNCODE := '1';
	  P_PO_RTNMSG  := '�ɹ�'||'-'||v_count1;
		--�����쳣
		EXCEPTION
    WHEN OTHERS THEN
				 --Dbms_output.Put_line('������,�������Ϊ:'||sqlcode||';������ϢΪ:'||sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace);
       	 ROLLBACK;
   			 P_PO_RTNCODE := sqlcode;
   			 P_PO_RTNMSG  := '�ʲ�֤ȯ��-��ͬ�뻺�������('||v_pro_name||')ETLת��ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_ABS_ISSURE_CMRELEVENCE;
/

