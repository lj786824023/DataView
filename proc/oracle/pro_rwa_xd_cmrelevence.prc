CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_XD_CMRELEVENCE(
			 											P_DATA_DT_STR	IN	VARCHAR2,		--��������
       											P_PO_RTNCODE	OUT	VARCHAR2,		--���ر�� 1 �ɹ�,0 ʧ��
														P_PO_RTNMSG		OUT	VARCHAR2		--��������
				)
  /*
    �洢��������:RWA_DEV.PRO_RWA_XD_CMRELEVENCE
    ʵ�ֹ���:�Ŵ�ϵͳ-��ͬ�뻺�������,��ṹΪ��ͬ�����������
    ���ݿھ�:ȫ��
    ����Ƶ��:�³�
    ��  ��  :V1.0.0
    ��д��  :YUSJ
    ��дʱ��:2016-04-28
    ��  λ	:�Ϻ���˶��Ϣ�����ɷ����޹�˾
    Դ  ��1	:NCM_GUARANTY_INFO|��������Ϣ��
    Դ  ��2	:NCM_BUSINESS_DUEBILL|����ҵ������Ϣ��
    Դ  ��3	:NCM_BUSINESS_CONTRACT|����ҵ���ͬ��
    Դ  ��4	:NCM_GUARANTY_CONTRACT|������ͬ��Ϣ��
    Դ  ��5	:NCM_CONTRACT_RELATIVE|��ͬ������
    Դ  ��6	:NCM_GUARANTY_RELATIVE|������ͬ�뵣���������
    Դ  ��7	:RD_LOAN_NOR|��������
    Ŀ���	:RWA_XD_CMRELEVENCE|�Ŵ�ϵͳ-��ͬ�뻺�������
    ������	:��
    �����¼(�޸���|�޸�ʱ��|�޸�����):
    xlp  20190412 ����һ�ں������ݹ�������
    
    
    
  */
  AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  --v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_XD_CMRELEVENCE';
  --�����ж�ֵ����
  v_count1 INTEGER;
  --�����쳣����
  v_raise EXCEPTION;
    --������ʱ����
  --v_tabname VARCHAR2(200);
  --���崴�����
  --v_create VARCHAR2(1000);

  BEGIN
    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*�����ȫ�����ݼ��������Ŀ���*/
    --1.���Ŀ����е�ԭ�м�¼
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_XD_CMRELEVENCE';

    /*1.1 ������Ч����º�ͬ��Ӧ�ĵ���ѺƷ����(��ͨ)*/
    INSERT INTO RWA_DEV.RWA_XD_CMRELEVENCE(
         				 DATADATE       									--��������
         				,DATANO                           --������ˮ��
         				,CONTRACTID                       --��ͬ����
         				,MITIGATIONID                     --���������
         				,MITIGCATEGORY                    --����������
         				,SGUARCONTRACTID                  --Դ������ͬ����
         				,GROUPID                          --������
                ,FLAG
    )WITH TEMP_CMRELEVENCE1 AS(SELECT DISTINCT  T3.SERIALNO AS CONTRACTNO
                               FROM RWA_DEV.NCM_BUSINESS_DUEBILL T1
                               INNER JOIN RWA_DEV.NCM_BUSINESS_TYPE T2
                               ON T1.BUSINESSTYPE=T2.TYPENO
                               AND T1.DATANO=T2.DATANO
                               AND T2.SORTNO NOT LIKE '30%' --�ų������ҵ��
                               INNER JOIN RWA_DEV.NCM_BUSINESS_CONTRACT T3
                               ON T1.RELATIVESERIALNO2=T3.SERIALNO        --�����Խ��Ϊ׼�����Թ�����ͬʱ����Ӧ�üӺ�ͬ����Ч����
                               AND T1.DATANO=T3.DATANO
                               --AND T3.BUSINESSTYPE || T3.BUSINESSNATURE <> '1030301005'
                               INNER JOIN RWA_DEV.NCM_BUSINESS_HISTORY T31
                               ON T1.SERIALNO=T31.SERIALNO
                               AND T31.DATANO=P_DATA_DT_STR
                               AND T31.BALANCE>0
                               WHERE T1.DATANO=P_DATA_DT_STR
                               --AND T1.BALANCE>0           --��Ч�ж�����
                               AND T1.SERIALNO<>'BD2014110400000001'
                               AND T1.BUSINESSTYPE NOT IN ('1040101010','1040101020','1040101030','1040102010','1040102020','1040102040','10303010'
                                   ,'1040105060','1040201010','1040201020','1040202010','105010','11105010','11105020','10302010','10302015','10302020')  --�ų�ͬҵ���ع���Ͷ�ʣ�ί�д���ҵ��,���ֺ�ת��Ҳ�ų�����Ʊ����Ϣ��Ϊ����
                             )
    ,TEMP_RELATIVE AS (SELECT DISTINCT T5.GUARANTYID,T1.CONTRACTNO
                       FROM TEMP_CMRELEVENCE1 T1
                       INNER JOIN
                           (SELECT SERIALNO,OBJECTNO
                            FROM RWA_DEV.NCM_CONTRACT_RELATIVE
                            WHERE DATANO=P_DATA_DT_STR
                            AND OBJECTTYPE = 'GuarantyContract'
                            GROUP BY  SERIALNO,OBJECTNO
                            ) T2
                      ON T1.CONTRACTNO=T2.SERIALNO
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
         				,T2.CONTRACTNO																			          AS	contractid          						--��ͬ����  (��������ͬ���ж�����ͬ�Ƿ���Ч)
         				,'YP'||T1.GUARANTYID                         								  AS 	mitigationid        						--���������(��֤���ݵĻ��ͺ�ͬ�͵���Ѻ�Ļ��ͺ�ͬ�ֿ�ȡ)
         				,'03' 						  																          AS	mitigcategory       						--����������   ȫ�ǵ���ѺƷ
         				,''      																	                    AS	sguarcontractid     						--Դ������ͬ����(�������)
         				,''																									          AS	groupid             						--������
                ,'DZY|PT'
    FROM   RWA_DEV.NCM_GUARANTY_INFO T1
    INNER JOIN TEMP_RELATIVE T2
    ON T1.GUARANTYID=T2.GUARANTYID
    WHERE T1.DATANO=P_DATA_DT_STR
    --AND T1.CLRSTATUS='01'                  --modify by yushuangjiang
    --AND T1.CLRGNTSTATUS IN ('03','10')     --���ھ������Ŵ��¿�ȷ����������״̬�޶�����ȥ��
    AND T1.GUARANTYTYPEID NOT IN('004001004001','004001005001','004001006001','004001006002','001001003001')     --����֤����������֤,�����Ա������������Ա��� ����Ϊ��֤
    and exists (select 1 from rwa_dev.rwa_xd_contract c where T2.CONTRACTNO = c.contractid AND C.DATANO=P_DATA_DT_STR)
    ;
    COMMIT;

    /*1.2 ������Ч����º�ͬ��Ӧ�ĵ���ѺƷ����(���ڴ���-΢����)*/
   /* INSERT INTO RWA_DEV.RWA_XD_CMRELEVENCE(
                 DATADATE                         --��������
                ,DATANO                           --������ˮ��
                ,CONTRACTID                       --��ͬ����
                ,MITIGATIONID                     --���������
                ,MITIGCATEGORY                    --����������
                ,SGUARCONTRACTID                  --Դ������ͬ����
                ,GROUPID                          --������
                ,flag
    )WITH TEMP_CMRELEVENCE1 AS(SELECT DISTINCT  T3.SERIALNO AS CONTRACTNO
                               FROM RWA_DEV.NCM_BUSINESS_DUEBILL T1
                               INNER JOIN RWA_DEV.NCM_BUSINESS_TYPE T2
                               ON T1.BUSINESSTYPE=T2.TYPENO
                               AND T1.DATANO=T2.DATANO
                               AND T2.SORTNO NOT LIKE '30%' --�ų������ҵ��
                               INNER JOIN RWA_DEV.NCM_BUSINESS_CONTRACT T3
                               ON T1.RELATIVESERIALNO2=T3.SERIALNO        --�����Խ��Ϊ׼�����Թ�����ͬʱ����Ӧ�üӺ�ͬ����Ч����
                               AND T1.DATANO=T3.DATANO
                               --AND T3.BUSINESSTYPE || T3.BUSINESSNATURE <> '1030301005'
                               INNER JOIN \*RWA_DEV.NCM_BUSINESS_HISTORY T31
                               ON T1.SERIALNO=T31.SERIALNO
                               AND T31.DATANO=P_DATA_DT_STR
                               AND (T31.OVERDUEBALANCE+T31.DULLBALANCE+T31.BADBALANCE)>0   --ȡ�����ڵļ�¼*\
                                          rwa_dev.brd_loan_nor t31
                               ON  T1.SERIALNO = t31.crdt_acct_no
                               AND t31.datano = P_DATA_DT_STR
                               AND t31.sbjt_cd = '13100001' --����΢����
                               WHERE  T1.DATANO=P_DATA_DT_STR
                               --AND T1.BUSINESSTYPE='11103030'  --ֻȡ΢����ҵ��
                               )
    ,TEMP_RELATIVE AS (SELECT DISTINCT T5.GUARANTYID,T1.CONTRACTNO
                       FROM TEMP_CMRELEVENCE1 T1
                       INNER JOIN
                           (SELECT SERIALNO,OBJECTNO
                            FROM RWA_DEV.NCM_CONTRACT_RELATIVE
                            WHERE DATANO=P_DATA_DT_STR
                            AND OBJECTTYPE = 'GuarantyContract'
                            GROUP BY  SERIALNO,OBJECTNO
                            ) T2
                      ON T1.CONTRACTNO=T2.SERIALNO
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
                TO_DATE(P_DATA_DT_STR,'YYYYMMDD')                             AS  datadate                        --��������
                ,T1.DATANO                                                    AS  datano                          --������ˮ��
                ,T2.CONTRACTNO                                                AS  contractid                      --��ͬ����  (��������ͬ���ж�����ͬ�Ƿ���Ч)
                ,'YP'||T1.GUARANTYID                                          AS  mitigationid                    --���������(��֤���ݵĻ��ͺ�ͬ�͵���Ѻ�Ļ��ͺ�ͬ�ֿ�ȡ)
                ,'03'                                                         AS  mitigcategory                   --����������   ȫ�ǵ���ѺƷ
                ,''                                                           AS  sguarcontractid                 --Դ������ͬ����(�������)
                ,''                                                           AS  groupid                         --������
                ,'DZY|YQWLD'
    FROM   RWA_DEV.NCM_GUARANTY_INFO T1
    INNER JOIN TEMP_RELATIVE T2
    ON T1.GUARANTYID=T2.GUARANTYID
    WHERE T1.DATANO=P_DATA_DT_STR
    --AND T1.CLRSTATUS='01'                  --modify by yushuangjiang
    --AND T1.CLRGNTSTATUS IN ('03','10')     --���ھ������Ŵ��¿�ȷ����������״̬�޶�����ȥ��
    AND T1.GUARANTYTYPEID NOT IN('004001004001','004001005001','004001006001','004001006002','001001003001')     --����֤����������֤,�����Ա������������Ա��� ����Ϊ��֤
    AND NOT EXISTS (SELECT 1 FROM RWA_DEV.RWA_XD_CMRELEVENCE T7 WHERE 'YP'||T2.GUARANTYID=T7.mitigationid AND T2.CONTRACTNO=T7.contractid AND T7.DATANO=P_DATA_DT_STR)
    and exists (select 1 from rwa_dev.rwa_xd_contract c where T2.CONTRACTNO = c.contractid AND C.DATANO=P_DATA_DT_STR)
    ;
    COMMIT;*/
    
    /*1.3 ������Ч����º�ͬ��Ӧ�ĵ���ѺƷ����(���ڴ���-����ҵ��)*/
    INSERT INTO RWA_DEV.RWA_XD_CMRELEVENCE(
                 DATADATE                         --��������
                ,DATANO                           --������ˮ��
                ,CONTRACTID                       --��ͬ����
                ,MITIGATIONID                     --���������
                ,MITIGCATEGORY                    --����������
                ,SGUARCONTRACTID                  --Դ������ͬ����
                ,GROUPID                          --������
                ,flag
    )WITH TEMP_CMRELEVENCE1 AS(SELECT DISTINCT  T3.SERIALNO AS CONTRACTNO
                               FROM RWA_DEV.NCM_BUSINESS_DUEBILL T1
                               INNER JOIN RWA_DEV.NCM_BUSINESS_TYPE T2
                               ON T1.BUSINESSTYPE=T2.TYPENO
                               AND T1.DATANO=T2.DATANO
                               AND T2.SORTNO NOT LIKE '30%' --�ų������ҵ��
                               INNER JOIN RWA_DEV.NCM_BUSINESS_CONTRACT T3
                               ON T1.RELATIVESERIALNO2=T3.SERIALNO        --�����Խ��Ϊ׼�����Թ�����ͬʱ����Ӧ�üӺ�ͬ����Ч����
                               AND T1.DATANO=T3.DATANO
                               INNER JOIN BRD_LOAN_NOR T4 
                               ON  T4.DATANO = P_DATA_DT_STR
                               AND substr(T4.SBJT_CD,1,4) = '1310' --��Ŀ��� ���ڴ���
                               and T4.SBJT_CD != '13100001' --���в���΢���������ڴ���
                               AND T4.CRDT_ACCT_NO = T1.SERIALNO
                                       WHERE  T1.DATANO=P_DATA_DT_STR
    
                               )
    ,TEMP_RELATIVE AS (SELECT DISTINCT T5.GUARANTYID,T1.CONTRACTNO
                       FROM TEMP_CMRELEVENCE1 T1
                       INNER JOIN
                           (SELECT SERIALNO,OBJECTNO
                            FROM RWA_DEV.NCM_CONTRACT_RELATIVE
                            WHERE DATANO=P_DATA_DT_STR
                            AND OBJECTTYPE = 'GuarantyContract'
                            GROUP BY  SERIALNO,OBJECTNO
                            ) T2
                      ON T1.CONTRACTNO=T2.SERIALNO
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
                TO_DATE(P_DATA_DT_STR,'YYYYMMDD')                             AS  datadate                        --��������
                ,T1.DATANO                                                    AS  datano                          --������ˮ��
                ,T2.CONTRACTNO                                                AS  contractid                      --��ͬ����  (��������ͬ���ж�����ͬ�Ƿ���Ч)
                ,'YP'||T1.GUARANTYID                                          AS  mitigationid                    --���������(��֤���ݵĻ��ͺ�ͬ�͵���Ѻ�Ļ��ͺ�ͬ�ֿ�ȡ)
                ,'03'                                                         AS  mitigcategory                   --����������   ȫ�ǵ���ѺƷ
                ,''                                                           AS  sguarcontractid                 --Դ������ͬ����(�������)
                ,''                                                           AS  groupid                         --������
                ,'DZY|YQ'
    FROM   RWA_DEV.NCM_GUARANTY_INFO T1
    INNER JOIN TEMP_RELATIVE T2
    ON T1.GUARANTYID=T2.GUARANTYID
    WHERE T1.DATANO=P_DATA_DT_STR
    --AND T1.CLRSTATUS='01'                  --modify by yushuangjiang
    --AND T1.CLRGNTSTATUS IN ('03','10')     --���ھ������Ŵ��¿�ȷ����������״̬�޶�����ȥ��
    AND T1.GUARANTYTYPEID NOT IN('004001004001','004001005001','004001006001','004001006002','001001003001')     --����֤����������֤,�����Ա������������Ա��� ����Ϊ��֤
    AND NOT EXISTS (SELECT 1 FROM RWA_DEV.RWA_XD_CMRELEVENCE T7 WHERE 'YP'||T2.GUARANTYID=T7.mitigationid AND T2.CONTRACTNO=T7.contractid AND T7.DATANO=P_DATA_DT_STR)
    and exists (select 1 from rwa_dev.rwa_xd_contract c where T2.CONTRACTNO = c.contractid AND C.DATANO=P_DATA_DT_STR)
    ;
    COMMIT;
    
    /*1.4 ������Ч����º�ͬ��Ӧ�ĵ���ѺƷ����(׷�ӵ�PUTOUT�� �ϵĵ���ѺƷ��Ϣ)*/
    INSERT INTO RWA_DEV.RWA_XD_CMRELEVENCE(
                 DATADATE                         --��������
                ,DATANO                           --������ˮ��
                ,CONTRACTID                       --��ͬ����
                ,MITIGATIONID                     --���������
                ,MITIGCATEGORY                    --����������
                ,SGUARCONTRACTID                  --Դ������ͬ����
                ,GROUPID                          --������
                ,flag
    )WITH TEMP_CMRELEVENCE1 AS(SELECT DISTINCT  T3.SERIALNO AS CONTRACTNO,T4.SERIALNO AS BPSERIALNO
                               FROM RWA_DEV.NCM_BUSINESS_DUEBILL T1
                               INNER JOIN RWA_DEV.NCM_BUSINESS_TYPE T2
                               ON T1.BUSINESSTYPE=T2.TYPENO
                               AND T1.DATANO=T2.DATANO
                               AND T2.SORTNO NOT LIKE '30%' --�ų������ҵ��
                               INNER JOIN RWA_DEV.NCM_BUSINESS_CONTRACT T3
                               ON T1.RELATIVESERIALNO2=T3.SERIALNO        --�����Խ��Ϊ׼�����Թ�����ͬʱ����Ӧ�üӺ�ͬ����Ч����
                               AND T1.DATANO=T3.DATANO
                               --AND T3.BUSINESSTYPE || T3.BUSINESSNATURE <> '1030301005'
                               INNER JOIN RWA_DEV.NCM_BUSINESS_PUTOUT T4
                               ON T3.SERIALNO=T4.CONTRACTSERIALNO
                               AND T4.DATANO=P_DATA_DT_STR
                               INNER JOIN RWA_DEV.NCM_BUSINESS_HISTORY T31
                               ON T1.SERIALNO=T31.SERIALNO
                               AND T31.DATANO=P_DATA_DT_STR
                               AND T31.BALANCE>0
                                          /*rwa_dev.brd_loan_nor t31
                               ON   t1.serialno=t31.crdt_acct_no
                               AND  t31.datano = P_DATA_DT_STR
                               AND  t31.cur_bal > 0*/
                               WHERE T1.DATANO=P_DATA_DT_STR
                               --AND T1.BALANCE>0           --��Ч�ж�����
                               AND T1.SERIALNO<>'BD2014110400000001'
                               AND T1.BUSINESSTYPE NOT IN ('1040101010','1040101020','1040101030','1040102010','1040102020','1040102040','10303010'
                                   ,'1040105060','1040201010','1040201020','1040202010','105010','11105010','11105020','10302010','10302015','10302020')  --�ų�ͬҵ���ع���Ͷ�ʣ�ί�д���ҵ��,���ֺ�ת��Ҳ�ų�����Ʊ����Ϣ��Ϊ����
                             )
    ,TEMP_RELATIVE AS (SELECT DISTINCT T5.GUARANTYID,T1.CONTRACTNO
                       FROM TEMP_CMRELEVENCE1 T1
                      INNER JOIN (SELECT OBJECTNO, GUARANTYID
                                  FROM RWA_DEV.NCM_GUARANTY_RELATIVE
                                  WHERE DATANO=P_DATA_DT_STR
                                  AND OBJECTTYPE='PutOutApply'
                                  GROUP BY OBJECTNO, GUARANTYID
                                  ) T4
                      ON T1.BPSERIALNO=T4.OBJECTNO
                      INNER JOIN RWA_DEV.NCM_GUARANTY_INFO T5
                      ON T4.GUARANTYID=T5.GUARANTYID
                      AND T5.DATANO=P_DATA_DT_STR
                   )

     SELECT
                TO_DATE(P_DATA_DT_STR,'YYYYMMDD')                             AS  datadate                        --��������
                ,T1.DATANO                                                    AS  datano                          --������ˮ��
                ,T2.CONTRACTNO                                                AS  contractid                      --��ͬ����  (��������ͬ���ж�����ͬ�Ƿ���Ч)
                ,'YP'||T1.GUARANTYID                                          AS  mitigationid                    --���������(��֤���ݵĻ��ͺ�ͬ�͵���Ѻ�Ļ��ͺ�ͬ�ֿ�ȡ)
                ,'03'                                                         AS  mitigcategory                   --����������   ȫ�ǵ���ѺƷ
                ,''                                                           AS  sguarcontractid                 --Դ������ͬ����(�������)
                ,''                                                           AS  groupid                         --������
                ,'DZY|PUTOUT'
    FROM   RWA_DEV.NCM_GUARANTY_INFO T1
    INNER JOIN TEMP_RELATIVE T2
    ON T1.GUARANTYID=T2.GUARANTYID
    WHERE T1.DATANO=P_DATA_DT_STR
    --AND T1.CLRSTATUS='01'                  --modify by yushuangjiang
    --AND T1.CLRGNTSTATUS IN ('03','10')     --���ھ������Ŵ��¿�ȷ����������״̬�޶�����ȥ��
    AND T1.GUARANTYTYPEID NOT IN('004001004001','004001005001','004001006001','004001006002','001001003001')     --����֤����������֤,�����Ա������������Ա��� ����Ϊ��֤
    AND NOT EXISTS (SELECT 1 FROM RWA_DEV.RWA_XD_CMRELEVENCE T7 WHERE 'YP'||T2.GUARANTYID=T7.mitigationid AND T2.CONTRACTNO=T7.contractid AND T7.DATANO=P_DATA_DT_STR)
    and exists (select 1 from rwa_dev.rwa_xd_contract c where T2.CONTRACTNO = c.contractid AND C.DATANO=P_DATA_DT_STR) 
   ;
    COMMIT;
    
    /*1.5 ������Ч����º�ͬ��Ӧ�ĵ���ѺƷ����(Ʊ�����֣�ת����_��ת)*/
   /* INSERT INTO RWA_DEV.RWA_XD_CMRELEVENCE(
                 DATADATE                         --��������
                ,DATANO                           --������ˮ��
                ,CONTRACTID                       --��ͬ����
                ,MITIGATIONID                     --���������
                ,MITIGCATEGORY                    --����������
                ,SGUARCONTRACTID                  --Դ������ͬ����
                ,GROUPID                          --������
                ,flag
    )\*WITH TEMP_CMRELEVENCE1 AS(SELECT DISTINCT  T3.SERIALNO AS CONTRACTNO
                               FROM RWA_DEV.NCM_BUSINESS_DUEBILL T1
                               INNER JOIN RWA_DEV.NCM_BUSINESS_CONTRACT T3
                               ON T1.RELATIVESERIALNO2=T3.SERIALNO        --�����Խ��Ϊ׼�����Թ�����ͬʱ����Ӧ�üӺ�ͬ����Ч����
                               AND T1.DATANO=T3.DATANO
                               AND T3.DATANO=P_DATA_DT_STR
                               --AND T3.BUSINESSTYPE || T3.BUSINESSNATURE <> '1030301005'    --�ų��ⲿת����
                               INNER JOIN RWA_DEV.NCM_BUSINESS_HISTORY T31
                               ON T1.SERIALNO=T31.SERIALNO
                               AND T31.DATANO=P_DATA_DT_STR
                               AND T31.BALANCE>0
                                          \*rwa_dev.brd_loan_nor t31
                               ON    t1.serialno = t31.crdt_acct_no
                               AND    t31.datano = P_DATA_DT_STR
                               AND    t31.cur_bal > 0*\
                               WHERE T1.DATANO=P_DATA_DT_STR
                               --AND T1.BALANCE>0           --��Ч�ж�����
                               AND T1.BUSINESSTYPE IN ('10302010','10302015','10302020')  --���ֺ�ת������Ʊ����Ϣ��Ϊ����
                             )*\
     SELECT
                TO_DATE(P_DATA_DT_STR,'YYYYMMDD')                             AS  datadate                        --��������
                ,T1.DATANO                                                    AS  datano                          --������ˮ��
                ,T2.CONTRACTNO                                                AS  contractid                      --��ͬ����  (��������ͬ���ж�����ͬ�Ƿ���Ч)
                ,'PJ'||T1.SERIALNO                                            AS  mitigationid                    --���������(��֤���ݵĻ��ͺ�ͬ�͵���Ѻ�Ļ��ͺ�ͬ�ֿ�ȡ)
                ,'03'                                                         AS  mitigcategory                   --����������   ȫ�ǵ���ѺƷ
                ,''                                                           AS  sguarcontractid                 --Դ������ͬ����(�������)
                ,''                                                           AS  groupid                         --������
                ,'DZY|TXZT'
    FROM RWA_DEV.NCM_BILL_INFO T1
    INNER JOIN (SELECT DISTINCT  T3.SERIALNO AS CONTRACTNO
                               FROM RWA_DEV.NCM_BUSINESS_DUEBILL T1
                               INNER JOIN RWA_DEV.NCM_BUSINESS_CONTRACT T3
                               ON T1.RELATIVESERIALNO2=T3.SERIALNO        --�����Խ��Ϊ׼�����Թ�����ͬʱ����Ӧ�üӺ�ͬ����Ч����
                               AND T1.DATANO=T3.DATANO
                               AND T3.DATANO=P_DATA_DT_STR
                               --AND T3.BUSINESSTYPE || T3.BUSINESSNATURE <> '1030301005'    --�ų��ⲿת����
                               INNER JOIN RWA_DEV.NCM_BUSINESS_HISTORY T31
                               ON T1.SERIALNO=T31.SERIALNO
                               AND T31.DATANO=P_DATA_DT_STR
                               AND T31.BALANCE>0
                                          \*rwa_dev.brd_loan_nor t31
                               ON    t1.serialno = t31.crdt_acct_no
                               AND    t31.datano = P_DATA_DT_STR
                               AND    t31.cur_bal > 0*\
                               WHERE T1.DATANO=P_DATA_DT_STR
                               --AND T1.BALANCE>0           --��Ч�ж�����
                               AND T1.BUSINESSTYPE IN ('10302010','10302015','10302020')  --���ֺ�ת������Ʊ����Ϣ��Ϊ����
                           ) T2
    ON T1.OBJECTNO = T2.CONTRACTNO
    WHERE T1.DATANO=P_DATA_DT_STR
    AND T1.OBJECTTYPE='BusinessContract'
    and exists (select 1 from rwa_dev.rwa_xd_contract c where T2.CONTRACTNO = c.contractid and C.DATANO=P_DATA_DT_STR)
    ;
    COMMIT;*/
    
    /*2.1 ������Ч����º�ͬ�б�֤�������*/
    INSERT INTO RWA_DEV.RWA_XD_CMRELEVENCE(
                 DATADATE                         --��������
                ,DATANO                           --������ˮ��
                ,CONTRACTID                       --��ͬ����
                ,MITIGATIONID                     --���������
                ,MITIGCATEGORY                    --����������
                ,SGUARCONTRACTID                  --Դ������ͬ����
                ,GROUPID                          --������
                ,flag
    )WITH TEMP_CMRELEVENCE3 AS(SELECT DISTINCT  T1.RELATIVESERIALNO2 AS CONTRACTNO
                               FROM RWA_DEV.NCM_BUSINESS_DUEBILL T1
                               INNER JOIN RWA_DEV.NCM_BUSINESS_TYPE T2
                               ON T1.BUSINESSTYPE=T2.TYPENO
                               AND T1.DATANO=T2.DATANO
                               AND T2.SORTNO NOT LIKE '30%' --�ų������ҵ��
                               INNER JOIN RWA_DEV.NCM_BUSINESS_CONTRACT T3
                               ON T1.RELATIVESERIALNO2=T3.SERIALNO        --�����Խ��Ϊ׼�����Թ�����ͬʱ����Ӧ�üӺ�ͬ����Ч����
                               AND T1.DATANO=T3.DATANO
                               --AND T3.BUSINESSTYPE || T3.BUSINESSNATURE <> '1030301005'
                               INNER JOIN RWA_DEV.NCM_BUSINESS_HISTORY T31
                               ON T1.SERIALNO=T31.SERIALNO
                               AND T31.DATANO=P_DATA_DT_STR
                               AND T31.BALANCE>0
                                         /* rwa_dev.brd_loan_nor t31
                               ON    t1.serialno = t31.crdt_acct_no
                               AND   t31.datano = P_DATA_DT_STR
                               AND   t31.cur_bal > 0*/
                               WHERE T1.DATANO=P_DATA_DT_STR
                               --AND T1.BALANCE>0           --��Ч�ж�����
                               AND T1.SERIALNO<>'BD2014110400000001'
                               AND T1.BUSINESSTYPE NOT IN ('1040101010','1040101020','1040101030','1040102010','1040102020','1040102040','10303010'
                                   ,'1040105060','1040201010','1040201020','1040202010','105010','11105010','11105020','10302010','10302015','10302020')  --�ų�ͬҵ���ع���Ͷ�ʣ�ί�д���ҵ��,���ֺ�ת��Ҳ�ų�����Ʊ����Ϣ��Ϊ����
                             )
    SELECT
                TO_DATE(P_DATA_DT_STR,'YYYYMMDD')                            AS datadate                        --��������
                ,T1.DATANO                                                   AS datano                          --������ˮ��
                ,T1.SERIALNO                                                 AS contractid                      --��ͬ����  (��������ͬ���ж�����ͬ�Ƿ���Ч)
                ,'HT'|| T2.CONTRACTNO || T3.BAILCURRENCY                     AS mitigationid                    --���������(��֤���ݵĻ��ͺ�ͬ�͵���Ѻ�Ļ��ͺ�ͬ�ֿ�ȡ)
                ,'03'                                                        AS mitigcategory                   --����������   ��֤���ǽ�����ѺƷ
                ,T1.SERIALNO                                                 AS sguarcontractid                 --Դ������ͬ����(�������)
                ,''                                                          AS groupid                         --������
                ,'BZJ|PT'
    FROM  RWA_DEV.NCM_BUSINESS_CONTRACT T1
    INNER JOIN TEMP_CMRELEVENCE3 T2
    ON T1.SERIALNO=T2.CONTRACTNO
    INNER JOIN RWA_DEV.RWA_TEMP_BAIL1 T3
    ON T1.SERIALNO=T3.CONTRACTNO
    AND T3.BAILBALANCE<9999999999999    --�޳�������
    --AND T3.ISMAX='1'            --�����һ�ڵ��߼������������BAIL2����Ϊ����BAIL1����Ҫ�������־  modify by yushuangjiang
    WHERE T1.DATANO=P_DATA_DT_STR
    and exists (select 1 from rwa_dev.rwa_xd_contract c where T1.SERIALNO = c.contractid and C.DATANO=P_DATA_DT_STR)
    ;
    COMMIT;
    
    /*2.2 ������Ч����º�ͬ�б�֤������ݣ����ڴ���-΢������*/
    /*INSERT INTO RWA_DEV.RWA_XD_CMRELEVENCE(
                 DATADATE                         --��������
                ,DATANO                           --������ˮ��
                ,CONTRACTID                       --��ͬ����
                ,MITIGATIONID                     --���������
                ,MITIGCATEGORY                    --����������
                ,SGUARCONTRACTID                  --Դ������ͬ����
                ,GROUPID                          --������
                ,flag
    )WITH TEMP_CMRELEVENCE3 AS(SELECT DISTINCT  T1.RELATIVESERIALNO2 AS CONTRACTNO
                               FROM RWA_DEV.NCM_BUSINESS_DUEBILL T1
                               INNER JOIN RWA_DEV.NCM_BUSINESS_TYPE T2
                               ON T1.BUSINESSTYPE=T2.TYPENO
                               AND T1.DATANO=T2.DATANO
                               AND T2.SORTNO NOT LIKE '30%' --�ų������ҵ��
                               INNER JOIN RWA_DEV.NCM_BUSINESS_CONTRACT T3
                               ON T1.RELATIVESERIALNO2=T3.SERIALNO        --�����Խ��Ϊ׼�����Թ�����ͬʱ����Ӧ�üӺ�ͬ����Ч����
                               AND T1.DATANO=T3.DATANO
                               --AND T3.BUSINESSTYPE || T3.BUSINESSNATURE <> '1030301005'
                               INNER JOIN \*RWA_DEV.NCM_BUSINESS_HISTORY T31
                               ON T1.SERIALNO=T31.SERIALNO
                               AND T31.DATANO=P_DATA_DT_STR
                               AND (T31.OVERDUEBALANCE+T31.DULLBALANCE+T31.BADBALANCE)>0   --ȡ�����ڵļ�¼*\
                                          rwa_dev.brd_loan_nor t31
                               ON     t1.serialno = t31.crdt_acct_no
                               AND    t31.datano = P_DATA_DT_STR
                               AND    t31.sbjt_cd = '13100001' --����΢����
                               WHERE  T1.DATANO=P_DATA_DT_STR
                               --AND T1.BUSINESSTYPE='11103030'  --ֻȡ΢����ҵ��
                               )
    SELECT
                TO_DATE(P_DATA_DT_STR,'YYYYMMDD')                            AS datadate                        --��������
                ,T1.DATANO                                                   AS datano                          --������ˮ��
                ,T1.SERIALNO                                                 AS contractid                      --��ͬ����  (��������ͬ���ж�����ͬ�Ƿ���Ч)
                ,'HT' || T2.CONTRACTNO || T3.BAILCURRENCY                    AS mitigationid                    --���������(��֤���ݵĻ��ͺ�ͬ�͵���Ѻ�Ļ��ͺ�ͬ�ֿ�ȡ)
                ,'03'                                                        AS mitigcategory                   --����������   ��֤���ǽ�����ѺƷ
                ,T1.SERIALNO                                                 AS sguarcontractid                 --Դ������ͬ����(�������)
                ,''                                                          AS groupid                         --������
                ,'BZJ|YQWLD'
    FROM  RWA_DEV.NCM_BUSINESS_CONTRACT T1
    INNER JOIN TEMP_CMRELEVENCE3 T2
    ON T1.SERIALNO=T2.CONTRACTNO
    INNER JOIN RWA_DEV.RWA_TEMP_BAIL1 T3
    ON T1.SERIALNO=T3.CONTRACTNO
    --AND T3.ISMAX='1'       --�����һ�ڵ��߼������������BAIL2����Ϊ����BAIL1����Ҫ�������־  modify by yushuangjiang
    WHERE T1.DATANO=P_DATA_DT_STR
    AND NOT EXISTS(SELECT 1 FROM RWA_DEV.RWA_XD_CMRELEVENCE T3 WHERE 'HT'||T1.SERIALNO||T3.BAILCURRENCY=T3.MITIGATIONID AND T1.SERIALNO=T3.CONTRACTID AND T3.DATANO=P_DATA_DT_STR)
    and exists (select 1 from rwa_dev.rwa_xd_contract c where T1.SERIALNO = c.contractid and C.DATANO=P_DATA_DT_STR)
    ;
    COMMIT;*/
    
    /*2.3 ������Ч����º�ͬ�б�֤������ݣ����ڴ���-����ҵ��*/
    INSERT INTO RWA_DEV.RWA_XD_CMRELEVENCE(
                 DATADATE                         --��������
                ,DATANO                           --������ˮ��
                ,CONTRACTID                       --��ͬ����
                ,MITIGATIONID                     --���������
                ,MITIGCATEGORY                    --����������
                ,SGUARCONTRACTID                  --Դ������ͬ����
                ,GROUPID                          --������
                ,flag
    )WITH TEMP_CMRELEVENCE3 AS(
                               SELECT DISTINCT  T3.SERIALNO AS CONTRACTNO
                               FROM RWA_DEV.NCM_BUSINESS_DUEBILL T1
                               INNER JOIN RWA_DEV.NCM_BUSINESS_TYPE T2
                               ON T1.BUSINESSTYPE=T2.TYPENO
                               AND T1.DATANO=T2.DATANO
                               AND T2.SORTNO NOT LIKE '30%' --�ų������ҵ��
                               INNER JOIN RWA_DEV.NCM_BUSINESS_CONTRACT T3
                               ON T1.RELATIVESERIALNO2=T3.SERIALNO        --�����Խ��Ϊ׼�����Թ�����ͬʱ����Ӧ�üӺ�ͬ����Ч����
                               AND T1.DATANO=T3.DATANO
                               INNER JOIN BRD_LOAN_NOR T4 
                               ON  T4.DATANO = P_DATA_DT_STR
                               AND substr(T4.SBJT_CD,1,4) = '1310' --��Ŀ���
                               AND t4.sbjt_cd != '13100001' --���в���΢���������ڴ���
                               AND T4.CRDT_ACCT_NO = T1.SERIALNO
                                       WHERE  T1.DATANO=P_DATA_DT_STR
                               )
    SELECT
                TO_DATE(P_DATA_DT_STR,'YYYYMMDD')                            AS datadate                        --��������
                ,T1.DATANO                                                   AS datano                          --������ˮ��
                ,T1.SERIALNO                                                 AS contractid                      --��ͬ����  (��������ͬ���ж�����ͬ�Ƿ���Ч)
                ,'HT' || T2.CONTRACTNO || T3.BAILCURRENCY                    AS mitigationid                    --���������(��֤���ݵĻ��ͺ�ͬ�͵���Ѻ�Ļ��ͺ�ͬ�ֿ�ȡ)
                ,'03'                                                        AS mitigcategory                   --����������   ��֤���ǽ�����ѺƷ
                ,T1.SERIALNO                                                 AS sguarcontractid                 --Դ������ͬ����(�������)
                ,''                                                          AS groupid                         --������
                ,'BZJ|YQ'
    FROM  RWA_DEV.NCM_BUSINESS_CONTRACT T1
    INNER JOIN TEMP_CMRELEVENCE3 T2
    ON T1.SERIALNO=T2.CONTRACTNO
    INNER JOIN RWA_DEV.RWA_TEMP_BAIL1 T3
    ON T1.SERIALNO=T3.CONTRACTNO
    AND T3.BAILBALANCE<9999999999999    --�޳�������
    --AND T3.ISMAX='1'         --�����һ�ڵ��߼������������BAIL2����Ϊ����BAIL1����Ҫ�������־  modify by yushuangjiang
    WHERE T1.DATANO=P_DATA_DT_STR
    AND NOT EXISTS(SELECT 1 FROM RWA_DEV.RWA_XD_CMRELEVENCE T3 WHERE 'HT'||T1.SERIALNO||T3.BAILCURRENCY=T3.MITIGATIONID AND T1.SERIALNO=T3.CONTRACTID AND T3.DATANO=P_DATA_DT_STR)
    and exists (select 1 from rwa_dev.rwa_xd_contract c where T1.SERIALNO = c.contractid and C.DATANO=P_DATA_DT_STR)
    ;
    COMMIT;
    
    --3.1 ���뱣֤������Ϣ��-��ͨ��֤
    INSERT INTO RWA_DEV.RWA_XD_CMRELEVENCE(
                 DATADATE                         --��������
                ,DATANO                           --������ˮ��
                ,CONTRACTID                       --��ͬ����
                ,MITIGATIONID                     --���������
                ,MITIGCATEGORY                    --����������
                ,SGUARCONTRACTID                  --Դ������ͬ����
                ,GROUPID                          --������
                ,flag
    )WITH TEMP_CMRELEVENCE4 AS(SELECT DISTINCT  T1.RELATIVESERIALNO2 AS CONTRACTNO
                               FROM RWA_DEV.NCM_BUSINESS_DUEBILL T1
                               INNER JOIN RWA_DEV.NCM_BUSINESS_TYPE T2
                               ON T1.BUSINESSTYPE=T2.TYPENO
                               AND T1.DATANO=T2.DATANO
                               AND T2.SORTNO NOT LIKE '30%' --�ų������ҵ��
                               INNER JOIN RWA_DEV.NCM_BUSINESS_CONTRACT T3
                               ON T1.RELATIVESERIALNO2=T3.SERIALNO        --�����Խ��Ϊ׼�����Թ�����ͬʱ����Ӧ�üӺ�ͬ����Ч����
                               AND T1.DATANO=T3.DATANO
                               --AND T3.BUSINESSTYPE || T3.BUSINESSNATURE <> '1030301005'
                               INNER JOIN RWA_DEV.NCM_BUSINESS_HISTORY T31
                               ON T1.SERIALNO=T31.SERIALNO
                               AND T31.DATANO=P_DATA_DT_STR
                               AND T31.BALANCE>0
                                          /*rwa_dev.brd_loan_nor t31
                               ON    t1.serialno = t31.crdt_acct_no
                               AND   t31.datano = P_DATA_DT_STR
                               AND   t31.cur_bal > 0*/
                               WHERE T1.DATANO=P_DATA_DT_STR
                               --AND T1.BALANCE>0           --��Ч�ж�����
                               AND T1.SERIALNO<>'BD2014110400000001'
                               AND T1.BUSINESSTYPE NOT IN ('1040101010','1040101020','1040101030','1040102010','1040102020','1040102040','10303010','10201060','10202080','10201080','1020301010','1020301020'
                                   ,'1040105060','1040201010','1040201020','1040202010','105010','11105010','11105020','10302010','10302015','10302020')  --�ų�ͬҵ���ع���Ͷ�ʣ�ί�д���ҵ��,���ֺ�ת��Ҳ�ų�����Ʊ����Ϣ��Ϊ����
                             )
    ,TEMP_RELATIVE AS (SELECT DISTINCT T1.CONTRACTNO,T3.SERIALNO
                       FROM TEMP_CMRELEVENCE4 T1
                       INNER JOIN
                           (SELECT SERIALNO,OBJECTNO
                            FROM RWA_DEV.NCM_CONTRACT_RELATIVE
                            WHERE DATANO=P_DATA_DT_STR
                            AND OBJECTTYPE = 'GuarantyContract'
                            GROUP BY  SERIALNO,OBJECTNO
                            ) T2
                      ON T1.CONTRACTNO=T2.SERIALNO
                      INNER JOIN RWA_DEV.NCM_GUARANTY_CONTRACT T3
                      ON  T2.OBJECTNO = T3.SERIALNO
                      AND T3.DATANO=P_DATA_DT_STR
                      AND T3.GUARANTYTYPE='010'
                    )
    SELECT
                TO_DATE(P_DATA_DT_STR,'YYYYMMDD')                            AS datadate                        --��������
                ,T1.DATANO                                                   AS datano                          --������ˮ��
                ,T2.CONTRACTNO                                               AS contractid                      --��ͬ����  (��������ͬ���ж�����ͬ�Ƿ���Ч)
                ,'BZ'||T1.SERIALNO                                           AS mitigationid                    --���������(��֤���ݵĻ��ͺ�ͬ�͵���Ѻ�Ļ��ͺ�ͬ�ֿ�ȡ)
                ,'02'                                                        AS mitigcategory                   --����������   02-��֤
                ,T1.SERIALNO                                                 AS sguarcontractid                 --Դ������ͬ����(�������)
                ,''                                                          AS groupid                         --������
                ,'BZ|PT'
    FROM RWA_DEV.NCM_GUARANTY_CONTRACT T1
    INNER JOIN TEMP_RELATIVE T2
    ON T1.SERIALNO=T2.SERIALNO
    WHERE T1.DATANO=P_DATA_DT_STR
    AND T1.GUARANTYTYPE='010' --010��֤
    and exists (select 1 from rwa_dev.rwa_xd_contract c where T2.CONTRACTNO = c.contractid and C.DATANO=P_DATA_DT_STR);
    COMMIT;
    
    --3.2 ���뱣֤������Ϣ��-��֤�����ڴ���-΢������
    INSERT INTO RWA_DEV.RWA_XD_CMRELEVENCE(
                 DATADATE                         --��������
                ,DATANO                           --������ˮ��
                ,CONTRACTID                       --��ͬ����
                ,MITIGATIONID                     --���������
                ,MITIGCATEGORY                    --����������
                ,SGUARCONTRACTID                  --Դ������ͬ����
                ,GROUPID                          --������
                ,flag
    )WITH TEMP_CMRELEVENCE4 AS(SELECT DISTINCT  T1.RELATIVESERIALNO2 AS CONTRACTNO
                               FROM RWA_DEV.NCM_BUSINESS_DUEBILL T1
                               INNER JOIN RWA_DEV.NCM_BUSINESS_TYPE T2
                               ON T1.BUSINESSTYPE=T2.TYPENO
                               AND T1.DATANO=T2.DATANO
                               AND T2.SORTNO NOT LIKE '30%' --�ų������ҵ��
                               INNER JOIN RWA_DEV.NCM_BUSINESS_CONTRACT T3
                               ON T1.RELATIVESERIALNO2=T3.SERIALNO        --�����Խ��Ϊ׼�����Թ�����ͬʱ����Ӧ�üӺ�ͬ����Ч����
                               AND T1.DATANO=T3.DATANO
                               --AND T3.BUSINESSTYPE || T3.BUSINESSNATURE <> '1030301005'
                               INNER JOIN /*RWA_DEV.NCM_BUSINESS_HISTORY T31
                               ON T1.SERIALNO=T31.SERIALNO
                               AND T31.DATANO=P_DATA_DT_STR
                               AND (T31.OVERDUEBALANCE+T31.DULLBALANCE+T31.BADBALANCE)>0   --ȡ�����ڵļ�¼*/
                                          rwa_dev.brd_loan_nor t31
                               ON     t1.serialno = t31.crdt_acct_no
                               AND    t31.datano = P_DATA_DT_STR
                               AND    t31.sbjt_cd = '13100001'--����΢����
                               WHERE  T1.DATANO=P_DATA_DT_STR
                               --AND T1.BUSINESSTYPE='11103030'  --ֻȡ΢����ҵ��
                               )
    ,TEMP_RELATIVE AS (SELECT DISTINCT T1.CONTRACTNO,T3.SERIALNO
                       FROM TEMP_CMRELEVENCE4 T1
                       INNER JOIN
                           (SELECT SERIALNO,OBJECTNO
                            FROM RWA_DEV.NCM_CONTRACT_RELATIVE
                            WHERE DATANO=P_DATA_DT_STR
                            AND OBJECTTYPE = 'GuarantyContract'
                            GROUP BY  SERIALNO,OBJECTNO
                            ) T2
                      ON T1.CONTRACTNO=T2.SERIALNO
                      INNER JOIN RWA_DEV.NCM_GUARANTY_CONTRACT T3
                      ON  T2.OBJECTNO = T3.SERIALNO
                      AND T3.DATANO=P_DATA_DT_STR
                      AND T3.GUARANTYTYPE='010'
                    )
    SELECT
                TO_DATE(P_DATA_DT_STR,'YYYYMMDD')                            AS datadate                        --��������
                ,T1.DATANO                                                   AS datano                          --������ˮ��
                ,T2.CONTRACTNO                                               AS contractid                      --��ͬ����  (��������ͬ���ж�����ͬ�Ƿ���Ч)
                ,'BZ'||T1.SERIALNO                                           AS mitigationid                    --���������(��֤���ݵĻ��ͺ�ͬ�͵���Ѻ�Ļ��ͺ�ͬ�ֿ�ȡ)
                ,'02'                                                        AS mitigcategory                   --����������   02-��֤
                ,T1.SERIALNO                                                 AS sguarcontractid                 --Դ������ͬ����(�������)
                ,''                                                          AS groupid                         --������
                ,'BZ|YQWLD'
    FROM RWA_DEV.NCM_GUARANTY_CONTRACT T1
    INNER JOIN TEMP_RELATIVE T2
    ON T1.SERIALNO=T2.SERIALNO
    WHERE T1.DATANO=P_DATA_DT_STR
    AND T1.GUARANTYTYPE='010'
    AND NOT EXISTS(SELECT 1 FROM RWA_DEV.RWA_XD_CMRELEVENCE T3 WHERE T2.CONTRACTNO=T3.contractid AND 'BZ'||T2.SERIALNO=T3.mitigationid AND T3.DATANO=P_DATA_DT_STR)
    and exists (select 1 from rwa_dev.rwa_xd_contract c where T2.CONTRACTNO = c.contractid and C.DATANO=P_DATA_DT_STR)
    ;
    COMMIT;
    
    --3.3 ���뱣֤������Ϣ��-��֤�����ڴ���-����ҵ��
    INSERT INTO RWA_DEV.RWA_XD_CMRELEVENCE(
                 DATADATE                         --��������
                ,DATANO                           --������ˮ��
                ,CONTRACTID                       --��ͬ����
                ,MITIGATIONID                     --���������
                ,MITIGCATEGORY                    --����������
                ,SGUARCONTRACTID                  --Դ������ͬ����
                ,GROUPID                          --������
                ,flag
    )WITH TEMP_CMRELEVENCE4 AS(
                               SELECT DISTINCT  T3.SERIALNO AS CONTRACTNO
                               FROM RWA_DEV.NCM_BUSINESS_DUEBILL T1
                               INNER JOIN RWA_DEV.NCM_BUSINESS_TYPE T2
                               ON T1.BUSINESSTYPE=T2.TYPENO
                               AND T1.DATANO=T2.DATANO
                               AND T2.SORTNO NOT LIKE '30%' --�ų������ҵ��
                               INNER JOIN RWA_DEV.NCM_BUSINESS_CONTRACT T3
                               ON T1.RELATIVESERIALNO2=T3.SERIALNO        --�����Խ��Ϊ׼�����Թ�����ͬʱ����Ӧ�üӺ�ͬ����Ч����
                               AND T1.DATANO=T3.DATANO
                               INNER JOIN BRD_LOAN_NOR T4 
                               ON  T4.DATANO = P_DATA_DT_STR
                               AND T4.CRDT_ACCT_NO = T1.SERIALNO
                               AND substr(T4.SBJT_CD,1,4) = '1310' --��Ŀ���
                               AND T4.SBJT_CD != '13100001' --���в���΢���������ڴ���
                               WHERE  T1.DATANO=P_DATA_DT_STR
                               )
    ,TEMP_RELATIVE AS (SELECT DISTINCT T1.CONTRACTNO,T3.SERIALNO
                       FROM TEMP_CMRELEVENCE4 T1
                       INNER JOIN
                           (SELECT SERIALNO,OBJECTNO
                            FROM RWA_DEV.NCM_CONTRACT_RELATIVE
                            WHERE DATANO=P_DATA_DT_STR
                            AND OBJECTTYPE = 'GuarantyContract'
                            GROUP BY  SERIALNO,OBJECTNO
                            ) T2
                      ON T1.CONTRACTNO=T2.SERIALNO
                      INNER JOIN RWA_DEV.NCM_GUARANTY_CONTRACT T3
                      ON  T2.OBJECTNO = T3.SERIALNO
                      AND T3.DATANO=P_DATA_DT_STR
                      AND T3.GUARANTYTYPE='010'
                    )
    SELECT
                TO_DATE(P_DATA_DT_STR,'YYYYMMDD')                            AS datadate                        --��������
                ,T1.DATANO                                                   AS datano                          --������ˮ��
                ,T2.CONTRACTNO                                               AS contractid                      --��ͬ����  (��������ͬ���ж�����ͬ�Ƿ���Ч)
                ,'BZ'||T1.SERIALNO                                           AS mitigationid                    --���������(��֤���ݵĻ��ͺ�ͬ�͵���Ѻ�Ļ��ͺ�ͬ�ֿ�ȡ)
                ,'02'                                                        AS mitigcategory                   --����������   02-��֤
                ,T1.SERIALNO                                                 AS sguarcontractid                 --Դ������ͬ����(�������)
                ,''                                                          AS groupid                         --������
                ,'BZ|YQ'
    FROM RWA_DEV.NCM_GUARANTY_CONTRACT T1
    INNER JOIN TEMP_RELATIVE T2
    ON T1.SERIALNO=T2.SERIALNO
    WHERE T1.DATANO=P_DATA_DT_STR
    AND T1.GUARANTYTYPE='010'
    AND NOT EXISTS(SELECT 1 FROM RWA_DEV.RWA_XD_CMRELEVENCE T3 WHERE T2.CONTRACTNO=T3.contractid AND 'BZ'||T2.SERIALNO=T3.mitigationid AND T3.DATANO=P_DATA_DT_STR)
    and exists (select 1 from rwa_dev.rwa_xd_contract c where T2.CONTRACTNO = c.contractid AND  C.DATANO=P_DATA_DT_STR)
    ;
    COMMIT;
    
    --3.4 ���뱣֤������Ϣ��-׷�ӵ�PUTOUT���ϵı�֤
    INSERT INTO RWA_DEV.RWA_XD_CMRELEVENCE(
                 DATADATE                         --��������
                ,DATANO                           --������ˮ��
                ,CONTRACTID                       --��ͬ����
                ,MITIGATIONID                     --���������
                ,MITIGCATEGORY                    --����������
                ,SGUARCONTRACTID                  --Դ������ͬ����
                ,GROUPID                          --������
                ,flag
    )WITH TEMP_CMRELEVENCE4 AS(SELECT DISTINCT  T1.RELATIVESERIALNO2 AS CONTRACTNO,T3.PUTOUTDATE,T3.MATURITY,T2.ATTRIBUTE1,T4.SERIALNO AS BPSERLANO
                               FROM RWA_DEV.NCM_BUSINESS_DUEBILL T1
                               INNER JOIN RWA_DEV.NCM_BUSINESS_TYPE T2
                               ON T1.BUSINESSTYPE=T2.TYPENO
                               AND T1.DATANO=T2.DATANO
                               AND T2.SORTNO NOT LIKE '30%' --�ų������ҵ��
                               INNER JOIN RWA_DEV.NCM_BUSINESS_CONTRACT T3
                               ON T1.RELATIVESERIALNO2=T3.SERIALNO        --�����Խ��Ϊ׼�����Թ�����ͬʱ����Ӧ�üӺ�ͬ����Ч����
                               AND T1.DATANO=T3.DATANO
                               --AND T3.BUSINESSTYPE || T3.BUSINESSNATURE <> '1030301005'
                               INNER JOIN RWA_DEV.NCM_BUSINESS_PUTOUT T4
                               ON T3.SERIALNO=T4.CONTRACTSERIALNO
                               AND T4.DATANO=P_DATA_DT_STR
                               INNER JOIN RWA_DEV.NCM_BUSINESS_HISTORY T31
                               ON T1.SERIALNO=T31.SERIALNO
                               AND T31.DATANO=P_DATA_DT_STR
                               AND T31.BALANCE>0
                                          /*rwa_dev.brd_loan_nor t31
                               ON    t1.serialno = t31.crdt_acct_no
                               AND   t31.datano = P_DATA_DT_STR
                               AND   t31.cur_bal > 0*/
                               WHERE T1.DATANO=P_DATA_DT_STR
                               --AND T1.BALANCE>0           --��Ч�ж�����
                               AND T1.SERIALNO<>'BD2014110400000001'
                               AND T1.BUSINESSTYPE NOT IN ('1040101010','1040101020','1040101030','1040102010','1040102020','1040102040','10303010','10201060','10202080','10201080','1020301010','1020301020'
                                   ,'1040105060','1040201010','1040201020','1040202010','105010','11105010','11105020','10302010','10302015','10302020')  --�ų�ͬҵ���ع���Ͷ�ʣ�ί�д���ҵ��,���ֺ�ת��Ҳ�ų�����Ʊ����Ϣ��Ϊ����
                             )
    ,TEMP_RELATIVE AS (SELECT DISTINCT T1.CONTRACTNO,T3.SERIALNO
                       FROM TEMP_CMRELEVENCE4 T1
                       INNER JOIN (SELECT OBJECTNO, CONTRACTNO
                                  FROM RWA_DEV.NCM_GUARANTY_RELATIVE
                                  WHERE DATANO=P_DATA_DT_STR
                                  AND OBJECTTYPE='PutOutApply'
                                  GROUP BY OBJECTNO, CONTRACTNO
                                  ) T2
                      ON T1.BPSERLANO=T2.OBJECTNO
                      INNER JOIN RWA_DEV.NCM_GUARANTY_CONTRACT T3
                      ON T2.CONTRACTNO = T3.SERIALNO
                      AND T3.DATANO=P_DATA_DT_STR
                      AND T3.GUARANTYTYPE='010'
                    )
    SELECT
                TO_DATE(P_DATA_DT_STR,'YYYYMMDD')                            AS datadate                        --��������
                ,T1.DATANO                                                   AS datano                          --������ˮ��
                ,T2.CONTRACTNO                                               AS contractid                      --��ͬ����  (��������ͬ���ж�����ͬ�Ƿ���Ч)
                ,'BZ'||T1.SERIALNO                                                 AS mitigationid                    --���������(��֤���ݵĻ��ͺ�ͬ�͵���Ѻ�Ļ��ͺ�ͬ�ֿ�ȡ)
                ,'02'                                                        AS mitigcategory                   --����������   02-��֤
                ,T1.SERIALNO                                                 AS sguarcontractid                 --Դ������ͬ����(�������)
                ,''                                                          AS groupid                         --������
                ,'BZ|PUTOUT'
    FROM RWA_DEV.NCM_GUARANTY_CONTRACT T1
    INNER JOIN TEMP_RELATIVE T2
    ON T1.SERIALNO=T2.SERIALNO
    WHERE T1.DATANO=P_DATA_DT_STR
    AND T1.GUARANTYTYPE='010'
    AND NOT EXISTS (SELECT 1 FROM RWA_DEV.RWA_XD_CMRELEVENCE T3 WHERE T2.CONTRACTNO=T3.contractid AND 'BZ'||T2.SERIALNO=T3.mitigationid AND T3.DATANO=P_DATA_DT_STR)
    and exists (select 1 from rwa_dev.rwa_xd_contract c where T2.CONTRACTNO = c.contractid and C.DATANO=P_DATA_DT_STR)
    ;
    COMMIT;
    
    --3.5 ���뱣֤������Ϣ�� ����Ѻ�㡢����Ѻ�㡢����͢���ж��в�Ϊ��-��ҵ����Ϣ��Ϊ������ͬ��Ϣ��������ʽ�Ǳ�֤��
    INSERT INTO RWA_DEV.RWA_XD_CMRELEVENCE(
                 DATADATE                         --��������
                ,DATANO                           --������ˮ��
                ,CONTRACTID                       --��ͬ����
                ,MITIGATIONID                     --���������
                ,MITIGCATEGORY                    --����������
                ,SGUARCONTRACTID                  --Դ������ͬ����
                ,GROUPID                          --������
                ,flag
    )WITH TEMP_CMRELEVENCE4 AS(SELECT DISTINCT  T1.RELATIVESERIALNO2 AS CONTRACTNO
                               FROM RWA_DEV.NCM_BUSINESS_DUEBILL T1
                               INNER JOIN RWA_DEV.NCM_BUSINESS_CONTRACT T3
                               ON T1.RELATIVESERIALNO2=T3.SERIALNO        --�����Խ��Ϊ׼�����Թ�����ͬʱ����Ӧ�üӺ�ͬ����Ч����
                               AND T1.DATANO=T3.DATANO
                               INNER JOIN RWA_DEV.NCM_BUSINESS_HISTORY T31
                               ON T1.SERIALNO=T31.SERIALNO
                               AND T31.DATANO=P_DATA_DT_STR
                               AND T31.BALANCE>0
                                         /* rwa_dev.brd_loan_nor t31
                               ON    t1.serialno = t31.crdt_acct_no
                               AND   t31.datano = P_DATA_DT_STR
                               AND   t31.cur_bal > 0*/
                               WHERE T1.DATANO=P_DATA_DT_STR
                               --AND T1.BALANCE>0           --��Ч�ж�����
                               --AND T1.SERIALNO NOT LIKE 'BD%'
                               AND T1.BUSINESSTYPE
                                   IN ('10201060','10202080','10201080') --����Ѻ�㡢����Ѻ�㡢����͢
                             )
    SELECT
                TO_DATE(P_DATA_DT_STR,'YYYYMMDD')                            AS datadate                        --��������
                ,T1.DATANO                                                   AS datano                          --������ˮ��
                ,T2.CONTRACTNO                                               AS contractid                      --��ͬ����  (��������ͬ���ж�����ͬ�Ƿ���Ч)
                ,'BZ'||T1.SERIALNO                                           AS mitigationid                    --���������(��֤���ݵĻ��ͺ�ͬ�͵���Ѻ�Ļ��ͺ�ͬ�ֿ�ȡ)
                ,'02'                                                        AS mitigcategory                   --����������   02-��֤
                ,T1.SERIALNO                                                 AS sguarcontractid                 --Դ������ͬ����(�������)
                ,''                                                          AS groupid                         --������
                ,'BZ|YH'
    FROM RWA_DEV.NCM_BUSINESS_CONTRACT T1
    INNER JOIN TEMP_CMRELEVENCE4 T2
    ON T1.SERIALNO=T2.CONTRACTNO
    INNER JOIN RWA_DEV.NCM_BUSINESS_PUTOUT T3
    ON T1.SERIALNO=T3.CONTRACTSERIALNO
    AND T1.DATANO=T3.DATANO
    AND T3.ACCEPTORBANKID IS NOT NULL          --�ж��в�Ϊ�ղ�������֤ 20190805 ACCEPTORBANKID��Ϊ�յ�ֻ��5����¼
    WHERE T1.DATANO=P_DATA_DT_STR
    and exists (select 1 from rwa_dev.rwa_xd_contract c where T2.CONTRACTNO = c.contractid AND C.DATANO=P_DATA_DT_STR)
    ;
    COMMIT;
      
    --3.6 ���뱣֤������Ϣ�� -��׷��Ȩ����������׷��Ȩ�������������̲�Ϊ��-��ҵ����Ϣ��Ϊ������ͬ��Ϣ��������ʽ�Ǳ�֤��
    INSERT INTO RWA_DEV.RWA_XD_CMRELEVENCE(
                 DATADATE                         --��������
                ,DATANO                           --������ˮ��
                ,CONTRACTID                       --��ͬ����
                ,MITIGATIONID                     --���������
                ,MITIGCATEGORY                    --����������
                ,SGUARCONTRACTID                  --Դ������ͬ����
                ,GROUPID                          --������
                ,flag
    )WITH TEMP_CMRELEVENCE4 AS(SELECT DISTINCT  T1.RELATIVESERIALNO2 AS CONTRACTNO
                               FROM RWA_DEV.NCM_BUSINESS_DUEBILL T1
                               INNER JOIN RWA_DEV.NCM_BUSINESS_CONTRACT T3
                               ON T1.RELATIVESERIALNO2=T3.SERIALNO        --�����Խ��Ϊ׼�����Թ�����ͬʱ����Ӧ�üӺ�ͬ����Ч����
                               AND T1.DATANO=T3.DATANO
                               INNER JOIN RWA_DEV.NCM_BUSINESS_HISTORY T31
                               ON T1.SERIALNO=T31.SERIALNO
                               AND T31.DATANO=P_DATA_DT_STR
                               AND T31.BALANCE>0
                                /*          rwa_dev.brd_loan_nor t31
                               ON    t1.serialno = t31.crdt_acct_no
                               AND   t31.datano = P_DATA_DT_STR
                               AND   t31.cur_bal > 0*/
                               WHERE T1.DATANO=P_DATA_DT_STR
                               --AND T1.BALANCE>0           --��Ч�ж�����
                               --AND T1.SERIALNO NOT LIKE 'BD%'
                               AND T1.BUSINESSTYPE
                                   IN ('1020301010','1020301020'�� --��׷��Ȩ����������׷��Ȩ��������
                             )
    SELECT
                TO_DATE(P_DATA_DT_STR,'YYYYMMDD')                            AS datadate                        --��������
                ,T1.DATANO                                                   AS datano                          --������ˮ��
                ,T2.CONTRACTNO                                               AS contractid                      --��ͬ����  (��������ͬ���ж�����ͬ�Ƿ���Ч)
                ,'BZ'||T1.SERIALNO                                           AS mitigationid                    --���������(��֤���ݵĻ��ͺ�ͬ�͵���Ѻ�Ļ��ͺ�ͬ�ֿ�ȡ)
                ,'02'                                                        AS mitigcategory                   --����������   02-��֤
                ,T1.SERIALNO                                                 AS sguarcontractid                 --Դ������ͬ����(�������)
                ,''                                                          AS groupid                         --������
                ,'BZ|BL'
    FROM RWA_DEV.NCM_BUSINESS_CONTRACT T1
    INNER JOIN TEMP_CMRELEVENCE4 T2
    ON T1.SERIALNO=T2.CONTRACTNO
    INNER JOIN RWA_DEV.NCM_BUSINESS_PUTOUT T3
    ON T1.SERIALNO=T3.CONTRACTSERIALNO
    AND T1.DATANO=T3.DATANO
    AND T3.FACTORID IS NOT NULL               --�����̲�Ϊ����Ϊ��֤ 20190805FACTORID��Ϊ�յļ�¼Ϊ0
    WHERE T1.DATANO=P_DATA_DT_STR
    and exists (select 1 from rwa_dev.rwa_xd_contract c where T2.CONTRACTNO = c.contractid AND C.DATANO=P_DATA_DT_STR)
    ;
    COMMIT;
    
    --3.7 ����ѺƷ����Ϊ����֤����������֤����Ϊ��֤
    INSERT INTO RWA_DEV.RWA_XD_CMRELEVENCE(
                 DATADATE                         --��������
                ,DATANO                           --������ˮ��
                ,CONTRACTID                       --��ͬ����
                ,MITIGATIONID                     --���������
                ,MITIGCATEGORY                    --����������
                ,SGUARCONTRACTID                  --Դ������ͬ����
                ,GROUPID                          --������
                ,flag
    )WITH TEMP_CMRELEVENCE1 AS(SELECT DISTINCT  T3.SERIALNO AS CONTRACTNO
                               FROM RWA_DEV.NCM_BUSINESS_DUEBILL T1
                               INNER JOIN RWA_DEV.NCM_BUSINESS_TYPE T2
                               ON T1.BUSINESSTYPE=T2.TYPENO
                               AND T1.DATANO=T2.DATANO
                               AND T2.SORTNO NOT LIKE '30%' --�ų������ҵ��
                               INNER JOIN RWA_DEV.NCM_BUSINESS_CONTRACT T3
                               ON T1.RELATIVESERIALNO2=T3.SERIALNO        --�����Խ��Ϊ׼�����Թ�����ͬʱ����Ӧ�üӺ�ͬ����Ч����
                               AND T1.DATANO=T3.DATANO
                               --AND T3.BUSINESSTYPE || T3.BUSINESSNATURE <> '1030301005'
                               INNER JOIN RWA_DEV.NCM_BUSINESS_HISTORY T31
                               ON T1.SERIALNO=T31.SERIALNO
                               AND T31.DATANO=P_DATA_DT_STR
                               AND T31.BALANCE>0
                                          /*rwa_dev.brd_loan_nor t31
                               ON    t1.serialno = t31.crdt_acct_no
                               AND   t31.datano = P_DATA_DT_STR
                               AND   t31.cur_bal > 0       */
                               WHERE T1.DATANO=P_DATA_DT_STR
                               --AND T1.BALANCE>0           --��Ч�ж�����
                               AND T1.SERIALNO<>'BD2014110400000001'
                               AND T1.BUSINESSTYPE NOT IN ('1040101010','1040101020','1040101030','1040102010','1040102020','1040102040','10303010'
                                   ,'1040105060','1040201010','1040201020','1040202010','105010','11105010','11105020','10302010','10302015','10302020')  --�ų�ͬҵ���ع���Ͷ�ʣ�ί�д���ҵ��,���ֺ�ת��Ҳ�ų�����Ʊ����Ϣ��Ϊ����
                             )
    ,TEMP_RELATIVE AS (SELECT DISTINCT T5.GUARANTYID,T1.CONTRACTNO
                       FROM TEMP_CMRELEVENCE1 T1
                       INNER JOIN
                           (SELECT SERIALNO,OBJECTNO
                            FROM RWA_DEV.NCM_CONTRACT_RELATIVE
                            WHERE DATANO=P_DATA_DT_STR
                            AND OBJECTTYPE = 'GuarantyContract'
                            GROUP BY  SERIALNO,OBJECTNO
                            ) T2
                      ON T1.CONTRACTNO=T2.SERIALNO
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
                TO_DATE(P_DATA_DT_STR,'YYYYMMDD')                             AS  datadate                        --��������
                ,T1.DATANO                                                    AS  datano                          --������ˮ��
                ,T2.CONTRACTNO                                                AS  contractid                      --��ͬ����  (��������ͬ���ж�����ͬ�Ƿ���Ч)
                ,'BZ'||T1.GUARANTYID                                          AS  mitigationid                    --���������(��֤���ݵĻ��ͺ�ͬ�͵���Ѻ�Ļ��ͺ�ͬ�ֿ�ȡ)
                ,'02'                                                         AS  mitigcategory                   --����������   ȫ�ǵ���ѺƷ
                ,''                                                           AS  sguarcontractid                 --Դ������ͬ����(�������)
                ,''                                                           AS  groupid                         --������
                ,'BZ|XYZ'
    FROM   RWA_DEV.NCM_GUARANTY_INFO T1
    INNER JOIN TEMP_RELATIVE T2
    ON T1.GUARANTYID=T2.GUARANTYID
    WHERE T1.DATANO=P_DATA_DT_STR
    --AND T1.CLRSTATUS='01'                  --modify by yushuangjiang
    --AND T1.CLRGNTSTATUS IN ('03','10')     --���ھ������Ŵ��¿�ȷ����������״̬�޶�����ȥ��
    AND T1.GUARANTYTYPEID IN('004001004001','004001005001','004001006001','004001006002')     --����֤����������֤,�����Ա������������Ա��� ����Ϊ��֤
    and exists (select 1 from rwa_dev.rwa_xd_contract c where T2.CONTRACTNO = c.contractid AND C.DATANO=P_DATA_DT_STR)
    ;
    COMMIT;
    
    
    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_XD_CMRELEVENCE',cascade => true);

    /*Ŀ�������ͳ��*/
    SELECT COUNT(1) INTO v_count1 FROM RWA_DEV.RWA_XD_CMRELEVENCE;
    --Dbms_output.Put_line('RWA_XD_CMRELEVENCE��ǰ��������ݼ�¼Ϊ:' || (v_count3-v_count2) || '��');
		--Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̽��� ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

		P_PO_RTNCODE := '1';
	  P_PO_RTNMSG  := '�ɹ�'||'-'||v_count1;
		--�����쳣
		EXCEPTION
    WHEN OTHERS THEN
				 --Dbms_output.Put_line('������,�������Ϊ:'||sqlcode||';������ϢΪ:'||sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace);
       	 ROLLBACK;
   			 P_PO_RTNCODE := sqlcode;
   			 P_PO_RTNMSG  := '�Ŵ�ϵͳ-��ͬ�뻺�������(pro_rwa_xd_cmrelevence)ETLת��ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_XD_CMRELEVENCE;
/

