CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_TMP_DEAL(
			 											p_data_dt_str	IN	VARCHAR2,		--�������� yyyyMMdd
       											p_po_rtncode	OUT	VARCHAR2,		--���ر�� 1 �ɹ�,0 ʧ��
														p_po_rtnmsg		OUT	VARCHAR2		--��������
				)
  /*
    �洢��������:RWA_DEV.PRO_RWA_TMP_DEAL
    ʵ�ֹ���:RWAϵͳ-��ʱ������׼��
    ���ݿھ�:ȫ��
    ����Ƶ��:�³�����
    ��  ��  :V1.0.0
    ��д��  :LISY
    ��дʱ��:2017-07-31
    ��  λ  :�Ϻ���˶��Ϣ�����ɷ����޹�˾
    Դ  ��1 :RWA_DEV.CBS_LNU|���ֿ�Ƭ��
    Ŀ���1 :RWA_DEV.TMP_CURRENCY_CHANGE|����ת����ʱ��
    �����¼(�޸���|�޸�ʱ��|�޸�����):
    */
  AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_TMP_DEAL';
  --�����쳣����
  v_raise EXCEPTION;
  --���嵱ǰ����ļ�¼��
  v_count INTEGER;

  BEGIN
    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*�����ȫ�����ݼ��������Ŀ���*/
    --1. ���Ŀ����е�ԭ�м�¼
    --1.1 ��ձ���ת����ʱ��������
    DELETE FROM RWA_DEV.TMP_CURRENCY_CHANGE WHERE DATANO = p_data_dt_str;

    COMMIT;

    --2.���������������ݴ�Դ����뵽Ŀ�����
    --2.1 ����ת����ʱ��
    INSERT INTO RWA_DEV.TMP_CURRENCY_CHANGE(
                DATANO                               	 --�����ڴ�
                ,CURRENCYCODE                          --����
                ,MIDDLEPRICE                           --�м��
    )
		SELECT
								p_data_dt_str                            		 AS DATANO           		 --�ڴ�
                ,T1.CCY																			 AS CURRENCYCODE         --����
                ,MAX(T1.JZRAT)                         			 AS MIDDLEPRICE          --��׼����Ϊ�м��
		FROM 				RWA_DEV.NNS_JT_EXRATE T1	             		 	--������Ϣ��
		WHERE 			T1.DATANO = p_data_dt_str
		GROUP BY		T1.CCY
		;

    COMMIT;

    --�������Ϣ
    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'TMP_CURRENCY_CHANGE',cascade => true);

    --2.2 ���¼��ſͻ���Ϣ��ʱ��
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.TMP_GROUP_MEMBERS';

    INSERT INTO RWA_DEV.TMP_GROUP_MEMBERS(GROUPID,MEMBERID)
    SELECT T2.GROUPID AS GROUPID,  --���ű��
           T2.MEMBERCUSTOMERID AS MEMBERID  --���ų�Ա���
		  FROM RWA_DEV.NCM_GROUP_INFO T1  --���ſͻ�������Ϣ��
		 INNER JOIN (SELECT TT.GROUPID,TT.MEMBERCUSTOMERID,MAX(TT.VERSIONSEQ) AS VERSIONSEQ  --ȡ��������µ�һ��
                 FROM RWA_DEV.NCM_GROUP_FAMILY_MEMBER TT
                 WHERE DATANO = p_data_dt_str
                 GROUP BY TT.GROUPID,TT.MEMBERCUSTOMERID ) T2  --���ż��׳�Ա
		    ON T1.GROUPID = T2.GROUPID
		   AND T1.REFVERSIONSEQ = T2.VERSIONSEQ
		 WHERE T1.STATUS = '1'
		   AND T1.FAMILYMAPSTATUS = '2'
		   AND T1.DATANO = p_data_dt_str
		UNION
		SELECT T2.GROUPID AS GROUPID, --���ű��
           T2.GROUPID AS MEMBERID --���ű��
		  FROM RWA_DEV.NCM_GROUP_INFO T1
		 INNER JOIN RWA_DEV.NCM_GROUP_FAMILY_MEMBER T2
		    ON T1.GROUPID = T2.GROUPID
		   AND T1.REFVERSIONSEQ = T2.VERSIONSEQ
		   AND T2.DATANO = p_data_dt_str
		 WHERE T1.STATUS = '1'
		   AND T1.FAMILYMAPSTATUS = '2'
		   AND T1.DATANO = p_data_dt_str
		UNION
		SELECT T2.GROUPID AS GROUPID, 
           T2.MEMBERCUSTOMERID AS MEMBERID
		  FROM RWA_DEV.NCM_GROUP_INFO T1
		 INNER JOIN (SELECT TT.GROUPID,TT.MEMBERCUSTOMERID,MAX(TT.VERSIONSEQ) AS VERSIONSEQ  --ȡ��������µ�һ��
                 FROM RWA_DEV.NCM_GROUP_FAMILY_MEMBER TT
                 WHERE DATANO = p_data_dt_str
                 GROUP BY TT.GROUPID,TT.MEMBERCUSTOMERID ) T2
		    ON T1.GROUPID = T2.GROUPID
		   AND T1.CURRENTVERSIONSEQ = T2.VERSIONSEQ
		 INNER JOIN RWA_DEV.NCM_GROUP_FAMILY_OPINION T3
		    ON T1.GROUPID = T3.GROUPID
		   AND T3.APPROVETYPE = '2'
		   AND T3.FAMILYSEQ = T1.CURRENTVERSIONSEQ
		   AND T3.DATANO = p_data_dt_str
		 WHERE T1.STATUS = '1'
		   AND T1.FAMILYMAPSTATUS IN ('0', '1')  --���װ汾״̬
		   AND T1.DATANO = p_data_dt_str
		UNION
		SELECT T2.GROUPID AS GROUPID, 
           T2.GROUPID AS MEMBERID
		  FROM RWA_DEV.NCM_GROUP_INFO T1
		 INNER JOIN RWA_DEV.NCM_GROUP_FAMILY_MEMBER T2
		    ON T1.GROUPID = T2.GROUPID
		   AND T1.CURRENTVERSIONSEQ = T2.VERSIONSEQ
		   AND T2.DATANO = p_data_dt_str
		 INNER JOIN RWA_DEV.NCM_GROUP_FAMILY_OPINION T3
		    ON T1.GROUPID = T3.GROUPID
		   AND T3.APPROVETYPE = '2'
		   AND T3.FAMILYSEQ = T1.CURRENTVERSIONSEQ
		   AND T3.DATANO = p_data_dt_str
		 WHERE T1.STATUS = '1'
		   AND T1.FAMILYMAPSTATUS IN ('0', '1') --���װ汾״̬
		   AND T1.DATANO = p_data_dt_str
		;

		COMMIT;

		--�������Ϣ
    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'TMP_GROUP_MEMBERS',cascade => true);

    --------------------------------------------------------��ͬ�±�֤����ϸ�߼�----------------------------------------------------
    --1.���Ŀ����е�ԭ�м�¼
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_TEMP_BAIL1';
    
    /*RWA���ڣ��޸ı�֤���߼�������С���ο�ȷ�ϣ��Ŵ���֤�𶼴Ӻ��ı�֤���˻����ȡ*/
    --modify by yushuangjiang
    --���ı�֤���˻�����Ը���ݱ��ݺŹ����������˺Ź�������ͬ�Ź���������˳�����½�ݺţ�������ˮ�ţ���ͬ��
    --1.���Ȳ����ݺſ��Թ����ı�֤����Ϣ
    INSERT INTO RWA_DEV.RWA_TEMP_BAIL1(
                CONTRACTNO
                ,BAILCURRENCY
                ,ISSUM
                ,BAILBALANCE
    )
    SELECT      T1.RELATIVESERIALNO2 AS  CONTRACTNO
                ,T2.BAILCURRENCY AS BAILCURRENCY
                ,'0'
                ,SUM(T2.BAILBALANCE) AS BAILBALANCE
    FROM RWA_DEV.NCM_BUSINESS_HISTORY T1
    INNER JOIN NCM_BAILACCOUNTINFO_TMP T2
    ON T1.Serialno=T2.RELATIVECONTRACTNO   --��ݺŹ���
    AND T2.DATANO=p_data_dt_str
    AND T2.BAILBALANCE IS NOT NULL
    AND T2.BAILBALANCE>0
    WHERE T1.BALANCE>0
    AND T1.DATANO=p_data_dt_str
    AND T1.BUSINESSTYPE NOT IN ('1040101010','1040101020','1040101030','1040102010','1040102020','1040102040','1040105060','1040201010','1040201020','1040202010','105010','10303010'
                                ,'108010','10201080','10202091','11105010','11105020','11103030'
                                ,'10302020','10302030' --�ų��������Ŵ����һ������Ȼ��֤�������
                                )
    GROUP BY T1.RELATIVESERIALNO2,T2.BAILCURRENCY
    ;
    COMMIT; 
    
    --2.�ٲ��������ˮ�ſ��Թ����ı�֤����Ϣ
    INSERT INTO RWA_DEV.RWA_TEMP_BAIL1(
                CONTRACTNO
                ,BAILCURRENCY
                ,ISSUM
                ,BAILBALANCE
    )
    SELECT      T1.RELATIVESERIALNO2 AS  CONTRACTNO
                ,T2.BAILCURRENCY AS BAILCURRENCY
                ,'0'
                ,SUM(T2.BAILBALANCE) AS BAILBALANCE
    FROM RWA_DEV.NCM_BUSINESS_HISTORY T1
    INNER JOIN NCM_BAILACCOUNTINFO_TMP T2
    ON T1.RELATIVESERIALNO1=T2.RELATIVECONTRACTNO   --������ˮ�Ź���
    AND T2.DATANO=p_data_dt_str
    AND T2.BAILBALANCE IS NOT NULL
    AND T2.BAILBALANCE>0
    WHERE T1.BALANCE>0
    AND T1.DATANO=p_data_dt_str
    AND NOT EXISTS(SELECT 1 FROM RWA_DEV.RWA_TEMP_BAIL1 T3 WHERE T1.RELATIVESERIALNO2=T3.CONTRACTNO) --�ų���ͬ���Ѿ�����ʱ���д��ڵļ�¼��˵����ݺ��Ѿ�������
    AND T1.BUSINESSTYPE NOT IN ('1040101010','1040101020','1040101030','1040102010','1040102020','1040102040','1040105060','1040201010','1040201020','1040202010','105010','10303010'
                                ,'108010','10201080','10202091','11105010','11105020','11103030'
                                ,'10302020','10302030' --�ų��������Ŵ����һ������Ȼ��֤�������
                                )
    GROUP BY T1.RELATIVESERIALNO2,T2.BAILCURRENCY
    ;
    COMMIT; 
    
    --3.�������ͬ�ſ��Թ����ı�֤����Ϣ
    INSERT INTO RWA_DEV.RWA_TEMP_BAIL1(
                CONTRACTNO
                ,BAILCURRENCY
                ,ISSUM
                ,BAILBALANCE
    )
    SELECT      T1.RELATIVESERIALNO2 AS  CONTRACTNO
                ,T2.BAILCURRENCY AS BAILCURRENCY
                ,'0'
                ,SUM(T2.BAILBALANCE) AS BAILBALANCE
    FROM RWA_DEV.NCM_BUSINESS_HISTORY T1
    INNER JOIN NCM_BAILACCOUNTINFO_TMP T2
    ON T1.RELATIVESERIALNO2=T2.RELATIVECONTRACTNO   --��ͬ�Ź���
    AND T2.DATANO=p_data_dt_str
    AND T2.BAILBALANCE IS NOT NULL  --��֤����Ϊ�յ��ų���
    AND T2.BAILBALANCE>0
    WHERE T1.BALANCE>0
    AND T1.DATANO=p_data_dt_str
    AND NOT EXISTS(SELECT 1 FROM RWA_DEV.RWA_TEMP_BAIL1 T3 WHERE T1.RELATIVESERIALNO2=T3.CONTRACTNO) --�ų���ͬ���Ѿ�����ʱ���д��ڵļ�¼��˵��ǰ���Ѿ�������
    AND T1.BUSINESSTYPE NOT IN ('1040101010','1040101020','1040101030','1040102010','1040102020','1040102040','1040105060','1040201010','1040201020','1040202010','105010','10303010'
                                ,'108010','10201080','10202091','11105010','11105020','11103030'
                                ,'10302020','10302030' --�ų��������Ŵ����һ������Ȼ��֤�������
                                )
    GROUP BY T1.RELATIVESERIALNO2,T2.BAILCURRENCY
    ;
    COMMIT; 
    
    /*--������ķ��صı�֤������ - ��Ȳ㣬ע�Ͷ���һ���߼�������
    INSERT INTO RWA_DEV.RWA_TEMP_BAIL1(
                CONTRACTNO
                ,BAILCURRENCY
                ,ISSUM
                ,BAILBALANCE
    )
    SELECT
                T3.SERIALNO
                ,NVL(T1.BAILCURRENCY,T3.BUSINESSCURRENCY)
                ,'0'
                ,SUM(nvl(T1.BailBalance,0))
    FROM RWA_DEV.NCM_BAILACCOUNTINFO_TMP T1
    INNER JOIN RWA_DEV.NCM_CL_OCCUPY T2 --��ȱ�
    ON T1.RELATIVECONTRACTNO=T2.OBJECTNO
    AND T2.OBJECTTYPE='BusinessContract'
    AND T2.DATANO=p_data_dt_str
    INNER JOIN RWA_DEV.NCM_Business_Contract T3
    ON T2.RELATIVESERIALNO=T3.SERIALNO
    AND T3.DATANO=p_data_dt_str 
    AND T3.businesstype not in('11103030', '11103035', '11103036') --��ȡ ��E��,��I��,΢����
    AND NVL(T3.LINETYPE,'0010')<>'0040'
    INNER JOIN (SELECT DISTINCT relativeserialno2
                FROM RWA_DEV.NCM_BUSINESS_HISTORY WHERE DATANO=p_data_dt_str AND BALANCE>0 AND INPUTDATE=p_data_dt_str
                ) T4
    ON T1.RELATIVECONTRACTNO=T4.relativeserialno2
    WHERE T1.DATANO=p_data_dt_str
    GROUP BY T3.SERIALNO,NVL(T1.BAILCURRENCY,T3.BUSINESSCURRENCY),'0'
    ;
    COMMIT;

    --������ķ��صı�֤������ - ��ͬ��
    INSERT INTO RWA_DEV.RWA_TEMP_BAIL1(
                CONTRACTNO
                ,BAILCURRENCY
                ,ISSUM
                ,BAILBALANCE
    )
    SELECT
                T2.SERIALNO
                ,NVL(T1.BAILCURRENCY,T2.BUSINESSCURRENCY)
                ,'0'
                ,SUM(nvl(T1.BailBalance,0))
    FROM RWA_DEV.NCM_BAILACCOUNTINFO_TMP T1
    INNER JOIN RWA_DEV.NCM_Business_Contract T2
    ON T1.RELATIVECONTRACTNO=T2.SERIALNO
    AND T2.DATANO=p_data_dt_str
    AND T2.businesstype not in('11103030', '11103035', '11103036') --��ȡ ��E��,��I��,΢����
    AND NVL(T2.LINETYPE,'0010')<>'0040'
    INNER JOIN (SELECT DISTINCT relativeserialno2
                FROM RWA_DEV.NCM_BUSINESS_HISTORY WHERE DATANO=p_data_dt_str AND BALANCE>0 AND INPUTDATE=p_data_dt_str
                ) T4
    ON T2.SERIALNO=T4.relativeserialno2
    WHERE T1.DATANO=p_data_dt_str
    GROUP BY T2.SERIALNO,NVL(T1.BAILCURRENCY,T2.BUSINESSCURRENCY),'0'
    ;
    COMMIT;

    --������ķ��صı�֤������ - ���˲�
    INSERT INTO RWA_DEV.RWA_TEMP_BAIL1(
                CONTRACTNO
                ,BAILCURRENCY
                ,ISSUM
                ,BAILBALANCE
    )
    SELECT
                T2.SERIALNO
                ,NVL(T1.BAILCURRENCY,T2.BUSINESSCURRENCY)
                ,'0'
                ,SUM(nvl(T1.BailBalance,0))
     FROM RWA_DEV.NCM_BAILACCOUNTINFO_TMP T1
     INNER JOIN (SELECT DISTINCT relativeserialno2,relativeserialno1
                 FROM RWA_DEV.NCM_BUSINESS_HISTORY WHERE DATANO=p_data_dt_str AND BALANCE>0 AND INPUTDATE=p_data_dt_str
                 ) T4
     ON T1.RELATIVECONTRACTNO=T4.relativeserialno1
     INNER JOIN RWA_DEV.NCM_Business_Contract T2
     ON T4.relativeserialno2=T2.SERIALNO
     AND T2.DATANO=p_data_dt_str
     AND T2.businesstype not in('11103030', '11103035', '11103036') --��ȡ ��E��,��I��,΢����
     AND NVL(T2.LINETYPE,'0010')<>'0040'
     WHERE T1.DATANO=p_data_dt_str
     AND EXISTS(SELECT 1 FROM RWA_DEV.NCM_BUSINESS_PUTOUT T5
                 WHERE T1.RELATIVECONTRACTNO=T5.SERIALNO AND T2.SERIALNO=T5.CONTRACTSERIALNO
                 AND T5.DATANO=p_data_dt_str)
     GROUP BY T2.SERIALNO,NVL(T1.BAILCURRENCY,T2.BUSINESSCURRENCY),'0'
     ;
     COMMIT;

     --�Ժ���ȡ���ı�֤���պ�ͬ�Ž��л���(��Ϊ�п����Ƕ�㶼���˱�֤��ģ�����Ҫ����)
     INSERT INTO RWA_DEV.RWA_TEMP_BAIL1(
                CONTRACTNO
                ,BAILCURRENCY
                ,ISSUM
                ,BAILBALANCE
     )
     SELECT
                T1.CONTRACTNO
                ,T1.BAILCURRENCY
                ,'1'
                ,SUM(T1.BAILBALANCE)
     FROM RWA_DEV.RWA_TEMP_BAIL1 T1
     GROUP BY T1.CONTRACTNO,T1.BAILCURRENCY,'1'
     ;
     COMMIT;*/

     --�������Ϣ
    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_TEMP_BAIL1',cascade => true);

     ---------------------------------------BP��BC������TEMP��ȡ��֤������ֵ����Ϊ��ͬ���ı�֤��
     /*--1.���Ŀ����е�ԭ�м�¼
     EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_TEMP_BAIL2';

     --�������BP���ϵ���Ч��֤��
     INSERT INTO RWA_DEV.RWA_TEMP_BAIL2(
                 CONTRACTNO
                 ,BAILCURRENCY
                 ,ISMAX
                 ,BAILBALANCE
     )
     SELECT
                 T2.CONTRACTSERIALNO
                 ,NVL(T2.BAILCURRENCY,T1.BUSINESSCURRENCY)
                 ,'0'
                 ,SUM(T2.BAILSUM)
     FROM RWA_DEV.NCM_BUSINESS_HISTORY T1
     INNER JOIN RWA_DEV.NCM_BUSINESS_PUTOUT T2
     ON T1.RELATIVESERIALNO1=T2.SERIALNO
     AND T2.DATANO=p_data_dt_str
     AND T2.BAILSUM>0
     WHERE T1.DATANO=p_data_dt_str AND T1.BALANCE>0
     GROUP BY T2.CONTRACTSERIALNO,NVL(T2.BAILCURRENCY,T1.BUSINESSCURRENCY),'0'
     ;
     COMMIT;

     --�������BC���ϵ���Ч��֤��
     INSERT INTO RWA_DEV.RWA_TEMP_BAIL2(
                 CONTRACTNO
                 ,BAILCURRENCY
                 ,ISMAX
                 ,BAILBALANCE
     )
     SELECT
                 T1.SERIALNO
                 ,NVL(T1.BAILCURRENCY,T1.BUSINESSCURRENCY)
                 ,'0'
                 ,T1.BAILSUM
     FROM RWA_DEV.NCM_BUSINESS_CONTRACT T1
     INNER JOIN (SELECT DISTINCT RELATIVESERIALNO2 FROM  RWA_DEV.NCM_BUSINESS_HISTORY
                 WHERE DATANO=p_data_dt_str
                 AND BALANCE>0) T2
     ON T1.SERIALNO=T2.RELATIVESERIALNO2
     WHERE T1.DATANO=p_data_dt_str AND T1.BAILSUM>0
     AND NOT EXISTS(SELECT 1 FROM RWA_DEV.RWA_TEMP_BAIL2 T3 WHERE T1.SERIALNO=T3.CONTRACTNO)
     ;
     COMMIT;

     --���������Ч�ı�֤������
     INSERT INTO RWA_DEV.RWA_TEMP_BAIL2(
                 CONTRACTNO
                 ,BAILCURRENCY
                 ,ISMAX
                 ,BAILBALANCE
     )
     SELECT
                 T1.CONTRACTNO
                 ,T1.BAILCURRENCY
                 ,'0'
                 ,T1.BAILBALANCE
     FROM RWA_DEV.RWA_TEMP_BAIL1 T1
     WHERE T1.BAILBALANCE>0 AND ISSUM='1'  --������Ļ��ܹ�����������0
     ;
     COMMIT;

     --�����ι����ı�֤��ȡ���ֵ����Ϊ���ս��
     INSERT INTO RWA_DEV.RWA_TEMP_BAIL2(
                 CONTRACTNO
                 ,BAILCURRENCY
                 ,ISMAX
                 ,BAILBALANCE
     )
     SELECT
                 T1.CONTRACTNO
                 ,T1.BAILCURRENCY
                 ,'1'
                 ,MAX(T1.BAILBALANCE)
     FROM RWA_DEV.RWA_TEMP_BAIL2 T1
     WHERE T1.BAILBALANCE>0
     GROUP BY T1.CONTRACTNO,T1.BAILCURRENCY,'1'
     ;
     COMMIT;

    --�������Ϣ
    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_TEMP_BAIL2',cascade => true);
*/
    /*Ŀ�������ͳ��*/
    --ͳ�Ʋ���ļ�¼��
    SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_TEMP_BAIL1;


    p_po_rtncode := '1';
	  p_po_rtnmsg  := '�ɹ�' || '-' || v_count;
		--�����쳣
		EXCEPTION
    WHEN OTHERS THEN
				 --Dbms_output.Put_line('������,�������Ϊ:'||sqlcode||';������ϢΪ:'||sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace);
       	 ROLLBACK;
   			 p_po_rtncode := sqlcode;
   			 p_po_rtnmsg  := '��ʱ������׼��('|| v_pro_name ||')����ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_TMP_DEAL;
/

