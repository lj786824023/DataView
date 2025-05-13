CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_EI_CLIENT(
                             p_data_dt_str  IN  VARCHAR2,    --��������
                             p_po_rtncode  OUT  VARCHAR2,    --���ر�� 1 �ɹ�,0 ʧ��
                            p_po_rtnmsg    OUT  VARCHAR2    --��������
        )
  /*
    �洢��������:RWA_DEV.PRO_RWA_EI_CLIENT
    ʵ�ֹ���:���������,���������������Ϣ
    ���ݿھ�:ȫ��
    ����Ƶ��:�³�
    ��  ��  :V1.0.0
    ��д��  :SHUXD
    ��дʱ��:2016-07-05
    ��  λ   :�Ϻ���˶��Ϣ�����ɷ����޹�˾
    Դ  ��1 :RWA.RWA_WP_COUNTRYRATING|����������
    Դ  ��2 :RWA_DEV.NCM_CUSTOMER_INFO|�ͻ���
    Դ  ��3 :RWA_DEV.RWA_EI_EXPOSURE|���÷��ձ�¶��
    Դ  ��4 :RWA_DEV.RWA_EI_CONTRACT|��ͬ��
    Դ  ��5 :RWA_DEV.RWA_EI_GUARANTEE|��֤��
    Դ  ��6 :RWA_DEV.RWA_EI_COLLATERAL|����ѺƷ��
    Դ  ��7 :RWA_DEV.BL_CUSTOMER_INFO|��¼�ͻ����ܱ�
    Դ  ��8 :RWA.ORG_INFO|������
    Դ  ��9 :RWA.CODE_LIBRARY|�����
    Դ  ��10:RWA.RWA_WP_SUBCOMPANY|�ӹ�˾���ñ�
    Դ  ��11:RWA_DEV.CCS_ACCT|����Ҵ����ʻ���
    Դ  ��12:RWA.RWA_WS_ASSET|��ծ�ʲ���¼��
    Դ  ��13:RAW_DEV.RWA_XF_CLINET|���ѽ��ڲ���������Ϣ��
    
    Ŀ���  :RWA_DEV.RWA_EI_CLIENT|����������ܱ�
    ������  :��
    �����¼(�޸���|�޸�ʱ��|�޸�����)
    xlpang  2019/05/29  �������ѽ��ڲ�������ͻ���Ϣ��EI��  
    
    */
  AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_EI_CLIENT';
  --�����ж�ֵ����
  v_count INTEGER;
  --�����쳣����
  v_raise EXCEPTION;

  BEGIN
    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    BEGIN
    --ɾ�������ڵķ���������˴洢�����ǵ�һ�����׳��쳣�����Ժ���
    EXECUTE IMMEDIATE 'ALTER TABLE RWA_DEV.RWA_EI_CLIENT DROP PARTITION CLIENT' || p_data_dt_str;

    COMMIT;

    EXCEPTION
      WHEN OTHERS THEN
        IF (SQLCODE <> '-2149') THEN
          --�״η���truncate�����2149�쳣
         p_po_rtncode := sqlcode;
          p_po_rtnmsg  := '���ܲ��������('|| v_pro_name ||')ETLת��ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;
         RETURN;
        END IF;
    END;

    --����һ����ǰ�����µķ���
    EXECUTE IMMEDIATE 'ALTER TABLE RWA_DEV.RWA_EI_CLIENT ADD PARTITION CLIENT' || p_data_dt_str || ' VALUES(TO_DATE('|| p_data_dt_str || ',''YYYYMMDD''))';

    COMMIT;
    --2.���������������ݴ�Դ����뵽Ŀ�����
    --2.1 �Ѳ�¼�ͻ����ܱ�������������Ϣ
    INSERT INTO RWA_DEV.RWA_EI_CLIENT(
                 DATADATE                                 --��������
                ,DATANO                                  --������ˮ��
                ,CLIENTID                                --��������ID
                ,SOURCECLIENTID                          --Դ��������ID
                ,SSYSID                                  --ԴϵͳID
                ,CLIENTNAME                              --������������
                ,SORGID                                  --Դ����ID
                ,SORGNAME                                --Դ��������
                ,ORGSORTNO                               --�������������
                ,ORGID                                   --��������ID
                ,ORGNAME                                 --������������
                ,INDUSTRYID                              --������ҵ����
                ,INDUSTRYNAME                            --������ҵ����
                ,CLIENTTYPE                              --�����������
                ,CLIENTSUBTYPE                           --��������С��
                ,COUNTRYCODE                             --����/��������
                ,REGISTSTATE                             --ע����һ����
                ,RCERATING                               --����ע����ⲿ����
                ,RCERAGENCY                              --����ע����ⲿ��������
                ,ORGANIZATIONCODE                        --��֯��������
                ,CONSOLIDATEDSCFLAG                      --�Ƿ񲢱��ӹ�˾
                ,SLCLIENTFLAG                            --רҵ����ͻ���ʶ
                ,SLCLIENTTYPE                            --רҵ����ͻ�����
                ,EXPOCATEGORYIRB                         --��������¶���
                ,DEFAULTFLAG                             --ΥԼ��ʶ
                ,MODELIRATING                            --ģ���ڲ�����
                ,MODELPD                                 --ģ��ΥԼ����
                ,IRATING                                 --�ڲ�����
                ,PD                                      --ΥԼ����
                ,CLIENTERATING                           --���������ⲿ����
                ,CCPFLAG                                 --���뽻�׶��ֱ�ʶ
                ,QUALCCPFLAG                             --�Ƿ�ϸ����뽻�׶���
                ,CLEARMEMBERFLAG                         --�����Ա��ʶ
                ,MSMBFLAG                                --���Ų�΢С��ҵ��ʶ
                ,SSMBFLAG                                --��׼С΢��ҵ��ʶ
                ,SSMBFLAGSTD         										 --Ȩ�ط���׼С΢��ҵ��ʶ
                ,ANNUALSALE                              --��˾�ͻ������۶�
                ,MODELID                                 --ģ��ID
                ,NEWDEFAULTFLAG                          --����ΥԼ��ʶ
                ,DEFAULTDATE                             --ΥԼʱ��
                ,COMPANYSIZE                             --��ҵ��ģ
    )
    WITH TMP_CUST_ERATING AS (
							    SELECT CUSTOMERID
							    			,CLIENTERATING
							  		FROM (SELECT T1.CUSTOMERID 	AS CUSTOMERID,
							                    T1.EVALUTEORG 	AS EVALUTEORG,
							                    T2.DESCRATING		AS CLIENTERATING,
							                    T1.EVALUTEDATE	AS EVALUTEDATE,
							                    RANK() OVER(PARTITION BY T1.CUSTOMERID ORDER BY T1.EVALUTEDATE DESC) AS RK,
							                    ROW_NUMBER() OVER(PARTITION BY T1.CUSTOMERID, T1.EVALUTEDATE ORDER BY T2.DESCRATING) AS RM,
							                    COUNT(1) OVER(PARTITION BY T1.CUSTOMERID, T1.EVALUTEDATE) AS RN
							               FROM RWA_DEV.NCM_CUSTOMER_RATING T1
							          LEFT JOIN RWA_DEV.RWA_CD_RATING_MAPPING T2
							                 ON T1.EVALUTEORG = T2.SRCRATINGORG
							                AND T1.EVALUTELEVEL = T2.SRCRATINGNAME
							                AND T2.MAPPINGTYPE = '01' --ȫ������
							                AND T2.SRCRATINGTYPE = '01' --��������
							              WHERE T1.EVALUTEDATE <= P_DATA_DT_STR
							              	AND T1.DATANO = P_DATA_DT_STR)
								 WHERE RK = 1
								   AND RM = (CASE WHEN RN = 1 THEN 1 ELSE 2 END)							--ȡ���µ�һ��������������һ����ڶ��������ȡ�ڶ��õ�����
   	)
   	, TMP_CUST_IRATING AS (
   								SELECT CUSTID,
									       CUSTNAME,
									       ORGCERTCODE,
									       MODELID,
									       PDCODE,
									       PDLEVEL,
									       PDADJCODE,
									       PDADJLEVEL,
									       PD,
									       PDVAVLIDDATE
									  FROM RWA_DEV.RWA_TEMP_PDLEVEL
									 WHERE ROWID IN
									       (SELECT MAX(ROWID) FROM RWA_DEV.RWA_TEMP_PDLEVEL GROUP BY ORGCERTCODE)
   	)
     SELECT
                TO_DATE(T1.DATANO,'YYYYMMDD')                           AS DATADATE            --��������
                ,T1.DATANO                                              AS DATANO              --������ˮ��
                ,T1.CUSTOMERID                                          AS CLIENTID            --��������ID
                ,T1.CUSTOMERID                                          AS SOURCECLIENTID      --Դ��������ID
                ,'BL'                                                   AS SSYSID              --ԴϵͳID
                ,T1.CUSTOMERNAME                                        AS CLIENTNAME          --������������
                ,T1.ORGID                                               AS SORGID              --Դ����ID
                ,T4.ORGNAME                                             AS SORGNAME            --Դ��������
                ,T4.SORTNO                                              AS ORGSORTNO           --�������������
                ,T1.ORGID                                               AS ORGID               --��������ID
                ,T4.ORGNAME                                             AS ORGNAME             --������������
                ,T1.INDUSTRYTYPE                                        AS INDUSTRYID          --������ҵ����
                ,T5.ITEMNAME                                            AS INDUSTRYNAME        --������ҵ����
                ,CASE WHEN T1.CUSTOMERCATEGORY IS NOT NULL THEN SUBSTR(T1.CUSTOMERCATEGORY,1,2)
                      ELSE ''
                 END                                                    AS CLIENTTYPE          --�����������
                ,T1.CUSTOMERCATEGORY                                    AS CLIENTSUBTYPE       --��������С��
                ,CASE WHEN  (T1.COUNTRYCODE IS NULL OR T1.COUNTRYCODE='OTH')
                      THEN  'CHN'
                      ELSE  T1.COUNTRYCODE
                 END                                                    AS COUNTRYCODE         --����/��������
                ,CASE WHEN (T1.COUNTRYCODE IS NULL OR T1.COUNTRYCODE='OTH' OR T1.COUNTRYCODE='CHN') THEN '01'
                      ELSE '02'
                 END                                                    AS REGISTSTATE         --ע����һ����
                ,CASE WHEN (T1.COUNTRYCODE IS NULL OR T1.COUNTRYCODE='OTH' OR T1.COUNTRYCODE='CHN') THEN ''
                      ELSE NVL(T3.RATINGRESULT,'0124')
                 END                                                    AS RCERATING           --����ע����ⲿ����
                ,CASE WHEN (T1.COUNTRYCODE IS NULL OR T1.COUNTRYCODE='OTH' OR T1.COUNTRYCODE='CHN') THEN ''
                      ELSE NVL(T3.RATINGORG,'01')
                 END                                                    AS RCERAGENCY          --����ע����ⲿ��������
                ,T1.CERTID                                              AS ORGANIZATIONCODE    --��֯��������
                ,CASE WHEN T6.ORGANIZATIONCODE IS NOT NULL THEN '1'
                      ELSE '0'
                 END                                                    AS CONSOLIDATEDSCFLAG  --�Ƿ񲢱��ӹ�˾
                ,''                                                     AS SLCLIENTFLAG        --רҵ����ͻ���ʶ
                ,''                                                     AS SLCLIENTTYPE        --רҵ����ͻ�����
                ,''                                                     AS EXPOCATEGORYIRB     --��������¶���
                ,CASE WHEN NVL(T7.PDADJCODE,T14.PDADJCODE) = 'D' THEN '1'
                      ELSE '0'
                 END                                                    AS DEFAULTFLAG         --ΥԼ��ʶ
                ,NVL(T7.PDLEVEL,T14.PDLEVEL)	                          AS MODELIRATING        --ģ���ڲ�����
                ,NVL(T7.PD,T14.PD)		                                  AS MODELPD             --ģ��ΥԼ����
                ,NVL(T7.PDADJLEVEL,T14.PDADJLEVEL)                      AS IRATING             --�ڲ�����
                ,NVL(T7.PD,T14.PD)		                                  AS PD                  --ΥԼ����
                ,CASE WHEN T1.ERATING IS NULL THEN T12.CLIENTERATING
                 ELSE RWA_DEV.GETSTANDARDRATING1(T1.ERATING)
                 END																	                  AS CLIENTERATING       --���������ⲿ����
                ,'0'                                                    AS CCPFLAG             --���뽻�׶��ֱ�ʶ
                ,'0'                                                    AS QUALCCPFLAG         --�Ƿ�ϸ����뽻�׶���
                ,'0'                                                    AS CLEARMEMBERFLAG     --�����Ա��ʶ
                ,CASE WHEN T1.SCOPE IN ('4','5','02','03')
                      THEN '1'
                      ELSE '0'
                 END                                                    AS MSMBFLAG            --���Ų�΢С��ҵ��ʶ
                ,'0'                                                    AS SSMBFLAG            --��׼С΢��ҵ��ʶ
                ,'0'                                                    AS SSMBFLAGSTD         --Ȩ�ط���׼С΢��ҵ��ʶ
                ,T9.AVEFINANCESUM	                                      AS ANNUALSALE          --��˾�ͻ������۶�
                ,NVL(T7.MODELID,T14.MODELID)                      			AS MODELID             --ģ��ID
                ,DECODE(NVL(T9.NEWDEFAULTFLAG,'1'),'0','1','0')         AS NEWDEFAULTFLAG      --����ΥԼ��ʶ
                ,CASE WHEN NVL(T7.PDADJCODE,T14.PDADJCODE) = 'D' THEN TO_DATE(NVL(T7.PDVAVLIDDATE,T14.PDVAVLIDDATE),'YYYYMMDD')
                 ELSE NULL
                 END					                                        	AS DEFAULTDATE         --ΥԼʱ��
                ,CASE WHEN T1.SCOPE = '2' THEN '00'																						 --������ҵ
                			WHEN T1.SCOPE = '3' THEN '01'																						 --������ҵ
                			WHEN T1.SCOPE = '4' THEN '02'																						 --С����ҵ
                			WHEN T1.SCOPE = '5' THEN '03'																						 --΢����ҵ
                			ELSE NVL(T1.SCOPE,'01')																									 --Ĭ��������ҵ
                 END		                                                AS COMPANYSIZE         --��ҵ��ģ
    FROM        RWA_DEV.BL_CUSTOMER_INFO T1
    LEFT JOIN   RWA.RWA_WP_COUNTRYRATING T3
    ON          T1.COUNTRYCODE = T3.COUNTRYCODE
    LEFT JOIN   RWA.ORG_INFO T4
    ON          T1.ORGID = T4.ORGID
    LEFT JOIN   RWA.CODE_LIBRARY T5
    ON          T1.INDUSTRYTYPE = T5.ITEMNO
    AND         T5.CODENO = 'IndustryType'
    LEFT JOIN   RWA.RWA_WP_SUBCOMPANY T6
    ON          REPLACE(T1.CERTID,'-') = REPLACE(T6.ORGANIZATIONCODE,'-')
    AND					T1.CERTTYPE IN ('Ent01','Ent02')
    LEFT JOIN		RWA_DEV.RWA_TEMP_PDLEVEL T7
	  ON					T1.CUSTOMERID = T7.CUSTID
	  LEFT JOIN		RWA_DEV.NCM_CUSTOMER_INFO T9
	  ON					T1.CUSTOMERID = T9.CUSTOMERID
	  AND					T9.DATANO = p_data_dt_str
	  LEFT JOIN		TMP_CUST_ERATING T12
	  ON					T1.CUSTOMERID = T12.CUSTOMERID
	  LEFT JOIN		TMP_CUST_IRATING T14
	  ON					REPLACE(T1.CERTID,'-','') = REPLACE(T14.ORGCERTCODE,'-','')
	  AND					T1.CERTTYPE IN ('Ent01','Ent02')
    WHERE       T1.DATANO = P_DATA_DT_STR
    ;

    COMMIT;

    --����Ͷ�ʿͻ���EI�ͻ���
    INSERT INTO RWA_DEV.RWA_EI_CLIENT(
                 DATADATE                                 --��������
                ,DATANO                                  --������ˮ��
                ,CLIENTID                                --��������ID
                ,SOURCECLIENTID                          --Դ��������ID
                ,SSYSID                                  --ԴϵͳID
                ,CLIENTNAME                              --������������
                ,SORGID                                  --Դ����ID
                ,SORGNAME                                --Դ��������
                ,ORGSORTNO                               --�������������
                ,ORGID                                   --��������ID
                ,ORGNAME                                 --������������
                ,INDUSTRYID                              --������ҵ����
                ,INDUSTRYNAME                            --������ҵ����
                ,CLIENTTYPE                              --�����������
                ,CLIENTSUBTYPE                           --��������С��
                ,COUNTRYCODE                             --����/��������
                ,REGISTSTATE                             --ע����һ����
                ,RCERATING                               --����ע����ⲿ����
                ,RCERAGENCY                              --����ע����ⲿ��������
                ,ORGANIZATIONCODE                        --��֯��������
                ,CONSOLIDATEDSCFLAG                      --�Ƿ񲢱��ӹ�˾
                ,SLCLIENTFLAG                            --רҵ����ͻ���ʶ
                ,SLCLIENTTYPE                            --רҵ����ͻ�����
                ,EXPOCATEGORYIRB                         --��������¶���
                ,DEFAULTFLAG                             --ΥԼ��ʶ
                ,MODELIRATING                            --ģ���ڲ�����
                ,MODELPD                                 --ģ��ΥԼ����
                ,IRATING                                 --�ڲ�����
                ,PD                                      --ΥԼ����
                ,CLIENTERATING                           --���������ⲿ����
                ,CCPFLAG                                 --���뽻�׶��ֱ�ʶ
                ,QUALCCPFLAG                             --�Ƿ�ϸ����뽻�׶���
                ,CLEARMEMBERFLAG                         --�����Ա��ʶ
                ,MSMBFLAG                                --���Ų�΢С��ҵ��ʶ
                ,SSMBFLAG                                --��׼С΢��ҵ��ʶ
                ,SSMBFLAGSTD         										 --Ȩ�ط���׼С΢��ҵ��ʶ
                ,ANNUALSALE                              --��˾�ͻ������۶�
                ,MODELID                                 --ģ��ID
                ,NEWDEFAULTFLAG                          --����ΥԼ��ʶ
                ,DEFAULTDATE                             --ΥԼʱ��
                ,COMPANYSIZE                             --��ҵ��ģ
     )
     SELECT
                 T1.DATADATE                                 --��������
                ,T1.DATANO                                  --������ˮ��
                ,T1.CLIENTID                                --��������ID
                ,T1.SOURCECLIENTID                          --Դ��������ID
                ,T1.SSYSID                                  --ԴϵͳID
                ,T1.CLIENTNAME                              --������������
                ,T1.SORGID                                  --Դ����ID
                ,T1.SORGNAME                                --Դ��������
                ,T1.ORGSORTNO                               --�������������
                ,T1.ORGID                                   --��������ID
                ,T1.ORGNAME                                 --������������
                ,T1.INDUSTRYID                              --������ҵ����
                ,T1.INDUSTRYNAME                            --������ҵ����
                ,T1.CLIENTTYPE                              --�����������
                ,T1.CLIENTSUBTYPE                           --��������С��
                ,T1.COUNTRYCODE                             --����/��������
                ,T1.REGISTSTATE                             --ע����һ����
                ,T1.RCERATING                               --����ע����ⲿ����
                ,T1.RCERAGENCY                              --����ע����ⲿ��������
                ,T1.ORGANIZATIONCODE                        --��֯��������
                ,T1.CONSOLIDATEDSCFLAG                      --�Ƿ񲢱��ӹ�˾
                ,T1.SLCLIENTFLAG                            --רҵ����ͻ���ʶ
                ,T1.SLCLIENTTYPE                            --רҵ����ͻ�����
                ,T1.EXPOCATEGORYIRB                         --��������¶���
                ,T1.DEFAULTFLAG                             --ΥԼ��ʶ
                ,T1.MODELIRATING                            --ģ���ڲ�����
                ,T1.MODELPD                                 --ģ��ΥԼ����
                ,T1.IRATING                                 --�ڲ�����
                ,T1.PD                                      --ΥԼ����
                ,T1.CLIENTERATING                           --���������ⲿ����
                ,T1.CCPFLAG                                 --���뽻�׶��ֱ�ʶ
                ,T1.QUALCCPFLAG                             --�Ƿ�ϸ����뽻�׶���
                ,T1.CLEARMEMBERFLAG                         --�����Ա��ʶ
                ,T1.MSMBFLAG                                --���Ų�΢С��ҵ��ʶ
                ,T1.SSMBFLAG                                --��׼С΢��ҵ��ʶ
                ,T1.SSMBFLAGSTD                             --Ȩ�ط���׼С΢��ҵ��ʶ
                ,T1.ANNUALSALE                              --��˾�ͻ������۶�
                ,T1.MODELID                                 --ģ��ID
                ,T1.NEWDEFAULTFLAG                          --����ΥԼ��ʶ
                ,T1.DEFAULTDATE                             --ΥԼʱ��
                ,NVL(T1.COMPANYSIZE,'01')                   --��ҵ��ģ Ĭ��������ҵ
    FROM RWA_DEV.RWA_TZ_CLIENT T1
    WHERE       T1.DATANO = P_DATA_DT_STR
    AND NOT EXISTS (SELECT 1 FROM RWA_EI_CLIENT T6 WHERE T1.CLIENTID = T6.CLIENTID AND T6.DATADATE = TO_DATE(P_DATA_DT_STR, 'YYYYMMDD'))
    ;

    COMMIT;

    --2.2 ���ݼ��пͻ���������÷��ձ�¶(�ų����ÿ�ϵͳ�ģ����ÿ��ͻ����浥������)�Ĳ���������Ϣ
    INSERT INTO RWA_DEV.RWA_EI_CLIENT(
                 DATADATE                                 --��������
                ,DATANO                                  --������ˮ��
                ,CLIENTID                                --��������ID
                ,SOURCECLIENTID                          --Դ��������ID
                ,SSYSID                                  --ԴϵͳID
                ,CLIENTNAME                              --������������
                ,SORGID                                  --Դ����ID
                ,SORGNAME                                --Դ��������
                ,ORGSORTNO                               --�������������
                ,ORGID                                   --��������ID
                ,ORGNAME                                 --������������
                ,INDUSTRYID                              --������ҵ����
                ,INDUSTRYNAME                            --������ҵ����
                ,CLIENTTYPE                              --�����������
                ,CLIENTSUBTYPE                           --��������С��
                ,COUNTRYCODE                             --����/��������
                ,REGISTSTATE                             --ע����һ����
                ,RCERATING                               --����ע����ⲿ����
                ,RCERAGENCY                              --����ע����ⲿ��������
                ,ORGANIZATIONCODE                        --��֯��������
                ,CONSOLIDATEDSCFLAG                      --�Ƿ񲢱��ӹ�˾
                ,SLCLIENTFLAG                            --רҵ����ͻ���ʶ
                ,SLCLIENTTYPE                            --רҵ����ͻ�����
                ,EXPOCATEGORYIRB                         --��������¶���
                ,DEFAULTFLAG                             --ΥԼ��ʶ
                ,MODELIRATING                            --ģ���ڲ�����
                ,MODELPD                                 --ģ��ΥԼ����
                ,IRATING                                 --�ڲ�����
                ,PD                                      --ΥԼ����
                ,CLIENTERATING                           --���������ⲿ����
                ,CCPFLAG                                 --���뽻�׶��ֱ�ʶ
                ,QUALCCPFLAG                             --�Ƿ�ϸ����뽻�׶���
                ,CLEARMEMBERFLAG                         --�����Ա��ʶ
                ,MSMBFLAG                                --���Ų�΢С��ҵ��ʶ
                ,SSMBFLAG                                --��׼С΢��ҵ��ʶ
                ,SSMBFLAGSTD         										 --Ȩ�ط���׼С΢��ҵ��ʶ
                ,ANNUALSALE                              --��˾�ͻ������۶�
                ,MODELID                                 --ģ��ID
                ,NEWDEFAULTFLAG                          --����ΥԼ��ʶ
                ,DEFAULTDATE                             --ΥԼʱ��
                ,COMPANYSIZE                             --��ҵ��ģ
    )
    WITH TMP_CUST_ERATING AS (
							    SELECT CUSTOMERID
							    			,CLIENTERATING
							  		FROM (SELECT T1.CUSTOMERID 	AS CUSTOMERID,
							                    T1.EVALUTEORG 	AS EVALUTEORG,
							                    T2.DESCRATING		AS CLIENTERATING,
							                    T1.EVALUTEDATE	AS EVALUTEDATE,
							                    RANK() OVER(PARTITION BY T1.CUSTOMERID ORDER BY T1.EVALUTEDATE DESC) AS RK,
							                    ROW_NUMBER() OVER(PARTITION BY T1.CUSTOMERID, T1.EVALUTEDATE ORDER BY T2.DESCRATING) AS RM,
							                    COUNT(1) OVER(PARTITION BY T1.CUSTOMERID, T1.EVALUTEDATE) AS RN
							               FROM RWA_DEV.NCM_CUSTOMER_RATING T1
							          LEFT JOIN RWA_DEV.RWA_CD_RATING_MAPPING T2
							                 ON T1.EVALUTEORG = T2.SRCRATINGORG
							                AND T1.EVALUTELEVEL = T2.SRCRATINGNAME
							                AND T2.MAPPINGTYPE = '01' --ȫ������
							                AND T2.SRCRATINGTYPE = '01' --��������
							              WHERE T1.EVALUTEDATE <= P_DATA_DT_STR
							              	AND T1.DATANO = P_DATA_DT_STR)
								 WHERE RK = 1
								   AND RM = (CASE WHEN RN = 1 THEN 1 ELSE 2 END)							--ȡ���µ�һ��������������һ����ڶ��������ȡ�ڶ��õ�����
   	)
     SELECT
                TO_DATE(T1.DATANO,'YYYYMMDD')                           AS DATADATE            --��������
                ,T1.DATANO                                              AS DATANO              --������ˮ��
                ,T1.CUSTOMERID                                          AS CLIENTID            --��������ID
                ,T1.CUSTOMERID                                          AS SOURCECLIENTID      --Դ��������ID
                ,'HX'                                                   AS SSYSID              --ԴϵͳID
                ,T1.CUSTOMERNAME                                        AS CLIENTNAME          --������������
                ,T1.ORGID                                               AS SORGID              --Դ����ID
                ,T4.ORGNAME                                             AS SORGNAME            --Դ��������
                ,T4.SORTNO                                              AS ORGSORTNO           --�������������
                --,T1.ORGID                                               AS ORGID               --��������ID
                ,DECODE(SUBSTR(T1.ORGID,1,1),'@','01000000',T1.ORGID)
                --,T4.ORGNAME                                             AS ORGNAME             --������������
                ,NVL(T4.ORGNAME,'����')
                ,T1.INDUSTRYTYPE                                        AS INDUSTRYID          --������ҵ����
                ,T5.ITEMNAME                                            AS INDUSTRYNAME        --������ҵ����
                ,''/*SUBSTR(T1.RWACUSTOMERTYPE,1,2)*/                         AS CLIENTTYPE          --�����������
                ,''/*T1.RWACUSTOMERTYPE */                                    AS CLIENTSUBTYPE       --��������С��
                ,CASE WHEN  (T1.COUNTRYCODE IS NULL OR T1.COUNTRYCODE='OTH')
                      THEN  'CHN'
                      ELSE  T1.COUNTRYCODE
                 END                                                    AS COUNTRYCODE         --����/��������
                ,CASE WHEN (T1.COUNTRYCODE IS NULL OR T1.COUNTRYCODE='OTH' OR T1.COUNTRYCODE='CHN') THEN '01'
                      ELSE '02'
                 END                                                    AS REGISTSTATE         --ע����һ����
                ,CASE WHEN (T1.COUNTRYCODE IS NULL OR T1.COUNTRYCODE='OTH' OR T1.COUNTRYCODE='CHN') THEN ''
                      ELSE NVL(T3.RATINGRESULT,'0124')
                 END                                                    AS RCERATING           --����ע����ⲿ����
                ,CASE WHEN (T1.COUNTRYCODE IS NULL OR T1.COUNTRYCODE='OTH' OR T1.COUNTRYCODE='CHN') THEN ''
                      ELSE NVL(T3.RATINGORG,'01')
                 END                                                    AS RCERAGENCY          --����ע����ⲿ��������
                ,CASE WHEN T1.CUSTOMERTYPE='0321000001'
                      THEN ''
                      ELSE T1.CERTID
                 END                                                    AS ORGANIZATIONCODE    --��֯��������
                ,CASE WHEN T6.ORGANIZATIONCODE IS NOT NULL THEN '1'
                      ELSE '0'
                 END                                                    AS CONSOLIDATEDSCFLAG  --�Ƿ񲢱��ӹ�˾
                ,'0'                                                    AS SLCLIENTFLAG        --רҵ����ͻ���ʶ
                ,''                                                     AS SLCLIENTTYPE        --רҵ����ͻ�����
                ,''                                                     AS EXPOCATEGORYIRB     --��������¶���
                ,CASE WHEN T7.PDADJCODE = 'D' THEN '1'
                      ELSE '0'
                 END                                                    AS DEFAULTFLAG         --ΥԼ��ʶ
                ,T7.PDLEVEL	                          									AS MODELIRATING        --ģ���ڲ�����
                ,T7.PD		                        						          AS MODELPD             --ģ��ΥԼ����
                ,T7.PDADJLEVEL										                      AS IRATING             --�ڲ�����
                ,T7.PD								                                  AS PD                  --ΥԼ����
                ,T12.CLIENTERATING							                        AS CLIENTERATING       --���������ⲿ����
                ,'0'                                                    AS CCPFLAG             --���뽻�׶��ֱ�ʶ
                ,'0'                                                    AS QUALCCPFLAG         --�Ƿ�ϸ����뽻�׶���
                ,'0'                                                    AS CLEARMEMBERFLAG     --�����Ա��ʶ
                ,CASE WHEN T1.SCOPE IN ('02','03')
                      THEN '1'
                      ELSE '0'
                 END                                                    AS MSMBFLAG            --���Ų�΢С��ҵ��ʶ
                ,NVL(T1.ISSUPERVISESTANDARSMENT,'0')                    AS SSMBFLAG            --��׼С΢��ҵ��ʶ
                ,'0'                                                    AS SSMBFLAGSTD         --Ȩ�ط���׼С΢��ҵ��ʶ
                ,T1.AVEFINANCESUM                                       AS ANNUALSALE          --��˾�ͻ������۶�
                ,T7.MODELID									                      			AS MODELID             --ģ��ID
                ,DECODE(NVL(T1.NEWDEFAULTFLAG,'1'),'0','1','0')         AS NEWDEFAULTFLAG      --����ΥԼ��ʶ
                ,CASE WHEN T7.PDADJCODE = 'D' THEN TO_DATE(T7.PDVAVLIDDATE,'YYYYMMDD')
                 ELSE NULL
                 END					                                        	AS DEFAULTDATE         --ΥԼʱ��
                ,CASE WHEN T1.SCOPE = '2' THEN '00'                                             --������ҵ
                      WHEN T1.SCOPE = '3' THEN '01'                                            --������ҵ
                      WHEN T1.SCOPE = '4' THEN '02'                                            --С����ҵ
                      WHEN T1.SCOPE = '5' THEN '03'                                            --΢����ҵ
                      ELSE NVL(T1.SCOPE,'01')                                                  --Ĭ��������ҵ
                 END                                                    AS COMPANYSIZE         --��ҵ��ģ
    FROM        RWA_DEV.NCM_CUSTOMER_INFO T1
    INNER JOIN   (SELECT DISTINCT CLIENTID
                        -- ,SUM(NORMALPRINCIPAL) AS BALANCE
                  FROM RWA_DEV.RWA_EI_EXPOSURE
                  WHERE DATADATE = TO_DATE(P_DATA_DT_STR, 'YYYYMMDD')
                  AND   SSYSID NOT IN('XYK','LC','DZ')
                  AND ACCSUBJECT1<>'13010511'      --�ų��ڲ�ת���֣��ڲ�ת���ֺ��浥������
                  ) T2
    ON           T1.CUSTOMERID = T2.CLIENTID
    LEFT JOIN   RWA.RWA_WP_COUNTRYRATING T3
    ON          T1.COUNTRYCODE = T3.COUNTRYCODE
    LEFT JOIN   RWA.ORG_INFO T4
    ON          T1.ORGID = T4.ORGID
    LEFT JOIN   RWA.CODE_LIBRARY T5
    ON          T1.INDUSTRYTYPE = T5.ITEMNO
    AND         T5.CODENO = 'IndustryType'
    LEFT JOIN   RWA.RWA_WP_SUBCOMPANY T6
    ON          T1.CERTID=T6.ORGANIZATIONCODE
    LEFT JOIN   RWA_DEV.RWA_TEMP_PDLEVEL T7
    ON          T1.CUSTOMERID=T7.CUSTID
    LEFT JOIN		TMP_CUST_ERATING T12
	  ON					T1.CUSTOMERID = T12.CUSTOMERID
    WHERE       T1.DATANO = P_DATA_DT_STR
    AND NOT EXISTS (SELECT 1 FROM RWA_EI_CLIENT T6 WHERE T1.CUSTOMERID = T6.CLIENTID AND T6.DATADATE = TO_DATE(P_DATA_DT_STR, 'YYYYMMDD'))
    ;

    COMMIT;
    
     --2.2 ���ݼ��пͻ���������÷��ձ�¶(�ڲ�ת����)�Ĳ���������Ϣ
    INSERT INTO RWA_DEV.RWA_EI_CLIENT(
                 DATADATE                                 --��������
                ,DATANO                                  --������ˮ��
                ,CLIENTID                                --��������ID
                ,SOURCECLIENTID                          --Դ��������ID
                ,SSYSID                                  --ԴϵͳID
                ,CLIENTNAME                              --������������
                ,SORGID                                  --Դ����ID
                ,SORGNAME                                --Դ��������
                ,ORGSORTNO                               --�������������
                ,ORGID                                   --��������ID
                ,ORGNAME                                 --������������
                ,INDUSTRYID                              --������ҵ����
                ,INDUSTRYNAME                            --������ҵ����
                ,CLIENTTYPE                              --�����������
                ,CLIENTSUBTYPE                           --��������С��
                ,COUNTRYCODE                             --����/��������
                ,REGISTSTATE                             --ע����һ����
                ,RCERATING                               --����ע����ⲿ����
                ,RCERAGENCY                              --����ע����ⲿ��������
                ,ORGANIZATIONCODE                        --��֯��������
                ,CONSOLIDATEDSCFLAG                      --�Ƿ񲢱��ӹ�˾
                ,SLCLIENTFLAG                            --רҵ����ͻ���ʶ
                ,SLCLIENTTYPE                            --רҵ����ͻ�����
                ,EXPOCATEGORYIRB                         --��������¶���
                ,DEFAULTFLAG                             --ΥԼ��ʶ
                ,MODELIRATING                            --ģ���ڲ�����
                ,MODELPD                                 --ģ��ΥԼ����
                ,IRATING                                 --�ڲ�����
                ,PD                                      --ΥԼ����
                ,CLIENTERATING                           --���������ⲿ����
                ,CCPFLAG                                 --���뽻�׶��ֱ�ʶ
                ,QUALCCPFLAG                             --�Ƿ�ϸ����뽻�׶���
                ,CLEARMEMBERFLAG                         --�����Ա��ʶ
                ,MSMBFLAG                                --���Ų�΢С��ҵ��ʶ
                ,SSMBFLAG                                --��׼С΢��ҵ��ʶ
                ,SSMBFLAGSTD                             --Ȩ�ط���׼С΢��ҵ��ʶ
                ,ANNUALSALE                              --��˾�ͻ������۶�
                ,MODELID                                 --ģ��ID
                ,NEWDEFAULTFLAG                          --����ΥԼ��ʶ
                ,DEFAULTDATE                             --ΥԼʱ��
                ,COMPANYSIZE                             --��ҵ��ģ
    )
     SELECT
                TO_DATE(TEMP.DATANO,'YYYYMMDD')                           AS DATADATE            --��������
                ,TEMP.DATANO                                              AS DATANO              --������ˮ��
                ,TEMP.CUSTNO                                              AS CLIENTID            --��������ID
                ,TEMP.CUSTNO                                              AS SOURCECLIENTID      --Դ��������ID
                ,'PJ'                                                     AS SSYSID              --ԴϵͳID
                ,TEMP.CUSTNAME                                          AS CLIENTNAME          --������������
                ,'9998'                                             AS SORGID              --Դ����ID
                ,'δ֪'                                                 AS SORGNAME            --Դ��������
                ,'1'                                        AS ORGSORTNO           --�������������
                ,'9998'                                             AS ORGID               --��������ID
                ,'δ֪'                                                 AS ORGNAME             --������������
                ,'999999'                                               AS INDUSTRYID          --������ҵ����
                ,'δ֪'                                                 AS INDUSTRYNAME        --������ҵ����
                ,''                                                     AS CLIENTTYPE          --�����������
                ,''                                                     AS CLIENTSUBTYPE       --��������С��
                ,'CHN'                                                  AS COUNTRYCODE         --����/��������
                ,'01'                                                   AS REGISTSTATE         --ע����һ����
                ,''                                                     AS RCERATING           --����ע����ⲿ����
                ,''                                                     AS RCERAGENCY          --����ע����ⲿ��������
                ,''                                                     AS ORGANIZATIONCODE    --��֯��������
                ,'0'                                                    AS CONSOLIDATEDSCFLAG  --�Ƿ񲢱��ӹ�˾
                ,''                                                     AS SLCLIENTFLAG        --רҵ����ͻ���ʶ
                ,''                                                     AS SLCLIENTTYPE        --רҵ����ͻ�����
                ,''                                                     AS EXPOCATEGORYIRB     --��������¶���
                ,'0'                                                    AS DEFAULTFLAG         --ΥԼ��ʶ
                ,''                                                     AS MODELIRATING        --ģ���ڲ�����
                ,''                                                     AS MODELPD             --ģ��ΥԼ����
                ,''                                                     AS IRATING             --�ڲ�����
                ,''                                                     AS PD                  --ΥԼ����
                ,''                                                     AS CLIENTERATING       --���������ⲿ����
                ,'0'                                                    AS CCPFLAG             --���뽻�׶��ֱ�ʶ
                ,'0'                                                    AS QUALCCPFLAG         --�Ƿ�ϸ����뽻�׶���
                ,'0'                                                    AS CLEARMEMBERFLAG     --�����Ա��ʶ
                ,'0'                                                    AS MSMBFLAG            --���Ų�΢С��ҵ��ʶ
                ,'0'                                                    AS SSMBFLAG            --��׼С΢��ҵ��ʶ
                ,'0'                                                    AS SSMBFLAGSTD         --Ȩ�ط���׼С΢��ҵ��ʶ
                ,NULL                                                   AS ANNUALSALE          --��˾�ͻ������۶�
                ,''                                                     AS MODELID             --ģ��ID
                ,'0'                                                    AS NEWDEFAULTFLAG      --����ΥԼ��ʶ
                ,''                                                     AS DEFAULTDATE         --ΥԼʱ��
                ,'01'                                                   AS COMPANYSIZE         --��ҵ��ģ Ĭ��������ҵ
    FROM
    (SELECT DISTINCT T1.DATANO, substr(T1.BILL_NO,2,12) AS CUSTNO,T2.UBANK_NAME AS CUSTNAME
    FROM BRD_BILL T1
    INNER JOIN ebs_union_bank T2
    ON substr(T1.BILL_NO,2,12)=T2.ubank_no
    AND T1.DATANO=T2.DATANO
    WHERE SUBSTR(T1.SBJT_CD, 1, 6)='130105'   --����Ϊ�ڲ�ת����
    AND ATL_PAY_AMT <> 0
    AND T1.DATANO=p_data_dt_str) TEMP 
    ;

    COMMIT;

    --2.2 ���ݼ��пͻ���������÷��ձ�¶(���ÿ�ϵͳ�ģ����ÿ��ͻ����ﵥ������)�Ĳ���������Ϣ
    INSERT INTO RWA_DEV.RWA_EI_CLIENT(
                 DATADATE                                 --��������
                ,DATANO                                  --������ˮ��
                ,CLIENTID                                --��������ID
                ,SOURCECLIENTID                          --Դ��������ID
                ,SSYSID                                  --ԴϵͳID
                ,CLIENTNAME                              --������������
                ,SORGID                                  --Դ����ID
                ,SORGNAME                                --Դ��������
                ,ORGSORTNO                               --�������������
                ,ORGID                                   --��������ID
                ,ORGNAME                                 --������������
                ,INDUSTRYID                              --������ҵ����
                ,INDUSTRYNAME                            --������ҵ����
                ,CLIENTTYPE                              --�����������
                ,CLIENTSUBTYPE                           --��������С��
                ,COUNTRYCODE                             --����/��������
                ,REGISTSTATE                             --ע����һ����
                ,RCERATING                               --����ע����ⲿ����
                ,RCERAGENCY                              --����ע����ⲿ��������
                ,ORGANIZATIONCODE                        --��֯��������
                ,CONSOLIDATEDSCFLAG                      --�Ƿ񲢱��ӹ�˾
                ,SLCLIENTFLAG                            --רҵ����ͻ���ʶ
                ,SLCLIENTTYPE                            --רҵ����ͻ�����
                ,EXPOCATEGORYIRB                         --��������¶���
                ,DEFAULTFLAG                             --ΥԼ��ʶ
                ,MODELIRATING                            --ģ���ڲ�����
                ,MODELPD                                 --ģ��ΥԼ����
                ,IRATING                                 --�ڲ�����
                ,PD                                      --ΥԼ����
                ,CLIENTERATING                           --���������ⲿ����
                ,CCPFLAG                                 --���뽻�׶��ֱ�ʶ
                ,QUALCCPFLAG                             --�Ƿ�ϸ����뽻�׶���
                ,CLEARMEMBERFLAG                         --�����Ա��ʶ
                ,MSMBFLAG                                --���Ų�΢С��ҵ��ʶ
                ,SSMBFLAG                                --��׼С΢��ҵ��ʶ
                ,SSMBFLAGSTD         										 --Ȩ�ط���׼С΢��ҵ��ʶ
                ,ANNUALSALE                              --��˾�ͻ������۶�
                ,MODELID                                 --ģ��ID
                ,NEWDEFAULTFLAG                          --����ΥԼ��ʶ
                ,DEFAULTDATE                             --ΥԼʱ��
                ,COMPANYSIZE                             --��ҵ��ģ
    ) 
    SELECT DISTINCT 
                TO_DATE(p_data_dt_str,'YYYYMMDD')                       AS DATADATE            --��������
                ,p_data_dt_str                                          AS DATANO              --������ˮ��
                ,T1.CLIENTID                                            AS CLIENTID            --��������ID
                ,T1.CLIENTID                                            AS SOURCECLIENTID      --Դ��������ID
                ,'XYK'                                                  AS SSYSID              --ԴϵͳID
                ,T1.CLIENTNAME                                        AS CLIENTNAME          --������������
                ,'9998'                                             AS SORGID              --Դ����ID
                ,'��������'                                         AS SORGNAME            --Դ��������
                ,'1'                                              AS ORGSORTNO           --�������������
                ,'9998'                                             AS ORGID               --��������ID
                ,'��������'                                         AS ORGNAME             --������������
                ,''                                                     AS INDUSTRYID          --������ҵ����
                ,''                                                     AS INDUSTRYNAME        --������ҵ����
                ,'04'                                                   AS CLIENTTYPE          --�����������
                ,'0401'                                                 AS CLIENTSUBTYPE       --��������С��   --Ĭ�� ��Ȼ��
                ,'CHN'                                                  AS COUNTRYCODE         --����/��������
                ,'01'                                                   AS REGISTSTATE         --ע����һ����
                ,''                                                     AS RCERATING           --����ע����ⲿ����
                ,''                                                     AS RCERAGENCY          --����ע����ⲿ��������
                ,''                                                     AS ORGANIZATIONCODE    --��֯��������
                ,'0'                                                    AS CONSOLIDATEDSCFLAG  --�Ƿ񲢱��ӹ�˾
                ,''                                                     AS SLCLIENTFLAG        --רҵ����ͻ���ʶ
                ,''                                                     AS SLCLIENTTYPE        --רҵ����ͻ�����
                ,''                                                     AS EXPOCATEGORYIRB     --��������¶���
                ,'0'                                                    AS DEFAULTFLAG         --ΥԼ��ʶ
                ,''                                                     AS MODELIRATING        --ģ���ڲ�����
                ,''                                                     AS MODELPD             --ģ��ΥԼ����
                ,''                                                     AS IRATING             --�ڲ�����
                ,''                                                     AS PD                  --ΥԼ����
                ,''                                                     AS CLIENTERATING       --���������ⲿ����
                ,'0'                                                    AS CCPFLAG             --���뽻�׶��ֱ�ʶ
                ,'0'                                                    AS QUALCCPFLAG         --�Ƿ�ϸ����뽻�׶���
                ,'0'                                                    AS CLEARMEMBERFLAG     --�����Ա��ʶ
                ,'0'                                                    AS MSMBFLAG            --���Ų�΢С��ҵ��ʶ
                ,'0'                                                    AS SSMBFLAG            --��׼С΢��ҵ��ʶ
                ,'0'                                                    AS SSMBFLAGSTD         --Ȩ�ط���׼С΢��ҵ��ʶ
                ,NULL                                                   AS ANNUALSALE          --��˾�ͻ������۶�
                ,''                                                     AS MODELID             --ģ��ID
                ,'0'                                                    AS NEWDEFAULTFLAG      --����ΥԼ��ʶ
                ,''                                                     AS DEFAULTDATE         --ΥԼʱ��
                ,''                                                     AS COMPANYSIZE         --��ҵ��ģ
    FROM RWA_XYK_EXPOSURE T1
    WHERE NOT EXISTS (SELECT 1 FROM RWA_EI_CLIENT T6 WHERE T1.CLIENTID = T6.CLIENTID AND T6.DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD'))
    ;

    COMMIT;


     --2.2 ���ݼ��пͻ���������÷��ձ�¶(��ծ�ʲ���¼��Ϣ����ծ�ʲ��ͻ����ﵥ������)�Ĳ���������Ϣ
    INSERT INTO RWA_DEV.RWA_EI_CLIENT(
                 DATADATE                                 --��������
                ,DATANO                                  --������ˮ��
                ,CLIENTID                                --��������ID
                ,SOURCECLIENTID                          --Դ��������ID
                ,SSYSID                                  --ԴϵͳID
                ,CLIENTNAME                              --������������
                ,SORGID                                  --Դ����ID
                ,SORGNAME                                --Դ��������
                ,ORGSORTNO                               --�������������
                ,ORGID                                   --��������ID
                ,ORGNAME                                 --������������
                ,INDUSTRYID                              --������ҵ����
                ,INDUSTRYNAME                            --������ҵ����
                ,CLIENTTYPE                              --�����������
                ,CLIENTSUBTYPE                           --��������С��
                ,COUNTRYCODE                             --����/��������
                ,REGISTSTATE                             --ע����һ����
                ,RCERATING                               --����ע����ⲿ����
                ,RCERAGENCY                              --����ע����ⲿ��������
                ,ORGANIZATIONCODE                        --��֯��������
                ,CONSOLIDATEDSCFLAG                      --�Ƿ񲢱��ӹ�˾
                ,SLCLIENTFLAG                            --רҵ����ͻ���ʶ
                ,SLCLIENTTYPE                            --רҵ����ͻ�����
                ,EXPOCATEGORYIRB                         --��������¶���
                ,DEFAULTFLAG                             --ΥԼ��ʶ
                ,MODELIRATING                            --ģ���ڲ�����
                ,MODELPD                                 --ģ��ΥԼ����
                ,IRATING                                 --�ڲ�����
                ,PD                                      --ΥԼ����
                ,CLIENTERATING                           --���������ⲿ����
                ,CCPFLAG                                 --���뽻�׶��ֱ�ʶ
                ,QUALCCPFLAG                             --�Ƿ�ϸ����뽻�׶���
                ,CLEARMEMBERFLAG                         --�����Ա��ʶ
                ,MSMBFLAG                                --���Ų�΢С��ҵ��ʶ
                ,SSMBFLAG                                --��׼С΢��ҵ��ʶ
                ,SSMBFLAGSTD         										 --Ȩ�ط���׼С΢��ҵ��ʶ
                ,ANNUALSALE                              --��˾�ͻ������۶�
                ,MODELID                                 --ģ��ID
                ,NEWDEFAULTFLAG                          --����ΥԼ��ʶ
                ,DEFAULTDATE                             --ΥԼʱ��
                ,COMPANYSIZE                             --��ҵ��ģ
    )
     SELECT
                TO_DATE(T1.DATANO,'YYYYMMDD')                           AS DATADATE            --��������
                ,T1.DATANO                                              AS DATANO              --������ˮ��
                ,'DZ-' || T1.GUARANTYID                                 AS CLIENTID            --��������ID
                ,T1.GUARANTYID                                          AS SOURCECLIENTID      --Դ��������ID
                ,'DZ'                                                   AS SSYSID              --ԴϵͳID
                ,T1.GUARANTYNAME                                        AS CLIENTNAME          --������������
                ,'9998'                                         		AS SORGID              --Դ����ID
                ,'��������'                                   AS SORGNAME            --Դ��������
                ,'1'                                              AS ORGSORTNO           --�������������
                ,'9998'                                         		AS ORGID               --��������ID
                ,'��������'                                   AS ORGNAME             --������������
                ,'J1622'                                               AS INDUSTRYID          --������ҵ����
                ,'δ֪'                                                 AS INDUSTRYNAME        --������ҵ����
                ,'03'                                                   AS CLIENTTYPE          --�����������
                ,'0301'                                                 AS CLIENTSUBTYPE       --��������С��
                ,'CHN'                                                  AS COUNTRYCODE         --����/��������
                ,'01'                                                   AS REGISTSTATE         --ע����һ����
                ,''                                                     AS RCERATING           --����ע����ⲿ����
                ,''                                                     AS RCERAGENCY          --����ע����ⲿ��������
                ,''                                                     AS ORGANIZATIONCODE    --��֯��������
                ,'0'                                                    AS CONSOLIDATEDSCFLAG  --�Ƿ񲢱��ӹ�˾
                ,''                                                     AS SLCLIENTFLAG        --רҵ����ͻ���ʶ
                ,''                                                     AS SLCLIENTTYPE        --רҵ����ͻ�����
                ,''                                                     AS EXPOCATEGORYIRB     --��������¶���
                ,'0'                                                    AS DEFAULTFLAG         --ΥԼ��ʶ
                ,''                                                     AS MODELIRATING        --ģ���ڲ�����
                ,''                                                     AS MODELPD             --ģ��ΥԼ����
                ,''                                                     AS IRATING             --�ڲ�����
                ,''                                                     AS PD                  --ΥԼ����
                ,''                                                     AS CLIENTERATING       --���������ⲿ����
                ,'0'                                                    AS CCPFLAG             --���뽻�׶��ֱ�ʶ
                ,'0'                                                    AS QUALCCPFLAG         --�Ƿ�ϸ����뽻�׶���
                ,'0'                                                    AS CLEARMEMBERFLAG     --�����Ա��ʶ
                ,'0'                                                    AS MSMBFLAG            --���Ų�΢С��ҵ��ʶ
                ,'0'                                                    AS SSMBFLAG            --��׼С΢��ҵ��ʶ
                ,'0'                                                    AS SSMBFLAGSTD         --Ȩ�ط���׼С΢��ҵ��ʶ
                ,NULL                                                   AS ANNUALSALE          --��˾�ͻ������۶�
                ,''                                                     AS MODELID             --ģ��ID
                ,'0'                                                    AS NEWDEFAULTFLAG      --����ΥԼ��ʶ
                ,''                                                     AS DEFAULTDATE         --ΥԼʱ��
                ,'01'                                                   AS COMPANYSIZE         --��ҵ��ģ Ĭ��������ҵ
    FROM 				RWA_DEV.NCM_ASSET_DEBT_INFO T1
    LEFT JOIN 	RWA.ORG_INFO T2
    ON 					T1.MANAGEORGID=T2.ORGID
    WHERE  			T1.DATANO=P_DATA_DT_STR
    ;

    COMMIT;

     --2.4 ���ݼ��пͻ���������÷��ձ�֤�Ĳ���������Ϣ
    INSERT INTO RWA_DEV.RWA_EI_CLIENT(
                 DATADATE                                 --��������
                ,DATANO                                  --������ˮ��
                ,CLIENTID                                --��������ID
                ,SOURCECLIENTID                          --Դ��������ID
                ,SSYSID                                  --ԴϵͳID
                ,CLIENTNAME                              --������������
                ,SORGID                                  --Դ����ID
                ,SORGNAME                                --Դ��������
                ,ORGSORTNO                               --�������������
                ,ORGID                                   --��������ID
                ,ORGNAME                                 --������������
                ,INDUSTRYID                              --������ҵ����
                ,INDUSTRYNAME                            --������ҵ����
                ,CLIENTTYPE                              --�����������
                ,CLIENTSUBTYPE                           --��������С��
                ,COUNTRYCODE                             --����/��������
                ,REGISTSTATE                             --ע����һ����
                ,RCERATING                               --����ע����ⲿ����
                ,RCERAGENCY                              --����ע����ⲿ��������
                ,ORGANIZATIONCODE                        --��֯��������
                ,CONSOLIDATEDSCFLAG                      --�Ƿ񲢱��ӹ�˾
                ,SLCLIENTFLAG                            --רҵ����ͻ���ʶ
                ,SLCLIENTTYPE                            --רҵ����ͻ�����
                ,EXPOCATEGORYIRB                         --��������¶���
                ,DEFAULTFLAG                             --ΥԼ��ʶ
                ,MODELIRATING                            --ģ���ڲ�����
                ,MODELPD                                 --ģ��ΥԼ����
                ,IRATING                                 --�ڲ�����
                ,PD                                      --ΥԼ����
                ,CLIENTERATING                           --���������ⲿ����
                ,CCPFLAG                                 --���뽻�׶��ֱ�ʶ
                ,QUALCCPFLAG                             --�Ƿ�ϸ����뽻�׶���
                ,CLEARMEMBERFLAG                         --�����Ա��ʶ
                ,MSMBFLAG                                --���Ų�΢С��ҵ��ʶ
                ,SSMBFLAG                                --��׼С΢��ҵ��ʶ
                ,SSMBFLAGSTD         										 --Ȩ�ط���׼С΢��ҵ��ʶ
                ,ANNUALSALE                              --��˾�ͻ������۶�
                ,MODELID                                 --ģ��ID
                ,NEWDEFAULTFLAG                          --����ΥԼ��ʶ
                ,DEFAULTDATE                             --ΥԼʱ��
                ,COMPANYSIZE                             --��ҵ��ģ
    )
    WITH TMP_CUST_ERATING AS (
							    SELECT CUSTOMERID
							    			,CLIENTERATING
							  		FROM (SELECT T1.CUSTOMERID 	AS CUSTOMERID,
							                    T1.EVALUTEORG 	AS EVALUTEORG,
							                    T2.DESCRATING		AS CLIENTERATING,
							                    T1.EVALUTEDATE	AS EVALUTEDATE,
							                    RANK() OVER(PARTITION BY T1.CUSTOMERID ORDER BY T1.EVALUTEDATE DESC) AS RK,
							                    ROW_NUMBER() OVER(PARTITION BY T1.CUSTOMERID, T1.EVALUTEDATE ORDER BY T2.DESCRATING) AS RM,
							                    COUNT(1) OVER(PARTITION BY T1.CUSTOMERID, T1.EVALUTEDATE) AS RN
							               FROM RWA_DEV.NCM_CUSTOMER_RATING T1
							          LEFT JOIN RWA_DEV.RWA_CD_RATING_MAPPING T2
							                 ON T1.EVALUTEORG = T2.SRCRATINGORG
							                AND T1.EVALUTELEVEL = T2.SRCRATINGNAME
							                AND T2.MAPPINGTYPE = '01' --ȫ������
							                AND T2.SRCRATINGTYPE = '01' --��������
							              WHERE T1.EVALUTEDATE <= P_DATA_DT_STR
							              	AND T1.DATANO = P_DATA_DT_STR)
								 WHERE RK = 1
								   AND RM = (CASE WHEN RN = 1 THEN 1 ELSE 2 END)							--ȡ���µ�һ��������������һ����ڶ��������ȡ�ڶ��õ�����
   	)
     SELECT
                TO_DATE(T1.DATANO,'YYYYMMDD')                           AS DATADATE            --��������
                ,T1.DATANO                                              AS DATANO              --������ˮ��
                ,T1.CUSTOMERID                                          AS CLIENTID            --��������ID
                ,T1.CUSTOMERID                                          AS SOURCECLIENTID      --Դ��������ID
                ,'BZ'                                                   AS SSYSID              --ԴϵͳID
                ,T1.CUSTOMERNAME                                        AS CLIENTNAME          --������������
                ,T1.ORGID                                               AS SORGID              --Դ����ID
                ,T4.ORGNAME                                             AS SORGNAME            --Դ��������
                ,T4.SORTNO                                              AS ORGSORTNO           --�������������
                --,T1.ORGID                                               AS ORGID               --��������ID
                ,DECODE(SUBSTR(T1.ORGID,1,1),'@','01000000',T1.ORGID)
                --,T4.ORGNAME                                             AS ORGNAME             --������������
                ,NVL(T4.ORGNAME,'����')
                ,'999999'                                               AS INDUSTRYID          --������ҵ����  ��ȫ��ȷ�ϱ�֤����ҵ��û���õģ�ֱ�ӹ鵽δ֪��ҵ
                ,'δ֪'                                                 AS INDUSTRYNAME        --������ҵ����
                ,''/*SUBSTR(T1.RWACUSTOMERTYPE,1,2)*/                         AS CLIENTTYPE          --�����������
                ,''/*T1.RWACUSTOMERTYPE */                                    AS CLIENTSUBTYPE       --��������С��
                ,CASE WHEN  (T1.COUNTRYCODE IS NULL OR T1.COUNTRYCODE='OTH')
                      THEN  'CHN'
                      ELSE  T1.COUNTRYCODE
                 END                                                    AS COUNTRYCODE         --����/��������
                ,CASE WHEN (T1.COUNTRYCODE IS NULL OR T1.COUNTRYCODE='OTH' OR T1.COUNTRYCODE='CHN') THEN '01'
                      ELSE '02'
                 END                                                    AS REGISTSTATE         --ע����һ����
                ,CASE WHEN (T1.COUNTRYCODE IS NULL OR T1.COUNTRYCODE='OTH' OR T1.COUNTRYCODE='CHN') THEN ''
                      ELSE NVL(T3.RATINGRESULT,'0124')
                 END                                                    AS RCERATING           --����ע����ⲿ����
                ,CASE WHEN (T1.COUNTRYCODE IS NULL OR T1.COUNTRYCODE='OTH' OR T1.COUNTRYCODE='CHN') THEN ''
                      ELSE NVL(T3.RATINGORG,'01')
                 END                                                    AS RCERAGENCY          --����ע����ⲿ��������
                ,CASE WHEN T1.CUSTOMERTYPE='0321000001'
                      THEN ''
                      ELSE T1.CERTID
                 END                                                    AS ORGANIZATIONCODE    --��֯��������
                ,CASE WHEN T6.ORGANIZATIONCODE IS NOT NULL THEN '1'
                      ELSE '0'
                 END                                                    AS CONSOLIDATEDSCFLAG  --�Ƿ񲢱��ӹ�˾
                ,'0'                                                    AS SLCLIENTFLAG        --רҵ����ͻ���ʶ
                ,''                                                     AS SLCLIENTTYPE        --רҵ����ͻ�����
                ,''                                                     AS EXPOCATEGORYIRB     --��������¶���
                ,CASE WHEN T8.PDADJCODE='D' THEN '1'
                      ELSE '0'
                 END                                                    AS DEFAULTFLAG         --ΥԼ��ʶ
                ,T8.PDLEVEL										                          AS MODELIRATING        --ģ���ڲ�����
                ,T8.PD									                                AS MODELPD             --ģ��ΥԼ����
                ,T8.PDADJLEVEL			 							                      AS IRATING             --�ڲ�����
                ,T8.PD								                                  AS PD                  --ΥԼ����
                ,T12.CLIENTERATING                                      AS CLIENTERATING       --���������ⲿ����
                ,'0'                                                    AS CCPFLAG             --���뽻�׶��ֱ�ʶ
                ,'0'                                                    AS QUALCCPFLAG         --�Ƿ�ϸ����뽻�׶���
                ,'0'                                                    AS CLEARMEMBERFLAG     --�����Ա��ʶ
                ,CASE WHEN T1.SCOPE IN ('02','03')
                      THEN '1'
                      ELSE '0'
                 END                                                    AS MSMBFLAG            --���Ų�΢С��ҵ��ʶ
                ,NVL(T1.ISSUPERVISESTANDARSMENT,'0')                    AS SSMBFLAG            --��׼С΢��ҵ��ʶ
                ,'0'                                                    AS SSMBFLAGSTD         --Ȩ�ط���׼С΢��ҵ��ʶ
                ,T1.AVEFINANCESUM                                       AS ANNUALSALE          --��˾�ͻ������۶�
                ,T8.MODELID                                             AS MODELID             --ģ��ID
                ,DECODE(NVL(T1.NEWDEFAULTFLAG,'1'),'0','1','0')         AS NEWDEFAULTFLAG      --����ΥԼ��ʶ
                ,CASE WHEN T8.PDADJCODE = 'D' THEN TO_DATE(T8.PDVAVLIDDATE,'YYYYMMDD')
                      ELSE NULL
                 END                                         						AS DEFAULTDATE         --ΥԼʱ��
                ,CASE WHEN T1.SCOPE = '2' THEN '00'                                             --������ҵ
                      WHEN T1.SCOPE = '3' THEN '01'                                            --������ҵ
                      WHEN T1.SCOPE = '4' THEN '02'                                            --С����ҵ
                      WHEN T1.SCOPE = '5' THEN '03'                                            --΢����ҵ
                      ELSE NVL(T1.SCOPE,'01')                                                  --Ĭ��������ҵ
                 END                                                    AS COMPANYSIZE         --��ҵ��ģ
    FROM        RWA_DEV.NCM_CUSTOMER_INFO T1
    INNER JOIN   (SELECT DISTINCT GUARANTORID FROM RWA_DEV.RWA_EI_GUARANTEE WHERE DATADATE = TO_DATE(P_DATA_DT_STR, 'YYYYMMDD')) T2
    ON           T1.CUSTOMERID = T2.GUARANTORID
    LEFT JOIN   RWA.RWA_WP_COUNTRYRATING T3
    ON           T1.COUNTRYCODE = T3.COUNTRYCODE
    LEFT JOIN   RWA.ORG_INFO T4
    ON           T1.ORGID = T4.ORGID
    LEFT JOIN   RWA.CODE_LIBRARY T5
    ON           T1.INDUSTRYTYPE = T5.ITEMNO
    AND         T5.CODENO = 'IndustryType'
    LEFT JOIN   RWA.RWA_WP_SUBCOMPANY T6
    ON          T1.CERTID=T6.ORGANIZATIONCODE
    LEFT JOIN   RWA_DEV.RWA_TEMP_PDLEVEL T8
    ON          T1.CUSTOMERID=T8.CUSTID
    LEFT JOIN		TMP_CUST_ERATING T12
    ON					T1.CUSTOMERID = T12.CUSTOMERID
    WHERE       T1.DATANO = P_DATA_DT_STR
    AND NOT EXISTS (SELECT 1 FROM RWA_EI_CLIENT T6 WHERE T1.CUSTOMERID = T6.CLIENTID AND T6.DATADATE = TO_DATE(P_DATA_DT_STR, 'YYYYMMDD'))
    ;

    COMMIT;

    --2.5 ���ݼ��пͻ���������÷��յ���ѺƷ�Ĳ���������Ϣ
    INSERT INTO RWA_DEV.RWA_EI_CLIENT(
                 DATADATE                                 --��������
                ,DATANO                                  --������ˮ��
                ,CLIENTID                                --��������ID
                ,SOURCECLIENTID                          --Դ��������ID
                ,SSYSID                                  --ԴϵͳID
                ,CLIENTNAME                              --������������
                ,SORGID                                  --Դ����ID
                ,SORGNAME                                --Դ��������
                ,ORGSORTNO                               --�������������
                ,ORGID                                   --��������ID
                ,ORGNAME                                 --������������
                ,INDUSTRYID                              --������ҵ����
                ,INDUSTRYNAME                            --������ҵ����
                ,CLIENTTYPE                              --�����������
                ,CLIENTSUBTYPE                           --��������С��
                ,COUNTRYCODE                             --����/��������
                ,REGISTSTATE                             --ע����һ����
                ,RCERATING                               --����ע����ⲿ����
                ,RCERAGENCY                              --����ע����ⲿ��������
                ,ORGANIZATIONCODE                        --��֯��������
                ,CONSOLIDATEDSCFLAG                      --�Ƿ񲢱��ӹ�˾
                ,SLCLIENTFLAG                            --רҵ����ͻ���ʶ
                ,SLCLIENTTYPE                            --רҵ����ͻ�����
                ,EXPOCATEGORYIRB                         --��������¶���
                ,DEFAULTFLAG                             --ΥԼ��ʶ
                ,MODELIRATING                            --ģ���ڲ�����
                ,MODELPD                                 --ģ��ΥԼ����
                ,IRATING                                 --�ڲ�����
                ,PD                                      --ΥԼ����
                ,CLIENTERATING                           --���������ⲿ����
                ,CCPFLAG                                 --���뽻�׶��ֱ�ʶ
                ,QUALCCPFLAG                             --�Ƿ�ϸ����뽻�׶���
                ,CLEARMEMBERFLAG                         --�����Ա��ʶ
                ,MSMBFLAG                                --���Ų�΢С��ҵ��ʶ
                ,SSMBFLAG                                --��׼С΢��ҵ��ʶ
                ,SSMBFLAGSTD         										 --Ȩ�ط���׼С΢��ҵ��ʶ
                ,ANNUALSALE                              --��˾�ͻ������۶�
                ,MODELID                                 --ģ��ID
                ,NEWDEFAULTFLAG                          --����ΥԼ��ʶ
                ,DEFAULTDATE                             --ΥԼʱ��
                ,COMPANYSIZE                             --��ҵ��ģ
    )
    WITH TMP_CUST_ERATING AS (
							    SELECT CUSTOMERID
							    			,CLIENTERATING
							  		FROM (SELECT T1.CUSTOMERID 	AS CUSTOMERID,
							                    T1.EVALUTEORG 	AS EVALUTEORG,
							                    T2.DESCRATING		AS CLIENTERATING,
							                    T1.EVALUTEDATE	AS EVALUTEDATE,
							                    RANK() OVER(PARTITION BY T1.CUSTOMERID ORDER BY T1.EVALUTEDATE DESC) AS RK,
							                    ROW_NUMBER() OVER(PARTITION BY T1.CUSTOMERID, T1.EVALUTEDATE ORDER BY T2.DESCRATING) AS RM,
							                    COUNT(1) OVER(PARTITION BY T1.CUSTOMERID, T1.EVALUTEDATE) AS RN
							               FROM RWA_DEV.NCM_CUSTOMER_RATING T1
							          LEFT JOIN RWA_DEV.RWA_CD_RATING_MAPPING T2
							                 ON T1.EVALUTEORG = T2.SRCRATINGORG
							                AND T1.EVALUTELEVEL = T2.SRCRATINGNAME
							                AND T2.MAPPINGTYPE = '01' --ȫ������
							                AND T2.SRCRATINGTYPE = '01' --��������
							              WHERE T1.EVALUTEDATE <= P_DATA_DT_STR
							              	AND T1.DATANO = P_DATA_DT_STR)
								 WHERE RK = 1
								   AND RM = (CASE WHEN RN = 1 THEN 1 ELSE 2 END)							--ȡ���µ�һ��������������һ����ڶ��������ȡ�ڶ��õ�����
   	)
     SELECT
                TO_DATE(T1.DATANO,'YYYYMMDD')                           AS DATADATE            --��������
                ,T1.DATANO                                              AS DATANO              --������ˮ��
                ,T1.CUSTOMERID                                          AS CLIENTID            --��������ID
                ,T1.CUSTOMERID                                          AS SOURCECLIENTID      --Դ��������ID
                ,'FX'                                                   AS SSYSID              --ԴϵͳID
                ,T1.CUSTOMERNAME                                        AS CLIENTNAME          --������������
                ,T1.ORGID                                               AS SORGID              --Դ����ID
                ,T4.ORGNAME                                             AS SORGNAME            --Դ��������
                ,T4.SORTNO                                              AS ORGSORTNO           --�������������
                --,T1.ORGID                                               AS ORGID               --��������ID
                ,decode(substr(T1.ORGID,1,1),'@','01000000',T1.ORGID)
                --,T4.ORGNAME                                             AS ORGNAME             --������������
                ,NVL(T4.ORGNAME,'����')
                ,T1.INDUSTRYTYPE                                        AS INDUSTRYID          --������ҵ����
                ,T5.ITEMNAME                                            AS INDUSTRYNAME        --������ҵ����
                ,''/*SUBSTR(T1.RWACUSTOMERTYPE,1,2)*/                         AS CLIENTTYPE          --�����������
                ,''/*T1.RWACUSTOMERTYPE */                                    AS CLIENTSUBTYPE       --��������С��
                ,CASE WHEN  (T1.COUNTRYCODE IS NULL OR T1.COUNTRYCODE='OTH')
                      THEN  'CHN'
                      ELSE  T1.COUNTRYCODE
                 END                                                    AS COUNTRYCODE         --����/��������
                ,CASE WHEN (T1.COUNTRYCODE IS NULL OR T1.COUNTRYCODE='OTH' OR T1.COUNTRYCODE='CHN') THEN '01'
                      ELSE '02'
                 END                                                    AS REGISTSTATE         --ע����һ����
                ,CASE WHEN (T1.COUNTRYCODE IS NULL OR T1.COUNTRYCODE='OTH' OR T1.COUNTRYCODE='CHN') THEN ''
                      ELSE NVL(T3.RATINGRESULT,'0124')
                 END                                                    AS RCERATING           --����ע����ⲿ����
                ,CASE WHEN (T1.COUNTRYCODE IS NULL OR T1.COUNTRYCODE='OTH' OR T1.COUNTRYCODE='CHN') THEN ''
                      ELSE NVL(T3.RATINGORG,'01')
                 END                                                    AS RCERAGENCY          --����ע����ⲿ��������
                ,CASE WHEN T1.CUSTOMERTYPE='0321000001'
                      THEN ''
                      ELSE T1.CERTID
                 END                                                    AS ORGANIZATIONCODE    --��֯��������
                ,CASE WHEN T6.ORGANIZATIONCODE IS NOT NULL THEN '1'
                      ELSE '0'
                 END                                                    AS CONSOLIDATEDSCFLAG  --�Ƿ񲢱��ӹ�˾
                ,''                                                     AS SLCLIENTFLAG        --רҵ����ͻ���ʶ
                ,''                                                     AS SLCLIENTTYPE        --רҵ����ͻ�����
                ,''                                                     AS EXPOCATEGORYIRB     --��������¶���
                ,CASE WHEN T8.PDADJCODE='D' THEN '1'
                      ELSE '0'
                 END                                                    AS DEFAULTFLAG         --ΥԼ��ʶ
                ,T8.PDLEVEL										                          AS MODELIRATING        --ģ���ڲ�����
                ,T8.PD								                                  AS MODELPD             --ģ��ΥԼ����
                ,T8.PDADJLEVEL										                      AS IRATING             --�ڲ�����
                ,T8.PD								                                  AS PD                  --ΥԼ����
                ,T12.CLIENTERATING                                      AS CLIENTERATING       --���������ⲿ����
                ,'0'                                                    AS CCPFLAG             --���뽻�׶��ֱ�ʶ
                ,'0'                                                    AS QUALCCPFLAG         --�Ƿ�ϸ����뽻�׶���
                ,'0'                                                    AS CLEARMEMBERFLAG     --�����Ա��ʶ
                ,CASE WHEN T1.SCOPE IN ('02','03')
                      THEN '1'
                      ELSE '0'
                 END                                                    AS MSMBFLAG            --���Ų�΢С��ҵ��ʶ
                ,NVL(T1.ISSUPERVISESTANDARSMENT,'0')                    AS SSMBFLAG            --��׼С΢��ҵ��ʶ
                ,'0'                                                    AS SSMBFLAGSTD         --Ȩ�ط���׼С΢��ҵ��ʶ
                ,NULL                                                   AS ANNUALSALE          --��˾�ͻ������۶�
                ,T8.MODELID                                      				AS MODELID             --ģ��ID
                ,DECODE(NVL(T1.NEWDEFAULTFLAG,'1'),'0','1','0')         AS NEWDEFAULTFLAG      --����ΥԼ��ʶ
                ,CASE WHEN T8.PDADJCODE = 'D' THEN TO_DATE(T8.PDVAVLIDDATE,'YYYYMMDD')
                      ELSE NULL
                 END                                         						AS DEFAULTDATE         --ΥԼʱ��
                ,CASE WHEN T1.SCOPE = '2' THEN '00'                                             --������ҵ
                      WHEN T1.SCOPE = '3' THEN '01'                                            --������ҵ
                      WHEN T1.SCOPE = '4' THEN '02'                                            --С����ҵ
                      WHEN T1.SCOPE = '5' THEN '03'                                            --΢����ҵ
                      ELSE NVL(T1.SCOPE,'01')                                                  --Ĭ��������ҵ
                 END                                                    AS COMPANYSIZE         --��ҵ��ģ
    FROM        RWA_DEV.NCM_CUSTOMER_INFO T1
    INNER JOIN   (SELECT DISTINCT ISSUERID FROM RWA_DEV.RWA_EI_COLLATERAL WHERE DATADATE = TO_DATE(P_DATA_DT_STR, 'YYYYMMDD')) T2
    ON           T1.CUSTOMERID = T2.ISSUERID
    LEFT JOIN   RWA.RWA_WP_COUNTRYRATING T3
    ON           T1.COUNTRYCODE = T3.COUNTRYCODE
    LEFT JOIN   RWA.ORG_INFO T4
    ON           T1.ORGID = T4.ORGID
    LEFT JOIN   RWA.CODE_LIBRARY T5
    ON           T1.INDUSTRYTYPE = T5.ITEMNO
    AND         T5.CODENO = 'IndustryType'
    LEFT JOIN   RWA.RWA_WP_SUBCOMPANY T6
    ON          T1.CERTID=T6.ORGANIZATIONCODE
    LEFT JOIN   RWA_DEV.RWA_TEMP_PDLEVEL T8
    ON          T1.CUSTOMERID=T8.CUSTID
    LEFT JOIN		TMP_CUST_ERATING T12
    ON					T1.CUSTOMERID = T12.CUSTOMERID
    WHERE       T1.DATANO = P_DATA_DT_STR
    AND NOT EXISTS (SELECT 1 FROM RWA_EI_CLIENT T6 WHERE T1.CUSTOMERID = T6.CLIENTID AND T6.DATADATE = TO_DATE(P_DATA_DT_STR, 'YYYYMMDD'))
    ;

    COMMIT;

     --������ƿͻ���EI�ͻ���
    INSERT INTO RWA_DEV.RWA_EI_CLIENT(
                 DATADATE                                 --��������
                ,DATANO                                  --������ˮ��
                ,CLIENTID                                --��������ID
                ,SOURCECLIENTID                          --Դ��������ID
                ,SSYSID                                  --ԴϵͳID
                ,CLIENTNAME                              --������������
                ,SORGID                                  --Դ����ID
                ,SORGNAME                                --Դ��������
                ,ORGSORTNO                               --�������������
                ,ORGID                                   --��������ID
                ,ORGNAME                                 --������������
                ,INDUSTRYID                              --������ҵ����
                ,INDUSTRYNAME                            --������ҵ����
                ,CLIENTTYPE                              --�����������
                ,CLIENTSUBTYPE                           --��������С��
                ,COUNTRYCODE                             --����/��������
                ,REGISTSTATE                             --ע����һ����
                ,RCERATING                               --����ע����ⲿ����
                ,RCERAGENCY                              --����ע����ⲿ��������
                ,ORGANIZATIONCODE                        --��֯��������
                ,CONSOLIDATEDSCFLAG                      --�Ƿ񲢱��ӹ�˾
                ,SLCLIENTFLAG                            --רҵ����ͻ���ʶ
                ,SLCLIENTTYPE                            --רҵ����ͻ�����
                ,EXPOCATEGORYIRB                         --��������¶���
                ,DEFAULTFLAG                             --ΥԼ��ʶ
                ,MODELIRATING                            --ģ���ڲ�����
                ,MODELPD                                 --ģ��ΥԼ����
                ,IRATING                                 --�ڲ�����
                ,PD                                      --ΥԼ����
                ,CLIENTERATING                           --���������ⲿ����
                ,CCPFLAG                                 --���뽻�׶��ֱ�ʶ
                ,QUALCCPFLAG                             --�Ƿ�ϸ����뽻�׶���
                ,CLEARMEMBERFLAG                         --�����Ա��ʶ
                ,MSMBFLAG                                --���Ų�΢С��ҵ��ʶ
                ,SSMBFLAG                                --��׼С΢��ҵ��ʶ
                ,SSMBFLAGSTD         										 --Ȩ�ط���׼С΢��ҵ��ʶ
                ,ANNUALSALE                              --��˾�ͻ������۶�
                ,MODELID                                 --ģ��ID
                ,NEWDEFAULTFLAG                          --����ΥԼ��ʶ
                ,DEFAULTDATE                             --ΥԼʱ��
                ,COMPANYSIZE                             --��ҵ��ģ
    )
    SELECT
                 T1.DATADATE                                 --��������
                ,T1.DATANO                                  --������ˮ��
                ,T1.CLIENTID                                --��������ID
                ,T1.SOURCECLIENTID                          --Դ��������ID
                ,T1.SSYSID                                  --ԴϵͳID
                ,T1.CLIENTNAME                              --������������
                ,T1.SORGID                                  --Դ����ID
                ,T1.SORGNAME                                --Դ��������
                ,T1.ORGSORTNO                               --�������������
                ,T1.ORGID                                   --��������ID
                ,T1.ORGNAME                                 --������������
                ,T1.INDUSTRYID                              --������ҵ����
                ,T1.INDUSTRYNAME                            --������ҵ����
                ,T1.CLIENTTYPE                              --�����������
                ,T1.CLIENTSUBTYPE                           --��������С��
                ,T1.COUNTRYCODE                             --����/��������
                ,T1.REGISTSTATE                             --ע����һ����
                ,T1.RCERATING                               --����ע����ⲿ����
                ,T1.RCERAGENCY                              --����ע����ⲿ��������
                ,T1.ORGANIZATIONCODE                        --��֯��������
                ,T1.CONSOLIDATEDSCFLAG                      --�Ƿ񲢱��ӹ�˾
                ,T1.SLCLIENTFLAG                            --רҵ����ͻ���ʶ
                ,T1.SLCLIENTTYPE                            --רҵ����ͻ�����
                ,T1.EXPOCATEGORYIRB                         --��������¶���
                ,T1.DEFAULTFLAG                             --ΥԼ��ʶ
                ,T1.MODELIRATING                            --ģ���ڲ�����
                ,T1.MODELPD                                 --ģ��ΥԼ����
                ,T1.IRATING                                 --�ڲ�����
                ,T1.PD                                      --ΥԼ����
                ,T1.CLIENTERATING                           --���������ⲿ����
                ,T1.CCPFLAG                                 --���뽻�׶��ֱ�ʶ
                ,T1.QUALCCPFLAG                             --�Ƿ�ϸ����뽻�׶���
                ,T1.CLEARMEMBERFLAG                         --�����Ա��ʶ
                ,T1.MSMBFLAG                                --���Ų�΢С��ҵ��ʶ
                ,T1.SSMBFLAG                                --��׼С΢��ҵ��ʶ
                ,T1.SSMBFLAGSTD                             --Ȩ�ط���׼С΢��ҵ��ʶ
                ,T1.ANNUALSALE                              --��˾�ͻ������۶�
                ,T1.MODELID                                 --ģ��ID
                ,T1.NEWDEFAULTFLAG                          --����ΥԼ��ʶ
                ,T1.DEFAULTDATE                             --ΥԼʱ��
                ,NVL(T1.COMPANYSIZE,'01')                   --��ҵ��ģ
    FROM RWA_DEV.RWA_LC_CLIENT T1
    WHERE       T1.DATANO = P_DATA_DT_STR
    AND NOT EXISTS (SELECT 1 FROM RWA_EI_CLIENT T6 WHERE T1.CLIENTID = T6.CLIENTID AND T6.DATADATE = TO_DATE(P_DATA_DT_STR, 'YYYYMMDD'))
    ;

    COMMIT;

    --�����г����ղ���ծȯͶ�ʷ����˵�EI�ͻ���
    INSERT INTO RWA_DEV.RWA_EI_CLIENT(
                 DATADATE                                --��������
                ,DATANO                                  --������ˮ��
                ,CLIENTID                                --��������ID
                ,SOURCECLIENTID                          --Դ��������ID
                ,SSYSID                                  --ԴϵͳID
                ,CLIENTNAME                              --������������
                ,SORGID                                  --Դ����ID
                ,SORGNAME                                --Դ��������
                ,ORGSORTNO                               --�������������
                ,ORGID                                   --��������ID
                ,ORGNAME                                 --������������
                ,INDUSTRYID                              --������ҵ����
                ,INDUSTRYNAME                            --������ҵ����
                ,CLIENTTYPE                              --�����������
                ,CLIENTSUBTYPE                           --��������С��
                ,COUNTRYCODE                             --����/��������
                ,REGISTSTATE                             --ע����һ����
                ,RCERATING                               --����ע����ⲿ����
                ,RCERAGENCY                              --����ע����ⲿ��������
                ,ORGANIZATIONCODE                        --��֯��������
                ,CONSOLIDATEDSCFLAG                      --�Ƿ񲢱��ӹ�˾
                ,SLCLIENTFLAG                            --רҵ����ͻ���ʶ
                ,SLCLIENTTYPE                            --רҵ����ͻ�����
                ,EXPOCATEGORYIRB                         --��������¶���
                ,DEFAULTFLAG                             --ΥԼ��ʶ
                ,MODELIRATING                            --ģ���ڲ�����
                ,MODELPD                                 --ģ��ΥԼ����
                ,IRATING                                 --�ڲ�����
                ,PD                                      --ΥԼ����
                ,CLIENTERATING                           --���������ⲿ����
                ,CCPFLAG                                 --���뽻�׶��ֱ�ʶ
                ,QUALCCPFLAG                             --�Ƿ�ϸ����뽻�׶���
                ,CLEARMEMBERFLAG                         --�����Ա��ʶ
                ,MSMBFLAG                                --���Ų�΢С��ҵ��ʶ
                ,SSMBFLAG                                --��׼С΢��ҵ��ʶ
                ,SSMBFLAGSTD         										 --Ȩ�ط���׼С΢��ҵ��ʶ
                ,ANNUALSALE                              --��˾�ͻ������۶�
                ,MODELID                                 --ģ��ID
                ,NEWDEFAULTFLAG                          --����ΥԼ��ʶ
                ,DEFAULTDATE                             --ΥԼʱ��
                ,COMPANYSIZE                             --��ҵ��ģ
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
												         WHERE AS_OF_DATE <= P_DATA_DT_STR
												           AND DATANO = P_DATA_DT_STR)
												 WHERE RM = 1
												   AND NVL(INITIAL_COST, 0) + NVL(INT_ADJUST, 0) + NVL(MKT_VALUE_CHANGE, 0) + NVL(ACCOUNTABLE_INT, 0) <> 0
		)
		,TMP_CUST_ERATING AS (
							    SELECT CUSTOMERID
							    			,CLIENTERATING
							  		FROM (SELECT T1.CUSTOMERID 	AS CUSTOMERID,
							                    T1.EVALUTEORG 	AS EVALUTEORG,
							                    T2.DESCRATING		AS CLIENTERATING,
							                    T1.EVALUTEDATE	AS EVALUTEDATE,
							                    RANK() OVER(PARTITION BY T1.CUSTOMERID ORDER BY T1.EVALUTEDATE DESC) AS RK,
							                    ROW_NUMBER() OVER(PARTITION BY T1.CUSTOMERID, T1.EVALUTEDATE ORDER BY T2.DESCRATING) AS RM,
							                    COUNT(1) OVER(PARTITION BY T1.CUSTOMERID, T1.EVALUTEDATE) AS RN
							               FROM RWA_DEV.NCM_CUSTOMER_RATING T1
							          LEFT JOIN RWA_DEV.RWA_CD_RATING_MAPPING T2
							                 ON T1.EVALUTEORG = T2.SRCRATINGORG
							                AND T1.EVALUTELEVEL = T2.SRCRATINGNAME
							                AND T2.MAPPINGTYPE = '01' --ȫ������
							                AND T2.SRCRATINGTYPE = '01' --��������
							              WHERE T1.EVALUTEDATE <= P_DATA_DT_STR
							              	AND T1.DATANO = P_DATA_DT_STR)
								 WHERE RK = 1
								   AND RM = (CASE WHEN RN = 1 THEN 1 ELSE 2 END)							--ȡ���µ�һ��������������һ����ڶ��������ȡ�ڶ��õ�����
   	)
    SELECT
                 TO_DATE(p_data_dt_str,'YYYYMMDD')          --��������
                ,p_data_dt_str                              --������ˮ��
                ,T1.CUSTOMERID                                          AS CLIENTID            --��������ID
                ,T1.CUSTOMERID                                          AS SOURCECLIENTID      --Դ��������ID
                ,'TZZQ'                                                 AS SSYSID              --ԴϵͳID
                ,T1.CUSTOMERNAME                                        AS CLIENTNAME          --������������
                ,T1.ORGID                                               AS SORGID              --Դ����ID
                ,T8.ORGNAME                                             AS SORGNAME            --Դ��������
                ,T8.SORTNO                                              AS ORGSORTNO           --�������������
                --,T1.ORGID                                               AS ORGID               --��������ID
                ,DECODE(SUBSTR(T1.ORGID,1,1),'@','01000000',T1.ORGID)
                --,T8.ORGNAME                                             AS ORGNAME             --������������
                ,nvl(T8.ORGNAME,'����')
                ,T1.INDUSTRYTYPE                                        AS INDUSTRYID          --������ҵ����
                ,T9.ITEMNAME                                            AS INDUSTRYNAME        --������ҵ����
                ,''/*SUBSTR(T1.RWACUSTOMERTYPE,1,2)*/                         AS CLIENTTYPE          --�����������
                ,''/*T1.RWACUSTOMERTYPE */                                    AS CLIENTSUBTYPE       --��������С��
                ,CASE WHEN  (T1.COUNTRYCODE IS NULL OR T1.COUNTRYCODE='OTH')
                      THEN  'CHN'
                      ELSE  T1.COUNTRYCODE
                 END                                                    AS COUNTRYCODE         --����/��������
                ,CASE WHEN (T1.COUNTRYCODE IS NULL OR T1.COUNTRYCODE='OTH' OR T1.COUNTRYCODE='CHN') THEN '01'
                      ELSE '02'
                 END                                                    AS REGISTSTATE         --ע����һ����
                ,CASE WHEN (T1.COUNTRYCODE IS NULL OR T1.COUNTRYCODE='OTH' OR T1.COUNTRYCODE='CHN') THEN ''
                      ELSE NVL(T7.RATINGRESULT,'0124')
                 END                                                    AS RCERATING           --����ע����ⲿ����
                ,CASE WHEN (T1.COUNTRYCODE IS NULL OR T1.COUNTRYCODE='OTH' OR T1.COUNTRYCODE='CHN') THEN ''
                      ELSE NVL(T7.RATINGORG,'01')
                 END                                                    AS RCERAGENCY          --����ע����ⲿ��������
                ,CASE WHEN T1.CUSTOMERTYPE='0321000001'
                      THEN ''
                      ELSE T1.CERTID
                 END                                                    AS ORGANIZATIONCODE    --��֯��������
                ,CASE WHEN T10.ORGANIZATIONCODE IS NOT NULL THEN '1'
                      ELSE '0'
                 END                                                    AS CONSOLIDATEDSCFLAG  --�Ƿ񲢱��ӹ�˾
                ,'0'                                                    AS SLCLIENTFLAG        --רҵ����ͻ���ʶ
                ,''                                                     AS SLCLIENTTYPE        --רҵ����ͻ�����
                ,''                                                     AS EXPOCATEGORYIRB     --��������¶���
                ,CASE WHEN T11.PDADJCODE='D' THEN '1'
                      ELSE '0'
                 END                                                    AS DEFAULTFLAG         --ΥԼ��ʶ
                ,T11.PDLEVEL										                        AS MODELIRATING        --ģ���ڲ�����
                ,T11.PD								                                  AS MODELPD             --ģ��ΥԼ����
                ,T11.PDADJLEVEL										                      AS IRATING             --�ڲ�����
                ,T11.PD								                                  AS PD                  --ΥԼ����
                ,T12.CLIENTERATING                                      AS CLIENTERATING       --���������ⲿ����
                ,'0'                                                    AS CCPFLAG             --���뽻�׶��ֱ�ʶ
                ,'0'                                                    AS QUALCCPFLAG         --�Ƿ�ϸ����뽻�׶���
                ,'0'                                                    AS CLEARMEMBERFLAG     --�����Ա��ʶ
                ,CASE WHEN T1.SCOPE IN ('02','03')
                      THEN '1'
                      ELSE '0'
                 END                                                    AS MSMBFLAG            --���Ų�΢С��ҵ��ʶ
                ,NVL(T1.ISSUPERVISESTANDARSMENT,'0')                    AS SSMBFLAG            --��׼С΢��ҵ��ʶ
                ,'0'                                                    AS SSMBFLAGSTD         --Ȩ�ط���׼С΢��ҵ��ʶ
                ,T1.AVEFINANCESUM                                       AS ANNUALSALE          --��˾�ͻ������۶�
                ,T11.MODELID                                            AS MODELID             --ģ��ID
                ,DECODE(NVL(T1.NEWDEFAULTFLAG,'1'),'0','1','0')         AS NEWDEFAULTFLAG      --����ΥԼ��ʶ
                ,CASE WHEN T11.PDADJCODE = 'D' THEN TO_DATE(T11.PDVAVLIDDATE,'YYYYMMDD')
                      ELSE NULL
                 END                                         						AS DEFAULTDATE         --ΥԼʱ��
                ,CASE WHEN T1.SCOPE = '2' THEN '00'                                             --������ҵ
                      WHEN T1.SCOPE = '3' THEN '01'                                            --������ҵ
                      WHEN T1.SCOPE = '4' THEN '02'                                            --С����ҵ
                      WHEN T1.SCOPE = '5' THEN '03'                                            --΢����ҵ
                      ELSE NVL(T1.SCOPE,'01')                                                  --Ĭ��������ҵ
                 END                                                    AS COMPANYSIZE         --��ҵ��ģ
    FROM        NCM_CUSTOMER_INFO T1
    INNER JOIN  (SELECT DISTINCT BONDPUBLISHID
                 FROM        RWA_DEV.NCM_BOND_INFO T2
                 INNER JOIN  RWA_DEV.NCM_BUSINESS_DUEBILL T3
                 ON          T2.OBJECTNO=T3.RELATIVESERIALNO2
                 AND         T3.DATANO =P_DATA_DT_STR
                 INNER JOIN  TEMP_BND_BOOK T4
                 ON          T3.THIRDPARTYACCOUNTS='CW_IMPORTDATA' || T4.BOND_ID
                 INNER JOIN  RWA_DEV.FNS_BND_INFO_B T5
                 ON          T4.BOND_ID=T5.BOND_ID
                 AND         T5.ASSET_CLASS = '10'                                        --���������˻������г�����
                 AND         T5.DATANO =P_DATA_DT_STR
                 WHERE       T2.OBJECTTYPE = 'BusinessContract'
                 AND         T2.DATANO = P_DATA_DT_STR
                 ) T2
    ON T1.CUSTOMERID=T2.BONDPUBLISHID
    LEFT JOIN   RWA.RWA_WP_COUNTRYRATING T7
    ON          T1.COUNTRYCODE = T7.COUNTRYCODE
    LEFT JOIN   RWA.ORG_INFO T8
    ON          T1.ORGID = T8.ORGID
    LEFT JOIN   RWA.CODE_LIBRARY T9
    ON          T1.INDUSTRYTYPE = T9.ITEMNO
    AND         T9.CODENO = 'IndustryType'
    LEFT JOIN   RWA.RWA_WP_SUBCOMPANY T10
    ON          T1.CERTID = T10.ORGANIZATIONCODE
    LEFT JOIN   RWA_DEV.RWA_TEMP_PDLEVEL T11
    ON          T1.CUSTOMERID = T11.CUSTID
    LEFT JOIN		TMP_CUST_ERATING T12
    ON					T1.CUSTOMERID = T12.CUSTOMERID
    WHERE       T1.DATANO = P_DATA_DT_STR
    AND NOT EXISTS (SELECT 1 FROM RWA_EI_CLIENT T6 WHERE T1.CUSTOMERID = T6.CLIENTID AND T6.DATADATE = TO_DATE(P_DATA_DT_STR, 'YYYYMMDD'))
    ;

    COMMIT;



 --ͬҵ�ͻ���Ϣ+����  OPICS 
INSERT INTO RWA_DEV.RWA_EI_CLIENT(
                DATADATE                           --��������
                ,DATANO                                  --������ˮ��
                ,CLIENTID                                --��������ID
                ,SOURCECLIENTID                          --Դ��������ID
                ,SSYSID                                  --ԴϵͳID
                ,CLIENTNAME                              --������������
                ,SORGID                                  --Դ����ID
                ,SORGNAME                                --Դ��������
                ,ORGSORTNO                               --�������������
                ,ORGID                                   --��������ID
                ,ORGNAME                                 --������������
                ,INDUSTRYID                              --������ҵ����
                ,INDUSTRYNAME                            --������ҵ����
                ,CLIENTTYPE                              --�����������
                ,CLIENTSUBTYPE                           --��������С��
                ,COUNTRYCODE                             --����/��������
                ,REGISTSTATE                             --ע����һ����
                ,RCERATING                               --����ע����ⲿ����
                ,RCERAGENCY                              --����ע����ⲿ��������
                ,ORGANIZATIONCODE                        --��֯��������
                ,CONSOLIDATEDSCFLAG                      --�Ƿ񲢱��ӹ�˾
                ,SLCLIENTFLAG                            --רҵ����ͻ���ʶ
                ,SLCLIENTTYPE                            --רҵ����ͻ�����
                ,EXPOCATEGORYIRB                         --��������¶���
                ,DEFAULTFLAG                             --ΥԼ��ʶ
                ,MODELIRATING                            --ģ���ڲ�����
                ,MODELPD                                 --ģ��ΥԼ����
                ,IRATING                                 --�ڲ�����
                ,PD                                      --ΥԼ����
                ,CLIENTERATING                           --���������ⲿ����
                ,CCPFLAG                                 --���뽻�׶��ֱ�ʶ
                ,QUALCCPFLAG                             --�Ƿ�ϸ����뽻�׶���
                ,CLEARMEMBERFLAG                         --�����Ա��ʶ
                ,MSMBFLAG                                --���Ų�΢С��ҵ��ʶ
                ,SSMBFLAG                                --��׼С΢��ҵ��ʶ
                ,SSMBFLAGSTD                              --Ȩ�ط���׼С΢��ҵ��ʶ
                ,ANNUALSALE                              --��˾�ͻ������۶�
                ,MODELID                                 --ģ��ID
                ,NEWDEFAULTFLAG                          --����ΥԼ��ʶ
                ,DEFAULTDATE                             --ΥԼʱ��
                ,COMPANYSIZE                             --��ҵ��ģ
     )
     SELECT
                 TO_DATE(T1.DATANO,'YYYYMMDD')                              --��������
                ,T1.DATANO                                  --������ˮ��
                ,'OPI'||TRIM(T1.CNO)                   --��������ID
                ,TRIM(T1.CNO)                          --Դ��������ID
                ,'TY'                                  --ԴϵͳID
                ,T1.CFN1                             --������������
                ,'9998'                                  --Դ����ID
                ,'��������'                                --Դ��������
                ,'1'                               --�������������
                ,'9998'                                   --��������ID
                ,'��������'                                 --������������
                ,'J6621'                              --������ҵ����
                ,''                            --������ҵ����
                ,CASE 
                 WHEN TRIM(T1.ACCTNGTYPE) = 'D-CTRL-BNK' THEN '01'
                      ELSE '02' 
                 END                            --�����������
                ,CASE 
                   WHEN TRIM(T1.ACCTNGTYPE) IN('D-CCOM-BNK','D-ECOM-BNK','D-FORE-BNK','D-RCOM-BNK','D-T4SO-BNK') THEN '0202'
                   WHEN TRIM(T1.ACCTNGTYPE) IN('O-CHIN-BNK','O-FORE-BNK') THEN '0206'
                   WHEN TRIM(T1.ACCTNGTYPE) ='D-CTRL-BNK' THEN '0103' 
                   WHEN TRIM(T1.ACCTNGTYPE) ='OW-D-INS' THEN '0205'   
                   WHEN TRIM(T1.ACCTNGTYPE) ='DEFAULT' THEN '0206'
                   WHEN TRIM(T1.ACCTNGTYPE) ='D-POLY-BNK' THEN '0201'
                   ELSE ''
                 END               --��������С��
                ,T2.COUNTRYCODE                            --����/��������
                ,DECODE(T1.CCODE,'CN','01','02')                            --ע����һ����
                ,NVL(T2.RATINGRESULT,'0102')                                                      --����ע����ⲿ����
                ,'01'                              --����ע����ⲿ��������
                ,''                         --��֯��������
                ,'0'                       --�Ƿ񲢱��ӹ�˾
                ,'0'                            --רҵ����ͻ���ʶ
                ,''                             --רҵ����ͻ�����
                ,'020201'                          --��������¶���
                ,'0'                             --ΥԼ��ʶ
                ,''                             --ģ���ڲ�����
                ,''                                 --ģ��ΥԼ����
                ,''                                  --�ڲ�����
                ,''                                      --ΥԼ����
                ,NVL(T2.RATINGRESULT,'0102')          --���������ⲿ����
                ,'0'                                  --���뽻�׶��ֱ�ʶ
                ,'0'                             --�Ƿ�ϸ����뽻�׶���
                ,'0'                         --�����Ա��ʶ
                ,''                                --���Ų�΢С��ҵ��ʶ
                ,''                                --��׼С΢��ҵ��ʶ
                ,''                              --Ȩ�ط���׼С΢��ҵ��ʶ
                ,''                               --��˾�ͻ������۶�
                ,''                                  --ģ��ID
                ,'0'                           --����ΥԼ��ʶ
                ,''                              --ΥԼʱ��
                ,''                                          --��ҵ��ģ Ĭ��������ҵ
    FROM RWA_DEV.OPI_CUST T1
    LEFT JOIN RWA.RWA_WP_COUNTRYRATING T2
    ON T1.CCODE=T2.OPICODE
    WHERE T1.DATANO=p_data_dt_str;
    
    COMMIT;


     /*   
     ���ѽ��ڿͻ���Ϣ�����Ŵ��ͻ�ʱ������
     
     --�������ѽ��ڸ��˿ͻ���Ϣ��EI�ͻ���
    INSERT INTO RWA_DEV.RWA_EI_CLIENT(
                 DATADATE                                --��������
                ,DATANO                                  --������ˮ��
                ,CLIENTID                                --��������ID
                ,SOURCECLIENTID                          --Դ��������ID
                ,SSYSID                                  --ԴϵͳID
                ,CLIENTNAME                              --������������
                ,SORGID                                  --Դ����ID
                ,SORGNAME                                --Դ��������
                ,ORGSORTNO                               --�������������
                ,ORGID                                   --��������ID
                ,ORGNAME                                 --������������
                ,INDUSTRYID                              --������ҵ����
                ,INDUSTRYNAME                            --������ҵ����
                ,CLIENTTYPE                              --�����������
                ,CLIENTSUBTYPE                           --��������С��
                ,COUNTRYCODE                             --����/��������
                ,REGISTSTATE                             --ע����һ����
                ,RCERATING                               --����ע����ⲿ����
                ,RCERAGENCY                              --����ע����ⲿ��������
                ,ORGANIZATIONCODE                        --��֯��������
                ,CONSOLIDATEDSCFLAG                      --�Ƿ񲢱��ӹ�˾
                ,SLCLIENTFLAG                            --רҵ����ͻ���ʶ
                ,SLCLIENTTYPE                            --רҵ����ͻ�����
                ,EXPOCATEGORYIRB                         --��������¶���
                ,DEFAULTFLAG                             --ΥԼ��ʶ
                ,MODELIRATING                            --ģ���ڲ�����
                ,MODELPD                                 --ģ��ΥԼ����
                ,IRATING                                 --�ڲ�����
                ,PD                                      --ΥԼ����
                ,CLIENTERATING                           --���������ⲿ����
                ,CCPFLAG                                 --���뽻�׶��ֱ�ʶ
                ,QUALCCPFLAG                             --�Ƿ�ϸ����뽻�׶���
                ,CLEARMEMBERFLAG                         --�����Ա��ʶ
                ,MSMBFLAG                                --���Ų�΢С��ҵ��ʶ
                ,SSMBFLAG                                --��׼С΢��ҵ��ʶ
                ,SSMBFLAGSTD         										 --Ȩ�ط���׼С΢��ҵ��ʶ
                ,ANNUALSALE                              --��˾�ͻ������۶�
                ,MODELID                                 --ģ��ID
                ,NEWDEFAULTFLAG                          --����ΥԼ��ʶ
                ,DEFAULTDATE                             --ΥԼʱ��
                ,COMPANYSIZE                             --��ҵ��ģ
    )
    SELECT
                 DATADATE                                --��������
                ,DATANO                                  --������ˮ��
                ,CLIENTID                                --��������ID
                ,SOURCECLIENTID                          --Դ��������ID
                ,SSYSID                                  --ԴϵͳID
                ,CLIENTNAME                              --������������
                ,SORGID                                  --Դ����ID
                ,SORGNAME                                --Դ��������
                ,ORGSORTNO                               --�������������
                ,ORGID                                   --��������ID
                ,ORGNAME                                 --������������
                ,INDUSTRYID                              --������ҵ����
                ,INDUSTRYNAME                            --������ҵ����
                ,CLIENTTYPE                              --�����������
                ,CLIENTSUBTYPE                           --��������С��
                ,COUNTRYCODE                             --����/��������
                ,REGISTSTATE                             --ע����һ����
                ,RCERATING                               --����ע����ⲿ����
                ,RCERAGENCY                              --����ע����ⲿ��������
                ,ORGANIZATIONCODE                        --��֯��������
                ,CONSOLIDATEDSCFLAG                      --�Ƿ񲢱��ӹ�˾
                ,SLCLIENTFLAG                            --רҵ����ͻ���ʶ
                ,SLCLIENTTYPE                            --רҵ����ͻ�����
                ,EXPOCATEGORYIRB                         --��������¶���
                ,DEFAULTFLAG                             --ΥԼ��ʶ
                ,MODELIRATING                            --ģ���ڲ�����
                ,MODELPD                                 --ģ��ΥԼ����
                ,IRATING                                 --�ڲ�����
                ,PD                                      --ΥԼ����
                ,CLIENTERATING                           --���������ⲿ����
                ,CCPFLAG                                 --���뽻�׶��ֱ�ʶ
                ,QUALCCPFLAG                             --�Ƿ�ϸ����뽻�׶���
                ,CLEARMEMBERFLAG                         --�����Ա��ʶ
                ,MSMBFLAG                                --���Ų�΢С��ҵ��ʶ
                ,SSMBFLAG                                --��׼С΢��ҵ��ʶ
                ,SSMBFLAGSTD         										 --Ȩ�ط���׼С΢��ҵ��ʶ
                ,ANNUALSALE                              --��˾�ͻ������۶�
                ,MODELID                                 --ģ��ID
                ,NEWDEFAULTFLAG                          --����ΥԼ��ʶ
                ,DEFAULTDATE                             --ΥԼʱ��
                ,COMPANYSIZE                             --��ҵ��ģ
    FROM RWA_DEV.RWA_XF_CLIENT C
    WHERE C.DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD');
 
    COMMIT;*/
    
    
    ----------20191120  by wzb ----
    
       update rwa_ei_client
   set ORGID='9998',SORGID='9998',ORGNAME='���⴦��'
   where  ORGID IS NULL AND  datano=p_data_dt_str;
  COMMIT;
    
 
    --�������Ϣ
    --dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_EI_CLIENT',cascade => true);
    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_EI_CLIENT',partname => 'CLIENT'||p_data_dt_str,granularity => 'PARTITION',cascade => true);

    /*Ŀ�������ͳ��*/
    --ͳ�Ʋ���ļ�¼
    SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_EI_CLIENT WHERE DATANO = p_data_dt_str;
    --Dbms_output.Put_line('RWA_DEV.RWA_EI_CLIENT��ǰ��������ݼ�¼Ϊ:' || v_count || '��');

    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̽��� ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    p_po_rtncode := '1';
    p_po_rtnmsg  := '�ɹ�'||'-'||v_count;
    --�����쳣
    EXCEPTION
    WHEN OTHERS THEN
         --Dbms_output.Put_line('������,�������Ϊ:'||sqlcode||';������ϢΪ:'||sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace);
          ROLLBACK;
          p_po_rtncode := sqlcode;
          p_po_rtnmsg  := '���ܲ��������('|| v_pro_name ||')ETLת��ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_EI_CLIENT;
/

