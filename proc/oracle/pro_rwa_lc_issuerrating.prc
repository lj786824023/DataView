CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_LC_ISSUERRATING(
														p_data_dt_str IN  VARCHAR2, --��������
                            p_po_rtncode  OUT VARCHAR2, --���ر��
                            p_po_rtnmsg   OUT VARCHAR2  --��������
)
  /*
    �洢��������:RWA_DEV.PRO_RWA_LC_ISSUERRATING
    ʵ�ֹ���:���ϵͳ-ծȯ���Ͷ��-�г�����-������������Ϣ(������Դ��¼���н�ծȯ���Ͷ�������Ϣȫ������RWA�г�������ƽӿڱ�����������Ϣ����)
    ���ݿھ�:ȫ��
    ����Ƶ��:�³�����
    ��  ��  :V1.0.0
    ��д��  :QHJIANG
    ��дʱ��:2016-04-14
    ��  λ  :�Ϻ���˶��Ϣ�����ɷ����޹�˾
    Դ  ��1 :RWA.RWA_WS_FCII_BOND|ծȯ���Ͷ�ʲ�¼��
    Դ  ��2 :RWA.RWA_WP_DATASUPPLEMENT|��¼������ϸ��Ϣ��
    Ŀ���1 :RWA_DEV.RWA_LC_ISSUERRATING|������������Ϣ��
    ������  :��
    �����¼(�޸���|�޸�ʱ��|�޸�����):
    */

  AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_LC_ISSUERRATING';
  --�����쳣����
  v_raise EXCEPTION;
  --���嵱ǰ����ļ�¼��
  v_count INTEGER;

  BEGIN

    --DBMS_OUTPUT.PUT_LINE('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    --���Ŀ����е�ԭ�м�¼
    --DELETE FROM RWA_LC_ISSUERRATING WHERE DATADATE=TO_DATE(p_data_dt_str,'yyyyMMdd');
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_LC_ISSUERRATING';


    --DBMS_OUTPUT.PUT_LINE('��ʼ�����롾������������Ϣ��' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));
    INSERT INTO RWA_DEV.RWA_LC_ISSUERRATING(
        				DATADATE                               --��������
                ,ISSUERID                           	 --������ID
                ,ISSUERNAME                    	 	 		 --����������
                ,RATINGORG                     	 	 		 --��������
                ,RATINGRESULT                  	 	 		 --�������
                ,RATINGDATE                    	 	 		 --��������
                ,FETCHFLAG                     	 	 		 --ȡ����ʶ
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
           AND T3.C_ACC_TYPE = 'D'																					--D�������࣬�ò���������Ϊ�г�����
           AND T3.FLD_DATE = p_data_dt_str																	--��Ч����Ʋ�Ʒ���ֵ����ÿ�ո���
           AND T3.DATANO = p_data_dt_str
    )
    SELECT 			DATADATE
    						,ISSUERID
    						,ISSUERNAME
    						,RATINGORG
    						,RATINGRESULT
    						,RATINGDATE
    						,FETCHFLAG
    FROM
    (
    SELECT
    						DATADATE
    						,ISSUERID
    						,ISSUERNAME
    						,RATINGORG
    						,RATINGRESULT
    						,RATINGDATE
    						,FETCHFLAG
    						,RANK() OVER(PARTITION BY ISSUERID,RATINGORG ORDER BY RATINGDATE DESC)  AS RK
    						,ROW_NUMBER() OVER(PARTITION BY ISSUERID,RATINGORG,RATINGDATE ORDER BY RATINGRESULT) AS RM
    						,COUNT(1) OVER(PARTITION BY ISSUERID,RATINGORG,RATINGDATE) AS RN
    FROM
    (
    SELECT
    						TO_DATE(p_data_dt_str,'YYYYMMDD')						 AS DATADATE       --RWAϵͳ��ֵ
        				,'LC' || T1.C_ISSUER_IDENTIFICATION_TYPE || T1.C_ISSUER_IDENTIFICATION_NO
        										                         				 AS ISSUERID       --ծȯ������
        				,NVL(T1.C_RWA_PUBLISHNAME,T4.C_ORG_NAME)		 AS ISSUERNAME     --ծȯ������
        				,T5.DITEMNO							               			 AS RATINGORG      --������������
        				,T3.DESCRATING															 AS RATINGRESULT   --������������
        				,T1.C_ISSUER_RELEASE_DATE										 AS RATINGDATE     --Ĭ�� ��
        				,''                             						 AS FETCHFLAG      --ȡ����ʶ
    FROM 				RWA_DEV.ZGS_ATBOND T1
    INNER JOIN	TEMP_INVESTASSETDETAIL T2
    ON					T1.C_BOND_CODE = T2.FLD_ASSET_CODE
    INNER JOIN	RWA_DEV.RWA_CD_RATING_MAPPING T3
    ON					T1.C_SCORE_TYPE = T3.SRCRATINGORG
    AND					T1.C_BODY_SCORE = T3.SRCRATING
    AND					T3.MAPPINGTYPE = 'LCI'
    LEFT JOIN		RWA_DEV.ZGS_ATTYORG T4
    ON					T1.C_PUBLISHER = T4.C_ORG_ID
    AND					T4.DATANO = p_data_dt_str
    LEFT JOIN		RWA_DEV.RWA_CD_CODE_MAPPING T5
    ON					T1.C_SCORE_TYPE = T5.SITEMNO
    AND					T5.SYSID = 'LC'
    AND					T5.SCODENO = 'ERAgency'
   	WHERE 			T1.DATANO = p_data_dt_str
		AND					T1.C_BODY_SCORE IS NOT NULL
		AND					T1.C_ISSUER_IDENTIFICATION_NO IS NOT NULL
   	UNION
   	SELECT
    						TO_DATE(p_data_dt_str,'YYYYMMDD')						 AS DATADATE       --RWAϵͳ��ֵ
        				,'LC' || T1.C_ISSUER_IDENTIFICATION_TYPE || T1.C_ISSUER_IDENTIFICATION_NO
        										                         				 AS ISSUERID       --ծȯ������
        				,NVL(T1.C_RWA_PUBLISHNAME,T4.C_ORG_NAME)		 AS ISSUERNAME     --ծȯ������
        				,T5.DITEMNO							               			 AS RATINGORG      --������������
        				,T3.DESCRATING															 AS RATINGRESULT   --������������
        				,T1.C_ISSUER_RELEASE_DATE2									 AS RATINGDATE     --Ĭ�� ��
        				,''                             						 AS FETCHFLAG      --ȡ����ʶ
    FROM 				RWA_DEV.ZGS_ATBOND T1
    INNER JOIN	TEMP_INVESTASSETDETAIL T2
    ON					T1.C_BOND_CODE = T2.FLD_ASSET_CODE
    INNER JOIN	RWA_DEV.RWA_CD_RATING_MAPPING T3
    ON					T1.C_SCORE_TYPE_2 = T3.SRCRATINGORG
    AND					T1.C_BODY_SCORE_2 = T3.SRCRATING
    AND					T3.MAPPINGTYPE = 'LCI'
    LEFT JOIN		RWA_DEV.ZGS_ATTYORG T4
    ON					T1.C_PUBLISHER = T4.C_ORG_ID
    AND					T4.DATANO = p_data_dt_str
    LEFT JOIN		RWA_DEV.RWA_CD_CODE_MAPPING T5
    ON					T1.C_SCORE_TYPE_2 = T5.SITEMNO
    AND					T5.SYSID = 'LC'
   	WHERE 			T1.DATANO = p_data_dt_str
		AND					T1.C_BODY_SCORE_2 IS NOT NULL
		AND					T1.C_ISSUER_IDENTIFICATION_NO IS NOT NULL
		UNION
   	SELECT
    						TO_DATE(p_data_dt_str,'YYYYMMDD')						 AS DATADATE       --RWAϵͳ��ֵ
        				,'ZGZYZF'		                         				 AS ISSUERID       --ծȯ������(Ŀǰ���� ��) ���貹¼���ݡ�
        				,'�й���������'															 AS ISSUERNAME     --ծȯ������(Ŀǰ���� ��)
        				,'01'										               			 AS RATINGORG      --������������
        				,(SELECT RATINGRESULT FROM RWA.RWA_WP_COUNTRYRATING WHERE COUNTRYCODE = 'CHN' AND ISINUSE = '1')
        																										 AS RATINGRESULT   --������������
        				,(SELECT REPLACE(RATINGSTARTDATE,'/','') FROM RWA.RWA_WP_COUNTRYRATING WHERE COUNTRYCODE = 'CHN' AND ISINUSE = '1')
        																										 AS RATINGDATE     --Ĭ�� ��
        				,''                             						 AS FETCHFLAG      --ȡ����ʶ
    FROM 				RWA_DEV.ZGS_ATBOND T1
    INNER JOIN	TEMP_INVESTASSETDETAIL T2
    ON					T1.C_BOND_CODE = T2.FLD_ASSET_CODE
   	WHERE 			T1.C_BOND_TYPE IN ('01','17','19')							--��ծ��Ĭ�Ϸ�������Ϣ
		AND					T1.DATANO = p_data_dt_str
		)
		)
		WHERE				RK = 1
		AND					RM = (CASE WHEN RN = 1 THEN 1 ELSE 2 END)				--����ͬһ�ͻ���ͬһ�챻ͬһ�һ���������Σ�ȡ�ڶ��õ��������
   	;

		COMMIT;

		dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_LC_ISSUERRATING',cascade => true);

    --DBMS_OUTPUT.PUT_LINE('���������롾������������Ϣ��' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*Ŀ�������ͳ��*/
    --ͳ�Ʋ���ļ�¼
    SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_LC_ISSUERRATING;
    --Dbms_output.Put_line('RWA_DEV.RWA_LC_ISSUERRATING��ǰ��������ϵͳ-ծȯ���Ͷ��(�г�����)-������������Ϣ���ݼ�¼Ϊ: ' || v_count || ' ��');

    --DBMS_OUTPUT.PUT_LINE('��ִ�� ' || v_pro_name || ' �洢���̽��� ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));
    p_po_rtncode := '1';
    p_po_rtnmsg  := '�ɹ�' || '-' || v_count;

    --�����쳣
    EXCEPTION WHEN OTHERS THEN
        --DBMS_OUTPUT.PUT_LINE('������,�������Ϊ:'||sqlcode||';������ϢΪ:'||sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace);
        ROLLBACK;
        p_po_rtncode := sqlcode;
        p_po_rtnmsg  := '������������Ϣ('|| v_pro_name ||')ETLת��ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;

    RETURN;

END PRO_RWA_LC_ISSUERRATING;
/

